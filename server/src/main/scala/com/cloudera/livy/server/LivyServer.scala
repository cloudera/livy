/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.server

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.{EnumSet, Properties}
import java.util.concurrent._
import javax.servlet._

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.authentication.server._
import org.eclipse.jetty.servlet.FilterHolder
import org.scalatra.metrics.MetricsBootstrap
import org.scalatra.metrics.MetricsSupportExtensions._
import org.scalatra.servlet.{MultipartConfig, ServletApiImplicits}

import com.cloudera.livy._
import com.cloudera.livy.server.batch.BatchSessionServlet
import com.cloudera.livy.server.interactive.InteractiveSessionServlet
import com.cloudera.livy.server.recovery.{SessionStore, StateStore}
import com.cloudera.livy.sessions.BatchSessionManager
import com.cloudera.livy.sessions.SessionManager.SESSION_RECOVERY_MODE_OFF
import com.cloudera.livy.utils.LivySparkUtils._
import com.cloudera.livy.utils.SparkYarnApp

class LivyServer extends Logging {

  import LivyConf._

  private var server: WebServer = _
  private var _serverUrl: Option[String] = None
  // make livyConf accessible for testing
  private[livy] var livyConf: LivyConf = _

  private var kinitFailCount: Int = 0
  private var executor: ScheduledExecutorService = _

  def start(): Unit = {
    livyConf = new LivyConf().loadFromFile("livy.conf")

    val host = livyConf.get(SERVER_HOST)
    val port = livyConf.getInt(SERVER_PORT)
    val multipartConfig = MultipartConfig(
        maxFileSize = Some(livyConf.getLong(LivyConf.FILE_UPLOAD_MAX_SIZE))
      ).toMultipartConfigElement

    // Make sure the `spark-submit` program exists, otherwise much of livy won't work.
    testSparkHome(livyConf)
    testSparkSubmit(livyConf)

    testRecovery(livyConf)

    // Initialize YarnClient ASAP to save time.
    if (livyConf.isRunningOnYarn()) {
      Future { SparkYarnApp.yarnClient }
    }

    StateStore.init(livyConf)
    val sessionStore = new SessionStore(livyConf)
    val batchSessionManager = new BatchSessionManager(livyConf, sessionStore)

    server = new WebServer(livyConf, host, port)
    server.context.setResourceBase("src/main/com/cloudera/livy/server")
    server.context.addEventListener(
      new ServletContextListener() with MetricsBootstrap with ServletApiImplicits {

        private def mount(sc: ServletContext, servlet: Servlet, mappings: String*): Unit = {
          val registration = sc.addServlet(servlet.getClass().getName(), servlet)
          registration.addMapping(mappings: _*)
          registration.setMultipartConfig(multipartConfig)
        }

        override def contextDestroyed(sce: ServletContextEvent): Unit = {

        }

        override def contextInitialized(sce: ServletContextEvent): Unit = {
          try {
            val context = sce.getServletContext()
            context.initParameters(org.scalatra.EnvironmentKey) = livyConf.get(ENVIRONMENT)

            mount(context, new InteractiveSessionServlet(livyConf), "/sessions/*")

            val batchServlet = new BatchSessionServlet(batchSessionManager, sessionStore, livyConf)
            mount(context, batchServlet, "/batches/*")

            context.mountMetricsAdminServlet("/")
          } catch {
            case e: Throwable =>
              error("Exception thrown when initializing server", e)
              sys.exit(1)
          }
        }

      })

    livyConf.get(AUTH_TYPE) match {
      case authType @ KerberosAuthenticationHandler.TYPE =>
        val principal = SecurityUtil.getServerPrincipal(livyConf.get(AUTH_KERBEROS_PRINCIPAL),
          server.host)
        val keytab = livyConf.get(AUTH_KERBEROS_KEYTAB)
        require(principal != null,
          s"Kerberos auth requires ${AUTH_KERBEROS_PRINCIPAL.key} to be provided.")
        require(keytab != null,
          s"Kerberos auth requires ${AUTH_KERBEROS_KEYTAB.key} to be provided.")

        val holder = new FilterHolder(new AuthenticationFilter())
        holder.setInitParameter(AuthenticationFilter.AUTH_TYPE, authType)
        holder.setInitParameter(KerberosAuthenticationHandler.PRINCIPAL, principal)
        holder.setInitParameter(KerberosAuthenticationHandler.KEYTAB, keytab)
        holder.setInitParameter(KerberosAuthenticationHandler.NAME_RULES,
          livyConf.get(AUTH_KERBEROS_NAME_RULES))
        server.context.addFilter(holder, "/*", EnumSet.allOf(classOf[DispatcherType]))
        info(s"SPNEGO auth enabled (principal = $principal)")

        // run kinit periodically
        executor = Executors.newScheduledThreadPool(1,
          new ThreadFactory() {
            override def newThread(r: Runnable): Thread = {
              val thread = new Thread(r)
              thread.setName("kinit-thread")
              thread.setDaemon(true)
              thread
            }
          }
        )
        val launch_keytab = livyConf.get(LAUNCH_KERBEROS_KEYTAB)
        val launch_principal = SecurityUtil.getServerPrincipal(
          livyConf.get(LAUNCH_KERBEROS_PRINCIPAL), server.host)
        require(launch_keytab != null,
          s"Kerberos requires ${LAUNCH_KERBEROS_KEYTAB.key} to be provided.")
        require(launch_principal != null,
          s"Kerberos requires ${LAUNCH_KERBEROS_PRINCIPAL.key} to be provided.")
        if (!runKinit(launch_keytab, launch_principal)) {
          error("Failed to run kinit, stopping the server.")
          sys.exit(1)
        }
        startKinitThread(launch_keytab, launch_principal)

      case null =>
        // Nothing to do.

      case other =>
        throw new IllegalArgumentException(s"Invalid auth type: $other")
    }

    if (livyConf.getBoolean(CSRF_PROTECTION)) {
      info("CSRF protection is enabled.")
      val csrfHolder = new FilterHolder(new CsrfFilter())
      server.context.addFilter(csrfHolder, "/*", EnumSet.allOf(classOf[DispatcherType]))
    }

    if (livyConf.getBoolean(ACCESS_CONTROL_ENABLED)) {
      if (livyConf.get(AUTH_TYPE) != null) {
        info("Access control is enabled.")
        val accessHolder = new FilterHolder(new AccessFilter(livyConf))
        server.context.addFilter(accessHolder, "/*", EnumSet.allOf(classOf[DispatcherType]))
      } else {
        throw new IllegalArgumentException("Access control was requested but could " +
          "not be enabled, since authentication is disabled.")
      }
    }

    server.start()

    Runtime.getRuntime().addShutdownHook(new Thread("Livy Server Shutdown") {
      override def run(): Unit = {
        info("Shutting down Livy server.")
        server.stop()
      }
    })

    _serverUrl = Some(s"http://${server.host}:${server.port}")
    sys.props("livy.server.serverUrl") = _serverUrl.get
  }

  def runKinit(keytab: String, principal: String): Boolean = {
    val commands = Seq("kinit", "-kt", keytab, principal)
    val proc = new ProcessBuilder(commands: _*).inheritIO().start()
    proc.waitFor() match {
      case 0 =>
        debug("Ran kinit command successfully.")
        kinitFailCount = 0
        true
      case _ =>
        warn("Fail to run kinit command.")
        kinitFailCount += 1
        false
    }
  }

  def startKinitThread(keytab: String, principal: String): Unit = {
    val refreshInterval = livyConf.getTimeAsMs(LAUNCH_KERBEROS_REFRESH_INTERVAL)
    val kinitFailThreshold = livyConf.getInt(KINIT_FAIL_THRESHOLD)
    executor.schedule(
      new Runnable() {
        override def run(): Unit = {
          if (runKinit(keytab, principal)) {
            // schedule another kinit run with a fixed delay.
            executor.schedule(this, refreshInterval, TimeUnit.MILLISECONDS)
          } else {
            // schedule another retry at once or fail the livy server if too many times kinit fail
            if (kinitFailCount >= kinitFailThreshold) {
              error(s"Exit LivyServer after ${kinitFailThreshold} times failures running kinit.")
              if (server.server.isStarted()) {
                stop()
              } else {
                sys.exit(1)
              }
            } else {
              executor.submit(this)
            }
          }
        }
      }, refreshInterval, TimeUnit.MILLISECONDS)
  }

  def join(): Unit = server.join()

  def stop(): Unit = {
    if (server != null) {
      server.stop()
    }
  }

  def serverUrl(): String = {
    _serverUrl.getOrElse(throw new IllegalStateException("Server not yet started."))
  }

  private[livy] def testRecovery(livyConf: LivyConf): Unit = {
    if (!livyConf.isRunningOnYarn()) {
      // If recovery is turned on but we are not running on YARN, quit.
      require(livyConf.get(LivyConf.RECOVERY_MODE) == SESSION_RECOVERY_MODE_OFF,
        "Session recovery requires YARN.")
    }
  }
}

object LivyServer {

  def main(args: Array[String]): Unit = {
    val server = new LivyServer()
    try {
      server.start()
      server.join()
    } finally {
      server.stop()
    }
  }

}

/**
 * A Mini Livy server used for integration test.
 */
object MiniLivyMain extends Logging {

  def main(args: Array[String]): Unit = {
    val klass = getClass.getSimpleName
    info(s"$klass is starting up")

    val Array(configPath) = args

    var livyConf = Map(
      "livy.spark.master" -> "yarn",
      "livy.spark.deployMode" -> "cluster",
      "livy.server.yarn.poll-interval" -> "500ms")

    if (sys.env.contains("TRAVIS")) {
      livyConf ++= Map("livy.server.yarn.app-lookup-timeout" -> "2m")
    }

    val livyReplJars = {
      val jarsPath = sys.env("LIVY_HOME") + File.separator + "repl" + File.separator +
        sys.props("repl.jars.version") + File.separator + "target" + File.separator + "jars"
      val jarsDir = Option(new File(jarsPath))
        .filter(p => p.isDirectory && p.listFiles().nonEmpty)
        .getOrElse(throw new IllegalStateException(s"$jarsPath is not existed or empty"))
      jarsDir.listFiles().map(_.getAbsolutePath).mkString(",")
    }
    livyConf ++= Map("livy.repl.jars" -> livyReplJars)

    saveProperties(livyConf, new File(configPath + "/livy.conf"))

    val server = new LivyServer()
    server.start()

    val conf = server.livyConf
    conf.set("livy.repl.enableHiveContext", "true")

    // Write a serverUrl.conf file to the conf directory with the location of the Livy
    // server. Do it atomically since it's used by MiniCluster to detect when the Livy server
    // is up and ready.
    eventually(30 seconds, 1 second) {
      val serverUrlConf = Map("livy.server.serverUrl" -> server.serverUrl())
      saveProperties(serverUrlConf, new File(configPath + "/serverUrl.conf"))
    }

    info(s"$klass running")

    while(true) synchronized {
      wait()
    }
  }

  /**
   * A simplified version of scalatest eventually, rewritten here to avoid adding extra test
   * dependency.
   */
  private def eventually[T](timeout: Duration, interval: Duration)(func: => T): T = {
    def makeAttempt(): Either[Throwable, T] = {
      try {
        Right(func)
      } catch {
        case e if NonFatal(e) => Left(e)
      }
    }

    val startTime = System.currentTimeMillis()
    @tailrec
    def tryAgain(attempt: Int): T = {
      makeAttempt() match {
        case Right(result) => result
        case Left(e) =>
          val duration = System.currentTimeMillis() - startTime
          if (duration < timeout.toMillis) {
            Thread.sleep(interval.toMillis)
          } else {
            throw new TimeoutException(e.getMessage)
          }

          tryAgain(attempt + 1)
      }
    }

    tryAgain(1)
  }

  private def saveProperties(props: Map[String, String], dest: File): Unit = {
    val jprops = new Properties()
    props.foreach { case (k, v) => jprops.put(k, v) }

    val tempFile = new File(dest.getAbsolutePath() + ".tmp")
    val out = new OutputStreamWriter(new FileOutputStream(tempFile), UTF_8)
    try {
      jprops.store(out, "Configuration")
    } finally {
      out.close()
    }
    tempFile.renameTo(dest)
  }
}
