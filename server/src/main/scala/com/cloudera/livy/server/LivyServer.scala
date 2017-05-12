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

import java.io.{BufferedInputStream, InputStream}
import java.util.concurrent._
import java.util.EnumSet
import javax.servlet._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.authentication.server._
import org.eclipse.jetty.servlet.FilterHolder
import org.scalatra.metrics.MetricsBootstrap
import org.scalatra.metrics.MetricsSupportExtensions._
import org.scalatra.{NotFound, ScalatraServlet}
import org.scalatra.servlet.{MultipartConfig, ServletApiImplicits}

import com.cloudera.livy._
import com.cloudera.livy.server.batch.BatchSessionServlet
import com.cloudera.livy.server.interactive.InteractiveSessionServlet
import com.cloudera.livy.server.recovery.{SessionStore, StateStore}
import com.cloudera.livy.server.ui.UIServlet
import com.cloudera.livy.sessions.{BatchSessionManager, InteractiveSessionManager}
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

    // Test spark-submit and get Spark Scala version accordingly.
    val (sparkVersion, scalaVersionFromSparkSubmit) = sparkSubmitVersion(livyConf)
    testSparkVersion(sparkVersion)

    // If Spark and Scala version is set manually, should verify if they're consistent with
    // ones parsed from "spark-submit --version"
    val formattedSparkVersion = formatSparkVersion(sparkVersion)
    Option(livyConf.get(LIVY_SPARK_VERSION)).map(formatSparkVersion).foreach { version =>
      require(formattedSparkVersion == version,
        s"Configured Spark version $version is not equal to Spark version $formattedSparkVersion " +
          "got from spark-submit -version")
    }

    // Set formatted Spark and Scala version into livy configuration, this will be used by
    // session creation.
    // TODO Create a new class to pass variables from LivyServer to sessions and remove these
    // internal LivyConfs.
    livyConf.set(LIVY_SPARK_VERSION.key, formattedSparkVersion.productIterator.mkString("."))
    livyConf.set(LIVY_SPARK_SCALA_VERSION.key,
      sparkScalaVersion(formattedSparkVersion, scalaVersionFromSparkSubmit, livyConf))

    if (UserGroupInformation.isSecurityEnabled) {
      // If Hadoop security is enabled, run kinit periodically. runKinit() should be called
      // before any Hadoop operation, otherwise Kerberos exception will be thrown.
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
        livyConf.get(LAUNCH_KERBEROS_PRINCIPAL), host)
      require(launch_keytab != null,
        s"Kerberos requires ${LAUNCH_KERBEROS_KEYTAB.key} to be provided.")
      require(launch_principal != null,
        s"Kerberos requires ${LAUNCH_KERBEROS_PRINCIPAL.key} to be provided.")
      if (!runKinit(launch_keytab, launch_principal)) {
        error("Failed to run kinit, stopping the server.")
        sys.exit(1)
      }
      startKinitThread(launch_keytab, launch_principal)
    }

    testRecovery(livyConf)

    // Initialize YarnClient ASAP to save time.
    if (livyConf.isRunningOnYarn()) {
      SparkYarnApp.init(livyConf)
      Future { SparkYarnApp.yarnClient }
    }

    StateStore.init(livyConf)
    val sessionStore = new SessionStore(livyConf)
    val batchSessionManager = new BatchSessionManager(livyConf, sessionStore)
    val interactiveSessionManager = new InteractiveSessionManager(livyConf, sessionStore)

    server = new WebServer(livyConf, host, port)
    server.context.setResourceBase("src/main/com/cloudera/livy/server")

    val livyVersionServlet = new JsonServlet {
      before() { contentType = "application/json" }

      get("/") {
        Map("version" -> LIVY_VERSION,
          "user" -> LIVY_BUILD_USER,
          "revision" -> LIVY_REVISION,
          "branch" -> LIVY_BRANCH,
          "date" -> LIVY_BUILD_DATE,
          "url" -> LIVY_REPO_URL)
      }
    }

    // Servlet for hosting static files such as html, css, and js
    // Necessary since Jetty cannot set it's resource base inside a jar
    // Returns 404 if the file does not exist
    val staticResourceServlet = new ScalatraServlet {
      get("/*") {
        val fileName = params("splat")
        val notFoundMsg = "File not found"

        if (!fileName.isEmpty) {
          getClass.getResourceAsStream(s"ui/static/$fileName") match {
            case is: InputStream => new BufferedInputStream(is)
            case null => NotFound(notFoundMsg)
          }
        } else {
          NotFound(notFoundMsg)
        }
      }
    }

    def uiRedirectServlet(path: String) = new ScalatraServlet {
      get("/") {
        redirect(path)
      }
    }

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

            val interactiveServlet =
              new InteractiveSessionServlet(interactiveSessionManager, sessionStore, livyConf)
            mount(context, interactiveServlet, "/sessions/*")

            val batchServlet = new BatchSessionServlet(batchSessionManager, sessionStore, livyConf)
            mount(context, batchServlet, "/batches/*")

            if (livyConf.getBoolean(UI_ENABLED)) {
              val uiServlet = new UIServlet
              mount(context, uiServlet, "/ui/*")
              mount(context, staticResourceServlet, "/static/*")
              mount(context, uiRedirectServlet("/ui/"), "/*")
            } else {
              mount(context, uiRedirectServlet("/metrics"), "/*")
            }

            context.mountMetricsAdminServlet("/metrics")

            mount(context, livyVersionServlet, "/version/*")
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

    _serverUrl = Some(s"${server.protocol}://${server.host}:${server.port}")
    sys.props("livy.server.server-url") = _serverUrl.get
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
