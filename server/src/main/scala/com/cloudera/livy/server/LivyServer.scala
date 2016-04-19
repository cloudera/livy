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

import java.io.{File, IOException}
import java.net.InetAddress
import java.util.EnumSet
import javax.servlet._

import org.apache.hadoop.security.authentication.server._
import org.apache.hadoop.security.SecurityUtil
import org.eclipse.jetty.servlet.FilterHolder
import org.scalatra.metrics.MetricsBootstrap
import org.scalatra.metrics.MetricsSupportExtensions._
import org.scalatra.servlet.ServletApiImplicits

import com.cloudera.livy._
import com.cloudera.livy.server.batch.BatchSessionServlet
import com.cloudera.livy.server.client.ClientSessionServlet
import com.cloudera.livy.server.interactive.InteractiveSessionServlet
import com.cloudera.livy.util.LineBufferedProcess

class LivyServer extends Logging {

  private val ENVIRONMENT = LivyConf.Entry("livy.environment", "production")
  private val SERVER_HOST = LivyConf.Entry("livy.server.host", "0.0.0.0")
  private val SERVER_PORT = LivyConf.Entry("livy.server.port", 8998)
  private val AUTH_TYPE = LivyConf.Entry("livy.server.auth.type", null)
  private val KERBEROS_PRINCIPAL = LivyConf.Entry("livy.server.auth.kerberos.principal", null)
  private val KERBEROS_KEYTAB = LivyConf.Entry("livy.server.auth.kerberos.keytab", null)
  private val KERBEROS_NAME_RULES = LivyConf.Entry("livy.server.auth.kerberos.name_rules",
    "DEFAULT")

  private var server: WebServer = _
  private var _serverUrl: Option[String] = None

  def start(): Unit = {
    val livyConf = new LivyConf().loadFromFile("livy-defaults.conf")
    val host = livyConf.get(SERVER_HOST)
    val port = livyConf.getInt(SERVER_PORT)

    // Make sure the `spark-submit` program exists, otherwise much of livy won't work.
    testSparkHome(livyConf)
    testSparkSubmit(livyConf)

    server = new WebServer(livyConf, host, port)
    server.context.setResourceBase("src/main/com/cloudera/livy/server")
    server.context.addEventListener(
      new ServletContextListener() with MetricsBootstrap with ServletApiImplicits {

        override def contextDestroyed(sce: ServletContextEvent): Unit = {

        }

        override def contextInitialized(sce: ServletContextEvent): Unit = {
          try {
            val context = sce.getServletContext()
            context.initParameters(org.scalatra.EnvironmentKey) = livyConf.get(ENVIRONMENT)
            context.mount(new InteractiveSessionServlet(livyConf), "/sessions/*")
            context.mount(new BatchSessionServlet(livyConf), "/batches/*")
            context.mount(new ClientSessionServlet(livyConf), "/clients/*")
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
        val principal = SecurityUtil.getServerPrincipal(livyConf.get(KERBEROS_PRINCIPAL),
          server.host)
        val keytab = livyConf.get(KERBEROS_KEYTAB)
        require(principal != null,
          s"Kerberos auth requires ${KERBEROS_PRINCIPAL.key} to be provided.")
        require(keytab != null,
          s"Kerberos auth requires ${KERBEROS_KEYTAB.key} to be provided.")

        val holder = new FilterHolder(new AuthenticationFilter())
        holder.setInitParameter(AuthenticationFilter.AUTH_TYPE, authType)
        holder.setInitParameter(KerberosAuthenticationHandler.PRINCIPAL, principal)
        holder.setInitParameter(KerberosAuthenticationHandler.KEYTAB, keytab)
        holder.setInitParameter(KerberosAuthenticationHandler.NAME_RULES,
          livyConf.get(KERBEROS_NAME_RULES))
        server.context.addFilter(holder, "/*", EnumSet.allOf(classOf[DispatcherType]))
        info(s"SPNEGO auth enabled (principal = $principal)")
        if (!livyConf.getBoolean(LivyConf.IMPERSONATION_ENABLED)) {
          info(s"Enabling impersonation since auth type is $authType.")
          livyConf.set(LivyConf.IMPERSONATION_ENABLED, true)
        }

      case null =>
        // Nothing to do.

      case other =>
        throw new IllegalArgumentException(s"Invalid auth type: $other")
    }

    server.start()

    _serverUrl = Some(s"http://${server.host}:${server.port}")
    sys.props("livy.server.serverUrl") = _serverUrl.get
  }

  def join(): Unit = server.join()

  def stop(): Unit = {
    server.stop()
  }

  def serverUrl(): String = {
    _serverUrl.getOrElse(throw new IllegalStateException("Server not yet started."))
  }

  /**
   * Sets the spark-submit path if it's not configured in the LivyConf
   */
  private[server] def testSparkHome(livyConf: LivyConf): Unit = {
    val sparkHome = livyConf.sparkHome().getOrElse {
      throw new IllegalArgumentException("Livy requires the SPARK_HOME environment variable")
    }

    require(new File(sparkHome).isDirectory(), "SPARK_HOME path does not exist")
  }

  /**
   * Test that the configured `spark-submit` executable exists.
   *
   * @param livyConf
   */
  private[server] def testSparkSubmit(livyConf: LivyConf): Unit = {
    try {
      val version = sparkSubmitVersion(livyConf)
      logger.info(f"Using spark-submit version $version")
    } catch {
      case e: IOException =>
        throw new IOException("Failed to run spark-submit executable", e)
    }
  }

  /**
   * Return the version of the configured `spark-submit` version.
   *
   * @param livyConf
   * @return the version
   */
  private def sparkSubmitVersion(livyConf: LivyConf): String = {
    val sparkSubmit = livyConf.sparkSubmit()
    val pb = new ProcessBuilder(sparkSubmit, "--version")
    pb.redirectErrorStream(true)
    pb.redirectInput(ProcessBuilder.Redirect.PIPE)

    if (LivyConf.TEST_MODE) {
      pb.environment().put("LIVY_TEST_CLASSPATH", sys.props("java.class.path"))
    }

    val process = new LineBufferedProcess(pb.start())
    val exitCode = process.waitFor()
    val output = process.inputIterator.mkString("\n")

    val regex = """version (.*)""".r.unanchored

    output match {
      case regex(version) => version
      case _ =>
        throw new IOException(f"Unable to determine spark-submit version [$exitCode]:\n$output")
    }
  }

}

object LivyServer {

  def main(args: Array[String]): Unit = {
    val server = new LivyServer()
    try {
      server.join()
    } finally {
      server.stop()
    }
  }

}
