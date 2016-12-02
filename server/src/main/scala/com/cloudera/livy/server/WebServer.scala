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

import java.net.InetAddress
import javax.servlet.ServletContextListener

import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler.{HandlerCollection, RequestLogHandler}
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler}
import org.eclipse.jetty.util.ssl.SslContextFactory

import com.cloudera.livy.{LivyConf, Logging}

object WebServer {
  val KeystoreKey = "livy.keystore"
  val KeystorePasswordKey = "livy.keystore.password"
}

class WebServer(livyConf: LivyConf, var host: String, var port: Int) extends Logging {
  val server = new Server()

  server.setStopTimeout(1000)
  server.setStopAtShutdown(true)

  val (connector, protocol) = Option(livyConf.get(WebServer.KeystoreKey)) match {
    case None =>
      (new ServerConnector(server), "http")

    case Some(keystore) =>
      val https = new HttpConfiguration()
      https.addCustomizer(new SecureRequestCustomizer())

      val sslContextFactory = new SslContextFactory()
      sslContextFactory.setKeyStorePath(keystore)
      Option(livyConf.get(WebServer.KeystorePasswordKey))
        .foreach(sslContextFactory.setKeyStorePassword)
      Option(livyConf.get(WebServer.KeystorePasswordKey))
        .foreach(sslContextFactory.setKeyManagerPassword)

      (new ServerConnector(server,
        new SslConnectionFactory(sslContextFactory, "http/1.1"),
        new HttpConnectionFactory(https)), "https")
  }

  connector.setHost(host)
  connector.setPort(port)

  server.setConnectors(Array(connector))

  val context = new ServletContextHandler()

  context.setContextPath("/")
  context.addServlet(classOf[DefaultServlet], "/")

  val handlers = new HandlerCollection
  handlers.addHandler(context)

  // Configure the access log
  val requestLogHandler = new RequestLogHandler
  val requestLog = new NCSARequestLog(sys.env.getOrElse("LIVY_LOG_DIR",
    sys.env("LIVY_HOME") + "/logs") + "/yyyy_mm_dd.request.log")
  requestLog.setAppend(true)
  requestLog.setExtended(false)
  requestLog.setLogTimeZone("GMT")
  requestLogHandler.setRequestLog(requestLog)
  handlers.addHandler(requestLogHandler)

  server.setHandler(handlers)

  def addEventListener(listener: ServletContextListener): Unit = {
    context.addEventListener(listener)
  }

  def start(): Unit = {
    server.start()

    val connector = server.getConnectors()(0).asInstanceOf[NetworkConnector]

    if (host == "0.0.0.0") {
      host = InetAddress.getLocalHost.getCanonicalHostName
    }
    port = connector.getLocalPort

    info("Starting server on %s://%s:%d" format (protocol, host, port))
  }

  def join(): Unit = {
    server.join()
  }

  def stop(): Unit = {
    context.stop()
    server.stop()
  }
}

