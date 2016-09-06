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

package com.cloudera.livy.repl

import java.io.File
import java.net.URLClassLoader
import java.nio.file.Paths
import java.util.UUID

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.JPrintWriter
import scala.tools.nsc.interpreter.Results.Result
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.repl.SparkILoop

/**
 * Scala 2.11 version of SparkInterpreter
 */
class SparkInterpreter(conf: SparkConf)
  extends AbstractSparkInterpreter with SparkContextInitializer {

  protected var sparkContext: SparkContext = _
  private var sparkILoop: SparkILoop = _
  private var classServer: Object = _

  override def start(): SparkContext = {
    require(sparkILoop == null)

    val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    val outputDir = createTempDir(rootDir)
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)

    // Only Spark1 requires to create http server, Spark2 removes HttpServer class.
    Option(startHttpServer(outputDir)).foreach { case (server, uri) =>
      classServer = server
      conf.set("spark.repl.class.uri", uri)
    }

    val settings = new Settings()
    settings.processArguments(List("-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"), true)
    settings.usejavacp.value = true
    settings.embeddedDefaults(Thread.currentThread().getContextClassLoader())

    sparkILoop = new SparkILoop(None, new JPrintWriter(outputStream, true))
    sparkILoop.settings = settings
    sparkILoop.createInterpreter()
    sparkILoop.initializeSynchronous()

    restoreContextClassLoader {
      sparkILoop.setContextClassLoader()

      var classLoader = Thread.currentThread().getContextClassLoader
      while (classLoader != null) {
        if (classLoader.getClass.getCanonicalName ==
          "org.apache.spark.util.MutableURLClassLoader") {
          val extraJarPath = classLoader.asInstanceOf[URLClassLoader].getURLs()
            // Check if the file exists. Otherwise an exception will be thrown.
            .filter { u => u.getProtocol == "file" && new File(u.getPath).isFile }
            // Livy rsc and repl are also in the extra jars list. Filter them out.
            .filterNot { u => Paths.get(u.toURI).getFileName.toString.startsWith("livy-") }

          extraJarPath.foreach { p => debug(s"Adding $p to Scala interpreter's class path...") }
          sparkILoop.addUrlsToClassPath(extraJarPath: _*)
          classLoader = null
        } else {
          classLoader = classLoader.getParent
        }
      }

      createSparkContext(conf)
    }

    sparkContext
  }

  override def close(): Unit = synchronized {
    if (classServer != null) {
      val method = classServer.getClass.getMethod("stop")
      method.setAccessible(true)
      method.invoke(classServer)
      classServer = null
    }
    if (sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
    }

    if (sparkILoop != null) {
      sparkILoop.closeInterpreter()
      sparkILoop = null
    }
  }

  override protected def isStarted(): Boolean = {
    sparkContext != null && sparkILoop != null
  }

  override protected def interpret(code: String): Result = {
    sparkILoop.interpret(code)
  }

  override protected def valueOfTerm(name: String): Option[Any] = {
    // IMain#valueOfTerm will always return None, so use other way instead.
    Option(sparkILoop.lastRequest.lineRep.call("$result"))
  }

  protected def bind(name: String, tpe: String, value: Object, modifier: List[String]): Unit = {
    sparkILoop.beQuietDuring {
      sparkILoop.bind(name, tpe, value, modifier)
    }
  }

  private def createTempDir(rootDir: String): File = {
    try {
      val method = Class.forName("org.apache.spark.util.Util")
        .getMethod("createTempDir", classOf[String], classOf[String])
      method.setAccessible(true)
      method.invoke(null, rootDir, "spark").asInstanceOf[File]
    } catch {
      case NonFatal(e) =>
        val file = new File(rootDir, s"spark-${UUID.randomUUID().toString}")
        file.mkdir()
        file
    }
  }

  private def startHttpServer(outputDir: File): (Object, String) = {
    try {
      val securityManager = {
        val constructor = Class.forName("org.apache.spark.SecurityManager")
          .getConstructor(classOf[SparkConf])
        constructor.setAccessible(true)
        constructor.newInstance(conf).asInstanceOf[Object]
      }
      val classServerConstructor = Class.forName("org.apache.spark.HttpServer")
        .getConstructor(classOf[SparkConf],
          classOf[File],
          Class.forName("org.apache.spark.SecurityManager"),
          classOf[Int],
          classOf[String])
      classServerConstructor.setAccessible(true)
      // Create Http Server
      val server = classServerConstructor
        .newInstance(conf, outputDir, securityManager, new Integer(0), "HTTP server")
        .asInstanceOf[Object]

      // Start Http Server
      val startMethod = server.getClass.getMethod("start")
      startMethod.setAccessible(true)
      startMethod.invoke(server)

      // Get uri of this Http Server
      val uriMethod = server.getClass.getMethod("uri")
      uriMethod.setAccessible(true)
      val uri = uriMethod.invoke(server).asInstanceOf[String]
      (server, uri)
    } catch {
      // Spark 2.0+ removed HttpServer, so return null instead.
      case NonFatal(e) =>
        null
    }
  }
}
