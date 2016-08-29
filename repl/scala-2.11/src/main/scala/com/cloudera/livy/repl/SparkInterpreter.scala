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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

class SparkInterpreter(conf: SparkConf) extends AbstractSparkInterpreter {

  private var sparkContext: SparkContext = _
  private var sqlContext: SQLContext = _
  private var sparkILoop: SparkILoop = _

  override def start(): SparkContext = {
    require(sparkILoop == null)

    val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    val outputDir = createTempDir(rootDir)

    val classServer = createHttpServer(outputDir)
    invoke[Unit](
      Class.forName("org.apache.spark.HttpServer"), "start", classServer, Seq.empty, Seq.empty)
    val uri = invoke[String](Class.forName("org.apache.spark.HttpServer"),
        "uri", classServer, Seq.empty, Seq.empty)
    require(uri != null)
    conf.set("spark.repl.class.uri", uri)

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
        if (classLoader.getClass.getCanonicalName == "org.apache.spark.util.MutableURLClassLoader") {
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

      sparkContext = SparkContext.getOrCreate(conf)
      if (conf.getBoolean("spark.repl.enableHiveContext", false)) {
        try {
          val loader = Option(Thread.currentThread().getContextClassLoader)
            .getOrElse(getClass.getClassLoader)
          if (loader.getResource("hive-site.xml") == null) {
            warn("livy.repl.enableHiveContext is true but no hive-site.xml found on classpath.")
          }
          sqlContext = new HiveContext(sparkContext)
          info("Created sql context (with Hive support).")
        } catch {
          case _: java.lang.NoClassDefFoundError =>
            sqlContext = new SQLContext(sparkContext)
            info("Created sql context.")
        }
      } else {
        sqlContext = new SQLContext(sparkContext)
        info("Created sql context.")
      }
      sparkILoop.beQuietDuring {
        sparkILoop.intp.bind(
          "sc", "org.apache.spark.SparkContext", sparkContext, List("""@transient"""))
      }
      sparkILoop.beQuietDuring {
        sparkILoop.intp.bind("sqlContext", sqlContext.getClass.getCanonicalName,
          sqlContext, List("""@transient"""))
      }

      execute("import org.apache.spark.SparkContext._")
      execute("import sqlContext.implicits._")
      execute("import sqlContext.sql")
      execute("import org.apache.spark.sql.functions._")
    }

    sparkContext
  }

  override def close(): Unit = synchronized {
    if (sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
    }

    if (sparkILoop != null) {
      sparkILoop.closeInterpreter()
      sparkILoop = null
    }

    sqlContext = null
  }

  override def isStarted(): Boolean = {
    sparkContext != null && sparkILoop != null
  }

  override def interpret(code: String): Result = {
    sparkILoop.interpret(code)
  }

  override def valueOfTerm(name: String): Option[Any] = {
    sparkILoop.valueOfTerm(name)
  }

  private def createTempDir(rootDir: String): File = {
    try {
      invoke[File](Class.forName("org.apache.spark.util.Util"), "createTempDir", null,
        Seq(classOf[String], classOf[String]), Seq(rootDir, "spark"))
    } catch {
      case NonFatal(e) =>
        val file = new File(rootDir, s"spark-${UUID.randomUUID().toString}")
        file.mkdir()
        file
    }
  }

  private def createHttpServer(outputDir: File): Object = {
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
    classServerConstructor.newInstance(conf, outputDir, securityManager, new Integer(0),
      "HTTP server")
      .asInstanceOf[Object]
  }

  private def invoke[T](
      clz: Class[_],
      name: String,
      obj: Object,
      argClz: Seq[Class[_]],
      args: Seq[Object]): T = {
    try {
      val method = clz.getMethod(name, argClz: _*)
      method.setAccessible(true)
      method.invoke(obj, args: _*).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        warn(s"Fail to invoke method ${clz.getCanonicalName}#$name", e)
        null.asInstanceOf[T]
    }
  }
}
