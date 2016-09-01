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

import java.io._
import java.net.URLClassLoader
import java.nio.file.Paths

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Results.Result
import scala.tools.nsc.interpreter.JPrintWriter
import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.repl.SparkIMain
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

/**
 * This represents a Spark interpreter. It is not thread safe.
 */
class SparkInterpreter(conf: SparkConf)
  extends AbstractSparkInterpreter with SparkContextInitializer {

  private var sparkIMain: SparkIMain = _
  protected var sparkContext: SparkContext = _

  override def start(): SparkContext = {
    require(sparkIMain == null && sparkContext == null)

    val settings = new Settings()
    settings.embeddedDefaults(Thread.currentThread().getContextClassLoader())
    settings.usejavacp.value = true

    sparkIMain = new SparkIMain(settings, new JPrintWriter(outputStream, true))
    sparkIMain.initializeSynchronous()

    // Spark 1.6 does not have "classServerUri"; instead, the local directory where class files
    // are stored needs to be registered in SparkConf. See comment in
    // SparkILoop::createSparkContext().
    Try(sparkIMain.getClass().getMethod("classServerUri")) match {
      case Success(method) =>
        method.setAccessible(true)
        conf.set("spark.repl.class.uri", method.invoke(sparkIMain).asInstanceOf[String])

      case Failure(_) =>
        val outputDir = sparkIMain.getClass().getMethod("getClassOutputDirectory")
        outputDir.setAccessible(true)
        conf.set("spark.repl.class.outputDir",
          outputDir.invoke(sparkIMain).asInstanceOf[File].getAbsolutePath())
    }

    restoreContextClassLoader {
      // Call sparkIMain.setContextClassLoader() to make sure SparkContext and repl are using the
      // same ClassLoader. Otherwise if someone defined a new class in interactive shell,
      // SparkContext cannot see them and will result in job stage failure.
      val setContextClassLoaderMethod = sparkIMain.getClass().getMethod("setContextClassLoader")
      setContextClassLoaderMethod.setAccessible(true)
      setContextClassLoaderMethod.invoke(sparkIMain)

      // With usejavacp=true, the Scala interpreter looks for jars under System Classpath. But it
      // doesn't look for jars added to MutableURLClassLoader. Thus extra jars are not visible to
      // the interpreter. SparkContext can use them via JVM ClassLoaders but users cannot import
      // them using Scala import statement.
      //
      // For instance: If we import a package using SparkConf:
      // "spark.jars.packages": "com.databricks:spark-csv_2.10:1.4.0"
      // then "import com.databricks.spark.csv._" in the interpreter, it will throw an error.
      //
      // Adding them to the interpreter manually to fix this issue.
      var classLoader = Thread.currentThread().getContextClassLoader
      while (classLoader != null) {
        if (classLoader.getClass.getCanonicalName == "org.apache.spark.util.MutableURLClassLoader")
        {
          val extraJarPath = classLoader.asInstanceOf[URLClassLoader].getURLs()
            // Check if the file exists. Otherwise an exception will be thrown.
            .filter { u => u.getProtocol == "file" && new File(u.getPath).isFile }
            // Livy rsc and repl are also in the extra jars list. Filter them out.
            .filterNot { u => Paths.get(u.toURI).getFileName.toString.startsWith("livy-") }

          extraJarPath.foreach { p => debug(s"Adding $p to Scala interpreter's class path...") }
          sparkIMain.addUrlsToClassPath(extraJarPath: _*)
          classLoader = null
        } else {
          classLoader = classLoader.getParent
        }
      }

      createSparkContext(conf)
    }

    sparkContext
  }

  protected def bind(name: String, tpe: String, value: Object, modifier: List[String]): Unit = {
    sparkIMain.beQuietDuring {
      sparkIMain.bind(name, tpe, value, modifier)
    }
  }

  override def close(): Unit = synchronized {
    if (sparkContext != null) {
      sparkContext.stop()
    }

    if (sparkIMain != null) {
      sparkIMain.close()
      sparkIMain = null
    }
  }

  override protected def isStarted(): Boolean = {
    sparkContext != null && sparkIMain != null
  }

  override protected def interpret(code: String): Result = {
    sparkIMain.interpret(code)
  }

  override protected def valueOfTerm(name: String): Option[Any] = {
    sparkIMain.valueOfTerm(name)
  }
}
