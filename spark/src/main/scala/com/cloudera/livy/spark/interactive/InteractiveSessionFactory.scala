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

package com.cloudera.livy.spark.interactive

import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.nio.file.{Files, Paths}

import com.cloudera.livy.LivyConf
import com.cloudera.livy.sessions.{PySpark, SessionFactory}
import com.cloudera.livy.sessions.interactive.InteractiveSession
import com.cloudera.livy.spark.{SparkProcess, SparkProcessBuilder, SparkProcessBuilderFactory}
import com.cloudera.livy.spark.SparkProcessBuilder.{AbsolutePath, RelativePath}

object InteractiveSessionFactory {
  val LivyReplDriverClassPath = "livy.repl.driverClassPath"
  val LivyReplJars = "livy.repl.jars"
  val LivyServerUrl = "livy.server.serverUrl"
  val SparkDriverExtraJavaOptions = "spark.driver.extraJavaOptions"
  val SparkLivyCallbackUrl = "spark.livy.callbackUrl"
  val SparkLivyPort = "spark.livy.port"
  val SparkSubmitPyFiles = "spark.submit.pyFiles"
  val SparkYarnIsPython = "spark.yarn.isPython"
}

abstract class InteractiveSessionFactory(processFactory: SparkProcessBuilderFactory)
  extends SessionFactory[InteractiveSession, CreateInteractiveRequest] {

  import InteractiveSessionFactory._

  override def create(
      id: Int,
      owner: String,
      request: CreateInteractiveRequest): InteractiveSession = {
    require(request.kind != null, "Session kind is required.")
    val builder = sparkBuilder(id, request)
    val kind = request.kind.toString
    val process = builder.start(None, List(kind))

    create(id, owner, process, request)
  }

  protected def create(
      id: Int,
      owner: String,
      process: SparkProcess,
      request: CreateInteractiveRequest): InteractiveSession

  protected def sparkBuilder(id: Int, request: CreateInteractiveRequest): SparkProcessBuilder = {
    val builder = processFactory.builder()

    builder.className("com.cloudera.livy.repl.Main")
    builder.conf(request.conf)
    request.archives.map(RelativePath).foreach(builder.archive)
    request.driverCores.foreach(builder.driverCores)
    request.driverMemory.foreach(builder.driverMemory)
    request.executorCores.foreach(builder.executorCores)
    request.executorMemory.foreach(builder.executorMemory)
    request.numExecutors.foreach(builder.numExecutors)
    request.files.map(RelativePath).foreach(builder.file)

    val jars = request.jars.map(RelativePath) ++ livyJars(processFactory.livyConf).map(RelativePath)
    jars.foreach(builder.jar)

    request.proxyUser.foreach(builder.proxyUser)
    request.queue.foreach(builder.queue)
    request.name.foreach(builder.name)

    request.kind match {
      case PySpark() =>
        builder.conf(SparkYarnIsPython, "true", admin = true)

        // FIXME: Spark-1.4 seems to require us to manually upload the PySpark support files.
        // We should only do this for Spark 1.4.x
        val pySparkFiles = findPySparkArchives()
        builder.files(pySparkFiles.map(AbsolutePath))

        // We can't actually use `builder.pyFiles`, because livy-repl is a Jar, and
        // spark-submit will reject it because it isn't a Python file. Instead we'll pass it
        // through a special property that the livy-repl will use to expose these libraries in
        // the Python shell.
        builder.files(request.pyFiles.map(RelativePath))

        builder.conf(SparkSubmitPyFiles, (pySparkFiles ++ request.pyFiles).mkString(","),
          admin = true)
      case _ =>
    }

    sys.env.get("LIVY_REPL_JAVA_OPTS").foreach { replJavaOpts =>
      val javaOpts = builder.conf(SparkDriverExtraJavaOptions) match {
        case Some(javaOptions) => f"$javaOptions $replJavaOpts"
        case None => replJavaOpts
      }
      builder.conf(SparkDriverExtraJavaOptions, javaOpts, admin = true)
    }

    Option(processFactory.livyConf.get(LivyReplDriverClassPath))
      .foreach(builder.driverClassPath)

    sys.props.get(LivyServerUrl).foreach { serverUrl =>
      val callbackUrl = f"$serverUrl/sessions/$id/callback"
      builder.conf(SparkLivyCallbackUrl, callbackUrl, admin = true)
    }

    builder.conf(SparkLivyPort, "0", admin = true)

    builder.redirectOutput(Redirect.PIPE)
    builder.redirectErrorStream(true)

    builder
  }

  private def livyJars(livyConf: LivyConf): Seq[String] = {
    Option(livyConf.get(LivyReplJars)).map(_.split(",").toSeq).getOrElse {
      val home = sys.env("LIVY_HOME")
      val jars = Option(new File(home, "repl-jars"))
        .filter(_.isDirectory())
        .getOrElse(new File(home, "repl/target/jars"))
      require(jars.isDirectory(), "Cannot find Livy REPL jars.")
      jars.listFiles().map(_.getAbsolutePath()).toSeq
    }
  }

  private def findPySparkArchives(): Seq[String] = {
    sys.env.get("PYSPARK_ARCHIVES_PATH")
      .map(_.split(",").toSeq)
      .getOrElse {
        sys.env.get("SPARK_HOME") .map { case sparkHome =>
          val pyLibPath = Seq(sparkHome, "python", "lib").mkString(File.separator)
          val pyArchivesFile = new File(pyLibPath, "pyspark.zip")
          require(pyArchivesFile.exists(),
            "pyspark.zip not found; cannot run pyspark application in YARN mode.")

          val py4jFile = Files.newDirectoryStream(Paths.get(pyLibPath), "py4j-*-src.zip")
            .iterator()
            .next()
            .toFile

          require(py4jFile.exists(),
            "py4j-*-src.zip not found; cannot run pyspark application in YARN mode.")
          Seq(pyArchivesFile.getAbsolutePath, py4jFile.getAbsolutePath)
        }.getOrElse(Seq())
      }
  }
}
