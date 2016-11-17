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

package com.cloudera.livy.utils

import java.io.File
import java.lang.{Boolean => JBoolean}
import java.nio.file.{Files, Paths}

import scala.collection.mutable
import scala.collection.JavaConverters._

import com.google.common.annotations.VisibleForTesting

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.client.common.ClientConf
import com.cloudera.livy.client.common.ClientConf.ConfEntry

object SparkEnvironment {

  case class Entry(override val key: String, override val dflt: AnyRef) extends ConfEntry

  object Entry {
    def apply(key: String, dflt: Boolean): Entry = Entry(key, dflt: JBoolean)
  }

  val DEFAULT_ENV_NAME = "default"

  val SPARK_HOME = Entry("livy.server.spark-home", null)
  val SPARK_CONF_DIR = Entry("livy.server.spark-conf-dir", null)

  // Two configurations to specify Spark and related Scala version. These are internal
  // configurations will be set by LivyServer and used in session creation. It is not required to
  // set usually unless running with unofficial Spark + Scala versions
  // (like Spark 2.0 + Scala 2.10, Spark 1.6 + Scala 2.11)
  val LIVY_SPARK_SCALA_VERSION = Entry("livy.spark.scalaVersion", null)

  val ENABLE_HIVE_CONTEXT = Entry("livy.repl.enableHiveContext", false)

  val SPARKR_PACKAGE = Entry("livy.sparkr.package", null)
  val PYSPARK_ARCHIVES = Entry("livy.pyspark.archives", null)

  val SPARK_MASTER = "spark.master"
  val SPARK_DEPLOY_MODE = "spark.submit.deployMode"
  val SPARK_JARS = "spark.jars"
  val SPARK_FILES = "spark.files"
  val SPARK_ARCHIVES = "spark.yarn.dist.archives"
  val SPARK_PY_FILES = "spark.submit.pyFiles"
  val SPARK_YARN_IS_PYTHON = "spark.yarn.isPython"

  val SPARK_ENABLE_HIVE_CONTEXT = "spark.repl.enableHiveContext"
  val SPARK2_ENABLE_HIVE_CONTEXT = "spark.sql.catalogImplementation"

  // Spark major version passed from server to driver, used for interpreter to load right object.
  val SPARK_MAJOR_VERSION = "spark.livy.spark_major_version"

  val HARDCODED_SPARK_FILE_LISTS = Seq(
    SPARK_JARS,
    SPARK_FILES,
    SPARK_ARCHIVES,
    SPARK_PY_FILES,
    "spark.yarn.archive",
    "spark.yarn.dist.files",
    "spark.yarn.dist.jars",
    "spark.yarn.jar",
    "spark.yarn.jars"
  )

  @VisibleForTesting
  private[utils] val sparkEnvironments = new mutable.HashMap[String, SparkEnvironment]

  def getSparkEnv(livyConf: LivyConf, env: String): SparkEnvironment = {
    if (sparkEnvironments.contains(env)) {
      sparkEnvironments(env)
    } else {
      synchronized {
        if (sparkEnvironments.contains(env)) {
          sparkEnvironments(env)
        } else {
          val sparkEnv = createSparkEnv(livyConf, env)
          sparkEnv.environmentCheck(livyConf)
          sparkEnvironments(env) = sparkEnv
          sparkEnv
        }
      }
    }
  }

  @VisibleForTesting
  private[utils] def createSparkEnv(livyConf: LivyConf, env: String): SparkEnvironment = {
    val livySparkConfKeys = getClass.getMethods.filter {
      _.getReturnType.getCanonicalName == classOf[Entry].getCanonicalName
    }.map(_.invoke(this).asInstanceOf[Entry].key).toSet

    val sparkEnv = new SparkEnvironment(env)
    if (env == DEFAULT_ENV_NAME) {
      livyConf.asScala
        .filter { kv =>
          livySparkConfKeys.contains(kv.getKey) ||
            livySparkConfKeys.contains(kv.getKey.stripPrefix(s"$DEFAULT_ENV_NAME."))
        }
        .foreach(kv => sparkEnv.set(kv.getKey.stripPrefix(s"$DEFAULT_ENV_NAME."), kv.getValue))
    } else {
      livyConf.asScala
        .filter(kv => livySparkConfKeys.contains(kv.getKey.stripPrefix(s"$env.")))
        .foreach(kv => sparkEnv.set(kv.getKey.stripPrefix(s"$env."), kv.getValue))
    }

    sparkEnv
  }
}

/**
 * A isolated Spark environment used for isolating Spark related configurations, libraries.
 * Livy Can have multiple Spark environments differentiated by name, for example if user
 * configured in Livy conf like:
 *
 * test.livy.server.spark-home = xxx
 * test.livy.server.spark-conf-dir = xxx
 *
 * production.livy.server.spark-home = yyy
 * production.livy.server.spark-conf-dir = yyy
 *
 * Livy internally will have two isolated Spark environments "test" and "production". When user
 * create batch or interactive session, they could specify through "sparkEnv" in json body. Livy
 * server will honor this env name and pick right Spark environment. This is used for Livy to
 * support different Spark cluster in runtime.
 *
 * The Default Spark environment is "default" if user configured
 *
 * livy.server.spark-home = xxx
 * or:
 * default.livy.server.spark-home = xxx
 *
 * Livy server will treat configuration without prefix to "default" Spark environment to keep
 * backward compatibility.
 *
 * Also for environment variable, user's configuration
 *
 * SPARK_HOME or DEFAULT_SPARK_HOME will be treated as "default" Spark environment.
 * TEST_SPARK_HOME or TEST_SPARK_CONF_DIR will be allocated to "test" Spark environment.
 */
class SparkEnvironment private(name: String)
  extends ClientConf[SparkEnvironment](null) with Logging {

  import SparkEnvironment._

  private var _sparkVersion: (Int, Int) = _
  private var _scalaVersion: String = _

  /**
   * Return the location of the spark home directory. It will check livy conf as well as
   * environment variable. For "default" Spark environment, it will check SPARK_HOME or
   * DEFAULT_SPARK_HOME. For other Spark environment, it will check ${NAME}_SPARK_HOME.
   */
  def sparkHome(): String = Option(get(SPARK_HOME))
    .orElse {
      if (name == DEFAULT_ENV_NAME) {
        sys.env.get("SPARK_HOME").orElse(sys.env.get(DEFAULT_ENV_NAME.toUpperCase + "_SPARK_HOME"))
      } else {
        sys.env.get(name.toUpperCase + "_SPARK_HOME")
      }
    }.getOrElse(throw new IllegalStateException(s"SPARK_HOME is not configured"))

  /**
   * Return the location of Spark conf directory. It will check livy conf
   * "livy.server.spark-conf-dir" as well as environment variable. From "default" Spark
   * environment, it will check SPARK_CONF_DIR or DEFAULT_SPARK_CONF_DIR. For other Spark
   * environment, it will check ${NAME}_SPARK_CONF_DIR.
   */
  def sparkConfDir(): String = Option(get(SPARK_CONF_DIR))
    .orElse(
      if (name == DEFAULT_ENV_NAME) {
        sys.env.get("SPARK_CONF_DIR")
          .orElse(sys.env.get(DEFAULT_ENV_NAME.toUpperCase + "_SPARK_CONF_DIR"))
      } else {
        sys.env.get(name.toUpperCase + "_SPARK_CONF_DIR")
      }
    ).getOrElse(sparkHome() + File.separator + "conf")

  /** Return the path to the spark-submit executable. */
  def sparkSubmit(): String = {
    sparkHome() + File.separator + "bin" + File.separator + "spark-submit"
  }

  def sparkVersion(): (Int, Int) = {
    require(_sparkVersion != null)
    _sparkVersion
  }

  def scalaVersion(): String = {
    require(_scalaVersion != null)
    _scalaVersion
  }

  def environmentCheck(livyConf: LivyConf): Unit = {
    // Make sure the `spark-submit` program exists, otherwise much of livy won't work.
    LivySparkUtils.testSparkHome(this)

    // Test spark-submit and get Spark Scala version accordingly.
    val (sparkVersionFromSparkSubmit, scalaVersionFromSparkSubmit) =
    LivySparkUtils.sparkSubmitVersion(this)

    LivySparkUtils.testSparkVersion(sparkVersionFromSparkSubmit)

    _sparkVersion = LivySparkUtils.formatSparkVersion(sparkVersionFromSparkSubmit)
    _scalaVersion =
      LivySparkUtils.sparkScalaVersion(_sparkVersion, scalaVersionFromSparkSubmit, this)
  }


  def findSparkRArchive(): String = {
    Option(get(SPARKR_PACKAGE)).getOrElse {
      val path = Seq(sparkHome, "R", "lib", "sparkr.zip").mkString(File.separator)
      val rArchivesFile = new File(path)
      require(rArchivesFile.exists(), "sparkr.zip not found; cannot run sparkr application.")
      rArchivesFile.getAbsolutePath()
    }
  }

  def datanucleusJars(): Seq[String] = {
    if (sys.env.getOrElse("LIVY_INTEGRATION_TEST", "false").toBoolean) {
      // datanucleus jars has already been in classpath in integration test
      Seq.empty
    } else {
      val major = sparkVersion()._1
      val libdir = major match {
        case 1 =>
          if (new File(sparkHome, "RELEASE").isFile) {
            new File(sparkHome, "lib")
          } else {
            new File(sparkHome, "lib_managed/jars")
          }
        case 2 =>
          if (new File(sparkHome, "RELEASE").isFile) {
            new File(sparkHome, "jars")
          } else if (new File(sparkHome, "assembly/target/scala-2.11/jars").isDirectory) {
            new File(sparkHome, "assembly/target/scala-2.11/jars")
          } else {
            new File(sparkHome, "assembly/target/scala-2.10/jars")
          }
        case v =>
          throw new IllegalStateException(s"Unsupported spark major version: $major")
      }
      val jars = if (!libdir.isDirectory) {
        Seq.empty[String]
      } else {
        libdir.listFiles().filter(_.getName.startsWith("datanucleus-"))
          .map(_.getAbsolutePath).toSeq
      }
      if (jars.isEmpty) {
        warn("datanucleus jars can not be found")
      }
      jars
    }
  }

  def findPySparkArchives(): Seq[String] = {
    Option(get(PYSPARK_ARCHIVES))
      .map(_.split(",").toSeq)
      .getOrElse {
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
      }
  }
}
