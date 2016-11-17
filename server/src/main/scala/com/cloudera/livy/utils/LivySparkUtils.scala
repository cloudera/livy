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

import java.io.{File, IOException}

import scala.collection.SortedMap
import scala.math.Ordering.Implicits._

import com.cloudera.livy.{LivyConf, Logging}
import com.cloudera.livy.util.LineBufferedProcess

object LivySparkUtils extends Logging {

  // For each Spark version we supported, we need to add this mapping relation in case Scala
  // version cannot be detected from "spark-submit --version".
  private val _defaultSparkScalaVersion = SortedMap(
    // Spark 2.1 + Scala 2.11
    (2, 1) -> "2.11",
    // Spark 2.0 + Scala 2.11
    (2, 0) -> "2.11",
    // Spark 1.6 + Scala 2.10
    (1, 6) -> "2.10"
  )

  // Supported Spark version
  private val MIN_VERSION = (1, 6)
  private val MAX_VERSION = (2, 2)

  private val sparkVersionRegex = """version (.*)""".r.unanchored
  private val scalaVersionRegex = """Scala version (.*), Java""".r.unanchored

  /**
   * Test that Spark home is configured and configured Spark home is a directory.
   */
  def testSparkHome(sparkEnv: SparkEnvironment): Unit = {
    require(new File(sparkEnv.sparkHome()).isDirectory(), "SPARK_HOME path does not exist")
  }

  /**
   * Test that the configured `spark-submit` executable exists.
   *
   * @param sparkEnv
   */
   def testSparkSubmit(sparkEnv: SparkEnvironment): Unit = {
    try {
      testSparkVersion(sparkSubmitVersion(sparkEnv)._1)
    } catch {
      case e: IOException =>
        throw new IOException("Failed to run spark-submit executable", e)
    }
  }

  /**
   * Throw an exception if Spark version is not supported.
   * @param version Spark version
   */
  def testSparkVersion(version: String): Unit = {
    val v = formatSparkVersion(version)
    require(v >= MIN_VERSION, s"Unsupported Spark version $v")
    if (v >= MAX_VERSION) {
      warn(s"Current Spark $v is not verified in Livy, please use it carefully")
    }
  }

  /**
   * Call `spark-submit --version` and parse its output for Spark and Scala version.
   *
   * @param sparkEnv
   * @return Tuple with Spark and Scala version
   */
  def sparkSubmitVersion(sparkEnv: SparkEnvironment): (String, Option[String]) = {
    val sparkSubmit = sparkEnv.sparkSubmit()
    val pb = new ProcessBuilder(sparkSubmit, "--version")
    pb.redirectErrorStream(true)
    pb.redirectInput(ProcessBuilder.Redirect.PIPE)

    if (LivyConf.TEST_MODE) {
      pb.environment().put("LIVY_TEST_CLASSPATH", sys.props("java.class.path"))
    }

    val process = new LineBufferedProcess(pb.start())
    val exitCode = process.waitFor()
    val output = process.inputIterator.mkString("\n")

    var sparkVersion = ""
    output match {
      case sparkVersionRegex(version) => sparkVersion = version
      case _ =>
        throw new IOException(f"Unable to determine spark-submit version [$exitCode]:\n$output")
    }

    val scalaVersion = output match {
      case scalaVersionRegex(version) if version.nonEmpty => Some(formatScalaVersion(version))
      case _ => None
    }

    (sparkVersion, scalaVersion)
  }

  def sparkScalaVersion(
      formattedSparkVersion: (Int, Int),
      scalaVersionFromSparkSubmit: Option[String],
      sparkEnv: SparkEnvironment): String = {
    val scalaVersionInLivyConf = Option(sparkEnv.get(SparkEnvironment.LIVY_SPARK_SCALA_VERSION))
      .filter(_.nonEmpty)
      .map(formatScalaVersion)

    for (vSparkSubmit <- scalaVersionFromSparkSubmit; vLivyConf <- scalaVersionInLivyConf) {
      require(vSparkSubmit == vLivyConf,
        s"Scala version detected from spark-submit ($vSparkSubmit) does not match " +
          s"Scala version configured in livy.conf ($vLivyConf)")
    }

    scalaVersionInLivyConf
      .orElse(scalaVersionFromSparkSubmit)
      .getOrElse(defaultSparkScalaVersion(formattedSparkVersion))
  }

  /**
   * Return formatted Spark version.
   *
   * @param version Spark version
   * @return Two element tuple, one is major version and the other is minor version
   */
  def formatSparkVersion(version: String): (Int, Int) = {
    val versionPattern = """^(\d+)\.(\d+)(\..*)?$""".r
    versionPattern.findFirstMatchIn(version) match {
      case Some(m) =>
        (m.group(1).toInt, m.group(2).toInt)
      case None =>
        throw new IllegalArgumentException(s"Fail to parse Spark version from $version")
    }
  }

  /**
   * Return Scala binary version.
   * It strips the patch version if specified.
   * Throws if it cannot parse the version.
   *
   * @param scalaVersion Scala binary version String
   * @return Scala binary version
   */
  def formatScalaVersion(scalaVersion: String): String = {
    val versionPattern = """(\d)+\.(\d+)+.*""".r
    scalaVersion match {
      case versionPattern(major, minor) => s"$major.$minor"
      case _ => throw new IllegalArgumentException(s"Unrecognized Scala version: $scalaVersion")
    }
  }

  /**
   * Return the default Scala version of a Spark version.
   *
   * @param sparkVersion formatted Spark version.
   * @return Scala binary version
   */
  private[utils] def defaultSparkScalaVersion(sparkVersion: (Int, Int)): String = {
    _defaultSparkScalaVersion.get(sparkVersion)
      .orElse {
        if (sparkVersion < _defaultSparkScalaVersion.head._1) {
          throw new IllegalArgumentException(s"Spark version $sparkVersion is less than the " +
            s"minimum version ${_defaultSparkScalaVersion.head._1} supported by Livy")
        } else if (sparkVersion > _defaultSparkScalaVersion.last._1) {
          val (spark, scala) = _defaultSparkScalaVersion.last
          warn(s"Spark version $sparkVersion is greater then the maximum version " +
            s"$spark supported by Livy, will choose Scala version $scala instead, " +
            s"please specify manually if it is the expected Scala version you want")
          Some(scala)
        } else {
          None
        }
      }
      .getOrElse(
        throw new IllegalArgumentException(s"Fail to get Scala version from Spark $sparkVersion"))
  }
}
