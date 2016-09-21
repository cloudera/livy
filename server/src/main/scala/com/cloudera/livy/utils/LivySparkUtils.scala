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

import scala.math.Ordering.Implicits._

import com.cloudera.livy.LivyConf
import com.cloudera.livy.util.LineBufferedProcess

object LivySparkUtils {

  /**
   * Sets the spark-submit path if it's not configured in the LivyConf
   */
  def testSparkHome(livyConf: LivyConf): Unit = {
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
   def testSparkSubmit(livyConf: LivyConf): Unit = {
    try {
      testSparkVersion(sparkSubmitVersion(livyConf))
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
    // This is exclusive. Version which equals to this will be rejected.
    val maxVersion = (2, 1)
    val minVersion = (1, 6)

    val supportedVersion = formatSparkVersion(version) match {
      case v: (Int, Int) =>
        v >= minVersion && v < maxVersion
      case _ => false
    }
    require(supportedVersion, s"Unsupported Spark version $version.")
  }

  /**
   * Return the version of the configured `spark-submit` version.
   *
   * @param livyConf
   * @return the version
   */
  def sparkSubmitVersion(livyConf: LivyConf): String = {
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

  /**
    * Return formated Spark version.
    * @param version Spark version
    * @return Two element tuple, one is major version and the other is minor version
    */
  def formatSparkVersion(version: String): (Int, Int) = {
    val versionPattern = """(\d)+\.(\d)+(?:\.\d*)?""".r
    version match {
      case versionPattern(major, minor) =>
        (major.toInt, minor.toInt)
      case _ =>
        throw new IllegalArgumentException(s"Fail to parse Spark version from $version")
    }
  }
}
