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

import org.scalatest.FunSuite
import org.scalatest.Matchers

import com.cloudera.livy.{LivyBaseUnitTestSuite, LivyConf}
import com.cloudera.livy.server.LivyServer

class LivySparkUtilsSuite extends FunSuite with Matchers with LivyBaseUnitTestSuite {

  import LivySparkUtils._

  private val livyConf = new LivyConf()

  test("check for SPARK_HOME") {
    testSparkHome(livyConf)
  }

  test("check spark-submit version") {
    testSparkSubmit(livyConf)
  }

  test("should support Spark 1.6") {
    testSparkVersion("1.6.0")
    testSparkVersion("1.6.1")
    testSparkVersion("1.6.2")
    testSparkVersion("1.6")
  }

  test("should support Spark 2.0.x") {
    testSparkVersion("2.0.0")
    testSparkVersion("2.0.1")
    testSparkVersion("2.0.2")
    testSparkVersion("2.0.0.2.5.1.0-56") // LIVY-229
    testSparkVersion("2.0")
  }

  test("should not support Spark older than 1.6 or newer than 2.0") {
    intercept[IllegalArgumentException] { testSparkVersion("1.4.0") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.0") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.1") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.2") }

    intercept[IllegalArgumentException] { testSparkVersion("2.1.0") }
    intercept[IllegalArgumentException] { testSparkVersion("2.1.2") }
    intercept[IllegalArgumentException] { testSparkVersion("2.2.1") }
  }

  test("should fail on bad version") {
    intercept[IllegalArgumentException] { testSparkVersion("not a version") }
  }

  test("should error out if recovery is turned on but master isn't yarn") {
    val livyConf = new LivyConf()
    livyConf.set(LivyConf.LIVY_SPARK_MASTER, "local")
    livyConf.set(LivyConf.RECOVERY_MODE, "recovery")
    val s = new LivyServer()
    intercept[IllegalArgumentException] { s.testRecovery(livyConf) }
  }

  test("get correct Scala version") {
    formatScalaVersion("2.10.8", formatSparkVersion("2.0.0")) should be ("2.10")
    formatScalaVersion("2.11.4", formatSparkVersion("1.6.0")) should be ("2.11")
    formatScalaVersion("2.10", formatSparkVersion("2.0.0")) should be ("2.10")
    formatScalaVersion("2.10.x.x.x.x", formatSparkVersion("2.0.0")) should be ("2.10")

    // Will pick default Spark Scala version if the input Scala version string is not correct.
    formatScalaVersion("", formatSparkVersion("2.0.0")) should be ("2.11")
    formatScalaVersion("xxx", formatSparkVersion("1.6.1")) should be ("2.10")

    // Throw exception for unsupported Spark version.
    intercept[IllegalArgumentException] {
      formatScalaVersion("xxx", formatSparkVersion("1.5.0"))
    }
  }
}
