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
import com.cloudera.livy.LivyConf._
import com.cloudera.livy.server.LivyServer

class LivySparkUtilsSuite extends FunSuite with Matchers with LivyBaseUnitTestSuite {

  import LivySparkUtils._

  private val livyConf = new LivyConf()
  private val sparkEnv = SparkEnvironment.createSparkEnv(livyConf, "default")

  private val livyConf210 = new LivyConf()
  livyConf210.set("livy.spark.scalaVersion", "2.10.6")
  private val sparkEnv210 = SparkEnvironment.createSparkEnv(livyConf210, "default")

  private val livyConf211 = new LivyConf()
  livyConf211.set("livy.spark.scalaVersion", "2.11.1")
  private val sparkEnv211 = SparkEnvironment.createSparkEnv(livyConf211, "default")

  test("check for SPARK_HOME") {
    testSparkHome(sparkEnv)
  }

  test("check spark-submit version") {
    testSparkSubmit(sparkEnv)
  }

  test("should support Spark 1.6") {
    testSparkVersion("1.6.0")
    testSparkVersion("1.6.1")
    testSparkVersion("1.6.1-SNAPSHOT")
    testSparkVersion("1.6.2")
    testSparkVersion("1.6")
    testSparkVersion("1.6.3.2.5.0-12")
  }

  test("should support Spark 2.0.x") {
    testSparkVersion("2.0.0")
    testSparkVersion("2.0.1")
    testSparkVersion("2.0.2")
    testSparkVersion("2.0.3-SNAPSHOT")
    testSparkVersion("2.0.0.2.5.1.0-56") // LIVY-229
    testSparkVersion("2.0")
    testSparkVersion("2.1.0")
    testSparkVersion("2.1.1")
  }

  test("should not support Spark older than 1.6") {
    intercept[IllegalArgumentException] { testSparkVersion("1.4.0") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.0") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.1") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.2") }
    intercept[IllegalArgumentException] { testSparkVersion("1.5.0-cdh5.6.1") }
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

  test("formatScalaVersion() should format Scala version") {
    formatScalaVersion("2.10.8") shouldBe "2.10"
    formatScalaVersion("2.11.4") shouldBe "2.11"
    formatScalaVersion("2.10") shouldBe "2.10"
    formatScalaVersion("2.10.x.x.x.x") shouldBe "2.10"

    // Throw exception for bad Scala version.
    intercept[IllegalArgumentException] { formatScalaVersion("") }
    intercept[IllegalArgumentException] { formatScalaVersion("xxx") }
  }

  test("defaultSparkScalaVersion() should return default Scala version") {
    defaultSparkScalaVersion(formatSparkVersion("1.6.0")) shouldBe "2.10"
    defaultSparkScalaVersion(formatSparkVersion("1.6.1")) shouldBe "2.10"
    defaultSparkScalaVersion(formatSparkVersion("1.6.2")) shouldBe "2.10"
    defaultSparkScalaVersion(formatSparkVersion("2.0.0")) shouldBe "2.11"
    defaultSparkScalaVersion(formatSparkVersion("2.0.1")) shouldBe "2.11"

    // Throw exception for unsupported Spark version.
    intercept[IllegalArgumentException] { defaultSparkScalaVersion(formatSparkVersion("1.5.0")) }
  }

  test("sparkScalaVersion() should use spark-submit detected Scala version.") {
    sparkScalaVersion(formatSparkVersion("2.0.1"), Some("2.10"), sparkEnv) shouldBe "2.10"
    sparkScalaVersion(formatSparkVersion("1.6.0"), Some("2.11"), sparkEnv) shouldBe "2.11"
  }

  test("sparkScalaVersion() should throw if configured and detected Scala version mismatch.") {
    intercept[IllegalArgumentException] {
      sparkScalaVersion(formatSparkVersion("2.0.1"), Some("2.11"), sparkEnv210)
    }
    intercept[IllegalArgumentException] {
      sparkScalaVersion(formatSparkVersion("1.6.1"), Some("2.10"), sparkEnv211)
    }
  }

  test("sparkScalaVersion() should use configured Scala version if spark-submit doesn't tell.") {
    sparkScalaVersion(formatSparkVersion("1.6.0"), None, sparkEnv210) shouldBe "2.10"
    sparkScalaVersion(formatSparkVersion("1.6.2"), None, sparkEnv210) shouldBe "2.10"
    sparkScalaVersion(formatSparkVersion("2.0.0"), None, sparkEnv210) shouldBe "2.10"
    sparkScalaVersion(formatSparkVersion("2.0.1"), None, sparkEnv210) shouldBe "2.10"
    sparkScalaVersion(formatSparkVersion("1.6.0"), None, sparkEnv211) shouldBe "2.11"
    sparkScalaVersion(formatSparkVersion("1.6.2"), None, sparkEnv211) shouldBe "2.11"
    sparkScalaVersion(formatSparkVersion("2.0.0"), None, sparkEnv211) shouldBe "2.11"
    sparkScalaVersion(formatSparkVersion("2.0.1"), None, sparkEnv211) shouldBe "2.11"
  }

  test("sparkScalaVersion() should use default Spark Scala version.") {
<<<<<<< 2abb8a3d2850c506ffd2b8a210813f1b8353045f
    sparkScalaVersion(formatSparkVersion("1.6.0"), None, livyConf) shouldBe "2.10"
    sparkScalaVersion(formatSparkVersion("1.6.2"), None, livyConf) shouldBe "2.10"
    sparkScalaVersion(formatSparkVersion("2.0.0"), None, livyConf) shouldBe "2.11"
    sparkScalaVersion(formatSparkVersion("2.0.1"), None, livyConf) shouldBe "2.11"
    sparkScalaVersion(formatSparkVersion("2.1.0"), None, livyConf) shouldBe "2.11"
=======
    sparkScalaVersion(formatSparkVersion("1.6.0"), None, sparkEnv) shouldBe "2.10"
    sparkScalaVersion(formatSparkVersion("1.6.2"), None, sparkEnv) shouldBe "2.10"
    sparkScalaVersion(formatSparkVersion("2.0.0"), None, sparkEnv) shouldBe "2.11"
    sparkScalaVersion(formatSparkVersion("2.0.1"), None, sparkEnv) shouldBe "2.11"
>>>>>>> Add SparkEnvironment
  }
}
