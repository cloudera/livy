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

import org.scalatest.FunSuite

import com.cloudera.livy.{LivyBaseUnitTestSuite, LivyConf}

class LivyServerSuite extends FunSuite with LivyBaseUnitTestSuite {

  private val livyConf = new LivyConf()

  test("check for SPARK_HOME") {
    new LivyServer().testSparkHome(livyConf)
  }

  test("check spark-submit version") {
    new LivyServer().testSparkSubmit(livyConf)
  }

  test("should support Spark 1.6") {
    val s = new LivyServer()
    s.testSparkVersion("1.6.0")
    s.testSparkVersion("1.6.1")
    s.testSparkVersion("1.6.2")
  }

  test("should not support Spark older than 1.6") {
    val s = new LivyServer()
    intercept[IllegalArgumentException] { s.testSparkVersion("1.4.0") }
    intercept[IllegalArgumentException] { s.testSparkVersion("1.5.0") }
    intercept[IllegalArgumentException] { s.testSparkVersion("1.5.1") }
    intercept[IllegalArgumentException] { s.testSparkVersion("1.5.2") }
  }

  test("should not support Spark 2.0+") {
    val s = new LivyServer()
    intercept[IllegalArgumentException] { s.testSparkVersion("2.0.0") }
    intercept[IllegalArgumentException] { s.testSparkVersion("2.0.1") }
    intercept[IllegalArgumentException] { s.testSparkVersion("2.1.0") }
  }
}
