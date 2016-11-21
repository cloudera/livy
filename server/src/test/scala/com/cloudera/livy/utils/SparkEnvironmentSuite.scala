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

import com.cloudera.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SparkEnvironmentSuite extends FunSuite
  with Matchers with BeforeAndAfterAll with LivyBaseUnitTestSuite {
  import SparkEnvironment._

  override def afterAll(): Unit = {
    // clean the global data when this test suite is finished.
    sparkEnvironments.clear()
    super.afterAll()
  }

  test("default Spark environment") {
    val livyConf = new LivyConf(false)
      .set("livy.server.spark-home", "test-home")
      .set("livy.server.spark-conf-dir", "test-conf-dir")
      .set("livy.sparkr.package", "test-sparkr-package")
      .set("livy.pyspark.archives", "test-pyspark-archives")
    val sparkEnv = createSparkEnv(livyConf, "default")

    sparkEnv.get(SPARK_HOME) should be ("test-home")
    sparkEnv.get(SPARK_CONF_DIR) should be ("test-conf-dir")
    sparkEnv.get(PYSPARK_ARCHIVES) should be ("test-pyspark-archives")
    sparkEnv.get(SPARKR_PACKAGE) should be ("test-sparkr-package")

    sparkEnv.sparkHome() should be ("test-home")
    sparkEnv.sparkConfDir() should be ("test-conf-dir")
  }

  test("default Spark environment with environment specified") {
    val livyConf = new LivyConf(false)
      .set(SPARK_ENV_PREFIX + ".default." + SPARK_HOME.key, "test-default-home")
      .set(SPARK_ENV_PREFIX + ".default." + SPARK_CONF_DIR.key, "test-default-conf-dir")
    val sparkEnv = createSparkEnv(livyConf, "default")

    sparkEnv.get(SPARK_HOME) should be ("test-default-home")
    sparkEnv.get(SPARK_CONF_DIR) should be ("test-default-conf-dir")
    sparkEnv.get(PYSPARK_ARCHIVES) should be (null)
    sparkEnv.get(SPARKR_PACKAGE) should be (null)

    sparkEnv.sparkHome() should be ("test-default-home")
    sparkEnv.sparkConfDir() should be ("test-default-conf-dir")
  }

  test("default Spark environment using SPARK_HOME environment variable") {
    val livyConf = new LivyConf(false)
    val sparkEnv = createSparkEnv(livyConf, "default")

    sparkEnv.get(SPARK_HOME) should be (null)
    sparkEnv.get(SPARK_CONF_DIR) should be (null)
    sparkEnv.get(PYSPARK_ARCHIVES) should be (null)
    sparkEnv.get(SPARKR_PACKAGE) should be (null)

    sparkEnv.sparkHome() should not be (null)
    sparkEnv.sparkConfDir() should not be (null)
  }

  test("specify different Spark environments through configuration") {
    val livyConf = new LivyConf(false)
      .set(SPARK_ENV_PREFIX + ".test." + SPARK_HOME.key, "test-home")
      .set(SPARK_ENV_PREFIX + ".test." + PYSPARK_ARCHIVES.key, "test-home/python/pyspark.tgz")
      .set(SPARK_ENV_PREFIX + ".production." + SPARK_HOME.key, "production-home")
      .set(SPARK_ENV_PREFIX + ".production." + SPARKR_PACKAGE.key, "production-home/R/sparkr.zip")
      .set(SPARK_ENV_PREFIX + ".default." + SPARK_HOME.key, "default-home")
      .set(SPARK_ENV_PREFIX + ".default." + SPARK_CONF_DIR.key, "default-conf-dir")

    sparkEnvironments("test") = createSparkEnv(livyConf, "test")
    sparkEnvironments("production") = createSparkEnv(livyConf, "production")
    sparkEnvironments("default") = createSparkEnv(livyConf, "default")

    val testSparkEnv = getSparkEnv(livyConf, "test")
    testSparkEnv.sparkHome() should be ("test-home")
    testSparkEnv.sparkConfDir() should be ("test-home/conf")
    testSparkEnv.findPySparkArchives() should be (Seq("test-home/python/pyspark.tgz"))

    val prodSparkEnv = getSparkEnv(livyConf, "production")
    prodSparkEnv.sparkHome() should be ("production-home")
    prodSparkEnv.findSparkRArchive() should be ("production-home/R/sparkr.zip")
    prodSparkEnv.sparkConfDir() should be ("production-home/conf")

    val defaultSparkEnv = getSparkEnv(livyConf, "default")
    defaultSparkEnv.sparkHome() should be ("default-home")
    defaultSparkEnv.sparkConfDir() should be ("default-conf-dir")
  }

  test("create non-existed Spark environment") {
    val livyConf = new LivyConf(false)
    val sparkEnv = createSparkEnv(livyConf, "non-exist")

    sparkEnv.get(SPARK_HOME) should be (null)
    sparkEnv.get(SPARK_CONF_DIR) should be (null)
    sparkEnv.get(PYSPARK_ARCHIVES) should be (null)
    sparkEnv.get(SPARKR_PACKAGE) should be (null)

    intercept[Exception](sparkEnv.sparkHome())
    intercept[Exception](sparkEnv.sparkConfDir())
  }
}
