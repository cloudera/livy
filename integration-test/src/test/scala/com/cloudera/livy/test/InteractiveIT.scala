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

package com.cloudera.livy.test

import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import scala.language.postfixOps

import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.scalatest.OptionValues._

import com.cloudera.livy.rsc.RSCConf
import com.cloudera.livy.sessions._
import com.cloudera.livy.test.framework.{BaseIntegrationTestSuite, LivyRestClient}

class InteractiveIT extends BaseIntegrationTestSuite {
  test("basic interactive session") {
    withNewSession(Spark()) { s =>
      s.run("1+1").verifyResult("res0: Int = 2")
      s.run("sqlContext").verifyResult(startsWith("res1: org.apache.spark.sql.hive.HiveContext"))
      s.run("val sql = new org.apache.spark.sql.SQLContext(sc)").verifyResult(
        startsWith("sql: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext"))

      s.run("abcde").verifyError(evalue = ".*?:[0-9]+: error: not found: value abcde.*")
      s.run("throw new IllegalStateException()")
        .verifyError(evalue = ".*java\\.lang\\.IllegalStateException.*")

      // Make sure appInfo is reported correctly.
      val state = s.snapshot()
      state.appInfo.driverLogUrl.value should include ("containerlogs")
      state.appInfo.sparkUiUrl.value should startWith ("http")

      // Stop session and verify the YARN app state is finished.
      // This is important because if YARN app state is killed, Spark history is not archived.
      val appId = s.appId()
      s.stop()
      val appReport = cluster.yarnClient.getApplicationReport(appId)
      appReport.getYarnApplicationState() shouldEqual YarnApplicationState.FINISHED
    }
  }

  pytest("pyspark interactive session") {
    withNewSession(PySpark()) { s =>
      s.run("1+1").verifyResult("2")
      s.run("sqlContext").verifyResult(startsWith("<pyspark.sql.context.HiveContext"))
      s.run("sc.parallelize(range(100)).map(lambda x: x * 2).reduce(lambda x, y: x + y)")
        .verifyResult("9900")

      s.run("abcde").verifyError(ename = "NameError", evalue = "name 'abcde' is not defined")
      s.run("raise KeyError, 'foo'").verifyError(ename = "KeyError", evalue = "'foo'")
    }
  }

  rtest("R interactive session") {
    withNewSession(SparkR()) { s =>
      // R's output sometimes includes the count of statements, which makes it annoying to test
      // things. This helps a bit.
      val curr = new AtomicInteger()
      def count: Int = curr.incrementAndGet()

      s.run("1+1").verifyResult(startsWith(s"[$count] 2"))
      s.run("sqlContext <- sparkRSQL.init(sc)").verifyResult(null)
      s.run("hiveContext <- sparkRHive.init(sc)").verifyResult(null)
      s.run("""localDF <- data.frame(name=c("John", "Smith", "Sarah"), age=c(19, 23, 18))""")
        .verifyResult(null)
      s.run("df <- createDataFrame(sqlContext, localDF)").verifyResult(null)
      s.run("printSchema(df)").verifyResult(literal(
        """|root
          | |-- name: string (nullable = true)
          | |-- age: double (nullable = true)""".stripMargin))
    }
  }

  test("application kills session") {
    withNewSession(Spark()) { s =>
      s.run("System.exit(0)")
      s.verifySessionState(SessionState.Dead())
    }
  }

  test("should kill RSCDriver if it doesn't respond to end session") {
    val testConfName = s"${RSCConf.LIVY_SPARK_PREFIX}${RSCConf.Entry.TEST_STUCK_END_SESSION.key()}"
    withNewSession(Spark(), Map(testConfName -> "true")) { s =>
      val appId = s.appId()
      s.stop()
      val appReport = cluster.yarnClient.getApplicationReport(appId)
      appReport.getYarnApplicationState() shouldBe YarnApplicationState.KILLED
    }
  }

  test("user jars are properly imported in Scala interactive sessions") {
    // Include a popular Java library to test importing user jars.
    val sparkConf = Map("spark.jars.packages" -> "org.codehaus.plexus:plexus-utils:3.0.24")
    withNewSession(Spark(), sparkConf) { s =>
      // Check is the library loaded in JVM in the proper class loader.
      s.run("Thread.currentThread.getContextClassLoader.loadClass" +
          """("org.codehaus.plexus.util.FileUtils")""")
        .verifyResult(".*Class\\[_\\] = class org.codehaus.plexus.util.FileUtils")

      // Check does Scala interpreter see the library.
      s.run("import org.codehaus.plexus.util._").verifyResult("import org.codehaus.plexus.util._")

      // Check does SparkContext see classes defined by Scala interpreter.
      s.run("case class Item(i: Int)").verifyResult("defined class Item")
      s.run("val rdd = sc.parallelize(Array.fill(10){new Item(scala.util.Random.nextInt(1000))})")
        .verifyResult("rdd.*")
      s.run("rdd.count()").verifyResult(".*= 10")
    }
  }

  private def withNewSession[R]
    (kind: Kind, sparkConf: Map[String, String] = Map.empty)
    (f: (LivyRestClient#InteractiveSession) => R): R = {
    withSession(livyClient.startSession(kind, sparkConf)) { s =>
      s.verifySessionIdle()
      f(s)
    }
  }

  private def startsWith(result: String): String = Pattern.quote(result) + ".*"

  private def literal(result: String): String = Pattern.quote(result)
}
