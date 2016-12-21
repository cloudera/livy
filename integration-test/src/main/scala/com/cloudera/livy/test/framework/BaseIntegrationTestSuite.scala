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

package com.cloudera.livy.test.framework

import java.io.File
import java.util.UUID

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

import com.ning.http.client.AsyncHttpClient
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.util.ConverterUtils
import org.scalatest._

abstract class BaseIntegrationTestSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  import scala.concurrent.ExecutionContext.Implicits.global

  var cluster: Cluster = _
  var httpClient: AsyncHttpClient = _
  var livyClient: LivyRestClient = _

  protected def livyEndpoint: String = cluster.livyEndpoint

  protected val testLib = sys.props("java.class.path")
    .split(File.pathSeparator)
    .find(new File(_).getName().startsWith("livy-test-lib-"))
    .getOrElse(throw new Exception(s"Cannot find test lib in ${sys.props("java.class.path")}"))

  protected def getYarnLog(appId: String): String = {
    require(appId != null, "appId shouldn't be null")

    val appReport = cluster.yarnClient.getApplicationReport(ConverterUtils.toApplicationId(appId))
    assert(appReport != null, "appReport shouldn't be null")

    appReport.getDiagnostics()
  }

  protected def restartLivy(): Unit = {
    val f = future {
      cluster.stopLivy()
      cluster.runLivy()
    }
    Await.result(f, 3 minutes)
  }

  /** Uploads a file to HDFS and returns just its path. */
  protected def uploadToHdfs(file: File): String = {
    val hdfsPath = new Path(cluster.hdfsScratchDir(),
      UUID.randomUUID().toString() + "-" + file.getName())
    cluster.fs.copyFromLocalFile(new Path(file.toURI()), hdfsPath)
    hdfsPath.toUri().getPath()
  }

  /** Wrapper around test() to be used by pyspark tests. */
  protected def pytest(desc: String)(testFn: => Unit): Unit = {
    test(desc) {
      assume(cluster.isRealSpark(), "PySpark tests require a real Spark installation.")
      testFn
    }
  }

  /** Wrapper around test() to be used by SparkR tests. */
  protected def rtest(desc: String)(testFn: => Unit): Unit = {
    test(desc) {
      assume(!sys.props.getOrElse("skipRTests", "false").toBoolean, "Skipping R tests.")
      assume(cluster.isRealSpark(), "SparkR tests require a real Spark installation.")
      assume(cluster.hasSparkR(), "Spark under test does not support R.")
      testFn
    }
  }

  /** Clean up session and show info when test fails. */
  protected def withSession[S <: LivyRestClient#Session, R]
    (s: S)
    (f: (S) => R): R = {
    try {
      f(s)
    } catch {
      case NonFatal(e) =>
        try {
          val state = s.snapshot()
          info(s"Final session state: $state")
          state.appId.foreach { id => info(s"YARN diagnostics: ${getYarnLog(id)}") }
        } catch { case NonFatal(_) => }
        throw e
    } finally {
      try {
        s.stop()
      } catch {
        case NonFatal(e) => alert(s"Failed to stop session: $e")
      }
    }
  }

  // We need beforeAll() here because BatchIT's beforeAll() has to be executed after this.
  // Please create an issue if this breaks test logging for cluster creation.
  protected override def beforeAll() = {
    cluster = Cluster.get()
    httpClient = new AsyncHttpClient()
    livyClient = new LivyRestClient(httpClient, livyEndpoint)
  }
}
