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

import java.io.File
import java.util.UUID
import javax.servlet.http.HttpServletResponse._

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.client.common.HttpMessages._
import com.cloudera.livy.server.batch.CreateBatchRequest
import com.cloudera.livy.sessions.SessionState
import com.cloudera.livy.test.apps._
import com.cloudera.livy.test.framework.BaseIntegrationTestSuite

class BatchIT extends BaseIntegrationTestSuite {

  private var testLibPath: Path = _
  private var conf: Configuration = _
  private var fs: FileSystem = _

  test("upload test lib") {
    // Load the HDFS config from the generated path, if available.
    // TODO: how to do this for remote clusters.
    conf = new Configuration(false)
    cluster.configDir().listFiles().foreach { f =>
      if (f.getName().endsWith(".xml")) {
        conf.addResource(new Path(f.toURI()))
      }
    }

    val hdfsPath = new Path("/testlib-" + UUID.randomUUID().toString() + ".jar")
    fs = FileSystem.get(hdfsPath.toUri(), conf)
    fs.copyFromLocalFile(new Path(new File(testLib).toURI()), hdfsPath)
    testLibPath = fs.makeQualified(hdfsPath)
  }

  test("submit spark app") {
    assume(testLibPath != null, "Test lib not uploaded.")
    val output = "/" + UUID.randomUUID().toString()
    val result = runBatch(classOf[SimpleSparkApp], args = List(output))
    assert(result.state === SessionState.Success().toString)
    assert(fs.isDirectory(new Path(output)))
  }

  test("submit an app that fails") {
    assume(testLibPath != null, "Test lib not uploaded.")
    val output = "/" + UUID.randomUUID().toString()
    val result = runBatch(classOf[FailingApp], args = List(output))
    assert(result.state === SessionState.Error().toString)

    // The file is written to make sure the app actually ran, instead of just failing for
    // some other reason.
    assert(fs.isFile(new Path(output)))
  }

  private def runBatch(klass: Class[_], args: List[String] = Nil): SessionInfo = {
    val request = new CreateBatchRequest()
    request.file = testLibPath.toString()
    request.className = Some(klass.getName())
    request.args = args
    request.conf = Map("spark.yarn.maxAppAttempts" -> "1")

    val response = httpClient.preparePost(s"$livyEndpoint/batches")
      .setBody(mapper.writeValueAsString(request))
      .execute()
      .get()

    assert(response.getStatusCode() === SC_CREATED)

    val batchInfo = mapper.readValue(response.getResponseBodyAsStream(), classOf[SessionInfo])

    val terminalStates = Set(SessionState.Error(), SessionState.Dead(), SessionState.Success())
      .map(_.toString)

    var finished = false
    try {
      eventually(timeout(1 minute), interval(1 second)) {
        val response2 = httpClient.prepareGet(s"$livyEndpoint/batches/${batchInfo.id}")
          .execute()
          .get()
        assert(response2.getStatusCode() === SC_OK)
        val result = mapper.readValue(response2.getResponseBodyAsStream(), classOf[SessionInfo])
        assert(terminalStates.contains(result.state))

        finished = true
        result
      }
    } finally {
      if (!finished) {
        httpClient.prepareDelete(s"$livyEndpoint/batches/${batchInfo.id}").execute()
      }
    }
  }

}
