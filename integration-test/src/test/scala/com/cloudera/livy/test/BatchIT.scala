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
import javax.servlet.http.HttpServletResponse

import scala.annotation.tailrec
import scala.concurrent.duration._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.FileUtils

import com.cloudera.livy.server.batch.CreateBatchRequest
import com.cloudera.livy.sessions.{SessionKindModule, SessionState}
import com.cloudera.livy.test.framework.BaseIntegrationTestSuite

private class FatalException(msg: String) extends Exception(msg)

class BatchIT extends BaseIntegrationTestSuite {

  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  it("returns batch") {
    val rep = httpClient.prepareGet(s"$livyEndpoint/batches").execute().get()
    withClue(rep.getResponseBody) {
      rep.getStatusCode should equal(HttpServletResponse.SC_OK)
    }
  }

  // this test looks ugly
  it("can run PySpark script") {
    withClue(cluster.getLivyLog()) {
      val requestBody = new CreateBatchRequest()
      requestBody.file = uploadPySparkTestScript()

      val rep = httpClient.preparePost(s"$livyEndpoint/batches")
        .setBody(mapper.writeValueAsString(requestBody))
        .execute()
        .get()

      info("Batch submitted")

      val batchId: Int = withClue(rep.getResponseBody) {
        rep.getStatusCode should equal(HttpServletResponse.SC_CREATED)
        val newSession = mapper.readValue(rep.getResponseBodyAsStream, classOf[Map[String, Any]])
        newSession should contain key ("id")

        newSession("id").asInstanceOf[Int]
      }

      info(s"Batch id $batchId")

      // This looks ugly...
      @tailrec
      def waitUntilJobFinishes(deadline: Deadline = 5.minute.fromNow): Unit = {
        try {
          val rep = httpClient.prepareGet(s"$livyEndpoint/batches/$batchId").execute().get()
          withClue(rep.getResponseBody) {
            rep.getStatusCode should equal(HttpServletResponse.SC_OK)

            val sessionState =
              mapper.readValue(rep.getResponseBodyAsStream, classOf[Map[String, Any]])

            sessionState should contain key ("state")

            val validStates = Set(SessionState.Starting().toString, SessionState.Running().toString,
              SessionState.Success().toString)
            val currentState = sessionState("state").asInstanceOf[String]
            if (!validStates(currentState)) {
              throw new FatalException(s"Job is in unexpected state. $currentState $validStates")
            }

            sessionState("state") should equal(SessionState.Success().toString)
          }
        } catch {
          case e: FatalException => throw e
          case e: Exception =>
            if (deadline.isOverdue()) {
              throw new Exception(s"Job didn't finish in ${deadline.time}", e)
            } else {
              Thread.sleep(1.second.toMillis)
              waitUntilJobFinishes(deadline)
            }
        }
      }

      waitUntilJobFinishes()

      info(s"Batch job finished")
    }
  }

  private def uploadPySparkTestScript(): String = {
    val testScriptStream = getClass.getClassLoader.getResourceAsStream("pyspark-test.py")
    assert(testScriptStream != null, "Cannot find pyspark-test.py in test resource.")

    val tmpFile = File.createTempFile("pyspark-test", ".py")
    FileUtils.copyInputStreamToFile(testScriptStream, tmpFile)

    val destPath = s"/tmp/${tmpFile.getName}"
    cluster.upload(tmpFile.getAbsolutePath, destPath)

    tmpFile.delete()
    destPath
  }

}
