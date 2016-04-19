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

import javax.servlet.http.HttpServletResponse

import scala.annotation.tailrec
import scala.concurrent.duration._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import com.cloudera.livy.server.batch.CreateBatchRequest
import com.cloudera.livy.sessions.{SessionKindModule, SessionState}
import com.cloudera.livy.test.framework._

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
      requestBody.file = uploadPySparkTestScript("pyspark-test.py")

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
          val currentState = livyClient.getBatchStatus(batchId)

          val validStates = Set(SessionState.Starting().toString, SessionState.Running().toString,
            SessionState.Success().toString)

          if (!validStates(currentState)) {
            throw new FatalException(s"Job is in unexpected state. $currentState $validStates")
          }

          currentState should equal(SessionState.Success().toString)
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

  it("Running batch recovery") {
    withClue(cluster.getLivyLog()) {
      val requestBody = new CreateBatchRequest()
      requestBody.file = uploadPySparkTestScript("sleep.py")
      requestBody.args = List("1200") /* sleep for 20 min */

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

      val appId: String = WaitForAppId(batchId)

      info(s"Batch AppId $appId")

      // Now kill livy and restart
      //
      cluster.stopLivy()

      cluster.runLivy()

      val stateAfterRestart = livyClient.getBatchStatus(batchId)

      stateAfterRestart should equal(SessionState.Running().toString)

      httpClient.prepareDelete(s"$livyEndpoint/batches/$batchId").execute()
    }
  }

  it("Finished batch recovery") {
    withClue(cluster.getLivyLog()) {
      val requestBody = new CreateBatchRequest()
      requestBody.file = uploadPySparkTestScript("sleep.py")
      requestBody.args = List("5") /* sleep for only 5 second */

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

      val appId: String = WaitForAppId(batchId)

      info(s"Batch AppId $appId")

      // Now kill livy and restart
      //
      cluster.stopLivy()

      // Make sure job is finished before restarting Livy
      waitUntilJobFinishInYarn(appId)

      cluster.runLivy()

      EnsureBatchSuccessInLivy(batchId)
    }
  }

  private def uploadPySparkTestScript(fileName: String): String = {
    val tmpFile = TestUtil.saveTestSourceToTempFile(fileName)

    val destPath = s"/tmp/${tmpFile.getName}"
    cluster.upload(tmpFile.getAbsolutePath, destPath)

    tmpFile.delete()
    destPath
  }

  @tailrec
  private def WaitForAppId(batchId: Int, deadline: Deadline = 5.minute.fromNow): String = {
    val appId = livyClient.getBatchAppId (batchId)

    if (appId != null) {
      appId
    } else if (deadline.isOverdue()) {
      throw new Exception(s"Couldn't get application Id in ${deadline.time}")
    } else {
      Thread.sleep(1.second.toMillis)
      WaitForAppId(batchId, deadline)
    }
  }

  @tailrec
  private def waitUntilJobFinishInYarn (appId: String): Unit = {
    val appStatus = cluster.runCommand(s"yarn application -status $appId")
    if (appStatus.indexOf("State : FINISHED") != -1) {
      Thread.sleep(1.second.toMillis)
      waitUntilJobFinishInYarn(appId)
    }
  }

  @tailrec
  private def EnsureBatchSuccessInLivy (
                batchId: Int,
                deadline: Deadline = 1.minute.fromNow): Unit = {
    val curState = livyClient.getBatchStatus(batchId)
    if (curState == SessionState.Success().toString) {
      return
    } else if (curState != SessionState.Running().toString) {
      throw new FatalException(s"Job is in unexpected state: $curState")
    } else {
      Thread.sleep(1.second.toMillis)
      EnsureBatchSuccessInLivy(batchId, deadline)
    }

  }
}
