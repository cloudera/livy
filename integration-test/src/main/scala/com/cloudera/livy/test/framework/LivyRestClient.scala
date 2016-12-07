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

import java.util.regex.Pattern
import javax.servlet.http.HttpServletResponse

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Either, Left, Right}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ning.http.client.AsyncHttpClient
import com.ning.http.client.Response
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.util.ConverterUtils
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy.server.batch.CreateBatchRequest
import com.cloudera.livy.server.interactive.CreateInteractiveRequest
import com.cloudera.livy.sessions.{Kind, SessionKindModule, SessionState}
import com.cloudera.livy.utils.AppInfo

object LivyRestClient {
  private val BATCH_TYPE = "batches"
  private val INTERACTIVE_TYPE = "sessions"

  // TODO Define these in production code and share them with test code.
  @JsonIgnoreProperties(ignoreUnknown = true)
  private case class StatementResult(id: Int, state: String, output: Map[String, Any])

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class StatementError(ename: String, evalue: String, stackTrace: Seq[String])

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class SessionSnapshot(
    id: Int,
    appId: Option[String],
    state: String,
    appInfo: AppInfo,
    log: IndexedSeq[String])
}

class LivyRestClient(val httpClient: AsyncHttpClient, val livyEndpoint: String) {
  import LivyRestClient._

  val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  class Session(val id: Int, sessionType: String) {
    val url: String = s"$livyEndpoint/$sessionType/$id"

    def appId(): ApplicationId = {
      ConverterUtils.toApplicationId(snapshot().appId.get)
    }

    def snapshot(): SessionSnapshot = {
      val r = httpClient.prepareGet(url).execute().get()
      assertStatusCode(r, HttpServletResponse.SC_OK)

      mapper.readValue(r.getResponseBodyAsStream, classOf[SessionSnapshot])
    }

    def stop(): Unit = {
      httpClient.prepareDelete(url).execute().get()

      eventually(timeout(30 seconds), interval(1 second)) {
        verifySessionDoesNotExist()
      }
    }

    def verifySessionState(state: SessionState): Unit = {
      verifySessionState(Set(state))
    }

    def verifySessionState(states: Set[SessionState]): Unit = {
      val t = if (Cluster.isRunningOnTravis) 5.minutes else 2.minutes
      val strStates = states.map(_.toString)
      // Travis uses very slow VM. It needs a longer timeout.
      // Keeping the original timeout to avoid slowing down local development.
      eventually(timeout(t), interval(1 second)) {
        val s = snapshot().state
        assert(strStates.contains(s), s"Session $id state $s doesn't equal one of $strStates")
      }
    }

    def verifySessionDoesNotExist(): Unit = {
      val r = httpClient.prepareGet(url).execute().get()
      assertStatusCode(r, HttpServletResponse.SC_NOT_FOUND)
    }
  }

  class BatchSession(id: Int) extends Session(id, BATCH_TYPE) {
    def verifySessionDead(): Unit = verifySessionState(SessionState.Dead())
    def verifySessionRunning(): Unit = verifySessionState(SessionState.Running())
    def verifySessionSuccess(): Unit = verifySessionState(SessionState.Success())
  }

  class InteractiveSession(id: Int) extends Session(id, INTERACTIVE_TYPE) {
    class Statement(code: String) {
      val stmtId = {
        val requestBody = Map("code" -> code)
        val r = httpClient.preparePost(s"$url/statements")
          .setBody(mapper.writeValueAsString(requestBody))
          .execute()
          .get()
        assertStatusCode(r, HttpServletResponse.SC_CREATED)

        val newStmt = mapper.readValue(r.getResponseBodyAsStream, classOf[StatementResult])
        newStmt.id
      }

      final def result(): Either[String, StatementError] = {
        eventually(timeout(1 minute), interval(1 second)) {
          val r = httpClient.prepareGet(s"$url/statements/$stmtId")
            .execute()
            .get()
          assertStatusCode(r, HttpServletResponse.SC_OK)

          val newStmt = mapper.readValue(r.getResponseBodyAsStream, classOf[StatementResult])
          assert(newStmt.state == "available", s"Statement isn't available: ${newStmt.state}")

          val output = newStmt.output
          output.get("status") match {
            case Some("ok") =>
              val data = output("data").asInstanceOf[Map[String, Any]]
              var rst = data.getOrElse("text/plain", "")
              val magicRst = data.getOrElse("application/vnd.livy.table.v1+json", null)
              if (magicRst != null) {
                rst = mapper.writeValueAsString(magicRst)
              }
              Left(rst.asInstanceOf[String])
            case Some("error") => Right(mapper.convertValue(output, classOf[StatementError]))
            case Some(status) =>
              throw new IllegalStateException(s"Unknown statement $stmtId status: $status")
            case None =>
              throw new IllegalStateException(s"Unknown statement $stmtId output: $newStmt")
          }
        }
      }

      def verifyResult(expectedRegex: String): Unit = {
        result() match {
          case Left(result) =>
            if (expectedRegex != null) {
              matchStrings(result, expectedRegex)
            }
          case Right(error) =>
            assert(false, s"Got error from statement $stmtId $code: ${error.evalue}")
        }
      }

      def verifyError(
          ename: String = null, evalue: String = null, stackTrace: String = null): Unit = {
        result() match {
          case Left(result) =>
            assert(false, s"Statement $stmtId `$code` expected to fail, but succeeded.")
          case Right(error) =>
            val remoteStack = Option(error.stackTrace).getOrElse(Nil).mkString("\n")
            Seq(error.ename -> ename, error.evalue -> evalue, remoteStack -> stackTrace).foreach {
              case (actual, expected) if expected != null => matchStrings(actual, expected)
              case _ =>
            }
        }
      }

      private def matchStrings(actual: String, expected: String): Unit = {
        val regex = Pattern.compile(expected, Pattern.DOTALL)
        assert(regex.matcher(actual).matches(), s"$actual did not match regex $expected")
      }
    }

    def run(code: String): Statement = { new Statement(code) }

    def runFatalStatement(code: String): Unit = {
      val requestBody = Map("code" -> code)
      val r = httpClient.preparePost(s"$url/statements")
        .setBody(mapper.writeValueAsString(requestBody))
        .execute()

      verifySessionState(SessionState.Dead())
    }

    def verifySessionIdle(): Unit = {
      verifySessionState(SessionState.Idle())
    }
  }

  def startBatch(
      file: String,
      className: Option[String],
      args: List[String],
      sparkConf: Map[String, String]): BatchSession = {
    val r = new CreateBatchRequest()
    r.file = file
    r.className = className
    r.args = args
    r.conf = Map("spark.yarn.maxAppAttempts" -> "1") ++ sparkConf

    val id = start(BATCH_TYPE, mapper.writeValueAsString(r))
    new BatchSession(id)
  }

  def startSession(
      kind: Kind,
      sparkConf: Map[String, String],
      heartbeatTimeoutInSecond: Int): InteractiveSession = {
    val r = new CreateInteractiveRequest()
    r.kind = kind
    r.conf = sparkConf
    r.heartbeatTimeoutInSecond = heartbeatTimeoutInSecond

    val id = start(INTERACTIVE_TYPE, mapper.writeValueAsString(r))
    new InteractiveSession(id)
  }

  def connectSession(id: Int): InteractiveSession = { new InteractiveSession(id) }

  private def start(sessionType: String, body: String): Int = {
    val r = httpClient.preparePost(s"$livyEndpoint/$sessionType")
      .setBody(body)
      .execute()
      .get()

    assertStatusCode(r, HttpServletResponse.SC_CREATED)

    val newSession = mapper.readValue(r.getResponseBodyAsStream, classOf[SessionSnapshot])
    newSession.id
  }

  private def assertStatusCode(r: Response, expected: Int): Unit = {
    def pretty(r: Response): String = {
      s"${r.getStatusCode} ${r.getResponseBody}"
    }
    assert(r.getStatusCode() == expected, s"HTTP status code != $expected: ${pretty(r)}")
  }
}
