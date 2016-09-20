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

package com.cloudera.livy.server.batch

import java.io.FileWriter
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.scalatest.{BeforeAndAfterAll, FunSpec, ShouldMatchers}
import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.{LivyConf, Utils}
import com.cloudera.livy.sessions.SessionState
import com.cloudera.livy.utils.{AppInfo, SparkApp}

class BatchSessionSpec
  extends FunSpec
  with BeforeAndAfterAll
  with ShouldMatchers {

  val script: Path = {
    val script = Files.createTempFile("livy-test", ".py")
    script.toFile.deleteOnExit()
    val writer = new FileWriter(script.toFile)
    try {
      writer.write(
        """
          |print "hello world"
        """.stripMargin)
    } finally {
      writer.close()
    }
    script
  }

  describe("A Batch process") {
    it("should create a process") {
      val req = new CreateBatchRequest()
      req.file = script.toString
      req.conf = Map("spark.driver.extraClassPath" -> sys.props("java.class.path"))

      val conf = new LivyConf().set(LivyConf.LOCAL_FS_WHITELIST, sys.props("java.io.tmpdir"))
      val batch = new BatchSession(0, null, None, conf, req)

      Utils.waitUntil({ () => !batch.state.isActive }, Duration(10, TimeUnit.SECONDS))
      (batch.state match {
        case SessionState.Success(_) => true
        case _ => false
      }) should be (true)

      batch.logLines() should contain("hello world")
    }

    it("should update appId and appInfo") {
      val conf = new LivyConf()
      val req = new CreateBatchRequest()
      val mockApp = mock[SparkApp]
      val batch = new BatchSession(0, null, None, conf, req, Some(mockApp))

      val expectedAppId = "APPID"
      batch.appIdKnown(expectedAppId)
      batch.appId shouldEqual Some(expectedAppId)

      val expectedAppInfo = AppInfo(Some("DRIVER LOG URL"), Some("SPARK UI URL"))
      batch.infoChanged(expectedAppInfo)
      batch.appInfo shouldEqual expectedAppInfo
    }
  }
}
