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

package com.cloudera.livy.server.interactive

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.FunSpec

import com.cloudera.livy.LivyBaseUnitTestSuite
import com.cloudera.livy.sessions.{PySpark, SessionKindModule}

class CreateInteractiveRequestSpec extends FunSpec with LivyBaseUnitTestSuite {

  private val mapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    .registerModule(new SessionKindModule())

  describe("CreateInteractiveRequest") {

    it("should have default values for fields after deserialization") {
      val json = """{ "kind" : "pyspark" }"""
      val req = mapper.readValue(json, classOf[CreateInteractiveRequest])
      assert(req.kind === PySpark())
      assert(req.proxyUser === None)
      assert(req.jars === List())
      assert(req.pyFiles === List())
      assert(req.files === List())
      assert(req.driverMemory === None)
      assert(req.driverCores === None)
      assert(req.executorMemory === None)
      assert(req.executorCores === None)
      assert(req.numExecutors === None)
      assert(req.archives === List())
      assert(req.queue === None)
      assert(req.name === None)
      assert(req.conf === Map())
    }

  }

}
