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

package com.cloudera.livy.spark.batch

import com.fasterxml.jackson.databind.{JsonMappingException, ObjectMapper}
import org.scalatest.FunSpec

class CreateBatchRequestSpec extends FunSpec {

  private val mapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

  describe("CreateBatchRequest") {

    it("should have default values for fields after deserialization") {
      val json = """{ "file" : "foo" }"""
      val req = mapper.readValue(json, classOf[CreateBatchRequest])
      assert(req.file === "foo")
      assert(req.proxyUser === None)
      assert(req.args === List())
      assert(req.className === None)
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
