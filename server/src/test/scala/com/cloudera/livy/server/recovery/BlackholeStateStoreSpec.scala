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

package com.cloudera.livy.server.recovery

import org.scalatest.FunSpec
import org.scalatest.Matchers._

class BlackholeStateStoreSpec extends FunSpec {
  describe("BlackholeStateStore") {
    val stateStore = new BlackholeStateStore()

    it("set should not throw") {
      stateStore.set("", 1.asInstanceOf[Object])
    }

    it("get should return None") {
      val v = stateStore.get("", classOf[Object])
      v shouldBe None
    }

    it("getChildren should return empty list") {
      val c = stateStore.getChildren("")
      c shouldBe empty
    }

    it("remove should not throw") {
      stateStore.remove("")
    }
  }
}
