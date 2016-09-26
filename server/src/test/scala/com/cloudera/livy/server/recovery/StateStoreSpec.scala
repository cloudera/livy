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

import com.cloudera.livy.LivyConf
import com.cloudera.livy.sessions.SessionManager

class StateStoreSpec extends FunSpec {
  describe("StateStore") {
    val conf = new LivyConf()

    it("should throw an error on get if it's not initialized") {
      intercept[AssertionError](StateStore.get)
    }

    it("should initialize blackhole state store if recovery is disabled") {
      StateStore.init(conf)
      StateStore.get shouldBe a[BlackholeStateStore]
    }

    it("should pick the correct store according to state store config") {
      StateStore.pickStateStore(createConf("filesystem")) shouldBe
        FileSystemStateStore.getClass.getName
      StateStore.pickStateStore(createConf("zookeeper")) shouldBe
        ZooKeeperStateStore.getClass.getName
    }

    def createConf(stateStore: String): LivyConf = {
      val conf = new LivyConf()
      conf.set(LivyConf.RECOVERY_MODE.key, SessionManager.SESSION_RECOVERY_MODE_RECOVERY)
      conf.set(LivyConf.RECOVERY_STATE_STORE.key, stateStore)
      conf
    }
  }
}
