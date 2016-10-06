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

import scala.collection.JavaConverters._

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api._
import org.apache.curator.framework.listen.Listenable
import org.apache.zookeeper.data.Stat
import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.{LivyBaseUnitTestSuite, LivyConf}

class ZooKeeperStateStoreSpec extends FunSpec with LivyBaseUnitTestSuite {
  describe("ZooKeeperStateStore") {
    case class TestFixture(stateStore: ZooKeeperStateStore, curatorClient: CuratorFramework)
    val conf = new LivyConf()
    conf.set(LivyConf.RECOVERY_STATE_STORE_URL, "host")
    val key = "key"
    val prefixedKey = s"/livy/$key"

    def withMock[R](testBody: TestFixture => R): R = {
      val curatorClient = mock[CuratorFramework]
      when(curatorClient.getUnhandledErrorListenable())
        .thenReturn(mock[Listenable[UnhandledErrorListener]])
      val stateStore = new ZooKeeperStateStore(conf, Some(curatorClient))
      testBody(TestFixture(stateStore, curatorClient))
    }

    def mockExistsBuilder(curatorClient: CuratorFramework, exists: Boolean): Unit = {
      val existsBuilder = mock[ExistsBuilder]
      when(curatorClient.checkExists()).thenReturn(existsBuilder)
      if (exists) {
        when(existsBuilder.forPath(prefixedKey)).thenReturn(mock[Stat])
      }
    }

    it("should throw on bad config") {
      withMock { f =>
        val conf = new LivyConf()
        intercept[IllegalArgumentException] { new ZooKeeperStateStore(conf) }

        conf.set(LivyConf.RECOVERY_STATE_STORE_URL, "host")
        conf.set(ZooKeeperStateStore.ZK_RETRY_CONF, "bad")
        intercept[IllegalArgumentException] { new ZooKeeperStateStore(conf) }
      }
    }

    it("set should use curatorClient") {
      withMock { f =>
        mockExistsBuilder(f.curatorClient, true)

        val setDataBuilder = mock[SetDataBuilder]
        when(f.curatorClient.setData()).thenReturn(setDataBuilder)

        f.stateStore.set("key", 1.asInstanceOf[Object])

        verify(f.curatorClient).start()
        verify(setDataBuilder).forPath(prefixedKey, Array[Byte](49))
      }
    }

    it("set should create parents if they don't exist") {
      withMock { f =>
        mockExistsBuilder(f.curatorClient, false)

        val createBuilder = mock[CreateBuilder]
        when(f.curatorClient.create()).thenReturn(createBuilder)
        val p = mock[ProtectACLCreateModePathAndBytesable[String]]
        when(createBuilder.creatingParentsIfNeeded()).thenReturn(p)

        f.stateStore.set("key", 1.asInstanceOf[Object])

        verify(f.curatorClient).start()
        verify(p).forPath(prefixedKey, Array[Byte](49))
      }
    }

    it("get should retrieve data from curatorClient") {
      withMock { f =>
        mockExistsBuilder(f.curatorClient, true)

        val getDataBuilder = mock[GetDataBuilder]
        when(f.curatorClient.getData()).thenReturn(getDataBuilder)
        when(getDataBuilder.forPath(prefixedKey)).thenReturn(Array[Byte](50))

        val v = f.stateStore.get[Int]("key")

        verify(f.curatorClient).start()
        v shouldBe Some(2)
      }
    }

    it("get should return None if key doesn't exist") {
      withMock { f =>
        mockExistsBuilder(f.curatorClient, false)

        val v = f.stateStore.get[Int]("key")

        verify(f.curatorClient).start()
        v shouldBe None
      }
    }

    it("getChildren should use curatorClient") {
      withMock { f =>
        mockExistsBuilder(f.curatorClient, true)

        val getChildrenBuilder = mock[GetChildrenBuilder]
        when(f.curatorClient.getChildren()).thenReturn(getChildrenBuilder)
        val children = List("abc", "def")
        when(getChildrenBuilder.forPath(prefixedKey)).thenReturn(children.asJava)

        val c = f.stateStore.getChildren("key")

        verify(f.curatorClient).start()
        c shouldBe children
      }
    }

    it("getChildren should return empty list if key doesn't exist") {
      withMock { f =>
        mockExistsBuilder(f.curatorClient, false)

        val c = f.stateStore.getChildren("key")

        verify(f.curatorClient).start()
        c shouldBe empty
      }
    }

    it("remove should use curatorClient") {
      withMock { f =>
        val deleteBuilder = mock[DeleteBuilder]
        when(f.curatorClient.delete()).thenReturn(deleteBuilder)
        val g = mock[ChildrenDeletable]
        when(deleteBuilder.guaranteed()).thenReturn(g)

        f.stateStore.remove(key)

        verify(g).forPath(prefixedKey)
      }
    }
  }
}
