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

import java.io.{FileNotFoundException, InputStream, IOException}
import java.util

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Options.{CreateOpts, Rename}
import org.apache.hadoop.fs.permission.FsPermission
import org.hamcrest.Description
import org.mockito.ArgumentMatcher
import org.mockito.Matchers.{any, anyInt, argThat, eq => equal}
import org.mockito.Mockito.{atLeastOnce, verify, when}
import org.mockito.internal.matchers.Equals
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar.mock

import com.cloudera.livy.{LivyBaseUnitTestSuite, LivyConf}

class FileSystemStateStoreSpec extends FunSpec with LivyBaseUnitTestSuite {
  describe("FileSystemStateStore") {
    def pathEq(wantedPath: String): Path = argThat(new ArgumentMatcher[Path] {
      private val matcher = new Equals(wantedPath)

      override def matches(path: Any): Boolean = matcher.matches(path.toString)

      override def describeTo(d: Description): Unit = { matcher.describeTo(d) }
    })

    def makeConf(): LivyConf = {
      val conf = new LivyConf()
      conf.set(LivyConf.RECOVERY_STATE_STORE_URL, "file://tmp/")

      conf
    }

    def mockFileContext(rootDirPermission: String): FileContext = {
      val fileContext = mock[FileContext]
      val rootDirStatus = mock[FileStatus]
      when(fileContext.getFileStatus(any())).thenReturn(rootDirStatus)
      when(rootDirStatus.getPermission).thenReturn(new FsPermission(rootDirPermission))

      fileContext
    }

    it("should throw if url is not configured") {
      intercept[IllegalArgumentException](new FileSystemStateStore(new LivyConf()))
    }

    it("should set and verify file permission") {
      val fileContext = mockFileContext("700")
      new FileSystemStateStore(makeConf(), Some(fileContext))

      verify(fileContext).setUMask(new FsPermission("077"))
    }

    it("should reject insecure permission") {
      def test(permission: String): Unit = {
        val fileContext = mockFileContext(permission)

        intercept[IllegalArgumentException](new FileSystemStateStore(makeConf(), Some(fileContext)))
      }
      test("600")
      test("400")
      test("677")
      test("670")
      test("607")
    }

    it("set should write with an intermediate file") {
      val fileContext = mockFileContext("700")
      val outputStream = mock[FSDataOutputStream]
      when(fileContext.create(pathEq("/key.tmp"), any[util.EnumSet[CreateFlag]], any[CreateOpts]))
        .thenReturn(outputStream)

      val stateStore = new FileSystemStateStore(makeConf(), Some(fileContext))

      stateStore.set("key", "value")

      verify(outputStream).write(""""value"""".getBytes)
      verify(outputStream, atLeastOnce).close()


      verify(fileContext).rename(pathEq("/key.tmp"), pathEq("/key"), equal(Rename.OVERWRITE))
      verify(fileContext).delete(pathEq("/.key.tmp.crc"), equal(false))
    }

    it("get should read file") {
      val fileContext = mockFileContext("700")
      abstract class MockInputStream extends InputStream with Seekable with PositionedReadable {}
      val inputStream: InputStream = mock[MockInputStream]
      when(inputStream.read(any[Array[Byte]](), anyInt(), anyInt())).thenAnswer(new Answer[Int] {
        private var firstCall = true
        override def answer(invocation: InvocationOnMock): Int = {
          if (firstCall) {
            firstCall = false
            val buf = invocation.getArguments()(0).asInstanceOf[Array[Byte]]
            val b = """"value"""".getBytes()
            b.copyToArray(buf)
            b.length
          } else {
            -1
          }
        }
      })

      when(fileContext.open(pathEq("/key"))).thenReturn(new FSDataInputStream(inputStream))

      val stateStore = new FileSystemStateStore(makeConf(), Some(fileContext))

      stateStore.get[String]("key") shouldBe Some("value")

      verify(inputStream, atLeastOnce).close()
    }

    it("get non-existent key should return None") {
      val fileContext = mockFileContext("700")
      when(fileContext.open(any())).thenThrow(new FileNotFoundException("Unit test"))

      val stateStore = new FileSystemStateStore(makeConf(), Some(fileContext))

      stateStore.get[String]("key") shouldBe None
    }

    it("getChildren should list file") {
      val parentPath = "path"
      def makeFileStatus(name: String): FileStatus = {
        val fs = new FileStatus()
        fs.setPath(new Path(parentPath, name))
        fs
      }
      val children = Seq("c1", "c2")

      val fileContext = mockFileContext("700")
      val util = mock[FileContext#Util]
      when(util.listStatus(pathEq(s"/$parentPath")))
        .thenReturn(children.map(makeFileStatus).toArray)
      when(fileContext.util()).thenReturn(util)

      val stateStore = new FileSystemStateStore(makeConf(), Some(fileContext))
      stateStore.getChildren(parentPath) should contain theSameElementsAs children
    }

    def getChildrenErrorTest(error: Exception): Unit = {
      val parentPath = "path"

      val fileContext = mockFileContext("700")
      val util = mock[FileContext#Util]
      when(util.listStatus(pathEq(s"/$parentPath"))).thenThrow(error)
      when(fileContext.util()).thenReturn(util)

      val stateStore = new FileSystemStateStore(makeConf(), Some(fileContext))
      stateStore.getChildren(parentPath) shouldBe empty
    }

    it("getChildren should return empty list if the key doesn't exist") {
      getChildrenErrorTest(new IOException("Unit test"))
    }

    it("getChildren should return empty list if key doesn't exist") {
      getChildrenErrorTest(new FileNotFoundException("Unit test"))
    }

    it("remove should delete file") {
      val fileContext = mockFileContext("700")

      val stateStore = new FileSystemStateStore(makeConf(), Some(fileContext))
      stateStore.remove("key")

      verify(fileContext).delete(pathEq("/key"), equal(false))
    }
  }
}
