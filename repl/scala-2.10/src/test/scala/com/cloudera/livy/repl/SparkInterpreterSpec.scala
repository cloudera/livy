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

package com.cloudera.livy.repl

import org.scalatest._

import com.cloudera.livy.LivyBaseUnitTestSuite

class SparkInterpreterSpec extends FunSpec with Matchers with LivyBaseUnitTestSuite {
  describe("SparkInterpreter") {
    val interpreter = new SparkInterpreter(null)

    it("should parse Scala compile error.") {
      // Regression test for LIVY-260.
      val error =
        """<console>:27: error: type mismatch;
          | found   : Int
          | required: String
          |       sc.setJobGroup(groupName, groupName, true)
          |                      ^
          |<console>:27: error: type mismatch;
          | found   : Int
          | required: String
          |       sc.setJobGroup(groupName, groupName, true)
          |                                 ^
          |""".stripMargin

      val expectedTraceback = AbstractSparkInterpreter.KEEP_NEWLINE_REGEX.split(
        """ found   : Int
          | required: String
          |       sc.setJobGroup(groupName, groupName, true)
          |                      ^
          |<console>:27: error: type mismatch;
          | found   : Int
          | required: String
          |       sc.setJobGroup(groupName, groupName, true)
          |                                 ^
          |""".stripMargin)

      val (ename, traceback) = interpreter.parseError(error)
      ename shouldBe "<console>:27: error: type mismatch;"
      traceback shouldBe expectedTraceback
    }

    it("should parse Scala runtime error and remove internal frames.") {
      val error =
        """java.lang.RuntimeException: message
          |        at $iwC$$iwC$$iwC$$iwC$$iwC.error(<console>:25)
          |        at $iwC$$iwC$$iwC.error2(<console>:27)
          |        at $iwC$$iwC.<init>(<console>:41)
          |        at $iwC.<init>(<console>:43)
          |        at <init>(<console>:45)
          |        at .<init>(<console>:49)
          |        at .<clinit>(<console>)
          |        at .<init>(<console>:7)
          |        at .<clinit>(<console>)
          |        at $print(<console>)
          |        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
          |""".stripMargin

      val expectedTraceback = AbstractSparkInterpreter.KEEP_NEWLINE_REGEX.split(
        """        at <user code>.error(<console>:25)
          |        at <user code>.error2(<console>:27)
          |""".stripMargin)

      val (ename, traceback) = interpreter.parseError(error)
      ename shouldBe "java.lang.RuntimeException: message"
      traceback shouldBe expectedTraceback
    }
  }
}
