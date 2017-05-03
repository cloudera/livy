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
      // Regression test for LIVY-.
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

      val parsedError = AbstractSparkInterpreter.KEEP_NEWLINE_REGEX.split(error)

      val expectedTraceback = parsedError.tail

      val (ename, traceback) = interpreter.parseError(error)
      ename shouldBe "<console>:27: error: type mismatch;"
      traceback shouldBe expectedTraceback
    }

    it("should parse Scala runtime error.") {
      val error =
        """java.lang.RuntimeException: message
          |    ... 48 elided
          |
          |Tailing message""".stripMargin

      val parsedError = AbstractSparkInterpreter.KEEP_NEWLINE_REGEX.split(error)

      val expectedTraceback = parsedError.tail

      val (ename, traceback) = interpreter.parseError(error)
      ename shouldBe "java.lang.RuntimeException: message"
      traceback shouldBe expectedTraceback
    }
  }
}
