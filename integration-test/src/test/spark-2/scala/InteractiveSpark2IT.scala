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

package com.cloudera.livy.test

import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import scala.language.postfixOps

import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.scalatest.OptionValues._

import com.cloudera.livy.rsc.RSCConf
import com.cloudera.livy.sessions._
import com.cloudera.livy.test.framework.{BaseIntegrationTestSuite, LivyRestClient}

class InteractiveSpark2IT extends BaseIntegrationTestSuite {
  test("variable spark is set") {
    withNewSession(Spark()) { s =>
      s.run("spark.version").verifyResult(startsWith("res0: String = 2."))
    }
  }

  private def withNewSession[R]
    (kind: Kind, sparkConf: Map[String, String] = Map.empty, waitForIdle: Boolean = true)
    (f: (LivyRestClient#InteractiveSession) => R): R = {
    withSession(livyClient.startSession(kind, sparkConf)) { s =>
      if (waitForIdle) {
        s.verifySessionIdle()
      }
      f(s)
    }
  }

  private def startsWith(result: String): String = Pattern.quote(result) + ".*"

  private def literal(result: String): String = Pattern.quote(result)
}
