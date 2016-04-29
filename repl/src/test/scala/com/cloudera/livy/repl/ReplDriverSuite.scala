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

import java.net.URI
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.launcher.SparkLauncher
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually._

import com.cloudera.livy._
import com.cloudera.livy.rsc.{PingJob, RSCClient, RSCConf}
import com.cloudera.livy.sessions.Spark

class ReplDriverSuite extends FunSuite {

  private implicit val formats = DefaultFormats

  test("start a repl session using the rsc") {
    val client = new LivyClientBuilder()
      .setConf("spark.master", "local")
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, sys.props("java.class.path"))
      .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, sys.props("java.class.path"))
      .setConf(RSCConf.Entry.LIVY_JARS.key(), "")
      .setURI(new URI("local:spark"))
      .setConf(RSCConf.Entry.DRIVER_CLASS.key(), classOf[ReplDriver].getName())
      .setConf(RSCConf.Entry.SESSION_KIND.key(), Spark().toString)
      .build()
      .asInstanceOf[RSCClient]

    try {
      // This is sort of what InteractiveSession.scala does to detect an idle session.
      val handle = client.submit(new PingJob()).get(1, TimeUnit.MINUTES)

      assert(client.getReplState().get(10, TimeUnit.SECONDS) === "idle")

      val statementId = client.submitReplCode("1 + 1")
      eventually(timeout(30 seconds), interval(100 millis)) {
        val rawResult = client.getReplJobResult(statementId).get(10, TimeUnit.SECONDS)
        val result = parse(rawResult)
        assert((result \ Session.STATUS).extract[String] === Session.OK)
      }
    } finally {
      client.stop(true);
    }
  }

}
