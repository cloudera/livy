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

package com.cloudera.livy.test.framework

import com.ning.http.client.AsyncHttpClient
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers, Suite}

class BaseIntegrationTestSuite extends Suite with FunSpecLike with BeforeAndAfterAll with Matchers {
  val cluster = ClusterPool.get.lease()
  val httpClient = new AsyncHttpClient()

  override def beforeAll(): Unit = {
    cluster.runLivy()
  }

  override def afterAll(): Unit = {
    cluster.stopLivy()
  }

  protected def livyEndpoint: String = cluster.livyEndpoint
}
