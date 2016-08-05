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

package com.cloudera.livy

import java.util.concurrent.{ExecutionException, Future => JFuture, TimeUnit}

import scala.concurrent.duration.Duration

package object scalaapi {

  /**
   *  A Scala Client for Livy which is a wrapper over the Java client
   *  @constructor Creates a Scala client
   *  @param livyJavaClient  the Java client of Livy
   *  {{{
   *     import com.cloudera.livy._
   *     import com.cloudera.livy.scalaapi._
   *     val url = "http://example.com"
   *     val livyJavaClient = new LivyClientBuilder(false).setURI(new URI(url))).build()
   *     val livyScalaClient = livyJavaClient.asScalaClient
   *  }}}
   */
  implicit class ScalaWrapper(livyJavaClient: LivyClient) {
    def asScalaClient: LivyScalaClient = new LivyScalaClient(livyJavaClient)
  }

  private[livy] def getJavaFutureResult[T](jFuture: JFuture[T],
                                           atMost: Duration = Duration.Undefined): T = {
    try {
      if (!atMost.isFinite()) jFuture.get else jFuture.get(atMost.toMillis, TimeUnit.MILLISECONDS)
    } catch {
      case executionException: ExecutionException => throw executionException.getCause
    }
  }
}
