/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy

import java.io.File
import java.net.URI

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

package object client {

  case class LivyScalaClient(livyJavaClient: LivyClient) {

    def submit[T](block: JobContext => T): JobHandle[T] = {

      val job = new Job[T] {
        @throws(classOf[Exception])
        override def call(jobContext: JobContext): T = block(jobContext)
      }

      livyJavaClient.submit(job)
    }

    def run[T](block: JobContext => T): Future[_] = {

      val job = new Job[T] {
        @throws(classOf[Exception])
        override def call(jobContext: JobContext): T = block(jobContext)
      }
      Future {
        livyJavaClient.run(job).get()
      }
    }

    def stop(shutdownContext: Boolean) = livyJavaClient.stop(shutdownContext)

    def uploadJar(jar: File): Future[_] = Future {
      livyJavaClient.uploadJar(jar).get()
    }

    def addJar(uRI: URI): Future[_] = Future {
      livyJavaClient.addJar(uRI).get()
    }

    def uploadFile(file: File): Future[_] = Future {
      livyJavaClient.uploadFile(file).get()
    }

    def addFile(uRI: URI): Future[_] = Future {
      livyJavaClient.addJar(uRI).get()
    }

  }

  object LivyScalaConverter {

    implicit class ScalaWrapper(livyJavaClient: LivyClient) {

      def asScalaClient = new LivyScalaClient(livyJavaClient)

    }

  }

}


