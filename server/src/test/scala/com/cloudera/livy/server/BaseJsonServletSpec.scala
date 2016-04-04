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

package com.cloudera.livy.server

import javax.servlet.http.HttpServletResponse._

import scala.reflect.ClassTag

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.FunSpecLike
import org.scalatra._
import org.scalatra.test.scalatest.ScalatraSuite

/**
 * Base class that enhances ScalatraSuite so that it's easier to test JsonServlet
 * implementations. Variants of the test methods (get, post, etc) exist with the "j"
 * prefix; these automatically serialize the body of the request to JSON, and
 * deserialize the result from JSON.
 *
 * In case the response is not JSON, the expected type for the test function should be
 * `Unit`, and the `response` object should be checked directly.
 */
abstract class BaseJsonServletSpec extends ScalatraSuite with FunSpecLike {

  protected val mapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

  protected val defaultHeaders: Map[String, String] = Map("Content-Type" -> "application/json")

  protected def jdelete[R: ClassTag](
      uri: String,
      expectedStatus: Int = SC_OK,
      headers: Map[String, String] = defaultHeaders)
      (fn: R => Unit): Unit = {
    delete(uri, headers = headers)(doTest(expectedStatus, fn))
  }

  protected def jget[R: ClassTag](
      uri: String,
      expectedStatus: Int = SC_OK,
      headers: Map[String, String] = defaultHeaders)
      (fn: R => Unit): Unit = {
    get(uri, headers = headers)(doTest(expectedStatus, fn))
  }

  protected def jpatch[R: ClassTag](
      uri: String,
      body: AnyRef,
      expectedStatus: Int = SC_OK,
      headers: Map[String, String] = defaultHeaders)
      (fn: R => Unit): Unit = {
    patch(uri, body = toJson(body), headers = headers)(doTest(expectedStatus, fn))
  }

  protected def jpost[R: ClassTag](
      uri: String,
      body: AnyRef,
      expectedStatus: Int = SC_CREATED,
      headers: Map[String, String] = defaultHeaders)
      (fn: R => Unit): Unit = {
    post(uri, body = toJson(body), headers = headers)(doTest(expectedStatus, fn))
  }

  /** A version of jpost specific for testing file upload. */
  protected def jupload[R: ClassTag](
      uri: String,
      files: Iterable[(String, Any)],
      headers: Map[String, String] = Map(),
      expectedStatus: Int = SC_OK)
      (fn: R => Unit): Unit = {
    post(uri, Map.empty, files)(doTest(expectedStatus, fn))
  }

  protected def jput[R: ClassTag](
      uri: String,
      body: AnyRef,
      expectedStatus: Int = SC_OK,
      headers: Map[String, String] = defaultHeaders)
      (fn: R => Unit): Unit = {
    put(uri, body = toJson(body), headers = headers)(doTest(expectedStatus, fn))
  }

  private def doTest[R: ClassTag](expectedStatus: Int, fn: R => Unit)
      (implicit klass: ClassTag[R]): Unit = {
    if (status != expectedStatus) {
      // Yeah this is weird, but we don't want to evaluate "response.body" if there's no error.
      assert(status === expectedStatus,
        s"Unexpected response status: $status != $expectedStatus (${response.body})")
    }
    // Only try to parse the body if response is in the "OK" range (20x).
    if ((status / 100) * 100 == SC_OK) {
      val result =
        if (header("Content-Type").startsWith("application/json")) {
          if (header("Content-Length").toInt > 0) {
            mapper.readValue(response.inputStream, klass.runtimeClass)
          } else {
            null
          }
        } else {
          assert(klass.runtimeClass == classOf[Unit])
          ()
        }
      fn(result.asInstanceOf[R])
    }
  }

  private def toJson(obj: Any): Array[Byte] = mapper.writeValueAsBytes(obj)

}
