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

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatra._

/**
 * An abstract servlet that provides overridden implementations for "post", "put" and "patch"
 * that can deserialize JSON data directly into user-defined types, without having to go through
 * a json4s intermediate. Results are also automatically serialized into JSON if the content type
 * says so.
 *
 * Serialization and deserialization are done through Jackson directly, so all Jackson features
 * are available.
 */
abstract class JsonServlet extends ScalatraServlet with FutureSupport {

  override protected implicit def executor: ExecutionContext = ExecutionContext.global

  private lazy val _defaultMapper = new ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

  /**
   * Override this method if you need a custom Jackson object mapper; the default mapper
   * has the default configuration, plus the Scala module.
   */
  protected def createMapper(): ObjectMapper = _defaultMapper

  protected final val mapper = createMapper()

  error {
    case e: JsonParseException => BadRequest(e.getMessage)
  }

  protected def jdelete(t: RouteTransformer*)(action: => Any): Route = {
    delete(t: _*) {
      doAction[Unit](request, response, _ => action)
    }
  }

  protected def jget(t: RouteTransformer*)(action: => Any): Route = {
    get(t: _*) {
      doAction[Unit](request, response, _ => action)
    }
  }

  protected def jpatch[T: ClassTag](t: RouteTransformer*)(action: T => Any): Route = {
    patch(t: _*) {
      doAction(request, response, action)
    }
  }

  protected def jpost[T: ClassTag](t: RouteTransformer*)(action: T => Any): Route = {
    post(t: _*) {
      doAction(request, response, action)
    }
  }

  protected def jput[T: ClassTag](t: RouteTransformer*)(action: T => Any): Route = {
    put(t: _*) {
      doAction[T](request, response, action)
    }
  }

  private def doAction[T: ClassTag](
      req: HttpServletRequest,
      res: HttpServletResponse,
      action: T => Any)(implicit klass: ClassTag[T]): Any = {
    val arg =
      if (klass.runtimeClass != classOf[Unit]) {
        mapper.readValue(req.getInputStream(), klass.runtimeClass).asInstanceOf[T]
      } else {
        ()
      }

    toResult(action(arg.asInstanceOf[T]), res)
  }

  private def isJson(res: HttpServletResponse, headers: Map[String, String] = Map()): Boolean = {
    val ctypeHeader = "Content-Type"
    headers.get(ctypeHeader).orElse(Option(res.getHeader(ctypeHeader)))
      .map(_.startsWith("application/json")).getOrElse(false)
  }

  private def toResult(obj: Any, res: HttpServletResponse): Any = obj match {
    case async: AsyncResult =>
      new AsyncResult {
        val is = async.is.map(toResult(_, res))
      }
    case ActionResult(status, body, headers) if isJson(res, headers) =>
      ActionResult(status, toJson(body), headers)
    case body if isJson(res) =>
      Ok(toJson(body))
    case other =>
      other
  }

  private def toJson(obj: Any): Any = {
    if (obj != null && obj != ()) {
      mapper.writeValueAsBytes(obj)
    } else {
      null
    }
  }

}
