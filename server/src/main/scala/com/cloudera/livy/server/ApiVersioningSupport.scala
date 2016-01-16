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

import javax.servlet.http.HttpServletRequest

import org.scalatra.{NotAcceptable, ScalatraBase}

/**
 * Livy's servlets can mix-in this trait to get API version support.
 *
 * Example: {{{
 * import ApiVersions._
 * class FooServlet
 *   ...
 *   with ApiVersioningSupport
 *   ...
 * {
 *   get("/test") {
 *     ...
 *   }
 *   get("/test", apiVersion <= v0_2) {
 *     ...
 *   }
 *   get("/test", apiVersion <= v0_1) {
 *     ...
 *   }
 * }
 * }}}
 */
trait ApiVersioningSupport extends AbstractApiVersioningSupport {
  this: ScalatraBase =>
  // Link the abstract trait to Livy's version enum.
  override val apiVersions = ApiVersions
  override type ApiVersionType = ApiVersions.Value
}

trait AbstractApiVersioningSupport {
  this: ScalatraBase =>
  protected val apiVersions: Enumeration
  protected type ApiVersionType

  /**
   * Before proceeding with routing, validate the specified API version in the request.
   * If validation passes, cache the parsed API version as a per-request attribute.
   */
  before() {
    request(AbstractApiVersioningSupport.ApiVersionKey) = request.getHeader("Accept") match {
      case acceptHeader @ AbstractApiVersioningSupport.AcceptHeaderRegex(apiVersion) =>
        try {
            apiVersions.withName(apiVersion).asInstanceOf[ApiVersionType]
        } catch {
          case e: NoSuchElementException =>
            halt(NotAcceptable(e.getMessage))
        }
      case _ =>
        // Return the latest version.
        apiVersions.apply(apiVersions.maxId - 1).asInstanceOf[ApiVersionType]
    }
  }

  /**
   * @return The specified API version in the request.
   */
  def apiVersion: ApiVersionType = {
    request(AbstractApiVersioningSupport.ApiVersionKey).asInstanceOf[ApiVersionType]
  }

}

object AbstractApiVersioningSupport {
  // Get every character after "application/vnd.livy.v" until hitting a + sign.
  private final val AcceptHeaderRegex = """application/vnd\.livy\.v([^\+]*).*""".r

  // AbstractApiVersioningSupport uses a per-request attribute to store the parsed API version.
  // This is the key name for the attribute.
  private final val ApiVersionKey = "apiVersion"
}
