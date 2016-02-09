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

import java.nio.charset.StandardCharsets.UTF_8
import javax.servlet.http.HttpServletResponse._

import org.scalatra._

class JsonServletSpec extends BaseJsonServletSpec {

  addServlet(new TestJsonServlet(), "/*")

  describe("The JSON servlet") {

    it("should serialize result of delete") {
      jdelete[MethodReturn]("/delete") { result =>
        assert(result.value === "delete")
      }
    }

    it("should serialize result of get") {
      jget[MethodReturn]("/get") { result =>
        assert(result.value === "get")
      }
    }

    it("should serialize an ActionResult's body") {
      jpost[MethodReturn]("/post", MethodArg("post")) { result =>
        assert(result.value === "post")
      }
    }

    it("should wrap a raw result") {
      jput[MethodReturn]("/put", MethodArg("put")) { result =>
        assert(result.value === "put")
      }
    }

    it("should bypass non-json results") {
      jpatch[Unit]("/patch", MethodArg("patch"), expectedStatus = SC_NOT_FOUND) { _ =>
        assert(response.body === "patch")
      }
    }

    it("should translate JSON errors to BadRequest") {
      post("/post", "abcde".getBytes(UTF_8), headers = defaultHeaders) {
        assert(status === SC_BAD_REQUEST)
      }
    }

    it("should respect user-installed error handlers") {
      post("/error", headers = defaultHeaders) {
        assert(status === SC_SERVICE_UNAVAILABLE)
        assert(response.body === "error")
      }
    }

    it("should handle empty return values") {
      jget[MethodReturn]("/empty") { result =>
        assert(result == null)
      }
    }

  }

}

private case class MethodArg(value: String)

private case class MethodReturn(value: String)

private class TestJsonServlet extends JsonServlet {

  before() {
    contentType = "application/json"
  }

  delete("/delete") {
    Ok(MethodReturn("delete"))
  }

  get("/get") {
    Ok(MethodReturn("get"))
  }

  jpost[MethodArg]("/post") { arg =>
    Created(MethodReturn(arg.value))
  }

  jput[MethodArg]("/put") { arg =>
    MethodReturn(arg.value)
  }

  jpatch[MethodArg]("/patch") { arg =>
    contentType = "text/plain"
    NotFound(arg.value)
  }

  get("/empty") {
    ()
  }

  post("/error") {
    throw new IllegalStateException("error")
  }

  // Install an error handler to make sure the parent's still work.
  error {
    case e: IllegalStateException =>
      contentType = "text/plain"
      ServiceUnavailable(e.getMessage())
  }

}

