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

import java.util.regex.Pattern
import javax.servlet._
import javax.servlet.http.HttpServletRequest

object SelectiveFilter {

  val BYPASS_PATH_RE_LIST = "livy.auth.filter.bypass_paths"

}

/**
 * A servlet filter that wraps another filter, and bypasses it for a configured set of URIs.
 */
class SelectiveFilter(wrapped: Filter) extends Filter {

  private var bypassPaths: Seq[Pattern] = Nil

  override def init(config: FilterConfig): Unit = {
    Option(config.getInitParameter(SelectiveFilter.BYPASS_PATH_RE_LIST)).foreach { cfg =>
      bypassPaths = cfg.split(",").map(_.trim).filter(_.nonEmpty).map(Pattern.compile).toSeq
    }
    wrapped.init(config)
  }

  override def destroy(): Unit = {
    wrapped.destroy()
  }

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain): Unit = {
    val req = request.asInstanceOf[HttpServletRequest]
    val servlet = req.getServletPath().stripSuffix("/")
    val path = servlet + Option(req.getPathInfo()).getOrElse("")

    if (bypassPaths.exists(_.matcher(path).matches())) {
      chain.doFilter(request, response)
    } else {
      wrapped.doFilter(request, response, chain)
    }
  }

}
