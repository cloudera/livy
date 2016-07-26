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

import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.cloudera.livy.LivyConf

class AccessFilter(livyConf: LivyConf) extends Filter {

  private lazy val allowedUsers = livyConf.allowedUsers().toSet

  override def init(filterConfig: FilterConfig): Unit = {}

  override def doFilter(request: ServletRequest,
                        response: ServletResponse,
                        chain: FilterChain): Unit = {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val remoteUser = httpRequest.getRemoteUser
    if(allowedUsers.contains(remoteUser)) {
      chain.doFilter(request, response)
    } else {
      val httpServletResponse = response.asInstanceOf[HttpServletResponse]
        httpServletResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED,
          "User not authorised to use Livy.")
    }
  }

  override def destroy(): Unit = {}
}

