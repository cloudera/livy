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

import java.io.IOException
import java.util.Enumeration
import javax.servlet._
import javax.servlet.http.HttpServletRequest

import edu.internet2.middleware.grouper.ws.j2ee.{HttpServletRequestCopier, ServletFilterLogger}

import com.cloudera.livy.{LivyConf, Logging}

/**
 * Logs Requests Body
 */
class ServletLoggerFilter(livyConf: LivyConf) extends ServletFilterLogger with Logging {

  import LivyConf._

  override def init(filterConfig: FilterConfig): Unit = {}
  override def destroy(): Unit = {}

  @throws(classOf[IOException])
  @throws(classOf[ServletException])
  override def doFilter(servletRequest: ServletRequest,
                        servletResponse: ServletResponse,
                        chain: FilterChain): Unit = {
    var requestCopier: HttpServletRequestCopier = new HttpServletRequestCopier(
      servletRequest.asInstanceOf[HttpServletRequest])

    try {
      chain.doFilter(requestCopier, servletResponse)
    } finally {
      logRequestHandler(requestCopier)
    }
  }

  /**
   * Method that handles copying request
   */
  def logRequestHandler(servletRequest: HttpServletRequestCopier): Unit = {
    try {
      val requestCopier: HttpServletRequestCopier = servletRequest
      requestCopier.finishReading()

      var requestParams = new StringBuilder()
      val enumeration = servletRequest.getParameterNames()
      while (enumeration.hasMoreElements()) {
        val name: String = enumeration.nextElement()
        requestParams.append(name + " = " + servletRequest.getParameter(name) + ", ")
      }

      val httpRequest = servletRequest.asInstanceOf[HttpServletRequest]
      logHttpRequest(httpRequest, requestCopier)
    } catch {
      case e: Exception => sys.error("Error in handling Request to be logged")
    }
  }

  /**
   * Method that logs request
   */
  def logHttpRequest(httpRequest: HttpServletRequest,
                    requestCopier: HttpServletRequestCopier): Unit = {
    try {
      val ip = httpRequest.getRemoteAddr
      val method = httpRequest.getMethod
      val requestCopy = requestCopier.getCopy

      if (method.contains("POST")) {
        if (livyConf.get(AUTH_TYPE) != null) {
          val userid = httpRequest.getRemoteUser
          info("\nUser: " + userid.toString()
                + "\nIP : " + ip.toString()
                + "\n" + new String(requestCopy))
        } else {
          info("\nIP : " + ip.toString()
                + "\n" + new String(requestCopy))
        }
      }
    } catch {
      case e: Exception => error("Error logging request", e)
    }
  }

}
