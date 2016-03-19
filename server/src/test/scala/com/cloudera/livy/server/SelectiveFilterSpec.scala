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
import javax.servlet.http.HttpServletRequest

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import org.mockito.Matchers.{eq => meq, _}
import org.mockito.Mockito._
import org.scalatest.FunSpec

class SelectiveFilterSpec extends FunSpec {

  describe("The filter") {

    it("should bypass configured paths") {
      val wrapped = mock(classOf[Filter])
      val filter = new SelectiveFilter(wrapped)

      val config = mock(classOf[FilterConfig])
      when(config.getInitParameter(meq(SelectiveFilter.BYPASS_PATH_RE_LIST)))
        .thenReturn("/bypass,/anotherBypass[0-9]+")
      filter.init(config)
      verify(wrapped).init(config)

      val res = mock(classOf[ServletResponse])
      val chain = mock(classOf[FilterChain])
      val req = mock(classOf[HttpServletRequest])
      when(req.getServletPath()).thenReturn("/")

      when(req.getPathInfo()).thenReturn("/bypass")
      filter.doFilter(req, res, chain)
      verify(wrapped, never()).doFilter(any(), any(), any())

      when(req.getPathInfo()).thenReturn("/anotherBypass1234")
      filter.doFilter(req, res, chain)
      verify(wrapped, never()).doFilter(any(), any(), any())

      when(req.getPathInfo()).thenReturn("/goThrough")
      filter.doFilter(req, res, chain)
      when(req.getPathInfo()).thenReturn("/bypass/noMatch")
      filter.doFilter(req, res, chain)
      verify(wrapped, times(2)).doFilter(any(), any(), any())

      filter.destroy()
      verify(wrapped).destroy()
    }

  }

}
