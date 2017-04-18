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

package com.cloudera.livy.server.ui

import org.scalatra.ScalatraServlet

import com.cloudera.livy.LivyConf
import com.cloudera.livy.Logging

class UIServlet(livyConf: LivyConf) extends ScalatraServlet with Logging {
  before() { contentType = "text/html" }

  get("/") {
    <link rel="stylesheet" href="/static/sessions.css" type="text/css"/> ++
      <script src="/static/jquery-3.2.1.min.js"></script> ++
      <script src="/static/sessions.js"></script> ++
      <div id="sessions"></div>
  }
}
