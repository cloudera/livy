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

import scala.xml.Node

import org.scalatra.ScalatraServlet

import com.cloudera.livy.LivyConf

class UIServlet extends ScalatraServlet {
  before() { contentType = "text/html" }

  def getHeader(title: String): Seq[Node] =
    <head>
      <link rel="stylesheet" href="/static/bootstrap.min.css" type="text/css"/>
      <link rel="stylesheet" href="/static/dataTables.bootstrap.min.css" type="text/css"/>
      <link rel="stylesheet" href="/static/livy-ui.css" type="text/css"/>
      <script src="/static/jquery-3.2.1.min.js"></script>
      <script src="/static/bootstrap.min.js"></script>
      <script src="/static/jquery.dataTables.min.js"></script>
      <script src="/static/dataTables.bootstrap.min.js"></script>
      <script src="/static/all-sessions.js"></script>
      <title>{title}</title>
    </head>

  def navBar(pageName: String): Seq[Node] =
    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" href="#">
            <img alt="Livy" src="/static/livy-mini-logo.png"/>
          </a>
        </div>
        <div class="collapse navbar-collapse">
          <ul class="nav navbar-nav">
            <li><a href="#">{pageName}</a></li>
          </ul>
        </div>
      </div>
    </nav>

  def createPage(pageName: String, pageContents: Seq[Node]): Seq[Node] =
    <html>
      {getHeader("Livy - " + pageName)}
      <body>
        <div class="container">
          {navBar(pageName)}
          {pageContents}
        </div>
      </body>
    </html>

  get("/") {
    val content =
      <div id="all-sessions">
        <div id="interactive-sessions"></div>
        <div id="batches"></div>
      </div>

    createPage("Sessions", content)
  }
}
