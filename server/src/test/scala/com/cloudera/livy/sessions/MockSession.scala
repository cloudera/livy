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

package com.cloudera.livy.sessions

import java.net.URI

import com.cloudera.livy.LivyConf

class MockSession(id: Int, owner: String, conf: LivyConf) extends Session(id, owner, conf) {
  override val proxyUser = None

  override protected def stopSession(): Unit = ()

  override def logLines(): IndexedSeq[String] = IndexedSeq()

  override def state: SessionState = SessionState.Idle()

  override def appId: Option[String] = None

  override val timeout: Long = 0L

  override def resolveURIs(uris: Seq[String]): Seq[String] = super.resolveURIs(uris)

  override def resolveURI(uri: URI): URI = super.resolveURI(uri)

  override def prepareConf(conf: Map[String, String],
      jars: Seq[String],
      files: Seq[String],
      archives: Seq[String],
      pyFiles: Seq[String]): Map[String, String] = {
    super.prepareConf(conf, jars, files, archives, pyFiles)
  }

}
