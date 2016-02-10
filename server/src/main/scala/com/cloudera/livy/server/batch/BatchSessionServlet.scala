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

package com.cloudera.livy.server.batch

import javax.servlet.http.HttpServletRequest

import com.cloudera.livy.server.SessionServlet
import com.cloudera.livy.sessions.SessionManager
import com.cloudera.livy.sessions.batch.BatchSession
import com.cloudera.livy.spark.batch.CreateBatchRequest

case class BatchSessionView(id: Long, state: String, log: Seq[String])

class BatchSessionServlet(batchManager: SessionManager[BatchSession, CreateBatchRequest])
  extends SessionServlet(batchManager)
{

  override protected def clientSessionView(session: BatchSession, req: HttpServletRequest): Any = {
    val logs =
      if (isOwner(session, req)) {
        val lines = session.logLines()

        val size = 10
        val from = math.max(0, lines.length - size)
        val until = from + size

        lines.view(from, until).toSeq
      } else {
        Nil
      }
    BatchSessionView(session.id, session.state.toString, logs)
  }

}
