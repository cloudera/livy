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

package com.cloudera

import java.util.Properties

import scala.util.control.NonFatal

package object livy {

  private object LivyBuildInfo {
    val (
        livyVersion: String,
        livyBuildUser: String,
        livyRevision: String,
        livyBranch: String,
        livyBuildDate: String,
        livyRepo: String
      ) = {
      val unknown = "<unknown>"
      val defaultValue = (unknown, unknown, unknown, unknown, unknown, unknown)
      val resource = Option(Thread.currentThread().getContextClassLoader
        .getResourceAsStream("livy-version-info.properties"))

      try {
        resource.map { r =>
          val properties = new Properties()
          properties.load(r)
          (
            properties.getProperty("version", unknown),
            properties.getProperty("user", unknown),
            properties.getProperty("revision", unknown),
            properties.getProperty("branch", unknown),
            properties.getProperty("date", unknown),
            properties.getProperty("url", unknown)
          )
        }.getOrElse(defaultValue)
      } catch {
        case NonFatal(e) =>
          // swallow the exception
          defaultValue
      } finally {
        try {
          resource.foreach(_.close())
        } catch {
          case NonFatal(e) => // swallow the exception in closing the stream
        }
      }
    }
  }

  val LIVY_VERSION = LivyBuildInfo.livyVersion
  val LIVY_BUILD_USER = LivyBuildInfo.livyBuildUser
  val LIVY_REVISION = LivyBuildInfo.livyRevision
  val LIVY_BRANCH = LivyBuildInfo.livyBranch
  val LIVY_BUILD_DATE = LivyBuildInfo.livyBuildDate
  val LIVY_REPO_URL = LivyBuildInfo.livyRepo
}
