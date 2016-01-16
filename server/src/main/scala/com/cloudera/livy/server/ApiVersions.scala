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

/**
 * This enum defines Livy's API versions.
 * [[com.cloudera.livy.server.AbstractApiVersioningSupport]] uses this for API version checking.
 *
 * Version is defined as <major version>.<minor version>.
 * When making backward compatible change (adding methods/fields), bump minor version.
 * When making backward incompatible change (renaming/removing methods/fields), bump major version.
 * This ensures our users can safely migrate to a newer API version if major version is unchanged.
 */
object ApiVersions extends Enumeration {
  type ApiVersions = Value
  // ApiVersions are ordered and the ordering is defined by the order of Value() calls.
  // Please make sure API version is defined in ascending order (Older API before newer).
  // AbstractApiVersioningSupport relies on the ordering.
  val v0_1 = Value("0.1")
}
