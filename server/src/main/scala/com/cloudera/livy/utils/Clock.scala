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
package com.cloudera.livy.utils

/**
 * A lot of Livy code relies on time related functions like Thread.sleep.
 * To timing effects from unit test, this class is created to mock out time.
 *
 * Code in Livy should not call Thread.sleep() directly. It should call this class instead.
 */
object Clock {
  private var _sleep: Long => Unit = Thread.sleep

  def withSleepMethod(mockSleep: Long => Unit)(f: => Unit): Unit = {
    try {
      _sleep = mockSleep
      f
    } finally {
      _sleep = Thread.sleep
    }
  }

  def sleep: Long => Unit = _sleep
}
