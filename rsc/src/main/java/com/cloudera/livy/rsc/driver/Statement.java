/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.rsc.driver;

import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.annotation.JsonRawValue;

public class Statement {
  public final Integer id;
  public final AtomicReference<StatementState> state;
  @JsonRawValue
  public volatile String output;

  public Statement(Integer id, StatementState state, String output) {
    this.id = id;
    this.state = new AtomicReference<>(state);
    this.output = output;
  }

  public Statement() {
    this(null, null, null);
  }

  public boolean compareAndTransition(final StatementState from, final StatementState to) {
    if (state.compareAndSet(from, to)) {
      StatementState.validate(from, to);
      return true;
    }
    return false;
  }

  public boolean checkStateAndExecute(final StatementState s, final Runnable runnable) {
    if (state.get() == s) {
      runnable.run();
      return true;
    } else {
      return false;
    }
  }
}
