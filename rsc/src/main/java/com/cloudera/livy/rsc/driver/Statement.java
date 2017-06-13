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
  public final String code;
  public final AtomicReference<StatementState> state;
  @JsonRawValue
  public volatile String output;
  public double progress;

  public Statement(Integer id, String code, StatementState state, String output) {
    this.id = id;
    this.code = code;
    this.state = new AtomicReference<>(state);
    this.output = output;
    this.progress = 0.0;
  }

  public Statement() {
    this(null, null, null, null);
  }

  public boolean compareAndTransit(final StatementState from, final StatementState to) {
    if (state.compareAndSet(from, to)) {
      StatementState.validate(from, to);
      return true;
    }
    return false;
  }

  public void updateProgress(double p) {
    if (this.state.get().isOneOf(StatementState.Cancelled, StatementState.Available)) {
      this.progress = 1.0;
    } else {
      this.progress = p;
    }
  }
}
