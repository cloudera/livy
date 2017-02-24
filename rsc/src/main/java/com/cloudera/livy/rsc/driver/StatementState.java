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

import java.util.*;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum StatementState {
  Waiting("waiting"),
  Running("running"),
  Available("available"),
  Cancelling("cancelling"),
  Cancelled("cancelled");

  private static final Logger LOG = LoggerFactory.getLogger(StatementState.class);

  private final String state;

  StatementState(final String text) {
      this.state = text;
  }

  @JsonValue
  @Override
  public String toString() {
      return state;
  }

  public boolean isOneOf(StatementState... states) {
    for (StatementState s : states) {
      if (s == this) {
        return true;
      }
    }
    return false;
  }

  private static final Map<StatementState, List<StatementState>> PREDECESSORS;

  static void put(StatementState key,
    Map<StatementState, List<StatementState>> map,
    StatementState... values) {
    map.put(key, Collections.unmodifiableList(Arrays.asList(values)));
  }

  static {
    final Map<StatementState, List<StatementState>> predecessors =
      new EnumMap<>(StatementState.class);
    put(Waiting, predecessors);
    put(Running, predecessors, Waiting);
    put(Available, predecessors, Running);
    put(Cancelling, predecessors, Running);
    put(Cancelled, predecessors, Waiting, Cancelling);

    PREDECESSORS = Collections.unmodifiableMap(predecessors);
  }

  static boolean isValid(StatementState from, StatementState to) {
    return PREDECESSORS.get(to).contains(from);
  }

  static void validate(StatementState from, StatementState to) {
    LOG.debug("{} -> {}", from, to);

    Preconditions.checkState(isValid(from, to), "Illegal Transition: %s -> %s", from, to);
  }
}
