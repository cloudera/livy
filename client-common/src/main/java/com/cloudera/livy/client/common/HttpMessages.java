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

package com.cloudera.livy.client.common;

import java.util.List;
import java.util.Map;

import com.cloudera.livy.JobHandle.State;
import com.cloudera.livy.annotations.Private;

/**
 * There are the Java representations of the JSON messages used by the client protocol.
 *
 * Note that Jackson requires an empty constructor (or annotations) to be able to instantiate
 * types, so the extra noise is necessary here.
 */
@Private
public class HttpMessages {

  public static interface ClientMessage {

  }

  public static class CreateClientRequest implements ClientMessage {

    public final Map<String, String> conf;

    public CreateClientRequest(Map<String, String> conf) {
      this.conf = conf;
    }

    private CreateClientRequest() {
      this(null);
    }

  }

  public static class SessionInfo implements ClientMessage {

    public final int id;
    public final String appId;
    public final String owner;
    public final String proxyUser;
    public final String state;
    public final String kind;
    public final List<String> log;

    public SessionInfo(int id, String appId, String owner, String proxyUser, String state,
        String kind, List<String> log) {
      this.id = id;
      this.appId = appId;
      this.owner = owner;
      this.proxyUser = proxyUser;
      this.state = state;
      this.kind = kind;
      this.log = log;
    }

    private SessionInfo() {
      this(-1, null, null, null, null, null, null);
    }

  }

  public static class SerializedJob implements ClientMessage {

    public final byte[] job;

    public SerializedJob(byte[] job) {
      this.job = job;
    }

    private SerializedJob() {
      this(null);
    }

  }

  public static class AddResource implements ClientMessage {

    public final String uri;

    public AddResource(String uri) {
      this.uri = uri;
    }

    private AddResource() {
      this(null);
    }

  }

  public static class JobStatus implements ClientMessage {

    public final long id;
    public final State state;
    public final byte[] result;
    public final String error;

    public JobStatus(long id, State state, byte[] result, String error) {
      this.id = id;
      this.state = state;
      this.error = error;

      // json4s, at least, seems confused about whether a "null" in the JSON payload should
      // become a null array or a byte array with length 0. Since there shouldn't be any
      // valid serialized object in a byte array of size 0, translate that to null.
      this.result = (result != null && result.length > 0) ? result : null;

      if (this.result != null && state != State.SUCCEEDED) {
        throw new IllegalArgumentException("Result cannot be set unless job succeeded.");
      }
      // The check for "result" is not completely correct, but is here to make the unit tests work.
      if (this.result == null && error != null && state != State.FAILED) {
        throw new IllegalArgumentException("Error cannot be set unless job failed.");
      }
    }

    private JobStatus() {
      this(-1, null, null, null);
    }

  }

}
