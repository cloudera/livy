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

package com.cloudera.livy.rsc;

import com.cloudera.livy.Job;
import com.cloudera.livy.rsc.rpc.RpcDispatcher;

public abstract class BaseProtocol extends RpcDispatcher {

  protected static class CancelJob {

    public final String id;

    CancelJob(String id) {
      this.id = id;
    }

    CancelJob() {
      this(null);
    }

  }

  protected static class EndSession {

  }

  protected static class Error {

    public final String cause;

    public Error(Throwable cause) {
      if (cause == null) {
        this.cause = "";
      } else {
        this.cause = Utils.stackTraceAsString(cause);
      }
    }

    public Error() {
      this(null);
    }

  }

  protected static class BypassJobRequest {

    public final String id;
    public final byte[] serializedJob;
    public final boolean synchronous;

    public BypassJobRequest(String id, byte[] serializedJob, boolean synchronous) {
      this.id = id;
      this.serializedJob = serializedJob;
      this.synchronous = synchronous;
    }

    public BypassJobRequest() {
      this(null, null, false);
    }

  }

  protected static class GetBypassJobStatus {

    public final String id;

    public GetBypassJobStatus(String id) {
      this.id = id;
    }

    public GetBypassJobStatus() {
      this(null);
    }

  }

  protected static class JobRequest<T> {

    public final String id;
    public final Job<T> job;

    public JobRequest(String id, Job<T> job) {
      this.id = id;
      this.job = job;
    }

    public JobRequest() {
      this(null, null);
    }

  }

  protected static class JobResult<T> {

    public final String id;
    public final T result;
    public final String error;

    public JobResult(String id, T result, Throwable error) {
      this.id = id;
      this.result = result;
      this.error = error != null ? Utils.stackTraceAsString(error) : null;
    }

    public JobResult() {
      this(null, null, null);
    }

  }

  protected static class JobStarted {

    public final String id;

    public JobStarted(String id) {
      this.id = id;
    }

    public JobStarted() {
      this(null);
    }

  }

  protected static class SyncJobRequest<T> {

    public final Job<T> job;

    public SyncJobRequest(Job<T> job) {
      this.job = job;
    }

    public SyncJobRequest() {
      this(null);
    }

  }

  public static class RemoteDriverAddress {

    public final String host;
    public final int port;

    public RemoteDriverAddress(String host, int port) {
      this.host = host;
      this.port = port;
    }

    public RemoteDriverAddress() {
      this(null, -1);
    }

  }

  public static class ReplJobRequest {

    public final String code;
    public final String id;

    public ReplJobRequest(String code, String id) {
      this.code = code;
      this.id = id;
    }

    public ReplJobRequest() {
      this(null, null);
    }
  }

  public static class GetReplJobResult {

    public final String id;

    public GetReplJobResult(String id) {
      this.id = id;
    }

    public GetReplJobResult() {
      this(null);
    }

  }

  public static class GetReplState {

  }

  public static class InitializationError {

    public final String stackTrace;

    public InitializationError(String stackTrace) {
      this.stackTrace = stackTrace;
    }

    public InitializationError() {
      this(null);
    }

  }

}
