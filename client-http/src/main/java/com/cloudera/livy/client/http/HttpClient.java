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

package com.cloudera.livy.client.http;

import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.client.common.BufferUtils;
import com.cloudera.livy.client.common.Serializer;
import static com.cloudera.livy.client.common.HttpMessages.*;
import static com.cloudera.livy.client.http.HttpConf.Entry.*;

/**
 * What is currently missing:
 * - monitoring of spark job IDs launched by jobs
 */
class HttpClient implements LivyClient {

  private final HttpConf config;
  private final LivyConnection conn;
  private final int sessionId;
  private final ScheduledExecutorService executor;
  private final Serializer serializer;

  private boolean stopped;

  HttpClient(URI uri, HttpConf httpConf) {
    this.config = httpConf;
    this.stopped = false;

    // If the given URI looks like it refers to an existing session, then try to connect to
    // an existing session. Note this means that any Spark configuration in httpConf will be
    // unused.
    Matcher m = Pattern.compile("(.*)" + LivyConnection.SESSIONS_URI + "/([0-9]+)")
      .matcher(uri.getPath());

    try {
      if (m.matches()) {
        URI base = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
          m.group(1), uri.getQuery(), uri.getFragment());

        this.conn = new LivyConnection(base, httpConf);
        this.sessionId = Integer.parseInt(m.group(2));
        conn.post(null, SessionInfo.class, "/%d/connect", sessionId);
      } else {
        Map<String, String> sessionConf = new HashMap<>();
        for (Map.Entry<String, String> e : config) {
          sessionConf.put(e.getKey(), e.getValue());
        }

        ClientMessage create = new CreateClientRequest(sessionConf);
        this.conn = new LivyConnection(uri, httpConf);
        this.sessionId = conn.post(create, SessionInfo.class, "/").id;
      }
    } catch (Exception e) {
      throw propagate(e);
    }

    // Because we only have one connection to the server, we don't need more than a single
    // threaded executor here.
    this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "HttpClient-" + sessionId);
        t.setDaemon(true);
        return t;
      }
    });

    this.serializer = new Serializer();
  }

  @Override
  public <T> JobHandle<T> submit(Job<T> job) {
    return sendJob("submit-job", job);
  }

  @Override
  public <T> Future<T> run(Job<T> job) {
    return sendJob("run-job", job);
  }

  @Override
  public synchronized void stop(boolean shutdownContext) {
    if (!stopped) {
      executor.shutdownNow();
      try {
        if (shutdownContext) {
          conn.delete(Map.class, "/%s", sessionId);
        }
      } catch (Exception e) {
        throw propagate(e);
      } finally {
        try {
          conn.close();
        } catch (Exception e) {
          // Ignore.
        }
      }
      stopped = true;
    }
  }

  public Future<?> uploadJar(File jar) {
    return uploadResource(jar, "upload-jar", "jar");
  }

  @Override
  public Future<?> addJar(URI uri) {
    return addResource("add-jar", uri);
  }

  public Future<?> uploadFile(File file) {
    return uploadResource(file, "upload-file", "file");
  }

  @Override
  public Future<?> addFile(URI uri) {
    return addResource("add-file", uri);
  }

  private Future<?> uploadResource(final File file, final String command, final String paramName) {
    Callable<Void> task = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        conn.post(file, Void.class,  paramName, "/%d/%s", sessionId, command);
        return null;
      }
    };
    return executor.submit(task);
  }

  private Future<?> addResource(final String command, final URI resource) {
    Callable<Void> task = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ClientMessage msg = new AddResource(resource.toString());
        conn.post(msg, Void.class, "/%d/%s", sessionId, command);
        return null;
      }
    };
    return executor.submit(task);
  }

  private <T> JobHandleImpl<T> sendJob(final String command, Job<T> job) {
    final ByteBuffer serializedJob = serializer.serialize(job);
    JobHandleImpl<T> handle = new JobHandleImpl<T>(config, conn, sessionId, executor, serializer);
    handle.start(command, serializedJob);
    return handle;
  }

  private RuntimeException propagate(Exception cause) {
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    } else {
      throw new RuntimeException(cause);
    }
  }

  // For testing.
  int getSessionId() {
    return sessionId;
  }

}
