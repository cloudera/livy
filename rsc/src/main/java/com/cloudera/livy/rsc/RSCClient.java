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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.client.common.BufferUtils;
import com.cloudera.livy.rsc.Utils;
import com.cloudera.livy.rsc.driver.AddJarJob;
import com.cloudera.livy.rsc.rpc.Rpc;
import static com.cloudera.livy.rsc.RSCConf.Entry.*;

public class RSCClient implements LivyClient {
  private static final Logger LOG = LoggerFactory.getLogger(RSCClient.class);

  private final ContextInfo ctx;
  private final RSCClientFactory factory;
  private final RSCConf conf;
  private final Map<String, JobHandleImpl<?>> jobs;
  public final Rpc driverRpc;
  private final ClientProtocol protocol;
  private final EventLoopGroup eventLoopGroup;
  private volatile boolean isAlive;

  RSCClient(RSCClientFactory factory, RSCConf conf, ContextInfo ctx) throws IOException {
    this.ctx = ctx;
    this.factory = factory;
    this.conf = conf;
    this.jobs = new ConcurrentHashMap<>();
    this.protocol = new ClientProtocol();
    this.eventLoopGroup = new NioEventLoopGroup(
        conf.getInt(RPC_MAX_THREADS),
        Utils.newDaemonThreadFactory("Client-RPC-Handler-" + ctx.getClientId() + "-%d"));

    try {
      this.driverRpc = Rpc.createClient(conf,
        eventLoopGroup,
        ctx.getRemoteAddress(),
        ctx.getRemotePort(),
        ctx.getClientId(),
        ctx.getSecret(),
        protocol).get();
    } catch (Throwable e) {
      ctx.dispose(true);
      throw Utils.propagate(e);
    }

    driverRpc.addListener(new Rpc.Listener() {
        @Override
        public void rpcClosed(Rpc rpc) {
          if (isAlive) {
            LOG.warn("Client RPC channel closed unexpectedly.");
            isAlive = false;
          }
        }
    });

    isAlive = true;
    LOG.debug("Connected to context {} ({}).", ctx.getClientId(), driverRpc.getChannel());
  }

  @Override
  public <T> JobHandle<T> submit(Job<T> job) {
    return protocol.submit(job);
  }

  @Override
  public <T> Future<T> run(Job<T> job) {
    return protocol.run(job);
  }

  @Override
  public synchronized void stop(boolean shutdownContext) {
    if (isAlive) {
      isAlive = false;
      try {
        if (shutdownContext) {
          protocol.endSession();
          ctx.dispose(false);
        }
      } catch (Exception e) {
        LOG.warn("Exception while waiting for end session reply.", e);
        ctx.dispose(true);
      } finally {
        driverRpc.close();
        eventLoopGroup.shutdownGracefully();
      }
      LOG.debug("Disconnected from context {}, shutdown = {}.", ctx.getClientId(),
        shutdownContext);
    }
  }

  @Override
  public Future<?> uploadJar(File jar) {
    throw new UnsupportedOperationException("Use addJar to add the jar to the remote context!");
  }

  @Override
  public Future<?> addJar(URI uri) {
    return submit(new AddJarJob(uri.toString()));
  }

  @Override
  public Future<?> uploadFile(File file) {
    throw new UnsupportedOperationException("Use addFile to add the file to the remote context!");
  }

  @Override
  public Future<?> addFile(URI uri) {
    return submit(new AddFileJob(uri.toString()));
  }

  public String bypass(ByteBuffer serializedJob, boolean sync) {
    return protocol.bypass(serializedJob, sync);
  }

  public Future<BypassJobStatus> getBypassJobStatus(String id) {
    return protocol.getBypassJobStatus(id);
  }

  public void cancel(String jobId) {
    protocol.cancel(jobId);
  }

  ContextInfo getContextInfo() {
    return ctx;
  }

  public String submitReplCode(String code) throws Exception {
    String id = UUID.randomUUID().toString();
    driverRpc.call(new BaseProtocol.ReplJobRequest(code, id));
    return id;
  }

  public Future<String> getReplJobResult(String id) throws Exception {
    return driverRpc.call(new BaseProtocol.GetReplJobResult(id), String.class);
  }

  public Future<String> getReplState() {
    return driverRpc.call(new BaseProtocol.GetReplState(), String.class);
  }

  private class ClientProtocol extends BaseProtocol {

    <T> JobHandleImpl<T> submit(Job<T> job) {
      final String jobId = UUID.randomUUID().toString();
      Object msg = new JobRequest<T>(jobId, job);

      final Promise<T> promise = driverRpc.createPromise();
      final JobHandleImpl<T> handle = new JobHandleImpl<T>(RSCClient.this,
        promise, jobId);
      jobs.put(jobId, handle);

      final io.netty.util.concurrent.Future<Void> rpc = driverRpc.call(msg);
      LOG.debug("Send JobRequest[{}].", jobId);

      // Link the RPC and the promise so that events from one are propagated to the other as
      // needed.
      rpc.addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Void>>() {
        @Override
        public void operationComplete(io.netty.util.concurrent.Future<Void> f) {
          if (f.isSuccess()) {
            handle.changeState(JobHandle.State.QUEUED);
          } else if (!promise.isDone()) {
            promise.setFailure(f.cause());
          }
        }
      });
      promise.addListener(new GenericFutureListener<Promise<T>>() {
        @Override
        public void operationComplete(Promise<T> p) {
          if (jobId != null) {
            jobs.remove(jobId);
          }
          if (p.isCancelled() && !rpc.isDone()) {
            rpc.cancel(true);
          }
        }
      });
      return handle;
    }

    <T> Future<T> run(Job<T> job) {
      @SuppressWarnings("unchecked")
      final io.netty.util.concurrent.Future<T> rpc = (io.netty.util.concurrent.Future<T>)
        driverRpc.call(new SyncJobRequest(job), Object.class);
      return rpc;
    }

    String bypass(ByteBuffer serializedJob, boolean sync) {
      String jobId = UUID.randomUUID().toString();
      Object msg = new BypassJobRequest(jobId, BufferUtils.toByteArray(serializedJob), sync);
      driverRpc.call(msg);
      return jobId;
    }

    Future<BypassJobStatus> getBypassJobStatus(String id) {
      return driverRpc.call(new GetBypassJobStatus(id), BypassJobStatus.class);
    }

    void cancel(String jobId) {
      driverRpc.call(new CancelJob(jobId));
    }

    Future<?> endSession() {
      return driverRpc.call(new EndSession());
    }

    private void handle(ChannelHandlerContext ctx, InitializationError msg) {
      LOG.warn("Error reported from remote driver: %s", msg.stackTrace);
    }

    private void handle(ChannelHandlerContext ctx, JobResult msg) {
      JobHandleImpl<?> handle = jobs.remove(msg.id);
      if (handle != null) {
        LOG.info("Received result for {}", msg.id);
        // TODO: need a better exception for this.
        Throwable error = msg.error != null ? new RuntimeException(msg.error) : null;
        if (error == null) {
          handle.setSuccess(msg.result);
        } else {
          handle.setFailure(error);
        }
      } else {
        LOG.warn("Received result for unknown job {}", msg.id);
      }
    }

    private void handle(ChannelHandlerContext ctx, JobStarted msg) {
      JobHandleImpl<?> handle = jobs.get(msg.id);
      if (handle != null) {
        handle.changeState(JobHandle.State.STARTED);
      } else {
        LOG.warn("Received event for unknown job {}", msg.id);
      }
    }

  }

  private static class AddFileJob implements Job<Object> {

    private final String path;

    AddFileJob() {
      this(null);
    }

    AddFileJob(String path) {
      this.path = path;
    }

    @Override
    public Object call(JobContext jc) throws Exception {
      jc.sc().addFile(path);
      return null;
    }

  }

}
