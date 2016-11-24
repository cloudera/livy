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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.client.common.BufferUtils;
import com.cloudera.livy.rsc.driver.AddFileJob;
import com.cloudera.livy.rsc.driver.AddJarJob;
import com.cloudera.livy.rsc.rpc.Rpc;
import static com.cloudera.livy.rsc.RSCConf.Entry.*;

public class RSCClient implements LivyClient {
  private static final Logger LOG = LoggerFactory.getLogger(RSCClient.class);
  private static final AtomicInteger EXECUTOR_GROUP_ID = new AtomicInteger();

  private final RSCConf conf;
  private final Promise<ContextInfo> contextInfoPromise;
  private final Map<String, JobHandleImpl<?>> jobs;
  private final ClientProtocol protocol;
  private final Promise<Rpc> driverRpc;
  private final int executorGroupId;
  private final EventLoopGroup eventLoopGroup;
  private final Promise<URI> serverUriPromise;

  private ContextInfo contextInfo;
  private volatile boolean isAlive;

  private SessionStateListener stateListener;

  RSCClient(RSCConf conf, Promise<ContextInfo> ctx) throws IOException {
    this.conf = conf;
    this.contextInfoPromise = ctx;
    this.jobs = new ConcurrentHashMap<>();
    this.protocol = new ClientProtocol();
    this.driverRpc = ImmediateEventExecutor.INSTANCE.newPromise();
    this.executorGroupId = EXECUTOR_GROUP_ID.incrementAndGet();
    this.eventLoopGroup = new NioEventLoopGroup(
        conf.getInt(RPC_MAX_THREADS),
        Utils.newDaemonThreadFactory("RSCClient-" + executorGroupId + "-%d"));
    this.serverUriPromise = ImmediateEventExecutor.INSTANCE.newPromise();

    Utils.addListener(this.contextInfoPromise, new FutureListener<ContextInfo>() {
      @Override
      public void onSuccess(ContextInfo info) throws Exception {
        connectToContext(info);
        String url = String.format("rsc://%s:%s@%s:%d",
          info.clientId, info.secret, info.remoteAddress, info.remotePort);
        serverUriPromise.setSuccess(URI.create(url));
      }

      @Override
      public void onFailure(Throwable error) {
        connectionError(error);
        serverUriPromise.setFailure(error);
      }
    });

    isAlive = true;
  }

  public void registerStateListener(SessionStateListener stateListener) {
    this.stateListener = stateListener;
  }

  private synchronized void connectToContext(final ContextInfo info) throws Exception {
    this.contextInfo = info;

    try {
      Promise<Rpc> promise = Rpc.createClient(conf,
        eventLoopGroup,
        info.remoteAddress,
        info.remotePort,
        info.clientId,
        info.secret,
        protocol);
      Utils.addListener(promise, new FutureListener<Rpc>() {
        @Override
        public void onSuccess(Rpc rpc) throws Exception {
          driverRpc.setSuccess(rpc);
          Utils.addListener(rpc.getChannel().closeFuture(), new FutureListener<Void>() {
            @Override
            public void onSuccess(Void unused) {
              if (isAlive) {
                LOG.warn("Client RPC channel closed unexpectedly.");
                try {
                  stop(false);
                } catch (Exception e) { /* stop() itself prints warning. */ }
              }
            }
          });
          LOG.debug("Connected to context {} ({}, {}).", info.clientId,
            rpc.getChannel(), executorGroupId);
        }

        @Override
        public void onFailure(Throwable error) throws Exception {
          driverRpc.setFailure(error);
          connectionError(error);
        }
      });
    } catch (Exception e) {
      connectionError(e);
    }
  }

  private void connectionError(Throwable error) {
    LOG.error("Failed to connect to context.", error);
    try {
      stop(false);
    } catch (Exception e) { /* stop() itself prints warning. */ }
  }

  private <T> io.netty.util.concurrent.Future<T> deferredCall(final Object msg,
      final Class<T> retType) {
    if (driverRpc.isSuccess()) {
      try {
        return driverRpc.get().call(msg, retType);
      } catch (Exception ie) {
        throw Utils.propagate(ie);
      }
    }

    // No driver RPC yet, so install a listener and return a promise that will be ready when
    // the driver is up and the message is actually delivered.
    final Promise<T> promise = eventLoopGroup.next().newPromise();
    final FutureListener<T> callListener = new FutureListener<T>() {
      @Override
      public void onSuccess(T value) throws Exception {
        promise.setSuccess(value);
      }

      @Override
      public void onFailure(Throwable error) throws Exception {
        promise.setFailure(error);
      }
    };

    Utils.addListener(driverRpc, new FutureListener<Rpc>() {
      @Override
      public void onSuccess(Rpc rpc) throws Exception {
        Utils.addListener(rpc.call(msg, retType), callListener);
      }

      @Override
      public void onFailure(Throwable error) throws Exception {
        promise.setFailure(error);
      }
    });
    return promise;
  }

  public Future<URI> getServerUri() {
    return serverUriPromise;
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
        this.contextInfoPromise.cancel(true);

        if (shutdownContext && driverRpc.isSuccess()) {
          protocol.endSession();

          // Because the remote context won't really reply to the end session message -
          // since it closes the channel while handling it, we wait for the RPC's channel
          // to close instead.
          long stopTimeout = conf.getTimeAsMs(CLIENT_SHUTDOWN_TIMEOUT);
          driverRpc.get().getChannel().closeFuture().get(stopTimeout,
            TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        LOG.warn("Exception while waiting for end session reply.", e);
        Utils.propagate(e);
      } finally {
        if (driverRpc.isSuccess()) {
          try {
            driverRpc.get().close();
          } catch (Exception e) {
            LOG.warn("Error stopping RPC.", e);
          }
        }

        // Report failure for all pending jobs, so that clients can react.
        for (Map.Entry<String, JobHandleImpl<?>> e : jobs.entrySet()) {
          LOG.info("Failing pending job {} due to shutdown.", e.getKey());
          e.getValue().setFailure(new IOException("RSCClient instance stopped."));
        }

        eventLoopGroup.shutdownGracefully();
      }
      if (contextInfo != null) {
        LOG.debug("Disconnected from context {}, shutdown = {}.", contextInfo.clientId,
          shutdownContext);
      }
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
    return contextInfo;
  }

  public Future<Integer> submitReplCode(String code) throws Exception {
    return deferredCall(new BaseProtocol.ReplJobRequest(code), Integer.class);
  }

  public void cancelReplCode(int statementId) throws Exception {
    deferredCall(new BaseProtocol.CancelReplJobRequest(statementId), Void.class);
  }

  public Future<ReplJobResults> getReplJobResults(Integer from, Integer size) throws Exception {
    return deferredCall(new BaseProtocol.GetReplJobResults(from, size), ReplJobResults.class);
  }

  public Future<ReplJobResults> getReplJobResults() throws Exception {
    return deferredCall(new BaseProtocol.GetReplJobResults(), ReplJobResults.class);
  }

  private class ClientProtocol extends BaseProtocol {

    <T> JobHandleImpl<T> submit(Job<T> job) {
      final String jobId = UUID.randomUUID().toString();
      Object msg = new JobRequest<T>(jobId, job);

      final Promise<T> promise = eventLoopGroup.next().newPromise();
      final JobHandleImpl<T> handle = new JobHandleImpl<T>(RSCClient.this,
        promise, jobId);
      jobs.put(jobId, handle);

      final io.netty.util.concurrent.Future<Void> rpc = deferredCall(msg, Void.class);
      LOG.debug("Sending JobRequest[{}].", jobId);

      Utils.addListener(rpc, new FutureListener<Void>() {
        @Override
        public void onSuccess(Void unused) throws Exception {
          handle.changeState(JobHandle.State.QUEUED);
        }

        @Override
        public void onFailure(Throwable error) throws Exception {
          error.printStackTrace();
          promise.tryFailure(error);
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

    @SuppressWarnings("unchecked")
    <T> Future<T> run(Job<T> job) {
      return (Future<T>) deferredCall(new SyncJobRequest(job), Object.class);
    }

    String bypass(ByteBuffer serializedJob, boolean sync) {
      String jobId = UUID.randomUUID().toString();
      Object msg = new BypassJobRequest(jobId, BufferUtils.toByteArray(serializedJob), sync);
      deferredCall(msg, Void.class);
      return jobId;
    }

    Future<BypassJobStatus> getBypassJobStatus(String id) {
      return deferredCall(new GetBypassJobStatus(id), BypassJobStatus.class);
    }

    void cancel(String jobId) {
      deferredCall(new CancelJob(jobId), Void.class);
    }

    Future<?> endSession() {
      return deferredCall(new EndSession(), Void.class);
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

    private void handle(ChannelHandlerContext ctx, ReplState msg) {
      LOG.trace("Received repl state for {}", msg.state);

      if (stateListener != null) {
        stateListener.onStateUpdated(msg.state);
      }
    }
  }
}
