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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.client.common.Serializer;
import com.cloudera.livy.rsc.BaseProtocol;
import com.cloudera.livy.rsc.BypassJobStatus;
import com.cloudera.livy.rsc.RSCConf;
import com.cloudera.livy.rsc.Utils;
import com.cloudera.livy.rsc.rpc.Rpc;
import com.cloudera.livy.rsc.rpc.RpcDispatcher;
import com.cloudera.livy.rsc.rpc.RpcServer;
import static com.cloudera.livy.rsc.RSCConf.Entry.*;

/**
 * Driver code for the Spark client library.
 */
@Sharable
public class RSCDriver extends BaseProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(RSCDriver.class);

  private final Serializer serializer;
  private final Object jcLock;
  private final Object shutdownLock;
  private final ExecutorService executor;
  private final File localTmpDir;
  // Used to queue up requests while the SparkContext is being created.
  private final List<JobWrapper<?>> jobQueue;
  // Keeps track of connected clients.
  private final Collection<Rpc> clients;

  final Map<String, JobWrapper<?>> activeJobs;
  private final Collection<BypassJobWrapper> bypassJobs;

  private RpcServer server;
  private volatile JobContextImpl jc;
  private volatile boolean running;

  protected final SparkConf conf;
  protected final RSCConf livyConf;

  public RSCDriver(SparkConf conf, RSCConf livyConf) throws Exception {
    Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwx------");
    this.localTmpDir = Files.createTempDirectory("rsc-tmp",
      PosixFilePermissions.asFileAttribute(perms)).toFile();
    this.executor = Executors.newCachedThreadPool();
    this.jobQueue = new LinkedList<>();
    this.clients = new ConcurrentLinkedDeque<>();
    this.serializer = new Serializer();

    this.conf = conf;
    this.livyConf = livyConf;
    this.jcLock = new Object();
    this.shutdownLock = new Object();

    this.activeJobs = new ConcurrentHashMap<>();
    this.bypassJobs = new ConcurrentLinkedDeque<>();
  }

  public final synchronized void shutdown() {
    if (!running) {
      return;
    }

    running = false;
    synchronized (shutdownLock) {
      shutdownLock.notifyAll();
    }
    synchronized (jcLock) {
      jcLock.notifyAll();
    }
  }

  private void initializeServer() throws Exception {
    String clientId = livyConf.get(CLIENT_ID);
    Utils.checkArgument(clientId != null, "No client ID provided.");
    String secret = livyConf.get(CLIENT_SECRET);
    Utils.checkArgument(secret != null, "No secret provided.");

    String launcherAddress = livyConf.get(LAUNCHER_ADDRESS);
    Utils.checkArgument(launcherAddress != null, "Missing launcher address.");
    int launcherPort = livyConf.getInt(LAUNCHER_PORT);
    Utils.checkArgument(launcherPort > 0, "Missing launcher port.");

    LOG.info("Connecting to: {}:{}", launcherAddress, launcherPort);

    // We need to unset this configuration since it doesn't really apply for the driver side.
    // If the driver runs on a multi-homed machine, this can lead to issues where the Livy
    // server cannot connect to the auto-detected address, but since the driver can run anywhere
    // on the cluster, it would be tricky to solve that problem in a generic way.
    livyConf.set(RPC_SERVER_ADDRESS, null);

    // Bring up the RpcServer an register the secret provided by the Livy server as a client.
    LOG.info("Starting RPC server...");
    this.server = new RpcServer(livyConf);
    server.registerClient(clientId, secret, new RpcServer.ClientCallback() {
      @Override
      public RpcDispatcher onNewClient(Rpc client) {
        clients.add(client);
        client.addListener(new Rpc.Listener() {
          @Override
          public void rpcClosed(Rpc rpc) {
            clients.remove(rpc);
          }
        });
        LOG.debug("Registered new connection from {}.", client.getChannel());
        return RSCDriver.this;
      }
    });

    // The RPC library takes care of timing out this.
    Rpc callbackRpc = Rpc.createClient(livyConf, server.getEventLoopGroup(),
      launcherAddress, launcherPort, clientId, secret, this).get();
    try {
      // There's no timeout here because we expect the launching side to kill the underlying
      // application if it takes too long to connect back and send its address.
      callbackRpc.call(new RemoteDriverAddress(server.getAddress(), server.getPort())).get();
    } finally {
      callbackRpc.close();
    }
  }

  /**
   * Initializes the SparkContext used by this driver. This implementation creates a
   * context with the provided configuration. Subclasses can override this behavior,
   * and returning a null context is allowed. In that case, the context exposed by
   * JobContext will be null.
   */
  protected JavaSparkContext initializeContext() throws Exception {
    long t1 = System.nanoTime();
    LOG.info("Starting Spark context...");
    JavaSparkContext sc = new JavaSparkContext(conf);
    LOG.info("Spark context finished initialization in {}ms",
      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1));
    return sc;
  }

  /**
   * Called to shut down the driver; any initialization done by initializeContext() should
   * be undone here. This is guaranteed to be called only once.
   */
  protected void shutdownContext() {
    if (jc != null) {
      jc.stop();
    }
    executor.shutdownNow();
    try {
      FileUtils.deleteDirectory(localTmpDir);
    } catch (IOException e) {
      LOG.warn("Failed to delete local tmp dir: " + localTmpDir, e);
    }
  }

  private void shutdownServer() {
    if (server != null) {
      server.close();
    }
  }

  private void broadcast(Object msg) {
    for (Rpc client : clients) {
      try {
        client.call(msg);
      } catch (Exception e) {
        LOG.warn("Failed to send message to client " + client, e);
      }
    }
  }

  void run() throws Exception {
    this.running = true;

    // Set up a class loader that can be modified, so that we can add jars uploaded
    // by the client to the driver's class path.
    ClassLoader driverClassLoader = new MutableClassLoader(
      Thread.currentThread().getContextClassLoader());
    Thread.currentThread().setContextClassLoader(driverClassLoader);

    try {
      initializeServer();

      JavaSparkContext sc = initializeContext();
      synchronized (jcLock) {
        jc = new JobContextImpl(sc, localTmpDir);
        jcLock.notifyAll();
      }

      synchronized (jcLock) {
        for (JobWrapper<?> job : jobQueue) {
          submit(job);
        }
        jobQueue.clear();
      }

      synchronized (shutdownLock) {
        try {
          while (running) {
            shutdownLock.wait();
          }
        } catch (InterruptedException ie) {
          // Nothing to do.
        }
      }
    } finally {
      // Cancel any pending jobs.
      for (JobWrapper<?> job : activeJobs.values()) {
        job.cancel();
      }

      try {
        shutdownContext();
      } catch (Exception e) {
        LOG.warn("Error during shutdown.", e);
      }
      try {
        shutdownServer();
      } catch (Exception e) {
        LOG.warn("Error during shutdown.", e);
      }
    }
  }

  public void submit(JobWrapper<?> job) {
    if (jc != null) {
      job.submit(executor);
      return;
    }
    synchronized (jcLock) {
      if (jc != null) {
        job.submit(executor);
      } else {
        LOG.info("SparkContext not yet up, queueing job request.");
        jobQueue.add(job);
      }
    }
  }

  JobContextImpl jobContext() {
    return jc;
  }

  Serializer serializer() {
    return serializer;
  }

  <T> void jobFinished(String jobId, T result, Throwable error) {
    LOG.debug("Send job({}) result to Client.", jobId);
    broadcast(new JobResult<T>(jobId, result, error));
  }

  void jobStarted(String jobId) {
    broadcast(new JobStarted(jobId));
  }

  public void handle(ChannelHandlerContext ctx, CancelJob msg) {
    JobWrapper<?> job = activeJobs.get(msg.id);
    if (job == null || !job.cancel()) {
      LOG.info("Requested to cancel an already finished job.");
    }
  }

  public void handle(ChannelHandlerContext ctx, EndSession msg) {
    LOG.debug("Shutting down due to EndSession request.");
    shutdown();
  }

  public void handle(ChannelHandlerContext ctx, JobRequest<?> msg) {
    LOG.info("Received job request {}", msg.id);
    JobWrapper<?> wrapper = new JobWrapper<>(this, msg.id, msg.job);
    activeJobs.put(msg.id, wrapper);
    submit(wrapper);
  }

  public void handle(ChannelHandlerContext ctx, BypassJobRequest msg) throws Exception {
    LOG.info("Received bypass job request {}", msg.id);
    BypassJobWrapper wrapper = new BypassJobWrapper(this, msg.id, msg.serializedJob);
    bypassJobs.add(wrapper);
    activeJobs.put(msg.id, wrapper);
    if (msg.synchronous) {
      waitForJobContext();
      try {
        wrapper.call();
      } catch (Throwable t) {
        // Wrapper already logged and saved the exception, just avoid it bubbling up
        // to the RPC layer.
      }
    } else {
      submit(wrapper);
    }
  }

  @SuppressWarnings("unchecked")
  public Object handle(ChannelHandlerContext ctx, SyncJobRequest msg) throws Exception {
    waitForJobContext();
    return msg.job.call(jc);
  }

  public BypassJobStatus handle(ChannelHandlerContext ctx, GetBypassJobStatus msg) {
    for (Iterator<BypassJobWrapper> it = bypassJobs.iterator(); it.hasNext();) {
      BypassJobWrapper job = it.next();
      if (job.jobId.equals(msg.id)) {
        BypassJobStatus status = job.getStatus();
        switch (status.state) {
          case CANCELLED:
          case FAILED:
          case SUCCEEDED:
            it.remove();
            break;

          default:
            // No-op.
        }
        return status;
      }
    }

    throw new NoSuchElementException(msg.id);
  }

  private void waitForJobContext() throws InterruptedException {
    synchronized (jcLock) {
      while (jc == null) {
        jcLock.wait();
        if (!running) {
          throw new IllegalStateException("Remote context is shutting down.");
        }
      }
    }
  }

}

