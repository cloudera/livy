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

package com.cloudera.livy.client.local;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.client.common.BufferUtils;
import com.cloudera.livy.client.local.rpc.Rpc;
import static com.cloudera.livy.client.local.LocalConf.Entry.*;

public class LocalClient implements LivyClient {
  private static final Logger LOG = LoggerFactory.getLogger(LocalClient.class);

  private final ContextInfo ctx;
  private final LocalClientFactory factory;
  private final LocalConf conf;
  private final Map<String, JobHandleImpl<?>> jobs;
  public final Rpc driverRpc;
  private final ClientProtocol protocol;
  private final EventLoopGroup eventLoopGroup;
  private volatile boolean isAlive;
  private final boolean repl;

  LocalClient(LocalClientFactory factory, LocalConf conf, ContextInfo ctx) throws IOException {
    this.ctx = ctx;
    this.factory = factory;
    this.conf = conf;
    this.jobs = Maps.newConcurrentMap();
    this.protocol = new ClientProtocol();
    this.eventLoopGroup = new NioEventLoopGroup(
        conf.getInt(RPC_MAX_THREADS),
        new ThreadFactoryBuilder()
            .setNameFormat("Client-RPC-Handler-" + ctx.getClientId() + "-%d")
            .setDaemon(true)
            .build());
    String replMode = conf.get("repl");
    this.repl = replMode != null && replMode.equals("true");

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
      throw Throwables.propagate(e);
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
    return run(new AddJarJob(uri.toString()));
  }

  @Override
  public Future<?> uploadFile(File file) {
    throw new UnsupportedOperationException("Use addFile to add the file to the remote context!");
  }

  @Override
  public Future<?> addFile(URI uri) {
    return run(new AddFileJob(uri.toString()));
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

  private Thread startDriver(final RpcServer rpcServer, final String clientId, final String secret)
      throws IOException {
    Runnable runnable;
    final String serverAddress = rpcServer.getAddress();
    final String serverPort = String.valueOf(rpcServer.getPort());

    if (conf.get(CLIENT_IN_PROCESS) != null) {
      // Mostly for testing things quickly. Do not do this in production.
      LOG.warn("!!!! Running remote driver in-process. !!!!");
      runnable = new Runnable() {
        @Override
        public void run() {
          List<String> args = Lists.newArrayList();
          args.add("--remote-host");
          args.add(serverAddress);
          args.add("--remote-port");
          args.add(serverPort);
          args.add("--client-id");
          args.add(clientId);
          args.add("--secret");
          args.add(secret);

          for (Map.Entry<String, String> e : conf) {
            args.add("--conf");
            args.add(String.format("%s=%s", e.getKey(), e.getValue()));
          }
          try {
            RemoteDriver.main(args.toArray(new String[args.size()]));
          } catch (Exception e) {
            LOG.error("Error running driver.", e);
          }
        }
      };
    } else {
      // If a Spark installation is provided, use the spark-submit script. Otherwise, call the
      // SparkSubmit class directly, which has some caveats (like having to provide a proper
      // version of Guava on the classpath depending on the deploy mode).
      String sparkHome = conf.get(SPARK_HOME_KEY);
      if (sparkHome == null) {
        sparkHome = System.getenv(SPARK_HOME_ENV);
      }
      if (sparkHome == null) {
        sparkHome = System.getProperty(SPARK_HOME_KEY);
      }

      conf.set(CLIENT_ID, clientId);
      conf.set(CLIENT_SECRET, secret);

      // Disable multiple attempts since the RPC server doesn't yet support multiple
      // connections for the same registered app.
      conf.set("spark.yarn.maxAppAttempts", "1");

      File confFile = writeConfToFile();

      // Define how to pass options to the child process. If launching in client (or local)
      // mode, the driver options need to be passed directly on the command line. Otherwise,
      // SparkSubmit will take care of that for us.
      String master = conf.get("spark.master");
      Preconditions.checkArgument(master != null, "spark.master is not defined.");

      List<String> argv = Lists.newArrayList();

      if (sparkHome != null) {
        argv.add(new File(sparkHome, "bin/spark-submit").getAbsolutePath());
      } else {
        LOG.info("No spark.home provided, calling SparkSubmit directly.");
        argv.add(new File(System.getProperty("java.home"), "bin/java").getAbsolutePath());

        if (master.startsWith("local") ||
              master.startsWith("mesos") ||
              master.endsWith("-client") ||
              master.startsWith("spark")) {
          String mem = conf.get("spark.driver.memory");
          if (mem != null) {
            argv.add("-Xms" + mem);
            argv.add("-Xmx" + mem);
          }

          String cp = conf.get("spark.driver.extraClassPath");
          if (cp != null) {
            argv.add("-classpath");
            argv.add(cp);
          }

          String libPath = conf.get("spark.driver.extraLibPath");
          if (libPath != null) {
            argv.add("-Djava.library.path=" + libPath);
          }

          String extra = conf.get(DRIVER_OPTS_KEY);
          if (extra != null) {
            for (String opt : extra.split("[ ]")) {
              if (!opt.trim().isEmpty()) {
                argv.add(opt.trim());
              }
            }
          }
        }

        argv.add("org.apache.spark.deploy.SparkSubmit");
      }

      if (master.equals("yarn-cluster")) {
        String executorCores = conf.get("spark.executor.cores");
        if (executorCores != null) {
          argv.add("--executor-cores");
          argv.add(executorCores);
        }

        String executorMemory = conf.get("spark.executor.memory");
        if (executorMemory != null) {
          argv.add("--executor-memory");
          argv.add(executorMemory);
        }

        String numOfExecutors = conf.get("spark.executor.instances");
        if (numOfExecutors != null) {
          argv.add("--num-executors");
          argv.add(numOfExecutors);
        }
      }

      argv.add("--properties-file");
      argv.add(confFile.getAbsolutePath());
      argv.add("--class");
      String className;
      if (repl) {
        className = REPL.class.getName();
      } else {
        className = RemoteDriver.class.getName();
      }
      argv.add(className);

      if (conf.get(PROXY_USER) != null) {
        argv.add("--proxy-user");
        argv.add(conf.get(PROXY_USER));
      }

      String jar = "spark-internal";
      String livyJars = conf.get(LIVY_JARS);
      if (livyJars == null) {
        String livyHome = System.getenv("LIVY_HOME");
        Preconditions.checkState(livyHome != null,
          "Need one of LIVY_HOME or %s set.", LIVY_JARS.key());

        File clientJars = new File(livyHome, "client-jars");
        Preconditions.checkState(clientJars.isDirectory(),
          "Cannot find 'client-jars' directory under LIVY_HOME.");

        List<String> jars = new ArrayList<>();
        for (File f : clientJars.listFiles()) {
          jars.add(f.getAbsolutePath());
        }
        livyJars = Joiner.on(",").join(jars);
      }

      String userJars = conf.get(SPARK_JARS_KEY);
      if (userJars != null) {
        String allJars = Joiner.on(",").join(livyJars, userJars);
        conf.set(SPARK_JARS_KEY, allJars);
      } else {
        argv.add("--jars");
        argv.add(livyJars);
      }

      argv.add(jar);
      argv.add("--remote-host");
      argv.add(serverAddress);
      argv.add("--remote-port");
      argv.add(serverPort);

      LOG.info("Running client driver with argv: {}", Joiner.on(" ").join(argv));
      final Process child = new ProcessBuilder(argv.toArray(new String[argv.size()])).start();

      int childId = childIdGenerator.incrementAndGet();
      redirect("stdout-redir-" + childId, child.getInputStream());
      redirect("stderr-redir-" + childId, child.getErrorStream());

      runnable = new Runnable() {
        @Override
        public void run() {
          try {
            int exitCode = child.waitFor();
            if (exitCode != 0) {
              rpcServer.cancelClient(clientId, "Child process exited before connecting back");
              LOG.warn("Child process exited with code {}.", exitCode);
            }
          } catch (InterruptedException ie) {
            LOG.warn("Waiting thread interrupted, killing child process.");
            Thread.interrupted();
            child.destroy();
          } catch (Exception e) {
            LOG.warn("Exception while waiting for child process.", e);
          }
        }
      };
    }

    Thread thread = new Thread(runnable);
    thread.setDaemon(true);
    thread.setName("Driver");
    thread.start();
    return thread;
  @VisibleForTesting
  ContextInfo getContextInfo() {
    return ctx;
  private Thread startDriver(final RpcServer rpcServer, final String clientId, final String secret)
      throws IOException {
    Runnable runnable;
    final String serverAddress = rpcServer.getAddress();
    final String serverPort = String.valueOf(rpcServer.getPort());

    if (conf.get(CLIENT_IN_PROCESS) != null) {
      // Mostly for testing things quickly. Do not do this in production.
      LOG.warn("!!!! Running remote driver in-process. !!!!");
      runnable = new Runnable() {
        @Override
        public void run() {
          List<String> args = Lists.newArrayList();
          args.add("--remote-host");
          args.add(serverAddress);
          args.add("--remote-port");
          args.add(serverPort);
          args.add("--client-id");
          args.add(clientId);
          args.add("--secret");
          args.add(secret);

          for (Map.Entry<String, String> e : conf) {
            args.add("--conf");
            args.add(String.format("%s=%s", e.getKey(), e.getValue()));
          }
          try {
            RemoteDriver.main(args.toArray(new String[args.size()]));
          } catch (Exception e) {
            LOG.error("Error running driver.", e);
          }
        }
      };
    } else {
      // If a Spark installation is provided, use the spark-submit script. Otherwise, call the
      // SparkSubmit class directly, which has some caveats (like having to provide a proper
      // version of Guava on the classpath depending on the deploy mode).
      String sparkHome = conf.get(SPARK_HOME_KEY);
      if (sparkHome == null) {
        sparkHome = System.getenv(SPARK_HOME_ENV);
      }
      if (sparkHome == null) {
        sparkHome = System.getProperty(SPARK_HOME_KEY);
      }

      conf.set(CLIENT_ID, clientId);
      conf.set(CLIENT_SECRET, secret);

      // Disable multiple attempts since the RPC server doesn't yet support multiple
      // connections for the same registered app.
      conf.set("spark.yarn.maxAppAttempts", "1");

      File confFile = writeConfToFile();

      // Define how to pass options to the child process. If launching in client (or local)
      // mode, the driver options need to be passed directly on the command line. Otherwise,
      // SparkSubmit will take care of that for us.
      String master = conf.get("spark.master");
      Preconditions.checkArgument(master != null, "spark.master is not defined.");

      List<String> argv = Lists.newArrayList();

      if (sparkHome != null) {
        argv.add(new File(sparkHome, "bin/spark-submit").getAbsolutePath());
      } else {
        LOG.info("No spark.home provided, calling SparkSubmit directly.");
        argv.add(new File(System.getProperty("java.home"), "bin/java").getAbsolutePath());

        if (master.startsWith("local") ||
              master.startsWith("mesos") ||
              master.endsWith("-client") ||
              master.startsWith("spark")) {
          String mem = conf.get("spark.driver.memory");
          if (mem != null) {
            argv.add("-Xms" + mem);
            argv.add("-Xmx" + mem);
          }

          String cp = conf.get("spark.driver.extraClassPath");
          if (cp != null) {
            argv.add("-classpath");
            argv.add(cp);
          }

          String libPath = conf.get("spark.driver.extraLibPath");
          if (libPath != null) {
            argv.add("-Djava.library.path=" + libPath);
          }

          String extra = conf.get(DRIVER_OPTS_KEY);
          if (extra != null) {
            for (String opt : extra.split("[ ]")) {
              if (!opt.trim().isEmpty()) {
                argv.add(opt.trim());
              }
            }
          }
        }

        argv.add("org.apache.spark.deploy.SparkSubmit");
      }

      if (master.equals("yarn-cluster")) {
        String executorCores = conf.get("spark.executor.cores");
        if (executorCores != null) {
          argv.add("--executor-cores");
          argv.add(executorCores);
        }

        String executorMemory = conf.get("spark.executor.memory");
        if (executorMemory != null) {
          argv.add("--executor-memory");
          argv.add(executorMemory);
        }

        String numOfExecutors = conf.get("spark.executor.instances");
        if (numOfExecutors != null) {
          argv.add("--num-executors");
          argv.add(numOfExecutors);
        }
      }

      argv.add("--properties-file");
      argv.add(confFile.getAbsolutePath());
      argv.add("--class");
      String className;
      if (repl) {
        className = "com.cloudera.livy.repl.REPL";
      } else {
        className = RemoteDriver.class.getName();
      }
      argv.add(className);

      if (conf.get(PROXY_USER) != null) {
        argv.add("--proxy-user");
        argv.add(conf.get(PROXY_USER));
      }

      String jar = "spark-internal";
      String livyJars = conf.get(LIVY_JARS);
      if (livyJars == null) {
        String livyHome = System.getenv("LIVY_HOME");
        Preconditions.checkState(livyHome != null,
          "Need one of LIVY_HOME or %s set.", LIVY_JARS.key());

        File clientJars = new File(livyHome, "client-jars");
        Preconditions.checkState(clientJars.isDirectory(),
          "Cannot find 'client-jars' directory under LIVY_HOME.");

        List<String> jars = new ArrayList<>();
        for (File f : clientJars.listFiles()) {
          jars.add(f.getAbsolutePath());
        }
        livyJars = Joiner.on(",").join(jars);
      }

      String userJars = conf.get(SPARK_JARS_KEY);
      if (userJars != null) {
        String allJars = Joiner.on(",").join(livyJars, userJars);
        conf.set(SPARK_JARS_KEY, allJars);
      } else {
        argv.add("--jars");
        argv.add(livyJars);
      }

      argv.add(jar);
      argv.add("--remote-host");
      argv.add(serverAddress);
      argv.add("--remote-port");
      argv.add(serverPort);

      LOG.info("Running client driver with argv: {}", Joiner.on(" ").join(argv));
      final Process child = new ProcessBuilder(argv.toArray(new String[argv.size()])).start();

      int childId = childIdGenerator.incrementAndGet();
      redirect("stdout-redir-" + childId, child.getInputStream());
      redirect("stderr-redir-" + childId, child.getErrorStream());

      runnable = new Runnable() {
        @Override
        public void run() {
          try {
            int exitCode = child.waitFor();
            if (exitCode != 0) {
              rpcServer.cancelClient(clientId, "Child process exited before connecting back");
              LOG.warn("Child process exited with code {}.", exitCode);
            }
          } catch (InterruptedException ie) {
            LOG.warn("Waiting thread interrupted, killing child process.");
            Thread.interrupted();
            child.destroy();
          } catch (Exception e) {
            LOG.warn("Exception while waiting for child process.", e);
          }
        }
      };
    }

    Thread thread = new Thread(runnable);
    thread.setDaemon(true);
    thread.setName("Driver");
    thread.start();
    return thread;
  }

  private class ClientProtocol extends BaseProtocol {

    <T> JobHandleImpl<T> submit(Job<T> job) {
      final String jobId = UUID.randomUUID().toString();
      Object msg = new JobRequest<T>(jobId, job);

      final Promise<T> promise = driverRpc.createPromise();
      final JobHandleImpl<T> handle = new JobHandleImpl<T>(LocalClient.this,
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

    JsonAST.JValue executeCode(String code) throws Exception {
      return driverRpc.call(new REPLJobRequest(code), JsonAST.JValue.class).get();
    }

    void cancel(String jobId) {
      driverRpc.call(new CancelJob(jobId));
    }

    Future<?> endSession() {
      return driverRpc.call(new EndSession());
    }

    private void handle(ChannelHandlerContext ctx, java.lang.Error msg) {
      LOG.warn("Error reported from remote driver.", msg.getCause());
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

    private void handle(ChannelHandlerContext ctx, JobSubmitted msg) {
      JobHandleImpl<?> handle = jobs.get(msg.clientJobId);
      if (handle != null) {
        LOG.info("Received spark job ID: {} for {}", msg.sparkJobId, msg.clientJobId);
        handle.addSparkJobId(msg.sparkJobId);
      } else {
        LOG.warn("Received spark job ID: {} for unknown job {}", msg.sparkJobId, msg.clientJobId);
      }
    }

  }

  private static class AddJarJob implements Job<Object> {

    private final String path;

    AddJarJob() {
      this(null);
    }

    AddJarJob(String path) {
      this.path = path;
    }

    @Override
    public Object call(JobContext jc) throws Exception {
      jc.sc().addJar(path);
      return null;
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
