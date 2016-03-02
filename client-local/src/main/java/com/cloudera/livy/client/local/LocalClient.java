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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermission.*;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.client.common.BufferUtils;
import com.cloudera.livy.client.local.driver.RemoteDriver;
import com.cloudera.livy.client.local.rpc.Rpc;
import com.cloudera.livy.client.local.rpc.RpcServer;
import static com.cloudera.livy.client.local.LocalConf.Entry.*;

public class LocalClient implements LivyClient {
  private static final Logger LOG = LoggerFactory.getLogger(LocalClient.class);

  private static final long DEFAULT_SHUTDOWN_TIMEOUT = 10000; // In milliseconds

  private static final String SPARK_JARS_KEY = "spark.jars";
  private static final String SPARK_HOME_ENV = "SPARK_HOME";
  private static final String SPARK_HOME_KEY = "spark.home";
  private static final String DRIVER_OPTS_KEY = "spark.driver.extraJavaOptions";
  private static final String EXECUTOR_OPTS_KEY = "spark.executor.extraJavaOptions";

  private final LocalClientFactory factory;
  private final LocalConf conf;
  private final AtomicInteger childIdGenerator;
  private final Thread driverThread;
  private final Map<String, JobHandleImpl<?>> jobs;
  private final Rpc driverRpc;
  private final ClientProtocol protocol;
  private volatile boolean isAlive;

  LocalClient(LocalClientFactory factory, LocalConf conf) throws IOException {
    this.factory = factory;
    this.conf = conf;
    this.childIdGenerator = new AtomicInteger();
    this.jobs = Maps.newConcurrentMap();

    String clientId = UUID.randomUUID().toString();
    String secret = factory.getServer().createSecret();
    this.driverThread = startDriver(factory.getServer(), clientId, secret);
    this.protocol = new ClientProtocol();

    try {
      // The RPC server will take care of timeouts here.
      this.driverRpc = factory.getServer().registerClient(clientId, secret, protocol).get();
    } catch (Throwable e) {
      LOG.warn("Error while waiting for client to connect.", e);
      driverThread.interrupt();
      try {
        driverThread.join();
      } catch (InterruptedException ie) {
        // Give up.
        LOG.debug("Interrupted before driver thread was finished.");
      }
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
  public void stop(boolean shutdownContext) {
    if (!shutdownContext) {
      LOG.warn("shutdownContext=false is not supported for local clients.");
    }
    if (isAlive) {
      isAlive = false;
      try {
        protocol.endSession();

        try {
          driverThread.join(DEFAULT_SHUTDOWN_TIMEOUT);
        } catch (InterruptedException ie) {
          LOG.debug("Interrupted before driver thread was finished.");
        }
        if (driverThread.isAlive()) {
          LOG.warn("Timed out shutting down remote driver, interrupting...");
          driverThread.interrupt();
        }
      } catch (Exception e) {
        LOG.warn("Exception while waiting for end session reply.", e);
      } finally {
        driverRpc.close();
        factory.unref();
      }
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
      final SparkLauncher launcher = new SparkLauncher();
      String sparkHome = conf.get(SPARK_HOME_KEY);
      if (sparkHome == null) {
        sparkHome = System.getenv(SPARK_HOME_ENV);
      }
      if (sparkHome == null) {
        sparkHome = System.getProperty(SPARK_HOME_KEY);
      }
      launcher.setSparkHome(sparkHome);

      conf.set(CLIENT_ID, clientId);
      conf.set(CLIENT_SECRET, secret);

      launcher.setAppResource("spark-internal");
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
           launcher.addJar(f.getAbsolutePath());
        }
        livyJars = Joiner.on(",").join(jars);
      }
      String userJars = conf.get(SPARK_JARS_KEY);
      if (userJars != null) {
        String allJars = Joiner.on(",").join(livyJars, userJars);
        conf.set(SPARK_JARS_KEY, allJars);
      }

      // Disable multiple attempts since the RPC server doesn't yet support multiple
      // connections for the same registered app.
      conf.set("spark.yarn.maxAppAttempts", "1");

      File confFile = writeConfToFile();

      // Define how to pass options to the child process. If launching in client (or local)
      // mode, the driver options need to be passed directly on the command line. Otherwise,
      // SparkSubmit will take care of that for us.
      String master = conf.get("spark.master");
      Preconditions.checkArgument(master != null, "spark.master is not defined.");
      launcher.setMaster(master);
      launcher.setPropertiesFile(confFile.getAbsolutePath());
      launcher.setMainClass(RemoteDriver.class.getName());
      if (conf.get(PROXY_USER) != null) {
        launcher.addSparkArg("--proxy-user", conf.get(PROXY_USER));
      }
      launcher.addAppArgs("--remote-host", serverAddress);
      launcher.addAppArgs("--remote-port",  serverPort);

      final Process child = launcher.launch();
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

  private void redirect(String name, InputStream in) {
    Thread thread = new Thread(new Redirector(in));
    thread.setName(name);
    thread.setDaemon(true);
    thread.start();
  }

  /**
   * Write the configuration to a file readable only by the process's owner. Livy properties
   * are written with an added prefix so that they can be loaded using SparkConf on the driver
   * side.
   */
  private File writeConfToFile() throws IOException {
    Properties confView = new Properties();
    for (Map.Entry<String, String> e : conf) {
      String key = e.getKey();
      if (!key.startsWith(LocalConf.SPARK_CONF_PREFIX)) {
        key = LocalConf.LIVY_SPARK_PREFIX + key;
      }
      confView.setProperty(key, e.getValue());
    }

    File file = File.createTempFile("livyConf", ".properties");
    Files.setPosixFilePermissions(file.toPath(), EnumSet.of(OWNER_READ, OWNER_WRITE));
    file.deleteOnExit();

    Writer writer = new OutputStreamWriter(new FileOutputStream(file), UTF_8);
    try {
      confView.store(writer, "Livy App Context Configuration");
    } finally {
      writer.close();
    }

    return file;
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

  private class Redirector implements Runnable {

    private final BufferedReader in;

    Redirector(InputStream in) {
      this.in = new BufferedReader(new InputStreamReader(in));
    }

    @Override
    public void run() {
      try {
        String line = null;
        while ((line = in.readLine()) != null) {
          LOG.info(line);
        }
      } catch (Exception e) {
        LOG.warn("Error in redirector thread.", e);
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
