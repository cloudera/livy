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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.apache.spark.SparkException;
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

import org.apache.spark.launcher.SparkLauncher;

public class LocalClient implements LivyClient {
  private static final Logger LOG = LoggerFactory.getLogger(LocalClient.class);

  private static final long DEFAULT_SHUTDOWN_TIMEOUT = 10000; // In milliseconds

  private static final String OSX_TEST_OPTS = "SPARK_OSX_TEST_OPTS";
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

  LocalClient(LocalClientFactory factory, LocalConf conf) throws IOException, SparkException {
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
  public void stop() {
    if (isAlive) {
      isAlive = false;
      try {
        protocol.endSession();
      } catch (Exception e) {
        LOG.warn("Exception while waiting for end session reply.", e);
      } finally {
        driverRpc.close();
        factory.unref();
      }
    }

    long endTime = System.currentTimeMillis() + DEFAULT_SHUTDOWN_TIMEOUT;
    try {
      driverThread.join(DEFAULT_SHUTDOWN_TIMEOUT);
    } catch (InterruptedException ie) {
      LOG.debug("Interrupted before driver thread was finished.");
    }
    if (endTime - System.currentTimeMillis() <= 0) {
      LOG.warn("Timed out shutting down remote driver, interrupting...");
      driverThread.interrupt();
    }
  }

  @Override
  public Future<?> addJar(URI uri) {
    return run(new AddJarJob(uri.toString()));
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

  void cancel(String jobId) {
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
            String key = e.getKey();
            if (!key.startsWith("spark.")) {
              key = LocalConf.SPARK_CONF_PREFIX + key;
            }
            args.add("--conf");
            args.add(String.format("%s=%s", key, e.getValue()));
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

      String osxTestOpts = "";
      if (Strings.nullToEmpty(System.getProperty("os.name")).toLowerCase().contains("mac")) {
        osxTestOpts = Strings.nullToEmpty(System.getenv(OSX_TEST_OPTS));
      }

      String driverJavaOpts = Joiner.on(" ").skipNulls().join(
          osxTestOpts, conf.get(DRIVER_OPTS_KEY));
      String executorJavaOpts = Joiner.on(" ").skipNulls().join(
          osxTestOpts, conf.get(EXECUTOR_OPTS_KEY));


      // Create a file with all the job properties to be read by client process. Change the
      // file's permissions so that only the owner can read it. This avoid having the
      // connection secret show up in the child process's command line.
      File properties = File.createTempFile("client-props.", ".properties");
      if (!properties.setReadable(false) || !properties.setReadable(true, true)) {
        throw new IOException("Cannot change permissions of job properties file.");
      }
      properties.deleteOnExit();

      Properties allProps = new Properties();
      // first load the defaults from spark-defaults.conf if available
      try {
        URL sparkDefaultsUrl = Thread.currentThread().getContextClassLoader().getResource("spark-defaults.conf");
        if (sparkDefaultsUrl != null) {
          LOG.info("Loading spark defaults: " + sparkDefaultsUrl);
          allProps.load(new ByteArrayInputStream(Resources.toByteArray(sparkDefaultsUrl)));
        }
      } catch (Exception e) {
        String msg = "Exception trying to load spark-defaults.conf: " + e;
        throw new IOException(msg, e);
      }
      // then load the SparkClientImpl config
      for (Map.Entry<String, String> e : conf) {
        String key = e.getKey();
        if (!key.startsWith("spark.")) {
          key = LocalConf.SPARK_CONF_PREFIX + key;
        }
        allProps.put(key, e.getValue());
      }
      allProps.put(LocalConf.SPARK_CONF_PREFIX + CLIENT_ID.key(), clientId);
      allProps.put(LocalConf.SPARK_CONF_PREFIX + CLIENT_SECRET.key(), secret);
      allProps.put(DRIVER_OPTS_KEY, driverJavaOpts);
      allProps.put(EXECUTOR_OPTS_KEY, executorJavaOpts);

      Writer writer = new OutputStreamWriter(new FileOutputStream(properties), Charsets.UTF_8);
      try {
        allProps.store(writer, "Spark Context configuration");
      } finally {
        writer.close();
      }

      // Define how to pass options to the child process. If launching in client (or local)
      // mode, the driver options need to be passed directly on the command line. Otherwise,
      // SparkSubmit will take care of that for us.
      String master = conf.get("spark.master");
      Preconditions.checkArgument(master != null, "spark.master is not defined.");
      //---Modifying argv to be a map
       HashMap<String,String> argv=new HashMap<String,String>();
      if (sparkHome != null) {
        argv.put("sparkHome",new File(sparkHome, "bin/spark-submit").getAbsolutePath());
      } else {
        LOG.info("No spark.home provided, calling SparkSubmit directly.");
        argv.put("sparkHome",new File(System.getProperty("java.home"), "bin/java").getAbsolutePath());

        if (master.startsWith("local") || master.startsWith("mesos") || master.endsWith("-client") || master.startsWith("spark")) {
          String mem = conf.get("spark.driver.memory");
          if (mem != null) {
            argv.put("DriverMemory",mem);
            argv.put("Xms","-Xms" + mem);
            argv.put("Xmx","-Xmx" + mem);
          }

          String cp = conf.get("spark.driver.extraClassPath");
          if (cp != null) {
            argv.put("ClassPath",cp);
          }

          String libPath = conf.get("spark.driver.extraLibPath");
          if (libPath != null) {
            argv.put("LibraryPath",libPath);
          }

          String extra = conf.get(DRIVER_OPTS_KEY);
          if (extra != null) {
            for (String opt : extra.split("[ ]")) {
              if (!opt.trim().isEmpty()) {
                argv.put("DriverOptsKey",opt.trim());
              }
            }
          }
        }
      }

      if (master.equals("yarn-cluster")) {
        String executorCores = conf.get("spark.executor.cores");
        if (executorCores != null) {
          argv.put("executor-cores",executorCores);
        }

        String executorMemory = conf.get("spark.executor.memory");
        if (executorMemory != null) {
          argv.put("executor-memory",executorMemory);
        }

        String numOfExecutors = conf.get("spark.executor.instances");
        if (numOfExecutors != null) {
          argv.put("num-executors",numOfExecutors);
        }
      }

      //--Check to see if properties file is required for Spark Launcher
      argv.put("class",RemoteDriver.class.getName());

      String jar = "spark-internal";
      String livyJars = conf.get(LIVY_JARS);
      List<String> jars = new ArrayList<>();
      if (livyJars == null) {
        String livyHome = System.getenv("LIVY_HOME");
        Preconditions.checkState(livyHome != null,
          "Need one of LIVY_HOME or %s set.", LIVY_JARS.key());

        File clientJars = new File(livyHome, "client-jars");
        Preconditions.checkState(clientJars.isDirectory(),
          "Cannot find 'client-jars' directory under LIVY_HOME.");

        //List<String> jars = new ArrayList<>();
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
        argv.put("jars",livyJars);
      }

      //LOG.info("Running client driver with argv: {}", Joiner.on(" ").join(argv));
      //-----Comment existing livy launcher
     // final Process child = new ProcessBuilder(argv.toArray(new String[argv.size()])).start();
        final SparkLauncher launcher= new SparkLauncher();
        launcher.setSparkHome(argv.get("sparkHome"));
        launcher.setAppResource(jar);
        launcher.setAppName("ClientID");
        launcher.setMainClass(argv.get("class"));
        launcher.setMaster(master);
        launcher.setConf("spark.driver.memory",argv.get("DriverMemory"));
        launcher.setConf("spark.executor.cores",argv.get("executor-cores"));
        launcher.setConf("spark.executor.memory",argv.get("executor-memory"));
        launcher.setConf("spark.executor.instances",argv.get("num-executors"));
        launcher.setConf("spark.driver.extraClassPath",argv.get("ClassPath"));
        launcher.setConf("spark.driver.extraLibraryPath",argv.get("LibraryPath"));
        for(String filepath : jars) { launcher.addJar(filepath); }
        launcher.addAppArgs("--properties-file" + properties.getAbsolutePath());
        launcher.addAppArgs("--remote-host" + serverAddress);
        launcher.addAppArgs("--remote-port" + serverPort);

        final Process child = launcher.launch();

      int childId = childIdGenerator.incrementAndGet();
      redirect("stdout-redir-" + childId, child.getInputStream());
      redirect("stderr-redir-" + childId, child.getErrorStream());

      runnable = new Runnable() {
        @Override
        public void run() {
          try {
            //modify line below to use spark launcher.waitFor()
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

    private void handle(ChannelHandlerContext ctx, JobMetrics msg) {
      JobHandleImpl<?> handle = jobs.get(msg.jobId);
      if (handle != null) {
        handle.getMetrics().addMetrics(msg.sparkJobId, msg.stageId, msg.taskId, msg.metrics);
      } else {
        LOG.warn("Received metrics for unknown job {}", msg.jobId);
      }
    }

    private void handle(ChannelHandlerContext ctx, JobResult msg) {
      JobHandleImpl<?> handle = jobs.remove(msg.id);
      if (handle != null) {
        LOG.info("Received result for {}", msg.id);
        Throwable error = msg.error != null ? new SparkException(msg.error) : null;
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
