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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermission.*;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Promise;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.client.common.TestUtils;
import com.cloudera.livy.rsc.driver.RSCDriverBootstrapper;
import com.cloudera.livy.rsc.rpc.Rpc;
import com.cloudera.livy.rsc.rpc.RpcDispatcher;
import com.cloudera.livy.rsc.rpc.RpcServer;
import static com.cloudera.livy.rsc.RSCConf.Entry.*;

/**
 * Encapsulates code needed to launch a new Spark context and collect information about how
 * to establish a client connection to it.
 */
class ContextLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(ContextLauncher.class);
  private static final AtomicInteger CHILD_IDS = new AtomicInteger();

  private static final String SPARK_DEPLOY_MODE = "spark.submit.deployMode";
  private static final String SPARK_JARS_KEY = "spark.jars";

  static DriverProcessInfo create(RSCClientFactory factory, RSCConf conf)
      throws IOException {
    ContextLauncher launcher = new ContextLauncher(factory, conf);
    return new DriverProcessInfo(launcher.promise, launcher.child.child);
  }

  private final Promise<ContextInfo> promise;
  private final ScheduledFuture<?> timeout;
  private final String clientId;
  private final String secret;
  private final ChildProcess child;
  private final RSCConf conf;
  private final RSCClientFactory factory;

  private ContextLauncher(RSCClientFactory factory, RSCConf conf) throws IOException {
    this.promise = factory.getServer().getEventLoopGroup().next().newPromise();
    this.clientId = UUID.randomUUID().toString();
    this.secret = factory.getServer().createSecret();
    this.conf = conf;
    this.factory = factory;

    final RegistrationHandler handler = new RegistrationHandler();
    try {
      factory.getServer().registerClient(clientId, secret, handler);
      String replMode = conf.get("repl");
      boolean repl = replMode != null && replMode.equals("true");

      conf.set(LAUNCHER_ADDRESS, factory.getServer().getAddress());
      conf.set(LAUNCHER_PORT, factory.getServer().getPort());
      conf.set(CLIENT_ID, clientId);
      conf.set(CLIENT_SECRET, secret);

      Utils.addListener(promise, new FutureListener<ContextInfo>() {
        @Override
        public void onFailure(Throwable error) throws Exception {
          // If promise is cancelled or failed, make sure spark-submit is not leaked.
          if (child != null) {
            child.kill();
          }
        }
      });

      this.child = startDriver(conf, promise);

      // Set up a timeout to fail the promise if we don't hear back from the context
      // after a configurable timeout.
      Runnable timeoutTask = new Runnable() {
        @Override
        public void run() {
          connectTimeout(handler);
        }
      };
      this.timeout = factory.getServer().getEventLoopGroup().schedule(timeoutTask,
        conf.getTimeAsMs(RPC_CLIENT_HANDSHAKE_TIMEOUT), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error("exception: ", e);
      dispose(true);
      throw Utils.propagate(e);
    }
  }

  private void connectTimeout(RegistrationHandler handler) {
    if (promise.tryFailure(new TimeoutException("Timed out waiting for context to start."))) {
      handler.dispose();
    }
    dispose(true);
  }

  private void dispose(boolean forceKill) {
    factory.getServer().unregisterClient(clientId);
    try {
      if (child != null) {
        if (forceKill) {
          child.kill();
        } else {
          child.detach();
        }
      }
    } finally {
      factory.unref();
    }
  }

  private static ChildProcess startDriver(final RSCConf conf, Promise<?> promise)
      throws IOException {
    String livyJars = conf.get(LIVY_JARS);
    if (livyJars == null) {
      String livyHome = System.getenv("LIVY_HOME");
      Utils.checkState(livyHome != null,
        "Need one of LIVY_HOME or %s set.", LIVY_JARS.key());
      File rscJars = new File(livyHome, "rsc-jars");
      if (!rscJars.isDirectory()) {
        rscJars = new File(livyHome, "rsc/target/jars");
      }
      Utils.checkState(rscJars.isDirectory(),
        "Cannot find 'client-jars' directory under LIVY_HOME.");
      List<String> jars = new ArrayList<>();
      for (File f : rscJars.listFiles()) {
         jars.add(f.getAbsolutePath());
      }
      livyJars = Utils.join(jars, ",");
    }
    merge(conf, SPARK_JARS_KEY, livyJars, ",");

    // Disable multiple attempts since the RPC server doesn't yet support multiple
    // connections for the same registered app.
    conf.set("spark.yarn.maxAppAttempts", "1");

    // Let the launcher go away when launcher in yarn cluster mode. This avoids keeping lots
    // of "small" Java processes lingering on the Livy server node.
    conf.set("spark.yarn.submit.waitAppCompletion", "false");

    if (!conf.getBoolean(CLIENT_IN_PROCESS) &&
        // For tests which doesn't shutdown RscDriver gracefully, JaCoCo exec isn't dumped properly.
        // Disable JaCoCo for this case.
        !conf.getBoolean(TEST_STUCK_END_SESSION)) {
      // For testing; propagate jacoco settings so that we also do coverage analysis
      // on the launched driver. We replace the name of the main file ("main.exec")
      // so that we don't end up fighting with the main test launcher.
      String jacocoArgs = TestUtils.getJacocoArgs();
      if (jacocoArgs != null) {
        merge(conf, SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, jacocoArgs, " ");
      }
    }

    final File confFile = writeConfToFile(conf);

    if (ContextLauncher.mockSparkSubmit != null) {
      LOG.warn("!!!! Using mock spark-submit. !!!!");
      return new ChildProcess(conf, promise, ContextLauncher.mockSparkSubmit, confFile);
    } else if (conf.getBoolean(CLIENT_IN_PROCESS)) {
      // Mostly for testing things quickly. Do not do this in production.
      LOG.warn("!!!! Running remote driver in-process. !!!!");
      Runnable child = new Runnable() {
        @Override
        public void run() {
          try {
            RSCDriverBootstrapper.main(new String[] { confFile.getAbsolutePath() });
          } catch (Exception e) {
            throw Utils.propagate(e);
          }
        }
      };
      return new ChildProcess(conf, promise, child, confFile);
    } else {
      final SparkLauncher launcher = new SparkLauncher();

      // Spark 1.x does not support specifying deploy mode in conf and needs special handling.
      String deployMode = conf.get(SPARK_DEPLOY_MODE);
      if (deployMode != null) {
        launcher.setDeployMode(deployMode);
      }

      launcher.setSparkHome(conf.get(SPARK_HOME));
      launcher.setAppResource("spark-internal");
      launcher.setPropertiesFile(confFile.getAbsolutePath());
      launcher.setMainClass(RSCDriverBootstrapper.class.getName());

      if (conf.get(PROXY_USER) != null) {
        launcher.addSparkArg("--proxy-user", conf.get(PROXY_USER));
      }

      return new ChildProcess(conf, promise, launcher.launch(), confFile);
    }
  }

  private static void merge(RSCConf conf, String key, String livyConf, String sep) {
    String confValue = Utils.join(Arrays.asList(livyConf, conf.get(key)), sep);
    conf.set(key, confValue);
  }

  /**
   * Write the configuration to a file readable only by the process's owner. Livy properties
   * are written with an added prefix so that they can be loaded using SparkConf on the driver
   * side.
   *
   * The default Spark configuration (from either SPARK_HOME or SPARK_CONF_DIR) is merged into
   * the user configuration, so that defaults set by Livy's admin take effect when not overridden
   * by the user.
   */
  private static File writeConfToFile(RSCConf conf) throws IOException {
    Properties confView = new Properties();
    for (Map.Entry<String, String> e : conf) {
      String key = e.getKey();
      if (!key.startsWith(RSCConf.SPARK_CONF_PREFIX)) {
        key = RSCConf.LIVY_SPARK_PREFIX + key;
      }
      confView.setProperty(key, e.getValue());
    }

    // Load the default Spark configuration.
    String confDir = conf.get(SPARK_CONF_DIR);
    if (confDir != null) {
      File sparkDefaults = new File(confDir + File.separator + "spark-defaults.conf");
      if (sparkDefaults.isFile()) {
        Properties sparkConf = new Properties();
        Reader r = new InputStreamReader(new FileInputStream(sparkDefaults), UTF_8);
        try {
          sparkConf.load(r);
        } finally {
          r.close();
        }

        for (String key : sparkConf.stringPropertyNames()) {
          if (!confView.containsKey(key)) {
            confView.put(key, sparkConf.getProperty(key));
          }
        }
      }
    }

    File file = File.createTempFile("livyConf", ".properties");
    Files.setPosixFilePermissions(file.toPath(), EnumSet.of(OWNER_READ, OWNER_WRITE));
    //file.deleteOnExit();

    Writer writer = new OutputStreamWriter(new FileOutputStream(file), UTF_8);
    try {
      confView.store(writer, "Livy App Context Configuration");
    } finally {
      writer.close();
    }

    return file;
  }


  private class RegistrationHandler extends BaseProtocol
    implements RpcServer.ClientCallback {

    volatile RemoteDriverAddress driverAddress;

    private Rpc client;

    @Override
    public RpcDispatcher onNewClient(Rpc client) {
      LOG.debug("New RPC client connected from {}.", client.getChannel());
      this.client = client;
      return this;
    }

    @Override
    public void onSaslComplete(Rpc client) {
    }

    void dispose() {
      if (client != null) {
        client.close();
      }
    }

    private void handle(ChannelHandlerContext ctx, RemoteDriverAddress msg) {
      ContextInfo info = new ContextInfo(msg.host, msg.port, clientId, secret);
      if (promise.trySuccess(info)) {
        timeout.cancel(true);
        LOG.debug("Received driver info for client {}: {}/{}.", client.getChannel(),
          msg.host, msg.port);
      } else {
        LOG.warn("Connection established but promise is already finalized.");
      }

      ctx.executor().submit(new Runnable() {
        @Override
        public void run() {
          dispose();
          ContextLauncher.this.dispose(false);
        }
      });
    }

  }

  private static class ChildProcess {

    private final RSCConf conf;
    private final Promise<?> promise;
    private final Process child;
    private final Thread monitor;
    private final File confFile;

    public ChildProcess(RSCConf conf, Promise<?> promise, Runnable child, File confFile) {
      this.conf = conf;
      this.promise = promise;
      this.monitor = monitor(child, CHILD_IDS.incrementAndGet());
      this.child = null;
      this.confFile = confFile;
    }

    public ChildProcess(RSCConf conf, Promise<?> promise, final Process childProc, File confFile) {
      int childId = CHILD_IDS.incrementAndGet();
      this.conf = conf;
      this.promise = promise;
      this.child = childProc;
      this.confFile = confFile;

      Runnable monitorTask = new Runnable() {
        @Override
        public void run() {
          try {
            int exitCode = child.waitFor();
            if (exitCode != 0) {
              LOG.warn("Child process exited with code {}.", exitCode);
              fail(new IOException(String.format("Child process exited with code %d.", exitCode)));
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
      this.monitor = monitor(monitorTask, childId);
    }

    private void fail(Throwable error) {
      promise.tryFailure(error);
    }

    public void kill() {
      if (child != null) {
        child.destroy();
      }
      monitor.interrupt();
      detach();

      if (!monitor.isAlive()) {
        return;
      }

      // Last ditch effort.
      if (monitor.isAlive()) {
        LOG.warn("Timed out shutting down remote driver, interrupting...");
        monitor.interrupt();
      }
    }

    public void detach() {
      try {
        monitor.join(conf.getTimeAsMs(CLIENT_SHUTDOWN_TIMEOUT));
      } catch (InterruptedException ie) {
        LOG.debug("Interrupted before driver thread was finished.");
      }
    }

    private Thread monitor(final Runnable task, int childId) {
      Runnable wrappedTask = new Runnable() {
        @Override
        public void run() {
          try {
            task.run();
          } finally {
            confFile.delete();
          }
        }
      };
      Thread thread = new Thread(wrappedTask);
      thread.setDaemon(true);
      thread.setName("ContextLauncher-" + childId);
      thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          LOG.warn("Child task threw exception.", e);
          fail(e);
        }
      });
      thread.start();
      return thread;
    }
  }

  // Just for testing.
  static Process mockSparkSubmit;

}
