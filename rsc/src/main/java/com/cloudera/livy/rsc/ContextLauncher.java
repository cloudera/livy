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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermission.*;

import io.netty.channel.ChannelHandlerContext;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.rsc.driver.RSCDriverBootstrapper;
import com.cloudera.livy.rsc.rpc.Rpc;
import com.cloudera.livy.rsc.rpc.RpcDispatcher;
import com.cloudera.livy.rsc.rpc.RpcServer;
import static com.cloudera.livy.rsc.RSCConf.Entry.*;

/**
 * Encapsulates code needed to launch a new Spark context and collect information about how
 * to establish a client connection to it.
 */
class ContextLauncher implements ContextInfo {

  private static final Logger LOG = LoggerFactory.getLogger(ContextLauncher.class);
  private static final AtomicInteger CHILD_IDS = new AtomicInteger();

  private static final String SPARK_JARS_KEY = "spark.jars";
  private static final String SPARK_ARCHIVES_KEY = "spark.yarn.dist.archives";
  private static final String SPARK_HOME_ENV = "SPARK_HOME";
  private static final String SPARK_HOME_KEY = "spark.home";

  private final String clientId;
  private final String secret;
  private final ChildProcess child;
  private final RSCConf conf;
  private final RSCClientFactory factory;

  private BaseProtocol.RemoteDriverAddress driverAddress;

  ContextLauncher(RSCClientFactory factory, RSCConf conf) throws IOException {
    this.clientId = UUID.randomUUID().toString();
    this.secret = factory.getServer().createSecret();
    this.conf = conf;
    this.factory = factory;

    RegistrationHandler handler = new RegistrationHandler();
    try {
      factory.getServer().registerClient(clientId, secret, handler);
      String replMode = conf.get("repl");
      boolean repl = replMode != null && replMode.equals("true");
      this.child = startDriver(factory.getServer(), conf, clientId, secret);

      // Wait for the handler to receive the driver information. Wait a little at a time so
      // that we can check whether the child process is still alive, and throw an error if the
      // process goes away before we get the information back.
      long timeout = conf.getTimeAsMs(RPC_CLIENT_HANDSHAKE_TIMEOUT);
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
      long now = System.nanoTime();
      long step = Math.min(500, TimeUnit.NANOSECONDS.toMillis(deadline - now));
      synchronized (handler) {
        while (handler.driverAddress == null && now < deadline) {
          handler.wait(Math.min(step, deadline - now));
          Utils.checkState(handler.driverAddress != null || !child.isFailed(),
            "Child has exited before address information was received.");
          now = System.nanoTime();
        }
      }
      Utils.checkState(handler.driverAddress != null,
        "Timed out waiting for driver connection information.");
      driverAddress = handler.driverAddress;
    } catch (Exception e) {
      factory.getServer().unregisterClient(clientId);
      throw Utils.propagate(e);
    } finally {
      handler.dispose();
    }
  }

  @Override
  public String getRemoteAddress() {
    return driverAddress.host;
  }

  @Override
  public int getRemotePort() {
    return driverAddress.port;
  }

  @Override
  public String getClientId() {
    return clientId;
  }

  @Override
  public String getSecret() {
    return secret;
  }

  @Override
  public void dispose(boolean forceKill) {
    try {
      if (forceKill) {
        child.kill();
      } else {
        child.detach();
      }
    } finally {
      factory.unref();
    }
  }

  private static ChildProcess startDriver(
      final RpcServer rpcServer,
      final RSCConf conf,
      final String clientId,
      final String secret) throws IOException {
    // Write out the config file used by the remote context.
    conf.set(LAUNCHER_ADDRESS, rpcServer.getAddress());
    conf.set(LAUNCHER_PORT, rpcServer.getPort());
    conf.set(CLIENT_ID, clientId);
    conf.set(CLIENT_SECRET, secret);

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

    if ("sparkr".equals(conf.get("session.kind"))) {
      merge(conf, SPARK_ARCHIVES_KEY, conf.get(RSCConf.Entry.SPARKR_PACKAGE), ",");
    }

    // Disable multiple attempts since the RPC server doesn't yet support multiple
    // connections for the same registered app.
    conf.set("spark.yarn.maxAppAttempts", "1");

    // Let the launcher go away when launcher in yarn cluster mode. This avoids keeping lots
    // of "small" Java processes lingering on the Livy server node.
    conf.set("spark.yarn.submit.waitAppCompletion", "false");

    // For testing; propagate jacoco settings so that we also do coverage analysis
    // on the launched driver. We replace the name of the main file ("main.exec")
    // so that we don't end up fighting with the main test launcher.
    String jacocoArgs = System.getProperty("jacoco.args");
    if (jacocoArgs != null) {
      jacocoArgs = jacocoArgs.replace("main.exec", "child.exec");
      merge(conf, SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, jacocoArgs, " ");
    }

    final File confFile = writeConfToFile(conf);

    if (conf.get(CLIENT_IN_PROCESS) != null) {
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
      return new ChildProcess(conf, child, confFile);
    } else {
      final SparkLauncher launcher = new SparkLauncher();
      String sparkHome = conf.get(SPARK_HOME_KEY);
      if (sparkHome == null) {
        sparkHome = System.getenv(SPARK_HOME_ENV);
      }

      if (sparkHome == null) {
        sparkHome = System.getProperty(SPARK_HOME_KEY);
      }
      launcher.setSparkHome(sparkHome);

      launcher.setAppResource("spark-internal");

      // Define how to pass options to the child process. If launching in client (or local)
      // mode, the driver options need to be passed directly on the command line. Otherwise,
      // SparkSubmit will take care of that for us.
      String master = conf.get("spark.master");
      Utils.checkArgument(master != null, "spark.master is not defined.");
      launcher.setMaster(master);
      launcher.setPropertiesFile(confFile.getAbsolutePath());
      launcher.setMainClass(RSCDriverBootstrapper.class.getName());
      if (conf.get(PROXY_USER) != null) {
        launcher.addSparkArg("--proxy-user", conf.get(PROXY_USER));
      }

      return new ChildProcess(conf, launcher.launch(), confFile);
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

  private static class Redirector implements Runnable {

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

      try {
        in.close();
      } catch (IOException ioe) {
        LOG.warn("Error closing child stream.", ioe);
      }
    }

  }

  private static class RegistrationHandler extends BaseProtocol
    implements RpcServer.ClientCallback {

    volatile RemoteDriverAddress driverAddress;

    private Rpc client;

    @Override
    public RpcDispatcher onNewClient(Rpc client) {
      LOG.debug("New RPC client connected from {}.", client.getChannel());
      this.client = client;
      return this;
    }

    void dispose() {
      if (client != null) {
        client.close();
      }
    }

    private void handle(ChannelHandlerContext ctx, RemoteDriverAddress msg) {
      LOG.debug("Received driver info for client {}: {}/{}.", client.getChannel(),
        msg.host, msg.port);
      synchronized (this) {
        this.driverAddress = msg;
        notifyAll();
      }
    }

  }

  private static class ChildProcess {

    private final RSCConf conf;
    private final Process child;
    private final Thread monitor;
    private final Thread stdout;
    private final Thread stderr;
    private final File confFile;
    private volatile boolean childFailed;

    public ChildProcess(RSCConf conf, Runnable child, File confFile) {
      this.conf = conf;
      this.monitor = monitor(child, CHILD_IDS.incrementAndGet());
      this.child = null;
      this.stdout = null;
      this.stderr = null;
      this.confFile = confFile;
      this.childFailed = false;
    }

    public ChildProcess(RSCConf conf, final Process childProc, File confFile) {
      int childId = CHILD_IDS.incrementAndGet();
      this.conf = conf;
      this.child = childProc;
      this.stdout = redirect("stdout-redir-" + childId, child.getInputStream());
      this.stderr = redirect("stderr-redir-" + childId, child.getErrorStream());
      this.confFile = confFile;
      this.childFailed = false;

      Runnable monitorTask = new Runnable() {
        @Override
        public void run() {
          try {
            int exitCode = child.waitFor();
            if (exitCode != 0) {
              LOG.warn("Child process exited with code {}.", exitCode);
              childFailed = true;
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

    public boolean isFailed() {
      return childFailed;
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
      if (stdout != null) {
        stdout.interrupt();
        try {
          stdout.join(conf.getTimeAsMs(CLIENT_SHUTDOWN_TIMEOUT));
        } catch (InterruptedException ie) {
          LOG.info("Interrupted while waiting for child stdout to finish.");
        }
      }
      if (stderr != null) {
        stderr.interrupt();
        try {
          stderr.join(conf.getTimeAsMs(CLIENT_SHUTDOWN_TIMEOUT));
        } catch (InterruptedException ie) {
          LOG.info("Interrupted while waiting for child stderr to finish.");
        }
      }

      try {
        monitor.join(conf.getTimeAsMs(CLIENT_SHUTDOWN_TIMEOUT));
      } catch (InterruptedException ie) {
        LOG.debug("Interrupted before driver thread was finished.");
      }
    }

    private Thread redirect(String name, InputStream in) {
      Thread thread = new Thread(new Redirector(in));
      thread.setName(name);
      thread.setDaemon(true);
      thread.start();
      return thread;
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
          childFailed = true;
        }
      });
      thread.start();
      return thread;
    }
  }

}
