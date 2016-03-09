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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermission.*;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.channel.ChannelHandlerContext;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.client.local.driver.RemoteDriver;
import com.cloudera.livy.client.local.rpc.Rpc;
import com.cloudera.livy.client.local.rpc.RpcDispatcher;
import com.cloudera.livy.client.local.rpc.RpcServer;
import static com.cloudera.livy.client.local.LocalConf.Entry.*;

/**
 * Encapsulates code needed to launch a new Spark context and collect information about how
 * to establish a client connection to it.
 */
class ContextLauncher implements ContextInfo {

  private static final Logger LOG = LoggerFactory.getLogger(ContextLauncher.class);
  private static final AtomicInteger CHILD_IDS = new AtomicInteger();
  private static final long DEFAULT_SHUTDOWN_TIMEOUT = 10000; // In milliseconds

  private static final String SPARK_JARS_KEY = "spark.jars";
  private static final String SPARK_HOME_ENV = "SPARK_HOME";
  private static final String SPARK_HOME_KEY = "spark.home";

  private final String clientId;
  private final String secret;
  private final Thread driverThread;

  private BaseProtocol.RemoteDriverAddress driverAddress;

  ContextLauncher(LocalClientFactory factory, LocalConf conf) throws IOException {
    this.clientId = UUID.randomUUID().toString();
    this.secret = factory.getServer().createSecret();

    RegistrationHandler handler = new RegistrationHandler();
    try {
      factory.getServer().registerClient(clientId, secret, handler);
      this.driverThread = startDriver(factory.getServer(), conf, clientId, secret);

      // Wait for the handler to receive the driver information.
      long timeout = conf.getTimeAsMs(RPC_CLIENT_HANDSHAKE_TIMEOUT);
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
      synchronized (handler) {
        long now = System.nanoTime();
        while (handler.driverAddress == null && now < deadline) {
          handler.wait(TimeUnit.NANOSECONDS.toMillis(deadline - now));
          now = System.nanoTime();
        }
      }
      Preconditions.checkState(handler.driverAddress != null,
        "Timed out waiting for driver connection information.");
      driverAddress = handler.driverAddress;
    } catch (Exception e) {
      factory.getServer().unregisterClient(clientId);
      throw Throwables.propagate(e);
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
  public void dispose() {
    try {
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
      LOG.warn("Exception while waiting for driver thread to finish.", e);
    }
  }

  private static Thread startDriver(
      final RpcServer rpcServer,
      final LocalConf conf,
      final String clientId,
      final String secret) throws IOException {
    Runnable runnable;
    final String serverAddress = rpcServer.getAddress();
    final String serverPort = String.valueOf(rpcServer.getPort());
    if (conf.get(CLIENT_IN_PROCESS) != null) {
      // Mostly for testing things quickly. Do not do this in production.
      LOG.warn("!!!! Running remote driver in-process. !!!!");
      runnable = new Runnable() {
        @Override
        public void run() {
          List<String> args = new ArrayList<>();
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

      File confFile = writeConfToFile(conf);

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
      int childId = CHILD_IDS.incrementAndGet();
      redirect("stdout-redir-" + childId, child.getInputStream());
      redirect("stderr-redir-" + childId, child.getErrorStream());

      runnable = new Runnable() {
        @Override
        public void run() {
          try {
            int exitCode = child.waitFor();
            if (exitCode != 0) {
              rpcServer.unregisterClient(clientId);
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

  private static void redirect(String name, InputStream in) {
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
  private static File writeConfToFile(LocalConf conf) throws IOException {
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

}
