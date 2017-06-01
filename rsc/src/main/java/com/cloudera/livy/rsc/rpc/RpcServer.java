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

package com.cloudera.livy.rsc.rpc;

import java.io.Closeable;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.rsc.RSCConf;
import com.cloudera.livy.rsc.Utils;
import static com.cloudera.livy.rsc.RSCConf.Entry.*;

/**
 * An RPC server. The server matches remote clients based on a secret that is generated on
 * the server - the secret needs to be given to the client through some other mechanism for
 * this to work.
 */
public class RpcServer implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);
  private static final SecureRandom RND = new SecureRandom();

  private final String address;
  private Channel channel;
  private final EventLoopGroup group;
  private final int port;
  private final ConcurrentMap<String, ClientInfo> pendingClients;
  private final RSCConf config;
  private final String portRange;
  private static enum PortRangeSchema{START_PORT, END_PORT, max};
  private final String PORT_DELIMITER = "~";
  /**
   * Creating RPC Server
   * @param lconf
   * @throws IOException
   * @throws InterruptedException
   */
  public RpcServer(RSCConf lconf) throws IOException, InterruptedException {
    this.config = lconf;
    this.portRange = config.get(LAUNCHER_PORT_RANGE);
    this.group = new NioEventLoopGroup(
    this.config.getInt(RPC_MAX_THREADS),
    Utils.newDaemonThreadFactory("RPC-Handler-%d"));
    int [] portData = getPortNumberAndRange();
    int startingPortNumber = portData[PortRangeSchema.START_PORT.ordinal()];
    int endPort = portData[PortRangeSchema.END_PORT.ordinal()];
    for(int tries = startingPortNumber ; tries<=endPort ; tries++){
      try {
        this.channel = getChannel(tries);
        break;
      }catch(SocketException e){
        LOG.warn("RPC not able to connect port " + tries + " " + e.getMessage());
      }
    }
    this.port = ((InetSocketAddress) channel.localAddress()).getPort();
    this.pendingClients = new ConcurrentHashMap<>();
    LOG.warn("Connected to the port " + this.port);
    String address = config.get(RPC_SERVER_ADDRESS);
    if (address == null) {
      address = config.findLocalAddress();
    }
    this.address = address;
  }

  /**
   * Get Port Numbers
   */
  public int[] getPortNumberAndRange() throws ArrayIndexOutOfBoundsException, NumberFormatException{
    String[] split = this.portRange.split(PORT_DELIMITER);
    int [] portRange=new int [PortRangeSchema.max.ordinal()];
    try {
      portRange[PortRangeSchema.START_PORT.ordinal()] =
      Integer.parseInt(split[PortRangeSchema.START_PORT.ordinal()]);
      portRange[PortRangeSchema.END_PORT.ordinal()] =
      Integer.parseInt(split[PortRangeSchema.END_PORT.ordinal()]);
    }catch(ArrayIndexOutOfBoundsException e) {
      LOG.error("Port Range format is not correct " + this.portRange);
      throw e;
    }
    catch(NumberFormatException e) {
      LOG.error("Port are not in numeric format " + this.portRange);
      throw e;
    }
    return portRange;
  }
  /**
   * @throws InterruptedException
   **/
  public Channel getChannel(int portNumber) throws BindException, InterruptedException{
      Channel channel = new ServerBootstrap()
      .group(group)
      .channel(NioServerSocketChannel.class)
      .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            SaslServerHandler saslHandler = new SaslServerHandler(config);
            final Rpc newRpc = Rpc.createServer(saslHandler, config, ch, group);
            saslHandler.rpc = newRpc;

            Runnable cancelTask = new Runnable() {
                @Override
                public void run() {
                  LOG.warn("Timed out waiting for hello from client.");
                  newRpc.close();
                }
            };
            saslHandler.cancelTask = group.schedule(cancelTask,
                config.getTimeAsMs(RPC_CLIENT_HANDSHAKE_TIMEOUT),
                TimeUnit.MILLISECONDS);
          }
      })
      .option(ChannelOption.SO_BACKLOG, 1)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true)
      .bind(portNumber)
      .sync()
      .channel();
  return channel;
  }
  /**
   * Tells the RPC server to expect connections from clients.
   *
   * @param clientId An identifier for the client. Must be unique.
   * @param secret The secret the client will send to the server to identify itself.
   * @param callback The callback for when a new client successfully connects with the given
   *                 credentials.
   */
  public void registerClient(String clientId, String secret, ClientCallback callback) {
    final ClientInfo client = new ClientInfo(clientId, secret, callback);
    if (pendingClients.putIfAbsent(clientId, client) != null) {
      throw new IllegalStateException(
          String.format("Client '%s' already registered.", clientId));
    }
  }

  /**
   * Stop waiting for connections for a given client ID.
   *
   * @param clientId The client ID to forget.
   */
  public void unregisterClient(String clientId) {
    pendingClients.remove(clientId);
  }

  /**
   * Creates a secret for identifying a client connection.
   */
  public String createSecret() {
    byte[] secret = new byte[config.getInt(RPC_SECRET_RANDOM_BITS) / 8];
    RND.nextBytes(secret);

    StringBuilder sb = new StringBuilder();
    for (byte b : secret) {
      if (b < 10) {
        sb.append("0");
      }
      sb.append(Integer.toHexString(b));
    }
    return sb.toString();
  }

  public String getAddress() {
    return address;
  }

  public int getPort() {
    return port;
  }

  public EventLoopGroup getEventLoopGroup() {
    return group;
  }

  @Override
  public void close() {
    try {
      channel.close();
      pendingClients.clear();
    } finally {
      group.shutdownGracefully();
    }
  }

  /**
   * A callback that can be registered to be notified when new clients are created and
   * successfully authenticate against the server.
   */
  public interface ClientCallback {

    /**
     * Called when a new client successfully connects.
     *
     * @param client The RPC instance for the new client.
     * @return The RpcDispatcher to be used for the client.
     */
    RpcDispatcher onNewClient(Rpc client);


    /**
     * Called when a new client successfully completed SASL authentication.
     *
     * @param client The RPC instance for the new client.
     */
    void onSaslComplete(Rpc client);
  }

  private class SaslServerHandler extends SaslHandler implements CallbackHandler {

    private final SaslServer server;
    private Rpc rpc;
    private ScheduledFuture<?> cancelTask;
    private String clientId;
    private ClientInfo client;

    SaslServerHandler(RSCConf config) throws IOException {
      super(config);
      this.server = Sasl.createSaslServer(config.get(SASL_MECHANISMS), Rpc.SASL_PROTOCOL,
        Rpc.SASL_REALM, config.getSaslOptions(), this);
    }

    @Override
    protected boolean isComplete() {
      return server.isComplete();
    }

    @Override
    protected String getNegotiatedProperty(String name) {
      return (String) server.getNegotiatedProperty(name);
    }

    @Override
    protected Rpc.SaslMessage update(Rpc.SaslMessage challenge) throws IOException {
      if (clientId == null) {
        Utils.checkArgument(challenge.clientId != null,
          "Missing client ID in SASL handshake.");
        clientId = challenge.clientId;
        client = pendingClients.get(clientId);
        Utils.checkArgument(client != null,
          "Unexpected client ID '%s' in SASL handshake.", clientId);
      }

      return new Rpc.SaslMessage(server.evaluateResponse(challenge.payload));
    }

    @Override
    public byte[] wrap(byte[] data, int offset, int len) throws IOException {
      return server.wrap(data, offset, len);
    }

    @Override
    public byte[] unwrap(byte[] data, int offset, int len) throws IOException {
      return server.unwrap(data, offset, len);
    }

    @Override
    public void dispose() throws IOException {
      if (!server.isComplete()) {
        onError(new SaslException("Server closed before SASL negotiation finished."));
      }
      server.dispose();
    }

    @Override
    protected void onComplete() throws Exception {
      cancelTask.cancel(true);

      RpcDispatcher dispatcher = null;
      try {
        dispatcher = client.callback.onNewClient(rpc);
      } catch (Exception e) {
        LOG.warn("Client callback threw an exception.", e);
      }

      if (dispatcher != null) {
        rpc.setDispatcher(dispatcher);
      }

      client.callback.onSaslComplete(rpc);
    }

    @Override
    protected void onError(Throwable error) {
      cancelTask.cancel(true);
    }

    @Override
    public void handle(Callback[] callbacks) {
      Utils.checkState(client != null, "Handshake not initialized yet.");
      for (Callback cb : callbacks) {
        if (cb instanceof NameCallback) {
          ((NameCallback)cb).setName(clientId);
        } else if (cb instanceof PasswordCallback) {
          ((PasswordCallback)cb).setPassword(client.secret.toCharArray());
        } else if (cb instanceof AuthorizeCallback) {
          ((AuthorizeCallback) cb).setAuthorized(true);
        } else if (cb instanceof RealmCallback) {
          RealmCallback rb = (RealmCallback) cb;
          rb.setText(rb.getDefaultText());
        }
      }
    }

  }

  private static class ClientInfo {

    final String id;
    final String secret;
    final ClientCallback callback;

    private ClientInfo(String id, String secret, ClientCallback callback) {
      this.id = id;
      this.secret = secret;
      this.callback = callback;
    }

  }

}

