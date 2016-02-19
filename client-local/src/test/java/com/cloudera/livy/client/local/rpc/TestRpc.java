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

package com.cloudera.livy.client.local.rpc;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.sasl.SaslException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

import com.cloudera.livy.client.local.LocalConf;
import static com.cloudera.livy.client.local.LocalConf.Entry.*;

public class TestRpc {

  private static final Logger LOG = LoggerFactory.getLogger(TestRpc.class);

  private Collection<Closeable> closeables;
  private LocalConf emptyConfig;

  @Before
  public void setUp() {
    closeables = Lists.newArrayList();
    emptyConfig = new LocalConf(null);
  }

  @After
  public void cleanUp() throws Exception {
    for (Closeable c : closeables) {
      IOUtils.closeQuietly(c);
    }
  }

  private <T extends Closeable> T autoClose(T closeable) {
    closeables.add(closeable);
    return closeable;
  }

  @Test
  public void testRpcDispatcher() throws Exception {
    Rpc serverRpc = autoClose(Rpc.createEmbedded(new TestDispatcher()));
    Rpc clientRpc = autoClose(Rpc.createEmbedded(new TestDispatcher()));

    TestMessage outbound = new TestMessage("Hello World!");
    Future<TestMessage> call = clientRpc.call(outbound, TestMessage.class);

    LOG.debug("Transferring messages...");
    transfer(serverRpc, clientRpc);

    TestMessage reply = call.get(10, TimeUnit.SECONDS);
    assertEquals(outbound.message, reply.message);
  }

  @Test
  public void testClientServer() throws Exception {
    RpcServer server = autoClose(new RpcServer(emptyConfig));
    Rpc[] rpcs = createRpcConnection(server);
    Rpc serverRpc = rpcs[0];
    Rpc client = rpcs[1];

    TestMessage outbound = new TestMessage("Hello World!");
    Future<TestMessage> call = client.call(outbound, TestMessage.class);
    TestMessage reply = call.get(10, TimeUnit.SECONDS);
    assertEquals(outbound.message, reply.message);

    TestMessage another = new TestMessage("Hello again!");
    Future<TestMessage> anotherCall = client.call(another, TestMessage.class);
    TestMessage anotherReply = anotherCall.get(10, TimeUnit.SECONDS);
    assertEquals(another.message, anotherReply.message);

    String errorMsg = "This is an error.";
    try {
      client.call(new ErrorCall(errorMsg)).get(10, TimeUnit.SECONDS);
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof RpcException);
      assertTrue(ee.getCause().getMessage().indexOf(errorMsg) >= 0);
    }

    // Test from server to client too.
    TestMessage serverMsg = new TestMessage("Hello from the server!");
    Future<TestMessage> serverCall = serverRpc.call(serverMsg, TestMessage.class);
    TestMessage serverReply = serverCall.get(10, TimeUnit.SECONDS);
    assertEquals(serverMsg.message, serverReply.message);
  }

  @Test
  public void testBadHello() throws Exception {
    RpcServer server = autoClose(new RpcServer(emptyConfig));

    Future<Rpc> serverRpcFuture = server.registerClient("client", "newClient",
        new TestDispatcher());
    NioEventLoopGroup eloop = new NioEventLoopGroup();

    Future<Rpc> clientRpcFuture = Rpc.createClient(emptyConfig, eloop,
        "localhost", server.getPort(), "client", "wrongClient", new TestDispatcher());

    try {
      autoClose(clientRpcFuture.get(10, TimeUnit.SECONDS));
      fail("Should have failed to create client with wrong secret.");
    } catch (ExecutionException ee) {
      // On failure, the SASL handler will throw an exception indicating that the SASL
      // negotiation failed.
      assertTrue("Unexpected exception: " + ee.getCause(),
        ee.getCause() instanceof SaslException);
    }

    serverRpcFuture.cancel(true);
  }

  @Test
  public void testCloseListener() throws Exception {
    RpcServer server = autoClose(new RpcServer(emptyConfig));
    Rpc[] rpcs = createRpcConnection(server);
    Rpc client = rpcs[1];

    final AtomicInteger closeCount = new AtomicInteger();
    client.addListener(new Rpc.Listener() {
        @Override
        public void rpcClosed(Rpc rpc) {
          closeCount.incrementAndGet();
        }
    });

    client.close();
    client.close();
    assertEquals(1, closeCount.get());
  }

  @Test
  public void testNotDeserializableRpc() throws Exception {
    RpcServer server = autoClose(new RpcServer(emptyConfig));
    Rpc[] rpcs = createRpcConnection(server);
    Rpc client = rpcs[1];

    try {
      client.call(new NotDeserializable(42)).get(10, TimeUnit.SECONDS);
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof RpcException);
      assertTrue(ee.getCause().getMessage().indexOf("KryoException") >= 0);
    }
  }

  @Test
  public void testEncryption() throws Exception {
    LocalConf eConf = new LocalConf(null)
      .setAll(emptyConfig)
      .set(SASL_QOP, Rpc.SASL_AUTH_CONF);
    RpcServer server = autoClose(new RpcServer(eConf));
    Rpc[] rpcs = createRpcConnection(server, eConf);
    Rpc client = rpcs[1];

    TestMessage outbound = new TestMessage("Hello World!");
    Future<TestMessage> call = client.call(outbound, TestMessage.class);
    TestMessage reply = call.get(10, TimeUnit.SECONDS);
    assertEquals(outbound.message, reply.message);
  }

  @Test
  public void testClientTimeout() throws Exception {
    RpcServer server = autoClose(new RpcServer(emptyConfig));
    String secret = server.createSecret();

    try {
      autoClose(server.registerClient("client", secret, new TestDispatcher(), 1L).get());
      fail("Server should have timed out client.");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof TimeoutException);
    }

    NioEventLoopGroup eloop = new NioEventLoopGroup();
    Future<Rpc> clientRpcFuture = Rpc.createClient(emptyConfig, eloop,
        "localhost", server.getPort(), "client", secret, new TestDispatcher());
    try {
      autoClose(clientRpcFuture.get());
      fail("Client should have failed to connect to server.");
    } catch (ExecutionException ee) {
      // Error should not be a timeout.
      assertFalse(ee.getCause() instanceof TimeoutException);
    }
  }

  private void transfer(Rpc serverRpc, Rpc clientRpc) {
    EmbeddedChannel client = (EmbeddedChannel) clientRpc.getChannel();
    EmbeddedChannel server = (EmbeddedChannel) serverRpc.getChannel();

    int count = 0;
    while (!client.outboundMessages().isEmpty()) {
      server.writeInbound(client.readOutbound());
      count++;
    }
    server.flush();
    LOG.debug("Transferred {} outbound client messages.", count);

    count = 0;
    while (!server.outboundMessages().isEmpty()) {
      client.writeInbound(server.readOutbound());
      count++;
    }
    client.flush();
    LOG.debug("Transferred {} outbound server messages.", count);
  }

  /**
   * Creates a client connection between the server and a client.
   *
   * @return two-tuple (server rpc, client rpc)
   */
  private Rpc[] createRpcConnection(RpcServer server) throws Exception {
    return createRpcConnection(server, emptyConfig);
  }

  private Rpc[] createRpcConnection(RpcServer server, LocalConf clientConf)
      throws Exception {
    String secret = server.createSecret();
    Future<Rpc> serverRpcFuture = server.registerClient("client", secret, new TestDispatcher());
    NioEventLoopGroup eloop = new NioEventLoopGroup();
    Future<Rpc> clientRpcFuture = Rpc.createClient(clientConf, eloop,
        "localhost", server.getPort(), "client", secret, new TestDispatcher());

    Rpc serverRpc = autoClose(serverRpcFuture.get(10, TimeUnit.SECONDS));
    Rpc clientRpc = autoClose(clientRpcFuture.get(10, TimeUnit.SECONDS));
    return new Rpc[] { serverRpc, clientRpc };
  }

  private static class TestMessage {

    final String message;

    public TestMessage() {
      this(null);
    }

    public TestMessage(String message) {
      this.message = message;
    }

  }

  private static class ErrorCall {

    final String error;

    public ErrorCall() {
      this(null);
    }

    public ErrorCall(String error) {
      this.error = error;
    }

  }

  private static class NotDeserializable {

    NotDeserializable(int unused) {

    }

  }

  private static class TestDispatcher extends RpcDispatcher {
    protected TestMessage handle(ChannelHandlerContext ctx, TestMessage msg) {
      return msg;
    }

    protected void handle(ChannelHandlerContext ctx, ErrorCall msg) {
      throw new IllegalArgumentException(msg.error);
    }

    protected void handle(ChannelHandlerContext ctx, NotDeserializable msg) {
      // No op. Shouldn't actually be called, if it is, the test will fail.
    }

  }
}
