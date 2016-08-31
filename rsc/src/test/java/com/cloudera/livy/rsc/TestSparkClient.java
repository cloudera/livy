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
import java.io.FileOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientBuilder;
import com.cloudera.livy.client.common.Serializer;
import com.cloudera.livy.rsc.rpc.RpcException;
import com.cloudera.livy.test.jobs.Echo;
import com.cloudera.livy.test.jobs.Failure;
import com.cloudera.livy.test.jobs.FileReader;
import com.cloudera.livy.test.jobs.GetCurrentUser;
import com.cloudera.livy.test.jobs.SQLGetTweets;
import com.cloudera.livy.test.jobs.Sleeper;
import com.cloudera.livy.test.jobs.SmallCount;
import static com.cloudera.livy.rsc.RSCConf.Entry.*;

public class TestSparkClient {

  private static final Logger LOG = LoggerFactory.getLogger(TestSparkClient.class);

  // Timeouts are bad... mmmkay.
  private static final long TIMEOUT = 100;

  private Properties createConf(boolean local) {
    Properties conf = new Properties();
    if (local) {
      conf.put(CLIENT_IN_PROCESS.key(), "true");
      conf.put(SparkLauncher.SPARK_MASTER, "local");
      conf.put("spark.app.name", "SparkClientSuite Local App");
    } else {
      String classpath = System.getProperty("java.class.path");
      conf.put("spark.app.name", "SparkClientSuite Remote App");
      conf.put(SparkLauncher.DRIVER_MEMORY, "512m");
      conf.put(SparkLauncher.DRIVER_EXTRA_CLASSPATH, classpath);
      conf.put(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, classpath);
    }

    conf.put(LIVY_JARS.key(), "");
    return conf;
  }

  @Test
  public void testJobSubmission() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        JobHandle.Listener<String> listener = newListener();
        JobHandle<String> handle = client.submit(new Echo<>("hello"));
        handle.addListener(listener);
        assertEquals("hello", handle.get(TIMEOUT, TimeUnit.SECONDS));

        // Try an invalid state transition on the handle. This ensures that the actual state
        // change we're interested in actually happened, since internally the handle serializes
        // state changes.
        assertFalse(((JobHandleImpl<String>)handle).changeState(JobHandle.State.SENT));

        verify(listener).onJobStarted(handle);
        verify(listener).onJobSucceeded(same(handle), eq(handle.get()));
      }
    });
  }

  @Test
  public void testSimpleSparkJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SmallCount(5));
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testJobFailure() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        JobHandle.Listener<Void> listener = newListener();
        JobHandle<Void> handle = client.submit(new Failure());
        handle.addListener(listener);
        try {
          handle.get(TIMEOUT, TimeUnit.SECONDS);
          fail("Should have thrown an exception.");
        } catch (ExecutionException ee) {
          assertTrue(ee.getCause().getMessage().contains(
            Failure.JobFailureException.class.getName()));
        }

        // Try an invalid state transition on the handle. This ensures that the actual state
        // change we're interested in actually happened, since internally the handle serializes
        // state changes.
        assertFalse(((JobHandleImpl<Void>)handle).changeState(JobHandle.State.SENT));

        verify(listener).onJobStarted(handle);
        verify(listener).onJobFailed(same(handle), any(Throwable.class));
      }
    });
  }

  @Test
  public void testSyncRpc() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        Future<String> result = client.run(new Echo<>("Hello"));
        assertEquals("Hello", result.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testRemoteClient() throws Exception {
    runTest(false, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SmallCount(5));
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testAddJarsAndFiles() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        File jar = null;
        File file = null;

        try {
          // Test that adding a jar to the remote context makes it show up in the classpath.
          jar = File.createTempFile("test", ".jar");

          JarOutputStream jarFile = new JarOutputStream(new FileOutputStream(jar));
          jarFile.putNextEntry(new ZipEntry("test.resource"));
          jarFile.write("test resource".getBytes("UTF-8"));
          jarFile.closeEntry();
          jarFile.close();

          client.addJar(new URI("file:" + jar.getAbsolutePath()))
            .get(TIMEOUT, TimeUnit.SECONDS);

          // Need to run a Spark job to make sure the jar is added to the class loader. Monitoring
          // SparkContext#addJar() doesn't mean much, we can only be sure jars have been distributed
          // when we run a task after the jar has been added.
          String result = client.submit(new FileReader("test.resource", true))
            .get(TIMEOUT, TimeUnit.SECONDS);
          assertEquals("test resource", result);

          // Test that adding a file to the remote context makes it available to executors.
          file = File.createTempFile("test", ".file");

          FileOutputStream fileStream = new FileOutputStream(file);
          fileStream.write("test file".getBytes("UTF-8"));
          fileStream.close();

          client.addJar(new URI("file:" + file.getAbsolutePath()))
            .get(TIMEOUT, TimeUnit.SECONDS);

          // The same applies to files added with "addFile". They're only guaranteed to be available
          // to tasks started after the addFile() call completes.
          result = client.submit(new FileReader(file.getName(), false))
            .get(TIMEOUT, TimeUnit.SECONDS);
          assertEquals("test file", result);
        } finally {
          if (jar != null) {
            jar.delete();
          }
          if (file != null) {
            file.delete();
          }
        }
      }
    });
  }

  @Test
  public void testSparkSQLJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      void call(LivyClient client) throws Exception {
        JobHandle<List<String>> handle = client.submit(new SQLGetTweets(false));
        List<String> topTweets = handle.get(TIMEOUT, TimeUnit.SECONDS);
        assertEquals(1, topTweets.size());
        assertEquals("[Adventures With Coffee, Code, and Writing.,0]",
                topTweets.get(0));
      }
    });
  }

  @Test
  public void testHiveJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      void call(LivyClient client) throws Exception {
        JobHandle<List<String>> handle = client.submit(new SQLGetTweets(true));
        List<String> topTweets = handle.get(TIMEOUT, TimeUnit.SECONDS);
        assertEquals(1, topTweets.size());
        assertEquals("[Adventures With Coffee, Code, and Writing.,0]",
                topTweets.get(0));
      }
    });
  }

  @Test
  public void testStreamingContext() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      void call(LivyClient client) throws Exception {
        JobHandle<Boolean> handle = client.submit(new SparkStreamingJob());
        Boolean streamingContextCreated = handle.get(TIMEOUT, TimeUnit.SECONDS);
        assertEquals(true, streamingContextCreated);
      }
    });
  }

  @Test
  public void testImpersonation() throws Exception {
    final String PROXY = "__proxy__";

    runTest(false, new TestFunction() {
      @Override
      void config(Properties conf) {
        conf.put(RSCConf.Entry.PROXY_USER.key(), PROXY);
      }

      @Override
      void call(LivyClient client) throws Exception {
        JobHandle<String> handle = client.submit(new GetCurrentUser());
        String userName = handle.get(TIMEOUT, TimeUnit.SECONDS);
        assertEquals(PROXY, userName);
      }
    });
  }

  @Test
  public void testConnectToRunningContext() throws Exception {
    runTest(false, new TestFunction() {
      @Override
      void call(LivyClient client) throws Exception {
        URI uri = disconnectClient(client);

        // If this tries to create a new context, it will fail because it's missing the
        // needed configuration from createConf().
        LivyClient newClient = new LivyClientBuilder()
          .setURI(uri)
          .build();

        try {
          JobHandle<String> handle = newClient.submit(new Echo<>("hello"));
          String result = handle.get(TIMEOUT, TimeUnit.SECONDS);
          assertEquals("hello", result);
        } finally {
          newClient.stop(true);
        }
      }
    });
  }

  @Test
  public void testServerIdleTimeout() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      void call(LivyClient client) throws Exception {
        // Close the old client and wait a couple of seconds for the timeout to trigger.
        URI uri = disconnectClient(client);
        TimeUnit.SECONDS.sleep(2);

        // Try to connect back with a new client, it should fail. Since there's no API to monitor
        // the connection state, we try to enqueue a long-running job and make sure that it fails,
        // in case the connection actually goes through.
        try {
          LivyClient newClient = new LivyClientBuilder()
            .setURI(uri)
            .build();

          try {
            newClient.submit(new Sleeper(TimeUnit.SECONDS.toMillis(TIMEOUT)))
              .get(TIMEOUT, TimeUnit.SECONDS);
          } catch (TimeoutException te) {
            // Shouldn't have gotten here, but catch this so that we stop the client.
            newClient.stop(true);
          }
          fail("Should have failed to contact RSC after idle timeout.");
        } catch (Exception e) {
          // Expected.
        }
      }

      @Override
      void config(Properties conf) {
        conf.setProperty(SERVER_IDLE_TIMEOUT.key(), "1s");
      }
    });
  }

  @Test
  public void testKillServerWhileSparkSubmitIsRunning() throws Exception {
    Properties conf = createConf(true);
    LivyClient client = null;
    PipedInputStream stubStream = new PipedInputStream(new PipedOutputStream());
    try {
      Process mockSparkSubmit = mock(Process.class);
      when(mockSparkSubmit.getInputStream()).thenReturn(stubStream);
      when(mockSparkSubmit.getErrorStream()).thenReturn(stubStream);

      // Block waitFor until process.destroy() is called.
      final CountDownLatch waitForCalled = new CountDownLatch(1);
      when(mockSparkSubmit.waitFor()).thenAnswer(new Answer<Integer>() {
        @Override
        public Integer answer(InvocationOnMock invocation) throws Throwable {
          waitForCalled.await();
          return 0;
        }
      });

      // Verify process.destroy() is called.
      final CountDownLatch destroyCalled = new CountDownLatch(1);
      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          destroyCalled.countDown();
          return null;
        }
      }).when(mockSparkSubmit).destroy();

      ContextLauncher.mockSparkSubmit = mockSparkSubmit;

      client = new LivyClientBuilder(false).setURI(new URI("rsc:/"))
        .setAll(conf)
        .build();

      client.stop(true);

      assertTrue(destroyCalled.await(5, TimeUnit.SECONDS));
      waitForCalled.countDown();
    } catch (Exception e) {
      // JUnit prints not so useful backtraces in test summary reports, and we don't see the
      // actual source line of the exception, so print the exception to the logs.
      LOG.error("Test threw exception.", e);
      throw e;
    } finally {
      ContextLauncher.mockSparkSubmit = null;
      stubStream.close();
      if (client != null) {
        client.stop(true);
      }
    }
  }

  @Test
  public void testBypass() throws Exception {
    runBypassTest(false);
  }

  @Test
  public void testBypassSync() throws Exception {
    runBypassTest(true);
  }

  private void runBypassTest(final boolean sync) throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        Serializer s = new Serializer();
        RSCClient lclient = (RSCClient) client;
        ByteBuffer job = s.serialize(new Echo<>("hello"));
        String jobId = lclient.bypass(job, sync);

        // Try to fetch the result, trying several times until the timeout runs out, and
        // backing off as attempts fail.
        long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(TIMEOUT, TimeUnit.SECONDS);
        long sleep = 100;
        BypassJobStatus status = null;
        while (System.nanoTime() < deadline) {
          BypassJobStatus currStatus = lclient.getBypassJobStatus(jobId).get(TIMEOUT,
            TimeUnit.SECONDS);
          assertNotEquals(JobHandle.State.CANCELLED, currStatus.state);
          assertNotEquals(JobHandle.State.FAILED, currStatus.state);
          if (currStatus.state.equals(JobHandle.State.SUCCEEDED)) {
            status = currStatus;
            break;
          } else if (deadline - System.nanoTime() > sleep * 2) {
            Thread.sleep(sleep);
            sleep *= 2;
          }
        }
        assertNotNull("Failed to fetch bypass job status.", status);
        assertEquals(JobHandle.State.SUCCEEDED, status.state);

        String resultVal = (String) s.deserialize(ByteBuffer.wrap(status.result));
        assertEquals("hello", resultVal);

        // After the result is retrieved, the driver should stop tracking the job and release
        // resources associated with it.
        try {
          lclient.getBypassJobStatus(jobId).get(TIMEOUT, TimeUnit.SECONDS);
          fail("Should have failed to retrieve status of released job.");
        } catch (ExecutionException ee) {
          assertTrue(ee.getCause() instanceof RpcException);
          assertTrue(ee.getCause().getMessage().contains(
            "java.util.NoSuchElementException: " + jobId));
        }
      }
    });
  }

  private <T> JobHandle.Listener<T> newListener() {
    @SuppressWarnings("unchecked")
    JobHandle.Listener<T> listener =
      (JobHandle.Listener<T>) mock(JobHandle.Listener.class);
    return listener;
  }

  private URI disconnectClient(LivyClient client) throws Exception {
    ContextInfo ctx = ((RSCClient) client).getContextInfo();
    URI uri = new URI(String.format("rsc://%s:%s@%s:%d", ctx.clientId, ctx.secret,
      ctx.remoteAddress, ctx.remotePort));

    // Close the old client and wait a couple of seconds for the timeout to trigger.
    client.stop(false);
    return uri;
  }

  private void runTest(boolean local, TestFunction test) throws Exception {
    Properties conf = createConf(local);
    LivyClient client = null;
    try {
      test.config(conf);
      client = new LivyClientBuilder(false).setURI(new URI("rsc:/"))
        .setAll(conf)
        .build();

      // Wait for the context to be up before running the test.
      assertNull(client.submit(new PingJob()).get(TIMEOUT, TimeUnit.SECONDS));

      test.call(client);
    } catch (Exception e) {
      // JUnit prints not so useful backtraces in test summary reports, and we don't see the
      // actual source line of the exception, so print the exception to the logs.
      LOG.error("Test threw exception.", e);
      throw e;
    } finally {
      if (client != null) {
        client.stop(true);
      }
    }
  }

  /* Since it's hard to test a streaming context, test that a
   * streaming context has been created. Also checks that improper
   * sequence of streaming context calls (i.e create, stop, retrieve)
   * result in a failure.
   */
  private static class SparkStreamingJob implements Job<Boolean> {
    @Override
    public Boolean call(JobContext jc) throws Exception {
      try {
        jc.streamingctx();
        fail("Access before creation: Should throw IllegalStateException");
      } catch (IllegalStateException ex) {
        // Expected.
      }
      try {
        jc.stopStreamingCtx();
        fail("Stop before creation: Should throw IllegalStateException");
      } catch (IllegalStateException ex) {
        // Expected.
      }
      try {
        jc.createStreamingContext(1000L);
        JavaStreamingContext streamingContext = jc.streamingctx();
        jc.stopStreamingCtx();
        jc.streamingctx();
        fail();
      } catch (IllegalStateException ex) {
        // Expected.
      }

      jc.createStreamingContext(1000L);
      JavaStreamingContext streamingContext = jc.streamingctx();
      jc.stopStreamingCtx();
      return streamingContext != null;
    }
  }

  private abstract static class TestFunction {
    abstract void call(LivyClient client) throws Exception;
    void config(Properties conf) { }
  }

}
