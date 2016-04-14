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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.LivyClientBuilder;
import com.cloudera.livy.client.common.Serializer;
import com.cloudera.livy.rsc.rpc.RpcException;
import static com.cloudera.livy.rsc.RSCConf.Entry.*;

public class TestSparkClient {

  private static final Logger LOG = LoggerFactory.getLogger(TestSparkClient.class);

  // Timeouts are bad... mmmkay.
  private static final long TIMEOUT = 40;

  private Properties createConf(boolean local) {
    Properties conf = new Properties();
    if (local) {
      conf.put(CLIENT_IN_PROCESS.key(), "true");
      conf.put(SparkLauncher.SPARK_MASTER, "local");
      conf.put("spark.app.name", "SparkClientSuite Local App");
    } else {
      String classpath = System.getProperty("java.class.path");
      conf.put(SparkLauncher.SPARK_MASTER, "local");
      conf.put("spark.app.name", "SparkClientSuite Remote App");
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
        JobHandle<String> handle = client.submit(new SimpleJob());
        handle.addListener(listener);
        assertEquals("hello", handle.get(TIMEOUT, TimeUnit.SECONDS));

        // Try an invalid state transition on the handle. This ensures that the actual state
        // change we're interested in actually happened, since internally the handle serializes
        // state changes.
        assertFalse(((JobHandleImpl<String>)handle).changeState(JobHandle.State.SENT));

        verify(listener).onJobQueued(handle);
        verify(listener).onJobStarted(handle);
        verify(listener).onJobSucceeded(same(handle), eq(handle.get()));

        // Try a PingJob, both to make sure it works and also to test "null" results.
        assertNull(client.submit(new PingJob()).get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testSimpleSparkJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SparkJob());
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testErrorJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        JobHandle.Listener<String> listener = newListener();
        JobHandle<String> handle = client.submit(new ErrorJob());
        handle.addListener(listener);
        try {
          handle.get(TIMEOUT, TimeUnit.SECONDS);
          fail("Should have thrown an exception.");
        } catch (ExecutionException ee) {
          assertTrue(ee.getCause().getMessage().contains("IllegalStateException: Hello"));
        }

        // Try an invalid state transition on the handle. This ensures that the actual state
        // change we're interested in actually happened, since internally the handle serializes
        // state changes.
        assertFalse(((JobHandleImpl<String>)handle).changeState(JobHandle.State.SENT));

        verify(listener).onJobQueued(handle);
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
        Future<String> result = client.run(new SyncRpc());
        assertEquals("Hello", result.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testRemoteClient() throws Exception {
    runTest(false, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SparkJob());
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
          String result = client.submit(new JarJob()).get(TIMEOUT, TimeUnit.SECONDS);
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
          result = client.submit(new FileJob(file.getName()))
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
        JobHandle<ArrayList<String>> handle = client.submit(new SparkSQLJob());
        ArrayList<String> topTweets = handle.get(TIMEOUT, TimeUnit.SECONDS);
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
        JobHandle<ArrayList<String>> handle = client.submit(new HiveJob());
        ArrayList<String> topTweets = handle.get(TIMEOUT, TimeUnit.SECONDS);
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
        JobHandle<String> handle = client.submit(new GetCurrentUserJob());
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
        ContextInfo ctx = ((RSCClient) client).getContextInfo();
        URI uri = new URI(String.format("local://%s:%s@%s:%d", ctx.getClientId(), ctx.getSecret(),
          ctx.getRemoteAddress(), ctx.getRemotePort()));

        // Close the old client to make sure the driver doesn't go away when it disconnects.
        client.stop(false);

        // If this tries to create a new context, it will fail because it's missing the
        // needed configuration from createConf().
        LivyClient newClient = new LivyClientBuilder()
          .setURI(uri)
          .build();

        try {
          JobHandle<String> handle = newClient.submit(new SimpleJob());
          String result = handle.get(TIMEOUT, TimeUnit.SECONDS);
          assertEquals("hello", result);
        } finally {
          newClient.stop(true);

          // Make sure the underlying ContextLauncher is cleaned up properly, since we did
          // a "stop(false)" above.
          ((RSCClient) client).getContextInfo().dispose(true);
        }
      }
    });
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
        ByteBuffer job = s.serialize(new AsyncSparkJob());
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

        Integer resultVal = (Integer) s.deserialize(ByteBuffer.wrap(status.result));
        assertEquals(Integer.valueOf(1), resultVal);

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

  private void runTest(boolean local, TestFunction test) throws Exception {
    Properties conf = createConf(local);
    LivyClient client = null;
    try {
      test.config(conf);
      client = new LivyClientBuilder(false).setURI(new URI("local:spark"))
        .setAll(conf)
        .build();
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

  private static byte[] toByteArray(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int read;
    while ((read = in.read(buf)) >= 0) {
      out.write(buf, 0, read);
    }
    return out.toByteArray();
  }

  private static class HiveJob implements Job<ArrayList<String>> {

    @Override
    public ArrayList<String> call(JobContext jc){
      String inputFile = "src/test/resources/testweet.json";
      HiveContext hivectx = jc.hivectx();

      DataFrame input = hivectx.jsonFile(inputFile);
      input.registerTempTable("tweets");

      DataFrame topTweets = hivectx.sql(
              "SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");
      ArrayList<String> tweetList = new ArrayList<>();
      for (Row r : topTweets.collect()) {
        tweetList.add(r.toString());
      }
      return tweetList;
    }

  }

  private static class SparkSQLJob implements Job<ArrayList<String>> {

    @Override
    public ArrayList<String> call(JobContext jc){
      String inputFile = "src/test/resources/testweet.json";
      SQLContext sqlctx = jc.sqlctx();
      DataFrame input = sqlctx.jsonFile(inputFile);
      input.registerTempTable("tweets");

      DataFrame topTweets = sqlctx.sql(
        "SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");
      ArrayList<String> tweetList = new ArrayList<>();
      for (Row r : topTweets.collect()) {
         tweetList.add(r.toString());
      }
      return tweetList;
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

  private static class SimpleJob implements Job<String> {

    @Override
    public String call(JobContext jc) {
      return "hello";
    }

  }

  private static class ErrorJob implements Job<String> {

    @Override
    public String call(JobContext jc) {
      throw new IllegalStateException("Hello");
    }

  }

  private static class SparkJob implements Job<Long> {

    @Override
    public Long call(JobContext jc) {
      JavaRDD<Integer> rdd = jc.sc().parallelize(Arrays.asList(1, 2, 3, 4, 5));
      return rdd.count();
    }

  }

  private static class AsyncSparkJob implements Job<Integer> {

    @Override
    public Integer call(JobContext jc) throws Exception {
      JavaRDD<Integer> rdd = jc.sc().parallelize(Arrays.asList(1, 2, 3, 4, 5));
      JavaFutureAction<?> future = rdd.foreachAsync(new VoidFunction<Integer>() {
        @Override
        public void call(Integer l) throws Exception {
          Thread.sleep(1);
        }
      });

      future.get(TIMEOUT, TimeUnit.SECONDS);

      return 1;
    }

  }

  private static class JarJob implements Job<String>, Function<Integer, String> {

    @Override
    public String call(JobContext jc) throws Exception {
      call(0);
      return jc.sc().parallelize(Arrays.asList(1)).map(this).collect().get(0);
    }

    @Override
    public String call(Integer i) throws Exception {
      ClassLoader ccl = Thread.currentThread().getContextClassLoader();
      InputStream in = ccl.getResourceAsStream("test.resource");
      byte[] bytes = toByteArray(in);
      in.close();
      return new String(bytes, 0, bytes.length, "UTF-8");
    }

  }

  private static class FileJob implements Job<String>, Function<Integer, String> {

    private final String fileName;

    FileJob(String fileName) {
      this.fileName = fileName;
    }

    @Override
    public String call(JobContext jc) {
      return jc.sc().parallelize(Arrays.asList(1)).map(this).collect().get(0);
    }

    @Override
    public String call(Integer i) throws Exception {
      InputStream in = new FileInputStream(SparkFiles.get(fileName));
      byte[] bytes = toByteArray(in);
      in.close();
      return new String(bytes, 0, bytes.length, "UTF-8");
    }

  }

  private static class SyncRpc implements Job<String> {

    @Override
    public String call(JobContext jc) {
      return "Hello";
    }

  }

  private static class GetCurrentUserJob implements Job<String> {

    @Override
    public String call(JobContext jc) throws Exception {
      return UserGroupInformation.getCurrentUser().getUserName();
    }

  }

  private abstract static class TestFunction {
    abstract void call(LivyClient client) throws Exception;
    void config(Properties conf) { }
  }

}
