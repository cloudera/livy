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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import org.apache.spark.SparkException;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;
import com.cloudera.livy.JobHandle;
import com.cloudera.livy.LivyClient;
import com.cloudera.livy.MetricsCollection;
import com.cloudera.livy.client.local.conf.RscConf;

public class TestSparkClient {

  // Timeouts are bad... mmmkay.
  private static final long TIMEOUT = 40;
  private static final RscConf RSC_CONF = new RscConf();

  private Map<String, String> createConf(boolean local) {
    Map<String, String> conf = new HashMap<String, String>();
    if (local) {
      conf.put(SparkClientFactory.CONF_KEY_IN_PROCESS, "true");
      conf.put("spark.master", "local");
      conf.put("spark.app.name", "SparkClientSuite Local App");
    } else {
      String classpath = System.getProperty("java.class.path");
      conf.put("spark.master", "local");
      conf.put("spark.app.name", "SparkClientSuite Remote App");
      conf.put("spark.driver.extraClassPath", classpath);
      conf.put("spark.executor.extraClassPath", classpath);
    }

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
          assertTrue(ee.getCause() instanceof SparkException);
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
    assumeTrue("Test requires a Spark installation in SPARK_HOME.",
      System.getenv("SPARK_HOME") != null);
    runTest(false, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SparkJob());
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testMetricsCollection() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(LivyClient client) throws Exception {
        JobHandle.Listener<Integer> listener = newListener();
        JobHandle<Integer> future = client.submit(new AsyncSparkJob());
        future.addListener(listener);
        future.get(TIMEOUT, TimeUnit.SECONDS);
        MetricsCollection metrics = future.getMetrics();
        assertEquals(1, metrics.getJobIds().size());
        assertTrue(metrics.getAllMetrics().executorRunTime >= 0L);
        verify(listener).onSparkJobStarted(same(future),
          eq(metrics.getJobIds().iterator().next()));

        JobHandle.Listener<Integer> listener2 = newListener();
        JobHandle<Integer> future2 = client.submit(new AsyncSparkJob());
        future2.addListener(listener2);
        future2.get(TIMEOUT, TimeUnit.SECONDS);
        MetricsCollection metrics2 = future2.getMetrics();
        assertEquals(1, metrics2.getJobIds().size());
        assertFalse(Objects.equal(metrics.getJobIds(), metrics2.getJobIds()));
        assertTrue(metrics2.getAllMetrics().executorRunTime >= 0L);
        verify(listener2).onSparkJobStarted(same(future2),
          eq(metrics2.getJobIds().iterator().next()));
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
  public void testStreamingContext() throws Exception{
    runTest(true, new TestFunction() {
      @Override
      void call(LivyClient client) throws Exception {
        JobHandle<Boolean> handle = client.submit(new SparkStreamingJob());
        Boolean streamingContextCreated = handle.get(TIMEOUT, TimeUnit.SECONDS);
        assertEquals(true, streamingContextCreated);
      }
    });
  }

  private <T extends Serializable> JobHandle.Listener<T> newListener() {
    @SuppressWarnings("unchecked")
    JobHandle.Listener<T> listener =
      (JobHandle.Listener<T>) mock(JobHandle.Listener.class);
    return listener;
  }

  private void runTest(boolean local, TestFunction test) throws Exception {
    Map<String, String> conf = createConf(local);
    SparkClientFactory.initialize(conf);
    LivyClient client = null;
    try {
      test.config(conf);
      client = SparkClientFactory.createClient(conf, RSC_CONF);
      test.call(client);
    } finally {
      if (client != null) {
        client.stop();
      }
      SparkClientFactory.stop();
    }
  }

  private static class HiveJob implements Job<ArrayList<String>> {

    @Override
    public ArrayList<String> call (JobContext jc){
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

      DataFrame topTweets = sqlctx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");
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
      } catch (IllegalStateException ex) {}
      try {
        jc.stopStreamingCtx();
        fail("Stop before creation: Should throw IllegalStateException");
      } catch (IllegalStateException ex) {}
      try {
        jc.createStreamingContext(1000L);
        JavaStreamingContext streamingContext = jc.streamingctx();
        jc.stopStreamingCtx();
        jc.streamingctx();
        fail();
      } catch (IllegalStateException ex) {}

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
      JavaFutureAction<?> future = jc.monitor(rdd.foreachAsync(new VoidFunction<Integer>() {
        @Override
        public void call(Integer l) throws Exception {

        }
      }), null);

      future.get(TIMEOUT, TimeUnit.SECONDS);

      return 1;
    }

  }

  private static class JarJob implements Job<String>, Function<Integer, String> {

    @Override
    public String call(JobContext jc) {
      return jc.sc().parallelize(Arrays.asList(1)).map(this).collect().get(0);
    }

    @Override
    public String call(Integer i) throws Exception {
      ClassLoader ccl = Thread.currentThread().getContextClassLoader();
      InputStream in = ccl.getResourceAsStream("test.resource");
      byte[] bytes = ByteStreams.toByteArray(in);
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
      byte[] bytes = ByteStreams.toByteArray(in);
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

  private abstract static class TestFunction {
    abstract void call(LivyClient client) throws Exception;
    void config(Map<String, String> conf) { }
  }

}
