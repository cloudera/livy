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

package com.cloudera.livy;

import java.io.File;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.Future;

public class TestClientFactory implements LivyClientFactory {

  @Override
  public LivyClient createClient(URI uri, Properties config) {
    switch (uri.getPath()) {
      case "match":
        return new Client(config);

      case "error":
        throw new IllegalStateException("error");

      default:
        return null;
    }
  }

  public static class Client implements LivyClient {

    public final Properties config;

    private Client(Properties config) {
      this.config = config;
    }

    @Override
    public <T> JobHandle<T> submit(Job<T> job) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<T> run(Job<T> job) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stop(boolean shutdownContext) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Future<?> uploadJar(File jar) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Future<?> addJar(URI uri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Future<?> uploadFile(File file) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Future<?> addFile(URI uri) {
      throw new UnsupportedOperationException();
    }

}

}
