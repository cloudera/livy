/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.client.http;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import static com.cloudera.livy.client.http.HttpConf.Entry.*;

/**
 * Abstracts a connection to the Livy server; serializes multiple requests so that we only need
 * one active HTTP connection (to keep resource usage down).
 */
class LivyConnection {

  private static final String APPLICATION_JSON = "application/json";

  private final URI server;
  private final String uriRoot;
  private final CloseableHttpClient client;
  private final ObjectMapper mapper;

  LivyConnection(URI uri, final HttpConf config) {
    HttpClientContext ctx = HttpClientContext.create();
    int port = uri.getPort() > 0 ? uri.getPort() : 8998;

    String path = uri.getPath() != null ? uri.getPath() : "";
    this.uriRoot = path + "/clients";

    RequestConfig reqConfig = new RequestConfig() {
      @Override
      public int getConnectTimeout() {
        return (int) config.getTimeAsMs(CONNETION_TIMEOUT);
      }

      @Override
      public int getSocketTimeout() {
        return (int) config.getTimeAsMs(SOCKET_TIMEOUT);
      }
    };

    HttpClientBuilder builder = HttpClientBuilder.create()
      .disableAutomaticRetries()
      .evictExpiredConnections()
      .evictIdleConnections(config.getTimeAsMs(CONNECTION_IDLE_TIMEOUT), TimeUnit.MILLISECONDS)
      .setConnectionManager(new BasicHttpClientConnectionManager())
      .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
      .setDefaultRequestConfig(reqConfig)
      .setMaxConnTotal(1)
      .setUserAgent("livy-client-http");

    this.server = uri;
    this.client = builder.build();
    this.mapper = new ObjectMapper();
  }

  synchronized void close() throws IOException {
    client.close();
  }

  synchronized <V> V delete(Class<V> retType, String uri, Object... uriParams) throws Exception {
    return sendJSONRequest(new HttpDelete(), retType, uri, uriParams);
  }

  synchronized <V> V get(Class<V> retType, String uri, Object... uriParams) throws Exception {
    return sendJSONRequest(new HttpGet(), retType, uri, uriParams);
  }

  synchronized <V> V post(
      Object body,
      Class<V> retType,
      String uri,
      Object... uriParams) throws Exception {
    HttpPost post = new HttpPost();
    byte[] bodyBytes = mapper.writeValueAsBytes(body);
    post.setEntity(new ByteArrayEntity(bodyBytes));
    return sendJSONRequest(post, retType, uri, uriParams);
  }

  synchronized <V> V post(
      File f,
      Class<V> retType,
      String paramName,
      String uri,
      Object... uriParams) throws Exception {
    HttpPost post = new HttpPost();
    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.addPart(paramName, new FileBody(f));
    post.setEntity(builder.build());
    return sendRequest(post, retType, uri, uriParams);
  }

  private <V> V sendJSONRequest(
      HttpRequestBase req,
      Class<V> retType,
      String uri,
      Object... uriParams) throws Exception {
    req.setHeader(HttpHeaders.ACCEPT, APPLICATION_JSON);
    req.setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
    req.setHeader(HttpHeaders.CONTENT_ENCODING, "UTF-8");
    return sendRequest(req, retType, uri, uriParams);
  }

  private <V> V sendRequest(
      HttpRequestBase req,
      Class<V> retType,
      String uri,
      Object... uriParams) throws Exception {
    req.setURI(new URI(server.getScheme(), null, server.getHost(), server.getPort(),
      uriRoot + String.format(uri, uriParams), null, null));
    req.setURI(new URI(server.getScheme(), null, server.getHost(), server.getPort(),
      uriRoot + String.format(uri, uriParams), null, null));

    try (CloseableHttpResponse res = client.execute(req)) {
      int status = (res.getStatusLine().getStatusCode() / 100) * 100;
      HttpEntity entity = res.getEntity();
      if (status == HttpStatus.SC_OK) {
        if (!Void.class.equals(retType)) {
          return mapper.readValue(entity.getContent(), retType);
        } else {
          return null;
        }
      } else {
        String error = EntityUtils.toString(entity);
        throw new IOException(String.format("%s: %s", res.getStatusLine().getReasonPhrase(),
          error));
      }
    }
  }

}
