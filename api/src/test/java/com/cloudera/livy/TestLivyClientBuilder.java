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

import java.net.URI;
import java.util.Properties;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestLivyClientBuilder {

  @Test
  public void testMatch() throws Exception {
    Properties props = new Properties();
    props.setProperty("prop1", "_prop1_");
    props.setProperty("prop3", "prop3");

    TestClientFactory.Client client = (TestClientFactory.Client)
      new LivyClientBuilder(false)
        .setURI(new URI("match"))
        .setConf("prop1", "prop1")
        .setConf("prop2", "prop2")
        .setAll(props)
        .build();

    assertNotNull(client);
    assertEquals("_prop1_", client.config.getProperty("prop1"));
    assertEquals("prop2", client.config.getProperty("prop2"));
    assertEquals("prop3", client.config.getProperty("prop3"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingUri() throws Exception {
    new LivyClientBuilder(false).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMismatch() throws Exception {
    assertNull(new LivyClientBuilder(false).setURI(new URI("mismatch")).build());
  }

  @Test
  public void testFactoryError() throws Exception {
    try {
      assertNull(new LivyClientBuilder(false).setURI(new URI("error")).build());
    } catch (IllegalArgumentException e) {
      assertNotNull(e.getCause());
      assertTrue(e.getCause() instanceof IllegalStateException);
    }
  }

  @Test
  public void testDefaultConfig() throws Exception {
    TestClientFactory.Client client = (TestClientFactory.Client)
      new LivyClientBuilder().build();
    assertEquals("override", client.config.getProperty("spark.config"));
  }

  @Test
  public void testRedaction() throws Exception {
    try {
      new LivyClientBuilder(false).setURI(new URI("mismatch://user@host")).build();
      fail("Should have failed to create client.");
    } catch (IllegalArgumentException e) {
      assertFalse("Should have redacted user information.",
        e.getMessage().contains("user@host"));
    }
  }

}
