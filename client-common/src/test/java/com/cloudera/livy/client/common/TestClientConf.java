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

package com.cloudera.livy.client.common;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestClientConf {

  @Test
  public void testClientConf() {
    TestConf conf = new TestConf();

    assertEquals("default", conf.get(TestConf.Entry.STRING));
    assertEquals(false, conf.getBoolean(TestConf.Entry.BOOLEAN));
    assertEquals(42, conf.getInt(TestConf.Entry.INT));
    assertEquals(84L, conf.getLong(TestConf.Entry.LONG));
    assertEquals(168L, conf.getTimeAsMs(TestConf.Entry.TIME));

    try {
      conf.get(TestConf.Entry.INT);
      fail("Should have failed to retrieve int as string.");
    } catch (IllegalArgumentException ie) {
      // Expected.
    }

    conf.set(TestConf.Entry.INT, 336);
    assertEquals(336, conf.getInt(TestConf.Entry.INT));

    try {
      conf.set(TestConf.Entry.INT, "abcde");
      fail("Should have failed to set int as string.");
    } catch (IllegalArgumentException ie) {
      // Expected.
    }

    conf.set(TestConf.Entry.BOOLEAN, true);
    assertEquals(true, conf.getBoolean(TestConf.Entry.BOOLEAN));
  }

  private static class TestConf extends ClientConf<TestConf> {

    static enum Entry implements ConfEntry {
      STRING("string", "default"),
      BOOLEAN("bool", false),
      INT("int", 42),
      LONG("long", 84L),
      TIME("time", "168ms");

      private final String key;
      private final Object dflt;

      private Entry(String key, Object dflt) {
        this.key = key;
        this.dflt = dflt;
      }

      @Override
      public String key() { return key; }

      @Override
      public Object dflt() { return dflt; }

    }

    TestConf() {
      super(null);
    }

  }

}
