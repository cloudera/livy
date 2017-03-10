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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestClientConf {

  @Test
  public void testTypes() {
    TestConf conf = new TestConf(null);

    assertNull(conf.get(TestConf.Entry.NULL));
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

    conf.set(TestConf.Entry.STRING, "aString");
    assertEquals("aString", conf.get(TestConf.Entry.STRING));

    conf.set(TestConf.Entry.BOOLEAN, true);
    assertEquals(true, conf.getBoolean(TestConf.Entry.BOOLEAN));

    conf.set(TestConf.Entry.LONG, 42L);
    assertEquals(42L, conf.getLong(TestConf.Entry.LONG));

    conf.set(TestConf.Entry.LONG, null);
    assertEquals(84L, conf.getLong(TestConf.Entry.LONG));

    conf.set(TestConf.Entry.TIME_NO_DEFAULT, "100");
    assertEquals(100L, conf.getTimeAsMs(TestConf.Entry.TIME_NO_DEFAULT));
  }

  @Test
  public void testRawProperties() {
    Properties dflt = new Properties();
    dflt.put("key1", "val1");
    dflt.put("key2", "val2");

    TestConf conf = new TestConf(dflt);
    conf.set("key2", "anotherVal");

    assertEquals("val1", conf.get("key1"));
    assertEquals("anotherVal", conf.get("key2"));

    conf.setIfMissing("key2", "yetAnotherVal");
    assertEquals("anotherVal", conf.get("key2"));

    conf.setIfMissing("key3", "val3");
    assertEquals("val3", conf.get("key3"));

    int count = 0;
    for (Map.Entry<String, String> e : conf) {
      count++;
    }
    assertEquals(3, count);

    TestConf newProps = new TestConf(null);
    newProps.set("key4", "val4");
    newProps.set("key5", "val5");
    conf.setAll(newProps);
    assertEquals("val4", conf.get("key4"));
    assertEquals("val5", conf.get("key5"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidTime() {
    TestConf conf = new TestConf(null);
    conf.set(TestConf.Entry.TIME, "invalid");
    conf.getTimeAsMs(TestConf.Entry.TIME);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidTimeSuffix() {
    TestConf conf = new TestConf(null);
    conf.set(TestConf.Entry.TIME, "100foo");
    conf.getTimeAsMs(TestConf.Entry.TIME);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testTimeWithoutDefault() {
    TestConf conf = new TestConf(null);
    conf.getTimeAsMs(TestConf.Entry.TIME_NO_DEFAULT);
  }

  private static class TestConf extends ClientConf<TestConf> {

    static enum Entry implements ConfEntry {
      NULL("null", null),
      STRING("string", "default"),
      BOOLEAN("bool", false),
      INT("int", 42),
      LONG("long", 84L),
      TIME("time", "168ms"),
      TIME_NO_DEFAULT("time2", null);

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

    TestConf(Properties p) {
      super(p);
    }

    // TODO: Add tests for Conf Deprecation
    public Map<String, DeprecatedConf> getConfigsWithAlternatives() { return new HashMap<>(); }
    public Map<String, DeprecatedConf> getDeprecatedConfigs() { return new HashMap<>(); }

  }

}
