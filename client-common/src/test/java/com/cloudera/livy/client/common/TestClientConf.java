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

import java.util.Collections;
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


  @Test
  public void testDeprecation() {
    TestConf conf = new TestConf(null);

    assertNull(conf.get("depKey"));
    assertNull(conf.get("dep_alt"));
    assertNull(conf.get("new-key"));
    assertEquals("value", conf.get(TestConf.Entry.NEW_CONF));

    TestConf depProps = new TestConf(null);
    depProps.set("depKey", "dep-val");
    depProps.set("dep_alt", "alt-val");
    conf.setAll(depProps);
    assertEquals("dep-val", conf.get("depKey"));
    assertEquals("alt-val", conf.get("dep_alt"));
    assertEquals("alt-val", conf.get(TestConf.Entry.NEW_CONF));
    assertEquals("alt-val", conf.get("new-key"));

    conf.set("new-key", "new-val");
    assertEquals("new-val", conf.get(TestConf.Entry.NEW_CONF));
    assertEquals("alt-val", conf.get("dep_alt"));
    assertEquals("new-val", conf.get("new-key"));
  }

  private static class TestConf extends ClientConf<TestConf> {

    static enum Entry implements ConfEntry {
      NULL("null", null),
      STRING("string", "default"),
      BOOLEAN("bool", false),
      INT("int", 42),
      LONG("long", 84L),
      TIME("time", "168ms"),
      TIME_NO_DEFAULT("time2", null),
      NEW_CONF("new-key", "value");

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

    private static final Map<String, DeprecatedConf> configsWithAlternatives
      = new HashMap<String, DeprecatedConf>() {{
      put(TestConf.Entry.NEW_CONF.key, DepConf.DEP_WITH_ALT);
    }};

    private static final Map<String, DeprecatedConf> deprecatedConfigs
      = new HashMap<String, DeprecatedConf>() {{
      put(DepConf.DEP_NO_ALT.key, DepConf.DEP_NO_ALT);
    }};

    public Map<String, DeprecatedConf> getConfigsWithAlternatives() {
      return Collections.unmodifiableMap(configsWithAlternatives);
    }

    public Map<String, DeprecatedConf> getDeprecatedConfigs() {
      return Collections.unmodifiableMap(deprecatedConfigs);
    }

    static enum DepConf implements DeprecatedConf {
      DEP_WITH_ALT("dep_alt", "0.4"),
      DEP_NO_ALT("depKey", "1.0");

      private final String key;
      private final String version;
      private final String deprecationMessage;

      private DepConf(String key, String version) {
        this(key, version, "");
      }

      private DepConf(String key, String version, String deprecationMessage) {
        this.key = key;
        this.version = version;
        this.deprecationMessage = deprecationMessage;
      }

      @Override
      public String key() { return key; }

      @Override
      public String version() { return version; }

      @Override
      public String deprecationMessage() { return deprecationMessage; }
    }
  }

}
