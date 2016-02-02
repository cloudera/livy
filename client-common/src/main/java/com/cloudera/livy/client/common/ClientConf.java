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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.livy.annotations.Private;

/**
 * Base class with common functionality for type-safe configuration objects.
 */
@Private
public abstract class ClientConf<T extends ClientConf>
  implements Iterable<Map.Entry<String, String>> {

  public static interface ConfEntry {

    /** The key in the configuration file. */
    String key();

    /**
     * The default value, which also defines the type of the config. Supported types:
     * Boolean, Integer, Long, String. <code>null</code> maps to String.
     */
    Object dflt();

  }

  private static final Map<String, TimeUnit> TIME_SUFFIXES;

  static {
    TIME_SUFFIXES = new HashMap<>();
    TIME_SUFFIXES.put("us", TimeUnit.MICROSECONDS);
    TIME_SUFFIXES.put("ms", TimeUnit.MILLISECONDS);
    TIME_SUFFIXES.put("s", TimeUnit.SECONDS);
    TIME_SUFFIXES.put("m", TimeUnit.MINUTES);
    TIME_SUFFIXES.put("min", TimeUnit.MINUTES);
    TIME_SUFFIXES.put("h", TimeUnit.HOURS);
    TIME_SUFFIXES.put("d", TimeUnit.DAYS);
  }

  protected final ConcurrentMap<String, String> config;

  protected ClientConf(Properties config) {
    this.config = new ConcurrentHashMap<>();
    if (config != null) {
      for (String key : config.stringPropertyNames()) {
        this.config.put(key, config.getProperty(key));
      }
    }
  }

  public String get(String key) {
    return config.get(key);
  }

  @SuppressWarnings("unchecked")
  public T set(String key, String value) {
    config.put(key, value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T setIfMissing(String key, String value) {
    config.putIfAbsent(key, value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T setAll(ClientConf<?> other) {
    for (Map.Entry<String, String> e : other) {
      set(e.getKey(), e.getValue());
    }
    return (T) this;
  }

  public String get(ConfEntry e) {
    Object value = get(e, String.class);
    return (String) (value != null ? value : e.dflt());
  }

  public boolean getBoolean(ConfEntry e) {
    String val = get(e, Boolean.class);
    if (val != null) {
      return Boolean.parseBoolean(val);
    } else {
      return (Boolean) e.dflt();
    }
  }

  public int getInt(ConfEntry e) {
    String val = get(e, Integer.class);
    if (val != null) {
      return Integer.parseInt(val);
    } else {
      return (Integer) e.dflt();
    }
  }

  public long getLong(ConfEntry e) {
    String val = get(e, Long.class);
    if (val != null) {
      return Long.parseLong(val);
    } else {
      return (Long) e.dflt();
    }
  }

  public long getTimeAsMs(ConfEntry e) {
    String time = get(e, String.class);
    if (time == null) {
      check(e.dflt() != null,
        "ConfEntry %s doesn't have a default value, cannot convert to time value.", e.key());
      time = (String) e.dflt();
    }

    Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(time.toLowerCase());
    if (!m.matches()) {
      throw new NumberFormatException("Invalid time string: " + time);
    }

    long val = Long.parseLong(m.group(1));
    String suffix = m.group(2);

    if (suffix != null && !TIME_SUFFIXES.containsKey(suffix)) {
      throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
    }

    return TimeUnit.MILLISECONDS.convert(val,
      suffix != null ? TIME_SUFFIXES.get(suffix) : TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("unchecked")
  public T set(ConfEntry e, Object value) {
    check(typesMatch(value, e.dflt()), "Value doesn't match configuration entry type for %s.",
      e.key());
    if (value == null) {
      config.remove(e.key());
    } else {
      config.put(e.key(), value.toString());
    }
    return (T) this;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return config.entrySet().iterator();
  }

  private String get(ConfEntry e, Class<?> requestedType) {
    check(getType(e.dflt()).equals(requestedType), "Invalid type conversion requested for %s.",
      e.key());
    return config.get(e.key());
  }

  private boolean typesMatch(Object test, Object expected) {
    return getType(test).equals(getType(expected));
  }

  private Class<?> getType(Object o) {
    return (o != null) ? o.getClass() : String.class;
  }

  private void check(boolean test, String message, Object... args) {
    if (!test) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

}
