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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A few simple utility functions used by the code, mostly to avoid a direct dependency
 * on Guava.
 */
public class Utils {

  public static void checkArgument(boolean condition) {
    if (!condition) {
      throw new IllegalArgumentException();
    }
  }

  public static void checkArgument(boolean condition, String msg, Object... args) {
    if (!condition) {
      throw new IllegalArgumentException(String.format(msg, args));
    }
  }

  public static void checkState(boolean condition, String msg, Object... args) {
    if (!condition) {
      throw new IllegalStateException(String.format(msg, args));
    }
  }

  public static void checkNotNull(Object o) {
    if (o == null) {
      throw new NullPointerException();
    }
  }

  public static RuntimeException propagate(Throwable t) {
    if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    } else {
      throw new RuntimeException(t);
    }
  }

  public static ThreadFactory newDaemonThreadFactory(final String nameFormat) {
    return new ThreadFactory() {

      private final AtomicInteger threadId = new AtomicInteger();

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(String.format(nameFormat, threadId.incrementAndGet()));
        t.setDaemon(true);
        return t;
      }

    };
  }

  public static String join(Iterable<String> strs, String sep) {
    StringBuilder sb = new StringBuilder();
    for (String s : strs) {
      if (s != null) {
        sb.append(s).append(sep);
      }
    }
    if (sb.length() > 0) {
      sb.setLength(sb.length() - sep.length());
    }
    return sb.toString();
  }

  public static String stackTraceAsString(Throwable t) {
    StringBuilder sb = new StringBuilder();
    sb.append(t.getClass().getName()).append(": ").append(t.getMessage());
    for (StackTraceElement e : t.getStackTrace()) {
      sb.append(e.toString());
      sb.append("\n");
    }
    if (sb.length() > 0) {
      sb.setLength(sb.length() - 1);
    }
    return sb.toString();
  }

  private Utils() { }

}