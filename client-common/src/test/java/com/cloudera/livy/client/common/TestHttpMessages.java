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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import static org.junit.Assert.*;

import com.cloudera.livy.JobHandle.State;

public class TestHttpMessages {

  /**
   * Tests that all defined messages can be serialized and deserialized using Jackson.
   */
  @Test
  public void testMessageSerialization() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    for (Class<?> msg : HttpMessages.class.getClasses()) {
      if (msg.isInterface()) {
        continue;
      }

      String name = msg.getSimpleName();

      Constructor c = msg.getConstructors()[0];
      Object[] params = new Object[c.getParameterTypes().length];
      for (int i = 0; i < params.length; i++) {
        params[i] = dummyValue(c.getParameterTypes()[i]);
      }

      Object o1 = c.newInstance(params);
      byte[] serialized = mapper.writeValueAsBytes(o1);
      Object o2 = mapper.readValue(serialized, msg);

      assertNotNull("could not deserialize " + name, o2);
      for (Field f : msg.getFields()) {
        checkEquals(name, f, o1, o2);
      }
    }

  }

  private Object dummyValue(Class<?> type) {
    switch (type.getSimpleName()) {
      case "int": return 42;
      case "long": return 84L;
      case "byte[]": return new byte[] { (byte) 0x42, (byte) 0x84 };
      case "String": return "test";
      case "State": return State.SUCCEEDED;
      case "Map" :
        Map<String, String> map = new HashMap<>();
        map.put("dummy1", "dummy2");
        return map;
      default: throw new IllegalArgumentException("FIX ME: " + type.getSimpleName());
    }
  }

  private void checkEquals(String name, Field f, Object o1, Object o2) throws Exception {
    Object v1 = f.get(o1);
    Object v2 = f.get(o2);

    boolean match;
    if (!f.getType().isArray()) {
      match = v1.equals(v2);
    } else if (v1 instanceof byte[]) {
      match = Arrays.equals((byte[]) v1, (byte[]) v2);
    } else {
      throw new IllegalArgumentException("FIX ME: " + f.getType().getSimpleName());
    }

    assertTrue(
      String.format("Field %s of %s does not match after deserialization.", f.getName(), name),
      match);
  }

}
