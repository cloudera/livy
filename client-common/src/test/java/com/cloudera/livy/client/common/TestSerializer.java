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

package com.cloudera.livy.client.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestSerializer {

  private static final String MESSAGE = "Hello World!";

  @Test
  public void testSerializer() throws Exception {
    Object decoded = doSerDe(MESSAGE);
    assertEquals(MESSAGE, decoded);
  }

  @Test
  public void testAutoRegistration() throws Exception {
    Object decoded = doSerDe(new TestMessage(MESSAGE), TestMessage.class);
    assertTrue(decoded instanceof TestMessage);
    assertEquals(MESSAGE, ((TestMessage)decoded).data);
  }

  private Object doSerDe(Object data, Class<?>... klasses) {
    Serializer s = new Serializer(klasses);
    ByteBuffer serialized = s.serialize(data);
    return s.deserialize(serialized);
  }

  private ByteBuffer newBuffer() {
    return ByteBuffer.allocate(1024);
  }

  private static class TestMessage {
    final String data;

    TestMessage() {
      this(null);
    }

    TestMessage(String data) {
      this.data = data;
    }
  }

}
