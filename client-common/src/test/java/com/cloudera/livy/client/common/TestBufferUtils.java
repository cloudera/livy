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

import java.nio.ByteBuffer;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestBufferUtils {

  @Test
  public void testWrappedArray() {
    byte[] array = new byte[] { 0x1b, 0x2b };
    byte[] unwrapped = BufferUtils.toByteArray(ByteBuffer.wrap(array));
    assertSame(array, unwrapped);
  }

  @Test
  public void testShortArray() {
    byte[] array = new byte[] { 0x1b, 0x2b };
    byte[] unwrapped = BufferUtils.toByteArray(ByteBuffer.wrap(array, 0, 1));
    assertNotSame(array, unwrapped);
    assertEquals(1, unwrapped.length);
  }

  @Test
  public void testOffsetArray() {
    byte[] array = new byte[] { 0x1b, 0x2b };
    byte[] unwrapped = BufferUtils.toByteArray(ByteBuffer.wrap(array, 1, 1));
    assertNotSame(array, unwrapped);
    assertEquals(1, unwrapped.length);
  }

  @Test
  public void testDirectBuffer() {
    ByteBuffer direct = ByteBuffer.allocateDirect(1);
    direct.put((byte) 0x1b);
    assertFalse(direct.hasArray());
    direct.flip();

    byte[] unwrapped = BufferUtils.toByteArray(direct);
    assertEquals(0x1b, unwrapped[0]);
  }

}
