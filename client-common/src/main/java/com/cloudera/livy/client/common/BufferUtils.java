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

import com.cloudera.livy.annotations.Private;

/**
 * Utility methods for dealing with byte buffers and byte arrays.
 */
@Private
public class BufferUtils {

  public static byte[] toByteArray(ByteBuffer buf) {
    byte[] bytes;
    if (buf.hasArray() && buf.arrayOffset() == 0 &&
        buf.remaining() == buf.array().length) {
      bytes = buf.array();
    } else {
      bytes = new byte[buf.remaining()];
      buf.get(bytes);
    }
    return bytes;
  }

}
