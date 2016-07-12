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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.shaded.org.objenesis.strategy.StdInstantiatorStrategy;

import com.cloudera.livy.annotations.Private;

/**
 * Utility class to serialize user data using Kryo.
 */
@Private
public class Serializer {

  // Kryo docs say 0-8 are taken. Strange things happen if you don't set an ID when registering
  // classes.
  private static final int REG_ID_BASE = 16;

  private final ThreadLocal<Kryo> kryos;

  public Serializer(final Class<?>... klasses) {
    this.kryos = new ThreadLocal<Kryo>() {
      @Override
      protected Kryo initialValue() {
        Kryo kryo = new Kryo();
        int count = 0;
        for (Class<?> klass : klasses) {
          kryo.register(klass, REG_ID_BASE + count);
          count++;
        }
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
        return kryo;
      }
    };
  }

  public Object deserialize(ByteBuffer data) {
    byte[] b = new byte[data.remaining()];
    data.get(b);
    Input kryoIn = new Input(b);
    return kryos.get().readClassAndObject(kryoIn);
  }

  public ByteBuffer serialize(Object data) {
    ByteBufferOutputStream out = new ByteBufferOutputStream();
    Output kryoOut = new Output(out);
    kryos.get().writeClassAndObject(kryoOut, data);
    kryoOut.flush();
    return out.getBuffer();
  }

  private static class ByteBufferOutputStream extends ByteArrayOutputStream {

    public ByteBuffer getBuffer() {
      ByteBuffer result = ByteBuffer.wrap(buf, 0, count);
      buf = null;
      reset();
      return result;
    }

  }

}