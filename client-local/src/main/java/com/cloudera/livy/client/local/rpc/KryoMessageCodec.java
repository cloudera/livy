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

package com.cloudera.livy.client.local.rpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.shaded.org.objenesis.strategy.StdInstantiatorStrategy;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.livy.client.common.Serializer;

/**
 * Codec that serializes / deserializes objects using Kryo. Objects are encoded with a 4-byte
 * header with the length of the serialized data.
 */
class KryoMessageCodec extends ByteToMessageCodec<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(KryoMessageCodec.class);

  private final int maxMessageSize;
  private final Serializer serializer;
  private volatile EncryptionHandler encryptionHandler;

  public KryoMessageCodec(int maxMessageSize, Class<?>... messages) {
    this.maxMessageSize = maxMessageSize;
    this.serializer = new Serializer(messages);
    this.encryptionHandler = null;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
      throws Exception {
    if (in.readableBytes() < 4) {
      return;
    }

    in.markReaderIndex();
    int msgSize = in.readInt();
    checkSize(msgSize);

    if (in.readableBytes() < msgSize) {
      // Incomplete message in buffer.
      in.resetReaderIndex();
      return;
    }

    try {
      ByteBuffer nioBuffer = maybeDecrypt(in.nioBuffer(in.readerIndex(), msgSize));
      Object msg = serializer.deserialize(nioBuffer);
      LOG.debug("Decoded message of type {} ({} bytes)",
          msg != null ? msg.getClass().getName() : msg, msgSize);
      out.add(msg);
    } finally {
      in.skipBytes(msgSize);
    }
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf buf)
      throws Exception {
    ByteBuffer msgData = maybeEncrypt(serializer.serialize(msg));
    LOG.debug("Encoded message of type {} ({} bytes)", msg.getClass().getName(),
      msgData.remaining());
    checkSize(msgData.remaining());

    buf.ensureWritable(msgData.remaining() + 4);
    buf.writeInt(msgData.remaining());
    buf.writeBytes(msgData);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (encryptionHandler != null) {
      encryptionHandler.dispose();
    }
    super.channelInactive(ctx);
  }

  private void checkSize(int msgSize) {
    Preconditions.checkArgument(msgSize > 0, "Message size (%s bytes) must be positive.", msgSize);
    Preconditions.checkArgument(maxMessageSize <= 0 || msgSize <= maxMessageSize,
        "Message (%s bytes) exceeds maximum allowed size (%s bytes).", msgSize, maxMessageSize);
  }

  private ByteBuffer maybeEncrypt(ByteBuffer data) throws Exception {
    return doWrapOrUnWrap(data, true);
  }

  private ByteBuffer maybeDecrypt(ByteBuffer data) throws Exception {
    return doWrapOrUnWrap(data, false);
  }

  private ByteBuffer doWrapOrUnWrap(ByteBuffer data, boolean wrap) throws IOException {
    if (encryptionHandler == null) {
      return data;
    }

    byte[] byteData;
    int len = data.limit() - data.position();
    int offset;
    if (data.hasArray()) {
      byteData = data.array();
      offset = data.position() + data.arrayOffset();
      data.position(data.limit());
    } else {
      byteData = new byte[len];
      offset = 0;
      data.get(byteData);
    }

    byte[] result;
    if (wrap) {
      result = encryptionHandler.wrap(byteData, offset, len);
    } else {
      result = encryptionHandler.unwrap(byteData, offset, len);
    }
    return ByteBuffer.wrap(result);
  }

  void setEncryptionHandler(EncryptionHandler handler) {
    this.encryptionHandler = handler;
  }

  interface EncryptionHandler {

    byte[] wrap(byte[] data, int offset, int len) throws IOException;

    byte[] unwrap(byte[] data, int offset, int len) throws IOException;

    void dispose() throws IOException;

  }

}
