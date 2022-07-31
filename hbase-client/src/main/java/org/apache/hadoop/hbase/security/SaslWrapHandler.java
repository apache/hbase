/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.hadoop.hbase.security;

import javax.security.sasl.SaslException;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.handler.codec.MessageToByteEncoder;

/**
 * wrap sasl messages.
 */
@InterfaceAudience.Private
public class SaslWrapHandler extends MessageToByteEncoder<ByteBuf> {

  public interface Wrapper {
    byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException;
  }

  private final Wrapper wrapper;

  public SaslWrapHandler(Wrapper wrapper) {
    this.wrapper = wrapper;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
    byte[] bytes = new byte[msg.readableBytes()];
    msg.readBytes(bytes);
    byte[] wrapperBytes = wrapper.wrap(bytes, 0, bytes.length);
    out.ensureWritable(4 + wrapperBytes.length);
    out.writeInt(wrapperBytes.length);
    out.writeBytes(wrapperBytes);
  }
}
