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

import javax.security.sasl.SaslClient;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.Unpooled;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;

/**
 * Unwrap sasl messages. Should be placed after a
 * io.netty.handler.codec.LengthFieldBasedFrameDecoder
 */
@InterfaceAudience.Private
public class SaslUnwrapHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private final SaslClient saslClient;

  public SaslUnwrapHandler(SaslClient saslClient) {
    this.saslClient = saslClient;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    SaslUtil.safeDispose(saslClient);
    ctx.fireChannelInactive();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    byte[] bytes = new byte[msg.readableBytes()];
    msg.readBytes(bytes);
    ctx.fireChannelRead(Unpooled.wrappedBuffer(saslClient.unwrap(bytes, 0, bytes.length)));
  }
}
