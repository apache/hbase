/**
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;

/**
 * Unwrap messages with Crypto AES. Should be placed after a
 * {@link io.netty.handler.codec.LengthFieldBasedFrameDecoder}
 */
@InterfaceAudience.Private
public class CryptoAESUnwrapHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private final CryptoAES cryptoAES;

  public CryptoAESUnwrapHandler(CryptoAES cryptoAES) {
    this.cryptoAES = cryptoAES;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    byte[] bytes = new byte[msg.readableBytes()];
    msg.readBytes(bytes);
    ctx.fireChannelRead(Unpooled.wrappedBuffer(cryptoAES.unwrap(bytes, 0, bytes.length)));
  }
}
