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
package org.apache.hadoop.hbase.ipc;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;

import java.nio.ByteBuffer;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Handle connection preamble.
 * @since 2.0.0`
 */
@InterfaceAudience.Private
class NettyRpcServerPreambleHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private final NettyRpcServer rpcServer;

  public NettyRpcServerPreambleHandler(NettyRpcServer rpcServer) {
    this.rpcServer = rpcServer;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    NettyServerRpcConnection conn = createNettyServerRpcConnection(ctx.channel());
    ByteBuffer buf = ByteBuffer.allocate(msg.readableBytes());
    msg.readBytes(buf);
    buf.flip();
    if (!conn.processPreamble(buf)) {
      conn.close();
      return;
    }
    ChannelPipeline p = ctx.pipeline();
    ((NettyRpcFrameDecoder) p.get("frameDecoder")).setConnection(conn);
    ((NettyRpcServerRequestDecoder) p.get("decoder")).setConnection(conn);
    p.remove(this);
    p.remove("preambleDecoder");
  }

  protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
    return new NettyServerRpcConnection(rpcServer, channel);
  }
}
