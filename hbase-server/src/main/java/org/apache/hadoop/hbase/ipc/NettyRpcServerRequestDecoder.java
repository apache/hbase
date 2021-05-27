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
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.hbase.thirdparty.io.netty.channel.group.ChannelGroup;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Decoder for rpc request.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyRpcServerRequestDecoder extends ChannelInboundHandlerAdapter {

  private final ChannelGroup allChannels;

  private final MetricsHBaseServer metrics;

  public NettyRpcServerRequestDecoder(ChannelGroup allChannels, MetricsHBaseServer metrics) {
    this.allChannels = allChannels;
    this.metrics = metrics;
  }

  private NettyServerRpcConnection connection;

  void setConnection(NettyServerRpcConnection connection) {
    this.connection = connection;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    allChannels.add(ctx.channel());
    NettyRpcServer.LOG.trace("Connection {}; # active connections={}",
        ctx.channel().remoteAddress(), (allChannels.size() - 1));
    super.channelActive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf input = (ByteBuf) msg;
    // 4 bytes length field
    metrics.receivedBytes(input.readableBytes() + 4);
    connection.process(input);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    allChannels.remove(ctx.channel());
    NettyRpcServer.LOG.trace("Disconnection {}; # active connections={}",
        ctx.channel().remoteAddress(), (allChannels.size() - 1));
    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
    allChannels.remove(ctx.channel());
    NettyRpcServer.LOG.trace("Connection {}; caught unexpected downstream exception.",
        ctx.channel().remoteAddress(), e);
    ctx.channel().close();
  }
}
