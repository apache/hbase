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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Decoder for rpc request.
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
    if (NettyRpcServer.LOG.isDebugEnabled()) {
      NettyRpcServer.LOG.debug("Connection from " + ctx.channel().remoteAddress() +
          "; # active connections: " + (allChannels.size() - 1));
    }
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
    if (NettyRpcServer.LOG.isDebugEnabled()) {
      NettyRpcServer.LOG.debug("Disconnecting client: " + ctx.channel().remoteAddress() +
          ". Number of active connections: " + (allChannels.size() - 1));
    }
    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
    allChannels.remove(ctx.channel());
    if (NettyRpcServer.LOG.isDebugEnabled()) {
      NettyRpcServer.LOG.debug("Connection from " + ctx.channel().remoteAddress() +
          " catch unexpected exception from downstream.",
        e.getCause());
    }
    ctx.channel().close();
  }
}