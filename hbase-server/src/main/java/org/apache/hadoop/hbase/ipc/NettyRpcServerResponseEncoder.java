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
package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.util.NettyUnsafeUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.buffer.Unpooled;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPromise;

/**
 * Encoder for {@link RpcResponse}.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyRpcServerResponseEncoder extends ChannelOutboundHandlerAdapter {

  private static final ConnectionClosedException EXCEPTION =
    new ConnectionClosedException("Channel outbound bytes exceeded fatal threshold");
  static final String NAME = "NettyRpcServerResponseEncoder";

  private final NettyRpcServer rpcServer;
  private final NettyServerRpcConnection conn;
  private final MetricsHBaseServer metrics;

  NettyRpcServerResponseEncoder(NettyRpcServer rpcServer, NettyServerRpcConnection conn,
    MetricsHBaseServer metrics) {
    this.rpcServer = rpcServer;
    this.conn = conn;
    this.metrics = metrics;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
    throws Exception {

    // drop the message if fatal threshold is reached, as the connection will be closed
    if (handleFatalThreshold(ctx)) {
      promise.setFailure(EXCEPTION);
      return;
    }

    if (msg instanceof RpcResponse) {
      RpcResponse resp = (RpcResponse) msg;
      BufferChain buf = resp.getResponse();
      ctx.write(Unpooled.wrappedBuffer(buf.getBuffers()), promise).addListener(f -> {
        resp.done();
        if (f.isSuccess()) {
          metrics.sentBytes(buf.size());
        }
      });
    } else {
      ctx.write(msg, promise);
    }
  }

  private boolean handleFatalThreshold(ChannelHandlerContext ctx) {
    int fatalThreshold = rpcServer.getWriteBufferFatalThreshold();
    if (fatalThreshold <= 0) {
      return false;
    }

    Channel channel = ctx.channel();
    long outboundBytes = NettyUnsafeUtils.getTotalPendingOutboundBytes(channel);
    if (outboundBytes < fatalThreshold) {
      return false;
    }

    if (conn.isConnectionOpen()) {
      metrics.maxOutboundBytesExceeded();
      RpcServer.LOG.warn(
        "{}: Closing connection because outbound buffer size of {} exceeds fatal threshold of {}",
        channel.remoteAddress(), outboundBytes, fatalThreshold);
      conn.abort();
    }

    return true;

  }
}
