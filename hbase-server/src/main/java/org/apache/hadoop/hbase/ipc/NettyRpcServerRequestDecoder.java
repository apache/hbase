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

import java.util.function.BooleanSupplier;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;

/**
 * Decoder for rpc request.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyRpcServerRequestDecoder extends SimpleChannelInboundHandler<ByteBuf> {

  private final MetricsHBaseServer metrics;

  private final NettyServerRpcConnection connection;
  private final BooleanSupplier isWritabilityBackpressureEnabled;

  private boolean writable;
  private long unwritableStartTime;

  public NettyRpcServerRequestDecoder(MetricsHBaseServer metrics,
    NettyServerRpcConnection connection, BooleanSupplier isWritabilityBackpressureEnabled) {
    super(false);
    this.metrics = metrics;
    this.connection = connection;
    this.isWritabilityBackpressureEnabled = isWritabilityBackpressureEnabled;
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    // if backpressure is enabled, we want to manage the channel's setAutoRead based on
    // whether the channel is writable. We also track metrics about how much time the channel
    // spends unwritable.
    if (isWritabilityBackpressureEnabled.getAsBoolean()) {
      boolean oldWritableValue = this.writable;

      this.writable = ctx.channel().isWritable();
      ;
      ctx.channel().config().setAutoRead(this.writable);

      if (!oldWritableValue && this.writable) {
        // changing from not writable to writable, update metrics
        metrics.unwritableTime(EnvironmentEdgeManager.currentTime() - unwritableStartTime);
        unwritableStartTime = 0;
      } else if (oldWritableValue && !this.writable) {
        // changing from writable to non-writable, set start time
        unwritableStartTime = EnvironmentEdgeManager.currentTime();
      }
    }
    ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
    NettyRpcServer.LOG.warn("Connection {}; caught unexpected downstream exception.",
      ctx.channel().remoteAddress(), e);
    NettyFutureUtils.safeClose(ctx);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    // 4 bytes length field
    metrics.receivedBytes(msg.readableBytes() + 4);
    connection.process(msg);
  }
}
