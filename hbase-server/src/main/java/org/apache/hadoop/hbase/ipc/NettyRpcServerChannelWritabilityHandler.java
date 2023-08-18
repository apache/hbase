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
import java.util.function.IntSupplier;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NettyUnsafeUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelDuplexHandler;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPromise;
import org.apache.hbase.thirdparty.io.netty.util.ReferenceCountUtil;

/**
 * Handler to enforce writability protections on our server channels: <br>
 * - Responds to channel writability events, which are triggered when the total pending bytes for a
 * channel passes configured high and low watermarks. When high watermark is exceeded, the channel
 * is setAutoRead(false). This way, we won't accept new requests from the client until some pending
 * outbound bytes are successfully received by the client.<br>
 * - Pre-processes any channel write requests. If the total pending outbound bytes exceeds a fatal
 * threshold, the channel is forcefully closed and the write is set to failed. This handler should
 * be the last handler in the pipeline so that it's the first handler to receive any messages sent
 * to channel.write() or channel.writeAndFlush().
 */
@InterfaceAudience.Private
public class NettyRpcServerChannelWritabilityHandler extends ChannelDuplexHandler {

  static final String NAME = "NettyRpcServerChannelWritabilityHandler";

  private final MetricsHBaseServer metrics;
  private final IntSupplier pendingBytesFatalThreshold;
  private final BooleanSupplier isWritabilityBackpressureEnabled;

  private boolean writable = true;
  private long unwritableStartTime;

  NettyRpcServerChannelWritabilityHandler(MetricsHBaseServer metrics,
    IntSupplier pendingBytesFatalThreshold, BooleanSupplier isWritabilityBackpressureEnabled) {
    this.metrics = metrics;
    this.pendingBytesFatalThreshold = pendingBytesFatalThreshold;
    this.isWritabilityBackpressureEnabled = isWritabilityBackpressureEnabled;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
    throws Exception {
    if (handleFatalThreshold(ctx)) {
      promise.setFailure(
        new ConnectionClosedException("Channel outbound bytes exceeded fatal threshold"));
      if (msg instanceof RpcResponse) {
        ((RpcResponse) msg).done();
      } else {
        ReferenceCountUtil.release(msg);
      }
      return;
    }
    ctx.write(msg, promise);
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (isWritabilityBackpressureEnabled.getAsBoolean()) {
      handleWritabilityChanged(ctx);
    }
    ctx.fireChannelWritabilityChanged();
  }

  private boolean handleFatalThreshold(ChannelHandlerContext ctx) {
    int fatalThreshold = pendingBytesFatalThreshold.getAsInt();
    if (fatalThreshold <= 0) {
      return false;
    }

    Channel channel = ctx.channel();
    long outboundBytes = NettyUnsafeUtils.getTotalPendingOutboundBytes(channel);
    if (outboundBytes < fatalThreshold) {
      return false;
    }

    if (channel.isOpen()) {
      metrics.maxOutboundBytesExceeded();
      RpcServer.LOG.warn(
        "{}: Closing connection because outbound buffer size of {} exceeds fatal threshold of {}",
        channel.remoteAddress(), outboundBytes, fatalThreshold);
      NettyUnsafeUtils.closeImmediately(channel);
    }

    return true;
  }

  private void handleWritabilityChanged(ChannelHandlerContext ctx) {
    boolean oldWritableValue = this.writable;

    this.writable = ctx.channel().isWritable();
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
}
