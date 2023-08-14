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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
import org.apache.hadoop.hbase.util.NettyUnsafeUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOption;
import org.apache.hbase.thirdparty.io.netty.util.AttributeKey;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * RpcConnection implementation for netty rpc server.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyServerRpcConnection extends ServerRpcConnection<NettyRpcServer> {

  private static final AttributeKey<NettyServerRpcConnection> ATTR =
    AttributeKey.newInstance("connection");
  final Channel channel;
  private boolean writable = true;
  private long unwritableStartTime;

  NettyServerRpcConnection(NettyRpcServer rpcServer, Channel channel) {
    super(rpcServer);
    this.channel = channel;
    channel.attr(ATTR).set(this);
    rpcServer.allChannels.add(channel);
    NettyRpcServer.LOG.trace("Connection {}; # active connections={}", channel.remoteAddress(),
      rpcServer.allChannels.size() - 1);
    // register close hook to release resources
    NettyFutureUtils.addListener(channel.closeFuture(), f -> {
      disposeSasl();
      callCleanupIfNeeded();
      NettyRpcServer.LOG.trace("Disconnection {}; # active connections={}", channel.remoteAddress(),
        rpcServer.allChannels.size() - 1);
      rpcServer.allChannels.remove(channel);
    });
    InetSocketAddress inetSocketAddress = ((InetSocketAddress) channel.remoteAddress());
    this.addr = inetSocketAddress.getAddress();
    if (addr == null) {
      this.hostAddress = "*Unknown*";
    } else {
      this.hostAddress = inetSocketAddress.getAddress().getHostAddress();
    }
    this.remotePort = inetSocketAddress.getPort();
  }

  static NettyServerRpcConnection get(Channel channel) {
    return channel.attr(ATTR).get();
  }

  void setupHandler() {
    channel.pipeline()
      .addBefore(NettyRpcServerResponseEncoder.NAME, "frameDecoder",
        new NettyRpcFrameDecoder(rpcServer.maxRequestSize, this))
      .addBefore(NettyRpcServerResponseEncoder.NAME, "decoder",
        new NettyRpcServerRequestDecoder(rpcServer.metrics, this));
  }

  void process(ByteBuf buf) throws IOException, InterruptedException {
    if (skipInitialSaslHandshake) {
      skipInitialSaslHandshake = false;
      buf.release();
      return;
    }
    this.callCleanup = () -> buf.release();
    ByteBuff byteBuff = new SingleByteBuff(buf.nioBuffer());
    try {
      processOneRpc(byteBuff);
    } catch (Exception e) {
      callCleanupIfNeeded();
      throw e;
    } finally {
      this.callCleanup = null;
    }
  }

  /**
   * Sets the writable state on the connection, and tracks metrics around time spent unwritable.
   * When unwritable, we setAutoRead(false) so that the server does not read any more bytes from the
   * client until it's able to flush some outbound bytes first.
   */
  void setWritable(boolean newWritableValue) {
    assert channel.eventLoop().inEventLoop();

    if (!rpcServer.isWriteBufferWaterMarkEnabled()) {
      return;
    }

    boolean oldWritableValue = this.writable;
    this.writable = newWritableValue;
    channel.config().setAutoRead(newWritableValue);

    if (!oldWritableValue && newWritableValue) {
      // changing from not writable to writable, update metrics
      rpcServer.getMetrics()
        .unwritableTime(EnvironmentEdgeManager.currentTime() - unwritableStartTime);
      unwritableStartTime = 0;
    } else if (oldWritableValue && !newWritableValue) {
      // changing from writable to non-writable, set start time
      unwritableStartTime = EnvironmentEdgeManager.currentTime();
    }
  }

  /**
   * Immediately and forcibly closes the connection. To be used only from the event loop and only in
   * cases where a more graceful close is not possible.
   */
  void abort() {
    assert channel.eventLoop().inEventLoop();

    // We need to forcefully abort, because otherwise memory will continue to build up
    // while graceful close is executed (dependent on handlers).
    // Setting SO_LINGER to 0 ensures that the socket is closed immediately, and we do not wait for
    // the client to acknowledge. closeDirect skips any handlers which may delay the close, such
    // as SslHandler which tries to send a close_notify and wait for reply from client.
    channel.config().setOption(ChannelOption.SO_LINGER, 0);
    NettyUnsafeUtils.closeDirect(channel);
  }

  @Override
  public synchronized void close() {
    channel.close();
  }

  @Override
  public boolean isConnectionOpen() {
    return channel.isOpen();
  }

  @Override
  public NettyServerCall createCall(int id, final BlockingService service,
    final MethodDescriptor md, RequestHeader header, Message param, CellScanner cellScanner,
    long size, final InetAddress remoteAddress, int timeout, CallCleanup reqCleanup) {
    return new NettyServerCall(id, service, md, header, param, cellScanner, this, size,
      remoteAddress, EnvironmentEdgeManager.currentTime(), timeout, this.rpcServer.bbAllocator,
      this.rpcServer.cellBlockBuilder, reqCleanup);
  }

  @Override
  protected void doRespond(RpcResponse resp) {
    NettyFutureUtils.safeWriteAndFlush(channel, resp);
  }

}
