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

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.WriteBufferWaterMark;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.net.SocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Netty client for the requests and responses.
 * @since 2.0.0
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class NettyRpcClient extends AbstractRpcClient<NettyRpcConnection> {

  final EventLoopGroup group;

  final Class<? extends Channel> channelClass;

  private final boolean shutdownGroupWhenClose;

  protected final WriteBufferWaterMark writeBufferWaterMark;

  protected static final String CLIENT_CONNECT_MAX_RETRIES = "hbase.ipc.client.connect.max.retries";
  protected static final String CLIENT_TCP_NODELAY = "hbase.ipc.client.tcpnodelay";
  protected static final String CLIENT_TCP_KEEPALIVE = "hbase.ipc.client.tcpkeepalive";
  protected static final String CLIENT_BUFFER_LOW_WATERMARK = "hbase.ipc.client.bufferlowwatermark";
  protected static final String CLIENT_BUFFER_HIGH_WATERMARK = "hbase.ipc.client.bufferhighwatermark";

  protected static final int DEFAULT_CLIENT_BUFFER_LOW_WATERMARK = 1024;
  protected static final int DEFAULT_CLIENT_BUFFER_HIGH_WATERMARK = 64 * 1024;

  public NettyRpcClient(Configuration configuration, String clusterId, SocketAddress localAddress,
      MetricsConnection metrics) {
    super(configuration, clusterId, localAddress, metrics);
    this.writeBufferWaterMark = new WriteBufferWaterMark(bufferLowWatermark, bufferHighWatermark);
    Pair<EventLoopGroup, Class<? extends Channel>> groupAndChannelClass = NettyRpcClientConfigHelper
        .getEventLoopConfig(conf);
    if (groupAndChannelClass == null) {
      // Use our own EventLoopGroup.
      this.group = new NioEventLoopGroup(0,
          new DefaultThreadFactory("IPC-NioEventLoopGroup", true, Thread.MAX_PRIORITY));
      this.channelClass = NioSocketChannel.class;
      this.shutdownGroupWhenClose = true;
    } else {
      this.group = groupAndChannelClass.getFirst();
      this.channelClass = groupAndChannelClass.getSecond();
      this.shutdownGroupWhenClose = false;
    }
  }

  /** Used in test only. */
  NettyRpcClient(Configuration configuration) {
    this(configuration, HConstants.CLUSTER_ID_DEFAULT, null, null);
  }

  @Override
  protected NettyRpcConnection createConnection(ConnectionId remoteId) throws IOException {
    return new NettyRpcConnection(this, remoteId);
  }

  @Override
  protected void closeInternal() {
    if (shutdownGroupWhenClose) {
      group.shutdownGracefully();
    }
  }
}
