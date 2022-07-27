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
import java.net.SocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Netty client for the requests and responses.
 * @since 2.0.0
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class NettyRpcClient extends AbstractRpcClient<NettyRpcConnection> {

  final EventLoopGroup group;

  final Class<? extends Channel> channelClass;

  private final boolean shutdownGroupWhenClose;

  public NettyRpcClient(Configuration configuration, String clusterId, SocketAddress localAddress,
    MetricsConnection metrics) {
    super(configuration, clusterId, localAddress, metrics);
    Pair<EventLoopGroup, Class<? extends Channel>> groupAndChannelClass =
      NettyRpcClientConfigHelper.getEventLoopConfig(conf);
    if (groupAndChannelClass == null) {
      // Use our own EventLoopGroup.
      int threadCount =
        conf.getInt(NettyRpcClientConfigHelper.HBASE_NETTY_EVENTLOOP_RPCCLIENT_THREADCOUNT_KEY, 0);
      this.group = new NioEventLoopGroup(threadCount,
        new DefaultThreadFactory("RPCClient(own)-NioEventLoopGroup", true, Thread.NORM_PRIORITY));
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
