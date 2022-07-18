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
package org.apache.hadoop.hbase.util;

import java.util.concurrent.ThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.ServerChannel;
import org.apache.hbase.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.epoll.EpollServerSocketChannel;
import org.apache.hbase.thirdparty.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Event loop group related config.
 */
@InterfaceAudience.Private
public class NettyEventLoopGroupConfig {

  public static final String NETTY_WORKER_COUNT_KEY = "hbase.netty.worker.count";
  public static final int DEFAULT_NETTY_WORKER_COUNT = 0;

  public static final String NETTY_NATIVETRANSPORT_KEY = "hbase.netty.nativetransport";
  public static final boolean DEFAULT_NETTY_NATIVETRANSPORT = true;

  private final EventLoopGroup group;

  private final Class<? extends ServerChannel> serverChannelClass;

  private final Class<? extends Channel> clientChannelClass;

  private static boolean useEpoll(Configuration conf) {
    // Config to enable native transport.
    final boolean epollEnabled =
      conf.getBoolean(NETTY_NATIVETRANSPORT_KEY, DEFAULT_NETTY_NATIVETRANSPORT);
    // Use the faster native epoll transport mechanism on linux if enabled and the
    // hardware architecture is either amd64 or aarch64. Netty is known to have native
    // epoll support for these combinations.
    return epollEnabled && JVM.isLinux() && (JVM.isAmd64() || JVM.isAarch64());
  }

  public NettyEventLoopGroupConfig(Configuration conf, String threadPoolName) {
    final boolean useEpoll = useEpoll(conf);
    final int workerCount = conf.getInt(NETTY_WORKER_COUNT_KEY,
      // For backwards compatibility we also need to consider
      // "hbase.netty.eventloop.rpcserver.thread.count"
      // if it is defined in site configuration instead.
      conf.getInt("hbase.netty.eventloop.rpcserver.thread.count", DEFAULT_NETTY_WORKER_COUNT));
    ThreadFactory eventLoopThreadFactory =
      new DefaultThreadFactory(threadPoolName, true, Thread.MAX_PRIORITY);
    if (useEpoll) {
      group = new EpollEventLoopGroup(workerCount, eventLoopThreadFactory);
      serverChannelClass = EpollServerSocketChannel.class;
      clientChannelClass = EpollSocketChannel.class;
    } else {
      group = new NioEventLoopGroup(workerCount, eventLoopThreadFactory);
      serverChannelClass = NioServerSocketChannel.class;
      clientChannelClass = NioSocketChannel.class;
    }
  }

  public EventLoopGroup group() {
    return group;
  }

  public Class<? extends ServerChannel> serverChannelClass() {
    return serverChannelClass;
  }

  public Class<? extends Channel> clientChannelClass() {
    return clientChannelClass;
  }
}
