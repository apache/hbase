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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Helper class for passing config to {@link NettyRpcClient}.
 * <p>
 * As hadoop Configuration can not pass an Object directly, we need to find a way to pass the
 * EventLoopGroup to {@code AsyncRpcClient} if we want to use a single {@code EventLoopGroup} for
 * the whole process.
 * @since 2.0.0
 */
@InterfaceAudience.Public
public final class NettyRpcClientConfigHelper {

  public static final String EVENT_LOOP_CONFIG = "hbase.rpc.client.event-loop.config";

  /**
   * Name of property to change netty rpc client eventloop thread count. Default is 0.
   * Tests may set this down from unlimited.
   */
  public static final String HBASE_NETTY_EVENTLOOP_RPCCLIENT_THREADCOUNT_KEY =
    "hbase.netty.eventloop.rpcclient.thread.count";

  private static final String CONFIG_NAME = "global-event-loop";

  private static final Map<String, Pair<EventLoopGroup, Class<? extends Channel>>>
    EVENT_LOOP_CONFIG_MAP = new HashMap<>();

  /**
   * Shutdown constructor.
   */
  private NettyRpcClientConfigHelper() {}

  /**
   * Set the EventLoopGroup and channel class for {@code AsyncRpcClient}.
   */
  public static void setEventLoopConfig(Configuration conf, EventLoopGroup group,
      Class<? extends Channel> channelClass) {
    Preconditions.checkNotNull(group, "group is null");
    Preconditions.checkNotNull(channelClass, "channel class is null");
    conf.set(EVENT_LOOP_CONFIG, CONFIG_NAME);
    EVENT_LOOP_CONFIG_MAP.put(CONFIG_NAME,
      Pair.<EventLoopGroup, Class<? extends Channel>> newPair(group, channelClass));
  }

  /**
   * The {@link NettyRpcClient} will create its own {@code NioEventLoopGroup}.
   */
  public static void createEventLoopPerClient(Configuration conf) {
    conf.set(EVENT_LOOP_CONFIG, "");
    EVENT_LOOP_CONFIG_MAP.clear();
  }

  private static volatile Pair<EventLoopGroup, Class<? extends Channel>> DEFAULT_EVENT_LOOP;

  private static Pair<EventLoopGroup, Class<? extends Channel>>
    getDefaultEventLoopConfig(Configuration conf) {
    Pair<EventLoopGroup, Class<? extends Channel>> eventLoop = DEFAULT_EVENT_LOOP;
    if (eventLoop != null) {
      return eventLoop;
    }
    synchronized (NettyRpcClientConfigHelper.class) {
      eventLoop = DEFAULT_EVENT_LOOP;
      if (eventLoop != null) {
        return eventLoop;
      }
      int threadCount = conf.getInt(HBASE_NETTY_EVENTLOOP_RPCCLIENT_THREADCOUNT_KEY, 0);
      eventLoop = new Pair<>(
        new NioEventLoopGroup(threadCount,
          new DefaultThreadFactory("RPCClient-NioEventLoopGroup", true, Thread.NORM_PRIORITY)),
        NioSocketChannel.class);
      DEFAULT_EVENT_LOOP = eventLoop;
    }
    return eventLoop;
  }

  static Pair<EventLoopGroup, Class<? extends Channel>> getEventLoopConfig(Configuration conf) {
    String name = conf.get(EVENT_LOOP_CONFIG);
    if (name == null) {
      return getDefaultEventLoopConfig(conf);
    }
    if (StringUtils.isBlank(name)) {
      return null;
    }
    return EVENT_LOOP_CONFIG_MAP.get(name);
  }
}
