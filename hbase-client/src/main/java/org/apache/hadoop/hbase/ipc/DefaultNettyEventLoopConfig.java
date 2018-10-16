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
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Pair;

/**
 * The default netty event loop config
 */
@InterfaceAudience.Private
class DefaultNettyEventLoopConfig {

  public static final Pair<EventLoopGroup, Class<? extends Channel>> GROUP_AND_CHANNEL_CLASS = Pair
      .<EventLoopGroup, Class<? extends Channel>> newPair(
        new NioEventLoopGroup(0,
            new DefaultThreadFactory("Default-IPC-NioEventLoopGroup", true, Thread.MAX_PRIORITY)),
        NioSocketChannel.class);
}
