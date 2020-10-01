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
package org.apache.hadoop.hbase;

import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.DNS;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.hbase.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.epoll.EpollServerSocketChannel;

@Category({ ClientTests.class, SmallTests.class })
public class TestNetty {

  private static final Logger LOG = LoggerFactory.getLogger(TestNetty.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestNetty.class);

  @Test
  public void test() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String hostname = DNS.getHostname(conf, DNS.ServerType.MASTER);
    LOG.info("hostname is {}", hostname);
    int port = 0;
    LOG.info("port is {}", port);
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    LOG.info("initial isa is {}", initialIsa);
    InetSocketAddress bindAddress =
      new InetSocketAddress(conf.get("hbase.master.ipc.address", hostname), port);
    LOG.info("bind address is {}", bindAddress);
    EpollEventLoopGroup group = new EpollEventLoopGroup();
    Channel serverChannel = new ServerBootstrap().group(group)
      .channel(EpollServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>() {

        @Override
        protected void initChannel(Channel ch) throws Exception {
          LOG.info("connected {}", ch);
          ch.close();
        }
      }).bind(bindAddress).sync().channel();
    LOG.info("server channel is {}", serverChannel);
    Thread.sleep(1000);
    serverChannel.close();
    group.shutdownGracefully();
  }
}
