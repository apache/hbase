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

import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.exceptions.SSLContextException;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;

@Category({ RPCTests.class, MediumTests.class })
public class TestNettyIPCSslFailure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNettyIPCSslFailure.class);

  private static final Configuration CONF = HBaseConfiguration.create();

  private NioEventLoopGroup group;

  private NettyRpcServer server;

  private NettyRpcClient client;

  private TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub;

  @Before
  public void setUp() throws IOException, SSLContextException {
    CONF.set(X509Util.HBASE_SERVER_NETTY_TLS_ENABLED, "true");
    CONF.unset(X509Util.TLS_CONFIG_KEYSTORE_LOCATION);
    group = new NioEventLoopGroup();
    server = new NettyRpcServer(null, getClass().getSimpleName(),
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1), true);
    NettyRpcClientConfigHelper.setEventLoopConfig(CONF, group, NioSocketChannel.class);
    client = new NettyRpcClient(CONF);
    server.start();
    stub = TestProtobufRpcServiceImpl.newBlockingStub(client, server.getListenerAddress());
  }

  @After
  public void tearDown() throws Exception {
    Closeables.close(client, true);
    server.stop();
    group.shutdownGracefully().sync();
  }

  @Test(expected = ServiceException.class)
  public void testInitSslThrowsException() throws ServiceException {
    stub.echo(null, TestProtos.EchoRequestProto.newBuilder().setMessage("test").build())
      .getMessage();
  }

}
