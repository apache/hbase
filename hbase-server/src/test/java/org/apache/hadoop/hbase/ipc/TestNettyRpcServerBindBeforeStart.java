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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Verifies that NettyRpcServer does not accept incoming connections until start() is called. This
 * closes the race window between the RPC port being bound and the server completing
 * security/authorization initialization, during which netty workers could otherwise run handler
 * code that reaches into UserGroupInformation before the main thread has logged in.
 */
@Tag(RPCTests.TAG)
@Tag(MediumTests.TAG)
public class TestNettyRpcServerBindBeforeStart {

  private RpcServer server;

  @BeforeEach
  public void setUp() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, NettyRpcServer.class.getName());
    server = RpcServerFactory.createRpcServer(null, "testBindBeforeStart",
      Lists.newArrayList(new BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), conf, new FifoRpcScheduler(conf, 1));
  }

  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void noConnectionAcceptedBeforeStart() throws Exception {
    int port = server.getListenerAddress().getPort();
    assertTrue(port > 0);

    // Server is bound but start() has not been called. Netty must not accept this child channel.
    try (Socket client = connectAndSend(port)) {
      Thread.sleep(1000);
      assertEquals(0, server.getNumOpenConnections(),
        "NettyRpcServer must not accept connections before start()");
    }

    // After start(), connections should be accepted.
    server.start();
    try (Socket client = connectAndSend(port)) {
      Waiter.waitFor(server.getConf(), 2000, () -> server.getNumOpenConnections() > 0);
    }
  }

  private static Socket connectAndSend(int port) throws IOException {
    Socket client = new Socket("localhost", port);
    client.getOutputStream().write(new byte[] { 'H', 'B', 'a', 's' });
    client.getOutputStream().flush();
    return client;
  }
}
