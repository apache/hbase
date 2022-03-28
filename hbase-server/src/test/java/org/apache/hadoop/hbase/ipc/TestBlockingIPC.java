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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ RPCTests.class, MediumTests.class })
public class TestBlockingIPC extends AbstractTestIPC {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBlockingIPC.class);

  @Override protected RpcServer createRpcServer(Server server, String name,
      List<RpcServer.BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
      Configuration conf, RpcScheduler scheduler) throws IOException {
    return RpcServerFactory.createRpcServer(server, name, services, bindAddress, conf, scheduler);
  }

  @Override
  protected BlockingRpcClient createRpcClientNoCodec(Configuration conf) {
    return new BlockingRpcClient(conf) {
      @Override
      Codec getCodec() {
        return null;
      }
    };
  }

  @Override
  protected BlockingRpcClient createRpcClient(Configuration conf) {
    return new BlockingRpcClient(conf);
  }

  @Override
  protected BlockingRpcClient createRpcClientRTEDuringConnectionSetup(Configuration conf)
      throws IOException {
    return new BlockingRpcClient(conf) {

      @Override
      boolean isTcpNoDelay() {
        throw new RuntimeException("Injected fault");
      }
    };
  }

  private static class TestFailingRpcServer extends SimpleRpcServer {

    TestFailingRpcServer(Server server, String name,
        List<RpcServer.BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
        Configuration conf, RpcScheduler scheduler) throws IOException {
      super(server, name, services, bindAddress, conf, scheduler, true);
    }

    final class FailingConnection extends SimpleServerRpcConnection {
      private FailingConnection(TestFailingRpcServer rpcServer, SocketChannel channel,
          long lastContact) {
        super(rpcServer, channel, lastContact);
      }

      @Override
      public void processRequest(ByteBuff buf) throws IOException, InterruptedException {
        // this will throw exception after the connection header is read, and an RPC is sent
        // from client
        throw new DoNotRetryIOException("Failing for test");
      }
    }

    @Override
    protected SimpleServerRpcConnection getConnection(SocketChannel channel, long time) {
      return new FailingConnection(this, channel, time);
    }
  }

  @Override
  protected RpcServer createTestFailingRpcServer(Server server, String name,
      List<RpcServer.BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
      Configuration conf, RpcScheduler scheduler) throws IOException {
    return new TestFailingRpcServer(server, name, services, bindAddress, conf, scheduler);
  }
}
