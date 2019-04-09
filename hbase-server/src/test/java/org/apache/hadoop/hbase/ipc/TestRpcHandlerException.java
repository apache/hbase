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

import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;

@RunWith(Parameterized.class)
@Category({ RPCTests.class, SmallTests.class })
public class TestRpcHandlerException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRpcHandlerException.class);

  private final static Configuration CONF = HBaseConfiguration.create();

  /**
   * Tests that the rpc scheduler is called when requests arrive. When Rpc handler thread dies, the
   * client will hang and the test will fail. The test is meant to be a unit test to test the
   * behavior.
   */
  private class AbortServer implements Abortable {
    private boolean aborted = false;

    @Override
    public void abort(String why, Throwable e) {
      aborted = true;
    }

    @Override
    public boolean isAborted() {
      return aborted;
    }
  }

  @Parameters(name = "{index}: rpcServerImpl={0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[] { SimpleRpcServer.class.getName() },
        new Object[] { NettyRpcServer.class.getName() });
  }

  @Parameter(0)
  public String rpcServerImpl;

  /*
   * This is a unit test to make sure to abort region server when the number of Rpc handler thread
   * caught errors exceeds the threshold. Client will hang when RS aborts.
   */
  @Test
  public void testRpcScheduler() throws IOException, InterruptedException {
    PriorityFunction qosFunction = mock(PriorityFunction.class);
    Abortable abortable = new AbortServer();
    CONF.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, rpcServerImpl);
    RpcScheduler scheduler = new SimpleRpcScheduler(CONF, 2, 0, 0, 0, qosFunction, abortable, 0);
    RpcServer rpcServer = RpcServerFactory.createRpcServer(null, "testRpcServer",
        Lists.newArrayList(new BlockingServiceAndInterface((BlockingService) SERVICE, null)),
        new InetSocketAddress("localhost", 0), CONF, scheduler);
    try (BlockingRpcClient client = new BlockingRpcClient(CONF)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      stub.echo(null, EchoRequestProto.newBuilder().setMessage("hello").build());
    } catch (Throwable e) {
      assert (abortable.isAborted() == true);
    } finally {
      rpcServer.stop();
    }
  }

}
