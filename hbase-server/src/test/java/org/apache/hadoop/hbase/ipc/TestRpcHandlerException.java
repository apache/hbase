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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;

@Category({ RPCTests.class, SmallTests.class })
public class TestRpcHandlerException {

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

  /*
   * This is a unit test to make sure to abort region server when the number of Rpc handler thread
   * caught errors exceeds the threshold. Client will hang when RS aborts.
   */
  @Ignore
  @Test
  public void testRpcScheduler() throws IOException, InterruptedException {
    PriorityFunction qosFunction = mock(PriorityFunction.class);
    Abortable abortable = new AbortServer();
    RpcScheduler scheduler = new SimpleRpcScheduler(CONF, 2, 0, 0, qosFunction, abortable, 0);
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
