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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandlerImpl;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({RPCTests.class, SmallTests.class})
public class TestCallRunner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCallRunner.class);

  /**
   * Does nothing but exercise a {@link CallRunner} outside of {@link RpcServer} context.
   */
  @Test
  public void testSimpleCall() {
    RpcServerInterface mockRpcServer = Mockito.mock(RpcServerInterface.class);
    Mockito.when(mockRpcServer.isStarted()).thenReturn(true);
    ServerCall mockCall = Mockito.mock(ServerCall.class);
    CallRunner cr = new CallRunner(mockRpcServer, mockCall);
    cr.setStatus(new MonitoredRPCHandlerImpl());
    cr.run();
  }

  @Test
  public void testCallCleanup() {
    RpcServerInterface mockRpcServer = Mockito.mock(RpcServerInterface.class);
    Mockito.when(mockRpcServer.isStarted()).thenReturn(true);
    ServerCall mockCall = Mockito.mock(ServerCall.class);
    Mockito.when(mockCall.disconnectSince()).thenReturn(1L);

    CallRunner cr = new CallRunner(mockRpcServer, mockCall);
    cr.setStatus(new MonitoredRPCHandlerImpl());
    cr.run();
    Mockito.verify(mockCall, Mockito.times(1)).cleanup();
  }

  @Test
  public void testCallRunnerDrop() {
    RpcServerInterface mockRpcServer = Mockito.mock(RpcServerInterface.class);
    Mockito.when(mockRpcServer.isStarted()).thenReturn(true);
    ServerCall mockCall = Mockito.mock(ServerCall.class);
    Mockito.when(mockCall.disconnectSince()).thenReturn(1L);

    CallRunner cr = new CallRunner(mockRpcServer, mockCall);
    cr.setStatus(new MonitoredRPCHandlerImpl());
    cr.drop();
    Mockito.verify(mockCall, Mockito.times(1)).cleanup();
  }
}
