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

import java.util.concurrent.Semaphore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

@Category(SmallTests.class)
public class TestAdaptiveFastPathRWQueueRpcExecutor {

  private static final Semaphore blocker = new Semaphore(0);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAdaptiveFastPathRWQueueRpcExecutor.class);

  @Test
  public void testInitialization() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.setFloat("hbase.ipc.server.callqueue.read.ratio", 0.5f);
    conf.setFloat("hbase.ipc.server.callqueue.scan.ratio", 0.5f);
    conf.setFloat("hbase.ipc.server.callqueue.handler.factor", 0.1f);
    conf.setFloat("hbase.ipc.server.callqueue.fastpath.adaptive.ratio", 0.1f);

    AdaptiveFastPathRWQueueRpcExecutor executor =
      new AdaptiveFastPathRWQueueRpcExecutor("testInitialization", 100, 250, null, conf, null);
    executor.start(0);
    Thread.sleep(1000);

    // When the adaptive ratio is 0.1, then the shared stack should contain
    // 25 * 0.1 + 25 * 0.1 + 50 * 0.1 = 9 handlers.
    Assert.assertEquals(23, executor.getReadStackLength());
    Assert.assertEquals(23, executor.getScanStackLength());
    Assert.assertEquals(45, executor.getWriteStackLength());
    Assert.assertEquals(9, executor.getSharedStackLength());
  }

  @Test
  public void testInvalidRatio() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.setFloat("hbase.ipc.server.callqueue.read.ratio", 0.5f);
    conf.setFloat("hbase.ipc.server.callqueue.scan.ratio", 0.5f);
    conf.setFloat("hbase.ipc.server.callqueue.handler.factor", 0.1f);
    conf.setFloat("hbase.ipc.server.callqueue.fastpath.adaptive.ratio", -0.5f);

    AdaptiveFastPathRWQueueRpcExecutor executor =
      new AdaptiveFastPathRWQueueRpcExecutor("testInvalidRatio", 100, 250, null, conf, null);
    executor.start(0);
    Thread.sleep(1000);

    // If the adaptive ratio is invalid, we just use the default ratio 0, with which the shared
    // stack should be empty.
    Assert.assertEquals(25, executor.getReadStackLength());
    Assert.assertEquals(25, executor.getScanStackLength());
    Assert.assertEquals(50, executor.getWriteStackLength());
    Assert.assertEquals(0, executor.getSharedStackLength());
  }

  @Test
  public void testCustomRatio() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.setFloat("hbase.ipc.server.callqueue.read.ratio", 0.5f);
    conf.setFloat("hbase.ipc.server.callqueue.scan.ratio", 0.5f);
    conf.setFloat("hbase.ipc.server.callqueue.handler.factor", 0.1f);
    conf.setFloat("hbase.ipc.server.callqueue.fastpath.adaptive.ratio", 0.2f);

    AdaptiveFastPathRWQueueRpcExecutor executor =
      new AdaptiveFastPathRWQueueRpcExecutor("testCustomRatio", 100, 250, null, conf, null);
    executor.start(0);
    Thread.sleep(1000);

    // When the adaptive ratio is 0.2, then the shared stack should contain
    // 25 * 0.2 + 25 * 0.2 + 50 * 0.2 = 20 handlers.
    Assert.assertEquals(20, executor.getReadStackLength());
    Assert.assertEquals(20, executor.getScanStackLength());
    Assert.assertEquals(40, executor.getWriteStackLength());
    Assert.assertEquals(20, executor.getSharedStackLength());
  }

  @Test
  public void testActiveHandlerCount() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.setFloat("hbase.ipc.server.callqueue.read.ratio", 0.5f);
    conf.setFloat("hbase.ipc.server.callqueue.scan.ratio", 0.5f);
    conf.setFloat("hbase.ipc.server.callqueue.handler.factor", 0.1f);
    conf.setFloat("hbase.ipc.server.callqueue.fastpath.adaptive.ratio", 0.2f);

    AdaptiveFastPathRWQueueRpcExecutor executor =
      new AdaptiveFastPathRWQueueRpcExecutor("testInitialization", 100, 250, null, conf, null);
    executor.start(0);
    Thread.sleep(1000);

    RpcCall call = Mockito.mock(RpcCall.class);
    ClientProtos.ScanRequest scanRequest =
      ClientProtos.ScanRequest.newBuilder().getDefaultInstanceForType();

    Mockito.when(call.getParam()).thenReturn(scanRequest);

    // When the adaptive ratio is 0.2, then the shared stack should contain
    // 25 * 0.2 + 25 * 0.2 + 50 * 0.2 = 20 handlers.
    // We send 30 CallRunner with mocked scan requests here.

    for (int i = 0; i < 30; i++) {
      CallRunner temp = new DummyCallRunner(null, call);
      executor.dispatch(temp);
    }
    // Wait for the dummy CallRunner being executed.
    Thread.sleep(2000);

    Assert.assertEquals(0, executor.getScanStackLength());
    Assert.assertEquals(10, executor.getSharedStackLength());
    Assert.assertEquals(40, executor.getWriteStackLength());
    Assert.assertEquals(20, executor.getReadStackLength());
    Assert.assertEquals(30, executor.getActiveScanHandlerCount()
      + executor.getActiveWriteHandlerCount() + executor.getActiveReadHandlerCount());
  }

  static class DummyCallRunner extends CallRunner {

    DummyCallRunner(RpcServerInterface rpcServer, RpcCall call) {
      super(rpcServer, call);
    }

    @Override
    public void run() {
      try {
        blocker.acquire();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
