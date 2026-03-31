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
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;

@Category({ RPCTests.class, MediumTests.class })
public class TestNettyChannelWritability {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNettyChannelWritability.class);

  private static final MetricsAssertHelper METRICS_ASSERT =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private static final byte[] CELL_BYTES = Bytes.toBytes("xyz");
  private static final KeyValue CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);

  /**
   * Test that we properly send configured watermarks to netty, and trigger setWritable when
   * necessary.
   */
  @Test
  public void testNettyWritableWatermarks() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(NettyRpcServer.CHANNEL_WRITABLE_LOW_WATERMARK_KEY, 1);
    conf.setInt(NettyRpcServer.CHANNEL_WRITABLE_HIGH_WATERMARK_KEY, 2);

    NettyRpcServer rpcServer = createRpcServer(conf, 0);
    try {
      sendAndReceive(conf, rpcServer, 5);
      METRICS_ASSERT.assertCounterGt("unwritableTime_numOps", 0,
        rpcServer.metrics.getMetricsSource());
    } finally {
      rpcServer.stop();
    }
  }

  /**
   * Test that our fatal watermark is honored, which requires artificially causing some queueing so
   * that pendingOutboundBytes increases.
   */
  @Test
  public void testNettyWritableFatalThreshold() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(NettyRpcServer.CHANNEL_WRITABLE_FATAL_WATERMARK_KEY, 1);

    // flushAfter is 3 here, with requestCount 5 below. If we never flush, the WriteTasks will sit
    // in the eventloop. So we flush a few at once, which will ensure that we hit fatal threshold
    NettyRpcServer rpcServer = createRpcServer(conf, 3);
    try {
      CompletionException exception =
        assertThrows(CompletionException.class, () -> sendAndReceive(conf, rpcServer, 5));
      assertTrue(exception.getCause().getCause() instanceof ServiceException);
      METRICS_ASSERT.assertCounterGt("maxOutboundBytesExceeded", 0,
        rpcServer.metrics.getMetricsSource());
    } finally {
      rpcServer.stop();
    }
  }

  private void sendAndReceive(Configuration conf, NettyRpcServer rpcServer, int requestCount)
    throws Exception {
    List<ExtendedCell> cells = new ArrayList<>();
    int count = 3;
    for (int i = 0; i < count; i++) {
      cells.add(CELL);
    }

    try (NettyRpcClient client = new NettyRpcClient(conf)) {
      rpcServer.start();
      TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub =
        newBlockingStub(client, rpcServer.getListenerAddress());
      CompletableFuture<Void>[] futures = new CompletableFuture[requestCount];
      for (int i = 0; i < requestCount; i++) {
        futures[i] = CompletableFuture.runAsync(() -> {
          try {
            sendMessage(cells, stub);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }
      CompletableFuture.allOf(futures).join();
    }
  }

  private void sendMessage(List<ExtendedCell> cells,
    TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub) throws Exception {
    HBaseRpcController pcrc =
      new HBaseRpcControllerImpl(PrivateCellUtil.createExtendedCellScanner(cells));
    String message = "hello";
    assertEquals(message,
      stub.echo(pcrc, TestProtos.EchoRequestProto.newBuilder().setMessage(message).build())
        .getMessage());
    int index = 0;
    CellScanner cellScanner = pcrc.cellScanner();
    assertNotNull(cellScanner);
    while (cellScanner.advance()) {
      assertEquals(CELL, cellScanner.current());
      index++;
    }
    assertEquals(cells.size(), index);
  }

  private NettyRpcServer createRpcServer(Configuration conf, int flushAfter) throws IOException {
    String name = "testRpcServer";
    ArrayList<RpcServer.BlockingServiceAndInterface> services =
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null));

    InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);
    FifoRpcScheduler scheduler = new FifoRpcScheduler(conf, 1);

    AtomicInteger writeCount = new AtomicInteger(0);

    return new NettyRpcServer(null, name, services, bindAddress, conf, scheduler, true) {
      @Override
      protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
        return new NettyServerRpcConnection(this, channel) {
          @Override
          protected void doRespond(RpcResponse resp) {
            if (writeCount.incrementAndGet() >= flushAfter) {
              super.doRespond(resp);
            } else {
              channel.write(resp);
            }
          }
        };
      }
    };
  }
}
