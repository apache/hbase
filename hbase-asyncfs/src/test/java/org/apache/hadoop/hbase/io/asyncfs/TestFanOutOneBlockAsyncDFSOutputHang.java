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
package org.apache.hadoop.hbase.io.asyncfs;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Testcase for HBASE-26679, here we introduce a separate test class and not put the testcase in
 * {@link TestFanOutOneBlockAsyncDFSOutput} because we will send heartbeat to DN when there is no
 * out going packet, the timeout is controlled by
 * {@link TestFanOutOneBlockAsyncDFSOutput#READ_TIMEOUT_MS},which is 2 seconds, it will keep sending
 * package out and DN will respond immedately and then mess up the testing handler added by us. So
 * in this test class we use the default value for timeout which is 60 seconds and it is enough for
 * this test.
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestFanOutOneBlockAsyncDFSOutputHang extends AsyncFSTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFanOutOneBlockAsyncDFSOutputHang.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFanOutOneBlockAsyncDFSOutputHang.class);

  private static DistributedFileSystem FS;

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  private static StreamSlowMonitor MONITOR;

  private static FanOutOneBlockAsyncDFSOutput OUT;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    startMiniDFSCluster(2);
    FS = CLUSTER.getFileSystem();
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    CHANNEL_CLASS = NioSocketChannel.class;
    MONITOR = StreamSlowMonitor.create(UTIL.getConfiguration(), "testMonitor");
    Path f = new Path("/testHang");
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    OUT = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true, false, (short) 2,
      FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS, MONITOR);
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    if (OUT != null) {
      OUT.recoverAndClose(null);
    }
    if (EVENT_LOOP_GROUP != null) {
      EVENT_LOOP_GROUP.shutdownGracefully().sync();
    }
    shutdownMiniDFSCluster();
  }

  /**
   * <pre>
   * This test is for HBASE-26679. Consider there are two dataNodes: dn1 and dn2,dn2 is a slow DN.
   * The threads sequence before HBASE-26679 is:
   * 1.We write some data to {@link FanOutOneBlockAsyncDFSOutput} and then flush it, there are one
   *   {@link FanOutOneBlockAsyncDFSOutput.Callback} in
   *   {@link FanOutOneBlockAsyncDFSOutput#waitingAckQueue}.
   * 2.The ack from dn1 arrives firstly and triggers Netty to invoke
   *   {@link FanOutOneBlockAsyncDFSOutput#completed} with dn1's channel, then in
   *   {@link FanOutOneBlockAsyncDFSOutput#completed}, dn1's channel is removed from
   *   {@link FanOutOneBlockAsyncDFSOutput.Callback#unfinishedReplicas}.
   * 3.But dn2 responds slowly, before dn2 sending ack,dn1 is shut down or have a exception,
   *   so {@link FanOutOneBlockAsyncDFSOutput#failed} is triggered by Netty with dn1's channel,
   *   and because the {@link FanOutOneBlockAsyncDFSOutput.Callback#unfinishedReplicas} does not
   *   contain dn1's channel,the {@link FanOutOneBlockAsyncDFSOutput.Callback} is skipped in
   *   {@link FanOutOneBlockAsyncDFSOutput#failed} method,and
   *   {@link FanOutOneBlockAsyncDFSOutput#state} is set to
   *   {@link FanOutOneBlockAsyncDFSOutput.State#BROKEN},and dn1,dn2 are all closed at the end of
   *   {@link FanOutOneBlockAsyncDFSOutput#failed}.
   * 4.{@link FanOutOneBlockAsyncDFSOutput#failed} is triggered again by dn2 because it is closed,
   *   but because {@link FanOutOneBlockAsyncDFSOutput#state} is already
   *   {@link FanOutOneBlockAsyncDFSOutput.State#BROKEN},the whole
   *   {@link FanOutOneBlockAsyncDFSOutput#failed} is skipped. So wait on the future
   *   returned by {@link FanOutOneBlockAsyncDFSOutput#flush} would be stuck for ever.
   * After HBASE-26679, for above step 4,even if the {@link FanOutOneBlockAsyncDFSOutput#state}
   * is already {@link FanOutOneBlockAsyncDFSOutput.State#BROKEN}, we would still try to trigger
   * {@link FanOutOneBlockAsyncDFSOutput.Callback#future}.
   * </pre>
   */
  @Test
  public void testFlushHangWhenOneDataNodeFailedBeforeOtherDataNodeAck() throws Exception {

    DataNodeProperties firstDataNodeProperties = null;
    try {

      final CyclicBarrier dn1AckReceivedCyclicBarrier = new CyclicBarrier(2);
      Map<Channel, DatanodeInfo> datanodeInfoMap = OUT.getDatanodeInfoMap();
      Iterator<Map.Entry<Channel, DatanodeInfo>> iterator = datanodeInfoMap.entrySet().iterator();
      assertTrue(iterator.hasNext());
      Map.Entry<Channel, DatanodeInfo> dn1Entry = iterator.next();
      Channel dn1Channel = dn1Entry.getKey();
      DatanodeInfo dn1DatanodeInfo = dn1Entry.getValue();
      final List<String> protobufDecoderNames = new ArrayList<String>();
      dn1Channel.pipeline().forEach((entry) -> {
        if (ProtobufDecoder.class.isInstance(entry.getValue())) {
          protobufDecoderNames.add(entry.getKey());
        }
      });
      assertTrue(protobufDecoderNames.size() == 1);
      dn1Channel.pipeline().addAfter(protobufDecoderNames.get(0), "dn1AckReceivedHandler",
        new ChannelInboundHandlerAdapter() {
          @Override
          public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            super.channelRead(ctx, msg);
            dn1AckReceivedCyclicBarrier.await();
          }
        });

      assertTrue(iterator.hasNext());
      Map.Entry<Channel, DatanodeInfo> dn2Entry = iterator.next();
      Channel dn2Channel = dn2Entry.getKey();

      /**
       * Here we add a {@link ChannelInboundHandlerAdapter} to eat all the responses to simulate a
       * slow dn2.
       */
      dn2Channel.pipeline().addFirst(new ChannelInboundHandlerAdapter() {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
          if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
          }
        }
      });

      byte[] b = new byte[10];
      Bytes.random(b);
      OUT.write(b, 0, b.length);
      CompletableFuture<Long> future = OUT.flush(false);
      /**
       * Wait for ack from dn1.
       */
      dn1AckReceivedCyclicBarrier.await();
      /**
       * First ack is received from dn1,we could stop dn1 now.
       */
      firstDataNodeProperties = findAndKillFirstDataNode(dn1DatanodeInfo);
      assertTrue(firstDataNodeProperties != null);
      try {
        /**
         * Before HBASE-26679,here we should be stuck, after HBASE-26679,we would fail soon with
         * {@link ExecutionException}.
         */
        future.get();
        fail();
      } catch (ExecutionException e) {
        assertTrue(e != null);
        LOG.info("expected exception caught when get future", e);
      }
      /**
       * Make sure all the data node channel are closed.
       */
      datanodeInfoMap.keySet().forEach(ch -> {
        try {
          ch.closeFuture().get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });
    } finally {
      if (firstDataNodeProperties != null) {
        CLUSTER.restartDataNode(firstDataNodeProperties);
      }
    }
  }

  private static DataNodeProperties findAndKillFirstDataNode(DatanodeInfo firstDatanodeInfo) {
    assertTrue(firstDatanodeInfo != null);
    ArrayList<DataNode> dataNodes = CLUSTER.getDataNodes();
    ArrayList<Integer> foundIndexes = new ArrayList<Integer>();
    int index = 0;
    for (DataNode dataNode : dataNodes) {
      if (firstDatanodeInfo.getXferAddr().equals(dataNode.getDatanodeId().getXferAddr())) {
        foundIndexes.add(index);
      }
      index++;
    }
    assertTrue(foundIndexes.size() == 1);
    return CLUSTER.stopDataNode(foundIndexes.get(0));
  }

}
