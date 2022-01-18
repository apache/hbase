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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.Encryptor;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.asyncfs.monitor.ExcludeDatanodeManager;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.DataChecksum;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@Category({ MiscTests.class, MediumTests.class })
public class TestFanOutOneBlockAsyncDFSOutput extends AsyncFSTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFanOutOneBlockAsyncDFSOutput.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFanOutOneBlockAsyncDFSOutput.class);

  private static DistributedFileSystem FS;

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  private static int READ_TIMEOUT_MS = 2000;

  private static StreamSlowMonitor MONITOR;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, READ_TIMEOUT_MS);
    startMiniDFSCluster(3);
    FS = CLUSTER.getFileSystem();
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    CHANNEL_CLASS = NioSocketChannel.class;
    MONITOR = StreamSlowMonitor.create(UTIL.getConfiguration(), "testMonitor");
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    if (EVENT_LOOP_GROUP != null) {
      EVENT_LOOP_GROUP.shutdownGracefully().sync();
    }
    shutdownMiniDFSCluster();
  }

  static void writeAndVerify(FileSystem fs, Path f, AsyncFSOutput out)
    throws IOException, InterruptedException, ExecutionException {
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    byte[] b = new byte[10];
    Random rand = new Random(12345);
    // test pipelined flush
    for (int i = 0; i < 10; i++) {
      rand.nextBytes(b);
      out.write(b);
      futures.add(out.flush(false));
      futures.add(out.flush(false));
    }
    for (int i = 0; i < 10; i++) {
      assertEquals((i + 1) * b.length, futures.get(2 * i).join().longValue());
      assertEquals((i + 1) * b.length, futures.get(2 * i + 1).join().longValue());
    }
    out.close();
    assertEquals(b.length * 10, fs.getFileStatus(f).getLen());
    byte[] actual = new byte[b.length];
    rand.setSeed(12345);
    try (FSDataInputStream in = fs.open(f)) {
      for (int i = 0; i < 10; i++) {
        in.readFully(actual);
        rand.nextBytes(b);
        assertArrayEquals(b, actual);
      }
      assertEquals(-1, in.read());
    }
  }

  @Test
  public void test() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true,
      false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS, MONITOR);
    writeAndVerify(FS, f, out);
  }

  @Test
  public void testRecover() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true,
      false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS, MONITOR);
    byte[] b = new byte[10];
    ThreadLocalRandom.current().nextBytes(b);
    out.write(b, 0, b.length);
    out.flush(false).get();
    // restart one datanode which causes one connection broken
    CLUSTER.restartDataNode(0);
    out.write(b, 0, b.length);
    try {
      out.flush(false).get();
      fail("flush should fail");
    } catch (ExecutionException e) {
      // we restarted one datanode so the flush should fail
      LOG.info("expected exception caught", e);
    }
    out.recoverAndClose(null);
    assertEquals(b.length, FS.getFileStatus(f).getLen());
    byte[] actual = new byte[b.length];
    try (FSDataInputStream in = FS.open(f)) {
      in.readFully(actual);
    }
    assertArrayEquals(b, actual);
  }

  @Test
  public void testHeartbeat() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true,
      false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS, MONITOR);
    Thread.sleep(READ_TIMEOUT_MS * 2);
    // the connection to datanode should still alive.
    writeAndVerify(FS, f, out);
  }

  /**
   * This is important for fencing when recover from RS crash.
   */
  @Test
  public void testCreateParentFailed() throws IOException {
    Path f = new Path("/" + name.getMethodName() + "/test");
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    try {
      FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true, false, (short) 3,
        FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS, MONITOR);
      fail("should fail with parent does not exist");
    } catch (RemoteException e) {
      LOG.info("expected exception caught", e);
      assertThat(e.unwrapRemoteException(), instanceOf(FileNotFoundException.class));
    }
  }

  @Test
  public void testConnectToDatanodeFailed()
    throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
    InvocationTargetException, InterruptedException, NoSuchFieldException {
    Field xceiverServerDaemonField = DataNode.class.getDeclaredField("dataXceiverServer");
    xceiverServerDaemonField.setAccessible(true);
    Class<?> xceiverServerClass =
      Class.forName("org.apache.hadoop.hdfs.server.datanode.DataXceiverServer");
    Method numPeersMethod = xceiverServerClass.getDeclaredMethod("getNumPeers");
    numPeersMethod.setAccessible(true);
    // make one datanode broken
    DataNodeProperties dnProp = CLUSTER.stopDataNode(0);
    Path f = new Path("/test");
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    try (FanOutOneBlockAsyncDFSOutput output = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS,
      f, true, false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS, MONITOR)) {
      // should exclude the dead dn when retry so here we only have 2 DNs in pipeline
      assertEquals(2, output.getPipeline().length);
    } finally {
      CLUSTER.restartDataNode(dnProp);
    }
  }

  @Test
  public void testExcludeFailedConnectToDatanode()
    throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
    InvocationTargetException, InterruptedException, NoSuchFieldException {
    Field xceiverServerDaemonField = DataNode.class.getDeclaredField("dataXceiverServer");
    xceiverServerDaemonField.setAccessible(true);
    Class<?> xceiverServerClass =
      Class.forName("org.apache.hadoop.hdfs.server.datanode.DataXceiverServer");
    Method numPeersMethod = xceiverServerClass.getDeclaredMethod("getNumPeers");
    numPeersMethod.setAccessible(true);
    // make one datanode broken
    DataNodeProperties dnProp = CLUSTER.stopDataNode(0);
    Path f = new Path("/test");
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    ExcludeDatanodeManager excludeDatanodeManager =
      new ExcludeDatanodeManager(HBaseConfiguration.create());
    StreamSlowMonitor streamSlowDNsMonitor =
      excludeDatanodeManager.getStreamSlowMonitor("testMonitor");
    assertEquals(0, excludeDatanodeManager.getExcludeDNs().size());
    try (FanOutOneBlockAsyncDFSOutput output = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS,
      f, true, false, (short) 3, FS.getDefaultBlockSize(), eventLoop,
      CHANNEL_CLASS, streamSlowDNsMonitor)) {
      // should exclude the dead dn when retry so here we only have 2 DNs in pipeline
      assertEquals(2, output.getPipeline().length);
      assertEquals(1, excludeDatanodeManager.getExcludeDNs().size());
    } finally {
      CLUSTER.restartDataNode(dnProp);
    }
  }

  @Test
  public void testWriteLargeChunk() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true,
      false, (short) 3, 1024 * 1024 * 1024, eventLoop, CHANNEL_CLASS, MONITOR);
    byte[] b = new byte[50 * 1024 * 1024];
    ThreadLocalRandom.current().nextBytes(b);
    out.write(b);
    out.flush(false);
    assertEquals(b.length, out.flush(false).get().longValue());
    out.close();
    assertEquals(b.length, FS.getFileStatus(f).getLen());
    byte[] actual = new byte[b.length];
    try (FSDataInputStream in = FS.open(f)) {
      in.readFully(actual);
    }
    assertArrayEquals(b, actual);
  }

  /**
   * This test is for HBASE-26679.
   */
  @Test
  public void testFlushStuckWhenOneDataNodeShutdown() throws Exception {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();

    Configuration conf = FS.getConf();
    conf.set(FanOutOneBlockAsyncDFSOutputHelper.ASYNC_DFS_OUTPUT_CLASS_NAME,
      MyFanOutOneBlockAsyncDFSOutput.class.getName());

    DataNodeProperties firstDataNodeProperties = null;
    try {
      MyFanOutOneBlockAsyncDFSOutput out =
          (MyFanOutOneBlockAsyncDFSOutput) FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f,
            true, false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS, MONITOR);

      byte[] b = new byte[10];
      ThreadLocalRandom.current().nextBytes(b);
      out.write(b, 0, b.length);
      CompletableFuture<Long> future = out.flush(false);
      /**
       * First ack is received from dataNode1,we could stop dataNode1 now.
       */
      out.stopCyclicBarrier.await();
      Channel firstDataNodeChannel = out.alreadyNotifiedChannelRef.get();
      assertTrue(firstDataNodeChannel != null);
      firstDataNodeProperties = findAndKillFirstDataNode(out.datanodeInfoMap, firstDataNodeChannel);
      assertTrue(firstDataNodeProperties != null);
      try {
        future.get();
        fail();
      } catch (Throwable e) {
        LOG.info("expected exception caught when get future", e);
      }
      /**
       * Make sure all the data node channel are closed.
       */
      out.datanodeInfoMap.keySet().forEach(ch -> ch.closeFuture().awaitUninterruptibly());
    } finally {
      conf.unset(FanOutOneBlockAsyncDFSOutputHelper.ASYNC_DFS_OUTPUT_CLASS_NAME);
      if (firstDataNodeProperties != null) {
        CLUSTER.restartDataNode(firstDataNodeProperties);
      }
    }

  }

  private static DataNodeProperties findAndKillFirstDataNode(
      Map<Channel, DatanodeInfo> datanodeInfoMap,
      Channel firstChannel) {
    DatanodeInfo firstDatanodeInfo = datanodeInfoMap.get(firstChannel);
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

  static class MyFanOutOneBlockAsyncDFSOutput extends FanOutOneBlockAsyncDFSOutput {

    private final AtomicReference<Channel> alreadyNotifiedChannelRef =
        new AtomicReference<Channel>(null);
    private final CyclicBarrier stopCyclicBarrier = new CyclicBarrier(2);
    private final Map<Channel, DatanodeInfo> datanodeInfoMap;

    @Override
    protected void completed(Channel channel) {
      /**
       * Here it is hard to simulate slow response from datanode because I can not modify the code
       * of HDFS, so here because this method is only invoked by
       * {@link FanOutOneBlockAsyncDFSOutput.AckHandler#channelRead0}, we simulate slow response
       * from datanode by just permit the acks from the first responding data node to forward,and
       * discard the acks from other slow data nodes.
       */
      boolean success = this.alreadyNotifiedChannelRef.compareAndSet(null, channel);
      if (channel.equals(this.alreadyNotifiedChannelRef.get())) {
        super.completed(channel);
      }

      if (success) {
        /**
         * Here we tell the test method we could stop the data node now which send the first ack.
         */
        try {
          stopCyclicBarrier.await();
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
    }

    MyFanOutOneBlockAsyncDFSOutput(Configuration conf, DistributedFileSystem dfs,
        DFSClient client, ClientProtocol namenode, String clientName, String src, long fileId,
        LocatedBlock locatedBlock, Encryptor encryptor, Map<Channel, DatanodeInfo> datanodeInfoMap,
        DataChecksum summer, ByteBufAllocator alloc, StreamSlowMonitor streamSlowMonitor) {
      super(conf, dfs, client, namenode, clientName, src, fileId, locatedBlock, encryptor,
          datanodeInfoMap, summer, alloc, streamSlowMonitor);
      this.datanodeInfoMap = datanodeInfoMap;

    }

  }

}
