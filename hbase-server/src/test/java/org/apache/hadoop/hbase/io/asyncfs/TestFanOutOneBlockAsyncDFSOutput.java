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
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@Category({ MiscTests.class, MediumTests.class })
public class TestFanOutOneBlockAsyncDFSOutput {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFanOutOneBlockAsyncDFSOutput.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFanOutOneBlockAsyncDFSOutput.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static DistributedFileSystem FS;

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  private static int READ_TIMEOUT_MS = 2000;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, READ_TIMEOUT_MS);
    TEST_UTIL.startMiniDFSCluster(3);
    FS = TEST_UTIL.getDFSCluster().getFileSystem();
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    CHANNEL_CLASS = NioSocketChannel.class;
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    if (EVENT_LOOP_GROUP != null) {
      EVENT_LOOP_GROUP.shutdownGracefully().sync();
    }
    TEST_UTIL.shutdownMiniDFSCluster();
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
      false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS);
    writeAndVerify(FS, f, out);
  }

  @Test
  public void testRecover() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true,
      false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS);
    byte[] b = new byte[10];
    ThreadLocalRandom.current().nextBytes(b);
    out.write(b, 0, b.length);
    out.flush(false).get();
    // restart one datanode which causes one connection broken
    TEST_UTIL.getDFSCluster().restartDataNode(0);
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
      false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS);
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
        FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS);
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
    DataNodeProperties dnProp = TEST_UTIL.getDFSCluster().stopDataNode(0);
    Path f = new Path("/test");
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    try (FanOutOneBlockAsyncDFSOutput output = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS,
      f, true, false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS)) {
      // should exclude the dead dn when retry so here we only have 2 DNs in pipeline
      assertEquals(2, output.getPipeline().length);
    } finally {
      TEST_UTIL.getDFSCluster().restartDataNode(dnProp);
    }
  }

  @Test
  public void testWriteLargeChunk() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true,
      false, (short) 3, 1024 * 1024 * 1024, eventLoop, CHANNEL_CLASS);
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
}
