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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Daemon;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MiscTests.class, MediumTests.class })
public class TestFanOutOneBlockAsyncDFSOutput {

  private static final Log LOG = LogFactory.getLog(TestFanOutOneBlockAsyncDFSOutput.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static DistributedFileSystem FS;

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static int READ_TIMEOUT_MS = 2000;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, READ_TIMEOUT_MS);
    TEST_UTIL.startMiniDFSCluster(3);
    FS = TEST_UTIL.getDFSCluster().getFileSystem();
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    if (EVENT_LOOP_GROUP != null) {
      EVENT_LOOP_GROUP.shutdownGracefully().sync();
    }
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  private void ensureAllDatanodeAlive() throws InterruptedException {
    // FanOutOneBlockAsyncDFSOutputHelper.createOutput is fail-fast, so we need to make sure that we
    // can create a FanOutOneBlockAsyncDFSOutput after a datanode restarting, otherwise some tests
    // will fail.
    for (;;) {
      try {
        FanOutOneBlockAsyncDFSOutput out =
            FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, new Path("/ensureDatanodeAlive"),
              true, true, (short) 3, FS.getDefaultBlockSize(), EVENT_LOOP_GROUP.next());
        out.close();
        break;
      } catch (IOException e) {
        Thread.sleep(100);
      }
    }
  }

  static void writeAndVerify(EventLoop eventLoop, DistributedFileSystem dfs, Path f,
      final FanOutOneBlockAsyncDFSOutput out)
      throws IOException, InterruptedException, ExecutionException {
    final byte[] b = new byte[10];
    ThreadLocalRandom.current().nextBytes(b);
    final FanOutOneBlockAsyncDFSOutputFlushHandler handler =
        new FanOutOneBlockAsyncDFSOutputFlushHandler();
    eventLoop.execute(new Runnable() {

      @Override
      public void run() {
        out.write(b, 0, b.length);
        out.flush(null, handler, false);
      }
    });
    assertEquals(b.length, handler.get());
    out.close();
    assertEquals(b.length, dfs.getFileStatus(f).getLen());
    byte[] actual = new byte[b.length];
    try (FSDataInputStream in = dfs.open(f)) {
      in.readFully(actual);
    }
    assertArrayEquals(b, actual);
  }

  @Test
  public void test() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    final FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f,
      true, false, (short) 3, FS.getDefaultBlockSize(), eventLoop);
    writeAndVerify(eventLoop, FS, f, out);
  }

  @Test
  public void testRecover() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    final FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f,
      true, false, (short) 3, FS.getDefaultBlockSize(), eventLoop);
    final byte[] b = new byte[10];
    ThreadLocalRandom.current().nextBytes(b);
    final FanOutOneBlockAsyncDFSOutputFlushHandler handler =
        new FanOutOneBlockAsyncDFSOutputFlushHandler();
    eventLoop.execute(new Runnable() {

      @Override
      public void run() {
        out.write(b, 0, b.length);
        out.flush(null, handler, false);
      }
    });
    handler.get();
    // restart one datanode which causes one connection broken
    TEST_UTIL.getDFSCluster().restartDataNode(0);
    try {
      handler.reset();
      eventLoop.execute(new Runnable() {

        @Override
        public void run() {
          out.write(b, 0, b.length);
          out.flush(null, handler, false);
        }
      });
      try {
        handler.get();
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
    } finally {
      ensureAllDatanodeAlive();
    }
  }

  @Test
  public void testHeartbeat() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    final FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f,
      true, false, (short) 3, FS.getDefaultBlockSize(), eventLoop);
    Thread.sleep(READ_TIMEOUT_MS * 2);
    // the connection to datanode should still alive.
    writeAndVerify(eventLoop, FS, f, out);
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
        FS.getDefaultBlockSize(), eventLoop);
      fail("should fail with parent does not exist");
    } catch (RemoteException e) {
      LOG.info("expected exception caught", e);
      assertTrue(e.unwrapRemoteException() instanceof FileNotFoundException);
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
    TEST_UTIL.getDFSCluster().getDataNodes().get(0).shutdownDatanode(true);
    try {
      Path f = new Path("/test");
      EventLoop eventLoop = EVENT_LOOP_GROUP.next();
      try {
        FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true, false, (short) 3,
          FS.getDefaultBlockSize(), eventLoop);
        fail("should fail with connection error");
      } catch (IOException e) {
        LOG.info("expected exception caught", e);
      }
      for (DataNode dn : TEST_UTIL.getDFSCluster().getDataNodes()) {
        Daemon daemon = (Daemon) xceiverServerDaemonField.get(dn);
        assertEquals(0, numPeersMethod.invoke(daemon.getRunnable()));
      }
    } finally {
      TEST_UTIL.getDFSCluster().restartDataNode(0);
      ensureAllDatanodeAlive();
    }
  }

  @Test
  public void testWriteLargeChunk() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    final FanOutOneBlockAsyncDFSOutput out = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f,
      true, false, (short) 3, 1024 * 1024 * 1024, eventLoop);
    byte[] b = new byte[50 * 1024 * 1024];
    ThreadLocalRandom.current().nextBytes(b);
    FanOutOneBlockAsyncDFSOutputFlushHandler handler =
        new FanOutOneBlockAsyncDFSOutputFlushHandler();
    eventLoop.execute(new Runnable() {

      @Override
      public void run() {
        out.write(b);
        out.flush(null, handler, false);
      }
    });
    assertEquals(b.length, handler.get());
    out.close();
    assertEquals(b.length, FS.getFileStatus(f).getLen());
    byte[] actual = new byte[b.length];
    try (FSDataInputStream in = FS.open(f)) {
      in.readFully(actual);
    }
    assertArrayEquals(b, actual);
  }
}
