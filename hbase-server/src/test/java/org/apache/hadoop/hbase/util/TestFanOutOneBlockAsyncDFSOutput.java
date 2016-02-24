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
package org.apache.hadoop.hbase.util;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MiscTests.class, MediumTests.class })
public class TestFanOutOneBlockAsyncDFSOutput {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static DistributedFileSystem FS;

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static int READ_TIMEOUT_MS = 2000;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    Logger.getLogger("org.apache.hadoop.hdfs.StateChange").setLevel(Level.DEBUG);
    Logger.getLogger("BlockStateChange").setLevel(Level.DEBUG);
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

  private void writeAndVerify(EventLoop eventLoop, Path f, final FanOutOneBlockAsyncDFSOutput out)
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
    assertEquals(b.length, FS.getFileStatus(f).getLen());
    byte[] actual = new byte[b.length];
    try (FSDataInputStream in = FS.open(f)) {
      in.readFully(actual);
    }
    assertArrayEquals(b, actual);
  }

  @Test
  public void test() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    final FanOutOneBlockAsyncDFSOutput out =
        FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true, false, (short) 3,
          FS.getDefaultBlockSize(), eventLoop);
    writeAndVerify(eventLoop, f, out);
  }

  @Test
  public void testRecover() throws IOException, InterruptedException, ExecutionException {
    Path f = new Path("/" + name.getMethodName());
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    final FanOutOneBlockAsyncDFSOutput out =
        FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true, false, (short) 3,
          FS.getDefaultBlockSize(), eventLoop);
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
      e.printStackTrace();
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
    final FanOutOneBlockAsyncDFSOutput out =
        FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true, false, (short) 3,
          FS.getDefaultBlockSize(), eventLoop);
    Thread.sleep(READ_TIMEOUT_MS * 2);
    // the connection to datanode should still alive.
    writeAndVerify(eventLoop, f, out);
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
      assertTrue(e.unwrapRemoteException() instanceof FileNotFoundException);
    }
  }
}
