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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider.AsyncWriter;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Testcase for HBASE-25905
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestAsyncFSWALRollStuck {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncFSWALRollStuck.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncFSWALRollStuck.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static EventLoopGroup EVENT_LOOP_GROUP = new NioEventLoopGroup();

  private static Class<? extends Channel> CHANNEL_CLASS = NioSocketChannel.class;

  private static ScheduledExecutorService EXECUTOR;

  private static BlockingQueue<CompletableFuture<Long>> FUTURES = new ArrayBlockingQueue<>(3);

  private static AtomicInteger SYNC_COUNT = new AtomicInteger(0);

  private static CountDownLatch ARRIVE = new CountDownLatch(1);

  private static CountDownLatch RESUME = new CountDownLatch(1);

  public static final class TestAsyncWriter extends AsyncProtobufLogWriter {

    public TestAsyncWriter(EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass) {
      super(eventLoopGroup, channelClass);
    }

    @Override
    public CompletableFuture<Long> sync(boolean forceSync) {
      int count = SYNC_COUNT.incrementAndGet();
      if (count < 3) {
        // we will mark these two futures as failure, to make sure that we have 2 edits in
        // unackedAppends, and trigger a WAL roll
        CompletableFuture<Long> f = new CompletableFuture<>();
        FUTURES.offer(f);
        return f;
      } else if (count == 3) {
        // for this future, we will mark it as succeeded, but before returning from this method, we
        // need to request a roll, to enter the special corner case, where we have left some edits
        // in unackedAppends but never tries to write them out which leads to a hang
        ARRIVE.countDown();
        try {
          RESUME.await();
        } catch (InterruptedException e) {
        }
        return super.sync(forceSync);
      } else {
        return super.sync(forceSync);
      }
    }
  }

  private static TableName TN;

  private static RegionInfo RI;

  private static MultiVersionConcurrencyControl MVCC;

  private static AsyncFSWAL WAL;

  private static ExecutorService ROLL_EXEC;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setClass(AsyncFSWALProvider.WRITER_IMPL, TestAsyncWriter.class, AsyncWriter.class);
    // set a very small size so we will reach the batch size when writing out a single edit
    conf.setLong(AsyncFSWAL.WAL_BATCH_SIZE, 1);

    TN = TableName.valueOf("test");
    RI = RegionInfoBuilder.newBuilder(TN).build();
    MVCC = new MultiVersionConcurrencyControl();

    EXECUTOR =
      Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder().setDaemon(true).build());

    Path rootDir = UTIL.getDataTestDir();
    ROLL_EXEC =
      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).build());
    WALActionsListener listener = new WALActionsListener() {

      @Override
      public void logRollRequested(RollRequestReason reason) {
        ROLL_EXEC.execute(() -> {
          try {
            WAL.rollWriter();
          } catch (Exception e) {
            LOG.warn("failed to roll writer", e);
          }
        });
      }

    };
    WAL = new AsyncFSWAL(UTIL.getTestFileSystem(), rootDir, "log", "oldlog", conf,
      Arrays.asList(listener), true, null, null, EVENT_LOOP_GROUP, CHANNEL_CLASS);
    WAL.init();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EXECUTOR.shutdownNow();
    ROLL_EXEC.shutdownNow();
    Closeables.close(WAL, true);
    UTIL.cleanupTestDir();
  }

  @Test
  public void testRoll() throws Exception {
    byte[] row = Bytes.toBytes("family");
    WALEdit edit = new WALEdit();
    edit.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setFamily(row)
      .setQualifier(row).setRow(row).setValue(row)
      .setTimestamp(EnvironmentEdgeManager.currentTime()).setType(Type.Put).build());
    WALKeyImpl key1 =
      new WALKeyImpl(RI.getEncodedNameAsBytes(), TN, EnvironmentEdgeManager.currentTime(), MVCC);
    WAL.appendData(RI, key1, edit);

    WALKeyImpl key2 = new WALKeyImpl(RI.getEncodedNameAsBytes(), TN, key1.getWriteTime() + 1, MVCC);
    long txid = WAL.appendData(RI, key2, edit);

    // we need to make sure the two edits have both been added unackedAppends, so we have two syncs
    UTIL.waitFor(10000, () -> FUTURES.size() == 2);
    FUTURES.poll().completeExceptionally(new IOException("inject error"));
    FUTURES.poll().completeExceptionally(new IOException("inject error"));
    ARRIVE.await();
    // resume after 1 seconds, to give us enough time to enter the roll state
    EXECUTOR.schedule(() -> RESUME.countDown(), 1, TimeUnit.SECONDS);
    // let's roll the wal, before the fix in HBASE-25905, it will hang forever inside
    // waitForSafePoint
    WAL.rollWriter();
    // make sure we can finally succeed
    WAL.sync(txid);
  }
}
