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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Provides FSHLog test cases.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestFSHLog extends AbstractTestFSWAL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFSHLog.class);

  private static final long TEST_TIMEOUT_MS = 10000;

  @Rule
  public TestName name = new TestName();

  @Override
  protected AbstractFSWAL<?> newWAL(FileSystem fs, Path rootDir, String walDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix) throws IOException {
    FSHLog fshLog = new FSHLog(fs, rootDir, walDir, archiveDir,
      conf, listeners, failIfWALExists, prefix, suffix);
    fshLog.init();
    return fshLog;
  }

  @Override
  protected AbstractFSWAL<?> newSlowWAL(FileSystem fs, Path rootDir, String walDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, final Runnable action)
      throws IOException {
    FSHLog fshLog = new FSHLog(fs, rootDir, walDir, archiveDir,
      conf, listeners, failIfWALExists, prefix, suffix) {

      @Override
      protected void atHeadOfRingBufferEventHandlerAppend() {
        action.run();
        super.atHeadOfRingBufferEventHandlerAppend();
      }
    };
    fshLog.init();
    return fshLog;
  }

  @Test
  public void testSyncRunnerIndexOverflow() throws IOException, NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    final String name = this.name.getMethodName();
    FSHLog log = new FSHLog(FS, CommonFSUtils.getRootDir(CONF), name,
      HConstants.HREGION_OLDLOGDIR_NAME, CONF, null, true, null, null);
    log.init();
    try {
      Field ringBufferEventHandlerField = FSHLog.class.getDeclaredField("ringBufferEventHandler");
      ringBufferEventHandlerField.setAccessible(true);
      FSHLog.RingBufferEventHandler ringBufferEventHandler =
          (FSHLog.RingBufferEventHandler) ringBufferEventHandlerField.get(log);
      Field syncRunnerIndexField =
          FSHLog.RingBufferEventHandler.class.getDeclaredField("syncRunnerIndex");
      syncRunnerIndexField.setAccessible(true);
      syncRunnerIndexField.set(ringBufferEventHandler, Integer.MAX_VALUE - 1);
      TableDescriptor htd =
          TableDescriptorBuilder.newBuilder(TableName.valueOf(this.name.getMethodName()))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of("row")).build();
      NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (byte[] fam : htd.getColumnFamilyNames()) {
        scopes.put(fam, 0);
      }
      RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
      MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
      for (int i = 0; i < 10; i++) {
        addEdits(log, hri, htd, 1, mvcc, scopes, "row");
      }
    } finally {
      log.close();
    }
  }

  /**
   * Test for WAL stall due to sync future overwrites. See HBASE-25984.
   */
  @Test
  public void testDeadlockWithSyncOverwrites() throws Exception {
    final CountDownLatch blockBeforeSafePoint = new CountDownLatch(1);

    class FailingWriter implements WALProvider.Writer {
      @Override public void sync(boolean forceSync) throws IOException {
        throw new IOException("Injected failure..");
      }

      @Override public void append(WAL.Entry entry) throws IOException {
      }

      @Override public long getLength() {
        return 0;
      }

      @Override
      public long getSyncedLength() {
        return 0;
      }

      @Override public void close() throws IOException {
      }
    }

    /*
     * Custom FSHLog implementation with a conditional wait before attaining safe point.
     */
    class CustomFSHLog extends FSHLog {
      public CustomFSHLog(FileSystem fs, Path rootDir, String logDir, String archiveDir,
                          Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
                          String prefix, String suffix) throws IOException {
        super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
      }

      @Override
      protected void beforeWaitOnSafePoint() {
        try {
          assertTrue(blockBeforeSafePoint.await(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      public SyncFuture publishSyncOnRingBuffer() {
        long sequence = getSequenceOnRingBuffer();
        return publishSyncOnRingBuffer(sequence, false);
      }
    }

    final String name = this.name.getMethodName();
    try (CustomFSHLog log = new CustomFSHLog(FS, CommonFSUtils.getRootDir(CONF), name,
        HConstants.HREGION_OLDLOGDIR_NAME, CONF, null, true, null, null)) {
      log.setWriter(new FailingWriter());
      Field ringBufferEventHandlerField =
          FSHLog.class.getDeclaredField("ringBufferEventHandler");
      ringBufferEventHandlerField.setAccessible(true);
      FSHLog.RingBufferEventHandler ringBufferEventHandler =
          (FSHLog.RingBufferEventHandler) ringBufferEventHandlerField.get(log);
      // Force a safe point
      FSHLog.SafePointZigZagLatch latch = ringBufferEventHandler.attainSafePoint();
      try {
        SyncFuture future0 = log.publishSyncOnRingBuffer();
        // Wait for the sync to be done.
        Waiter.waitFor(CONF, TEST_TIMEOUT_MS, future0::isDone);
        // Publish another sync from the same thread, this should not overwrite the done sync.
        SyncFuture future1 = log.publishSyncOnRingBuffer();
        assertFalse(future1.isDone());
        // Unblock the safe point trigger..
        blockBeforeSafePoint.countDown();
        // Wait for the safe point to be reached.
        // With the deadlock in HBASE-25984, this is never possible, thus blocking the sync pipeline.
        Waiter.waitFor(CONF, TEST_TIMEOUT_MS, latch::isSafePointAttained);
      } finally {
        // Force release the safe point, for the clean up.
        latch.releaseSafePoint();
      }
    }
  }

  /**
   * Test case for https://issues.apache.org/jira/browse/HBASE-16721
   */
  @Test
  public void testUnflushedSeqIdTracking() throws IOException, InterruptedException {
    final String name = this.name.getMethodName();
    final byte[] b = Bytes.toBytes("b");

    final AtomicBoolean startHoldingForAppend = new AtomicBoolean(false);
    final CountDownLatch holdAppend = new CountDownLatch(1);
    final CountDownLatch flushFinished = new CountDownLatch(1);
    final CountDownLatch putFinished = new CountDownLatch(1);

    try (FSHLog log = new FSHLog(FS, CommonFSUtils.getRootDir(CONF), name,
      HConstants.HREGION_OLDLOGDIR_NAME, CONF, null, true, null, null)) {
      log.init();
      log.registerWALActionsListener(new WALActionsListener() {
        @Override
        public void visitLogEntryBeforeWrite(WALKey logKey, WALEdit logEdit)
            throws IOException {
          if (startHoldingForAppend.get()) {
            try {
              holdAppend.await();
            } catch (InterruptedException e) {
              LOG.error(e.toString(), e);
            }
          }
        }
      });

      // open a new region which uses this WAL
      TableDescriptor htd =
          TableDescriptorBuilder.newBuilder(TableName.valueOf(this.name.getMethodName()))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(b)).build();
      RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
      ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
        0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
      final HRegion region = TEST_UTIL.createLocalHRegion(hri, CONF, htd, log);
      ExecutorService exec = Executors.newFixedThreadPool(2);

      // do a regular write first because of memstore size calculation.
      region.put(new Put(b).addColumn(b, b,b));

      startHoldingForAppend.set(true);
      exec.submit(new Runnable() {
        @Override
        public void run() {
          try {
            region.put(new Put(b).addColumn(b, b,b));
            putFinished.countDown();
          } catch (IOException e) {
            LOG.error(e.toString(), e);
          }
        }
      });

      // give the put a chance to start
      Threads.sleep(3000);

      exec.submit(new Runnable() {
        @Override
        public void run() {
          try {
            HRegion.FlushResult flushResult = region.flush(true);
            LOG.info("Flush result:" +  flushResult.getResult());
            LOG.info("Flush succeeded:" +  flushResult.isFlushSucceeded());
            flushFinished.countDown();
          } catch (IOException e) {
            LOG.error(e.toString(), e);
          }
        }
      });

      // give the flush a chance to start. Flush should have got the region lock, and
      // should have been waiting on the mvcc complete after this.
      Threads.sleep(3000);

      // let the append to WAL go through now that the flush already started
      holdAppend.countDown();
      putFinished.await();
      flushFinished.await();

      // check whether flush went through
      assertEquals("Region did not flush?", 1, region.getStoreFileList(new byte[][]{b}).size());

      // now check the region's unflushed seqIds.
      long seqId = log.getEarliestMemStoreSeqNum(hri.getEncodedNameAsBytes());
      assertEquals("Found seqId for the region which is already flushed",
          HConstants.NO_SEQNUM, seqId);

      region.close();
    }
  }
}
