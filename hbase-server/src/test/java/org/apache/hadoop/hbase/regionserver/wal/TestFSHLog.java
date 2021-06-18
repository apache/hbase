/**
 *
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Provides FSHLog test cases.
 */
@Category(MediumTests.class)
public class TestFSHLog {
  private static final Log LOG = LogFactory.getLog(TestFSHLog.class);

  private static final long TEST_TIMEOUT_MS = 10000;

  protected static Configuration conf;
  protected static FileSystem fs;
  protected static Path dir;
  protected static Path rootDir;
  protected static Path walRootDir;
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
    rootDir = TEST_UTIL.createRootDir();
    walRootDir = TEST_UTIL.createWALRootDir();
    dir = new Path(walRootDir, currentTest.getMethodName());
    assertNotEquals(rootDir, walRootDir);
  }

  @After
  public void tearDown() throws Exception {
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Make block sizes small.
    TEST_UTIL.getConfiguration().setInt("dfs.blocksize", 1024 * 1024);
    // quicker heartbeat interval for faster DN death notification
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.socket-timeout", 5000);

    // faster failover with cluster.shutdown();fs.close() idiom
    TEST_UTIL.getConfiguration()
        .setInt("hbase.ipc.client.connect.max.retries", 1);
    TEST_UTIL.getConfiguration().setInt(
        "dfs.client.block.recovery.retries", 1);
    TEST_UTIL.getConfiguration().setInt(
      "hbase.ipc.client.connection.maxidletime", 500);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        SampleRegionWALObserver.class.getName());
    TEST_UTIL.startMiniDFSCluster(3);

    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fs.delete(rootDir, true);
    fs.delete(walRootDir, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * A loaded WAL coprocessor won't break existing WAL test cases.
   */
  @Test
  public void testWALCoprocessorLoaded() throws Exception {
    // test to see whether the coprocessor is loaded or not.
    FSHLog log = null;
    try {
      log = new FSHLog(fs, walRootDir, dir.toString(),
          HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null, null);
      WALCoprocessorHost host = log.getCoprocessorHost();
      Coprocessor c = host.findCoprocessor(SampleRegionWALObserver.class.getName());
      assertNotNull(c);
    } finally {
      if (log != null) {
        log.close();
      }
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

      @Override public long getLength() throws IOException {
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
    }

    try (FSHLog log = new CustomFSHLog(fs, walRootDir, dir.toString(),
      HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null, null)) {
      log.setWriter(new FailingWriter());
      Field ringBufferEventHandlerField =
        FSHLog.class.getDeclaredField("ringBufferEventHandler");
      ringBufferEventHandlerField.setAccessible(true);
      FSHLog.RingBufferEventHandler ringBufferEventHandler =
        (FSHLog.RingBufferEventHandler) ringBufferEventHandlerField.get(log);
      // Force a safe point
      final FSHLog.SafePointZigZagLatch latch = ringBufferEventHandler.attainSafePoint();
      try {
        final SyncFuture future0 = log.publishSyncOnRingBuffer(null, false);
        // Wait for the sync to be done.
        Waiter.waitFor(conf, TEST_TIMEOUT_MS, new Waiter.Predicate<Exception>() {
          @Override
          public boolean evaluate() throws Exception {
            return future0.isDone();
          }
        });
        // Publish another sync from the same thread, this should not overwrite the done sync.
        SyncFuture future1 = log.publishSyncOnRingBuffer(null, false);
        assertFalse(future1.isDone());
        // Unblock the safe point trigger..
        blockBeforeSafePoint.countDown();
        // Wait for the safe point to be reached. With the deadlock in HBASE-25984, this is never
        // possible, thus blocking the sync pipeline.
        Waiter.waitFor(conf, TEST_TIMEOUT_MS, new Waiter.Predicate<Exception>() {
          @Override
          public boolean evaluate() throws Exception {
            return latch.isSafePointAttained();
          }
        });
      } finally {
        // Force release the safe point, for the clean up.
        latch.releaseSafePoint();
      }
    }
  }

  protected void addEdits(WAL log,
                          HRegionInfo hri,
                          HTableDescriptor htd,
                          int times,
                          MultiVersionConcurrencyControl mvcc)
      throws IOException {
    final byte[] row = Bytes.toBytes("row");
    for (int i = 0; i < times; i++) {
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, row, row, timestamp, row));
      WALKey key = new WALKey(hri.getEncodedNameAsBytes(), htd.getTableName(),
          WALKey.NO_SEQUENCE_ID, timestamp, WALKey.EMPTY_UUIDS, HConstants.NO_NONCE,
          HConstants.NO_NONCE, mvcc);
      log.append(htd, hri, key, cols, true);
    }
    log.sync();
  }

  /**
   * helper method to simulate region flush for a WAL.
   * @param wal
   * @param regionEncodedName
   */
  protected void flushRegion(WAL wal, byte[] regionEncodedName, Set<byte[]> flushedFamilyNames) {
    wal.startCacheFlush(regionEncodedName, flushedFamilyNames);
    wal.completeCacheFlush(regionEncodedName);
  }

  /**
   * tests the log comparator. Ensure that we are not mixing meta logs with non-meta logs (throws
   * exception if we do). Comparison is based on the timestamp present in the wal name.
   * @throws Exception
   */
  @Test 
  public void testWALComparator() throws Exception {
    FSHLog wal1 = null;
    FSHLog walMeta = null;
    try {
      wal1 = new FSHLog(fs, walRootDir, dir.toString(),
          HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null, null);
      LOG.debug("Log obtained is: " + wal1);
      Comparator<Path> comp = wal1.LOG_NAME_COMPARATOR;
      Path p1 = wal1.computeFilename(11);
      Path p2 = wal1.computeFilename(12);
      // comparing with itself returns 0
      assertTrue(comp.compare(p1, p1) == 0);
      // comparing with different filenum.
      assertTrue(comp.compare(p1, p2) < 0);
      walMeta = new FSHLog(fs, walRootDir, dir.toString(),
          HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null,
          DefaultWALProvider.META_WAL_PROVIDER_ID);
      Comparator<Path> compMeta = walMeta.LOG_NAME_COMPARATOR;

      Path p1WithMeta = walMeta.computeFilename(11);
      Path p2WithMeta = walMeta.computeFilename(12);
      assertTrue(compMeta.compare(p1WithMeta, p1WithMeta) == 0);
      assertTrue(compMeta.compare(p1WithMeta, p2WithMeta) < 0);
      // mixing meta and non-meta logs gives error
      boolean ex = false;
      try {
        comp.compare(p1WithMeta, p2);
      } catch (IllegalArgumentException e) {
        ex = true;
      }
      assertTrue("Comparator doesn't complain while checking meta log files", ex);
      boolean exMeta = false;
      try {
        compMeta.compare(p1WithMeta, p2);
      } catch (IllegalArgumentException e) {
        exMeta = true;
      }
      assertTrue("Meta comparator doesn't complain while checking log files", exMeta);
    } finally {
      if (wal1 != null) {
        wal1.close();
      }
      if (walMeta != null) {
        walMeta.close();
      }
    }
  }

  /**
   * On rolling a wal after reaching the threshold, {@link WAL#rollWriter()} returns the
   * list of regions which should be flushed in order to archive the oldest wal file.
   * <p>
   * This method tests this behavior by inserting edits and rolling the wal enough times to reach
   * the max number of logs threshold. It checks whether we get the "right regions" for flush on
   * rolling the wal.
   * @throws Exception
   */
  @Test 
  public void testFindMemStoresEligibleForFlush() throws Exception {
    LOG.debug("testFindMemStoresEligibleForFlush");
    Configuration conf1 = HBaseConfiguration.create(conf);
    conf1.setInt("hbase.regionserver.maxlogs", 1);
    FSHLog wal = new FSHLog(fs, walRootDir, dir.toString(),
        HConstants.HREGION_OLDLOGDIR_NAME, conf1, null, true, null, null);
    HTableDescriptor t1 =
        new HTableDescriptor(TableName.valueOf("t1")).addFamily(new HColumnDescriptor("row"));
    HTableDescriptor t2 =
        new HTableDescriptor(TableName.valueOf("t2")).addFamily(new HColumnDescriptor("row"));
    HRegionInfo hri1 =
        new HRegionInfo(t1.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    HRegionInfo hri2 =
        new HRegionInfo(t2.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    // add edits and roll the wal
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    try {
      addEdits(wal, hri1, t1, 2, mvcc);
      wal.rollWriter();
      // add some more edits and roll the wal. This would reach the log number threshold
      addEdits(wal, hri1, t1, 2, mvcc);
      wal.rollWriter();
      // with above rollWriter call, the max logs limit is reached.
      assertTrue(wal.getNumRolledLogFiles() == 2);

      // get the regions to flush; since there is only one region in the oldest wal, it should
      // return only one region.
      byte[][] regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(1, regionsToFlush.length);
      assertTrue(Bytes.equals(hri1.getEncodedNameAsBytes(), regionsToFlush[0]));
      // insert edits in second region
      addEdits(wal, hri2, t2, 2, mvcc);
      // get the regions to flush, it should still read region1.
      regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(regionsToFlush.length, 1);
      assertTrue(Bytes.equals(hri1.getEncodedNameAsBytes(), regionsToFlush[0]));
      // flush region 1, and roll the wal file. Only last wal which has entries for region1 should
      // remain.
      flushRegion(wal, hri1.getEncodedNameAsBytes(), t1.getFamiliesKeys());
      wal.rollWriter();
      // only one wal should remain now (that is for the second region).
      assertEquals(1, wal.getNumRolledLogFiles());
      // flush the second region
      flushRegion(wal, hri2.getEncodedNameAsBytes(), t2.getFamiliesKeys());
      wal.rollWriter(true);
      // no wal should remain now.
      assertEquals(0, wal.getNumRolledLogFiles());
      // add edits both to region 1 and region 2, and roll.
      addEdits(wal, hri1, t1, 2, mvcc);
      addEdits(wal, hri2, t2, 2, mvcc);
      wal.rollWriter();
      // add edits and roll the writer, to reach the max logs limit.
      assertEquals(1, wal.getNumRolledLogFiles());
      addEdits(wal, hri1, t1, 2, mvcc);
      wal.rollWriter();
      // it should return two regions to flush, as the oldest wal file has entries
      // for both regions.
      regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(2, regionsToFlush.length);
      // flush both regions
      flushRegion(wal, hri1.getEncodedNameAsBytes(), t1.getFamiliesKeys());
      flushRegion(wal, hri2.getEncodedNameAsBytes(), t2.getFamiliesKeys());
      wal.rollWriter(true);
      assertEquals(0, wal.getNumRolledLogFiles());
      // Add an edit to region1, and roll the wal.
      addEdits(wal, hri1, t1, 2, mvcc);
      // tests partial flush: roll on a partial flush, and ensure that wal is not archived.
      wal.startCacheFlush(hri1.getEncodedNameAsBytes(), t1.getFamiliesKeys());
      wal.rollWriter();
      wal.completeCacheFlush(hri1.getEncodedNameAsBytes());
      assertEquals(1, wal.getNumRolledLogFiles());
    } finally {
      if (wal != null) {
        wal.close();
      }
    }
  }

  @Test(expected=IOException.class)
  public void testFailedToCreateWALIfParentRenamed() throws IOException {
    final String name = "testFailedToCreateWALIfParentRenamed";
    FSHLog log = new FSHLog(fs, walRootDir, name, HConstants.HREGION_OLDLOGDIR_NAME,
        conf, null, true, null, null);
    long filenum = System.currentTimeMillis();
    Path path = log.computeFilename(filenum);
    log.createWriterInstance(path);
    Path parent = path.getParent();
    path = log.computeFilename(filenum + 1);
    Path newPath = new Path(parent.getParent(), parent.getName() + "-splitting");
    fs.rename(parent, newPath);
    log.createWriterInstance(path);
    fail("It should fail to create the new WAL");
  }

  /**
   * Test flush for sure has a sequence id that is beyond the last edit appended.  We do this
   * by slowing appends in the background ring buffer thread while in foreground we call
   * flush.  The addition of the sync over HRegion in flush should fix an issue where flush was
   * returning before all of its appends had made it out to the WAL (HBASE-11109).
   * see HBASE-11109
   * @throws IOException
   */
  @Test
  public void testFlushSequenceIdIsGreaterThanAllEditsInHFile() throws IOException {
    String testName = "testFlushSequenceIdIsGreaterThanAllEditsInHFile";
    final TableName tableName = TableName.valueOf(testName);
    final HRegionInfo hri = new HRegionInfo(tableName);
    final byte[] rowName = tableName.getName();
    final HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("f"));
    HRegion r = HRegion.createHRegion(hri, rootDir,
      TEST_UTIL.getConfiguration(), htd);
    HRegion.closeHRegion(r);
    final int countPerFamily = 10;
    final MutableBoolean goslow = new MutableBoolean(false);
    // subclass and doctor a method.
    FSHLog wal = new FSHLog(FileSystem.get(conf), walRootDir,
        testName, conf) {
      @Override
      void atHeadOfRingBufferEventHandlerAppend() {
        if (goslow.isTrue()) {
          Threads.sleep(100);
          LOG.debug("Sleeping before appending 100ms");
        }
        super.atHeadOfRingBufferEventHandlerAppend();
      }
    };
    HRegion region = HRegion.openHRegion(TEST_UTIL.getConfiguration(),
      TEST_UTIL.getTestFileSystem(), rootDir, hri, htd, wal);
    EnvironmentEdge ee = EnvironmentEdgeManager.getDelegate();
    try {
      List<Put> puts = null;
      for (HColumnDescriptor hcd: htd.getFamilies()) {
        puts =
          TestWALReplay.addRegionEdits(rowName, hcd.getName(), countPerFamily, ee, region, "x");
      }

      // Now assert edits made it in.
      final Get g = new Get(rowName);
      Result result = region.get(g);
      assertEquals(countPerFamily * htd.getFamilies().size(), result.size());

      // Construct a WALEdit and add it a few times to the WAL.
      WALEdit edits = new WALEdit();
      for (Put p: puts) {
        CellScanner cs = p.cellScanner();
        while (cs.advance()) {
          edits.add(cs.current());
        }
      }
      // Add any old cluster id.
      List<UUID> clusterIds = new ArrayList<UUID>();
      clusterIds.add(UUID.randomUUID());
      // Now make appends run slow.
      goslow.setValue(true);
      for (int i = 0; i < countPerFamily; i++) {
        final HRegionInfo info = region.getRegionInfo();
        final WALKey logkey = new WALKey(info.getEncodedNameAsBytes(), tableName,
            System.currentTimeMillis(), clusterIds, -1, -1, region.getMVCC());
        wal.append(htd, info, logkey, edits, true);
        region.getMVCC().completeAndWait(logkey.getWriteEntry());
      }
      region.flush(true);
      // FlushResult.flushSequenceId is not visible here so go get the current sequence id.
      long currentSequenceId = region.getSequenceId();
      // Now release the appends
      goslow.setValue(false);
      synchronized (goslow) {
        goslow.notifyAll();
      }
      assertTrue(currentSequenceId >= region.getSequenceId());
    } finally {
      region.close(true);
      wal.close();
    }
  }

  @Test
  public void testSyncRunnerIndexOverflow() throws IOException, NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    final String name = "testSyncRunnerIndexOverflow";
    FSHLog log =
        new FSHLog(fs, walRootDir, name, HConstants.HREGION_OLDLOGDIR_NAME, conf,
            null, true, null, null);
    try {
      Field ringBufferEventHandlerField = FSHLog.class.getDeclaredField("ringBufferEventHandler");
      ringBufferEventHandlerField.setAccessible(true);
      FSHLog.RingBufferEventHandler ringBufferEventHandler =
          (FSHLog.RingBufferEventHandler) ringBufferEventHandlerField.get(log);
      Field syncRunnerIndexField =
          FSHLog.RingBufferEventHandler.class.getDeclaredField("syncRunnerIndex");
      syncRunnerIndexField.setAccessible(true);
      syncRunnerIndexField.set(ringBufferEventHandler, Integer.MAX_VALUE - 1);
      HTableDescriptor htd =
          new HTableDescriptor(TableName.valueOf("t1")).addFamily(new HColumnDescriptor("row"));
      HRegionInfo hri =
          new HRegionInfo(htd.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
      for (int i = 0; i < 10; i++) {
        addEdits(log, hri, htd, 1, mvcc);
      }
    } finally {
      log.close();
    }
  }

  /**
   * Test case for https://issues.apache.org/jira/browse/HBASE-16721
   */
  @Test (timeout = 30000)
  public void testUnflushedSeqIdTracking() throws IOException, InterruptedException {
    final String name = "testSyncRunnerIndexOverflow";
    final byte[] b = Bytes.toBytes("b");

    final AtomicBoolean startHoldingForAppend = new AtomicBoolean(false);
    final CountDownLatch holdAppend = new CountDownLatch(1);
    final CountDownLatch flushFinished = new CountDownLatch(1);
    final CountDownLatch putFinished = new CountDownLatch(1);

    try (FSHLog log =
        new FSHLog(fs, walRootDir, name, HConstants.HREGION_OLDLOGDIR_NAME, conf,
            null, true, null, null)) {

      log.registerWALActionsListener(new WALActionsListener.Base() {
        @Override
        public void visitLogEntryBeforeWrite(HTableDescriptor htd, WALKey logKey, WALEdit logEdit)
            throws IOException {
          if (startHoldingForAppend.get()) {
            try {
              holdAppend.await();
            } catch (InterruptedException e) {
              LOG.error(e);
            }
          }
        }
      });

      // open a new region which uses this WAL
      HTableDescriptor htd =
          new HTableDescriptor(TableName.valueOf("t1")).addFamily(new HColumnDescriptor(b));
      HRegionInfo hri =
          new HRegionInfo(htd.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);

      final HRegion region = TEST_UTIL.createLocalHRegion(hri, conf, htd, log);
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
            LOG.error(e);
          }
        }
      });

      // give the put a chance to start
      Threads.sleep(3000);

      exec.submit(new Runnable() {
        @Override
        public void run() {
          try {
            Region.FlushResult flushResult = region.flush(true);
            LOG.info("Flush result:" +  flushResult.getResult());
            LOG.info("Flush succeeded:" +  flushResult.isFlushSucceeded());
            flushFinished.countDown();
          } catch (IOException e) {
            LOG.error(e);
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
      long seqId = log.getEarliestMemstoreSeqNum(hri.getEncodedNameAsBytes());
      assertEquals("Found seqId for the region which is already flushed",
          HConstants.NO_SEQNUM, seqId);

      region.close();
    }
  }
}
