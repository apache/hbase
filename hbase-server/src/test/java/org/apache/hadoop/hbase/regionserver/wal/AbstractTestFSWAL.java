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

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALCoprocessor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.SequenceId;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTestFSWAL {

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractTestFSWAL.class);

  protected static Configuration CONF;
  protected static FileSystem FS;
  protected static Path DIR;
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    FileStatus[] entries = FS.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      FS.delete(dir.getPath(), true);
    }
    final Path hbaseDir = TEST_UTIL.createRootDir();
    final Path hbaseWALDir = TEST_UTIL.createWALRootDir();
    DIR = new Path(hbaseWALDir, currentTest.getMethodName());
    assertNotEquals(hbaseDir, hbaseWALDir);
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
    TEST_UTIL.getConfiguration().setInt("hbase.ipc.client.connect.max.retries", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.recovery.retries", 1);
    TEST_UTIL.getConfiguration().setInt("hbase.ipc.client.connection.maxidletime", 500);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
      SampleRegionWALCoprocessor.class.getName());
    TEST_UTIL.startMiniDFSCluster(3);

    CONF = TEST_UTIL.getConfiguration();
    FS = TEST_UTIL.getDFSCluster().getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  protected abstract AbstractFSWAL<?> newWAL(FileSystem fs, Path rootDir, String WALDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix) throws IOException;

  protected abstract AbstractFSWAL<?> newSlowWAL(FileSystem fs, Path rootDir, String WALDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, Runnable action) throws IOException;

  /**
   * A loaded WAL coprocessor won't break existing WAL test cases.
   */
  @Test
  public void testWALCoprocessorLoaded() throws Exception {
    // test to see whether the coprocessor is loaded or not.
    AbstractFSWAL<?> wal = null;
    try {
      wal = newWAL(FS, CommonFSUtils.getWALRootDir(CONF), DIR.toString(),
          HConstants.HREGION_OLDLOGDIR_NAME, CONF, null, true, null, null);
      WALCoprocessorHost host = wal.getCoprocessorHost();
      Coprocessor c = host.findCoprocessor(SampleRegionWALCoprocessor.class);
      assertNotNull(c);
    } finally {
      if (wal != null) {
        wal.close();
      }
    }
  }

  protected void addEdits(WAL log, RegionInfo hri, TableDescriptor htd, int times,
      MultiVersionConcurrencyControl mvcc, NavigableMap<byte[], Integer> scopes)
      throws IOException {
    final byte[] row = Bytes.toBytes("row");
    for (int i = 0; i < times; i++) {
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, row, row, timestamp, row));
      WALKeyImpl key = new WALKeyImpl(hri.getEncodedNameAsBytes(), htd.getTableName(),
          SequenceId.NO_SEQUENCE_ID, timestamp, WALKey.EMPTY_UUIDS, HConstants.NO_NONCE,
          HConstants.NO_NONCE, mvcc, scopes);
      log.append(hri, key, cols, true);
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
    AbstractFSWAL<?> wal1 = null;
    AbstractFSWAL<?> walMeta = null;
    try {
      wal1 = newWAL(FS, CommonFSUtils.getWALRootDir(CONF), DIR.toString(),
          HConstants.HREGION_OLDLOGDIR_NAME, CONF, null, true, null, null);
      LOG.debug("Log obtained is: " + wal1);
      Comparator<Path> comp = wal1.LOG_NAME_COMPARATOR;
      Path p1 = wal1.computeFilename(11);
      Path p2 = wal1.computeFilename(12);
      // comparing with itself returns 0
      assertTrue(comp.compare(p1, p1) == 0);
      // comparing with different filenum.
      assertTrue(comp.compare(p1, p2) < 0);
      walMeta = newWAL(FS, CommonFSUtils.getWALRootDir(CONF), DIR.toString(),
          HConstants.HREGION_OLDLOGDIR_NAME, CONF, null, true, null,
          AbstractFSWALProvider.META_WAL_PROVIDER_ID);
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
   * On rolling a wal after reaching the threshold, {@link WAL#rollWriter()} returns the list of
   * regions which should be flushed in order to archive the oldest wal file.
   * <p>
   * This method tests this behavior by inserting edits and rolling the wal enough times to reach
   * the max number of logs threshold. It checks whether we get the "right regions" for flush on
   * rolling the wal.
   * @throws Exception
   */
  @Test
  public void testFindMemStoresEligibleForFlush() throws Exception {
    LOG.debug("testFindMemStoresEligibleForFlush");
    Configuration conf1 = HBaseConfiguration.create(CONF);
    conf1.setInt("hbase.regionserver.maxlogs", 1);
    AbstractFSWAL<?> wal = newWAL(FS, CommonFSUtils.getWALRootDir(conf1), DIR.toString(),
      HConstants.HREGION_OLDLOGDIR_NAME, conf1, null, true, null, null);
    TableDescriptor t1 = TableDescriptorBuilder.newBuilder(TableName.valueOf("t1"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("row")).build();
    TableDescriptor t2 = TableDescriptorBuilder.newBuilder(TableName.valueOf("t2"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("row")).build();
    RegionInfo hri1 = RegionInfoBuilder.newBuilder(t1.getTableName()).build();
    RegionInfo hri2 = RegionInfoBuilder.newBuilder(t2.getTableName()).build();
    // add edits and roll the wal
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    NavigableMap<byte[], Integer> scopes1 = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : t1.getColumnFamilyNames()) {
      scopes1.put(fam, 0);
    }
    NavigableMap<byte[], Integer> scopes2 = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : t2.getColumnFamilyNames()) {
      scopes2.put(fam, 0);
    }
    try {
      addEdits(wal, hri1, t1, 2, mvcc, scopes1);
      wal.rollWriter();
      // add some more edits and roll the wal. This would reach the log number threshold
      addEdits(wal, hri1, t1, 2, mvcc, scopes1);
      wal.rollWriter();
      // with above rollWriter call, the max logs limit is reached.
      assertTrue(wal.getNumRolledLogFiles() == 2);

      // get the regions to flush; since there is only one region in the oldest wal, it should
      // return only one region.
      byte[][] regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(1, regionsToFlush.length);
      assertEquals(hri1.getEncodedNameAsBytes(), regionsToFlush[0]);
      // insert edits in second region
      addEdits(wal, hri2, t2, 2, mvcc, scopes2);
      // get the regions to flush, it should still read region1.
      regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(1, regionsToFlush.length);
      assertEquals(hri1.getEncodedNameAsBytes(), regionsToFlush[0]);
      // flush region 1, and roll the wal file. Only last wal which has entries for region1 should
      // remain.
      flushRegion(wal, hri1.getEncodedNameAsBytes(), t1.getColumnFamilyNames());
      wal.rollWriter();
      // only one wal should remain now (that is for the second region).
      assertEquals(1, wal.getNumRolledLogFiles());
      // flush the second region
      flushRegion(wal, hri2.getEncodedNameAsBytes(), t2.getColumnFamilyNames());
      wal.rollWriter(true);
      // no wal should remain now.
      assertEquals(0, wal.getNumRolledLogFiles());
      // add edits both to region 1 and region 2, and roll.
      addEdits(wal, hri1, t1, 2, mvcc, scopes1);
      addEdits(wal, hri2, t2, 2, mvcc, scopes2);
      wal.rollWriter();
      // add edits and roll the writer, to reach the max logs limit.
      assertEquals(1, wal.getNumRolledLogFiles());
      addEdits(wal, hri1, t1, 2, mvcc, scopes1);
      wal.rollWriter();
      // it should return two regions to flush, as the oldest wal file has entries
      // for both regions.
      regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(2, regionsToFlush.length);
      // flush both regions
      flushRegion(wal, hri1.getEncodedNameAsBytes(), t1.getColumnFamilyNames());
      flushRegion(wal, hri2.getEncodedNameAsBytes(), t2.getColumnFamilyNames());
      wal.rollWriter(true);
      assertEquals(0, wal.getNumRolledLogFiles());
      // Add an edit to region1, and roll the wal.
      addEdits(wal, hri1, t1, 2, mvcc, scopes1);
      // tests partial flush: roll on a partial flush, and ensure that wal is not archived.
      wal.startCacheFlush(hri1.getEncodedNameAsBytes(), t1.getColumnFamilyNames());
      wal.rollWriter();
      wal.completeCacheFlush(hri1.getEncodedNameAsBytes());
      assertEquals(1, wal.getNumRolledLogFiles());
    } finally {
      if (wal != null) {
        wal.close();
      }
    }
  }

  @Test(expected = IOException.class)
  public void testFailedToCreateWALIfParentRenamed() throws IOException,
      CommonFSUtils.StreamLacksCapabilityException {
    final String name = "testFailedToCreateWALIfParentRenamed";
    AbstractFSWAL<?> wal = newWAL(FS, CommonFSUtils.getWALRootDir(CONF), name,
      HConstants.HREGION_OLDLOGDIR_NAME, CONF, null, true, null, null);
    long filenum = System.currentTimeMillis();
    Path path = wal.computeFilename(filenum);
    wal.createWriterInstance(path);
    Path parent = path.getParent();
    path = wal.computeFilename(filenum + 1);
    Path newPath = new Path(parent.getParent(), parent.getName() + "-splitting");
    FS.rename(parent, newPath);
    wal.createWriterInstance(path);
    fail("It should fail to create the new WAL");
  }

  /**
   * Test flush for sure has a sequence id that is beyond the last edit appended. We do this by
   * slowing appends in the background ring buffer thread while in foreground we call flush. The
   * addition of the sync over HRegion in flush should fix an issue where flush was returning before
   * all of its appends had made it out to the WAL (HBASE-11109).
   * @throws IOException
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-11109">HBASE-11109</a>
   */
  @Test
  public void testFlushSequenceIdIsGreaterThanAllEditsInHFile() throws IOException {
    String testName = currentTest.getMethodName();
    final TableName tableName = TableName.valueOf(testName);
    final RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).build();
    final byte[] rowName = tableName.getName();
    final TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
    HRegion r = HBaseTestingUtility.createRegionAndWAL(hri, TEST_UTIL.getDefaultRootDirPath(),
      TEST_UTIL.getConfiguration(), htd);
    HBaseTestingUtility.closeRegionAndWAL(r);
    final int countPerFamily = 10;
    final AtomicBoolean goslow = new AtomicBoolean(false);
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : htd.getColumnFamilyNames()) {
      scopes.put(fam, 0);
    }
    // subclass and doctor a method.
    AbstractFSWAL<?> wal = newSlowWAL(FS, CommonFSUtils.getWALRootDir(CONF), DIR.toString(),
        testName, CONF, null, true, null, null, new Runnable() {

          @Override
          public void run() {
            if (goslow.get()) {
              Threads.sleep(100);
              LOG.debug("Sleeping before appending 100ms");
            }
          }
        });
    HRegion region = HRegion.openHRegion(TEST_UTIL.getConfiguration(),
      TEST_UTIL.getTestFileSystem(), TEST_UTIL.getDefaultRootDirPath(), hri, htd, wal);
    EnvironmentEdge ee = EnvironmentEdgeManager.getDelegate();
    try {
      List<Put> puts = null;
      for (byte[] fam : htd.getColumnFamilyNames()) {
        puts =
            TestWALReplay.addRegionEdits(rowName, fam, countPerFamily, ee, region, "x");
      }

      // Now assert edits made it in.
      final Get g = new Get(rowName);
      Result result = region.get(g);
      assertEquals(countPerFamily * htd.getColumnFamilyNames().size(), result.size());

      // Construct a WALEdit and add it a few times to the WAL.
      WALEdit edits = new WALEdit();
      for (Put p : puts) {
        CellScanner cs = p.cellScanner();
        while (cs.advance()) {
          edits.add(cs.current());
        }
      }
      // Add any old cluster id.
      List<UUID> clusterIds = new ArrayList<>(1);
      clusterIds.add(TEST_UTIL.getRandomUUID());
      // Now make appends run slow.
      goslow.set(true);
      for (int i = 0; i < countPerFamily; i++) {
        final RegionInfo info = region.getRegionInfo();
        final WALKeyImpl logkey = new WALKeyImpl(info.getEncodedNameAsBytes(), tableName,
            System.currentTimeMillis(), clusterIds, -1, -1, region.getMVCC(), scopes);
        wal.append(info, logkey, edits, true);
        region.getMVCC().completeAndWait(logkey.getWriteEntry());
      }
      region.flush(true);
      // FlushResult.flushSequenceId is not visible here so go get the current sequence id.
      long currentSequenceId = region.getReadPoint(null);
      // Now release the appends
      goslow.set(false);
      assertTrue(currentSequenceId >= region.getReadPoint(null));
    } finally {
      region.close(true);
      wal.close();
    }
  }

  @Test
  public void testSyncNoAppend() throws IOException {
    String testName = currentTest.getMethodName();
    AbstractFSWAL<?> wal = newWAL(FS, CommonFSUtils.getWALRootDir(CONF), DIR.toString(), testName,
        CONF, null, true, null, null);
    try {
      wal.sync();
    } finally {
      wal.close();
    }
  }

  @Test
  public void testWriteEntryCanBeNull() throws IOException {
    String testName = currentTest.getMethodName();
    AbstractFSWAL<?> wal = newWAL(FS, CommonFSUtils.getWALRootDir(CONF), DIR.toString(), testName,
      CONF, null, true, null, null);
    wal.close();
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TableName.valueOf("table"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("row")).build();
    RegionInfo ri = RegionInfoBuilder.newBuilder(td.getTableName()).build();
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : td.getColumnFamilyNames()) {
      scopes.put(fam, 0);
    }
    long timestamp = System.currentTimeMillis();
    byte[] row = Bytes.toBytes("row");
    WALEdit cols = new WALEdit();
    cols.add(new KeyValue(row, row, row, timestamp, row));
    WALKeyImpl key =
        new WALKeyImpl(ri.getEncodedNameAsBytes(), td.getTableName(), SequenceId.NO_SEQUENCE_ID,
          timestamp, WALKey.EMPTY_UUIDS, HConstants.NO_NONCE, HConstants.NO_NONCE, mvcc, scopes);
    try {
      wal.append(ri, key, cols, true);
      fail("Should fail since the wal has already been closed");
    } catch (IOException e) {
      // expected
      assertThat(e.getMessage(), containsString("log is closed"));
      // the WriteEntry should be null since we fail before setting it.
      assertNull(key.getWriteEntry());
    }
  }

  @Test(expected = WALClosedException.class)
  public void testRollWriterForClosedWAL() throws IOException {
    String testName = currentTest.getMethodName();
    AbstractFSWAL<?> wal = newWAL(FS, CommonFSUtils.getWALRootDir(CONF), DIR.toString(), testName,
      CONF, null, true, null, null);
    wal.close();
    wal.rollWriter();
  }
}
