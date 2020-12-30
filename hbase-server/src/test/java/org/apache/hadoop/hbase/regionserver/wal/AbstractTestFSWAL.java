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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALCoprocessor;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.FlushPolicy;
import org.apache.hadoop.hbase.regionserver.FlushPolicyFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
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
      MultiVersionConcurrencyControl mvcc, NavigableMap<byte[], Integer> scopes, String cf)
      throws IOException {
    final byte[] row = Bytes.toBytes(cf);
    for (int i = 0; i < times; i++) {
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, row, row, timestamp, row));
      WALKeyImpl key = new WALKeyImpl(hri.getEncodedNameAsBytes(), htd.getTableName(),
          SequenceId.NO_SEQUENCE_ID, timestamp, WALKey.EMPTY_UUIDS, HConstants.NO_NONCE,
          HConstants.NO_NONCE, mvcc, scopes);
      log.appendData(hri, key, cols);
    }
    log.sync();
  }

  /**
   * helper method to simulate region flush for a WAL.
   */
  protected void flushRegion(WAL wal, byte[] regionEncodedName, Set<byte[]> flushedFamilyNames) {
    wal.startCacheFlush(regionEncodedName, flushedFamilyNames);
    wal.completeCacheFlush(regionEncodedName, HConstants.NO_SEQNUM);
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
   * the max number of logs threshold. It checks whether we get the "right regions and stores" for
   * flush on rolling the wal.
   * @throws Exception
   */
  @Test
  public void testFindMemStoresEligibleForFlush() throws Exception {
    LOG.debug("testFindMemStoresEligibleForFlush");
    Configuration conf1 = HBaseConfiguration.create(CONF);
    conf1.setInt("hbase.regionserver.maxlogs", 1);
    AbstractFSWAL<?> wal = newWAL(FS, CommonFSUtils.getWALRootDir(conf1), DIR.toString(),
      HConstants.HREGION_OLDLOGDIR_NAME, conf1, null, true, null, null);
    String cf1 = "cf1";
    String cf2 = "cf2";
    String cf3 = "cf3";
    TableDescriptor t1 = TableDescriptorBuilder.newBuilder(TableName.valueOf("t1"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf1)).build();
    TableDescriptor t2 = TableDescriptorBuilder.newBuilder(TableName.valueOf("t2"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf1)).build();
    RegionInfo hri1 = RegionInfoBuilder.newBuilder(t1.getTableName()).build();
    RegionInfo hri2 = RegionInfoBuilder.newBuilder(t2.getTableName()).build();

    List<ColumnFamilyDescriptor> cfs = new ArrayList();
    cfs.add(ColumnFamilyDescriptorBuilder.of(cf1));
    cfs.add(ColumnFamilyDescriptorBuilder.of(cf2));
    TableDescriptor t3 = TableDescriptorBuilder.newBuilder(TableName.valueOf("t3"))
      .setColumnFamilies(cfs).build();
    RegionInfo hri3 = RegionInfoBuilder.newBuilder(t3.getTableName()).build();

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
    NavigableMap<byte[], Integer> scopes3 = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : t3.getColumnFamilyNames()) {
      scopes3.put(fam, 0);
    }
    try {
      addEdits(wal, hri1, t1, 2, mvcc, scopes1, cf1);
      wal.rollWriter();
      // add some more edits and roll the wal. This would reach the log number threshold
      addEdits(wal, hri1, t1, 2, mvcc, scopes1, cf1);
      wal.rollWriter();
      // with above rollWriter call, the max logs limit is reached.
      assertTrue(wal.getNumRolledLogFiles() == 2);

      // get the regions to flush; since there is only one region in the oldest wal, it should
      // return only one region.
      Map<byte[], List<byte[]>> regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(1, regionsToFlush.size());
      assertEquals(hri1.getEncodedNameAsBytes(), (byte[])regionsToFlush.keySet().toArray()[0]);
      // insert edits in second region
      addEdits(wal, hri2, t2, 2, mvcc, scopes2, cf1);
      // get the regions to flush, it should still read region1.
      regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(1, regionsToFlush.size());
      assertEquals(hri1.getEncodedNameAsBytes(), (byte[])regionsToFlush.keySet().toArray()[0]);
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
      addEdits(wal, hri1, t1, 2, mvcc, scopes1, cf1);
      addEdits(wal, hri2, t2, 2, mvcc, scopes2, cf1);
      wal.rollWriter();
      // add edits and roll the writer, to reach the max logs limit.
      assertEquals(1, wal.getNumRolledLogFiles());
      addEdits(wal, hri1, t1, 2, mvcc, scopes1, cf1);
      wal.rollWriter();
      // it should return two regions to flush, as the oldest wal file has entries
      // for both regions.
      regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(2, regionsToFlush.size());
      // flush both regions
      flushRegion(wal, hri1.getEncodedNameAsBytes(), t1.getColumnFamilyNames());
      flushRegion(wal, hri2.getEncodedNameAsBytes(), t2.getColumnFamilyNames());
      wal.rollWriter(true);
      assertEquals(0, wal.getNumRolledLogFiles());
      // Add an edit to region1, and roll the wal.
      addEdits(wal, hri1, t1, 2, mvcc, scopes1, cf1);
      // tests partial flush: roll on a partial flush, and ensure that wal is not archived.
      wal.startCacheFlush(hri1.getEncodedNameAsBytes(), t1.getColumnFamilyNames());
      wal.rollWriter();
      wal.completeCacheFlush(hri1.getEncodedNameAsBytes(), HConstants.NO_SEQNUM);
      assertEquals(1, wal.getNumRolledLogFiles());

      // clear test data
      flushRegion(wal, hri1.getEncodedNameAsBytes(), t1.getColumnFamilyNames());
      wal.rollWriter(true);
      // add edits for three familes
      addEdits(wal, hri3, t3, 2, mvcc, scopes3, cf1);
      addEdits(wal, hri3, t3, 2, mvcc, scopes3, cf2);
      addEdits(wal, hri3, t3, 2, mvcc, scopes3, cf3);
      wal.rollWriter();
      addEdits(wal, hri3, t3, 2, mvcc, scopes3, cf1);
      wal.rollWriter();
      assertEquals(2, wal.getNumRolledLogFiles());
      // flush one family before archive oldest wal
      Set<byte[]> flushedFamilyNames = new HashSet<>();
      flushedFamilyNames.add(Bytes.toBytes(cf1));
      flushRegion(wal, hri3.getEncodedNameAsBytes(), flushedFamilyNames);
      regionsToFlush = wal.findRegionsToForceFlush();
      // then only two family need to be flushed when archive oldest wal
      assertEquals(1, regionsToFlush.size());
      assertEquals(hri3.getEncodedNameAsBytes(), (byte[])regionsToFlush.keySet().toArray()[0]);
      assertEquals(2, regionsToFlush.get(hri3.getEncodedNameAsBytes()).size());
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
    wal.init();
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

  private AbstractFSWAL<?> createHoldingWAL(String testName, AtomicBoolean startHoldingForAppend,
    CountDownLatch holdAppend) throws IOException {
    AbstractFSWAL<?> wal = newWAL(FS, CommonFSUtils.getRootDir(CONF), testName,
      HConstants.HREGION_OLDLOGDIR_NAME, CONF, null, true, null, null);
    wal.init();
    wal.registerWALActionsListener(new WALActionsListener() {
      @Override
      public void visitLogEntryBeforeWrite(WALKey logKey, WALEdit logEdit) throws IOException {
        if (startHoldingForAppend.get()) {
          try {
            holdAppend.await();
          } catch (InterruptedException e) {
            LOG.error(e.toString(), e);
          }
        }
      }
    });
    return wal;
  }

  private HRegion createHoldingHRegion(Configuration conf, TableDescriptor htd, WAL wal)
    throws IOException {
    RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    TEST_UTIL.createLocalHRegion(hri, CONF, htd, wal).close();
    RegionServerServices rsServices = mock(RegionServerServices.class);
    when(rsServices.getServerName()).thenReturn(ServerName.valueOf("localhost:12345", 123456));
    when(rsServices.getConfiguration()).thenReturn(conf);
    return HRegion.openHRegion(TEST_UTIL.getDataTestDir(), hri, htd, wal, conf, rsServices, null);
  }

  private void doPutWithAsyncWAL(ExecutorService exec, HRegion region, Put put,
    Runnable flushOrCloseRegion, AtomicBoolean startHoldingForAppend,
    CountDownLatch flushOrCloseFinished, CountDownLatch holdAppend)
    throws InterruptedException, IOException {
    // do a regular write first because of memstore size calculation.
    region.put(put);

    startHoldingForAppend.set(true);
    region.put(new Put(put).setDurability(Durability.ASYNC_WAL));

    // give the put a chance to start
    Threads.sleep(3000);

    exec.submit(flushOrCloseRegion);

    // give the flush a chance to start. Flush should have got the region lock, and
    // should have been waiting on the mvcc complete after this.
    Threads.sleep(3000);

    // let the append to WAL go through now that the flush already started
    holdAppend.countDown();
    flushOrCloseFinished.await();
  }

  // Testcase for HBASE-23181
  @Test
  public void testUnflushedSeqIdTrackingWithAsyncWal() throws IOException, InterruptedException {
    String testName = currentTest.getMethodName();
    byte[] b = Bytes.toBytes("b");
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(TableName.valueOf("table"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(b)).build();

    AtomicBoolean startHoldingForAppend = new AtomicBoolean(false);
    CountDownLatch holdAppend = new CountDownLatch(1);
    CountDownLatch closeFinished = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(1);
    AbstractFSWAL<?> wal = createHoldingWAL(testName, startHoldingForAppend, holdAppend);
    // open a new region which uses this WAL
    HRegion region = createHoldingHRegion(TEST_UTIL.getConfiguration(), htd, wal);
    try {
      doPutWithAsyncWAL(exec, region, new Put(b).addColumn(b, b, b), () -> {
        try {
          Map<?, ?> closeResult = region.close();
          LOG.info("Close result:" + closeResult);
          closeFinished.countDown();
        } catch (IOException e) {
          LOG.error(e.toString(), e);
        }
      }, startHoldingForAppend, closeFinished, holdAppend);

      // now check the region's unflushed seqIds.
      long seqId = wal.getEarliestMemStoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
      assertEquals("Found seqId for the region which is already closed", HConstants.NO_SEQNUM,
        seqId);
    } finally {
      exec.shutdownNow();
      region.close();
      wal.close();
    }
  }

  private static final Set<byte[]> STORES_TO_FLUSH =
    Collections.newSetFromMap(new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR));

  // Testcase for HBASE-23157
  @Test
  public void testMaxFlushedSequenceIdGoBackwards() throws IOException, InterruptedException {
    String testName = currentTest.getMethodName();
    byte[] a = Bytes.toBytes("a");
    byte[] b = Bytes.toBytes("b");
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(TableName.valueOf("table"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(a))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(b)).build();

    AtomicBoolean startHoldingForAppend = new AtomicBoolean(false);
    CountDownLatch holdAppend = new CountDownLatch(1);
    CountDownLatch flushFinished = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(2);
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setClass(FlushPolicyFactory.HBASE_FLUSH_POLICY_KEY, FlushSpecificStoresPolicy.class,
      FlushPolicy.class);
    AbstractFSWAL<?> wal = createHoldingWAL(testName, startHoldingForAppend, holdAppend);
    // open a new region which uses this WAL
    HRegion region = createHoldingHRegion(conf, htd, wal);
    try {
      Put put = new Put(a).addColumn(a, a, a).addColumn(b, b, b);
      doPutWithAsyncWAL(exec, region, put, () -> {
        try {
          HRegion.FlushResult flushResult = region.flush(true);
          LOG.info("Flush result:" + flushResult.getResult());
          LOG.info("Flush succeeded:" + flushResult.isFlushSucceeded());
          flushFinished.countDown();
        } catch (IOException e) {
          LOG.error(e.toString(), e);
        }
      }, startHoldingForAppend, flushFinished, holdAppend);

      // get the max flushed sequence id after the first flush
      long maxFlushedSeqId1 = region.getMaxFlushedSeqId();

      region.put(put);
      // this time we only flush family a
      STORES_TO_FLUSH.add(a);
      region.flush(false);

      // get the max flushed sequence id after the second flush
      long maxFlushedSeqId2 = region.getMaxFlushedSeqId();
      // make sure that the maxFlushedSequenceId does not go backwards
      assertTrue(
        "maxFlushedSeqId1(" + maxFlushedSeqId1 +
          ") is not greater than or equal to maxFlushedSeqId2(" + maxFlushedSeqId2 + ")",
        maxFlushedSeqId1 <= maxFlushedSeqId2);
    } finally {
      exec.shutdownNow();
      region.close();
      wal.close();
    }
  }

  public static final class FlushSpecificStoresPolicy extends FlushPolicy {

    @Override
    public Collection<HStore> selectStoresToFlush() {
      if (STORES_TO_FLUSH.isEmpty()) {
        return region.getStores();
      } else {
        return STORES_TO_FLUSH.stream().map(region::getStore).collect(Collectors.toList());
      }
    }
  }
}
