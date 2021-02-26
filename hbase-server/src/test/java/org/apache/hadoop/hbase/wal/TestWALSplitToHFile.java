/*
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
package org.apache.hadoop.hbase.wal;

import static org.apache.hadoop.hbase.regionserver.wal.AbstractTestWALReplay.addRegionEdits;
import static org.apache.hadoop.hbase.wal.WALSplitter.WAL_SPLIT_TO_HFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.CorruptHFileException;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.AbstractTestWALReplay;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestWALSplitToHFile {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALSplitToHFile.class);

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestWALReplay.class);
  static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private final EnvironmentEdge ee = EnvironmentEdgeManager.getDelegate();
  private Path rootDir = null;
  private String logName;
  private Path oldLogDir;
  private Path logDir;
  private FileSystem fs;
  private Configuration conf;
  private WALFactory wals;

  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] VALUE1 = Bytes.toBytes("value1");
  private static final byte[] VALUE2 = Bytes.toBytes("value2");
  private static final int countPerFamily = 10;

  @Rule
  public final TestName TEST_NAME = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean(WAL_SPLIT_TO_HFILE, true);
    UTIL.startMiniCluster(3);
    Path hbaseRootDir = UTIL.getDFSCluster().getFileSystem().makeQualified(new Path("/hbase"));
    LOG.info("hbase.rootdir=" + hbaseRootDir);
    CommonFSUtils.setRootDir(conf, hbaseRootDir);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create(UTIL.getConfiguration());
    this.conf.setBoolean(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS, false);
    this.fs = UTIL.getDFSCluster().getFileSystem();
    this.rootDir = CommonFSUtils.getRootDir(this.conf);
    this.oldLogDir = new Path(this.rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    String serverName =
        ServerName.valueOf(TEST_NAME.getMethodName() + "-manual", 16010, System.currentTimeMillis())
            .toString();
    this.logName = AbstractFSWALProvider.getWALDirectoryName(serverName);
    this.logDir = new Path(this.rootDir, logName);
    if (UTIL.getDFSCluster().getFileSystem().exists(this.rootDir)) {
      UTIL.getDFSCluster().getFileSystem().delete(this.rootDir, true);
    }
    this.wals = new WALFactory(conf, TEST_NAME.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    this.wals.close();
    UTIL.getDFSCluster().getFileSystem().delete(this.rootDir, true);
  }

  /*
   * @param p Directory to cleanup
   */
  private void deleteDir(final Path p) throws IOException {
    if (this.fs.exists(p)) {
      if (!this.fs.delete(p, true)) {
        throw new IOException("Failed remove of " + p);
      }
    }
  }

  private TableDescriptor createBasic3FamilyTD(final TableName tableName) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("a")).build());
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("b")).build());
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("c")).build());
    TableDescriptor td = builder.build();
    UTIL.getAdmin().createTable(td);
    return td;
  }

  private WAL createWAL(Configuration c, Path hbaseRootDir, String logName) throws IOException {
    FSHLog wal = new FSHLog(FileSystem.get(c), hbaseRootDir, logName, c);
    wal.init();
    return wal;
  }

  private WAL createWAL(FileSystem fs, Path hbaseRootDir, String logName) throws IOException {
    FSHLog wal = new FSHLog(fs, hbaseRootDir, logName, this.conf);
    wal.init();
    return wal;
  }

  private Pair<TableDescriptor, RegionInfo> setupTableAndRegion() throws IOException {
    final TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    final TableDescriptor td = createBasic3FamilyTD(tableName);
    final RegionInfo ri = RegionInfoBuilder.newBuilder(tableName).build();
    final Path tableDir = CommonFSUtils.getTableDir(this.rootDir, tableName);
    deleteDir(tableDir);
    FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, td, false);
    HRegion region = HBaseTestingUtility.createRegionAndWAL(ri, rootDir, this.conf, td);
    HBaseTestingUtility.closeRegionAndWAL(region);
    return new Pair<>(td, ri);
  }

  private void writeData(TableDescriptor td, HRegion region) throws IOException {
    final long timestamp = this.ee.currentTime();
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      region.put(new Put(ROW).addColumn(cfd.getName(), QUALIFIER, timestamp, VALUE1));
    }
  }

  @Test
  public void testDifferentRootDirAndWALRootDir() throws Exception {
    // Change wal root dir and reset the configuration
    Path walRootDir = UTIL.createWALRootDir();
    this.conf = HBaseConfiguration.create(UTIL.getConfiguration());

    FileSystem walFs = CommonFSUtils.getWALFileSystem(this.conf);
    this.oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    String serverName =
        ServerName.valueOf(TEST_NAME.getMethodName() + "-manual", 16010, System.currentTimeMillis())
            .toString();
    this.logName = AbstractFSWALProvider.getWALDirectoryName(serverName);
    this.logDir = new Path(walRootDir, logName);
    this.wals = new WALFactory(conf, TEST_NAME.getMethodName());

    Pair<TableDescriptor, RegionInfo> pair = setupTableAndRegion();
    TableDescriptor td = pair.getFirst();
    RegionInfo ri = pair.getSecond();

    WAL wal = createWAL(walFs, walRootDir, logName);
    HRegion region = HRegion.openHRegion(this.conf, this.fs, rootDir, ri, td, wal);
    writeData(td, region);

    // Now close the region without flush
    region.close(true);
    wal.shutdown();
    // split the log
    WALSplitter.split(walRootDir, logDir, oldLogDir, FileSystem.get(this.conf), this.conf, wals);

    WAL wal2 = createWAL(walFs, walRootDir, logName);
    HRegion region2 = HRegion.openHRegion(this.conf, this.fs, rootDir, ri, td, wal2);
    Result result2 = region2.get(new Get(ROW));
    assertEquals(td.getColumnFamilies().length, result2.size());
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      assertTrue(Bytes.equals(VALUE1, result2.getValue(cfd.getName(), QUALIFIER)));
    }
  }

  @Test
  public void testCorruptRecoveredHFile() throws Exception {
    Pair<TableDescriptor, RegionInfo> pair = setupTableAndRegion();
    TableDescriptor td = pair.getFirst();
    RegionInfo ri = pair.getSecond();

    WAL wal = createWAL(this.conf, rootDir, logName);
    HRegion region = HRegion.openHRegion(this.conf, this.fs, rootDir, ri, td, wal);
    writeData(td, region);

    // Now close the region without flush
    region.close(true);
    wal.shutdown();
    // split the log
    WALSplitter.split(rootDir, logDir, oldLogDir, FileSystem.get(this.conf), this.conf, wals);

    // Write a corrupt recovered hfile
    Path regionDir =
        new Path(CommonFSUtils.getTableDir(rootDir, td.getTableName()), ri.getEncodedName());
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      FileStatus[] files =
          WALSplitUtil.getRecoveredHFiles(this.fs, regionDir, cfd.getNameAsString());
      assertNotNull(files);
      assertTrue(files.length > 0);
      writeCorruptRecoveredHFile(files[0].getPath());
    }

    // Failed to reopen the region
    WAL wal2 = createWAL(this.conf, rootDir, logName);
    try {
      HRegion.openHRegion(this.conf, this.fs, rootDir, ri, td, wal2);
      fail("Should fail to open region");
    } catch (CorruptHFileException che) {
      // Expected
    }

    // Set skip errors to true and reopen the region
    this.conf.setBoolean(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS, true);
    HRegion region2 = HRegion.openHRegion(this.conf, this.fs, rootDir, ri, td, wal2);
    Result result2 = region2.get(new Get(ROW));
    assertEquals(td.getColumnFamilies().length, result2.size());
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      assertTrue(Bytes.equals(VALUE1, result2.getValue(cfd.getName(), QUALIFIER)));
      // Assert the corrupt file was skipped and still exist
      FileStatus[] files =
          WALSplitUtil.getRecoveredHFiles(this.fs, regionDir, cfd.getNameAsString());
      assertNotNull(files);
      assertEquals(1, files.length);
      assertTrue(files[0].getPath().getName().contains("corrupt"));
    }
  }

  @Test
  public void testPutWithSameTimestamp() throws Exception {
    Pair<TableDescriptor, RegionInfo> pair = setupTableAndRegion();
    TableDescriptor td = pair.getFirst();
    RegionInfo ri = pair.getSecond();

    WAL wal = createWAL(this.conf, rootDir, logName);
    HRegion region = HRegion.openHRegion(this.conf, this.fs, rootDir, ri, td, wal);
    final long timestamp = this.ee.currentTime();
    // Write data and flush
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      region.put(new Put(ROW).addColumn(cfd.getName(), QUALIFIER, timestamp, VALUE1));
    }
    region.flush(true);

    // Write data with same timestamp and do not flush
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      region.put(new Put(ROW).addColumn(cfd.getName(), QUALIFIER, timestamp, VALUE2));
    }
    // Now close the region without flush
    region.close(true);
    wal.shutdown();
    // split the log
    WALSplitter.split(rootDir, logDir, oldLogDir, FileSystem.get(this.conf), this.conf, wals);

    // reopen the region
    WAL wal2 = createWAL(this.conf, rootDir, logName);
    HRegion region2 = HRegion.openHRegion(conf, this.fs, rootDir, ri, td, wal2);
    Result result2 = region2.get(new Get(ROW));
    assertEquals(td.getColumnFamilies().length, result2.size());
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      assertTrue(Bytes.equals(VALUE2, result2.getValue(cfd.getName(), QUALIFIER)));
    }
  }

  @Test
  public void testRecoverSequenceId() throws Exception {
    Pair<TableDescriptor, RegionInfo> pair = setupTableAndRegion();
    TableDescriptor td = pair.getFirst();
    RegionInfo ri = pair.getSecond();

    WAL wal = createWAL(this.conf, rootDir, logName);
    HRegion region = HRegion.openHRegion(this.conf, this.fs, rootDir, ri, td, wal);
    Map<Integer, Map<String, Long>> seqIdMap = new HashMap<>();
    // Write data and do not flush
    for (int i = 0; i < countPerFamily; i++) {
      for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
        region.put(new Put(Bytes.toBytes(i)).addColumn(cfd.getName(), QUALIFIER, VALUE1));
        Result result = region.get(new Get(Bytes.toBytes(i)).addFamily(cfd.getName()));
        assertTrue(Bytes.equals(VALUE1, result.getValue(cfd.getName(), QUALIFIER)));
        List<Cell> cells = result.listCells();
        assertEquals(1, cells.size());
        seqIdMap.computeIfAbsent(i, r -> new HashMap<>()).put(cfd.getNameAsString(),
          cells.get(0).getSequenceId());
      }
    }

    // Now close the region without flush
    region.close(true);
    wal.shutdown();
    // split the log
    WALSplitter.split(rootDir, logDir, oldLogDir, FileSystem.get(this.conf), this.conf, wals);

    // reopen the region
    WAL wal2 = createWAL(this.conf, rootDir, logName);
    HRegion region2 = HRegion.openHRegion(conf, this.fs, rootDir, ri, td, wal2);
    // assert the seqid was recovered
    for (int i = 0; i < countPerFamily; i++) {
      for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
        Result result = region2.get(new Get(Bytes.toBytes(i)).addFamily(cfd.getName()));
        assertTrue(Bytes.equals(VALUE1, result.getValue(cfd.getName(), QUALIFIER)));
        List<Cell> cells = result.listCells();
        assertEquals(1, cells.size());
        assertEquals((long) seqIdMap.get(i).get(cfd.getNameAsString()),
          cells.get(0).getSequenceId());
      }
    }
  }

  /**
   * Test writing edits into an HRegion, closing it, splitting logs, opening
   * Region again.  Verify seqids.
   */
  @Test
  public void testWrittenViaHRegion()
      throws IOException, SecurityException, IllegalArgumentException, InterruptedException {
    Pair<TableDescriptor, RegionInfo> pair = setupTableAndRegion();
    TableDescriptor td = pair.getFirst();
    RegionInfo ri = pair.getSecond();

    // Write countPerFamily edits into the three families.  Do a flush on one
    // of the families during the load of edits so its seqid is not same as
    // others to test we do right thing when different seqids.
    WAL wal = createWAL(this.conf, rootDir, logName);
    HRegion region = HRegion.openHRegion(this.conf, this.fs, rootDir, ri, td, wal);
    long seqid = region.getOpenSeqNum();
    boolean first = true;
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      addRegionEdits(ROW, cfd.getName(), countPerFamily, this.ee, region, "x");
      if (first) {
        // If first, so we have at least one family w/ different seqid to rest.
        region.flush(true);
        first = false;
      }
    }
    // Now assert edits made it in.
    final Get g = new Get(ROW);
    Result result = region.get(g);
    assertEquals(countPerFamily * td.getColumnFamilies().length, result.size());
    // Now close the region (without flush), split the log, reopen the region and assert that
    // replay of log has the correct effect, that our seqids are calculated correctly so
    // all edits in logs are seen as 'stale'/old.
    region.close(true);
    wal.shutdown();
    try {
      WALSplitter.split(rootDir, logDir, oldLogDir, FileSystem.get(this.conf), this.conf, wals);
    } catch (Exception e) {
      LOG.debug("Got exception", e);
    }

    WAL wal2 = createWAL(this.conf, rootDir, logName);
    HRegion region2 = HRegion.openHRegion(conf, this.fs, rootDir, ri, td, wal2);
    long seqid2 = region2.getOpenSeqNum();
    assertTrue(seqid + result.size() < seqid2);
    final Result result1b = region2.get(g);
    assertEquals(result.size(), result1b.size());

    // Next test.  Add more edits, then 'crash' this region by stealing its wal
    // out from under it and assert that replay of the log adds the edits back
    // correctly when region is opened again.
    for (ColumnFamilyDescriptor hcd : td.getColumnFamilies()) {
      addRegionEdits(ROW, hcd.getName(), countPerFamily, this.ee, region2, "y");
    }
    // Get count of edits.
    final Result result2 = region2.get(g);
    assertEquals(2 * result.size(), result2.size());
    wal2.sync();
    final Configuration newConf = HBaseConfiguration.create(this.conf);
    User user = HBaseTestingUtility.getDifferentUser(newConf, td.getTableName().getNameAsString());
    user.runAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        WALSplitter.split(rootDir, logDir, oldLogDir, FileSystem.get(conf), conf, wals);
        FileSystem newFS = FileSystem.get(newConf);
        // Make a new wal for new region open.
        WAL wal3 = createWAL(newConf, rootDir, logName);
        Path tableDir = CommonFSUtils.getTableDir(rootDir, td.getTableName());
        HRegion region3 = new HRegion(tableDir, wal3, newFS, newConf, ri, td, null);
        long seqid3 = region3.initialize();
        Result result3 = region3.get(g);
        // Assert that count of cells is same as before crash.
        assertEquals(result2.size(), result3.size());

        // I can't close wal1.  Its been appropriated when we split.
        region3.close();
        wal3.close();
        return null;
      }
    });
  }

  /**
   * Test that we recover correctly when there is a failure in between the
   * flushes. i.e. Some stores got flushed but others did not.
   * Unfortunately, there is no easy hook to flush at a store level. The way
   * we get around this is by flushing at the region level, and then deleting
   * the recently flushed store file for one of the Stores. This would put us
   * back in the situation where all but that store got flushed and the region
   * died.
   * We restart Region again, and verify that the edits were replayed.
   */
  @Test
  public void testAfterPartialFlush()
      throws IOException, SecurityException, IllegalArgumentException {
    Pair<TableDescriptor, RegionInfo> pair = setupTableAndRegion();
    TableDescriptor td = pair.getFirst();
    RegionInfo ri = pair.getSecond();

    // Write countPerFamily edits into the three families.  Do a flush on one
    // of the families during the load of edits so its seqid is not same as
    // others to test we do right thing when different seqids.
    WAL wal = createWAL(this.conf, rootDir, logName);
    HRegion region = HRegion.openHRegion(this.conf, this.fs, rootDir, ri, td, wal);
    long seqid = region.getOpenSeqNum();
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      addRegionEdits(ROW, cfd.getName(), countPerFamily, this.ee, region, "x");
    }

    // Now assert edits made it in.
    final Get g = new Get(ROW);
    Result result = region.get(g);
    assertEquals(countPerFamily * td.getColumnFamilies().length, result.size());

    // Let us flush the region
    region.flush(true);
    region.close(true);
    wal.shutdown();

    // delete the store files in the second column family to simulate a failure
    // in between the flushcache();
    // we have 3 families. killing the middle one ensures that taking the maximum
    // will make us fail.
    int cf_count = 0;
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      cf_count++;
      if (cf_count == 2) {
        region.getRegionFileSystem().deleteFamily(cfd.getNameAsString());
      }
    }

    // Let us try to split and recover
    WALSplitter.split(rootDir, logDir, oldLogDir, FileSystem.get(this.conf), this.conf, wals);
    WAL wal2 = createWAL(this.conf, rootDir, logName);
    HRegion region2 = HRegion.openHRegion(this.conf, this.fs, rootDir, ri, td, wal2);
    long seqid2 = region2.getOpenSeqNum();
    assertTrue(seqid + result.size() < seqid2);

    final Result result1b = region2.get(g);
    assertEquals(result.size(), result1b.size());
  }

  /**
   * Test that we could recover the data correctly after aborting flush. In the
   * test, first we abort flush after writing some data, then writing more data
   * and flush again, at last verify the data.
   */
  @Test
  public void testAfterAbortingFlush() throws IOException {
    Pair<TableDescriptor, RegionInfo> pair = setupTableAndRegion();
    TableDescriptor td = pair.getFirst();
    RegionInfo ri = pair.getSecond();

    // Write countPerFamily edits into the three families. Do a flush on one
    // of the families during the load of edits so its seqid is not same as
    // others to test we do right thing when different seqids.
    WAL wal = createWAL(this.conf, rootDir, logName);
    RegionServerServices rsServices = Mockito.mock(RegionServerServices.class);
    Mockito.doReturn(false).when(rsServices).isAborted();
    when(rsServices.getServerName()).thenReturn(ServerName.valueOf("foo", 10, 10));
    when(rsServices.getConfiguration()).thenReturn(conf);
    Configuration customConf = new Configuration(this.conf);
    customConf.set(DefaultStoreEngine.DEFAULT_STORE_FLUSHER_CLASS_KEY,
        AbstractTestWALReplay.CustomStoreFlusher.class.getName());
    HRegion region = HRegion.openHRegion(this.rootDir, ri, td, wal, customConf, rsServices, null);
    int writtenRowCount = 10;
    List<ColumnFamilyDescriptor> families = Arrays.asList(td.getColumnFamilies());
    for (int i = 0; i < writtenRowCount; i++) {
      Put put = new Put(Bytes.toBytes(td.getTableName() + Integer.toString(i)));
      put.addColumn(families.get(i % families.size()).getName(), Bytes.toBytes("q"),
          Bytes.toBytes("val"));
      region.put(put);
    }

    // Now assert edits made it in.
    RegionScanner scanner = region.getScanner(new Scan());
    assertEquals(writtenRowCount, getScannedCount(scanner));

    // Let us flush the region
    AbstractTestWALReplay.CustomStoreFlusher.throwExceptionWhenFlushing.set(true);
    try {
      region.flush(true);
      fail("Injected exception hasn't been thrown");
    } catch (IOException e) {
      LOG.info("Expected simulated exception when flushing region, {}", e.getMessage());
      // simulated to abort server
      Mockito.doReturn(true).when(rsServices).isAborted();
      region.setClosing(false); // region normally does not accept writes after
      // DroppedSnapshotException. We mock around it for this test.
    }
    // writing more data
    int moreRow = 10;
    for (int i = writtenRowCount; i < writtenRowCount + moreRow; i++) {
      Put put = new Put(Bytes.toBytes(td.getTableName() + Integer.toString(i)));
      put.addColumn(families.get(i % families.size()).getName(), Bytes.toBytes("q"),
          Bytes.toBytes("val"));
      region.put(put);
    }
    writtenRowCount += moreRow;
    // call flush again
    AbstractTestWALReplay.CustomStoreFlusher.throwExceptionWhenFlushing.set(false);
    try {
      region.flush(true);
    } catch (IOException t) {
      LOG.info(
          "Expected exception when flushing region because server is stopped," + t.getMessage());
    }

    region.close(true);
    wal.shutdown();

    // Let us try to split and recover
    WALSplitter.split(rootDir, logDir, oldLogDir, FileSystem.get(this.conf), this.conf, wals);
    WAL wal2 = createWAL(this.conf, rootDir, logName);
    Mockito.doReturn(false).when(rsServices).isAborted();
    HRegion region2 = HRegion.openHRegion(this.rootDir, ri, td, wal2, this.conf, rsServices, null);
    scanner = region2.getScanner(new Scan());
    assertEquals(writtenRowCount, getScannedCount(scanner));
  }

  private int getScannedCount(RegionScanner scanner) throws IOException {
    int scannedCount = 0;
    List<Cell> results = new ArrayList<>();
    while (true) {
      boolean existMore = scanner.next(results);
      if (!results.isEmpty()) {
        scannedCount++;
      }
      if (!existMore) {
        break;
      }
      results.clear();
    }
    return scannedCount;
  }

  private void writeCorruptRecoveredHFile(Path recoveredHFile) throws Exception {
    // Read the recovered hfile
    int fileSize = (int) fs.listStatus(recoveredHFile)[0].getLen();
    FSDataInputStream in = fs.open(recoveredHFile);
    byte[] fileContent = new byte[fileSize];
    in.readFully(0, fileContent, 0, fileSize);
    in.close();

    // Write a corrupt hfile by append garbage
    Path path = new Path(recoveredHFile.getParent(), recoveredHFile.getName() + ".corrupt");
    FSDataOutputStream out;
    out = fs.create(path);
    out.write(fileContent);
    out.write(Bytes.toBytes("-----"));
    out.close();
  }
}
