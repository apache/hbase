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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test replay of edits out of a WAL split.
 */
@Category(MediumTests.class)
public class TestWALReplay {
  public static final Log LOG = LogFactory.getLog(TestWALReplay.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final EnvironmentEdge ee = EnvironmentEdgeManager.getDelegate();
  private Path hbaseRootDir = null;
  private Path oldLogDir;
  private Path logDir;
  private FileSystem fs;
  private Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("dfs.support.append", true);
    // The below config supported by 0.20-append and CDH3b2
    conf.setInt("dfs.client.block.recovery.retries", 2);
    TEST_UTIL.startMiniCluster(3);
    Path hbaseRootDir =
      TEST_UTIL.getDFSCluster().getFileSystem().makeQualified(new Path("/hbase"));
    LOG.info("hbase.rootdir=" + hbaseRootDir);
    conf.set(HConstants.HBASE_DIR, hbaseRootDir.toString());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    this.fs = TEST_UTIL.getDFSCluster().getFileSystem();
    this.hbaseRootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
    this.oldLogDir = new Path(this.hbaseRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    this.logDir = new Path(this.hbaseRootDir, HConstants.HREGION_LOGDIR_NAME);
    if (TEST_UTIL.getDFSCluster().getFileSystem().exists(this.hbaseRootDir)) {
      TEST_UTIL.getDFSCluster().getFileSystem().delete(this.hbaseRootDir, true);
    }
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.getDFSCluster().getFileSystem().delete(this.hbaseRootDir, true);
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

  /**
   * 
   * @throws Exception
   */
  @Test
  public void testReplayEditsAfterRegionMovedWithMultiCF() throws Exception {
    final byte[] tableName = Bytes
        .toBytes("testReplayEditsAfterRegionMovedWithMultiCF");
    byte[] family1 = Bytes.toBytes("cf1");
    byte[] family2 = Bytes.toBytes("cf2");
    byte[] qualifier = Bytes.toBytes("q");
    byte[] value = Bytes.toBytes("testV");
    byte[][] familys = { family1, family2 };
    TEST_UTIL.createTable(tableName, familys);
    HTable htable = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Put put = new Put(Bytes.toBytes("r1"));
    put.add(family1, qualifier, value);
    htable.put(put);
    ResultScanner resultScanner = htable.getScanner(new Scan());
    int count = 0;
    while (resultScanner.next() != null) {
      count++;
    }
    resultScanner.close();
    assertEquals(1, count);

    MiniHBaseCluster hbaseCluster = TEST_UTIL.getMiniHBaseCluster();
    List<HRegion> regions = hbaseCluster.getRegions(tableName);
    assertEquals(1, regions.size());

    // move region to another regionserver
    HRegion destRegion = regions.get(0);
    int originServerNum = hbaseCluster
        .getServerWith(destRegion.getRegionName());
    assertTrue("Please start more than 1 regionserver", hbaseCluster
        .getRegionServerThreads().size() > 1);
    int destServerNum = 0;
    while (destServerNum == originServerNum) {
      destServerNum++;
    }
    HRegionServer originServer = hbaseCluster.getRegionServer(originServerNum);
    HRegionServer destServer = hbaseCluster.getRegionServer(destServerNum);
    // move region to destination regionserver
    moveRegionAndWait(destRegion, destServer);

    // delete the row
    Delete del = new Delete(Bytes.toBytes("r1"));
    htable.delete(del);
    resultScanner = htable.getScanner(new Scan());
    count = 0;
    while (resultScanner.next() != null) {
      count++;
    }
    resultScanner.close();
    assertEquals(0, count);

    // flush region and make major compaction
    destServer.getOnlineRegion(destRegion.getRegionName()).flushcache();
    // wait to complete major compaction
    for (Store store : destServer.getOnlineRegion(destRegion.getRegionName())
        .getStores().values()) {
      store.triggerMajorCompaction();
    }
    destServer.getOnlineRegion(destRegion.getRegionName()).compactStores();

    // move region to origin regionserver
    moveRegionAndWait(destRegion, originServer);
    // abort the origin regionserver
    originServer.abort("testing");

    // see what we get
    Result result = htable.get(new Get(Bytes.toBytes("r1")));
    if (result != null) {
      assertTrue("Row is deleted, but we get" + result.toString(),
          (result == null) || result.isEmpty());
    }
    resultScanner.close();
  }

  private void moveRegionAndWait(HRegion destRegion, HRegionServer destServer)
      throws InterruptedException, MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    TEST_UTIL.getHBaseAdmin().move(
        destRegion.getRegionInfo().getEncodedNameAsBytes(),
        Bytes.toBytes(destServer.getServerName().getServerName()));
    while (true) {
      ServerName serverName = master.getAssignmentManager()
          .getRegionServerOfRegion(destRegion.getRegionInfo());
      if (serverName != null && serverName.equals(destServer.getServerName())) break;
      Thread.sleep(10);
    }
  }

  /**
   * Tests for hbase-2727.
   * @throws Exception
   * @see https://issues.apache.org/jira/browse/HBASE-2727
   */
  @Test
  public void test2727() throws Exception {
    // Test being able to have > 1 set of edits in the recovered.edits directory.
    // Ensure edits are replayed properly.
    final String tableNameStr = "test2727";
    HRegionInfo hri = createBasic3FamilyHRegionInfo(tableNameStr);
    Path basedir = new Path(hbaseRootDir, tableNameStr);
    deleteDir(basedir);
    fs.mkdirs(new Path(basedir, hri.getEncodedName()));

    HTableDescriptor htd = createBasic3FamilyHTD(tableNameStr);
    HRegion region2 = HRegion.createHRegion(hri,
        hbaseRootDir, this.conf, htd);
    region2.close();
    region2.getLog().closeAndDelete();
    final byte [] tableName = Bytes.toBytes(tableNameStr);
    final byte [] rowName = tableName;

    HLog wal1 = createWAL(this.conf);
    // Add 1k to each family.
    final int countPerFamily = 1000;
    for (HColumnDescriptor hcd: htd.getFamilies()) {
      addWALEdits(tableName, hri, rowName, hcd.getName(), countPerFamily, ee,
          wal1, htd);
    }
    wal1.close();
    runWALSplit(this.conf);

    HLog wal2 = createWAL(this.conf);
    // Up the sequenceid so that these edits are after the ones added above.
    wal2.setSequenceNumber(wal1.getSequenceNumber());
    // Add 1k to each family.
    for (HColumnDescriptor hcd: htd.getFamilies()) {
      addWALEdits(tableName, hri, rowName, hcd.getName(), countPerFamily,
          ee, wal2, htd);
    }
    wal2.close();
    runWALSplit(this.conf);

    HLog wal3 = createWAL(this.conf);
    wal3.setSequenceNumber(wal2.getSequenceNumber());
    try {
      final HRegion region = new HRegion(basedir, wal3, this.fs, this.conf, hri,
        htd, null);
      long seqid = region.initialize();
      assertTrue(seqid > wal3.getSequenceNumber());

      // TODO: Scan all.
      region.close();
    } finally {
      wal3.closeAndDelete();
    }
  }

  /**
   * Test case of HRegion that is only made out of bulk loaded files.  Assert
   * that we don't 'crash'.
   * @throws IOException
   * @throws IllegalAccessException
   * @throws NoSuchFieldException
   * @throws IllegalArgumentException
   * @throws SecurityException
   */
  @Test
  public void testRegionMadeOfBulkLoadedFilesOnly()
  throws IOException, SecurityException, IllegalArgumentException,
      NoSuchFieldException, IllegalAccessException, InterruptedException {
    final String tableNameStr = "testRegionMadeOfBulkLoadedFilesOnly";
    final HRegionInfo hri = createBasic3FamilyHRegionInfo(tableNameStr);
    final Path basedir = new Path(this.hbaseRootDir, tableNameStr);
    deleteDir(basedir);
    final HTableDescriptor htd = createBasic3FamilyHTD(tableNameStr);
    HRegion region2 = HRegion.createHRegion(hri,
        hbaseRootDir, this.conf, htd);
    region2.close();
    region2.getLog().closeAndDelete();
    HLog wal = createWAL(this.conf);
    HRegion region = HRegion.openHRegion(hri, htd, wal, this.conf);

    byte [] family = htd.getFamilies().iterator().next().getName();
    Path f =  new Path(basedir, "hfile");
    HFileTestUtil.createHFile(this.conf, fs, f, family, family, Bytes.toBytes(""),
        Bytes.toBytes("z"), 10);
    List <Pair<byte[],String>>  hfs= new ArrayList<Pair<byte[],String>>(1);
    hfs.add(Pair.newPair(family, f.toString()));
    region.bulkLoadHFiles(hfs, true);

    // Add an edit so something in the WAL
    byte [] row = Bytes.toBytes(tableNameStr);
    region.put((new Put(row)).add(family, family, family));
    wal.sync();
    final int rowsInsertedCount = 11;

    assertEquals(rowsInsertedCount, getScannedCount(region.getScanner(new Scan())));

    // Now 'crash' the region by stealing its wal
    final Configuration newConf = HBaseConfiguration.create(this.conf);
    User user = HBaseTestingUtility.getDifferentUser(newConf,
        tableNameStr);
    user.runAs(new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        runWALSplit(newConf);
        HLog wal2 = createWAL(newConf);
        HRegion region2 = new HRegion(basedir, wal2, FileSystem.get(newConf),
          newConf, hri, htd, null);
        long seqid2 = region2.initialize();
        assertTrue(seqid2 > -1);
        assertEquals(rowsInsertedCount, getScannedCount(region2.getScanner(new Scan())));

        // I can't close wal1.  Its been appropriated when we split.
        region2.close();
        wal2.closeAndDelete();
        return null;
      }
    });
  }

  /**
   * HRegion test case that is made of a major compacted HFile (created with three bulk loaded
   * files) and an edit in the memstore.
   * This is for HBASE-10958 "[dataloss] Bulk loading with seqids can prevent some log entries
   * from being replayed"
   * @throws IOException
   * @throws IllegalAccessException
   * @throws NoSuchFieldException
   * @throws IllegalArgumentException
   * @throws SecurityException
   */
  @Test
  public void testCompactedBulkLoadedFiles()
      throws IOException, SecurityException, IllegalArgumentException,
      NoSuchFieldException, IllegalAccessException, InterruptedException {
    final String tableNameStr = "testCompactedBulkLoadedFiles";
    final HRegionInfo hri = createBasic3FamilyHRegionInfo(tableNameStr);
    final Path basedir = new Path(this.hbaseRootDir, tableNameStr);
    deleteDir(basedir);
    final HTableDescriptor htd = createBasic3FamilyHTD(tableNameStr);
    HRegion region2 = HRegion.createHRegion(hri,
        hbaseRootDir, this.conf, htd);
    region2.close();
    region2.getLog().closeAndDelete();
    HLog wal = createWAL(this.conf);
    HRegion region = HRegion.openHRegion(hri, htd, wal, this.conf);

    // Add an edit so something in the WAL
    byte [] row = Bytes.toBytes(tableNameStr);
    byte [] family = htd.getFamilies().iterator().next().getName();
    region.put((new Put(row)).add(family, family, family));
    wal.sync();

    List <Pair<byte[],String>>  hfs= new ArrayList<Pair<byte[],String>>(1);
    for (int i = 0; i < 3; i++) {
      Path f = new Path(basedir, "hfile"+i);
      HFileTestUtil.createHFile(this.conf, fs, f, family, family, Bytes.toBytes(i + "00"),
          Bytes.toBytes(i + "50"), 10);
      hfs.add(Pair.newPair(family, f.toString()));
    }
    region.bulkLoadHFiles(hfs, true);
    final int rowsInsertedCount = 31;
    assertEquals(rowsInsertedCount, getScannedCount(region.getScanner(new Scan())));

    // major compact to turn all the bulk loaded files into one normal file
    region.compactStores(true);
    assertEquals(rowsInsertedCount, getScannedCount(region.getScanner(new Scan())));

    // Now 'crash' the region by stealing its wal
    final Configuration newConf = HBaseConfiguration.create(this.conf);
    User user = HBaseTestingUtility.getDifferentUser(newConf,
        tableNameStr);
    user.runAs(new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        runWALSplit(newConf);
        HLog wal2 = createWAL(newConf);
        HRegion region2 = new HRegion(basedir, wal2, FileSystem.get(newConf),
            newConf, hri, htd, null);
        long seqid2 = region2.initialize();
        assertTrue(seqid2 > -1);
        assertEquals(rowsInsertedCount, getScannedCount(region2.getScanner(new Scan())));

        // I can't close wal1.  Its been appropriated when we split.
        region2.close();
        wal2.closeAndDelete();
        return null;
      }
    });
  }

  /**
   * Test writing edits into an HRegion, closing it, splitting logs, opening
   * Region again.  Verify seqids.
   * @throws IOException
   * @throws IllegalAccessException
   * @throws NoSuchFieldException
   * @throws IllegalArgumentException
   * @throws SecurityException
   */
  @Test
  public void testReplayEditsWrittenViaHRegion()
  throws IOException, SecurityException, IllegalArgumentException,
      NoSuchFieldException, IllegalAccessException, InterruptedException {
    final String tableNameStr = "testReplayEditsWrittenViaHRegion";
    final HRegionInfo hri = createBasic3FamilyHRegionInfo(tableNameStr);
    final Path basedir = new Path(this.hbaseRootDir, tableNameStr);
    deleteDir(basedir);
    final byte[] rowName = Bytes.toBytes(tableNameStr);
    final int countPerFamily = 10;
    final HTableDescriptor htd = createBasic3FamilyHTD(tableNameStr);
    HRegion region3 = HRegion.createHRegion(hri,
            hbaseRootDir, this.conf, htd);
    region3.close();
    region3.getLog().closeAndDelete();
    // Write countPerFamily edits into the three families.  Do a flush on one
    // of the families during the load of edits so its seqid is not same as
    // others to test we do right thing when different seqids.
    HLog wal = createWAL(this.conf);
    HRegion region = new HRegion(basedir, wal, this.fs, this.conf, hri, htd, null);
    long seqid = region.initialize();
    // HRegionServer usually does this. It knows the largest seqid across all regions.
    wal.setSequenceNumber(seqid);
    boolean first = true;
    for (HColumnDescriptor hcd: htd.getFamilies()) {
      addRegionEdits(rowName, hcd.getName(), countPerFamily, this.ee, region, "x");
      if (first ) {
        // If first, so we have at least one family w/ different seqid to rest.
        region.flushcache();
        first = false;
      }
    }
    // Now assert edits made it in.
    final Get g = new Get(rowName);
    Result result = region.get(g, null);
    assertEquals(countPerFamily * htd.getFamilies().size(),
      result.size());
    // Now close the region (without flush), split the log, reopen the region and assert that
    // replay of log has the correct effect, that our seqids are calculated correctly so
    // all edits in logs are seen as 'stale'/old.
    region.close(true);
    wal.close();
    runWALSplit(this.conf);
    HLog wal2 = createWAL(this.conf);
    HRegion region2 = new HRegion(basedir, wal2, this.fs, this.conf, hri, htd, null);
    long seqid2 = region2.initialize();
    // HRegionServer usually does this. It knows the largest seqid across all regions.
    wal2.setSequenceNumber(seqid2);
    assertTrue(seqid + result.size() < seqid2);
    final Result result1b = region2.get(g, null);
    assertEquals(result.size(), result1b.size());

    // Next test.  Add more edits, then 'crash' this region by stealing its wal
    // out from under it and assert that replay of the log adds the edits back
    // correctly when region is opened again.
    for (HColumnDescriptor hcd: htd.getFamilies()) {
      addRegionEdits(rowName, hcd.getName(), countPerFamily, this.ee, region2, "y");
    }
    // Get count of edits.
    final Result result2 = region2.get(g, null);
    assertEquals(2 * result.size(), result2.size());
    wal2.sync();
    // Set down maximum recovery so we dfsclient doesn't linger retrying something
    // long gone.
    HBaseTestingUtility.setMaxRecoveryErrorCount(wal2.getOutputStream(), 1);
    final Configuration newConf = HBaseConfiguration.create(this.conf);
    User user = HBaseTestingUtility.getDifferentUser(newConf,
      tableNameStr);
    user.runAs(new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        runWALSplit(newConf);
        FileSystem newFS = FileSystem.get(newConf);
        // Make a new wal for new region open.
        HLog wal3 = createWAL(newConf);
        final AtomicInteger countOfRestoredEdits = new AtomicInteger(0);
        HRegion region3 = new HRegion(basedir, wal3, newFS, newConf, hri, htd, null) {
          @Override
          protected boolean restoreEdit(Store s, KeyValue kv) {
            boolean b = super.restoreEdit(s, kv);
            countOfRestoredEdits.incrementAndGet();
            return b;
          }
        };
        long seqid3 = region3.initialize();
        // HRegionServer usually does this. It knows the largest seqid across all regions.
        wal3.setSequenceNumber(seqid3);
        Result result3 = region3.get(g, null);
        // Assert that count of cells is same as before crash.
        assertEquals(result2.size(), result3.size());
        assertEquals(htd.getFamilies().size() * countPerFamily,
          countOfRestoredEdits.get());

        // I can't close wal1.  Its been appropriated when we split.
        region3.close();
        wal3.closeAndDelete();
        return null;
      }
    });
  }

  /**
   * Test that we recover correctly when there is a failure in between the
   * flushes. i.e. Some stores got flushed but others did not.
   *
   * Unfortunately, there is no easy hook to flush at a store level. The way
   * we get around this is by flushing at the region level, and then deleting
   * the recently flushed store file for one of the Stores. This would put us
   * back in the situation where all but that store got flushed and the region
   * died.
   *
   * We restart Region again, and verify that the edits were replayed.
   *
   * @throws IOException
   * @throws IllegalAccessException
   * @throws NoSuchFieldException
   * @throws IllegalArgumentException
   * @throws SecurityException
   */
  @Test
  public void testReplayEditsAfterPartialFlush()
  throws IOException, SecurityException, IllegalArgumentException,
      NoSuchFieldException, IllegalAccessException, InterruptedException {
    final String tableNameStr = "testReplayEditsWrittenViaHRegion";
    final HRegionInfo hri = createBasic3FamilyHRegionInfo(tableNameStr);
    final Path basedir = new Path(this.hbaseRootDir, tableNameStr);
    deleteDir(basedir);
    final byte[] rowName = Bytes.toBytes(tableNameStr);
    final int countPerFamily = 10;
    final HTableDescriptor htd = createBasic3FamilyHTD(tableNameStr);
    HRegion region3 = HRegion.createHRegion(hri,
            hbaseRootDir, this.conf, htd);
    region3.close();
    region3.getLog().closeAndDelete();
    // Write countPerFamily edits into the three families.  Do a flush on one
    // of the families during the load of edits so its seqid is not same as
    // others to test we do right thing when different seqids.
    HLog wal = createWAL(this.conf);
    HRegion region = new HRegion(basedir, wal, this.fs, this.conf, hri, htd, null);
    long seqid = region.initialize();
    // HRegionServer usually does this. It knows the largest seqid across all regions.
    wal.setSequenceNumber(seqid);
    for (HColumnDescriptor hcd: htd.getFamilies()) {
      addRegionEdits(rowName, hcd.getName(), countPerFamily, this.ee, region, "x");
    }

    // Now assert edits made it in.
    final Get g = new Get(rowName);
    Result result = region.get(g, null);
    assertEquals(countPerFamily * htd.getFamilies().size(),
      result.size());

    // Let us flush the region
    region.flushcache();
    region.close(true);
    wal.close();

    // delete the store files in the second column family to simulate a failure
    // in between the flushcache();
    // we have 3 families. killing the middle one ensures that taking the maximum
    // will make us fail.
    int cf_count = 0;
    for (HColumnDescriptor hcd: htd.getFamilies()) {
      cf_count++;
      if (cf_count == 2) {
        this.fs.delete(new Path(region.getRegionDir(), Bytes.toString(hcd.getName()))
            , true);
      }
    }


    // Let us try to split and recover
    runWALSplit(this.conf);
    HLog wal2 = createWAL(this.conf);
    HRegion region2 = new HRegion(basedir, wal2, this.fs, this.conf, hri, htd, null);
    long seqid2 = region2.initialize();
    // HRegionServer usually does this. It knows the largest seqid across all regions.
    wal2.setSequenceNumber(seqid2);
    assertTrue(seqid + result.size() < seqid2);

    final Result result1b = region2.get(g, null);
    assertEquals(result.size(), result1b.size());
  }

  /**
   * Test that we could recover the data correctly after aborting flush. In the
   * test, first we abort flush after writing some data, then writing more data
   * and flush again, at last verify the data.
   * @throws IOException
   */
  @Test
  public void testReplayEditsAfterAbortingFlush() throws IOException {
    final String tableNameStr = "testReplayEditsAfterAbortingFlush";
    final HRegionInfo hri = createBasic3FamilyHRegionInfo(tableNameStr);
    final Path basedir = new Path(this.hbaseRootDir, tableNameStr);
    deleteDir(basedir);
    final HTableDescriptor htd = createBasic3FamilyHTD(tableNameStr);
    HRegion region3 = HRegion.createHRegion(hri, hbaseRootDir, this.conf, htd);
    region3.close();
    region3.getLog().closeAndDelete();
    // Write countPerFamily edits into the three families. Do a flush on one
    // of the families during the load of edits so its seqid is not same as
    // others to test we do right thing when different seqids.
    HLog wal = createWAL(this.conf);
    final AtomicBoolean throwExceptionWhenFlushing = new AtomicBoolean(false);
    RegionServerServices rsServices = Mockito.mock(RegionServerServices.class);
    Mockito.doReturn(false).when(rsServices).isAborted();
    HRegion region = new HRegion(basedir, wal, this.fs, this.conf, hri, htd,
        rsServices) {
      @Override
      protected Store instantiateHStore(Path tableDir, HColumnDescriptor c)
          throws IOException {
        return new Store(tableDir, this, c, fs, conf) {
          @Override
          protected Path flushCache(final long logCacheFlushId,
              SortedSet<KeyValue> snapshot,
              TimeRangeTracker snapshotTimeRangeTracker,
              AtomicLong flushedSize, MonitoredTask status) throws IOException {
            if (throwExceptionWhenFlushing.get()) {
              throw new IOException("Simulated exception by tests");
            }
            return super.flushCache(logCacheFlushId, snapshot,
                snapshotTimeRangeTracker, flushedSize, status);
          }
        };
      }
    };
    long seqid = region.initialize();
    // HRegionServer usually does this. It knows the largest seqid across all
    // regions.
    wal.setSequenceNumber(seqid);

    int writtenRowCount = 10;
    List<HColumnDescriptor> families = new ArrayList<HColumnDescriptor>(
        htd.getFamilies());
    for (int i = 0; i < writtenRowCount; i++) {
      Put put = new Put(Bytes.toBytes(tableNameStr + Integer.toString(i)));
      put.add(families.get(i % families.size()).getName(), Bytes.toBytes("q"),
          Bytes.toBytes("val"));
      region.put(put);
    }

    // Now assert edits made it in.
    RegionScanner scanner = region.getScanner(new Scan());
    assertEquals(writtenRowCount, getScannedCount(scanner));

    // Let us flush the region
    throwExceptionWhenFlushing.set(true);
    try {
      region.flushcache();
      fail("Injected exception hasn't been thrown");
    } catch (Throwable t) {
      LOG.info("Expected simulated exception when flushing region,"
          + t.getMessage());
      // simulated to abort server
      Mockito.doReturn(true).when(rsServices).isAborted();
    }
    // writing more data
    int moreRow = 10;
    for (int i = writtenRowCount; i < writtenRowCount + moreRow; i++) {
      Put put = new Put(Bytes.toBytes(tableNameStr + Integer.toString(i)));
      put.add(families.get(i % families.size()).getName(), Bytes.toBytes("q"),
          Bytes.toBytes("val"));
      region.put(put);
    }
    writtenRowCount += moreRow;
    // call flush again
    throwExceptionWhenFlushing.set(false);
    try {
      region.flushcache();
    } catch (IOException t) {
      LOG.info("Expected exception when flushing region because server is stopped,"
          + t.getMessage());
    }

    region.close(true);
    wal.close();

    // Let us try to split and recover
    runWALSplit(this.conf);
    HLog wal2 = createWAL(this.conf);
    Mockito.doReturn(false).when(rsServices).isAborted();
    HRegion region2 = new HRegion(basedir, wal2, this.fs, this.conf, hri, htd,
        rsServices);
    long seqid2 = region2.initialize();
    // HRegionServer usually does this. It knows the largest seqid across all
    // regions.
    wal2.setSequenceNumber(seqid2);

    scanner = region2.getScanner(new Scan());
    assertEquals(writtenRowCount, getScannedCount(scanner));
  }

  private int getScannedCount(RegionScanner scanner) throws IOException {
    int scannedCount = 0;
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (true) {
      boolean existMore = scanner.next(results);
      if (!results.isEmpty())
        scannedCount++;
      if (!existMore)
        break;
      results.clear();
    }
    return scannedCount;
  }

  /**
   * Create an HRegion with the result of a HLog split and test we only see the
   * good edits
   * @throws Exception
   */
  @Test
  public void testReplayEditsWrittenIntoWAL() throws Exception {
    final String tableNameStr = "testReplayEditsWrittenIntoWAL";
    final HRegionInfo hri = createBasic3FamilyHRegionInfo(tableNameStr);
    final Path basedir = new Path(hbaseRootDir, tableNameStr);
    deleteDir(basedir);
    fs.mkdirs(new Path(basedir, hri.getEncodedName()));
    final HTableDescriptor htd = createBasic3FamilyHTD(tableNameStr);
    HRegion region2 = HRegion.createHRegion(hri,
            hbaseRootDir, this.conf, htd);
    region2.close();
    region2.getLog().closeAndDelete();
    final HLog wal = createWAL(this.conf);
    final byte[] tableName = Bytes.toBytes(tableNameStr);
    final byte[] rowName = tableName;
    final byte[] regionName = hri.getEncodedNameAsBytes();

    // Add 1k to each family.
    final int countPerFamily = 1000;
    for (HColumnDescriptor hcd: htd.getFamilies()) {
      addWALEdits(tableName, hri, rowName, hcd.getName(), countPerFamily,
          ee, wal, htd);
    }

    // Add a cache flush, shouldn't have any effect
    long logSeqId = wal.startCacheFlush(regionName);
    wal.completeCacheFlush(regionName, tableName, logSeqId, hri.isMetaRegion());

    // Add an edit to another family, should be skipped.
    WALEdit edit = new WALEdit();
    long now = ee.currentTimeMillis();
    edit.add(new KeyValue(rowName, Bytes.toBytes("another family"), rowName,
      now, rowName));
    wal.append(hri, tableName, edit, now, htd);

    // Delete the c family to verify deletes make it over.
    edit = new WALEdit();
    now = ee.currentTimeMillis();
    edit.add(new KeyValue(rowName, Bytes.toBytes("c"), null, now,
      KeyValue.Type.DeleteFamily));
    wal.append(hri, tableName, edit, now, htd);

    // Sync.
    wal.sync();
    // Set down maximum recovery so we dfsclient doesn't linger retrying something
    // long gone.
    HBaseTestingUtility.setMaxRecoveryErrorCount(wal.getOutputStream(), 1);
    // Make a new conf and a new fs for the splitter to run on so we can take
    // over old wal.
    final Configuration newConf = HBaseConfiguration.create(this.conf);
    User user = HBaseTestingUtility.getDifferentUser(newConf,
      ".replay.wal.secondtime");
    user.runAs(new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        runWALSplit(newConf);
        FileSystem newFS = FileSystem.get(newConf);
        // 100k seems to make for about 4 flushes during HRegion#initialize.
        newConf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 100);
        // Make a new wal for new region.
        HLog newWal = createWAL(newConf);
        final AtomicInteger flushcount = new AtomicInteger(0);
        try {
          final HRegion region =
              new HRegion(basedir, newWal, newFS, newConf, hri, htd, null) {
            protected FlushResult internalFlushcache(
                final HLog wal, final long myseqid, MonitoredTask status)
            throws IOException {
              LOG.info("InternalFlushCache Invoked");
              FlushResult fs = super.internalFlushcache(wal, myseqid,
                  Mockito.mock(MonitoredTask.class));
              flushcount.incrementAndGet();
              return fs;
            };
          };
          long seqid = region.initialize();
          // We flushed during init.
          assertTrue("Flushcount=" + flushcount.get(), flushcount.get() > 0);
          assertTrue(seqid > wal.getSequenceNumber());

          Get get = new Get(rowName);
          Result result = region.get(get, -1);
          // Make sure we only see the good edits
          assertEquals(countPerFamily * (htd.getFamilies().size() - 1),
            result.size());
          region.close();
        } finally {
          newWal.closeAndDelete();
        }
        return null;
      }
    });
  }

  @Test
  public void testSequentialEditLogSeqNum() throws IOException {
    final String tableNameStr = "testSequentialEditLogSeqNum";
    final HRegionInfo hri = createBasic3FamilyHRegionInfo(tableNameStr);
    final Path basedir = new Path(this.hbaseRootDir, tableNameStr);
    deleteDir(basedir);
    final byte[] rowName = Bytes.toBytes(tableNameStr);
    final int countPerFamily = 10;
    final HTableDescriptor htd = createBasic1FamilyHTD(tableNameStr);

    // Mock the HLog
    MockHLog wal = createMockWAL(this.conf);
    
    HRegion region = new HRegion(basedir, wal, this.fs, this.conf, hri, htd, null);
    long seqid = region.initialize();
    // HRegionServer usually does this. It knows the largest seqid across all
    // regions.
    wal.setSequenceNumber(seqid);
    for (HColumnDescriptor hcd : htd.getFamilies()) {
      addRegionEdits(rowName, hcd.getName(), countPerFamily, this.ee, region, "x");
    }
    // get the seq no after first set of entries.
    long sequenceNumber = wal.getSequenceNumber();

    // Let us flush the region
    // But this time completeflushcache is not yet done
    region.flushcache();
    for (HColumnDescriptor hcd : htd.getFamilies()) {
      addRegionEdits(rowName, hcd.getName(), 5, this.ee, region, "x");
    }
    long lastestSeqNumber = wal.getSequenceNumber();
    // get the current seq no
    wal.doCompleteCacheFlush = true;
    // allow complete cache flush with the previous seq number got after first
    // set of edits.
    wal.completeCacheFlush(hri.getEncodedNameAsBytes(), hri.getTableName(), sequenceNumber, false);
    wal.close();
    FileStatus[] listStatus = this.fs.listStatus(wal.getDir());
    HLogSplitter.splitLogFile(hbaseRootDir, listStatus[0], this.fs, this.conf,
        null);
    FileStatus[] listStatus1 = this.fs.listStatus(new Path(hbaseRootDir + "/"
        + tableNameStr + "/" + hri.getEncodedName() + "/recovered.edits"));
    int editCount = 0;
    for (FileStatus fileStatus : listStatus1) {
      editCount = Integer.parseInt(fileStatus.getPath().getName());
    }
    // The sequence number should be same 
    assertEquals(
        "The sequence number of the recoverd.edits and the current edit seq should be same",
        lastestSeqNumber, editCount);
  }
  
   static class MockHLog extends HLog {
    boolean doCompleteCacheFlush = false;

    public MockHLog(FileSystem fs, Path dir, Path oldLogDir, Configuration conf) throws IOException {
      super(fs, dir, oldLogDir, conf);
    }

    @Override
    public void completeCacheFlush(byte[] encodedRegionName, byte[] tableName, long logSeqId,
        boolean isMetaRegion) throws IOException {
      if (!doCompleteCacheFlush) {
        return;
      }
      super.completeCacheFlush(encodedRegionName, tableName, logSeqId, isMetaRegion);
    }
  }

  private HTableDescriptor createBasic1FamilyHTD(final String tableName) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor a = new HColumnDescriptor(Bytes.toBytes("a"));
    htd.addFamily(a);
    return htd;
  }
  
  private MockHLog createMockWAL(Configuration conf) throws IOException {
    MockHLog wal = new MockHLog(FileSystem.get(conf), logDir, oldLogDir, conf);
    // Set down maximum recovery so we dfsclient doesn't linger retrying something
    // long gone.
    HBaseTestingUtility.setMaxRecoveryErrorCount(wal.getOutputStream(), 1);
    return wal;
  }
  
    
  // Flusher used in this test.  Keep count of how often we are called and
  // actually run the flush inside here.
  class TestFlusher implements FlushRequester {
    private int count = 0;
    private HRegion r;

    @Override
    public void requestFlush(HRegion region) {
      count++;
      try {
        r.flushcache();
      } catch (IOException e) {
        throw new RuntimeException("Exception flushing", e);
      }
    }

    @Override
    public void requestDelayedFlush(HRegion region, long when) {
      // TODO Auto-generated method stub
      
    }
  }

  private void addWALEdits (final byte [] tableName, final HRegionInfo hri,
      final byte [] rowName, final byte [] family,
      final int count, EnvironmentEdge ee, final HLog wal, final HTableDescriptor htd)
  throws IOException {
    String familyStr = Bytes.toString(family);
    for (int j = 0; j < count; j++) {
      byte[] qualifierBytes = Bytes.toBytes(Integer.toString(j));
      byte[] columnBytes = Bytes.toBytes(familyStr + ":" + Integer.toString(j));
      WALEdit edit = new WALEdit();
      edit.add(new KeyValue(rowName, family, qualifierBytes,
        ee.currentTimeMillis(), columnBytes));
      wal.append(hri, tableName, edit, ee.currentTimeMillis(), htd);
    }
  }

  private void addRegionEdits (final byte [] rowName, final byte [] family,
      final int count, EnvironmentEdge ee, final HRegion r,
      final String qualifierPrefix)
  throws IOException {
    for (int j = 0; j < count; j++) {
      byte[] qualifier = Bytes.toBytes(qualifierPrefix + Integer.toString(j));
      Put p = new Put(rowName);
      p.add(family, qualifier, ee.currentTimeMillis(), rowName);
      r.put(p);
    }
  }

  /*
   * Creates an HRI around an HTD that has <code>tableName</code> and three
   * column families named 'a','b', and 'c'.
   * @param tableName Name of table to use when we create HTableDescriptor.
   */
   private HRegionInfo createBasic3FamilyHRegionInfo(final String tableName) {
    return new HRegionInfo(Bytes.toBytes(tableName), null, null, false);
   }

  /*
   * Run the split.  Verify only single split file made.
   * @param c
   * @return The single split file made
   * @throws IOException
   */
  private Path runWALSplit(final Configuration c) throws IOException {
    FileSystem fs = FileSystem.get(c);
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(c,
        this.hbaseRootDir, this.logDir, this.oldLogDir, fs);
    List<Path> splits = logSplitter.splitLog();
    // Split should generate only 1 file since there's only 1 region
    assertEquals("splits=" + splits, 1, splits.size());
    // Make sure the file exists
    assertTrue(fs.exists(splits.get(0)));
    LOG.info("Split file=" + splits.get(0));
    return splits.get(0);
  }

  /*
   * @param c
   * @return WAL with retries set down from 5 to 1 only.
   * @throws IOException
   */
  private HLog createWAL(final Configuration c) throws IOException {
    HLog wal = new HLog(FileSystem.get(c), logDir, oldLogDir, c);
    // Set down maximum recovery so we dfsclient doesn't linger retrying something
    // long gone.
    HBaseTestingUtility.setMaxRecoveryErrorCount(wal.getOutputStream(), 1);
    return wal;
  }

  private HTableDescriptor createBasic3FamilyHTD(final String tableName) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor a = new HColumnDescriptor(Bytes.toBytes("a"));
    htd.addFamily(a);
    HColumnDescriptor b = new HColumnDescriptor(Bytes.toBytes("b"));
    htd.addFamily(b);
    HColumnDescriptor c = new HColumnDescriptor(Bytes.toBytes("c"));
    htd.addFamily(c);
    return htd;
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

