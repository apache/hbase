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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test creating/using/deleting snapshots from the client
 * <p>
 * This is an end-to-end test for the snapshot utility
 *
 * TODO This is essentially a clone of TestSnapshotFromClient.  This is worth refactoring this
 * because there will be a few more flavors of snapshots that need to run these tests.
 */
@Category({ClientTests.class, LargeTests.class})
public class TestMobFlushSnapshotFromClient {
  private static final Log LOG = LogFactory.getLog(TestFlushSnapshotFromClient.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final TableName TABLE_NAME =
      TableName.valueOf(STRING_TABLE_NAME);
  private final int DEFAULT_NUM_ROWS = 100;

  /**
   * Setup the config for the cluster
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    ((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 10);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @Before
  public void setup() throws Exception {
    MobSnapshotTestingUtils.createMobTable(UTIL, TABLE_NAME, 1, TEST_FAM);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(TABLE_NAME);

    SnapshotTestingUtils.deleteAllSnapshots(UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  /**
   * Test simple flush snapshotting a table that is online
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testFlushTableSnapshot() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // put some stuff in the table
    Table table = ConnectionFactory.createConnection(UTIL.getConfiguration()).getTable(TABLE_NAME);
    SnapshotTestingUtils.loadData(UTIL, TABLE_NAME, DEFAULT_NUM_ROWS, TEST_FAM);

    LOG.debug("FS state before snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
        FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the enabled table
    String snapshotString = "offlineTableSnapshot";
    byte[] snapshot = Bytes.toBytes(snapshotString);
    admin.snapshot(snapshotString, TABLE_NAME, SnapshotDescription.Type.FLUSH);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    List<SnapshotDescription> snapshots = SnapshotTestingUtils.assertOneSnapshotThatMatches(admin,
      snapshot, TABLE_NAME);

    // make sure its a valid snapshot
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    LOG.debug("FS state after snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
        FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    SnapshotTestingUtils.confirmSnapshotValid(snapshots.get(0), TABLE_NAME, TEST_FAM, rootDir,
        admin, fs);
  }

   /**
   * Test snapshotting a table that is online without flushing
   * @throws Exception
   */
  @Test(timeout=30000)
  public void testSkipFlushTableSnapshot() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // put some stuff in the table
    Table table = ConnectionFactory.createConnection(UTIL.getConfiguration()).getTable(TABLE_NAME);
    UTIL.loadTable(table, TEST_FAM);

    LOG.debug("FS state before snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
        FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the enabled table
    String snapshotString = "skipFlushTableSnapshot";
    byte[] snapshot = Bytes.toBytes(snapshotString);
    admin.snapshot(snapshotString, TABLE_NAME, SnapshotDescription.Type.SKIPFLUSH);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    List<SnapshotDescription> snapshots = SnapshotTestingUtils.assertOneSnapshotThatMatches(admin,
        snapshot, TABLE_NAME);

    // make sure its a valid snapshot
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    LOG.debug("FS state after snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
        FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    SnapshotTestingUtils.confirmSnapshotValid(snapshots.get(0), TABLE_NAME, TEST_FAM, rootDir,
        admin, fs);

    admin.deleteSnapshot(snapshot);
    snapshots = admin.listSnapshots();
    SnapshotTestingUtils.assertNoSnapshots(admin);
  }


  /**
   * Test simple flush snapshotting a table that is online
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testFlushTableSnapshotWithProcedure() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // put some stuff in the table
    Table table = ConnectionFactory.createConnection(UTIL.getConfiguration()).getTable(TABLE_NAME);
    SnapshotTestingUtils.loadData(UTIL, TABLE_NAME, DEFAULT_NUM_ROWS, TEST_FAM);

    LOG.debug("FS state before snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
        FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the enabled table
    String snapshotString = "offlineTableSnapshot";
    byte[] snapshot = Bytes.toBytes(snapshotString);
    Map<String, String> props = new HashMap<String, String>();
    props.put("table", TABLE_NAME.getNameAsString());
    admin.execProcedure(SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION,
        snapshotString, props);


    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    List<SnapshotDescription> snapshots = SnapshotTestingUtils.assertOneSnapshotThatMatches(admin,
      snapshot, TABLE_NAME);

    // make sure its a valid snapshot
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    LOG.debug("FS state after snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
        FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    SnapshotTestingUtils.confirmSnapshotValid(snapshots.get(0), TABLE_NAME, TEST_FAM, rootDir,
        admin, fs);
  }

  @Test (timeout=300000)
  public void testSnapshotFailsOnNonExistantTable() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    TableName tableName = TableName.valueOf("_not_a_table");

    // make sure the table doesn't exist
    boolean fail = false;
    do {
    try {
      admin.getTableDescriptor(tableName);
      fail = true;
      LOG.error("Table:" + tableName + " already exists, checking a new name");
      tableName = TableName.valueOf(tableName+"!");
    } catch (TableNotFoundException e) {
      fail = false;
      }
    } while (fail);

    // snapshot the non-existant table
    try {
      admin.snapshot("fail", tableName, SnapshotDescription.Type.FLUSH);
      fail("Snapshot succeeded even though there is not table.");
    } catch (SnapshotCreationException e) {
      LOG.info("Correctly failed to snapshot a non-existant table:" + e.getMessage());
    }
  }

  @Test(timeout = 300000)
  public void testAsyncFlushSnapshot() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("asyncSnapshot")
        .setTable(TABLE_NAME.getNameAsString())
        .setType(SnapshotDescription.Type.FLUSH)
        .build();

    // take the snapshot async
    admin.takeSnapshotAsync(snapshot);

    // constantly loop, looking for the snapshot to complete
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    SnapshotTestingUtils.waitForSnapshotToComplete(master, snapshot, 200);
    LOG.info(" === Async Snapshot Completed ===");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);
    // make sure we get the snapshot
    SnapshotTestingUtils.assertOneSnapshotThatMatches(admin, snapshot);
  }

  @Test (timeout=300000)
  public void testSnapshotStateAfterMerge() throws Exception {
    int numRows = DEFAULT_NUM_ROWS;
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    // load the table so we have some data
    SnapshotTestingUtils.loadData(UTIL, TABLE_NAME, numRows, TEST_FAM);

    // Take a snapshot
    String snapshotBeforeMergeName = "snapshotBeforeMerge";
    admin.snapshot(snapshotBeforeMergeName, TABLE_NAME, SnapshotDescription.Type.FLUSH);

    // Clone the table
    TableName cloneBeforeMergeName = TableName.valueOf("cloneBeforeMerge");
    admin.cloneSnapshot(snapshotBeforeMergeName, cloneBeforeMergeName);
    SnapshotTestingUtils.waitForTableToBeOnline(UTIL, cloneBeforeMergeName);

    // Merge two regions
    List<HRegionInfo> regions = admin.getTableRegions(TABLE_NAME);
    Collections.sort(regions, new Comparator<HRegionInfo>() {
      public int compare(HRegionInfo r1, HRegionInfo r2) {
        return Bytes.compareTo(r1.getStartKey(), r2.getStartKey());
      }
    });

    int numRegions = admin.getTableRegions(TABLE_NAME).size();
    int numRegionsAfterMerge = numRegions - 2;
    admin.mergeRegions(regions.get(1).getEncodedNameAsBytes(),
        regions.get(2).getEncodedNameAsBytes(), true);
    admin.mergeRegions(regions.get(5).getEncodedNameAsBytes(),
        regions.get(6).getEncodedNameAsBytes(), true);

    // Verify that there's one region less
    waitRegionsAfterMerge(numRegionsAfterMerge);
    assertEquals(numRegionsAfterMerge, admin.getTableRegions(TABLE_NAME).size());

    // Clone the table
    TableName cloneAfterMergeName = TableName.valueOf("cloneAfterMerge");
    admin.cloneSnapshot(snapshotBeforeMergeName, cloneAfterMergeName);
    SnapshotTestingUtils.waitForTableToBeOnline(UTIL, cloneAfterMergeName);

    MobSnapshotTestingUtils.verifyMobRowCount(UTIL, TABLE_NAME, numRows);
    MobSnapshotTestingUtils.verifyMobRowCount(UTIL, cloneBeforeMergeName, numRows);
    MobSnapshotTestingUtils.verifyMobRowCount(UTIL, cloneAfterMergeName, numRows);

    // test that we can delete the snapshot
    UTIL.deleteTable(cloneAfterMergeName);
    UTIL.deleteTable(cloneBeforeMergeName);
  }

  @Test (timeout=300000)
  public void testTakeSnapshotAfterMerge() throws Exception {
    int numRows = DEFAULT_NUM_ROWS;
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    // load the table so we have some data
    SnapshotTestingUtils.loadData(UTIL, TABLE_NAME, numRows, TEST_FAM);

    // Merge two regions
    List<HRegionInfo> regions = admin.getTableRegions(TABLE_NAME);
    Collections.sort(regions, new Comparator<HRegionInfo>() {
      public int compare(HRegionInfo r1, HRegionInfo r2) {
        return Bytes.compareTo(r1.getStartKey(), r2.getStartKey());
      }
    });

    int numRegions = admin.getTableRegions(TABLE_NAME).size();
    int numRegionsAfterMerge = numRegions - 2;
    admin.mergeRegions(regions.get(1).getEncodedNameAsBytes(),
        regions.get(2).getEncodedNameAsBytes(), true);
    admin.mergeRegions(regions.get(5).getEncodedNameAsBytes(),
        regions.get(6).getEncodedNameAsBytes(), true);

    waitRegionsAfterMerge(numRegionsAfterMerge);
    assertEquals(numRegionsAfterMerge, admin.getTableRegions(TABLE_NAME).size());

    // Take a snapshot
    String snapshotName = "snapshotAfterMerge";
    SnapshotTestingUtils.snapshot(admin, snapshotName, TABLE_NAME.getNameAsString(),
      SnapshotDescription.Type.FLUSH, 3);

    // Clone the table
    TableName cloneName = TableName.valueOf("cloneMerge");
    admin.cloneSnapshot(snapshotName, cloneName);
    SnapshotTestingUtils.waitForTableToBeOnline(UTIL, cloneName);

    MobSnapshotTestingUtils.verifyMobRowCount(UTIL, TABLE_NAME, numRows);
    MobSnapshotTestingUtils.verifyMobRowCount(UTIL, cloneName, numRows);

    // test that we can delete the snapshot
    UTIL.deleteTable(cloneName);
  }

  /**
   * Basic end-to-end test of simple-flush-based snapshots
   */
  @Test (timeout=300000)
  public void testFlushCreateListDestroy() throws Exception {
    LOG.debug("------- Starting Snapshot test -------------");
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    // load the table so we have some data
    SnapshotTestingUtils.loadData(UTIL, TABLE_NAME, DEFAULT_NUM_ROWS, TEST_FAM);

    String snapshotName = "flushSnapshotCreateListDestroy";
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    SnapshotTestingUtils.createSnapshotAndValidate(admin, TABLE_NAME, Bytes.toString(TEST_FAM),
      snapshotName, rootDir, fs, true);
  }

  /**
   * Demonstrate that we reject snapshot requests if there is a snapshot already running on the
   * same table currently running and that concurrent snapshots on different tables can both
   * succeed concurrently.
   */
  @Test(timeout=300000)
  public void testConcurrentSnapshottingAttempts() throws IOException, InterruptedException {
    final String STRING_TABLE2_NAME = STRING_TABLE_NAME + "2";
    final TableName TABLE2_NAME =
        TableName.valueOf(STRING_TABLE2_NAME);

    int ssNum = 20;
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    // create second testing table
    SnapshotTestingUtils.createTable(UTIL, TABLE2_NAME, TEST_FAM);
    // load the table so we have some data
    SnapshotTestingUtils.loadData(UTIL, TABLE_NAME, DEFAULT_NUM_ROWS, TEST_FAM);
    SnapshotTestingUtils.loadData(UTIL, TABLE2_NAME, DEFAULT_NUM_ROWS, TEST_FAM);

    final CountDownLatch toBeSubmitted = new CountDownLatch(ssNum);
    // We'll have one of these per thread
    class SSRunnable implements Runnable {
      SnapshotDescription ss;
      SSRunnable(SnapshotDescription ss) {
        this.ss = ss;
      }

      @Override
      public void run() {
        try {
          Admin admin = UTIL.getHBaseAdmin();
          LOG.info("Submitting snapshot request: " + ClientSnapshotDescriptionUtils.toString(ss));
          admin.takeSnapshotAsync(ss);
        } catch (Exception e) {
          LOG.info("Exception during snapshot request: " + ClientSnapshotDescriptionUtils.toString(
              ss)
              + ".  This is ok, we expect some", e);
        }
        LOG.info("Submitted snapshot request: " + ClientSnapshotDescriptionUtils.toString(ss));
        toBeSubmitted.countDown();
      }
    };

    // build descriptions
    SnapshotDescription[] descs = new SnapshotDescription[ssNum];
    for (int i = 0; i < ssNum; i++) {
      SnapshotDescription.Builder builder = SnapshotDescription.newBuilder();
      builder.setTable(((i % 2) == 0 ? TABLE_NAME : TABLE2_NAME).getNameAsString());
      builder.setName("ss"+i);
      builder.setType(SnapshotDescription.Type.FLUSH);
      descs[i] = builder.build();
    }

    // kick each off its own thread
    for (int i=0 ; i < ssNum; i++) {
      new Thread(new SSRunnable(descs[i])).start();
    }

    // wait until all have been submitted
    toBeSubmitted.await();

    // loop until all are done.
    while (true) {
      int doneCount = 0;
      for (SnapshotDescription ss : descs) {
        try {
          if (admin.isSnapshotFinished(ss)) {
            doneCount++;
          }
        } catch (Exception e) {
          LOG.warn("Got an exception when checking for snapshot " + ss.getName(), e);
          doneCount++;
        }
      }
      if (doneCount == descs.length) {
        break;
      }
      Thread.sleep(100);
    }

    // dump for debugging
    logFSTree(FSUtils.getRootDir(UTIL.getConfiguration()));

    List<SnapshotDescription> taken = admin.listSnapshots();
    int takenSize = taken.size();
    LOG.info("Taken " + takenSize + " snapshots:  " + taken);
    assertTrue("We expect at least 1 request to be rejected because of we concurrently" +
        " issued many requests", takenSize < ssNum && takenSize > 0);

    // Verify that there's at least one snapshot per table
    int t1SnapshotsCount = 0;
    int t2SnapshotsCount = 0;
    for (SnapshotDescription ss : taken) {
      if (TableName.valueOf(ss.getTable()).equals(TABLE_NAME)) {
        t1SnapshotsCount++;
      } else if (TableName.valueOf(ss.getTable()).equals(TABLE2_NAME)) {
        t2SnapshotsCount++;
      }
    }
    assertTrue("We expect at least 1 snapshot of table1 ", t1SnapshotsCount > 0);
    assertTrue("We expect at least 1 snapshot of table2 ", t2SnapshotsCount > 0);

    UTIL.deleteTable(TABLE2_NAME);
  }

  private void logFSTree(Path root) throws IOException {
    FSUtils.logFileSystemState(UTIL.getDFSCluster().getFileSystem(), root, LOG);
  }

  private void waitRegionsAfterMerge(final long numRegionsAfterMerge)
      throws IOException, InterruptedException {
    Admin admin = UTIL.getHBaseAdmin();
    // Verify that there's one region less
    long startTime = System.currentTimeMillis();
    while (admin.getTableRegions(TABLE_NAME).size() != numRegionsAfterMerge) {
      // This may be flaky... if after 15sec the merge is not complete give up
      // it will fail in the assertEquals(numRegionsAfterMerge).
      if ((System.currentTimeMillis() - startTime) > 15000)
        break;
      Thread.sleep(100);
    }
    SnapshotTestingUtils.waitForTableToBeOnline(UTIL, TABLE_NAME);
  }
}

