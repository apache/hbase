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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
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
@Category(LargeTests.class)
public class TestFlushSnapshotFromClient {
  private static final Log LOG = LogFactory.getLog(TestFlushSnapshotFromClient.class);
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 2;
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final TableName TABLE_NAME = TableName.valueOf("test");
  private final int DEFAULT_NUM_ROWS = 100;

  /**
   * Setup the config for the cluster
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    //((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)AbstractRpcClient.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
  }

  protected static void setupConf(Configuration conf) {
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
  }

  @Before
  public void setup() throws Exception {
    SnapshotTestingUtils.createTable(UTIL, TABLE_NAME, TEST_FAM);
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
    SnapshotTestingUtils.loadData(UTIL, TABLE_NAME, DEFAULT_NUM_ROWS, TEST_FAM);

    LOG.debug("FS state before snapshot:");
    UTIL.getHBaseCluster().getMaster().getMasterFileSystem().logFileSystemState(LOG);

    // take a snapshot of the enabled table
    String snapshotString = "offlineTableSnapshot";
    byte[] snapshot = Bytes.toBytes(snapshotString);
    admin.snapshot(snapshotString, TABLE_NAME, SnapshotDescription.Type.FLUSH);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    List<SnapshotDescription> snapshots = SnapshotTestingUtils.assertOneSnapshotThatMatches(admin,
      snapshot, TABLE_NAME);

    // make sure its a valid snapshot
    LOG.debug("FS state after snapshot:");
    UTIL.getHBaseCluster().getMaster().getMasterFileSystem().logFileSystemState(LOG);

    SnapshotTestingUtils.confirmSnapshotValid(UTIL, snapshots.get(0), TABLE_NAME, TEST_FAM);
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
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      UTIL.loadTable(table, TEST_FAM);
    }

    LOG.debug("FS state before snapshot:");
    UTIL.getHBaseCluster().getMaster().getMasterFileSystem().logFileSystemState(LOG);

    // take a snapshot of the enabled table
    String snapshotString = "skipFlushTableSnapshot";
    byte[] snapshot = Bytes.toBytes(snapshotString);
    admin.snapshot(snapshotString, TABLE_NAME, SnapshotDescription.Type.SKIPFLUSH);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    List<SnapshotDescription> snapshots = SnapshotTestingUtils.assertOneSnapshotThatMatches(admin,
        snapshot, TABLE_NAME);

    // make sure its a valid snapshot
    LOG.debug("FS state after snapshot:");
    UTIL.getHBaseCluster().getMaster().getMasterFileSystem().logFileSystemState(LOG);

    SnapshotTestingUtils.confirmSnapshotValid(UTIL, snapshots.get(0), TABLE_NAME, TEST_FAM);

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
    SnapshotTestingUtils.loadData(UTIL, TABLE_NAME, DEFAULT_NUM_ROWS, TEST_FAM);

    LOG.debug("FS state before snapshot:");
    UTIL.getHBaseCluster().getMaster().getMasterFileSystem().logFileSystemState(LOG);

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
    LOG.debug("FS state after snapshot:");
    UTIL.getHBaseCluster().getMaster().getMasterFileSystem().logFileSystemState(LOG);

    SnapshotTestingUtils.confirmSnapshotValid(UTIL, snapshots.get(0), TABLE_NAME, TEST_FAM);
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
    UTIL.getHBaseCluster().getMaster().getMasterFileSystem().logFileSystemState(LOG);

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

    SnapshotTestingUtils.verifyRowCount(UTIL, TABLE_NAME, numRows);
    SnapshotTestingUtils.verifyRowCount(UTIL, cloneBeforeMergeName, numRows);
    SnapshotTestingUtils.verifyRowCount(UTIL, cloneAfterMergeName, numRows);

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

    SnapshotTestingUtils.verifyRowCount(UTIL, TABLE_NAME, numRows);
    SnapshotTestingUtils.verifyRowCount(UTIL, cloneName, numRows);

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
