/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.SnapshotManifestV1;
import org.apache.hadoop.hbase.snapshot.SnapshotManifestV2;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * This class tests that the use of a temporary snapshot directory supports snapshot functionality
 * while the temporary directory is on a different file system than the root directory
 * <p>
 * This is an end-to-end test for the snapshot utility
 */
@Category(LargeTests.class)
@RunWith(Parameterized.class)
public class TestSnapshotTemporaryDirectory {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotTemporaryDirectory.class);

  @Parameterized.Parameters public static Iterable<Integer> data() {
    return Arrays
        .asList(SnapshotManifestV1.DESCRIPTOR_VERSION, SnapshotManifestV2.DESCRIPTOR_VERSION);
  }

  @Parameterized.Parameter public int manifestVersion;

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotTemporaryDirectory.class);
  protected static final int NUM_RS = 2;
  protected static String TEMP_DIR =
      Paths.get("").toAbsolutePath().toString() + Path.SEPARATOR + UUID.randomUUID().toString();

  protected static Admin admin;
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  protected static final String STRING_TABLE_NAME = "test";
  protected static final byte[] TEST_FAM = Bytes.toBytes("fam");
  protected static final TableName TABLE_NAME = TableName.valueOf(STRING_TABLE_NAME);

  /**
   * Setup the config for the cluster
   *
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
    admin = UTIL.getHBaseAdmin();
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
    conf.set(SnapshotDescriptionUtils.SNAPSHOT_WORKING_DIR, "file://" + new Path(TEMP_DIR, ".tmpDir").toUri());
  }

  @Before
  public void setup() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    htd.setRegionReplication(getNumReplicas());
    UTIL.createTable(htd, new byte[][] { TEST_FAM }, UTIL.getConfiguration());
  }

  protected int getNumReplicas() {
    return 1;
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(TABLE_NAME);
    SnapshotTestingUtils.deleteAllSnapshots(UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
  }

  @AfterClass
  public static void cleanupTest() {
    try {
      UTIL.shutdownMiniCluster();
      FileUtils.deleteDirectory(new File(TEMP_DIR));
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test
  public void testRestoreDisabledSnapshot()
      throws IOException, InterruptedException {
    long tid = System.currentTimeMillis();
    TableName tableName = TableName.valueOf("testtb-" + tid);
    byte[] emptySnapshot = Bytes.toBytes("emptySnaptb-" + tid);
    byte[] snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    byte[] snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    int snapshot0Rows;
    int snapshot1Rows;

    // create Table and disable it
    SnapshotTestingUtils.createTable(UTIL, tableName, getNumReplicas(), TEST_FAM);
    admin.disableTable(tableName);

    // take an empty snapshot
    takeSnapshot(tableName, Bytes.toString(emptySnapshot), true);

    // enable table and insert data
    admin.enableTable(tableName);
    SnapshotTestingUtils.loadData(UTIL, tableName, 500, TEST_FAM);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      snapshot0Rows = UTIL.countRows(table);
    }
    admin.disableTable(tableName);

    // take a snapshot
    takeSnapshot(tableName, Bytes.toString(snapshotName0), true);

    // enable table and insert more data
    admin.enableTable(tableName);
    SnapshotTestingUtils.loadData(UTIL, tableName, 500, TEST_FAM);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      snapshot1Rows = UTIL.countRows(table);
    }

    SnapshotTestingUtils.verifyRowCount(UTIL, tableName, snapshot1Rows);
    admin.disableTable(tableName);
    takeSnapshot(tableName, Bytes.toString(snapshotName1), true);

    // Restore from snapshot-0
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    SnapshotTestingUtils.verifyRowCount(UTIL, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from emptySnapshot
    admin.disableTable(tableName);
    admin.restoreSnapshot(emptySnapshot);
    admin.enableTable(tableName);
    SnapshotTestingUtils.verifyRowCount(UTIL, tableName, 0);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from snapshot-1
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);
    SnapshotTestingUtils.verifyRowCount(UTIL, tableName, snapshot1Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from snapshot-1
    UTIL.deleteTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    SnapshotTestingUtils.verifyRowCount(UTIL, tableName, snapshot1Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
  }

  @Test
  public void testRestoreEnabledSnapshot()
      throws IOException, InterruptedException {
    long tid = System.currentTimeMillis();
    TableName tableName = TableName.valueOf("testtb-" + tid);
    byte[] emptySnapshot = Bytes.toBytes("emptySnaptb-" + tid);
    byte[] snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    byte[] snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    int snapshot0Rows;
    int snapshot1Rows;

    // create Table
    SnapshotTestingUtils.createTable(UTIL, tableName, getNumReplicas(), TEST_FAM);

    // take an empty snapshot
    takeSnapshot(tableName, Bytes.toString(emptySnapshot), false);

    // Insert data
    SnapshotTestingUtils.loadData(UTIL, tableName, 500, TEST_FAM);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      snapshot0Rows = UTIL.countRows(table);
    }

    // take a snapshot
    takeSnapshot(tableName, Bytes.toString(snapshotName0), false);

    // Insert more data
    SnapshotTestingUtils.loadData(UTIL, tableName, 500, TEST_FAM);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      snapshot1Rows = UTIL.countRows(table);
    }

    SnapshotTestingUtils.verifyRowCount(UTIL, tableName, snapshot1Rows);
    takeSnapshot(tableName, Bytes.toString(snapshotName1), false);

    // Restore from snapshot-0
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    SnapshotTestingUtils.verifyRowCount(UTIL, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from emptySnapshot
    admin.disableTable(tableName);
    admin.restoreSnapshot(emptySnapshot);
    admin.enableTable(tableName);
    SnapshotTestingUtils.verifyRowCount(UTIL, tableName, 0);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from snapshot-1
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);
    SnapshotTestingUtils.verifyRowCount(UTIL, tableName, snapshot1Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from snapshot-1
    UTIL.deleteTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    SnapshotTestingUtils.verifyRowCount(UTIL, tableName, snapshot1Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
  }

  /**
   * Test snapshotting a table that is offline
   *
   * @throws Exception if snapshot does not complete successfully
   */
  @Test
  public void testOfflineTableSnapshot() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // put some stuff in the table
    Table table = UTIL.getConnection().getTable(TABLE_NAME);
    UTIL.loadTable(table, TEST_FAM, false);

    LOG.debug("FS state before disable:");
    CommonFSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      CommonFSUtils.getRootDir(UTIL.getConfiguration()), LOG);
    // XXX if this is flakey, might want to consider using the async version and looping as
    // disableTable can succeed and still timeout.
    admin.disableTable(TABLE_NAME);

    LOG.debug("FS state before snapshot:");
    CommonFSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      CommonFSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the disabled table
    final String SNAPSHOT_NAME = "offlineTableSnapshot";
    byte[] snapshot = Bytes.toBytes(SNAPSHOT_NAME);
    takeSnapshot(TABLE_NAME, SNAPSHOT_NAME, true);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    List<SnapshotDescription> snapshots =
        SnapshotTestingUtils.assertOneSnapshotThatMatches(admin, snapshot, TABLE_NAME);

    // make sure its a valid snapshot
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    LOG.debug("FS state after snapshot:");
    CommonFSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      CommonFSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    SnapshotTestingUtils
        .confirmSnapshotValid(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshots.get(0)),
            TABLE_NAME, TEST_FAM, rootDir, admin, fs);

    admin.deleteSnapshot(snapshot);
    SnapshotTestingUtils.assertNoSnapshots(admin);
  }

  /**
   * Tests that snapshot has correct contents by taking snapshot, cloning it, then affirming
   * the contents of the original and cloned table match
   *
   * @throws Exception if snapshot does not complete successfully
   */
  @Test
  public void testSnapshotCloneContents() throws Exception {
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // put some stuff in the table
    Table table = UTIL.getConnection().getTable(TABLE_NAME);
    UTIL.loadTable(table, TEST_FAM);
    table.close();

    String snapshot1 = "TableSnapshot1";
    takeSnapshot(TABLE_NAME, snapshot1, false);
    LOG.debug("Snapshot1 completed.");

    TableName clone = TableName.valueOf("Table1Clone");
    admin.cloneSnapshot(snapshot1, clone, false);

    Scan original = new Scan();
    Scan cloned = new Scan();
    ResultScanner originalScan = admin.getConnection().getTable(TABLE_NAME).getScanner(original);
    ResultScanner clonedScan =
        admin.getConnection().getTable(TableName.valueOf("Table1Clone")).getScanner(cloned);

    Iterator<Result> i = originalScan.iterator();
    Iterator<Result> i2 = clonedScan.iterator();
    assertTrue(i.hasNext());
    while (i.hasNext()) {
      assertTrue(i2.hasNext());
      assertEquals(Bytes.toString(i.next().getValue(TEST_FAM, new byte[] {})),
          Bytes.toString(i2.next().getValue(TEST_FAM, new byte[] {})));
    }
    assertFalse(i2.hasNext());
    admin.deleteSnapshot(snapshot1);
    UTIL.deleteTable(clone);
    admin.close();
  }

  @Test
  public void testOfflineTableSnapshotWithEmptyRegion() throws Exception {
    // test with an empty table with one region

    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    LOG.debug("FS state before disable:");
    CommonFSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      CommonFSUtils.getRootDir(UTIL.getConfiguration()), LOG);
    admin.disableTable(TABLE_NAME);

    LOG.debug("FS state before snapshot:");
    CommonFSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      CommonFSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the disabled table
    byte[] snapshot = Bytes.toBytes("testOfflineTableSnapshotWithEmptyRegion");
    takeSnapshot(TABLE_NAME, Bytes.toString(snapshot), true);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    List<SnapshotDescription> snapshots =
      SnapshotTestingUtils.assertOneSnapshotThatMatches(admin, snapshot, TABLE_NAME);

    // make sure its a valid snapshot
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    LOG.debug("FS state after snapshot:");
    CommonFSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      CommonFSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    List<byte[]> emptyCfs = Lists.newArrayList(TEST_FAM); // no file in the region
    List<byte[]> nonEmptyCfs = Lists.newArrayList();
    SnapshotTestingUtils
        .confirmSnapshotValid(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshots.get(0)),
            TABLE_NAME, nonEmptyCfs, emptyCfs, rootDir, admin, fs);

    admin.deleteSnapshot(snapshot);
    SnapshotTestingUtils.assertNoSnapshots(admin);
  }

  // Ensures that the snapshot is transferred to the proper completed snapshot directory
  @Test
  public void testEnsureTemporaryDirectoryTransfer() throws Exception {
    Admin admin = null;
    TableName tableName2 = TableName.valueOf("testListTableSnapshots");
    try {
      admin = UTIL.getHBaseAdmin();

      HTableDescriptor htd = new HTableDescriptor(tableName2);
      UTIL.createTable(htd, new byte[][] { TEST_FAM }, UTIL.getConfiguration());

      String table1Snapshot1 = "Table1Snapshot1";
      takeSnapshot(TABLE_NAME, table1Snapshot1, false);
      LOG.debug("Snapshot1 completed.");

      String table1Snapshot2 = "Table1Snapshot2";
      takeSnapshot(TABLE_NAME, table1Snapshot2, false);
      LOG.debug("Snapshot2 completed.");

      String table2Snapshot1 = "Table2Snapshot1";
      takeSnapshot(TABLE_NAME, table2Snapshot1, false);
      LOG.debug("Table2Snapshot1 completed.");

      List<SnapshotDescription> listTableSnapshots = admin.listTableSnapshots("test.*", ".*");
      List<String> listTableSnapshotNames = new ArrayList<String>();
      assertEquals(3, listTableSnapshots.size());
      for (SnapshotDescription s : listTableSnapshots) {
        listTableSnapshotNames.add(s.getName());
      }
      assertTrue(listTableSnapshotNames.contains(table1Snapshot1));
      assertTrue(listTableSnapshotNames.contains(table1Snapshot2));
      assertTrue(listTableSnapshotNames.contains(table2Snapshot1));
    } finally {
      if (admin != null) {
        try {
          admin.deleteSnapshots("Table.*");
        } catch (SnapshotDoesNotExistException ignore) {
        }
        if (admin.tableExists(tableName2)) {
          UTIL.deleteTable(tableName2);
        }
        admin.close();
      }
    }
  }

  private void takeSnapshot(TableName tableName, String snapshotName, boolean disabled)
      throws IOException {
    SnapshotType type = disabled ? SnapshotType.DISABLED : SnapshotType.FLUSH;
    SnapshotDescription desc = new SnapshotDescription(snapshotName, tableName, type, null, -1,
        manifestVersion, null);
    admin.snapshot(desc);
  }
}
