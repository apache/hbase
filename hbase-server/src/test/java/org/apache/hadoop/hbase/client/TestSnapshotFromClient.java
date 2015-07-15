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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifestV1;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * Test create/using/deleting snapshots from the client
 * <p>
 * This is an end-to-end test for the snapshot utility
 */
@Category(LargeTests.class)
public class TestSnapshotFromClient {
  private static final Log LOG = LogFactory.getLog(TestSnapshotFromClient.class);
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "test";
  protected static final byte[] TEST_FAM = Bytes.toBytes("fam");
  protected static final TableName TABLE_NAME =
      TableName.valueOf(STRING_TABLE_NAME);

  /**
   * Setup the config for the cluster
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
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
  }

  @Before
  public void setup() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    htd.setRegionReplication(getNumReplicas());
    UTIL.createTable(htd, new byte[][]{TEST_FAM}, UTIL.getConfiguration());
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
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  /**
   * Test snapshotting not allowed hbase:meta and -ROOT-
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testMetaTablesSnapshot() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    byte[] snapshotName = Bytes.toBytes("metaSnapshot");

    try {
      admin.snapshot(snapshotName, TableName.META_TABLE_NAME);
      fail("taking a snapshot of hbase:meta should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * Test HBaseAdmin#deleteSnapshots(String) which deletes snapshots whose names match the parameter
   *
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testSnapshotDeletionWithRegex() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // put some stuff in the table
    HTable table = new HTable(UTIL.getConfiguration(), TABLE_NAME);
    UTIL.loadTable(table, TEST_FAM);
    table.close();

    byte[] snapshot1 = Bytes.toBytes("TableSnapshot1");
    admin.snapshot(snapshot1, TABLE_NAME);
    LOG.debug("Snapshot1 completed.");

    byte[] snapshot2 = Bytes.toBytes("TableSnapshot2");
    admin.snapshot(snapshot2, TABLE_NAME);
    LOG.debug("Snapshot2 completed.");

    String snapshot3 = "3rdTableSnapshot";
    admin.snapshot(Bytes.toBytes(snapshot3), TABLE_NAME);
    LOG.debug(snapshot3 + " completed.");

    // delete the first two snapshots
    admin.deleteSnapshots("TableSnapshot.*");
    List<SnapshotDescription> snapshots = admin.listSnapshots();
    assertEquals(1, snapshots.size());
    assertEquals(snapshots.get(0).getName(), snapshot3);

    admin.deleteSnapshot(snapshot3);
    admin.close();
  }
  /**
   * Test snapshotting a table that is offline
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testOfflineTableSnapshot() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // put some stuff in the table
    HTable table = new HTable(UTIL.getConfiguration(), TABLE_NAME);
    UTIL.loadTable(table, TEST_FAM, false);

    LOG.debug("FS state before disable:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);
    // XXX if this is flakey, might want to consider using the async version and looping as
    // disableTable can succeed and still timeout.
    admin.disableTable(TABLE_NAME);

    LOG.debug("FS state before snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the disabled table
    final String SNAPSHOT_NAME = "offlineTableSnapshot";
    byte[] snapshot = Bytes.toBytes(SNAPSHOT_NAME);

    SnapshotDescription desc = SnapshotDescription.newBuilder()
      .setType(SnapshotDescription.Type.DISABLED)
      .setTable(STRING_TABLE_NAME)
      .setName(SNAPSHOT_NAME)
      .setVersion(SnapshotManifestV1.DESCRIPTOR_VERSION)
      .build();
    admin.snapshot(desc);
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

  @Test (timeout=300000)
  public void testSnapshotFailsOnNonExistantTable() throws Exception {
    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);
    String tableName = "_not_a_table";

    // make sure the table doesn't exist
    boolean fail = false;
    do {
    try {
      admin.getTableDescriptor(TableName.valueOf(tableName));
      fail = true;
          LOG.error("Table:" + tableName + " already exists, checking a new name");
      tableName = tableName+"!";
    } catch (TableNotFoundException e) {
      fail = false;
      }
    } while (fail);

    // snapshot the non-existant table
    try {
      admin.snapshot("fail", TableName.valueOf(tableName));
      fail("Snapshot succeeded even though there is not table.");
    } catch (SnapshotCreationException e) {
      LOG.info("Correctly failed to snapshot a non-existant table:" + e.getMessage());
    }
  }

  @Test (timeout=300000)
  public void testOfflineTableSnapshotWithEmptyRegions() throws Exception {
    // test with an empty table with one region

    Admin admin = UTIL.getHBaseAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    LOG.debug("FS state before disable:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);
    admin.disableTable(TABLE_NAME);

    LOG.debug("FS state before snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the disabled table
    byte[] snapshot = Bytes.toBytes("testOfflineTableSnapshotWithEmptyRegions");
    admin.snapshot(snapshot, TABLE_NAME);
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

    List<byte[]> emptyCfs = Lists.newArrayList(TEST_FAM); // no file in the region
    List<byte[]> nonEmptyCfs = Lists.newArrayList();
    SnapshotTestingUtils.confirmSnapshotValid(snapshots.get(0), TABLE_NAME, nonEmptyCfs, emptyCfs,
      rootDir, admin, fs);

    admin.deleteSnapshot(snapshot);
    snapshots = admin.listSnapshots();
    SnapshotTestingUtils.assertNoSnapshots(admin);
  }

  @Test(timeout = 300000)
  public void testListTableSnapshots() throws Exception {
    Admin admin = null;
    TableName tableName2 = TableName.valueOf("testListTableSnapshots");
    try {
      admin = UTIL.getHBaseAdmin();

      HTableDescriptor htd = new HTableDescriptor(tableName2);
      UTIL.createTable(htd, new byte[][] { TEST_FAM }, UTIL.getConfiguration());

      String table1Snapshot1 = "Table1Snapshot1";
      admin.snapshot(table1Snapshot1, TABLE_NAME);
      LOG.debug("Snapshot1 completed.");

      String table1Snapshot2 = "Table1Snapshot2";
      admin.snapshot(table1Snapshot2, TABLE_NAME);
      LOG.debug("Snapshot2 completed.");

      String table2Snapshot1 = "Table2Snapshot1";
      admin.snapshot(Bytes.toBytes(table2Snapshot1), tableName2);
      LOG.debug(table2Snapshot1 + " completed.");

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

  @Test(timeout = 300000)
  public void testListTableSnapshotsWithRegex() throws Exception {
    Admin admin = null;
    try {
      admin = UTIL.getHBaseAdmin();

      String table1Snapshot1 = "Table1Snapshot1";
      admin.snapshot(table1Snapshot1, TABLE_NAME);
      LOG.debug("Snapshot1 completed.");

      String table1Snapshot2 = "Table1Snapshot2";
      admin.snapshot(table1Snapshot2, TABLE_NAME);
      LOG.debug("Snapshot2 completed.");

      String table2Snapshot1 = "Table2Snapshot1";
      admin.snapshot(Bytes.toBytes(table2Snapshot1), TABLE_NAME);
      LOG.debug(table2Snapshot1 + " completed.");

      List<SnapshotDescription> listTableSnapshots = admin.listTableSnapshots("test.*", "Table1.*");
      List<String> listTableSnapshotNames = new ArrayList<String>();
      assertEquals(2, listTableSnapshots.size());
      for (SnapshotDescription s : listTableSnapshots) {
        listTableSnapshotNames.add(s.getName());
      }
      assertTrue(listTableSnapshotNames.contains(table1Snapshot1));
      assertTrue(listTableSnapshotNames.contains(table1Snapshot2));
      assertFalse(listTableSnapshotNames.contains(table2Snapshot1));
    } finally {
      if (admin != null) {
        try {
          admin.deleteSnapshots("Table.*");
        } catch (SnapshotDoesNotExistException ignore) {
        }
        admin.close();
      }
    }
  }

  @Test(timeout = 300000)
  public void testDeleteTableSnapshots() throws Exception {
    Admin admin = null;
    TableName tableName2 = TableName.valueOf("testListTableSnapshots");
    try {
      admin = UTIL.getHBaseAdmin();

      HTableDescriptor htd = new HTableDescriptor(tableName2);
      UTIL.createTable(htd, new byte[][] { TEST_FAM }, UTIL.getConfiguration());

      String table1Snapshot1 = "Table1Snapshot1";
      admin.snapshot(table1Snapshot1, TABLE_NAME);
      LOG.debug("Snapshot1 completed.");

      String table1Snapshot2 = "Table1Snapshot2";
      admin.snapshot(table1Snapshot2, TABLE_NAME);
      LOG.debug("Snapshot2 completed.");

      String table2Snapshot1 = "Table2Snapshot1";
      admin.snapshot(Bytes.toBytes(table2Snapshot1), tableName2);
      LOG.debug(table2Snapshot1 + " completed.");

      admin.deleteTableSnapshots("test.*", ".*");
      assertEquals(0, admin.listTableSnapshots("test.*", ".*").size());
    } finally {
      if (admin != null) {
        if (admin.tableExists(tableName2)) {
          UTIL.deleteTable(tableName2);
        }
        admin.close();
      }
    }
  }

  @Test(timeout = 300000)
  public void testDeleteTableSnapshotsWithRegex() throws Exception {
    Admin admin = null;
    try {
      admin = UTIL.getHBaseAdmin();

      String table1Snapshot1 = "Table1Snapshot1";
      admin.snapshot(table1Snapshot1, TABLE_NAME);
      LOG.debug("Snapshot1 completed.");

      String table1Snapshot2 = "Table1Snapshot2";
      admin.snapshot(table1Snapshot2, TABLE_NAME);
      LOG.debug("Snapshot2 completed.");

      String table2Snapshot1 = "Table2Snapshot1";
      admin.snapshot(Bytes.toBytes(table2Snapshot1), TABLE_NAME);
      LOG.debug(table2Snapshot1 + " completed.");

      admin.deleteTableSnapshots("test.*", "Table1.*");
      assertEquals(1, admin.listTableSnapshots("test.*", ".*").size());
    } finally {
      if (admin != null) {
        try {
          admin.deleteTableSnapshots("test.*", ".*");
        } catch (SnapshotDoesNotExistException ignore) {
        }
        admin.close();
      }
    }
  }
}
