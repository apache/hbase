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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.fs.RegionStorage.StoreFileVisitor;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

/**
 * Test restore snapshots from the client
 */
@Category({LargeTests.class, ClientTests.class})
public class TestRestoreSnapshotFromClient {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder()
      .withTimeout(this.getClass())
      .withLookingForStuckThread(true)
      .build();

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected final byte[] FAMILY = Bytes.toBytes("cf");
  protected final byte[] TEST_FAMILY2 = Bytes.toBytes("cf2");

  protected TableName tableName;
  private byte[] emptySnapshot;
  private byte[] snapshotName0;
  private byte[] snapshotName1;
  private byte[] snapshotName2;
  private int snapshot0Rows;
  private int snapshot1Rows;
  private Admin admin;

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(3);
  }

  protected static void setupConf(Configuration conf) {
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean(
        "hbase.master.enabletable.roundrobin", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Initialize the tests with a table filled with some data
   * and two snapshots (snapshotName0, snapshotName1) of different states.
   * The tableName, snapshotNames and the number of rows in the snapshot are initialized.
   */
  @Before
  public void setup() throws Exception {
    this.admin = TEST_UTIL.getHBaseAdmin();

    long tid = System.currentTimeMillis();
    tableName =
        TableName.valueOf("testtb-" + tid);
    emptySnapshot = Bytes.toBytes("emptySnaptb-" + tid);
    snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    snapshotName2 = Bytes.toBytes("snaptb2-" + tid);

    // create Table and disable it
    createTable();
    admin.disableTable(tableName);

    // take an empty snapshot
    admin.snapshot(emptySnapshot, tableName);

    // enable table and insert data
    admin.enableTable(tableName);
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, FAMILY);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      snapshot0Rows = countRows(table);
    }
    admin.disableTable(tableName);

    // take a snapshot
    admin.snapshot(snapshotName0, tableName);

    // enable table and insert more data
    admin.enableTable(tableName);
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, FAMILY);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      snapshot1Rows = countRows(table);
    }
  }

  protected void createTable() throws Exception {
    SnapshotTestingUtils.createTable(TEST_UTIL, tableName, getNumReplicas(), FAMILY);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.deleteTable(tableName);
    SnapshotTestingUtils.deleteAllSnapshots(TEST_UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
  }

  @Test
  public void testRestoreSnapshot() throws IOException {
    verifyRowCount(TEST_UTIL, tableName, snapshot1Rows);
    admin.disableTable(tableName);
    admin.snapshot(snapshotName1, tableName);
    // Restore from snapshot-0
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    verifyRowCount(TEST_UTIL, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from emptySnapshot
    admin.disableTable(tableName);
    admin.restoreSnapshot(emptySnapshot);
    admin.enableTable(tableName);
    verifyRowCount(TEST_UTIL, tableName, 0);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from snapshot-1
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);
    verifyRowCount(TEST_UTIL, tableName, snapshot1Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());

    // Restore from snapshot-1
    TEST_UTIL.deleteTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    verifyRowCount(TEST_UTIL, tableName, snapshot1Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
  }

  protected int getNumReplicas() {
    return 1;
  }

  protected HColumnDescriptor getTestRestoreSchemaChangeHCD() {
    return new HColumnDescriptor(TEST_FAMILY2);
  }

  @Test
  public void testRestoreSchemaChange() throws Exception {
    Table table = TEST_UTIL.getConnection().getTable(tableName);

    // Add one column family and put some data in it
    admin.disableTable(tableName);
    admin.addColumnFamily(tableName, getTestRestoreSchemaChangeHCD());
    admin.enableTable(tableName);
    assertEquals(2, table.getTableDescriptor().getFamilies().size());
    HTableDescriptor htd = admin.getTableDescriptor(tableName);
    assertEquals(2, htd.getFamilies().size());
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, TEST_FAMILY2);
    long snapshot2Rows = snapshot1Rows + 500;
    assertEquals(snapshot2Rows, countRows(table));
    assertEquals(500, countRows(table, TEST_FAMILY2));
    Set<String> fsFamilies = getFamiliesFromFS(tableName);
    assertEquals(2, fsFamilies.size());

    // Take a snapshot
    admin.disableTable(tableName);
    admin.snapshot(snapshotName2, tableName);

    // Restore the snapshot (without the cf)
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    assertEquals(1, table.getTableDescriptor().getFamilies().size());
    try {
      countRows(table, TEST_FAMILY2);
      fail("family '" + Bytes.toString(TEST_FAMILY2) + "' should not exists");
    } catch (NoSuchColumnFamilyException e) {
      // expected
    }
    assertEquals(snapshot0Rows, countRows(table));
    htd = admin.getTableDescriptor(tableName);
    assertEquals(1, htd.getFamilies().size());
    fsFamilies = getFamiliesFromFS(tableName);
    assertEquals(1, fsFamilies.size());

    // Restore back the snapshot (with the cf)
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName2);
    admin.enableTable(tableName);
    htd = admin.getTableDescriptor(tableName);
    assertEquals(2, htd.getFamilies().size());
    assertEquals(2, table.getTableDescriptor().getFamilies().size());
    assertEquals(500, countRows(table, TEST_FAMILY2));
    assertEquals(snapshot2Rows, countRows(table));
    fsFamilies = getFamiliesFromFS(tableName);
    assertEquals(2, fsFamilies.size());
    table.close();
  }

  @Test
  public void testCloneSnapshotOfCloned() throws IOException, InterruptedException {
    TableName clonedTableName =
        TableName.valueOf("clonedtb-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    verifyRowCount(TEST_UTIL, clonedTableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(clonedTableName, admin, getNumReplicas());
    admin.disableTable(clonedTableName);
    admin.snapshot(snapshotName2, clonedTableName);
    TEST_UTIL.deleteTable(clonedTableName);
    waitCleanerRun();

    admin.cloneSnapshot(snapshotName2, clonedTableName);
    verifyRowCount(TEST_UTIL, clonedTableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(clonedTableName, admin, getNumReplicas());
    TEST_UTIL.deleteTable(clonedTableName);
  }

  @Test
  public void testCloneAndRestoreSnapshot() throws IOException, InterruptedException {
    TEST_UTIL.deleteTable(tableName);
    waitCleanerRun();

    admin.cloneSnapshot(snapshotName0, tableName);
    verifyRowCount(TEST_UTIL, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
    waitCleanerRun();

    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName0);
    admin.enableTable(tableName);
    verifyRowCount(TEST_UTIL, tableName, snapshot0Rows);
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
  }

  @Test
  public void testCorruptedSnapshot() throws IOException, InterruptedException {
    SnapshotTestingUtils.corruptSnapshot(TEST_UTIL, Bytes.toString(snapshotName0));
    TableName cloneName = TableName.valueOf("corruptedClone-" + System.currentTimeMillis());
    try {
      admin.cloneSnapshot(snapshotName0, cloneName);
      fail("Expected CorruptedSnapshotException, got succeeded cloneSnapshot()");
    } catch (CorruptedSnapshotException e) {
      // Got the expected corruption exception.
      // check for no references of the cloned table.
      assertFalse(admin.tableExists(cloneName));
    } catch (Exception e) {
      fail("Expected CorruptedSnapshotException got: " + e);
    }
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================
  private void waitCleanerRun() throws InterruptedException {
    TEST_UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
  }

  private Set<String> getFamiliesFromFS(final TableName tableName) throws IOException {
    MasterStorage mfs = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterStorage();
    final Set<String> families = new HashSet<String>();
    mfs.visitStoreFiles(tableName, new StoreFileVisitor() {
      @Override
      public void storeFile(HRegionInfo region, String family, StoreFileInfo storeFile)
          throws IOException {
        families.add(family);
      }
    });
    return families;
  }

  protected void verifyRowCount(final HBaseTestingUtility util, final TableName tableName,
      long expectedRows) throws IOException {
    SnapshotTestingUtils.verifyRowCount(util, tableName, expectedRows);
  }

  protected int countRows(final Table table, final byte[]... families) throws IOException {
    return TEST_UTIL.countRows(table, families);
  }
}
