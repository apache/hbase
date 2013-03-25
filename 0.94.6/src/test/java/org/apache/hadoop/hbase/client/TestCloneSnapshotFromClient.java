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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test clone snapshots from the client
 */
@Category(LargeTests.class)
public class TestCloneSnapshotFromClient {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final byte[] FAMILY = Bytes.toBytes("cf");

  private byte[] emptySnapshot;
  private byte[] snapshotName0;
  private byte[] snapshotName1;
  private byte[] snapshotName2;
  private int snapshot0Rows;
  private int snapshot1Rows;
  private byte[] tableName;
  private HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    TEST_UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean(
        "hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(3);
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
    tableName = Bytes.toBytes("testtb-" + tid);
    emptySnapshot = Bytes.toBytes("emptySnaptb-" + tid);
    snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    snapshotName2 = Bytes.toBytes("snaptb2-" + tid);

    // create Table and disable it
    createTable(tableName, FAMILY);
    admin.disableTable(tableName);

    // take an empty snapshot
    admin.snapshot(emptySnapshot, tableName);

    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    try {
      // enable table and insert data
      admin.enableTable(tableName);
      loadData(table, 500, FAMILY);
      snapshot0Rows = TEST_UTIL.countRows(table);
      admin.disableTable(tableName);

      // take a snapshot
      admin.snapshot(snapshotName0, tableName);

      // enable table and insert more data
      admin.enableTable(tableName);
      loadData(table, 500, FAMILY);
      snapshot1Rows = TEST_UTIL.countRows(table);
      admin.disableTable(tableName);

      // take a snapshot of the updated table
      admin.snapshot(snapshotName1, tableName);

      // re-enable table
      admin.enableTable(tableName);
    } finally {
      table.close();
    }
  }

  @After
  public void tearDown() throws Exception {
    if (admin.tableExists(tableName)) {
      TEST_UTIL.deleteTable(tableName);
    }
    admin.deleteSnapshot(snapshotName0);
    admin.deleteSnapshot(snapshotName1);

    // Ensure the archiver to be empty
    MasterFileSystem mfs = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    mfs.getFileSystem().delete(
      new Path(mfs.getRootDir(), HConstants.HFILE_ARCHIVE_DIRECTORY), true);
  }

  @Test(expected=SnapshotDoesNotExistException.class)
  public void testCloneNonExistentSnapshot() throws IOException, InterruptedException {
    String snapshotName = "random-snapshot-" + System.currentTimeMillis();
    String tableName = "random-table-" + System.currentTimeMillis();
    admin.cloneSnapshot(snapshotName, tableName);
  }

  @Test
  public void testCloneSnapshot() throws IOException, InterruptedException {
    byte[] clonedTableName = Bytes.toBytes("clonedtb-" + System.currentTimeMillis());
    testCloneSnapshot(clonedTableName, snapshotName0, snapshot0Rows);
    testCloneSnapshot(clonedTableName, snapshotName1, snapshot1Rows);
    testCloneSnapshot(clonedTableName, emptySnapshot, 0);
  }

  private void testCloneSnapshot(final byte[] tableName, final byte[] snapshotName,
      int snapshotRows) throws IOException, InterruptedException {
    // create a new table from snapshot
    admin.cloneSnapshot(snapshotName, tableName);
    verifyRowCount(tableName, snapshotRows);

    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  /**
   * Verify that tables created from the snapshot are still alive after source table deletion.
   */
  @Test
  public void testCloneLinksAfterDelete() throws IOException, InterruptedException {
    // Clone a table from the first snapshot
    byte[] clonedTableName = Bytes.toBytes("clonedtb1-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    verifyRowCount(clonedTableName, snapshot0Rows);

    // Take a snapshot of this cloned table.
    admin.disableTable(clonedTableName);
    admin.snapshot(snapshotName2, clonedTableName);

    // Clone the snapshot of the cloned table
    byte[] clonedTableName2 = Bytes.toBytes("clonedtb2-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName2, clonedTableName2);
    verifyRowCount(clonedTableName2, snapshot0Rows);
    admin.disableTable(clonedTableName2);

    // Remove the original table
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    waitCleanerRun();

    // Verify the first cloned table
    admin.enableTable(clonedTableName);
    verifyRowCount(clonedTableName, snapshot0Rows);

    // Verify the second cloned table
    admin.enableTable(clonedTableName2);
    verifyRowCount(clonedTableName2, snapshot0Rows);
    admin.disableTable(clonedTableName2);

    // Delete the first cloned table
    admin.disableTable(clonedTableName);
    admin.deleteTable(clonedTableName);
    waitCleanerRun();

    // Verify the second cloned table
    admin.enableTable(clonedTableName2);
    verifyRowCount(clonedTableName2, snapshot0Rows);

    // Clone a new table from cloned
    byte[] clonedTableName3 = Bytes.toBytes("clonedtb3-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName2, clonedTableName3);
    verifyRowCount(clonedTableName3, snapshot0Rows);

    // Delete the cloned tables
    admin.disableTable(clonedTableName2);
    admin.deleteTable(clonedTableName2);
    admin.disableTable(clonedTableName3);
    admin.deleteTable(clonedTableName3);
    admin.deleteSnapshot(snapshotName2);
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================
  private void createTable(final byte[] tableName, final byte[]... families) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte[] family: families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);
    }
    byte[][] splitKeys = new byte[16][];
    byte[] hex = Bytes.toBytes("0123456789abcdef");
    for (int i = 0; i < 16; ++i) {
      splitKeys[i] = new byte[] { hex[i] };
    }
    admin.createTable(htd, splitKeys);
  }

  public void loadData(final HTable table, int rows, byte[]... families) throws IOException {
    byte[] qualifier = Bytes.toBytes("q");
    table.setAutoFlush(false);
    while (rows-- > 0) {
      byte[] value = Bytes.add(Bytes.toBytes(System.currentTimeMillis()), Bytes.toBytes(rows));
      byte[] key = Bytes.toBytes(MD5Hash.getMD5AsHex(value));
      Put put = new Put(key);
      put.setWriteToWAL(false);
      for (byte[] family: families) {
        put.add(family, qualifier, value);
      }
      table.put(put);
    }
    table.flushCommits();
  }

  private void waitCleanerRun() throws InterruptedException {
    TEST_UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
  }

  private void verifyRowCount(final byte[] tableName, long expectedRows) throws IOException {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    assertEquals(expectedRows, TEST_UTIL.countRows(table));
    table.close();
  }
}
