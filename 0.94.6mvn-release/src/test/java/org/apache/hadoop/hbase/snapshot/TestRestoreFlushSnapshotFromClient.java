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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test clone/restore snapshots from the client
 *
 * TODO This is essentially a clone of TestRestoreSnapshotFromClient.  This is worth refactoring
 * this because there will be a few more flavors of snapshots that need to run these tests.
 */
@Category(LargeTests.class)
public class TestRestoreFlushSnapshotFromClient {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final byte[] FAMILY = Bytes.toBytes("cf");

  private byte[] snapshotName0;
  private byte[] snapshotName1;
  private byte[] snapshotName2;
  private int snapshot0Rows;
  private int snapshot1Rows;
  private byte[] tableName;
  private HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean(
        "hbase.master.enabletable.roundrobin", true);

    // Enable snapshot
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);

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
    snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    snapshotName2 = Bytes.toBytes("snaptb2-" + tid);

    // create Table and disable it
    createTable(tableName, FAMILY);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    try {
      loadData(table, 500, FAMILY);
      snapshot0Rows = TEST_UTIL.countRows(table);
      LOG.info("=== before snapshot with 500 rows");
      logFSTree();

      // take a snapshot
      admin.snapshot(Bytes.toString(snapshotName0), Bytes.toString(tableName),
          SnapshotDescription.Type.FLUSH);

      LOG.info("=== after snapshot with 500 rows");
      logFSTree();

      // insert more data
      loadData(table, 500, FAMILY);
      snapshot1Rows = TEST_UTIL.countRows(table);
      LOG.info("=== before snapshot with 1000 rows");
      logFSTree();

      // take a snapshot of the updated table
      admin.snapshot(Bytes.toString(snapshotName1), Bytes.toString(tableName),
          SnapshotDescription.Type.FLUSH);
      LOG.info("=== after snapshot with 1000 rows");
      logFSTree();
    } finally {
      table.close();
    }
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.deleteTable(tableName);
    admin.deleteSnapshot(snapshotName0);
    admin.deleteSnapshot(snapshotName1);

    // Ensure the archiver to be empty
    MasterFileSystem mfs = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    mfs.getFileSystem().delete(
      new Path(mfs.getRootDir(), HConstants.HFILE_ARCHIVE_DIRECTORY), true);
  }

  @Test
  public void testTakeFlushSnapshot() throws IOException {
    // taking happens in setup.
  }

  @Test
  public void testRestoreSnapshot() throws IOException {
    verifyRowCount(tableName, snapshot1Rows);

    // Restore from snapshot-0
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName0);
    logFSTree();
    admin.enableTable(tableName);
    LOG.info("=== after restore with 500 row snapshot");
    logFSTree();
    verifyRowCount(tableName, snapshot0Rows);

    // Restore from snapshot-1
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);
    verifyRowCount(tableName, snapshot1Rows);
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
  }

  private void testCloneSnapshot(final byte[] tableName, final byte[] snapshotName,
      int snapshotRows) throws IOException, InterruptedException {
    // create a new table from snapshot
    admin.cloneSnapshot(snapshotName, tableName);
    verifyRowCount(tableName, snapshotRows);

    TEST_UTIL.deleteTable(tableName);
  }

  @Test
  public void testRestoreSnapshotOfCloned() throws IOException, InterruptedException {
    byte[] clonedTableName = Bytes.toBytes("clonedtb-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    verifyRowCount(clonedTableName, snapshot0Rows);
    admin.snapshot(Bytes.toString(snapshotName2), Bytes.toString(clonedTableName), SnapshotDescription.Type.FLUSH);
    TEST_UTIL.deleteTable(clonedTableName);

    admin.cloneSnapshot(snapshotName2, clonedTableName);
    verifyRowCount(clonedTableName, snapshot0Rows);
    TEST_UTIL.deleteTable(clonedTableName);
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

  private void logFSTree() throws IOException {
    MasterFileSystem mfs = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    FSUtils.logFileSystemState(mfs.getFileSystem(), mfs.getRootDir(), LOG);
  }

  private void verifyRowCount(final byte[] tableName, long expectedRows) throws IOException {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    assertEquals(expectedRows, TEST_UTIL.countRows(table));
    table.close();
  }
}
