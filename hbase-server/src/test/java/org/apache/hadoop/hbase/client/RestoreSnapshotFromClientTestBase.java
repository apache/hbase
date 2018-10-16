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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Base class for testing restore snapshot
 */
public class RestoreSnapshotFromClientTestBase {
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected final byte[] FAMILY = Bytes.toBytes("cf");
  protected final byte[] TEST_FAMILY2 = Bytes.toBytes("cf2");

  protected TableName tableName;
  protected byte[] emptySnapshot;
  protected byte[] snapshotName0;
  protected byte[] snapshotName1;
  protected byte[] snapshotName2;
  protected int snapshot0Rows;
  protected int snapshot1Rows;
  protected Admin admin;

  @Rule
  public TestName name = new TestName();

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
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Initialize the tests with a table filled with some data and two snapshots (snapshotName0,
   * snapshotName1) of different states. The tableName, snapshotNames and the number of rows in the
   * snapshot are initialized.
   */
  @Before
  public void setup() throws Exception {
    this.admin = TEST_UTIL.getAdmin();

    long tid = System.currentTimeMillis();
    tableName = TableName.valueOf(getValidMethodName() + "-" + tid);
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
    SnapshotTestingUtils.deleteAllSnapshots(TEST_UTIL.getAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
  }

  protected int getNumReplicas() {
    return 1;
  }

  protected int countRows(Table table, byte[]... families) throws IOException {
    return TEST_UTIL.countRows(table, families);
  }

  protected void verifyRowCount(HBaseTestingUtility util, TableName tableName, long expectedRows)
      throws IOException {
    SnapshotTestingUtils.verifyRowCount(util, tableName, expectedRows);
  }

  protected final void splitRegion(RegionInfo regionInfo) throws IOException {
    byte[][] splitPoints = Bytes.split(regionInfo.getStartKey(), regionInfo.getEndKey(), 1);
    admin.split(regionInfo.getTable(), splitPoints[1]);
  }

  protected final String getValidMethodName() {
    return name.getMethodName().replaceAll("[^0-9A-Za-z_]", "_");
  }
}
