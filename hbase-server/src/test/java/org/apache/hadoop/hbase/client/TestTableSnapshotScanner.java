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
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({LargeTests.class, ClientTests.class})
public class TestTableSnapshotScanner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableSnapshotScanner.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestTableSnapshotScanner.class);
  private final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_REGION_SERVERS = 2;
  private static final byte[][] FAMILIES = {Bytes.toBytes("f1"), Bytes.toBytes("f2")};
  public static byte[] bbb = Bytes.toBytes("bbb");
  public static byte[] yyy = Bytes.toBytes("yyy");

  private FileSystem fs;
  private Path rootDir;

  public static void blockUntilSplitFinished(HBaseTestingUtility util, TableName tableName,
      int expectedRegionSize) throws Exception {
    for (int i = 0; i < 100; i++) {
      List<RegionInfo> hRegionInfoList = util.getAdmin().getRegions(tableName);
      if (hRegionInfoList.size() >= expectedRegionSize) {
        break;
      }
      Thread.sleep(1000);
    }
  }

  public void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numRegionServers(NUM_REGION_SERVERS).numDataNodes(NUM_REGION_SERVERS)
        .createRootDir(true).build();
    UTIL.startMiniCluster(option);
    rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    fs = rootDir.getFileSystem(UTIL.getConfiguration());
  }

  public void tearDownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private static void setupConf(Configuration conf) {
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
  }

  @After
  public void tearDown() throws Exception {
  }

  public static void createTableAndSnapshot(HBaseTestingUtility util, TableName tableName,
      String snapshotName, int numRegions)
      throws Exception {
    try {
      util.deleteTable(tableName);
    } catch(Exception ex) {
      // ignore
    }

    if (numRegions > 1) {
      util.createTable(tableName, FAMILIES, 1, bbb, yyy, numRegions);
    } else {
      util.createTable(tableName, FAMILIES);
    }
    Admin admin = util.getAdmin();

    // put some stuff in the table
    Table table = util.getConnection().getTable(tableName);
    util.loadTable(table, FAMILIES);

    Path rootDir = FSUtils.getRootDir(util.getConfiguration());
    FileSystem fs = rootDir.getFileSystem(util.getConfiguration());

    SnapshotTestingUtils.createSnapshotAndValidate(admin, tableName,
        Arrays.asList(FAMILIES), null, snapshotName, rootDir, fs, true);

    // load different values
    byte[] value = Bytes.toBytes("after_snapshot_value");
    util.loadTable(table, FAMILIES, value);

    // cause flush to create new files in the region
    admin.flush(tableName);
    table.close();
  }

  @Test
  public void testNoDuplicateResultsWhenSplitting() throws Exception {
    setupCluster();
    TableName tableName = TableName.valueOf("testNoDuplicateResultsWhenSplitting");
    String snapshotName = "testSnapshotBug";
    try {
      if (UTIL.getAdmin().tableExists(tableName)) {
        UTIL.deleteTable(tableName);
      }

      UTIL.createTable(tableName, FAMILIES);
      Admin admin = UTIL.getAdmin();

      // put some stuff in the table
      Table table = UTIL.getConnection().getTable(tableName);
      UTIL.loadTable(table, FAMILIES);

      // split to 2 regions
      admin.split(tableName, Bytes.toBytes("eee"));
      blockUntilSplitFinished(UTIL, tableName, 2);

      Path rootDir = FSUtils.getRootDir(UTIL.getConfiguration());
      FileSystem fs = rootDir.getFileSystem(UTIL.getConfiguration());

      SnapshotTestingUtils.createSnapshotAndValidate(admin, tableName,
        Arrays.asList(FAMILIES), null, snapshotName, rootDir, fs, true);

      // load different values
      byte[] value = Bytes.toBytes("after_snapshot_value");
      UTIL.loadTable(table, FAMILIES, value);

      // cause flush to create new files in the region
      admin.flush(tableName);
      table.close();

      Path restoreDir = UTIL.getDataTestDirOnTestFS(snapshotName);
      Scan scan = new Scan().withStartRow(bbb).withStopRow(yyy); // limit the scan

      TableSnapshotScanner scanner =
          new TableSnapshotScanner(UTIL.getConfiguration(), restoreDir, snapshotName, scan);

      verifyScanner(scanner, bbb, yyy);
      scanner.close();
    } finally {
      UTIL.getAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
      tearDownCluster();
    }
  }

  @Test
  public void testWithSingleRegion() throws Exception {
    testScanner(UTIL, "testWithSingleRegion", 1, false);
  }

  @Test
  public void testWithMultiRegion() throws Exception {
    testScanner(UTIL, "testWithMultiRegion", 10, false);
  }

  @Test
  public void testWithOfflineHBaseMultiRegion() throws Exception {
    testScanner(UTIL, "testWithMultiRegion", 20, true);
  }

  @Test
  public void testScannerWithRestoreScanner() throws Exception {
    setupCluster();
    TableName tableName = TableName.valueOf("testScanner");
    String snapshotName = "testScannerWithRestoreScanner";
    try {
      createTableAndSnapshot(UTIL, tableName, snapshotName, 50);
      Path restoreDir = UTIL.getDataTestDirOnTestFS(snapshotName);
      Scan scan = new Scan(bbb, yyy); // limit the scan

      Configuration conf = UTIL.getConfiguration();
      Path rootDir = FSUtils.getRootDir(conf);

      TableSnapshotScanner scanner0 =
          new TableSnapshotScanner(conf, restoreDir, snapshotName, scan);
      verifyScanner(scanner0, bbb, yyy);
      scanner0.close();

      // restore snapshot.
      RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);

      // scan the snapshot without restoring snapshot
      TableSnapshotScanner scanner =
          new TableSnapshotScanner(conf, rootDir, restoreDir, snapshotName, scan, true);
      verifyScanner(scanner, bbb, yyy);
      scanner.close();

      // check whether the snapshot has been deleted by the close of scanner.
      scanner = new TableSnapshotScanner(conf, rootDir, restoreDir, snapshotName, scan, true);
      verifyScanner(scanner, bbb, yyy);
      scanner.close();

      // restore snapshot again.
      RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);

      // check whether the snapshot has been deleted by the close of scanner.
      scanner = new TableSnapshotScanner(conf, rootDir, restoreDir, snapshotName, scan, true);
      verifyScanner(scanner, bbb, yyy);
      scanner.close();
    } finally {
      UTIL.getAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
      tearDownCluster();
    }
  }

  private void testScanner(HBaseTestingUtility util, String snapshotName, int numRegions,
      boolean shutdownCluster) throws Exception {
    setupCluster();
    TableName tableName = TableName.valueOf("testScanner");
    try {
      createTableAndSnapshot(util, tableName, snapshotName, numRegions);

      if (shutdownCluster) {
        util.shutdownMiniHBaseCluster();
      }

      Path restoreDir = util.getDataTestDirOnTestFS(snapshotName);
      Scan scan = new Scan(bbb, yyy); // limit the scan

      TableSnapshotScanner scanner = new TableSnapshotScanner(UTIL.getConfiguration(), restoreDir,
        snapshotName, scan);

      verifyScanner(scanner, bbb, yyy);
      scanner.close();
    } finally {
      if (!shutdownCluster) {
        util.getAdmin().deleteSnapshot(snapshotName);
        util.deleteTable(tableName);
        tearDownCluster();
      }
    }
  }

  private void verifyScanner(ResultScanner scanner, byte[] startRow, byte[] stopRow)
      throws IOException, InterruptedException {

    HBaseTestingUtility.SeenRowTracker rowTracker =
        new HBaseTestingUtility.SeenRowTracker(startRow, stopRow);

    while (true) {
      Result result = scanner.next();
      if (result == null) {
        break;
      }
      verifyRow(result);
      rowTracker.addRow(result.getRow());
    }

    // validate all rows are seen
    rowTracker.validate();
  }

  private static void verifyRow(Result result) throws IOException {
    byte[] row = result.getRow();
    CellScanner scanner = result.cellScanner();
    while (scanner.advance()) {
      Cell cell = scanner.current();

      //assert that all Cells in the Result have the same key
     Assert.assertEquals(0, Bytes.compareTo(row, 0, row.length,
         cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    }

    for (int j = 0; j < FAMILIES.length; j++) {
      byte[] actual = result.getValue(FAMILIES[j], FAMILIES[j]);
      Assert.assertArrayEquals("Row in snapshot does not match, expected:" + Bytes.toString(row)
          + " ,actual:" + Bytes.toString(actual), row, actual);
    }
  }

}
