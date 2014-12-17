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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapreduce.TestTableSnapshotInputFormat;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({LargeTests.class, ClientTests.class})
public class TestTableSnapshotScanner {

  private static final Log LOG = LogFactory.getLog(TestTableSnapshotInputFormat.class);
  private final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_REGION_SERVERS = 2;
  private static final byte[][] FAMILIES = {Bytes.toBytes("f1"), Bytes.toBytes("f2")};
  public static byte[] bbb = Bytes.toBytes("bbb");
  public static byte[] yyy = Bytes.toBytes("yyy");

  private FileSystem fs;
  private Path rootDir;

  public void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_REGION_SERVERS, true);
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
    Admin admin = util.getHBaseAdmin();

    // put some stuff in the table
    HTable table = new HTable(util.getConfiguration(), tableName);
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
        util.getHBaseAdmin().deleteSnapshot(snapshotName);
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
      byte[] actual = result.getValue(FAMILIES[j], null);
      Assert.assertArrayEquals("Row in snapshot does not match, expected:" + Bytes.toString(row)
          + " ,actual:" + Bytes.toString(actual), row, actual);
    }
  }

}
