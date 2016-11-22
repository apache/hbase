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

package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.fs.RegionStorage.StoreFileVisitor;
import org.apache.hadoop.hbase.fs.legacy.io.HFileLink;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Arrays;

public abstract class TableSnapshotInputFormatTestBase {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
      withTimeout(this.getClass()).withLookingForStuckThread(true).build();
  protected final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  protected static final int NUM_REGION_SERVERS = 2;
  protected static final byte[][] FAMILIES = {Bytes.toBytes("f1"), Bytes.toBytes("f2")};

  protected FileSystem fs;
  protected Path rootDir;

  public void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_REGION_SERVERS, true);
//    rootDir = UTIL.getHBaseCluster().getMaster().getMasterStorage().getRootDir();
    fs = rootDir.getFileSystem(UTIL.getConfiguration());
  }

  public void tearDownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private static void setupConf(Configuration conf) {
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
  }

  protected abstract void testWithMockedMapReduce(HBaseTestingUtility util, String snapshotName,
    int numRegions, int expectedNumSplits) throws Exception;

  protected abstract void testWithMapReduceImpl(HBaseTestingUtility util, TableName tableName,
    String snapshotName, Path tableDir, int numRegions, int expectedNumSplits,
    boolean shutdownCluster) throws Exception;

  protected abstract byte[] getStartRow();

  protected abstract byte[] getEndRow();

  @Test
  public void testWithMockedMapReduceSingleRegion() throws Exception {
    testWithMockedMapReduce(UTIL, "testWithMockedMapReduceSingleRegion", 1, 1);
  }

  @Test
  public void testWithMockedMapReduceMultiRegion() throws Exception {
    testWithMockedMapReduce(UTIL, "testWithMockedMapReduceMultiRegion", 10, 8);
  }

  @Test
  public void testWithMapReduceSingleRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceSingleRegion", 1, 1, false);
  }

  @Test
  public void testWithMapReduceMultiRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceMultiRegion", 10, 8, false);
  }

  @Test
  // run the MR job while HBase is offline
  public void testWithMapReduceAndOfflineHBaseMultiRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceAndOfflineHBaseMultiRegion", 10, 8, true);
  }

  // Test that snapshot restore does not create back references in the HBase root dir.
  @Test
  public void testRestoreSnapshotDoesNotCreateBackRefLinks() throws Exception {
    setupCluster();
    TableName tableName = TableName.valueOf("testRestoreSnapshotDoesNotCreateBackRefLinks");
    String snapshotName = "foo";

    try {
      createTableAndSnapshot(UTIL, tableName, snapshotName, getStartRow(), getEndRow(), 1);

      Path tmpTableDir = UTIL.getDataTestDirOnTestFS(snapshotName);

      testRestoreSnapshotDoesNotCreateBackRefLinksInit(tableName, snapshotName,tmpTableDir);

      UTIL.getHBaseCluster().getMaster().getMasterStorage().visitStoreFiles(tableName,
          new StoreFileVisitor() {
        @Override
        public void storeFile(HRegionInfo region, String family, StoreFileInfo storeFile)
            throws IOException {
          Path archiveStoreDir = HFileArchiveUtil.getStoreArchivePath(UTIL.getConfiguration(),
            region.getTable(), region.getEncodedName(), family);

          // assert back references directory is empty
          Path storeDir = storeFile.getPath().getParent();
          Path path = HFileLink.getBackReferencesDir(storeDir, storeFile.getPath().getName());
          assertFalse("There is a back reference in " + path, fs.exists(path));

          // assert back references directory is empty
          path = HFileLink.getBackReferencesDir(archiveStoreDir, storeFile.getPath().getName());
          assertFalse("There is a back reference in " + path, fs.exists(path));
        }
      });
    } finally {
      UTIL.getHBaseAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
      tearDownCluster();
    }
  }

  public abstract void testRestoreSnapshotDoesNotCreateBackRefLinksInit(TableName tableName,
      String snapshotName, Path tmpTableDir) throws Exception;

  protected void testWithMapReduce(HBaseTestingUtility util, String snapshotName,
      int numRegions, int expectedNumSplits, boolean shutdownCluster) throws Exception {
    setupCluster();
    try {
      Path tableDir = util.getDataTestDirOnTestFS(snapshotName);
      TableName tableName = TableName.valueOf("testWithMapReduce");
      testWithMapReduceImpl(util, tableName, snapshotName, tableDir, numRegions,
        expectedNumSplits, shutdownCluster);
    } finally {
      tearDownCluster();
    }
  }

  protected static void verifyRowFromMap(ImmutableBytesWritable key, Result result)
    throws IOException {
    byte[] row = key.get();
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

  protected static void createTableAndSnapshot(HBaseTestingUtility util, TableName tableName,
    String snapshotName, byte[] startRow, byte[] endRow, int numRegions)
    throws Exception {
    try {
      util.deleteTable(tableName);
    } catch(Exception ex) {
      // ignore
    }

    if (numRegions > 1) {
      util.createTable(tableName, FAMILIES, 1, startRow, endRow, numRegions);
    } else {
      util.createTable(tableName, FAMILIES);
    }
    Admin admin = util.getHBaseAdmin();

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

}
