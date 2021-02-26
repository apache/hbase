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

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TableSnapshotInputFormatTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TableSnapshotInputFormatTestBase.class);
  protected final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  protected static final int NUM_REGION_SERVERS = 2;
  protected static final byte[][] FAMILIES = {Bytes.toBytes("f1"), Bytes.toBytes("f2")};

  protected FileSystem fs;
  protected Path rootDir;

  @Before
  public void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numRegionServers(NUM_REGION_SERVERS).numDataNodes(NUM_REGION_SERVERS)
        .createRootDir(true).build();
    UTIL.startMiniCluster(option);
    rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    fs = rootDir.getFileSystem(UTIL.getConfiguration());
  }

  @After
  public void tearDownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private static void setupConf(Configuration conf) {
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
  }

  protected abstract void testWithMockedMapReduce(HBaseTestingUtility util, String snapshotName,
    int numRegions, int numSplitsPerRegion, int expectedNumSplits, boolean setLocalityEnabledTo)
    throws Exception;

  protected abstract void testWithMapReduceImpl(HBaseTestingUtility util, TableName tableName,
    String snapshotName, Path tableDir, int numRegions, int numSplitsPerRegion,
    int expectedNumSplits, boolean shutdownCluster) throws Exception;

  protected abstract byte[] getStartRow();

  protected abstract byte[] getEndRow();

  @Test
  public void testWithMockedMapReduceSingleRegion() throws Exception {
    testWithMockedMapReduce(UTIL, "testWithMockedMapReduceSingleRegion", 1, 1, 1, true);
  }

  @Test
  public void testWithMockedMapReduceMultiRegion() throws Exception {
    testWithMockedMapReduce(UTIL, "testWithMockedMapReduceMultiRegion", 10, 1, 8, false);
  }

  @Test
  public void testWithMapReduceSingleRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceSingleRegion", 1, 1, 1, false);
  }

  @Test
  public void testWithMapReduceMultiRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceMultiRegion", 10, 1, 8, false);
  }

  @Test
  // run the MR job while HBase is offline
  public void testWithMapReduceAndOfflineHBaseMultiRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceAndOfflineHBaseMultiRegion", 10, 1, 8, true);
  }

  // Test that snapshot restore does not create back references in the HBase root dir.
  @Test
  public void testRestoreSnapshotDoesNotCreateBackRefLinks() throws Exception {
    TableName tableName = TableName.valueOf("testRestoreSnapshotDoesNotCreateBackRefLinks");
    String snapshotName = "foo";

    try {
      createTableAndSnapshot(UTIL, tableName, snapshotName, getStartRow(), getEndRow(), 1);

      Path tmpTableDir = UTIL.getDataTestDirOnTestFS(snapshotName);

      testRestoreSnapshotDoesNotCreateBackRefLinksInit(tableName, snapshotName,tmpTableDir);

      Path rootDir = CommonFSUtils.getRootDir(UTIL.getConfiguration());
      for (Path regionDir : FSUtils.getRegionDirs(fs,
        CommonFSUtils.getTableDir(rootDir, tableName))) {
        for (Path storeDir : FSUtils.getFamilyDirs(fs, regionDir)) {
          for (FileStatus status : fs.listStatus(storeDir)) {
            System.out.println(status.getPath());
            if (StoreFileInfo.isValid(status)) {
              Path archiveStoreDir = HFileArchiveUtil.getStoreArchivePath(UTIL.getConfiguration(),
                tableName, regionDir.getName(), storeDir.getName());

              Path path = HFileLink.getBackReferencesDir(storeDir, status.getPath().getName());
              // assert back references directory is empty
              assertFalse("There is a back reference in " + path, fs.exists(path));

              path = HFileLink.getBackReferencesDir(archiveStoreDir, status.getPath().getName());
              // assert back references directory is empty
              assertFalse("There is a back reference in " + path, fs.exists(path));
            }
          }
        }
      }
    } finally {
      UTIL.getAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
    }
  }

  public abstract void testRestoreSnapshotDoesNotCreateBackRefLinksInit(TableName tableName,
      String snapshotName, Path tmpTableDir) throws Exception;

  protected void testWithMapReduce(HBaseTestingUtility util, String snapshotName, int numRegions,
      int numSplitsPerRegion, int expectedNumSplits, boolean shutdownCluster) throws Exception {
    Path tableDir = util.getDataTestDirOnTestFS(snapshotName);
    TableName tableName = TableName.valueOf("testWithMapReduce");
    testWithMapReduceImpl(util, tableName, snapshotName, tableDir, numRegions, numSplitsPerRegion,
      expectedNumSplits, shutdownCluster);
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

    for (byte[] family : FAMILIES) {
      byte[] actual = result.getValue(family, family);
      Assert.assertArrayEquals(
        "Row in snapshot does not match, expected:" + Bytes.toString(row) + " ,actual:" + Bytes
          .toString(actual), row, actual);
    }
  }

  protected static void createTableAndSnapshot(HBaseTestingUtility util, TableName tableName,
    String snapshotName, byte[] startRow, byte[] endRow, int numRegions)
    throws Exception {
    try {
      LOG.debug("Ensuring table doesn't exist.");
      util.deleteTable(tableName);
    } catch(Exception ex) {
      // ignore
    }

    LOG.info("creating table '" + tableName + "'");
    if (numRegions > 1) {
      util.createTable(tableName, FAMILIES, 1, startRow, endRow, numRegions);
    } else {
      util.createTable(tableName, FAMILIES);
    }
    Admin admin = util.getAdmin();

    LOG.info("put some stuff in the table");
    Table table = util.getConnection().getTable(tableName);
    util.loadTable(table, FAMILIES);

    Path rootDir = CommonFSUtils.getRootDir(util.getConfiguration());
    FileSystem fs = rootDir.getFileSystem(util.getConfiguration());

    LOG.info("snapshot");
    SnapshotTestingUtils.createSnapshotAndValidate(admin, tableName,
      Arrays.asList(FAMILIES), null, snapshotName, rootDir, fs, true);

    LOG.info("load different values");
    byte[] value = Bytes.toBytes("after_snapshot_value");
    util.loadTable(table, FAMILIES, value);

    LOG.info("cause flush to create new files in the region");
    admin.flush(tableName);
    table.close();
  }
}
