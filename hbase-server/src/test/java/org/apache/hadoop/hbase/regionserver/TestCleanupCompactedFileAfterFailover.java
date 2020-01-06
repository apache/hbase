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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({LargeTests.class})
public class TestCleanupCompactedFileAfterFailover {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCleanupCompactedFileAfterFailover.class);

  private static HBaseTestingUtility TEST_UTIL;
  private static Admin admin;

  private static TableName TABLE_NAME = TableName.valueOf("TestCleanupCompactedFileAfterFailover");
  private static byte[] ROW = Bytes.toBytes("row");
  private static byte[] FAMILY = Bytes.toBytes("cf");
  private static byte[] QUALIFIER = Bytes.toBytes("cq");
  private static byte[] VALUE = Bytes.toBytes("value");
  private static final int RS_NUMBER = 5;

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    // Set the scanner lease to 20min, so the scanner can't be closed by RegionServer
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1200000);
    TEST_UTIL.getConfiguration()
        .setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
    TEST_UTIL.getConfiguration().set("dfs.blocksize", "64000");
    TEST_UTIL.getConfiguration().set("dfs.namenode.fs-limits.min-block-size", "1024");
    TEST_UTIL.getConfiguration().set(TimeToLiveHFileCleaner.TTL_CONF_KEY, "0");
    TEST_UTIL.getConfiguration().setBoolean(HStore.FORCE_ARCHIVAL_AFTER_RESET, false);
    TEST_UTIL.startMiniCluster(RS_NUMBER);
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build());
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
  }

  @After
  public void after() throws Exception {
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
  }

  @Test
  public void testCleanupAfterFailoverWithCompactOnce() throws Exception {
    testCleanupAfterFailover(1, false, TEST_UTIL);
  }

  @Test
  public void testCleanupAfterFailoverWithCompactTwice() throws Exception {
    testCleanupAfterFailover(2, false, TEST_UTIL);
  }

  @Test
  public void testCleanupAfterFailoverWithCompactThreeTimes() throws Exception {
    testCleanupAfterFailover(3, false, TEST_UTIL);
  }

  static void testCleanupAfterFailover(int compactNum, boolean isForceArchive,
      HBaseTestingUtility testUtil) throws Exception {
    HRegionServer rsServedTable = null;
    List<HRegion> regions = new ArrayList<>();
    for (JVMClusterUtil.RegionServerThread rsThread : testUtil.getHBaseCluster()
        .getLiveRegionServerThreads()) {
      HRegionServer rs = rsThread.getRegionServer();
      if (rs.getOnlineTables().contains(TABLE_NAME)) {
        regions.addAll(rs.getRegions(TABLE_NAME));
        rsServedTable = rs;
      }
    }
    assertNotNull(rsServedTable);
    assertEquals("Table should only have one region", 1, regions.size());
    HRegion region = regions.get(0);
    HStore store = region.getStore(FAMILY);

    writeDataAndFlush(3, region, testUtil);
    assertEquals(3, store.getStorefilesCount());

    // Open a scanner and not close, then the storefile will be referenced
    KeyValueScanner kvs = store.getScanner(new Scan(), null, 0);

    region.compact(true);
    assertEquals(1, store.getStorefilesCount());
    // The compacted file should not be archived as there are references by user scanner
    assertEquals(3, store.getStoreEngine().getStoreFileManager().getCompactedfiles().size());

    for (int i = 1; i < compactNum; i++) {
      // Compact again
      region.compact(true);
      assertEquals(1, store.getStorefilesCount());
      // try archival - might not archive all eligible files given that they have
      // references in use, if isForceArchive enabled, will notify readers
      store.closeAndArchiveCompactedFiles();
      ((StoreScanner) kvs).next(new ArrayList<>());
      // try archival again since above scan call should have reset scanners
      // and also update the heap
      store.closeAndArchiveCompactedFiles();
      // Compacted storefiles still be 3 as the new compacted storefile was archived if
      // force-archival by scanner reset is disabled, otherwise count should be 0
      int compactedFiles = isForceArchive ? 0 : 3;
      assertEquals(compactedFiles, store.getStoreEngine().getStoreFileManager()
        .getCompactedfiles().size());
      assertEquals(1, store.getStoreEngine().getStoreFileManager().getStorefiles().size());
    }

    int walNum = rsServedTable.getWALs().size();
    // Roll WAL
    rsServedTable.getWalRoller().requestRollAll();
    // Flush again
    region.flush(true);
    // The WAL which contains compaction event marker should be archived
    assertEquals("The old WAL should be archived", walNum, rsServedTable.getWALs().size());

    rsServedTable.kill();
    // Sleep to wait failover
    Thread.sleep(3000);
    testUtil.waitTableAvailable(TABLE_NAME);

    regions.clear();
    for (JVMClusterUtil.RegionServerThread rsThread : testUtil.getHBaseCluster()
        .getLiveRegionServerThreads()) {
      HRegionServer rs = rsThread.getRegionServer();
      if (rs != rsServedTable && rs.getOnlineTables().contains(TABLE_NAME)) {
        regions.addAll(rs.getRegions(TABLE_NAME));
      }
    }
    assertEquals("Table should only have one region", 1, regions.size());
    region = regions.get(0);
    store = region.getStore(FAMILY);
    // The compacted storefile should be cleaned and only have 1 storefile
    assertEquals(1, store.getStorefilesCount());
  }

  private static void writeDataAndFlush(int fileNum, HRegion region,
      HBaseTestingUtility testUtil) throws Exception {
    Table table = testUtil.getConnection().getTable(TABLE_NAME);
    for (int i = 0; i < fileNum; i++) {
      for (int j = 0; j < 100; j++) {
        table.put(new Put(concat(ROW, j)).addColumn(FAMILY, QUALIFIER, concat(VALUE, j)));
      }
      region.flush(true);
    }
  }

  private static byte[] concat(byte[] base, int index) {
    return Bytes.toBytes(Bytes.toString(base) + "-" + index);
  }
}
