/*
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtil;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(LargeTests.TAG)
public class TestCleanupCompactedFileAfterFailover {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestCleanupCompactedFileAfterFailover.class);

  private static HBaseTestingUtil TEST_UTIL;
  private static Admin admin;
  private static Table table;

  private static TableName TABLE_NAME = TableName.valueOf("TestCleanupCompactedFileAfterFailover");
  private static byte[] ROW = Bytes.toBytes("row");
  private static byte[] FAMILY = Bytes.toBytes("cf");
  private static byte[] QUALIFIER = Bytes.toBytes("cq");
  private static byte[] VALUE = Bytes.toBytes("value");
  private static final int RS_NUMBER = 5;

  @BeforeAll
  public static void beforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    // Set the scanner lease to 20min, so the scanner can't be closed by RegionServer
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1200000);
    TEST_UTIL.getConfiguration().setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY,
      100);
    TEST_UTIL.getConfiguration().set("dfs.blocksize", "64000");
    TEST_UTIL.getConfiguration().set("dfs.namenode.fs-limits.min-block-size", "1024");
    TEST_UTIL.getConfiguration().set(TimeToLiveHFileCleaner.TTL_CONF_KEY, "0");
    TEST_UTIL.startMiniCluster(RS_NUMBER);
    admin = TEST_UTIL.getAdmin();
  }

  @AfterAll
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void before() throws Exception {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build());
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    table = TEST_UTIL.getConnection().getTable(TABLE_NAME);
  }

  @AfterEach
  public void after() throws Exception {
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
  }

  @Test
  public void testCleanupAfterFailoverWithCompactOnce() throws Exception {
    testCleanupAfterFailover(1);
  }

  @Test
  public void testCleanupAfterFailoverWithCompactTwice() throws Exception {
    testCleanupAfterFailover(2);
  }

  @Test
  public void testCleanupAfterFailoverWithCompactThreeTimes() throws Exception {
    testCleanupAfterFailover(3);
  }

  private void testCleanupAfterFailover(int compactNum) throws Exception {
    HRegionServer rsServedTable = null;
    List<HRegion> regions = new ArrayList<>();
    for (JVMClusterUtil.RegionServerThread rsThread : TEST_UTIL.getHBaseCluster()
      .getLiveRegionServerThreads()) {
      HRegionServer rs = rsThread.getRegionServer();
      if (rs.getOnlineTables().contains(TABLE_NAME)) {
        regions.addAll(rs.getRegions(TABLE_NAME));
        rsServedTable = rs;
      }
    }
    assertNotNull(rsServedTable);
    assertEquals(1, regions.size(), "Table should only have one region");
    HRegion region = regions.get(0);
    HStore store = region.getStore(FAMILY);

    writeDataAndFlush(3, region);
    assertEquals(3, store.getStorefilesCount());

    // Open a scanner and not close, then the storefile will be referenced
    store.getScanner(new Scan(), null, 0);

    region.compact(true);
    assertEquals(1, store.getStorefilesCount());
    // The compacted file should not be archived as there are references by user scanner
    assertEquals(3, store.getStoreEngine().getStoreFileManager().getCompactedfiles().size());

    for (int i = 1; i < compactNum; i++) {
      // Compact again
      region.compact(true);
      assertEquals(1, store.getStorefilesCount());
      store.closeAndArchiveCompactedFiles();
      // Compacted storefiles still be 3 as the new compacted storefile was archived
      assertEquals(3, store.getStoreEngine().getStoreFileManager().getCompactedfiles().size());
    }

    int walNum = rsServedTable.getWALs().size();
    // Roll WAL
    rsServedTable.getWalRoller().requestRollAll();
    // Flush again
    region.flush(true);
    // The WAL which contains compaction event marker should be archived
    assertEquals(walNum, rsServedTable.getWALs().size(), "The old WAL should be archived");

    rsServedTable.kill();
    // Sleep to wait failover
    Thread.sleep(3000);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);

    regions.clear();
    for (JVMClusterUtil.RegionServerThread rsThread : TEST_UTIL.getHBaseCluster()
      .getLiveRegionServerThreads()) {
      HRegionServer rs = rsThread.getRegionServer();
      if (rs != rsServedTable && rs.getOnlineTables().contains(TABLE_NAME)) {
        regions.addAll(rs.getRegions(TABLE_NAME));
      }
    }
    assertEquals(1, regions.size(), "Table should only have one region");
    region = regions.get(0);
    store = region.getStore(FAMILY);
    // The compacted storefile should be cleaned and only have 1 storefile
    assertEquals(1, store.getStorefilesCount());
  }

  private void writeDataAndFlush(int fileNum, HRegion region) throws Exception {
    for (int i = 0; i < fileNum; i++) {
      for (int j = 0; j < 100; j++) {
        table.put(new Put(concat(ROW, j)).addColumn(FAMILY, QUALIFIER, concat(VALUE, j)));
      }
      region.flush(true);
    }
  }

  private byte[] concat(byte[] base, int index) {
    return Bytes.toBytes(Bytes.toString(base) + "-" + index);
  }
}
