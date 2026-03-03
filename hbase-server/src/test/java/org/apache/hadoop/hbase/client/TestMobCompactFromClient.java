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
package org.apache.hadoop.hbase.client;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestMobCompactFromClient {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final String FAMILY = "info";
  private static final String MOB_FAMILY = "mob_info";
  private static final String QUALIFIER = "q";

  private static Admin admin;

  @BeforeAll
  public static void setup() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    admin = TEST_UTIL.getAdmin();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getTestFileSystem().delete(TEST_UTIL.getDataTestDir(), true);
  }

  @Test
  public void testCompactMobTableFromClientSize(TestInfo testInfo) throws Exception {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
    tableBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(MOB_FAMILY))
      .setMobEnabled(true).setMobThreshold(100L).build());
    tableBuilder
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(FAMILY)).build());
    admin.createTable(tableBuilder.build());

    assertTrue(admin.tableExists(tableName));

    try (Table table = admin.getConnection().getTable(tableName)) {
      // Put some data && flush the table
      for (int i = 0; i < 5; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.addColumn(Bytes.toBytes(MOB_FAMILY), Bytes.toBytes(QUALIFIER), makeDummyData(500));
        put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER), makeDummyData(10));
        table.put(put);
        admin.flush(tableName);
      }

      List<RegionInfo> regionInfos = admin.getRegions(tableName);
      assertEquals(1, regionInfos.size());
      RegionInfo regionInfo = regionInfos.get(0);
      HRegion region =
        TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionInfo.getEncodedName());
      HStore store1 = region.getStore(Bytes.toBytes(MOB_FAMILY));
      assertNotNull(store1);
      HStore store2 = region.getStore(Bytes.toBytes(FAMILY));
      assertNotNull(store2);

      assertEquals(5, store1.getStorefilesCount());
      assertEquals(5, store2.getStorefilesCount());

      admin.compact(tableName, Bytes.toBytes(MOB_FAMILY), CompactType.MOB);
      await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
        .until(() -> admin.getCompactionState(tableName, CompactType.MOB) == CompactionState.NONE);
      assertEquals(CompactionState.NONE, admin.getCompactionState(tableName, CompactType.MOB));

      int store1fileCount = store1.getStorefilesCount();
      int store2fileCount = store2.getStorefilesCount();
      assertTrue(store1fileCount < 5);
      assertEquals(5, store2fileCount);

      // Put some data && flush the table
      for (int i = 5; i < 10; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.addColumn(Bytes.toBytes(MOB_FAMILY), Bytes.toBytes(QUALIFIER), makeDummyData(500));
        put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER), makeDummyData(10));
        table.put(put);
        admin.flush(tableName);
      }
      assertEquals(store1fileCount + 5, store1.getStorefilesCount());
      assertEquals(store2fileCount + 5, store2.getStorefilesCount());

      admin.compact(tableName, CompactType.MOB);
      await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
        .until(() -> admin.getCompactionState(tableName, CompactType.MOB) == CompactionState.NONE);
      assertEquals(CompactionState.NONE, admin.getCompactionState(tableName, CompactType.MOB));

      assertTrue(store1.getStorefilesCount() < (store1fileCount + 5));
      assertEquals(store2fileCount + 5, store2.getStorefilesCount());
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testMajorCompactMobTableFromClientSize(TestInfo testInfo) throws Exception {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
    tableBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(MOB_FAMILY))
      .setMobEnabled(true).setMobThreshold(100L).build());
    tableBuilder
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(FAMILY)).build());
    admin.createTable(tableBuilder.build());

    assertTrue(admin.tableExists(tableName));

    try (Table table = admin.getConnection().getTable(tableName)) {
      // Put some data && flush the table
      for (int i = 0; i < 5; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.addColumn(Bytes.toBytes(MOB_FAMILY), Bytes.toBytes(QUALIFIER), makeDummyData(500));
        put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER), makeDummyData(10));
        table.put(put);
        admin.flush(tableName);
      }

      List<RegionInfo> regionInfos = admin.getRegions(tableName);
      assertEquals(1, regionInfos.size());
      RegionInfo regionInfo = regionInfos.get(0);
      HRegion region =
        TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionInfo.getEncodedName());
      HStore store1 = region.getStore(Bytes.toBytes(MOB_FAMILY));
      assertNotNull(store1);
      HStore store2 = region.getStore(Bytes.toBytes(FAMILY));
      assertNotNull(store2);
      assertEquals(5, store1.getStorefilesCount());
      assertEquals(5, store2.getStorefilesCount());

      admin.majorCompact(tableName, CompactType.MOB);
      await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
        .until(() -> admin.getCompactionState(tableName, CompactType.MOB) == CompactionState.NONE);
      assertEquals(CompactionState.NONE, admin.getCompactionState(tableName, CompactType.MOB));

      assertEquals(1, store1.getStorefilesCount());
      assertEquals(5, store2.getStorefilesCount());

      // Put some data && flush the table
      for (int i = 5; i < 10; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.addColumn(Bytes.toBytes(MOB_FAMILY), Bytes.toBytes(QUALIFIER), makeDummyData(500));
        put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER), makeDummyData(10));
        table.put(put);
        admin.flush(tableName);
      }
      assertEquals(6, store1.getStorefilesCount());
      assertEquals(10, store2.getStorefilesCount());

      admin.compact(tableName, CompactType.MOB);
      await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
        .until(() -> admin.getCompactionState(tableName, CompactType.MOB) == CompactionState.NONE);
      assertEquals(CompactionState.NONE, admin.getCompactionState(tableName, CompactType.MOB));

      assertEquals(1, store1.getStorefilesCount());
      assertEquals(10, store2.getStorefilesCount());
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testCompactMobTableWithNonFamilyFromClientSize(TestInfo testInfo) throws IOException {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
    tableBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(MOB_FAMILY))
      .setMobEnabled(true).setMobThreshold(100L).build());
    TableDescriptor tableDescriptor = tableBuilder.build();
    admin.createTable(tableDescriptor);

    assertTrue(admin.tableExists(tableName));

    try (Table table = admin.getConnection().getTable(tableName)) {
      // Put some data && flush the table
      for (int i = 0; i < 5; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.addColumn(Bytes.toBytes(MOB_FAMILY), Bytes.toBytes(QUALIFIER), makeDummyData(500));
        table.put(put);
        admin.flush(tableName);
      }
      assertFalse(tableDescriptor.hasColumnFamily(Bytes.toBytes(FAMILY)));
      assertThrows(NoSuchColumnFamilyException.class,
        () -> admin.compact(tableName, Bytes.toBytes(FAMILY), CompactType.MOB));
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  private byte[] makeDummyData(int size) {
    byte[] dummyData = new byte[size];
    Bytes.random(dummyData);
    return dummyData;
  }
}
