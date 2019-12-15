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

import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous table admin operations.
 * @see TestAsyncTableAdminApi2 This test and it used to be joined it was taking longer than our
 *     ten minute timeout so they were split.
 * @see TestAsyncTableAdminApi3 Another split out from this class so each runs under ten minutes.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableAdminApi extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncTableAdminApi.class);

  @Test
  public void testCreateTable() throws Exception {
    List<TableDescriptor> tables = admin.listTableDescriptors().get();
    int numTables = tables.size();
    createTableWithDefaultConf(tableName);
    tables = admin.listTableDescriptors().get();
    assertEquals(numTables + 1, tables.size());
    assertTrue("Table must be enabled.", TEST_UTIL.getHBaseCluster().getMaster()
        .getTableStateManager().isTableState(tableName, TableState.State.ENABLED));
    assertEquals(TableState.State.ENABLED, getStateFromMeta(tableName));
  }

  static TableState.State getStateFromMeta(TableName table) throws Exception {
    Optional<TableState> state = AsyncMetaTableAccessor
        .getTableState(ASYNC_CONN.getTable(TableName.META_TABLE_NAME), table).get();
    assertTrue(state.isPresent());
    return state.get().getState();
  }

  @Test
  public void testCreateTableNumberOfRegions() throws Exception {
    AsyncTable<AdvancedScanResultConsumer> metaTable = ASYNC_CONN.getTable(META_TABLE_NAME);

    createTableWithDefaultConf(tableName);
    List<HRegionLocation> regionLocations = AsyncMetaTableAccessor
      .getTableHRegionLocations(metaTable, tableName).get();
    assertEquals("Table should have only 1 region", 1, regionLocations.size());

    final TableName tableName2 = TableName.valueOf(tableName.getNameAsString() + "_2");
    createTableWithDefaultConf(tableName2, new byte[][] { new byte[] { 42 } });
    regionLocations = AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, tableName2).get();
    assertEquals("Table should have only 2 region", 2, regionLocations.size());

    final TableName tableName3 = TableName.valueOf(tableName.getNameAsString() + "_3");
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName3);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build(), Bytes.toBytes("a"), Bytes.toBytes("z"), 3).join();
    regionLocations = AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, tableName3).get();
    assertEquals("Table should have only 3 region", 3, regionLocations.size());

    final TableName tableName4 = TableName.valueOf(tableName.getNameAsString() + "_4");
    builder = TableDescriptorBuilder.newBuilder(tableName4);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    try {
      admin.createTable(builder.build(), "a".getBytes(), "z".getBytes(), 2).join();
      fail("Should not be able to create a table with only 2 regions using this API.");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }

    final TableName tableName5 = TableName.valueOf(tableName.getNameAsString() + "_5");
    builder = TableDescriptorBuilder.newBuilder(tableName5);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build(), new byte[] { 1 }, new byte[] { 127 }, 16).join();
    regionLocations = AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, tableName5).get();
    assertEquals("Table should have 16 region", 16, regionLocations.size());
  }

  @Test
  public void testCreateTableWithRegions() throws Exception {
    byte[][] splitKeys = { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 }, new byte[] { 3, 3, 3 },
      new byte[] { 4, 4, 4 }, new byte[] { 5, 5, 5 }, new byte[] { 6, 6, 6 },
      new byte[] { 7, 7, 7 }, new byte[] { 8, 8, 8 }, new byte[] { 9, 9, 9 }, };
    int expectedRegions = splitKeys.length + 1;
    boolean tablesOnMaster = LoadBalancer.isTablesOnMaster(TEST_UTIL.getConfiguration());
    createTableWithDefaultConf(tableName, splitKeys);

    boolean tableAvailable = admin.isTableAvailable(tableName, splitKeys).get();
    assertTrue("Table should be created with splitKyes + 1 rows in META", tableAvailable);

    AsyncTable<AdvancedScanResultConsumer> metaTable = ASYNC_CONN.getTable(META_TABLE_NAME);
    List<HRegionLocation> regions = AsyncMetaTableAccessor
      .getTableHRegionLocations(metaTable, tableName).get();
    Iterator<HRegionLocation> hris = regions.iterator();

    assertEquals(
      "Tried to create " + expectedRegions + " regions " + "but only found " + regions.size(),
      expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");

    RegionInfo hri;
    hris = regions.iterator();
    hri = hris.next().getRegion();
    assertTrue(hri.getStartKey() == null || hri.getStartKey().length == 0);
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[0]));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[0]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[1]));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[1]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[2]));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[2]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[3]));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[3]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[4]));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[4]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[5]));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[5]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[6]));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[6]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[7]));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[7]));
    assertTrue(Bytes.equals(hri.getEndKey(), splitKeys[8]));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), splitKeys[8]));
    assertTrue(hri.getEndKey() == null || hri.getEndKey().length == 0);
    if (tablesOnMaster) {
      verifyRoundRobinDistribution(regions, expectedRegions);
    }

    // Now test using start/end with a number of regions

    // Use 80 bit numbers to make sure we aren't limited
    byte[] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte[] endKey = { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };

    // Splitting into 10 regions, we expect (null,1) ... (9, null)
    // with (1,2) (2,3) (3,4) (4,5) (5,6) (6,7) (7,8) (8,9) in the middle
    expectedRegions = 10;
    final TableName tableName2 = TableName.valueOf(tableName.getNameAsString() + "_2");
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName2);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build(), startKey, endKey, expectedRegions).join();

    regions = AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, tableName2).get();
    assertEquals(
      "Tried to create " + expectedRegions + " regions " + "but only found " + regions.size(),
      expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");

    hris = regions.iterator();
    hri = hris.next().getRegion();
    assertTrue(hri.getStartKey() == null || hri.getStartKey().length == 0);
    assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 }));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 }));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 3, 3, 3, 3, 3, 3, 3, 3, 3, 3 }));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 3, 3, 3, 3, 3, 3, 3, 3, 3, 3 }));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 4, 4, 4, 4, 4, 4, 4, 4, 4, 4 }));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 4, 4, 4, 4, 4, 4, 4, 4, 4, 4 }));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 5, 5, 5, 5, 5, 5, 5, 5, 5, 5 }));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 5, 5, 5, 5, 5, 5, 5, 5, 5, 5 }));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 6, 6, 6, 6, 6, 6, 6, 6, 6, 6 }));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 6, 6, 6, 6, 6, 6, 6, 6, 6, 6 }));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 }));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 }));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8 }));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8 }));
    assertTrue(Bytes.equals(hri.getEndKey(), new byte[] { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 }));
    hri = hris.next().getRegion();
    assertTrue(Bytes.equals(hri.getStartKey(), new byte[] { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 }));
    assertTrue(hri.getEndKey() == null || hri.getEndKey().length == 0);
    if (tablesOnMaster) {
      // This don't work if master is not carrying regions. FIX. TODO.
      verifyRoundRobinDistribution(regions, expectedRegions);
    }

    // Try once more with something that divides into something infinite
    startKey = new byte[] { 0, 0, 0, 0, 0, 0 };
    endKey = new byte[] { 1, 0, 0, 0, 0, 0 };

    expectedRegions = 5;
    final TableName tableName3 = TableName.valueOf(tableName.getNameAsString() + "_3");
    builder = TableDescriptorBuilder.newBuilder(tableName3);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build(), startKey, endKey, expectedRegions).join();

    regions = AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, tableName3)
      .get();
    assertEquals(
      "Tried to create " + expectedRegions + " regions " + "but only found " + regions.size(),
      expectedRegions, regions.size());
    System.err.println("Found " + regions.size() + " regions");
    if (tablesOnMaster) {
      // This don't work if master is not carrying regions. FIX. TODO.
      verifyRoundRobinDistribution(regions, expectedRegions);
    }

    // Try an invalid case where there are duplicate split keys
    splitKeys = new byte[][] { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 },
      new byte[] { 3, 3, 3 }, new byte[] { 2, 2, 2 } };
    final TableName tableName4 = TableName.valueOf(tableName.getNameAsString() + "_4");
    try {
      createTableWithDefaultConf(tableName4, splitKeys);
      fail("Should not be able to create this table because of " + "duplicate split keys");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  private void verifyRoundRobinDistribution(List<HRegionLocation> regions, int expectedRegions) {
    int numRS = TEST_UTIL.getMiniHBaseCluster().getNumLiveRegionServers();

    Map<ServerName, List<RegionInfo>> server2Regions = new HashMap<>();
    regions.stream().forEach((loc) -> {
      ServerName server = loc.getServerName();
      server2Regions.computeIfAbsent(server, (s) -> new ArrayList<>()).add(loc.getRegion());
    });
    if (numRS >= 2) {
      // Ignore the master region server,
      // which contains less regions by intention.
      numRS--;
    }
    float average = (float) expectedRegions / numRS;
    int min = (int) Math.floor(average);
    int max = (int) Math.ceil(average);
    server2Regions.values().forEach((regionList) -> {
      assertTrue(regionList.size() == min || regionList.size() == max);
    });
  }

  @Test
  public void testCreateTableWithOnlyEmptyStartRow() throws Exception {
    byte[][] splitKeys = new byte[1][];
    splitKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
    try {
      createTableWithDefaultConf(tableName, splitKeys);
      fail("Test case should fail as empty split key is passed.");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testCreateTableWithEmptyRowInTheSplitKeys() throws Exception {
    byte[][] splitKeys = new byte[3][];
    splitKeys[0] = "region1".getBytes();
    splitKeys[1] = HConstants.EMPTY_BYTE_ARRAY;
    splitKeys[2] = "region2".getBytes();
    try {
      createTableWithDefaultConf(tableName, splitKeys);
      fail("Test case should fail as empty split key is passed.");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    createTableWithDefaultConf(tableName);
    assertTrue(admin.tableExists(tableName).get());
    TEST_UTIL.getAdmin().disableTable(tableName);
    admin.deleteTable(tableName).join();
    assertFalse(admin.tableExists(tableName).get());
  }

  @Test
  public void testTruncateTable() throws Exception {
    testTruncateTable(tableName, false);
  }

  @Test
  public void testTruncateTablePreservingSplits() throws Exception {
    testTruncateTable(tableName, true);
  }

  private void testTruncateTable(final TableName tableName, boolean preserveSplits)
      throws Exception {
    byte[][] splitKeys = new byte[2][];
    splitKeys[0] = Bytes.toBytes(4);
    splitKeys[1] = Bytes.toBytes(8);

    // Create & Fill the table
    createTableWithDefaultConf(tableName, splitKeys);
    AsyncTable<?> table = ASYNC_CONN.getTable(tableName);
    int expectedRows = 10;
    for (int i = 0; i < expectedRows; i++) {
      byte[] data = Bytes.toBytes(String.valueOf(i));
      Put put = new Put(data);
      put.addColumn(FAMILY, null, data);
      table.put(put).join();
    }
    assertEquals(10, table.scanAll(new Scan()).get().size());
    assertEquals(3, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());

    // Truncate & Verify
    admin.disableTable(tableName).join();
    admin.truncateTable(tableName, preserveSplits).join();
    assertEquals(0, table.scanAll(new Scan()).get().size());
    if (preserveSplits) {
      assertEquals(3, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());
    } else {
      assertEquals(1, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());
    }
  }

  @Test
  public void testCloneTableSchema() throws Exception {
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    testCloneTableSchema(tableName, newTableName, false);
  }

  @Test
  public void testCloneTableSchemaPreservingSplits() throws Exception {
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    testCloneTableSchema(tableName, newTableName, true);
  }

  private void testCloneTableSchema(final TableName tableName,
      final TableName newTableName, boolean preserveSplits) throws Exception {
    byte[][] splitKeys = new byte[2][];
    splitKeys[0] = Bytes.toBytes(4);
    splitKeys[1] = Bytes.toBytes(8);
    int NUM_FAMILYS = 2;
    int NUM_REGIONS = 3;
    int BLOCK_SIZE = 1024;
    int TTL = 86400;
    boolean BLOCK_CACHE = false;

    // Create the table
    TableDescriptor tableDesc = TableDescriptorBuilder
        .newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0))
        .setColumnFamily(ColumnFamilyDescriptorBuilder
            .newBuilder(FAMILY_1)
            .setBlocksize(BLOCK_SIZE)
            .setBlockCacheEnabled(BLOCK_CACHE)
            .setTimeToLive(TTL)
            .build()).build();
    admin.createTable(tableDesc, splitKeys).join();

    assertEquals(NUM_REGIONS, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());
    assertTrue("Table should be created with splitKyes + 1 rows in META",
        admin.isTableAvailable(tableName, splitKeys).get());

    // Clone & Verify
    admin.cloneTableSchema(tableName, newTableName, preserveSplits).join();
    TableDescriptor newTableDesc = admin.getDescriptor(newTableName).get();

    assertEquals(NUM_FAMILYS, newTableDesc.getColumnFamilyCount());
    assertEquals(BLOCK_SIZE, newTableDesc.getColumnFamily(FAMILY_1).getBlocksize());
    assertEquals(BLOCK_CACHE, newTableDesc.getColumnFamily(FAMILY_1).isBlockCacheEnabled());
    assertEquals(TTL, newTableDesc.getColumnFamily(FAMILY_1).getTimeToLive());
    TEST_UTIL.verifyTableDescriptorIgnoreTableName(tableDesc, newTableDesc);

    if (preserveSplits) {
      assertEquals(NUM_REGIONS, TEST_UTIL.getHBaseCluster().getRegions(newTableName).size());
      assertTrue("New table should be created with splitKyes + 1 rows in META",
          admin.isTableAvailable(newTableName, splitKeys).get());
    } else {
      assertEquals(1, TEST_UTIL.getHBaseCluster().getRegions(newTableName).size());
    }
  }

  @Test
  public void testCloneTableSchemaWithNonExistentSourceTable() throws Exception {
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    // test for non-existent source table
    try {
      admin.cloneTableSchema(tableName, newTableName, false).join();
      fail("Should have failed when source table doesn't exist.");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TableNotFoundException);
    }
  }

  @Test
  public void testCloneTableSchemaWithExistentDestinationTable() throws Exception {
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    byte[] FAMILY_0 = Bytes.toBytes("cf0");
    TEST_UTIL.createTable(tableName, FAMILY_0);
    TEST_UTIL.createTable(newTableName, FAMILY_0);
    // test for existent destination table
    try {
      admin.cloneTableSchema(tableName, newTableName, false).join();
      fail("Should have failed when destination table exists.");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TableExistsException);
    }
  }

  @Test
  public void testIsTableAvailableWithInexistantTable() throws Exception {
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    // test for inexistant table
    assertFalse(admin.isTableAvailable(newTableName).get());
  }
}
