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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous table admin operations.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableAdminApi extends TestAsyncAdminBase {

  @Test
  public void testTableExist() throws Exception {
    boolean exist;
    exist = admin.tableExists(tableName).get();
    assertEquals(false, exist);
    TEST_UTIL.createTable(tableName, FAMILY);
    exist = admin.tableExists(tableName).get();
    assertEquals(true, exist);
    exist = admin.tableExists(TableName.META_TABLE_NAME).get();
    assertEquals(true, exist);
  }

  @Test
  public void testListTables() throws Exception {
    int numTables = admin.listTableDescriptors().get().size();
    final TableName tableName1 = TableName.valueOf(tableName.getNameAsString() + "1");
    final TableName tableName2 = TableName.valueOf(tableName.getNameAsString() + "2");
    final TableName tableName3 = TableName.valueOf(tableName.getNameAsString() + "3");
    TableName[] tables = new TableName[] { tableName1, tableName2, tableName3 };
    for (int i = 0; i < tables.length; i++) {
      createTableWithDefaultConf(tables[i]);
    }

    List<TableDescriptor> tableDescs = admin.listTableDescriptors().get();
    int size = tableDescs.size();
    assertTrue(size >= tables.length);
    for (int i = 0; i < tables.length && i < size; i++) {
      boolean found = false;
      for (int j = 0; j < size; j++) {
        if (tableDescs.get(j).getTableName().equals(tables[i])) {
          found = true;
          break;
        }
      }
      assertTrue("Not found: " + tables[i], found);
    }

    List<TableName> tableNames = admin.listTableNames().get();
    size = tableNames.size();
    assertTrue(size == (numTables + tables.length));
    for (int i = 0; i < tables.length && i < size; i++) {
      boolean found = false;
      for (int j = 0; j < size; j++) {
        if (tableNames.get(j).equals(tables[i])) {
          found = true;
          break;
        }
      }
      assertTrue("Not found: " + tables[i], found);
    }

    for (int i = 0; i < tables.length; i++) {
      admin.disableTable(tables[i]).join();
      admin.deleteTable(tables[i]).join();
    }

    tableDescs = admin.listTableDescriptors(true).get();
    assertTrue("Not found system tables", tableDescs.size() > 0);
    tableNames = admin.listTableNames(true).get();
    assertTrue("Not found system tables", tableNames.size() > 0);
  }

  @Test
  public void testGetTableDescriptor() throws Exception {
    byte[][] families = { FAMILY, FAMILY_0, FAMILY_1 };
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    for (byte[] family : families) {
      builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    TableDescriptor desc = builder.build();
    admin.createTable(desc).join();
    ModifyableTableDescriptor modifyableDesc = ((ModifyableTableDescriptor) desc);
    TableDescriptor confirmedHtd = admin.getDescriptor(tableName).get();
    assertEquals(modifyableDesc.compareTo((ModifyableTableDescriptor) confirmedHtd), 0);
  }

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

  private TableState.State getStateFromMeta(TableName table) throws Exception {
    Optional<TableState> state = AsyncMetaTableAccessor
        .getTableState(ASYNC_CONN.getTable(TableName.META_TABLE_NAME), table).get();
    assertTrue(state.isPresent());
    return state.get().getState();
  }

  @Test
  public void testCreateTableNumberOfRegions() throws Exception {
    AsyncTable<AdvancedScanResultConsumer> metaTable = ASYNC_CONN.getTable(META_TABLE_NAME);

    createTableWithDefaultConf(tableName);
    List<HRegionLocation> regionLocations =
      AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName)).get();
    assertEquals("Table should have only 1 region", 1, regionLocations.size());

    final TableName tableName2 = TableName.valueOf(tableName.getNameAsString() + "_2");
    createTableWithDefaultConf(tableName2, new byte[][] { new byte[] { 42 } });
    regionLocations =
      AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName2)).get();
    assertEquals("Table should have only 2 region", 2, regionLocations.size());

    final TableName tableName3 = TableName.valueOf(tableName.getNameAsString() + "_3");
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName3);
    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build(), "a".getBytes(), "z".getBytes(), 3).join();
    regionLocations =
      AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName3)).get();
    assertEquals("Table should have only 3 region", 3, regionLocations.size());

    final TableName tableName4 = TableName.valueOf(tableName.getNameAsString() + "_4");
    builder = TableDescriptorBuilder.newBuilder(tableName4);
    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    try {
      admin.createTable(builder.build(), "a".getBytes(), "z".getBytes(), 2).join();
      fail("Should not be able to create a table with only 2 regions using this API.");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }

    final TableName tableName5 = TableName.valueOf(tableName.getNameAsString() + "_5");
    builder = TableDescriptorBuilder.newBuilder(tableName5);
    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build(), new byte[] { 1 }, new byte[] { 127 }, 16).join();
    regionLocations =
      AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName5)).get();
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
    List<HRegionLocation> regions =
      AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName)).get();
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
    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build(), startKey, endKey, expectedRegions).join();

    regions =
      AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName2)).get();
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
    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build(), startKey, endKey, expectedRegions).join();

    regions =
      AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName3)).get();
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

  private void verifyRoundRobinDistribution(List<HRegionLocation> regions, int expectedRegions)
      throws IOException {
    int numRS = ((ClusterConnection) TEST_UTIL.getConnection()).getCurrentNrHRS();

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
  public void testDisableAndEnableTable() throws Exception {
    createTableWithDefaultConf(tableName);
    AsyncTable<?> table = ASYNC_CONN.getTable(tableName);
    final byte[] row = Bytes.toBytes("row");
    final byte[] qualifier = Bytes.toBytes("qualifier");
    final byte[] value = Bytes.toBytes("value");
    Put put = new Put(row);
    put.addColumn(FAMILY, qualifier, value);
    table.put(put).join();
    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    table.get(get).get();

    this.admin.disableTable(tableName).join();
    assertTrue("Table must be disabled.", TEST_UTIL.getHBaseCluster().getMaster()
        .getTableStateManager().isTableState(tableName, TableState.State.DISABLED));
    assertEquals(TableState.State.DISABLED, getStateFromMeta(tableName));

    // Test that table is disabled
    get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    boolean ok = false;
    try {
      table.get(get).get();
    } catch (ExecutionException e) {
      ok = true;
    }
    ok = false;
    // verify that scan encounters correct exception
    try {
      table.scanAll(new Scan()).get();
    } catch (ExecutionException e) {
      ok = true;
    }
    assertTrue(ok);
    this.admin.enableTable(tableName).join();
    assertTrue("Table must be enabled.", TEST_UTIL.getHBaseCluster().getMaster()
        .getTableStateManager().isTableState(tableName, TableState.State.ENABLED));
    assertEquals(TableState.State.ENABLED, getStateFromMeta(tableName));

    // Test that table is enabled
    try {
      table.get(get).get();
    } catch (Exception e) {
      ok = false;
    }
    assertTrue(ok);
  }

  @Test
  public void testDisableAndEnableTables() throws Exception {
    final TableName tableName1 = TableName.valueOf(tableName.getNameAsString() + "1");
    final TableName tableName2 = TableName.valueOf(tableName.getNameAsString() + "2");
    createTableWithDefaultConf(tableName1);
    createTableWithDefaultConf(tableName2);
    AsyncTable<?> table1 = ASYNC_CONN.getTable(tableName1);
    AsyncTable<?> table2 = ASYNC_CONN.getTable(tableName1);

    final byte[] row = Bytes.toBytes("row");
    final byte[] qualifier = Bytes.toBytes("qualifier");
    final byte[] value = Bytes.toBytes("value");
    Put put = new Put(row);
    put.addColumn(FAMILY, qualifier, value);
    table1.put(put).join();
    table2.put(put).join();
    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    table1.get(get).get();
    table2.get(get).get();

    admin.listTableNames(Pattern.compile(tableName.getNameAsString() + ".*"), false).get()
        .forEach(t -> admin.disableTable(t).join());

    // Test that tables are disabled
    get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    boolean ok = false;
    try {
      table1.get(get).get();
    } catch (ExecutionException e) {
      ok = true;
    }
    assertTrue(ok);

    ok = false;
    try {
      table2.get(get).get();
    } catch (ExecutionException e) {
      ok = true;
    }
    assertTrue(ok);
    assertEquals(TableState.State.DISABLED, getStateFromMeta(tableName1));
    assertEquals(TableState.State.DISABLED, getStateFromMeta(tableName2));

    admin.listTableNames(Pattern.compile(tableName.getNameAsString() + ".*"), false).get()
        .forEach(t -> admin.enableTable(t).join());

    // Test that tables are enabled
    try {
      table1.get(get).get();
    } catch (Exception e) {
      ok = false;
    }
    try {
      table2.get(get).get();
    } catch (Exception e) {
      ok = false;
    }
    assertTrue(ok);
    assertEquals(TableState.State.ENABLED, getStateFromMeta(tableName1));
    assertEquals(TableState.State.ENABLED, getStateFromMeta(tableName2));
  }

  @Test
  public void testEnableTableRetainAssignment() throws Exception {
    byte[][] splitKeys = { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 }, new byte[] { 3, 3, 3 },
      new byte[] { 4, 4, 4 }, new byte[] { 5, 5, 5 }, new byte[] { 6, 6, 6 },
      new byte[] { 7, 7, 7 }, new byte[] { 8, 8, 8 }, new byte[] { 9, 9, 9 } };
    int expectedRegions = splitKeys.length + 1;
    createTableWithDefaultConf(tableName, splitKeys);

    AsyncTable<AdvancedScanResultConsumer> metaTable = ASYNC_CONN.getTable(META_TABLE_NAME);
    List<HRegionLocation> regions =
      AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName)).get();
    assertEquals(
      "Tried to create " + expectedRegions + " regions " + "but only found " + regions.size(),
      expectedRegions, regions.size());

    // Disable table.
    admin.disableTable(tableName).join();
    // Enable table, use retain assignment to assign regions.
    admin.enableTable(tableName).join();

    List<HRegionLocation> regions2 =
      AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName)).get();
    // Check the assignment.
    assertEquals(regions.size(), regions2.size());
    assertTrue(regions2.containsAll(regions));
  }

  @Test
  public void testDisableCatalogTable() throws Exception {
    try {
      this.admin.disableTable(TableName.META_TABLE_NAME).join();
      fail("Expected to throw ConstraintException");
    } catch (Exception e) {
    }
    // Before the fix for HBASE-6146, the below table creation was failing as the hbase:meta table
    // actually getting disabled by the disableTable() call.
    createTableWithDefaultConf(tableName);
  }

  @Test
  public void testAddColumnFamily() throws Exception {
    // Create a table with two families
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0));
    admin.createTable(builder.build()).join();
    admin.disableTable(tableName).join();
    // Verify the table descriptor
    verifyTableDescriptor(tableName, FAMILY_0);

    // Modify the table removing one family and verify the descriptor
    admin.addColumnFamily(tableName, ColumnFamilyDescriptorBuilder.of(FAMILY_1)).join();
    verifyTableDescriptor(tableName, FAMILY_0, FAMILY_1);
  }

  @Test
  public void testAddSameColumnFamilyTwice() throws Exception {
    // Create a table with one families
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0));
    admin.createTable(builder.build()).join();
    admin.disableTable(tableName).join();
    // Verify the table descriptor
    verifyTableDescriptor(tableName, FAMILY_0);

    // Modify the table removing one family and verify the descriptor
    admin.addColumnFamily(tableName, ColumnFamilyDescriptorBuilder.of(FAMILY_1)).join();
    verifyTableDescriptor(tableName, FAMILY_0, FAMILY_1);

    try {
      // Add same column family again - expect failure
      this.admin.addColumnFamily(tableName, ColumnFamilyDescriptorBuilder.of(FAMILY_1)).join();
      Assert.fail("Delete a non-exist column family should fail");
    } catch (Exception e) {
      // Expected.
    }
  }

  @Test
  public void testModifyColumnFamily() throws Exception {
    TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of(FAMILY_0);
    int blockSize = cfd.getBlocksize();
    admin.createTable(tdBuilder.addColumnFamily(cfd).build()).join();
    admin.disableTable(tableName).join();
    // Verify the table descriptor
    verifyTableDescriptor(tableName, FAMILY_0);

    int newBlockSize = 2 * blockSize;
    cfd = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_0).setBlocksize(newBlockSize).build();
    // Modify colymn family
    admin.modifyColumnFamily(tableName, cfd).join();

    TableDescriptor htd = admin.getDescriptor(tableName).get();
    ColumnFamilyDescriptor hcfd = htd.getColumnFamily(FAMILY_0);
    assertTrue(hcfd.getBlocksize() == newBlockSize);
  }

  @Test
  public void testModifyNonExistingColumnFamily() throws Exception {
    TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of(FAMILY_0);
    int blockSize = cfd.getBlocksize();
    admin.createTable(tdBuilder.addColumnFamily(cfd).build()).join();
    admin.disableTable(tableName).join();
    // Verify the table descriptor
    verifyTableDescriptor(tableName, FAMILY_0);

    int newBlockSize = 2 * blockSize;
    cfd = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_1).setBlocksize(newBlockSize).build();

    // Modify a column family that is not in the table.
    try {
      admin.modifyColumnFamily(tableName, cfd).join();
      Assert.fail("Modify a non-exist column family should fail");
    } catch (Exception e) {
      // Expected.
    }
  }

  @Test
  public void testDeleteColumnFamily() throws Exception {
    // Create a table with two families
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0))
        .addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_1));
    admin.createTable(builder.build()).join();
    admin.disableTable(tableName).join();
    // Verify the table descriptor
    verifyTableDescriptor(tableName, FAMILY_0, FAMILY_1);

    // Modify the table removing one family and verify the descriptor
    admin.deleteColumnFamily(tableName, FAMILY_1).join();
    verifyTableDescriptor(tableName, FAMILY_0);
  }

  @Test
  public void testDeleteSameColumnFamilyTwice() throws Exception {
    // Create a table with two families
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0))
        .addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_1));
    admin.createTable(builder.build()).join();
    admin.disableTable(tableName).join();
    // Verify the table descriptor
    verifyTableDescriptor(tableName, FAMILY_0, FAMILY_1);

    // Modify the table removing one family and verify the descriptor
    admin.deleteColumnFamily(tableName, FAMILY_1).join();
    verifyTableDescriptor(tableName, FAMILY_0);

    try {
      // Delete again - expect failure
      admin.deleteColumnFamily(tableName, FAMILY_1).join();
      Assert.fail("Delete a non-exist column family should fail");
    } catch (Exception e) {
      // Expected.
    }
  }

  private void verifyTableDescriptor(final TableName tableName, final byte[]... families)
      throws Exception {
    // Verify descriptor from master
    TableDescriptor htd = admin.getDescriptor(tableName).get();
    verifyTableDescriptor(htd, tableName, families);

    // Verify descriptor from HDFS
    MasterFileSystem mfs = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    Path tableDir = FSUtils.getTableDir(mfs.getRootDir(), tableName);
    TableDescriptor td = FSTableDescriptors.getTableDescriptorFromFs(mfs.getFileSystem(), tableDir);
    verifyTableDescriptor(td, tableName, families);
  }

  private void verifyTableDescriptor(final TableDescriptor htd, final TableName tableName,
      final byte[]... families) {
    Set<byte[]> htdFamilies = htd.getColumnFamilyNames();
    assertEquals(tableName, htd.getTableName());
    assertEquals(families.length, htdFamilies.size());
    for (byte[] familyName : families) {
      assertTrue("Expected family " + Bytes.toString(familyName), htdFamilies.contains(familyName));
    }
  }

  @Test
  public void testIsTableEnabledAndDisabled() throws Exception {
    createTableWithDefaultConf(tableName);
    assertTrue(admin.isTableEnabled(tableName).get());
    assertFalse(admin.isTableDisabled(tableName).get());
    admin.disableTable(tableName).join();
    assertFalse(admin.isTableEnabled(tableName).get());
    assertTrue(admin.isTableDisabled(tableName).get());
  }

  @Test
  public void testTableAvailableWithRandomSplitKeys() throws Exception {
    createTableWithDefaultConf(tableName);
    byte[][] splitKeys = new byte[1][];
    splitKeys = new byte[][] { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 } };
    boolean tableAvailable = admin.isTableAvailable(tableName, splitKeys).get();
    assertFalse("Table should be created with 1 row in META", tableAvailable);
  }

  @Test
  public void testCompactionTimestamps() throws Exception {
    createTableWithDefaultConf(tableName);
    AsyncTable<?> table = ASYNC_CONN.getTable(tableName);
    Optional<Long> ts = admin.getLastMajorCompactionTimestamp(tableName).get();
    assertFalse(ts.isPresent());
    Put p = new Put(Bytes.toBytes("row1"));
    p.addColumn(FAMILY, Bytes.toBytes("q"), Bytes.toBytes("v"));
    table.put(p).join();
    ts = admin.getLastMajorCompactionTimestamp(tableName).get();
    // no files written -> no data
    assertFalse(ts.isPresent());

    admin.flush(tableName).join();
    ts = admin.getLastMajorCompactionTimestamp(tableName).get();
    // still 0, we flushed a file, but no major compaction happened
    assertFalse(ts.isPresent());

    byte[] regionName = ASYNC_CONN.getRegionLocator(tableName)
        .getRegionLocation(Bytes.toBytes("row1")).get().getRegion().getRegionName();
    Optional<Long> ts1 = admin.getLastMajorCompactionTimestampForRegion(regionName).get();
    assertFalse(ts1.isPresent());
    p = new Put(Bytes.toBytes("row2"));
    p.addColumn(FAMILY, Bytes.toBytes("q"), Bytes.toBytes("v"));
    table.put(p).join();
    admin.flush(tableName).join();
    ts1 = admin.getLastMajorCompactionTimestamp(tableName).get();
    // make sure the region API returns the same value, as the old file is still around
    assertFalse(ts1.isPresent());

    for (int i = 0; i < 3; i++) {
      table.put(p).join();
      admin.flush(tableName).join();
    }
    admin.majorCompact(tableName).join();
    long curt = System.currentTimeMillis();
    long waitTime = 10000;
    long endt = curt + waitTime;
    CompactionState state = admin.getCompactionState(tableName).get();
    LOG.info("Current compaction state 1 is " + state);
    while (state == CompactionState.NONE && curt < endt) {
      Thread.sleep(100);
      state = admin.getCompactionState(tableName).get();
      curt = System.currentTimeMillis();
      LOG.info("Current compaction state 2 is " + state);
    }
    // Now, should have the right compaction state, let's wait until the compaction is done
    if (state == CompactionState.MAJOR) {
      state = admin.getCompactionState(tableName).get();
      LOG.info("Current compaction state 3 is " + state);
      while (state != CompactionState.NONE && curt < endt) {
        Thread.sleep(10);
        state = admin.getCompactionState(tableName).get();
        LOG.info("Current compaction state 4 is " + state);
      }
    }
    // Sleep to wait region server report
    Thread
        .sleep(TEST_UTIL.getConfiguration().getInt("hbase.regionserver.msginterval", 3 * 1000) * 2);

    ts = admin.getLastMajorCompactionTimestamp(tableName).get();
    // after a compaction our earliest timestamp will have progressed forward
    assertTrue(ts.isPresent());
    assertTrue(ts.get() > 0);
    // region api still the same
    ts1 = admin.getLastMajorCompactionTimestampForRegion(regionName).get();
    assertTrue(ts1.isPresent());
    assertEquals(ts.get(), ts1.get());
    table.put(p).join();
    admin.flush(tableName).join();
    ts = admin.getLastMajorCompactionTimestamp(tableName).join();
    assertTrue(ts.isPresent());
    assertEquals(ts.get(), ts1.get());
    ts1 = admin.getLastMajorCompactionTimestampForRegion(regionName).get();
    assertTrue(ts1.isPresent());
    assertEquals(ts.get(), ts1.get());
  }
}
