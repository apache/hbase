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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ LargeTests.class, ClientTests.class })
public class TestAdmin extends TestAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestAdmin.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAdmin.class);

  @Test
  public void testCreateTable() throws IOException {
    List<TableDescriptor> tables = ADMIN.listTableDescriptors();
    int numTables = tables.size();
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    tables = ADMIN.listTableDescriptors();
    assertEquals(numTables + 1, tables.size());
    assertTrue("Table must be enabled.", TEST_UTIL.getHBaseCluster().getMaster()
      .getTableStateManager().isTableState(tableName, TableState.State.ENABLED));
    assertEquals(TableState.State.ENABLED, getStateFromMeta(tableName));
  }

  @Test
  public void testTruncateTable() throws IOException {
    testTruncateTable(TableName.valueOf(name.getMethodName()), false);
  }

  @Test
  public void testTruncateTablePreservingSplits() throws IOException {
    testTruncateTable(TableName.valueOf(name.getMethodName()), true);
  }

  private void testTruncateTable(final TableName tableName, boolean preserveSplits)
      throws IOException {
    byte[][] splitKeys = new byte[2][];
    splitKeys[0] = Bytes.toBytes(4);
    splitKeys[1] = Bytes.toBytes(8);

    // Create & Fill the table
    Table table = TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY, splitKeys);
    try {
      TEST_UTIL.loadNumericRows(table, HConstants.CATALOG_FAMILY, 0, 10);
      assertEquals(10, TEST_UTIL.countRows(table));
    } finally {
      table.close();
    }
    assertEquals(3, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());

    // Truncate & Verify
    ADMIN.disableTable(tableName);
    ADMIN.truncateTable(tableName, preserveSplits);
    table = TEST_UTIL.getConnection().getTable(tableName);
    try {
      assertEquals(0, TEST_UTIL.countRows(table));
    } finally {
      table.close();
    }
    if (preserveSplits) {
      assertEquals(3, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());
    } else {
      assertEquals(1, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());
    }
  }

  @Test
  public void testCreateTableNumberOfRegions() throws IOException, InterruptedException {
    TableName table = TableName.valueOf(name.getMethodName());
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY);
    ADMIN.createTable(TableDescriptorBuilder.newBuilder(table).setColumnFamily(cfd).build());
    List<HRegionLocation> regions;
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(table)) {
      regions = l.getAllRegionLocations();
      assertEquals("Table should have only 1 region", 1, regions.size());
    }

    TableName table2 = TableName.valueOf(table.getNameAsString() + "_2");
    ADMIN.createTable(TableDescriptorBuilder.newBuilder(table2).setColumnFamily(cfd).build(),
      new byte[][] { new byte[] { 42 } });
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(table2)) {
      regions = l.getAllRegionLocations();
      assertEquals("Table should have only 2 region", 2, regions.size());
    }

    TableName table3 = TableName.valueOf(table.getNameAsString() + "_3");
    ADMIN.createTable(TableDescriptorBuilder.newBuilder(table3).setColumnFamily(cfd).build(),
      Bytes.toBytes("a"), Bytes.toBytes("z"), 3);
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(table3)) {
      regions = l.getAllRegionLocations();
      assertEquals("Table should have only 3 region", 3, regions.size());
    }

    TableName table4 = TableName.valueOf(table.getNameAsString() + "_4");
    try {
      ADMIN.createTable(TableDescriptorBuilder.newBuilder(table4).setColumnFamily(cfd).build(),
        Bytes.toBytes("a"), Bytes.toBytes("z"), 2);
      fail("Should not be able to create a table with only 2 regions using this API.");
    } catch (IllegalArgumentException eae) {
      // Expected
    }

    TableName table5 = TableName.valueOf(table.getNameAsString() + "_5");
    ADMIN.createTable(TableDescriptorBuilder.newBuilder(table5).setColumnFamily(cfd).build(),
      new byte[] { 1 }, new byte[] { 127 }, 16);
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(table5)) {
      regions = l.getAllRegionLocations();
      assertEquals("Table should have 16 region", 16, regions.size());
    }
  }

  @Test
  public void testCreateTableWithRegions() throws IOException, InterruptedException {
    TableName table = TableName.valueOf(name.getMethodName());
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY);
    byte[][] splitKeys = { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 }, new byte[] { 3, 3, 3 },
      new byte[] { 4, 4, 4 }, new byte[] { 5, 5, 5 }, new byte[] { 6, 6, 6 },
      new byte[] { 7, 7, 7 }, new byte[] { 8, 8, 8 }, new byte[] { 9, 9, 9 }, };
    int expectedRegions = splitKeys.length + 1;

    ADMIN.createTable(TableDescriptorBuilder.newBuilder(table).setColumnFamily(cfd).build(),
      splitKeys);

    boolean tableAvailable = ADMIN.isTableAvailable(table);
    assertTrue("Table should be created with splitKyes + 1 rows in META", tableAvailable);

    List<HRegionLocation> regions;
    Iterator<HRegionLocation> hris;
    RegionInfo hri;
    ClusterConnection conn = (ClusterConnection) TEST_UTIL.getConnection();
    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(table)) {
      regions = l.getAllRegionLocations();

      assertEquals(
        "Tried to create " + expectedRegions + " regions " + "but only found " + regions.size(),
        expectedRegions, regions.size());
      System.err.println("Found " + regions.size() + " regions");

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

      verifyRoundRobinDistribution(l, expectedRegions);
    }

    // Now test using start/end with a number of regions

    // Use 80 bit numbers to make sure we aren't limited
    byte[] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte[] endKey = { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };

    // Splitting into 10 regions, we expect (null,1) ... (9, null)
    // with (1,2) (2,3) (3,4) (4,5) (5,6) (6,7) (7,8) (8,9) in the middle

    expectedRegions = 10;

    TableName table2 = TableName.valueOf(table.getNameAsString() + "_2");
    ADMIN.createTable(TableDescriptorBuilder.newBuilder(table2).setColumnFamily(cfd).build(),
      startKey, endKey, expectedRegions);

    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(table2)) {
      regions = l.getAllRegionLocations();
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

      verifyRoundRobinDistribution(l, expectedRegions);
    }

    // Try once more with something that divides into something infinite

    startKey = new byte[] { 0, 0, 0, 0, 0, 0 };
    endKey = new byte[] { 1, 0, 0, 0, 0, 0 };

    expectedRegions = 5;

    TableName table3 = TableName.valueOf(table.getNameAsString() + "_3");
    ADMIN.createTable(TableDescriptorBuilder.newBuilder(table3).setColumnFamily(cfd).build(),
      startKey, endKey, expectedRegions);

    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(table3)) {
      regions = l.getAllRegionLocations();
      assertEquals(
        "Tried to create " + expectedRegions + " regions " + "but only found " + regions.size(),
        expectedRegions, regions.size());
      System.err.println("Found " + regions.size() + " regions");

      verifyRoundRobinDistribution(l, expectedRegions);
    }

    // Try an invalid case where there are duplicate split keys
    splitKeys = new byte[][] { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 },
      new byte[] { 3, 3, 3 }, new byte[] { 2, 2, 2 } };

    TableName table4 = TableName.valueOf(table.getNameAsString() + "_4");
    try {
      ADMIN.createTable(TableDescriptorBuilder.newBuilder(table4).setColumnFamily(cfd).build(),
        splitKeys);
      assertTrue("Should not be able to create this table because of " + "duplicate split keys",
        false);
    } catch (IllegalArgumentException iae) {
      // Expected
    }
  }

  @Test
  public void testCreateTableWithOnlyEmptyStartRow() throws IOException {
    final byte[] tableName = Bytes.toBytes(name.getMethodName());
    byte[][] splitKeys = new byte[1][];
    splitKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("col")).build();
    try {
      ADMIN.createTable(desc, splitKeys);
      fail("Test case should fail as empty split key is passed.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testCreateTableWithEmptyRowInTheSplitKeys() throws IOException {
    final byte[] tableName = Bytes.toBytes(name.getMethodName());
    byte[][] splitKeys = new byte[3][];
    splitKeys[0] = Bytes.toBytes("region1");
    splitKeys[1] = HConstants.EMPTY_BYTE_ARRAY;
    splitKeys[2] = Bytes.toBytes("region2");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("col")).build();
    try {
      ADMIN.createTable(desc, splitKeys);
      fail("Test case should fail as empty split key is passed.");
    } catch (IllegalArgumentException e) {
      LOG.info("Expected ", e);
    }
  }

  private void verifyRoundRobinDistribution(RegionLocator regionLocator, int expectedRegions)
      throws IOException {
    int numRS = TEST_UTIL.getMiniHBaseCluster().getNumLiveRegionServers();
    List<HRegionLocation> regions = regionLocator.getAllRegionLocations();
    Map<ServerName, List<RegionInfo>> server2Regions = new HashMap<>();
    for (HRegionLocation loc : regions) {
      ServerName server = loc.getServerName();
      List<RegionInfo> regs = server2Regions.get(server);
      if (regs == null) {
        regs = new ArrayList<>();
        server2Regions.put(server, regs);
      }
      regs.add(loc.getRegion());
    }
    boolean tablesOnMaster = LoadBalancer.isTablesOnMaster(TEST_UTIL.getConfiguration());
    if (tablesOnMaster) {
      // Ignore the master region server,
      // which contains less regions by intention.
      numRS--;
    }
    float average = (float) expectedRegions / numRS;
    int min = (int) Math.floor(average);
    int max = (int) Math.ceil(average);
    for (List<RegionInfo> regionList : server2Regions.values()) {
      assertTrue(
        "numRS=" + numRS + ", min=" + min + ", max=" + max + ", size=" + regionList.size() +
          ", tablesOnMaster=" + tablesOnMaster,
        regionList.size() == min || regionList.size() == max);
    }
  }

  @Test
  public void testCloneTableSchema() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    testCloneTableSchema(tableName, newTableName, false);
  }

  @Test
  public void testCloneTableSchemaPreservingSplits() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    testCloneTableSchema(tableName, newTableName, true);
  }

  private void testCloneTableSchema(final TableName tableName, final TableName newTableName,
      boolean preserveSplits) throws Exception {
    byte[] FAMILY_0 = Bytes.toBytes("cf0");
    byte[] FAMILY_1 = Bytes.toBytes("cf1");
    byte[][] splitKeys = new byte[2][];
    splitKeys[0] = Bytes.toBytes(4);
    splitKeys[1] = Bytes.toBytes(8);
    int NUM_FAMILYS = 2;
    int NUM_REGIONS = 3;
    int BLOCK_SIZE = 1024;
    int TTL = 86400;
    boolean BLOCK_CACHE = false;

    // Create the table
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_1).setBlocksize(BLOCK_SIZE)
        .setBlockCacheEnabled(BLOCK_CACHE).setTimeToLive(TTL).build())
      .build();
    ADMIN.createTable(tableDesc, splitKeys);

    assertEquals(NUM_REGIONS, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());
    assertTrue("Table should be created with splitKyes + 1 rows in META",
      ADMIN.isTableAvailable(tableName));

    // clone & Verify
    ADMIN.cloneTableSchema(tableName, newTableName, preserveSplits);
    TableDescriptor newTableDesc = ADMIN.getDescriptor(newTableName);

    assertEquals(NUM_FAMILYS, newTableDesc.getColumnFamilyCount());
    assertEquals(BLOCK_SIZE, newTableDesc.getColumnFamily(FAMILY_1).getBlocksize());
    assertEquals(BLOCK_CACHE, newTableDesc.getColumnFamily(FAMILY_1).isBlockCacheEnabled());
    assertEquals(TTL, newTableDesc.getColumnFamily(FAMILY_1).getTimeToLive());
    TEST_UTIL.verifyTableDescriptorIgnoreTableName(tableDesc, newTableDesc);

    if (preserveSplits) {
      assertEquals(NUM_REGIONS, TEST_UTIL.getHBaseCluster().getRegions(newTableName).size());
      assertTrue("New table should be created with splitKyes + 1 rows in META",
        ADMIN.isTableAvailable(newTableName));
    } else {
      assertEquals(1, TEST_UTIL.getHBaseCluster().getRegions(newTableName).size());
    }
  }

  @Test
  public void testCloneTableSchemaWithNonExistentSourceTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    // test for non-existent source table
    try {
      ADMIN.cloneTableSchema(tableName, newTableName, false);
      fail("Should have failed to create a new table by cloning non-existent source table.");
    } catch (TableNotFoundException ex) {
      // expected
    }
  }

  @Test
  public void testCloneTableSchemaWithExistentDestinationTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    byte[] FAMILY_0 = Bytes.toBytes("cf0");
    TEST_UTIL.createTable(tableName, FAMILY_0);
    TEST_UTIL.createTable(newTableName, FAMILY_0);
    // test for existent destination table
    try {
      ADMIN.cloneTableSchema(tableName, newTableName, false);
      fail("Should have failed to create a existent table.");
    } catch (TableExistsException ex) {
      // expected
    }
  }

  @Test
  public void testModifyTableOnTableWithRegionReplicas() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("cf")))
      .setRegionReplication(5).build();

    ADMIN.createTable(desc);

    int maxFileSize = 10000000;
    TableDescriptor newDesc =
      TableDescriptorBuilder.newBuilder(desc).setMaxFileSize(maxFileSize).build();

    ADMIN.modifyTable(newDesc);
    TableDescriptor newTableDesc = ADMIN.getDescriptor(tableName);
    assertEquals(maxFileSize, newTableDesc.getMaxFileSize());
  }

  /**
   * Verify schema modification takes.
   */
  @Test
  public void testOnlineChangeTableSchema() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<TableDescriptor> tables = ADMIN.listTableDescriptors();
    int numTables = tables.size();
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    tables = ADMIN.listTableDescriptors();
    assertEquals(numTables + 1, tables.size());
    // FIRST, do htabledescriptor changes.
    TableDescriptor htd = ADMIN.getDescriptor(tableName);
    // Make a copy and assert copy is good.
    TableDescriptor copy = TableDescriptorBuilder.newBuilder(htd).build();
    assertEquals(htd, copy);
    String key = "anyoldkey";
    assertNull(htd.getValue(key));
    // Now amend the copy. Introduce differences.
    long newFlushSize = htd.getMemStoreFlushSize() / 2;
    if (newFlushSize <= 0) {
      newFlushSize = TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE / 2;
    }
    copy = TableDescriptorBuilder.newBuilder(copy).setMemStoreFlushSize(newFlushSize)
      .setValue(key, key).build();
    ADMIN.modifyTable(copy);
    TableDescriptor modifiedHtd = ADMIN.getDescriptor(tableName);
    assertNotEquals(htd, modifiedHtd);
    assertEquals(copy, modifiedHtd);
    assertEquals(newFlushSize, modifiedHtd.getMemStoreFlushSize());
    assertEquals(key, modifiedHtd.getValue(key));

    // Now work on column family changes.
    int countOfFamilies = modifiedHtd.getColumnFamilyCount();
    assertTrue(countOfFamilies > 0);
    ColumnFamilyDescriptor hcd = modifiedHtd.getColumnFamilies()[0];
    int maxversions = hcd.getMaxVersions();
    int newMaxVersions = maxversions + 1;
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(hcd).setMaxVersions(newMaxVersions).build();
    byte[] hcdName = hcd.getName();
    ADMIN.modifyColumnFamily(tableName, hcd);
    modifiedHtd = ADMIN.getDescriptor(tableName);
    ColumnFamilyDescriptor modifiedHcd = modifiedHtd.getColumnFamily(hcdName);
    assertEquals(newMaxVersions, modifiedHcd.getMaxVersions());

    // Try adding a column
    assertFalse(ADMIN.isTableDisabled(tableName));
    String xtracolName = "xtracol";
    ColumnFamilyDescriptor xtracol = ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes(xtracolName)).setValue(xtracolName, xtracolName).build();
    ADMIN.addColumnFamily(tableName, xtracol);
    modifiedHtd = ADMIN.getDescriptor(tableName);
    hcd = modifiedHtd.getColumnFamily(xtracol.getName());
    assertNotNull(hcd);
    assertEquals(xtracolName, Bytes.toString(hcd.getValue(Bytes.toBytes(xtracolName))));

    // Delete the just-added column.
    ADMIN.deleteColumnFamily(tableName, xtracol.getName());
    modifiedHtd = ADMIN.getDescriptor(tableName);
    hcd = modifiedHtd.getColumnFamily(xtracol.getName());
    assertNull(hcd);

    // Delete the table
    ADMIN.disableTable(tableName);
    ADMIN.deleteTable(tableName);
    ADMIN.listTableDescriptors();
    assertFalse(ADMIN.tableExists(tableName));
  }
}
