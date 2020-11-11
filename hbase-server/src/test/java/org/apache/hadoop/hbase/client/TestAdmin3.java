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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ LargeTests.class, ClientTests.class })
public class TestAdmin3 extends TestAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestAdmin3.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAdmin3.class);

  @Test
  public void testDisableAndEnableTable() throws IOException {
    final byte[] row = Bytes.toBytes("row");
    final byte[] qualifier = Bytes.toBytes("qualifier");
    final byte[] value = Bytes.toBytes("value");
    final TableName table = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(table, HConstants.CATALOG_FAMILY);
    Put put = new Put(row);
    put.addColumn(HConstants.CATALOG_FAMILY, qualifier, value);
    ht.put(put);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    ht.get(get);

    ADMIN.disableTable(ht.getName());
    assertTrue("Table must be disabled.", TEST_UTIL.getHBaseCluster().getMaster()
      .getTableStateManager().isTableState(ht.getName(), TableState.State.DISABLED));
    assertEquals(TableState.State.DISABLED, getStateFromMeta(table));

    // Test that table is disabled
    get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    boolean ok = false;
    try {
      ht.get(get);
    } catch (TableNotEnabledException e) {
      ok = true;
    }
    ok = false;
    // verify that scan encounters correct exception
    Scan scan = new Scan();
    try {
      ResultScanner scanner = ht.getScanner(scan);
      Result res = null;
      do {
        res = scanner.next();
      } while (res != null);
    } catch (TableNotEnabledException e) {
      ok = true;
    }
    assertTrue(ok);
    ADMIN.enableTable(table);
    assertTrue("Table must be enabled.", TEST_UTIL.getHBaseCluster().getMaster()
      .getTableStateManager().isTableState(ht.getName(), TableState.State.ENABLED));
    assertEquals(TableState.State.ENABLED, getStateFromMeta(table));

    // Test that table is enabled
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = false;
    }
    assertTrue(ok);
    ht.close();
  }

  @Test
  public void testDisableAndEnableTables() throws IOException {
    final byte[] row = Bytes.toBytes("row");
    final byte[] qualifier = Bytes.toBytes("qualifier");
    final byte[] value = Bytes.toBytes("value");
    final TableName table1 = TableName.valueOf(name.getMethodName() + "1");
    final TableName table2 = TableName.valueOf(name.getMethodName() + "2");
    Table ht1 = TEST_UTIL.createTable(table1, HConstants.CATALOG_FAMILY);
    Table ht2 = TEST_UTIL.createTable(table2, HConstants.CATALOG_FAMILY);
    Put put = new Put(row);
    put.addColumn(HConstants.CATALOG_FAMILY, qualifier, value);
    ht1.put(put);
    ht2.put(put);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    ht1.get(get);
    ht2.get(get);

    TableName[] tableNames = ADMIN.listTableNames(Pattern.compile("testDisableAndEnableTable.*"));
    for (TableName tableName : tableNames) {
      ADMIN.disableTable(tableName);
    }

    // Test that tables are disabled
    get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    boolean ok = false;
    try {
      ht1.get(get);
      ht2.get(get);
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException e) {
      ok = true;
    }

    assertEquals(TableState.State.DISABLED, getStateFromMeta(table1));
    assertEquals(TableState.State.DISABLED, getStateFromMeta(table2));

    assertTrue(ok);
    for (TableName tableName : tableNames) {
      ADMIN.enableTable(tableName);
    }

    // Test that tables are enabled
    try {
      ht1.get(get);
    } catch (IOException e) {
      ok = false;
    }
    try {
      ht2.get(get);
    } catch (IOException e) {
      ok = false;
    }
    assertTrue(ok);

    ht1.close();
    ht2.close();

    assertEquals(TableState.State.ENABLED, getStateFromMeta(table1));
    assertEquals(TableState.State.ENABLED, getStateFromMeta(table2));
  }

  /**
   * Test retain assignment on enableTable.
   */
  @Test
  public void testEnableTableRetainAssignment() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[][] splitKeys = { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 }, new byte[] { 3, 3, 3 },
      new byte[] { 4, 4, 4 }, new byte[] { 5, 5, 5 }, new byte[] { 6, 6, 6 },
      new byte[] { 7, 7, 7 }, new byte[] { 8, 8, 8 }, new byte[] { 9, 9, 9 } };
    int expectedRegions = splitKeys.length + 1;
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
    ADMIN.createTable(desc, splitKeys);

    try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      List<HRegionLocation> regions = l.getAllRegionLocations();

      assertEquals(
        "Tried to create " + expectedRegions + " regions " + "but only found " + regions.size(),
        expectedRegions, regions.size());
      // Disable table.
      ADMIN.disableTable(tableName);
      // Enable table, use retain assignment to assign regions.
      ADMIN.enableTable(tableName);
      List<HRegionLocation> regions2 = l.getAllRegionLocations();

      // Check the assignment.
      assertEquals(regions.size(), regions2.size());
      assertTrue(regions2.containsAll(regions));
    }
  }

  @Test
  public void testEnableDisableAddColumnDeleteColumn() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    while (!ADMIN.isTableEnabled(TableName.valueOf(name.getMethodName()))) {
      Thread.sleep(10);
    }
    ADMIN.disableTable(tableName);
    try {
      TEST_UTIL.getConnection().getTable(tableName);
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException e) {
      // expected
    }

    ADMIN.addColumnFamily(tableName, ColumnFamilyDescriptorBuilder.of("col2"));
    ADMIN.enableTable(tableName);
    try {
      ADMIN.deleteColumnFamily(tableName, Bytes.toBytes("col2"));
    } catch (TableNotDisabledException e) {
      LOG.info(e.toString(), e);
    }
    ADMIN.disableTable(tableName);
    ADMIN.deleteTable(tableName);
  }

  @Test
  public void testGetTableDescriptor() throws IOException {
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("fam1"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("fam2"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("fam3")).build();
    ADMIN.createTable(htd);
    Table table = TEST_UTIL.getConnection().getTable(htd.getTableName());
    TableDescriptor confirmedHtd = table.getDescriptor();
    assertEquals(0, TableDescriptor.COMPARATOR.compare(htd, confirmedHtd));
    MetaTableAccessor.fullScanMetaAndPrint(TEST_UTIL.getConnection());
    table.close();
  }

  /**
   * Verify schema change for read only table
   */
  @Test
  public void testReadOnlyTableModify() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();

    // Make table read only
    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(ADMIN.getDescriptor(tableName)).setReadOnly(true).build();
    ADMIN.modifyTable(htd);

    // try to modify the read only table now
    htd = TableDescriptorBuilder.newBuilder(ADMIN.getDescriptor(tableName))
      .setCompactionEnabled(false).build();
    ADMIN.modifyTable(htd);
    // Delete the table
    ADMIN.disableTable(tableName);
    ADMIN.deleteTable(tableName);
    assertFalse(ADMIN.tableExists(tableName));
  }

  @Test
  public void testDeleteLastColumnFamily() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    while (!ADMIN.isTableEnabled(TableName.valueOf(name.getMethodName()))) {
      Thread.sleep(10);
    }

    // test for enabled table
    try {
      ADMIN.deleteColumnFamily(tableName, HConstants.CATALOG_FAMILY);
      fail("Should have failed to delete the only column family of a table");
    } catch (InvalidFamilyOperationException ex) {
      // expected
    }

    // test for disabled table
    ADMIN.disableTable(tableName);

    try {
      ADMIN.deleteColumnFamily(tableName, HConstants.CATALOG_FAMILY);
      fail("Should have failed to delete the only column family of a table");
    } catch (InvalidFamilyOperationException ex) {
      // expected
    }

    ADMIN.deleteTable(tableName);
  }

  @Test
  public void testDeleteEditUnknownColumnFamilyAndOrTable() throws IOException {
    // Test we get exception if we try to
    final TableName nonexistentTable = TableName.valueOf("nonexistent");
    final byte[] nonexistentColumn = Bytes.toBytes("nonexistent");
    ColumnFamilyDescriptor nonexistentHcd = ColumnFamilyDescriptorBuilder.of(nonexistentColumn);
    Exception exception = null;
    try {
      ADMIN.addColumnFamily(nonexistentTable, nonexistentHcd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      ADMIN.deleteTable(nonexistentTable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      ADMIN.deleteColumnFamily(nonexistentTable, nonexistentColumn);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      ADMIN.disableTable(nonexistentTable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      ADMIN.enableTable(nonexistentTable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      ADMIN.modifyColumnFamily(nonexistentTable, nonexistentHcd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      TableDescriptor htd = TableDescriptorBuilder.newBuilder(nonexistentTable)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
      ADMIN.modifyTable(htd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    // Now make it so at least the table exists and then do tests against a
    // nonexistent column family -- see if we get right exceptions.
    final TableName tableName =
      TableName.valueOf(name.getMethodName() + System.currentTimeMillis());
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build();
    ADMIN.createTable(htd);
    try {
      exception = null;
      try {
        ADMIN.deleteColumnFamily(htd.getTableName(), nonexistentHcd.getName());
      } catch (IOException e) {
        exception = e;
      }
      assertTrue("found=" + exception.getClass().getName(),
        exception instanceof InvalidFamilyOperationException);

      exception = null;
      try {
        ADMIN.modifyColumnFamily(htd.getTableName(), nonexistentHcd);
      } catch (IOException e) {
        exception = e;
      }
      assertTrue("found=" + exception.getClass().getName(),
        exception instanceof InvalidFamilyOperationException);
    } finally {
      ADMIN.disableTable(tableName);
      ADMIN.deleteTable(tableName);
    }
  }
}
