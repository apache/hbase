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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor;
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
 * ten minute timeout so they were split.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableAdminApi3 extends TestAsyncAdminBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncTableAdminApi3.class);

  @Test
  public void testTableExist() throws Exception {
    boolean exist;
    exist = admin.tableExists(tableName).get();
    assertFalse(exist);
    TEST_UTIL.createTable(tableName, FAMILY);
    exist = admin.tableExists(tableName).get();
    assertTrue(exist);
    exist = admin.tableExists(TableName.META_TABLE_NAME).get();
    assertTrue(exist);
    // meta table already exists
    exist = admin.tableExists(TableName.META_TABLE_NAME).get();
    assertTrue(exist);
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

    tableNames = new ArrayList<TableName>(tables.length + 1);
    tableDescs = admin.listTableDescriptors(tableNames).get();
    size = tableDescs.size();
    assertEquals(0, size);

    Collections.addAll(tableNames, tables);
    tableNames.add(TableName.META_TABLE_NAME);
    tableDescs = admin.listTableDescriptors(tableNames).get();
    size = tableDescs.size();
    assertEquals(tables.length + 1, size);
    for (int i = 0, j = 0; i < tables.length && j < size; i++, j++) {
      assertTrue("tableName should be equal in order",
          tableDescs.get(j).getTableName().equals(tables[i]));
    }
    assertTrue(tableDescs.get(size - 1).getTableName().equals(TableName.META_TABLE_NAME));

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
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    TableDescriptor desc = builder.build();
    admin.createTable(desc).join();
    ModifyableTableDescriptor modifyableDesc = ((ModifyableTableDescriptor) desc);
    TableDescriptor confirmedHtd = admin.getDescriptor(tableName).get();
    assertEquals(0, modifyableDesc.compareTo((ModifyableTableDescriptor) confirmedHtd));
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
    assertEquals(TableState.State.DISABLED, TestAsyncTableAdminApi.getStateFromMeta(tableName));

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
    assertEquals(TableState.State.ENABLED, TestAsyncTableAdminApi.getStateFromMeta(tableName));

    // Test that table is enabled
    try {
      table.get(get).get();
    } catch (Exception e) {
      ok = false;
    }
    assertTrue(ok);
    // meta table can not be disabled.
    try {
      admin.disableTable(TableName.META_TABLE_NAME).get();
      fail("meta table can not be disabled");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertThat(cause, instanceOf(DoNotRetryIOException.class));
    }
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
    assertEquals(TableState.State.DISABLED, TestAsyncTableAdminApi.getStateFromMeta(tableName1));
    assertEquals(TableState.State.DISABLED, TestAsyncTableAdminApi.getStateFromMeta(tableName2));

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
    assertEquals(TableState.State.ENABLED, TestAsyncTableAdminApi.getStateFromMeta(tableName1));
    assertEquals(TableState.State.ENABLED, TestAsyncTableAdminApi.getStateFromMeta(tableName2));
  }

  @Test
  public void testEnableTableRetainAssignment() throws Exception {
    byte[][] splitKeys = { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 }, new byte[] { 3, 3, 3 },
      new byte[] { 4, 4, 4 }, new byte[] { 5, 5, 5 }, new byte[] { 6, 6, 6 },
      new byte[] { 7, 7, 7 }, new byte[] { 8, 8, 8 }, new byte[] { 9, 9, 9 } };
    int expectedRegions = splitKeys.length + 1;
    createTableWithDefaultConf(tableName, splitKeys);

    AsyncTable<AdvancedScanResultConsumer> metaTable = ASYNC_CONN.getTable(META_TABLE_NAME);
    List<HRegionLocation> regions = AsyncMetaTableAccessor
      .getTableHRegionLocations(metaTable, tableName).get();
    assertEquals(
      "Tried to create " + expectedRegions + " regions " + "but only found " + regions.size(),
      expectedRegions, regions.size());

    // Disable table.
    admin.disableTable(tableName).join();
    // Enable table, use retain assignment to assign regions.
    admin.enableTable(tableName).join();

    List<HRegionLocation> regions2 = AsyncMetaTableAccessor
      .getTableHRegionLocations(metaTable, tableName).get();
    // Check the assignment.
    assertEquals(regions.size(), regions2.size());
    assertTrue(regions2.containsAll(regions));
  }

  @Test
  public void testIsTableEnabledAndDisabled() throws Exception {
    createTableWithDefaultConf(tableName);
    assertTrue(admin.isTableEnabled(tableName).get());
    assertFalse(admin.isTableDisabled(tableName).get());
    admin.disableTable(tableName).join();
    assertFalse(admin.isTableEnabled(tableName).get());
    assertTrue(admin.isTableDisabled(tableName).get());

    // meta table is always enabled
    assertTrue(admin.isTableEnabled(TableName.META_TABLE_NAME).get());
    assertFalse(admin.isTableDisabled(TableName.META_TABLE_NAME).get());
  }

  @Test
  public void testIsTableAvailable() throws Exception {
    createTableWithDefaultConf(tableName);
    TEST_UTIL.waitTableAvailable(tableName);
    assertTrue(admin.isTableAvailable(tableName).get());
    assertTrue(admin.isTableAvailable(TableName.META_TABLE_NAME).get());
  }
}
