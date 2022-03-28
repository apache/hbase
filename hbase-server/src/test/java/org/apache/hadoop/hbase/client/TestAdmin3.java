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

import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory.TRACKER_IMPL;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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
    //HBASE-26246 introduced persist of store file tracker into table descriptor
    htd = TableDescriptorBuilder.newBuilder(htd).setValue(TRACKER_IMPL,
      StoreFileTrackerFactory.getStoreFileTrackerName(TEST_UTIL.getConfiguration())).
      build();
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
      TableName.valueOf(name.getMethodName() + EnvironmentEdgeManager.currentTime());
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

  private static final String SRC_IMPL = "hbase.store.file-tracker.migration.src.impl";

  private static final String DST_IMPL = "hbase.store.file-tracker.migration.dst.impl";

  private void verifyModifyTableResult(TableName tableName, byte[] family, byte[] qual, byte[] row,
    byte[] value, String sft) throws IOException {
    TableDescriptor td = ADMIN.getDescriptor(tableName);
    assertEquals(sft, td.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
    // no migration related configs
    assertNull(td.getValue(SRC_IMPL));
    assertNull(td.getValue(DST_IMPL));
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      assertArrayEquals(value, table.get(new Get(row)).getValue(family, qual));
    }
  }

  @Test
  public void testModifyTableStoreFileTracker() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] family = Bytes.toBytes("info");
    byte[] qual = Bytes.toBytes("q");
    byte[] row = Bytes.toBytes(0);
    byte[] value = Bytes.toBytes(1);
    try (Table table = TEST_UTIL.createTable(tableName, family)) {
      table.put(new Put(row).addColumn(family, qual, value));
    }
    // change to FILE
    ADMIN.modifyTableStoreFileTracker(tableName, StoreFileTrackerFactory.Trackers.FILE.name());
    verifyModifyTableResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to FILE again, should have no effect
    ADMIN.modifyTableStoreFileTracker(tableName, StoreFileTrackerFactory.Trackers.FILE.name());
    verifyModifyTableResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to MIGRATION, and then to FILE
    ADMIN.modifyTable(TableDescriptorBuilder.newBuilder(ADMIN.getDescriptor(tableName))
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(SRC_IMPL,
        StoreFileTrackerFactory.Trackers.FILE.name())
      .setValue(DST_IMPL,
        StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .build());
    ADMIN.modifyTableStoreFileTracker(tableName, StoreFileTrackerFactory.Trackers.FILE.name());
    verifyModifyTableResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to MIGRATION, and then to DEFAULT
    ADMIN.modifyTable(TableDescriptorBuilder.newBuilder(ADMIN.getDescriptor(tableName))
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(SRC_IMPL,
        StoreFileTrackerFactory.Trackers.FILE.name())
      .setValue(DST_IMPL,
        StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .build());
    ADMIN.modifyTableStoreFileTracker(tableName, StoreFileTrackerFactory.Trackers.DEFAULT.name());
    verifyModifyTableResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.DEFAULT.name());
  }

  private void verifyModifyColumnFamilyResult(TableName tableName, byte[] family, byte[] qual,
    byte[] row, byte[] value, String sft) throws IOException {
    TableDescriptor td = ADMIN.getDescriptor(tableName);
    ColumnFamilyDescriptor cfd = td.getColumnFamily(family);
    assertEquals(sft, cfd.getConfigurationValue(StoreFileTrackerFactory.TRACKER_IMPL));
    // no migration related configs
    assertNull(cfd.getConfigurationValue(SRC_IMPL));
    assertNull(cfd.getConfigurationValue(DST_IMPL));
    assertNull(cfd.getValue(SRC_IMPL));
    assertNull(cfd.getValue(DST_IMPL));
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      assertArrayEquals(value, table.get(new Get(row)).getValue(family, qual));
    }
  }

  @Test
  public void testModifyColumnFamilyStoreFileTracker() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] family = Bytes.toBytes("info");
    byte[] qual = Bytes.toBytes("q");
    byte[] row = Bytes.toBytes(0);
    byte[] value = Bytes.toBytes(1);
    try (Table table = TEST_UTIL.createTable(tableName, family)) {
      table.put(new Put(row).addColumn(family, qual, value));
    }
    // change to FILE
    ADMIN.modifyColumnFamilyStoreFileTracker(tableName, family,
      StoreFileTrackerFactory.Trackers.FILE.name());
    verifyModifyColumnFamilyResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to FILE again, should have no effect
    ADMIN.modifyColumnFamilyStoreFileTracker(tableName, family,
      StoreFileTrackerFactory.Trackers.FILE.name());
    verifyModifyColumnFamilyResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to MIGRATION, and then to FILE
    TableDescriptor current = ADMIN.getDescriptor(tableName);
    ADMIN.modifyTable(TableDescriptorBuilder.newBuilder(current)
      .modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(current.getColumnFamily(family))
        .setConfiguration(StoreFileTrackerFactory.TRACKER_IMPL,
          StoreFileTrackerFactory.Trackers.MIGRATION.name())
        .setConfiguration(SRC_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
        .setConfiguration(DST_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name()).build())
      .build());
    ADMIN.modifyColumnFamilyStoreFileTracker(tableName, family,
      StoreFileTrackerFactory.Trackers.FILE.name());
    verifyModifyColumnFamilyResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to MIGRATION, and then to DEFAULT
    current = ADMIN.getDescriptor(tableName);
    ADMIN.modifyTable(TableDescriptorBuilder.newBuilder(current)
      .modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(current.getColumnFamily(family))
        .setConfiguration(StoreFileTrackerFactory.TRACKER_IMPL,
          StoreFileTrackerFactory.Trackers.MIGRATION.name())
        .setConfiguration(SRC_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
        .setConfiguration(DST_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name()).build())
      .build());
    ADMIN.modifyColumnFamilyStoreFileTracker(tableName, family,
      StoreFileTrackerFactory.Trackers.DEFAULT.name());
    verifyModifyColumnFamilyResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.DEFAULT.name());
  }

  @Test
  public void testModifyStoreFileTrackerError() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] family = Bytes.toBytes("info");
    TEST_UTIL.createTable(tableName, family).close();

    // table not exists
    assertThrows(TableNotFoundException.class,
      () -> ADMIN.modifyTableStoreFileTracker(TableName.valueOf("whatever"),
        StoreFileTrackerFactory.Trackers.FILE.name()));
    // family not exists
    assertThrows(NoSuchColumnFamilyException.class,
      () -> ADMIN.modifyColumnFamilyStoreFileTracker(tableName, Bytes.toBytes("not_exists"),
        StoreFileTrackerFactory.Trackers.FILE.name()));
    // to migration
    assertThrows(DoNotRetryIOException.class, () -> ADMIN.modifyTableStoreFileTracker(tableName,
      StoreFileTrackerFactory.Trackers.MIGRATION.name()));
    // disabled
    ADMIN.disableTable(tableName);
    assertThrows(TableNotEnabledException.class, () -> ADMIN.modifyTableStoreFileTracker(tableName,
      StoreFileTrackerFactory.Trackers.FILE.name()));
  }
}
