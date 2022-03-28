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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Test changing store file tracker implementation by altering table.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestChangeStoreFileTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestChangeStoreFileTracker.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @Rule
  public final TableNameTestRule tableName = new TableNameTestRule();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testCreateError() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName.getTableName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family"))
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(MigrationStoreFileTracker.SRC_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .setValue(MigrationStoreFileTracker.DST_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .build();
    UTIL.getAdmin().createTable(td);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testModifyError1() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName.getTableName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).build();
    UTIL.getAdmin().createTable(td);
    TableDescriptor newTd = TableDescriptorBuilder.newBuilder(td)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .build();
    UTIL.getAdmin().modifyTable(newTd);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testModifyError2() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName.getTableName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).build();
    UTIL.getAdmin().createTable(td);
    TableDescriptor newTd = TableDescriptorBuilder.newBuilder(td)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(MigrationStoreFileTracker.SRC_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .setValue(MigrationStoreFileTracker.DST_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .build();
    UTIL.getAdmin().modifyTable(newTd);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testModifyError3() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName.getTableName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).build();
    UTIL.getAdmin().createTable(td);
    TableDescriptor newTd = TableDescriptorBuilder.newBuilder(td)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(MigrationStoreFileTracker.SRC_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .setValue(MigrationStoreFileTracker.DST_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .build();
    UTIL.getAdmin().modifyTable(newTd);
  }

  // return the TableDescriptor for creating table
  private TableDescriptor createTableAndChangeToMigrationTracker() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName.getTableName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).build();
    UTIL.getAdmin().createTable(td);
    TableDescriptor newTd = TableDescriptorBuilder.newBuilder(td)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(MigrationStoreFileTracker.SRC_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .setValue(MigrationStoreFileTracker.DST_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .build();
    UTIL.getAdmin().modifyTable(newTd);
    return td;
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testModifyError4() throws IOException {
    TableDescriptor td = createTableAndChangeToMigrationTracker();
    TableDescriptor newTd = TableDescriptorBuilder.newBuilder(td)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(MigrationStoreFileTracker.SRC_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .setValue(MigrationStoreFileTracker.DST_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .build();
    UTIL.getAdmin().modifyTable(newTd);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testModifyError5() throws IOException {
    TableDescriptor td = createTableAndChangeToMigrationTracker();
    TableDescriptor newTd = TableDescriptorBuilder.newBuilder(td)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(MigrationStoreFileTracker.SRC_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .setValue(MigrationStoreFileTracker.DST_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .build();
    UTIL.getAdmin().modifyTable(newTd);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testModifyError6() throws IOException {
    TableDescriptor td = createTableAndChangeToMigrationTracker();
    TableDescriptor newTd =
      TableDescriptorBuilder.newBuilder(td).setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.DEFAULT.name()).build();
    UTIL.getAdmin().modifyTable(newTd);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testModifyError7() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName.getTableName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).build();
    UTIL.getAdmin().createTable(td);
    TableDescriptor newTd = TableDescriptorBuilder.newBuilder(tableName.getTableName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("family1"))
        .setConfiguration(StoreFileTrackerFactory.TRACKER_IMPL,
          StoreFileTrackerFactory.Trackers.MIGRATION.name())
        .build())
      .build();
    UTIL.getAdmin().modifyTable(newTd);
  }

  // actually a NPE as we do not specify the src and dst impl for migration store file tracker
  @Test(expected = IOException.class)
  public void testModifyError8() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName.getTableName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).build();
    UTIL.getAdmin().createTable(td);
    TableDescriptor newTd =
      TableDescriptorBuilder.newBuilder(td).setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name()).build();
    UTIL.getAdmin().modifyTable(newTd);
  }

  @Test
  public void testModifyError9() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName.getTableName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("family")).build();
    UTIL.getAdmin().createTable(td);
    UTIL.getAdmin().disableTable(td.getTableName());
    TableDescriptor newTd = TableDescriptorBuilder.newBuilder(td)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(MigrationStoreFileTracker.SRC_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .setValue(MigrationStoreFileTracker.DST_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .build();
    UTIL.getAdmin().modifyTable(newTd);
    TableDescriptor newTd2 = TableDescriptorBuilder.newBuilder(td)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .build();
    // changing from MIGRATION while table is disabled is not allowed
    assertThrows(TableNotEnabledException.class, () -> UTIL.getAdmin().modifyTable(newTd2));
  }

  private String getStoreFileName(TableName table, byte[] family) {
    return Iterables
      .getOnlyElement(Iterables.getOnlyElement(UTIL.getMiniHBaseCluster().getRegions(table))
        .getStore(family).getStorefiles())
      .getPath().getName();
  }

  @Test
  public void testModify() throws IOException {
    TableName tn = tableName.getTableName();
    byte[] row = Bytes.toBytes("row");
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] value = Bytes.toBytes("value");
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tn)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
    UTIL.getAdmin().createTable(td);
    try (Table table = UTIL.getConnection().getTable(tn)) {
      table.put(new Put(row).addColumn(family, qualifier, value));
    }
    UTIL.flush(tn);
    String fileName = getStoreFileName(tn, family);

    TableDescriptor newTd = TableDescriptorBuilder.newBuilder(td)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(MigrationStoreFileTracker.SRC_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .setValue(MigrationStoreFileTracker.DST_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .build();
    UTIL.getAdmin().modifyTable(newTd);
    assertEquals(fileName, getStoreFileName(tn, family));
    try (Table table = UTIL.getConnection().getTable(tn)) {
      assertArrayEquals(value, table.get(new Get(row)).getValue(family, qualifier));
    }

    TableDescriptor newTd2 = TableDescriptorBuilder.newBuilder(td)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .build();
    UTIL.getAdmin().modifyTable(newTd2);
    assertEquals(fileName, getStoreFileName(tn, family));
    try (Table table = UTIL.getConnection().getTable(tn)) {
      assertArrayEquals(value, table.get(new Get(row)).getValue(family, qualifier));
    }
  }
}
