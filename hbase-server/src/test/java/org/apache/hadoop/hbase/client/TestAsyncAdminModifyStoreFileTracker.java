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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncAdminModifyStoreFileTracker extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncAdminModifyStoreFileTracker.class);

  private static final String SRC_IMPL = "hbase.store.file-tracker.migration.src.impl";

  private static final String DST_IMPL = "hbase.store.file-tracker.migration.dst.impl";

  private void verifyModifyTableResult(TableName tableName, byte[] family, byte[] qual, byte[] row,
    byte[] value, String sft) throws IOException {
    TableDescriptor td = admin.getDescriptor(tableName).join();
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
    byte[] family = Bytes.toBytes("info");
    byte[] qual = Bytes.toBytes("q");
    byte[] row = Bytes.toBytes(0);
    byte[] value = Bytes.toBytes(1);
    try (Table table = TEST_UTIL.createTable(tableName, family)) {
      table.put(new Put(row).addColumn(family, qual, value));
    }
    // change to FILE
    admin.modifyTableStoreFileTracker(tableName, StoreFileTrackerFactory.Trackers.FILE.name())
      .join();
    verifyModifyTableResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to FILE again, should have no effect
    admin.modifyTableStoreFileTracker(tableName, StoreFileTrackerFactory.Trackers.FILE.name())
      .join();
    verifyModifyTableResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to MIGRATION, and then to FILE
    admin.modifyTable(TableDescriptorBuilder.newBuilder(admin.getDescriptor(tableName).join())
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(SRC_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .setValue(DST_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name()).build()).join();
    admin.modifyTableStoreFileTracker(tableName, StoreFileTrackerFactory.Trackers.FILE.name())
      .join();
    verifyModifyTableResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to MIGRATION, and then to DEFAULT
    admin.modifyTable(TableDescriptorBuilder.newBuilder(admin.getDescriptor(tableName).join())
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        StoreFileTrackerFactory.Trackers.MIGRATION.name())
      .setValue(SRC_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .setValue(DST_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name()).build()).join();
    admin.modifyTableStoreFileTracker(tableName, StoreFileTrackerFactory.Trackers.DEFAULT.name())
      .join();
    verifyModifyTableResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.DEFAULT.name());
  }

  private void verifyModifyColumnFamilyResult(TableName tableName, byte[] family, byte[] qual,
    byte[] row, byte[] value, String sft) throws IOException {
    TableDescriptor td = admin.getDescriptor(tableName).join();
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
    byte[] family = Bytes.toBytes("info");
    byte[] qual = Bytes.toBytes("q");
    byte[] row = Bytes.toBytes(0);
    byte[] value = Bytes.toBytes(1);
    try (Table table = TEST_UTIL.createTable(tableName, family)) {
      table.put(new Put(row).addColumn(family, qual, value));
    }
    // change to FILE
    admin.modifyColumnFamilyStoreFileTracker(tableName, family,
      StoreFileTrackerFactory.Trackers.FILE.name()).join();
    verifyModifyColumnFamilyResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to FILE again, should have no effect
    admin.modifyColumnFamilyStoreFileTracker(tableName, family,
      StoreFileTrackerFactory.Trackers.FILE.name()).join();
    verifyModifyColumnFamilyResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to MIGRATION, and then to FILE
    TableDescriptor current = admin.getDescriptor(tableName).join();
    admin.modifyTable(TableDescriptorBuilder.newBuilder(current)
      .modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(current.getColumnFamily(family))
        .setConfiguration(StoreFileTrackerFactory.TRACKER_IMPL,
          StoreFileTrackerFactory.Trackers.MIGRATION.name())
        .setConfiguration(SRC_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
        .setConfiguration(DST_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name()).build())
      .build()).join();
    admin.modifyColumnFamilyStoreFileTracker(tableName, family,
      StoreFileTrackerFactory.Trackers.FILE.name()).join();
    verifyModifyColumnFamilyResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.FILE.name());

    // change to MIGRATION, and then to DEFAULT
    current = admin.getDescriptor(tableName).join();
    admin.modifyTable(TableDescriptorBuilder.newBuilder(current)
      .modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(current.getColumnFamily(family))
        .setConfiguration(StoreFileTrackerFactory.TRACKER_IMPL,
          StoreFileTrackerFactory.Trackers.MIGRATION.name())
        .setConfiguration(SRC_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
        .setConfiguration(DST_IMPL, StoreFileTrackerFactory.Trackers.DEFAULT.name()).build())
      .build()).join();
    admin.modifyColumnFamilyStoreFileTracker(tableName, family,
      StoreFileTrackerFactory.Trackers.DEFAULT.name()).join();
    verifyModifyColumnFamilyResult(tableName, family, qual, row, value,
      StoreFileTrackerFactory.Trackers.DEFAULT.name());
  }

  @Test
  public void testModifyStoreFileTrackerError() throws IOException {
    byte[] family = Bytes.toBytes("info");
    TEST_UTIL.createTable(tableName, family).close();

    // table not exists
    assertThrows(TableNotFoundException.class,
      () -> FutureUtils.get(admin.modifyTableStoreFileTracker(TableName.valueOf("whatever"),
        StoreFileTrackerFactory.Trackers.FILE.name())));
    // family not exists
    assertThrows(NoSuchColumnFamilyException.class,
      () -> FutureUtils.get(admin.modifyColumnFamilyStoreFileTracker(tableName,
        Bytes.toBytes("not_exists"), StoreFileTrackerFactory.Trackers.FILE.name())));
    // to migration
    assertThrows(DoNotRetryIOException.class, () -> FutureUtils.get(admin
      .modifyTableStoreFileTracker(tableName, StoreFileTrackerFactory.Trackers.MIGRATION.name())));
    // disabled
    admin.disableTable(tableName).join();
    assertThrows(TableNotEnabledException.class, () -> FutureUtils.get(
      admin.modifyTableStoreFileTracker(tableName, StoreFileTrackerFactory.Trackers.FILE.name())));
  }
}
