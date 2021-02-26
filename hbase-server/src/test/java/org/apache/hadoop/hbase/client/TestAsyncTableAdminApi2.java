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

import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous table admin operations
 * @see TestAsyncTableAdminApi This test and it used to be joined it was taking longer than our
 * ten minute timeout so they were split.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableAdminApi2 extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncTableAdminApi2.class);

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
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0));
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
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0));
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
    admin.createTable(tdBuilder.setColumnFamily(cfd).build()).join();
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
    admin.createTable(tdBuilder.setColumnFamily(cfd).build()).join();
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
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_1));
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
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_1));
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
    Path tableDir = CommonFSUtils.getTableDir(mfs.getRootDir(), tableName);
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
