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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Verify that the HTableDescriptor is updated after addColumn(), deleteColumn() and modifyTable()
 * operations.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestTableDescriptorModificationFromClient {
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static TableName TABLE_NAME = null;
  private static final byte[] FAMILY_0 = Bytes.toBytes("cf0");
  private static final byte[] FAMILY_1 = Bytes.toBytes("cf1");

  /**
   * Start up a mini cluster and put a small table of empty regions into it.
   */
  @BeforeAll
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @BeforeEach
  public void setup(TestInfo testInfo) {
    TABLE_NAME = TableName.valueOf(testInfo.getTestMethod().get().getName());
  }

  @AfterAll
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testModifyTable() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    // Create a table with one family
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0)).build();
    admin.createTable(tableDescriptor);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      // Modify the table adding another family and verify the descriptor
      TableDescriptor modifiedtableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_1)).build();
      admin.modifyTable(modifiedtableDescriptor);
      verifyTableDescriptor(TABLE_NAME, FAMILY_0, FAMILY_1);
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testAddColumn() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    // Create a table with two families
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0)).build();
    admin.createTable(tableDescriptor);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      // Modify the table removing one family and verify the descriptor
      admin.addColumnFamily(TABLE_NAME, ColumnFamilyDescriptorBuilder.of(FAMILY_1));
      verifyTableDescriptor(TABLE_NAME, FAMILY_0, FAMILY_1);
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testAddSameColumnFamilyTwice() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    // Create a table with one families
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0)).build();
    admin.createTable(tableDescriptor);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      // Modify the table removing one family and verify the descriptor
      admin.addColumnFamily(TABLE_NAME, ColumnFamilyDescriptorBuilder.of(FAMILY_1));
      verifyTableDescriptor(TABLE_NAME, FAMILY_0, FAMILY_1);

      try {
        // Add same column family again - expect failure
        admin.addColumnFamily(TABLE_NAME, ColumnFamilyDescriptorBuilder.of(FAMILY_1));
        fail("Delete a non-exist column family should fail");
      } catch (InvalidFamilyOperationException e) {
        // Expected.
      }

    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testModifyColumnFamily() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();

    ColumnFamilyDescriptor cfDescriptor = ColumnFamilyDescriptorBuilder.of(FAMILY_0);
    int blockSize = cfDescriptor.getBlocksize();
    // Create a table with one families
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(cfDescriptor).build();
    admin.createTable(tableDescriptor);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      int newBlockSize = 2 * blockSize;
      cfDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(cfDescriptor).setBlocksize(newBlockSize).build();

      // Modify colymn family
      admin.modifyColumnFamily(TABLE_NAME, cfDescriptor);

      TableDescriptor htd = admin.getDescriptor(TABLE_NAME);
      ColumnFamilyDescriptor hcfd = htd.getColumnFamily(FAMILY_0);
      assertTrue(hcfd.getBlocksize() == newBlockSize);
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testModifyNonExistingColumnFamily() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();

    ColumnFamilyDescriptor cfDescriptor = ColumnFamilyDescriptorBuilder.of(FAMILY_1);
    int blockSize = cfDescriptor.getBlocksize();
    // Create a table with one families
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0)).build();
    admin.createTable(tableDescriptor);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      int newBlockSize = 2 * blockSize;
      cfDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(cfDescriptor).setBlocksize(newBlockSize).build();

      // Modify a column family that is not in the table.
      try {
        admin.modifyColumnFamily(TABLE_NAME, cfDescriptor);
        fail("Modify a non-exist column family should fail");
      } catch (InvalidFamilyOperationException e) {
        // Expected.
      }

    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testDeleteColumn() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    // Create a table with two families
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_1)).build();
    admin.createTable(tableDescriptor);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0, FAMILY_1);

      // Modify the table removing one family and verify the descriptor
      admin.deleteColumnFamily(TABLE_NAME, FAMILY_1);
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testDeleteSameColumnFamilyTwice() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    // Create a table with two families
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_1)).build();
    admin.createTable(tableDescriptor);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0, FAMILY_1);

      // Modify the table removing one family and verify the descriptor
      admin.deleteColumnFamily(TABLE_NAME, FAMILY_1);
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      try {
        // Delete again - expect failure
        admin.deleteColumnFamily(TABLE_NAME, FAMILY_1);
        fail("Delete a non-exist column family should fail");
      } catch (Exception e) {
        // Expected.
      }
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  private void verifyTableDescriptor(final TableName tableName, final byte[]... families)
    throws IOException {
    Admin admin = TEST_UTIL.getAdmin();

    // Verify descriptor from master
    TableDescriptor htd = admin.getDescriptor(tableName);
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
      assertTrue(htdFamilies.contains(familyName), "Expected family " + Bytes.toString(familyName));
    }
  }
}
