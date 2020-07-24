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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Verify that the HTableDescriptor is updated after
 * addColumn(), deleteColumn() and modifyTable() operations.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestTableDescriptorModificationFromClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableDescriptorModificationFromClient.class);

  @Rule public TestName name = new TestName();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static TableName TABLE_NAME = null;
  private static final byte[] FAMILY_0 = Bytes.toBytes("cf0");
  private static final byte[] FAMILY_1 = Bytes.toBytes("cf1");

  /**
   * Start up a mini cluster and put a small table of empty regions into it.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @Before
  public void setup() {
    TABLE_NAME = TableName.valueOf(name.getMethodName());

  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testModifyTable() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    // Create a table with one family
    HTableDescriptor baseHtd = new HTableDescriptor(TABLE_NAME);
    baseHtd.addFamily(new HColumnDescriptor(FAMILY_0));
    admin.createTable(baseHtd);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      // Modify the table adding another family and verify the descriptor
      HTableDescriptor modifiedHtd = new HTableDescriptor(TABLE_NAME);
      modifiedHtd.addFamily(new HColumnDescriptor(FAMILY_0));
      modifiedHtd.addFamily(new HColumnDescriptor(FAMILY_1));
      admin.modifyTable(TABLE_NAME, modifiedHtd);
      verifyTableDescriptor(TABLE_NAME, FAMILY_0, FAMILY_1);
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testAddColumn() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    // Create a table with two families
    HTableDescriptor baseHtd = new HTableDescriptor(TABLE_NAME);
    baseHtd.addFamily(new HColumnDescriptor(FAMILY_0));
    admin.createTable(baseHtd);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      // Modify the table removing one family and verify the descriptor
      admin.addColumnFamily(TABLE_NAME, new HColumnDescriptor(FAMILY_1));
      verifyTableDescriptor(TABLE_NAME, FAMILY_0, FAMILY_1);
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testAddSameColumnFamilyTwice() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    // Create a table with one families
    HTableDescriptor baseHtd = new HTableDescriptor(TABLE_NAME);
    baseHtd.addFamily(new HColumnDescriptor(FAMILY_0));
    admin.createTable(baseHtd);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      // Modify the table removing one family and verify the descriptor
      admin.addColumnFamily(TABLE_NAME, new HColumnDescriptor(FAMILY_1));
      verifyTableDescriptor(TABLE_NAME, FAMILY_0, FAMILY_1);

      try {
        // Add same column family again - expect failure
        admin.addColumnFamily(TABLE_NAME, new HColumnDescriptor(FAMILY_1));
        Assert.fail("Delete a non-exist column family should fail");
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

    HColumnDescriptor cfDescriptor = new HColumnDescriptor(FAMILY_0);
    int blockSize = cfDescriptor.getBlocksize();
    // Create a table with one families
    HTableDescriptor baseHtd = new HTableDescriptor(TABLE_NAME);
    baseHtd.addFamily(cfDescriptor);
    admin.createTable(baseHtd);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      int newBlockSize = 2 * blockSize;
      cfDescriptor.setBlocksize(newBlockSize);

      // Modify colymn family
      admin.modifyColumnFamily(TABLE_NAME, cfDescriptor);

      HTableDescriptor htd = admin.getTableDescriptor(TABLE_NAME);
      HColumnDescriptor hcfd = htd.getFamily(FAMILY_0);
      assertTrue(hcfd.getBlocksize() == newBlockSize);
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  @Test
  public void testModifyNonExistingColumnFamily() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();

    HColumnDescriptor cfDescriptor = new HColumnDescriptor(FAMILY_1);
    int blockSize = cfDescriptor.getBlocksize();
    // Create a table with one families
    HTableDescriptor baseHtd = new HTableDescriptor(TABLE_NAME);
    baseHtd.addFamily(new HColumnDescriptor(FAMILY_0));
    admin.createTable(baseHtd);
    admin.disableTable(TABLE_NAME);
    try {
      // Verify the table descriptor
      verifyTableDescriptor(TABLE_NAME, FAMILY_0);

      int newBlockSize = 2 * blockSize;
      cfDescriptor.setBlocksize(newBlockSize);

      // Modify a column family that is not in the table.
      try {
        admin.modifyColumnFamily(TABLE_NAME, cfDescriptor);
        Assert.fail("Modify a non-exist column family should fail");
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
    HTableDescriptor baseHtd = new HTableDescriptor(TABLE_NAME);
    baseHtd.addFamily(new HColumnDescriptor(FAMILY_0));
    baseHtd.addFamily(new HColumnDescriptor(FAMILY_1));
    admin.createTable(baseHtd);
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
    HTableDescriptor baseHtd = new HTableDescriptor(TABLE_NAME);
    baseHtd.addFamily(new HColumnDescriptor(FAMILY_0));
    baseHtd.addFamily(new HColumnDescriptor(FAMILY_1));
    admin.createTable(baseHtd);
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
        Assert.fail("Delete a non-exist column family should fail");
      } catch (Exception e) {
        // Expected.
      }
    } finally {
      admin.deleteTable(TABLE_NAME);
    }
  }

  private void verifyTableDescriptor(final TableName tableName,
                                     final byte[]... families) throws IOException {
    Admin admin = TEST_UTIL.getAdmin();

    // Verify descriptor from master
    HTableDescriptor htd = admin.getTableDescriptor(tableName);
    verifyTableDescriptor(htd, tableName, families);

    // Verify descriptor from HDFS
    MasterFileSystem mfs = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    Path tableDir = CommonFSUtils.getTableDir(mfs.getRootDir(), tableName);
    TableDescriptor td =
        FSTableDescriptors.getTableDescriptorFromFs(mfs.getFileSystem(), tableDir);
    verifyTableDescriptor(td, tableName, families);
  }

  private void verifyTableDescriptor(final TableDescriptor htd,
      final TableName tableName, final byte[]... families) {
    Set<byte[]> htdFamilies = htd.getColumnFamilyNames();
    assertEquals(tableName, htd.getTableName());
    assertEquals(families.length, htdFamilies.size());
    for (byte[] familyName: families) {
      assertTrue("Expected family " + Bytes.toString(familyName), htdFamilies.contains(familyName));
    }
  }
}
