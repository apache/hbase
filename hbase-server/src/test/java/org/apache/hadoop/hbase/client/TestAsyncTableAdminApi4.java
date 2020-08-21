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

import java.util.concurrent.CompletionException;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous table admin operations
 * @see TestAsyncTableAdminApi Split from it so each runs under ten minutes.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableAdminApi4 extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncTableAdminApi4.class);

  @Test
  public void testCloneTableSchema() throws Exception {
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    testCloneTableSchema(tableName, newTableName, false);
  }

  @Test
  public void testCloneTableSchemaPreservingSplits() throws Exception {
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    testCloneTableSchema(tableName, newTableName, true);
  }

  private void testCloneTableSchema(final TableName tableName,
      final TableName newTableName, boolean preserveSplits) throws Exception {
    byte[][] splitKeys = new byte[2][];
    splitKeys[0] = Bytes.toBytes(4);
    splitKeys[1] = Bytes.toBytes(8);
    int NUM_FAMILYS = 2;
    int NUM_REGIONS = 3;
    int BLOCK_SIZE = 1024;
    int TTL = 86400;
    boolean BLOCK_CACHE = false;

    // Create the table
    TableDescriptor tableDesc = TableDescriptorBuilder
        .newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_0))
        .setColumnFamily(ColumnFamilyDescriptorBuilder
            .newBuilder(FAMILY_1)
            .setBlocksize(BLOCK_SIZE)
            .setBlockCacheEnabled(BLOCK_CACHE)
            .setTimeToLive(TTL)
            .build()).build();
    admin.createTable(tableDesc, splitKeys).join();

    assertEquals(NUM_REGIONS, TEST_UTIL.getHBaseCluster().getRegions(tableName).size());
    assertTrue("Table should be created with splitKyes + 1 rows in META",
        admin.isTableAvailable(tableName, splitKeys).get());

    // Clone & Verify
    admin.cloneTableSchema(tableName, newTableName, preserveSplits).join();
    TableDescriptor newTableDesc = admin.getDescriptor(newTableName).get();

    assertEquals(NUM_FAMILYS, newTableDesc.getColumnFamilyCount());
    assertEquals(BLOCK_SIZE, newTableDesc.getColumnFamily(FAMILY_1).getBlocksize());
    assertEquals(BLOCK_CACHE, newTableDesc.getColumnFamily(FAMILY_1).isBlockCacheEnabled());
    assertEquals(TTL, newTableDesc.getColumnFamily(FAMILY_1).getTimeToLive());
    TEST_UTIL.verifyTableDescriptorIgnoreTableName(tableDesc, newTableDesc);

    if (preserveSplits) {
      assertEquals(NUM_REGIONS, TEST_UTIL.getHBaseCluster().getRegions(newTableName).size());
      assertTrue("New table should be created with splitKyes + 1 rows in META",
          admin.isTableAvailable(newTableName, splitKeys).get());
    } else {
      assertEquals(1, TEST_UTIL.getHBaseCluster().getRegions(newTableName).size());
    }
  }

  @Test
  public void testCloneTableSchemaWithNonExistentSourceTable() throws Exception {
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    // test for non-existent source table
    try {
      admin.cloneTableSchema(tableName, newTableName, false).join();
      fail("Should have failed when source table doesn't exist.");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TableNotFoundException);
    }
  }

  @Test
  public void testCloneTableSchemaWithExistentDestinationTable() throws Exception {
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    byte[] FAMILY_0 = Bytes.toBytes("cf0");
    TEST_UTIL.createTable(tableName, FAMILY_0);
    TEST_UTIL.createTable(newTableName, FAMILY_0);
    // test for existent destination table
    try {
      admin.cloneTableSchema(tableName, newTableName, false).join();
      fail("Should have failed when destination table exists.");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof TableExistsException);
    }
  }

  @Test
  public void testIsTableAvailableWithInexistantTable() throws Exception {
    final TableName newTableName = TableName.valueOf(tableName.getNameAsString() + "_new");
    // test for inexistant table
    assertFalse(admin.isTableAvailable(newTableName).get());
  }
}