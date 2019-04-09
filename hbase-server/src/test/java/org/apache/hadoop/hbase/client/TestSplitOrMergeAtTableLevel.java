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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MediumTests.class, ClientTests.class })
public class TestSplitOrMergeAtTableLevel {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSplitOrMergeAtTableLevel.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");

  @Rule
  public TestName name = new TestName();
  private static Admin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(2);
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testTableSplitSwitch() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setSplitEnabled(false).build();

    // create a table with split disabled
    Table t = TEST_UTIL.createTable(tableDesc, null);
    TEST_UTIL.waitTableAvailable(tableName);

    // load data into the table
    TEST_UTIL.loadTable(t, FAMILY, false);

    assertTrue(admin.getRegions(tableName).size() == 1);

    // check that we have split disabled
    assertFalse(admin.getDescriptor(tableName).isSplitEnabled());
    trySplitAndEnsureItFails(tableName);
    enableTableSplit(tableName);
    trySplitAndEnsureItIsSuccess(tableName);
  }

  @Test
  public void testTableSplitSwitchForPreSplittedTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    // create a table with split disabled
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setSplitEnabled(false)
        .build();
    Table t = TEST_UTIL.createTable(tableDesc, new byte[][] { Bytes.toBytes(10) });
    TEST_UTIL.waitTableAvailable(tableName);

    // load data into the table
    TEST_UTIL.loadTable(t, FAMILY, false);

    assertTrue(admin.getRegions(tableName).size() == 2);

    // check that we have split disabled
    assertFalse(admin.getDescriptor(tableName).isSplitEnabled());
    trySplitAndEnsureItFails(tableName);
    enableTableSplit(tableName);
    trySplitAndEnsureItIsSuccess(tableName);
  }

  @Test
  public void testTableMergeSwitch() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setMergeEnabled(false)
        .build();

    Table t = TEST_UTIL.createTable(tableDesc, null);
    TEST_UTIL.waitTableAvailable(tableName);
    TEST_UTIL.loadTable(t, FAMILY, false);

    // check merge is disabled for the table
    assertFalse(admin.getDescriptor(tableName).isMergeEnabled());

    trySplitAndEnsureItIsSuccess(tableName);
    Threads.sleep(10000);
    tryMergeAndEnsureItFails(tableName);
    admin.disableTable(tableName);
    enableTableMerge(tableName);
    admin.enableTable(tableName);
    tryMergeAndEnsureItIsSuccess(tableName);
  }

  @Test
  public void testTableMergeSwitchForPreSplittedTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setMergeEnabled(false)
        .build();

    Table t = TEST_UTIL.createTable(tableDesc, new byte[][] { Bytes.toBytes(10) });
    TEST_UTIL.waitTableAvailable(tableName);
    TEST_UTIL.loadTable(t, FAMILY, false);

    // check merge is disabled for the table
    assertFalse(admin.getDescriptor(tableName).isMergeEnabled());
    assertTrue(admin.getRegions(tableName).size() == 2);
    tryMergeAndEnsureItFails(tableName);
    enableTableMerge(tableName);
    tryMergeAndEnsureItIsSuccess(tableName);
  }

  private void trySplitAndEnsureItFails(final TableName tableName) throws Exception {
    // get the original table region count
    List<RegionInfo> regions = admin.getRegions(tableName);
    int originalCount = regions.size();

    // split the table and make sure region count does not increase
    Future<?> f = admin.splitRegionAsync(regions.get(0).getEncodedNameAsBytes(), Bytes.toBytes(2));
    try {
      f.get(10, TimeUnit.SECONDS);
      fail("Should not get here.");
    } catch (ExecutionException ee) {
      // expected to reach here
      // check and ensure that table does not get splitted
      assertTrue(admin.getRegions(tableName).size() == originalCount);
    }
  }

  /**
   * Method to enable split for the passed table and validate this modification.
   * @param tableName name of the table
   */
  private void enableTableSplit(final TableName tableName) throws Exception {
    // Get the original table descriptor
    TableDescriptor originalTableDesc = admin.getDescriptor(tableName);
    TableDescriptor modifiedTableDesc = TableDescriptorBuilder.newBuilder(originalTableDesc)
        .setSplitEnabled(true)
        .build();

    // Now modify the table descriptor and enable split for it
    admin.modifyTable(modifiedTableDesc);

    // Verify that split is enabled
    assertTrue(admin.getDescriptor(tableName).isSplitEnabled());
  }

  private void trySplitAndEnsureItIsSuccess(final TableName tableName)
      throws Exception {
    // get the original table region count
    List<RegionInfo> regions = admin.getRegions(tableName);
    int originalCount = regions.size();

    // split the table and wait until region count increases
    admin.split(tableName, Bytes.toBytes(3));
    TEST_UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return admin.getRegions(tableName).size() > originalCount;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Split has not finished yet";
      }
    });
  }

  private void tryMergeAndEnsureItFails(final TableName tableName) throws Exception {
    // assert we have at least 2 regions in the table
    List<RegionInfo> regions = admin.getRegions(tableName);
    int originalCount = regions.size();
    assertTrue(originalCount >= 2);

    byte[] nameOfRegionA = regions.get(0).getEncodedNameAsBytes();
    byte[] nameOfRegionB = regions.get(1).getEncodedNameAsBytes();

    // check and ensure that region do not get merged
    Future<?> f = admin.mergeRegionsAsync(nameOfRegionA, nameOfRegionB, true);
    try {
      f.get(10, TimeUnit.SECONDS);
      fail("Should not get here.");
    } catch (ExecutionException ee) {
      // expected to reach here
      // check and ensure that region do not get merged
      assertTrue(admin.getRegions(tableName).size() == originalCount);
    }
  }


  /**
   * Method to enable merge for the passed table and validate this modification.
   * @param tableName name of the table
   */
  private void enableTableMerge(final TableName tableName) throws Exception {
    // Get the original table descriptor
    TableDescriptor originalTableDesc = admin.getDescriptor(tableName);
    TableDescriptor modifiedTableDesc = TableDescriptorBuilder.newBuilder(originalTableDesc)
        .setMergeEnabled(true)
        .build();

    // Now modify the table descriptor and enable merge for it
    admin.modifyTable(modifiedTableDesc);

    // Verify that merge is enabled
    assertTrue(admin.getDescriptor(tableName).isMergeEnabled());
  }

  private void tryMergeAndEnsureItIsSuccess(final TableName tableName) throws Exception {
    // assert we have at least 2 regions in the table
    List<RegionInfo> regions = admin.getRegions(tableName);
    int originalCount = regions.size();
    assertTrue(originalCount >= 2);

    byte[] nameOfRegionA = regions.get(0).getEncodedNameAsBytes();
    byte[] nameOfRegionB = regions.get(1).getEncodedNameAsBytes();

    // merge the table regions and wait until region count decreases
    admin.mergeRegionsAsync(nameOfRegionA, nameOfRegionB, true);
    TEST_UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return admin.getRegions(tableName).size() < originalCount;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Merge has not finished yet";
      }
    });
  }
}
