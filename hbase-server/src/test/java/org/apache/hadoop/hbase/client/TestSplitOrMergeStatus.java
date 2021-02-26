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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.master.assignment.SplitTableRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteTableProcedure;
import org.apache.hadoop.hbase.master.procedure.DisableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MediumTests.class, ClientTests.class})
public class TestSplitOrMergeStatus {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSplitOrMergeStatus.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");

  @Rule
  public TestName name = new TestName();

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSplitSwitch() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table t = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);

    RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(t.getName());
    int originalCount = locator.getAllRegionLocations().size();

    Admin admin = TEST_UTIL.getAdmin();
    initSwitchStatus(admin);
    assertTrue(admin.splitSwitch(false, false));
    try {
      admin.split(t.getName());
      fail("Shouldn't get here");
    } catch (DoNotRetryIOException dnioe) {
      // Expected
    }
    int count = admin.getTableRegions(tableName).size();
    assertTrue(originalCount == count);
    assertFalse(admin.splitSwitch(true, false));
    admin.split(t.getName());
    while ((count = admin.getTableRegions(tableName).size()) == originalCount) {
      Threads.sleep(1);;
    }
    count = admin.getTableRegions(tableName).size();
    assertTrue(originalCount < count);
    admin.close();
  }


  @Ignore @Test
  public void testMergeSwitch() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table t = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);

    Admin admin = TEST_UTIL.getAdmin();
    int originalCount = admin.getTableRegions(tableName).size();
    initSwitchStatus(admin);
    admin.split(t.getName());
    int postSplitCount = -1;
    while ((postSplitCount = admin.getTableRegions(tableName).size()) == originalCount) {
      Threads.sleep(1);;
    }
    assertTrue("originalCount=" + originalCount + ", newCount=" + postSplitCount,
        originalCount != postSplitCount);

    // Merge switch is off so merge should NOT succeed.
    boolean[] results = admin.setSplitOrMergeEnabled(false, false, MasterSwitchType.MERGE);
    assertEquals(1, results.length);
    assertTrue(results[0]);
    List<HRegionInfo> regions = admin.getTableRegions(t.getName());
    assertTrue(regions.size() > 1);
    Future<?> f = admin.mergeRegionsAsync(regions.get(0).getEncodedNameAsBytes(),
      regions.get(1).getEncodedNameAsBytes(), true);
    try {
      f.get(10, TimeUnit.SECONDS);
      fail("Should not get here.");
    } catch (ExecutionException ee) {
      // Expected.
    }
    int count = admin.getTableRegions(tableName).size();
    assertTrue("newCount=" + postSplitCount + ", count=" + count, postSplitCount == count);

    results = admin.setSplitOrMergeEnabled(true, false, MasterSwitchType.MERGE);
    regions = admin.getTableRegions(t.getName());
    assertEquals(1, results.length);
    assertFalse(results[0]);
    f = admin.mergeRegionsAsync(regions.get(0).getEncodedNameAsBytes(),
      regions.get(1).getEncodedNameAsBytes(), true);
    f.get(10, TimeUnit.SECONDS);
    count = admin.getTableRegions(tableName).size();
    assertTrue((postSplitCount / 2 /*Merge*/) == count);
    admin.close();
  }

  @Test
  public void testMultiSwitches() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    boolean[] switches = admin.setSplitOrMergeEnabled(false, false,
      MasterSwitchType.SPLIT, MasterSwitchType.MERGE);
    for (boolean s : switches){
      assertTrue(s);
    }
    assertFalse(admin.isSplitOrMergeEnabled(MasterSwitchType.SPLIT));
    assertFalse(admin.isSplitOrMergeEnabled(MasterSwitchType.MERGE));
    admin.close();
  }

  @Test
  public void testSplitRegionReplicaRitRecovery() throws Exception {
    int startRowNum = 11;
    int rowCount = 60;
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    TEST_UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).setRegionReplication(2).build());
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
    ServerName serverName =
        RegionReplicaTestHelper.getRSCarryingReplica(TEST_UTIL, tableName, 1).get();
    List<RegionInfo> regions = TEST_UTIL.getAdmin().getRegions(tableName);
    insertData(tableName, startRowNum, rowCount);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);
    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions.get(0), splitKey));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    // Disable the table
    long procId1 = procExec
        .submitProcedure(new DisableTableProcedure(procExec.getEnvironment(), tableName, false));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    // Delete Table
    long procId2 =
        procExec.submitProcedure(new DeleteTableProcedure(procExec.getEnvironment(), tableName));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    AssignmentTestingUtil.killRs(TEST_UTIL, serverName);
    Threads.sleepWithoutInterrupt(5000);
    boolean hasRegionsInTransition = TEST_UTIL.getMiniHBaseCluster().getMaster()
        .getAssignmentManager().getRegionStates().hasRegionsInTransition();
    assertEquals(false, hasRegionsInTransition);
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  private void insertData(final TableName tableName, int startRow, int rowCount)
      throws IOException {
    Table t = TEST_UTIL.getConnection().getTable(tableName);
    Put p;
    for (int i = 0; i < rowCount; i++) {
      p = new Put(Bytes.toBytes("" + (startRow + i)));
      p.addColumn(FAMILY, Bytes.toBytes("q1"), Bytes.toBytes(i));
      t.put(p);
    }
  }

  private void initSwitchStatus(Admin admin) throws IOException {
    if (!admin.isSplitEnabled()) {
      admin.splitSwitch(true, false);
    }
    if (!admin.isMergeEnabled()) {
      admin.mergeSwitch(true, false);
    }
    assertTrue(admin.isSplitEnabled());
    assertTrue(admin.isMergeEnabled());
  }
}
