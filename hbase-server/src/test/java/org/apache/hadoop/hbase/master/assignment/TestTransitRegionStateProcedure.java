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
package org.apache.hadoop.hbase.master.assignment;

import static org.apache.hadoop.hbase.master.assignment.AssignmentManager.ASSIGN_MAX_ATTEMPTS;
import static org.apache.hadoop.hbase.master.assignment.RegionStateStore.getStateColumn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MasterTests.class, MediumTests.class })
public class TestTransitRegionStateProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTransitRegionStateProcedure.class);

  private static HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static byte[] CF = Bytes.toBytes("cf");

  @Rule
  public TestName name = new TestName();

  private TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    UTIL.getConfiguration().setInt(ASSIGN_MAX_ATTEMPTS, 1);
    UTIL.startMiniCluster(3);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    tableName = TableName.valueOf(name.getMethodName());
    UTIL.createTable(tableName, CF);
    UTIL.waitTableAvailable(tableName);
  }

  private void resetProcExecutorTestingKillFlag() {
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    assertTrue("expected executor to be running", procExec.isRunning());
  }

  @After
  public void tearDown() throws IOException {
    resetProcExecutorTestingKillFlag();
    UTIL.deleteTable(tableName);
  }

  private void testRecoveryAndDoubleExcution(TransitRegionStateProcedure proc) throws Exception {
    HMaster master = UTIL.getHBaseCluster().getMaster();
    AssignmentManager am = master.getAssignmentManager();
    RegionStateNode regionNode = am.getRegionStates().getRegionStateNode(proc.getRegion());
    assertFalse(regionNode.isInTransition());
    regionNode.setProcedure(proc);
    assertTrue(regionNode.isInTransition());
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    long procId = procExec.submitProcedure(proc);
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);
    regionNode = am.getRegionStates().getRegionStateNode(proc.getRegion());
    assertFalse(regionNode.isInTransition());
  }

  @Test
  public void testRecoveryAndDoubleExecutionMove() throws Exception {
    MasterProcedureEnv env =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment();
    HRegion region = UTIL.getMiniHBaseCluster().getRegions(tableName).get(0);
    long openSeqNum = region.getOpenSeqNum();
    TransitRegionStateProcedure proc =
      TransitRegionStateProcedure.move(env, region.getRegionInfo(), null);
    testRecoveryAndDoubleExcution(proc);
    HRegion region2 = UTIL.getMiniHBaseCluster().getRegions(tableName).get(0);
    long openSeqNum2 = region2.getOpenSeqNum();
    // confirm that the region is successfully opened
    assertTrue(openSeqNum2 > openSeqNum);
  }

  @Test
  public void testRecoveryAndDoubleExecutionReopen() throws Exception {
    MasterProcedureEnv env =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment();
    HRegionServer rs = UTIL.getRSForFirstRegionInTable(tableName);
    HRegion region = rs.getRegions(tableName).get(0);
    region.addReadRequestsCount(1);
    region.addWriteRequestsCount(2);
    long openSeqNum = region.getOpenSeqNum();
    TransitRegionStateProcedure proc =
      TransitRegionStateProcedure.reopen(env, region.getRegionInfo());
    testRecoveryAndDoubleExcution(proc);
    // should still be on the same RS
    HRegion region2 = rs.getRegions(tableName).get(0);
    long openSeqNum2 = region2.getOpenSeqNum();
    // confirm that the region is successfully opened
    assertTrue(openSeqNum2 > openSeqNum);
    // we check the available by scan after table created,
    // so the readRequestsCount should be 2 here
    assertEquals(2, region2.getReadRequestsCount());
    assertEquals(2, region2.getWriteRequestsCount());
  }

  @Test
  public void testRecoveryAndDoubleExecutionUnassignAndAssign() throws Exception {
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterProcedureEnv env = master.getMasterProcedureExecutor().getEnvironment();
    HRegion region = UTIL.getMiniHBaseCluster().getRegions(tableName).get(0);
    RegionInfo regionInfo = region.getRegionInfo();
    long openSeqNum = region.getOpenSeqNum();
    TransitRegionStateProcedure unassign = TransitRegionStateProcedure.unassign(env, regionInfo);
    testRecoveryAndDoubleExcution(unassign);
    AssignmentManager am = master.getAssignmentManager();
    assertTrue(am.getRegionStates().getRegionState(regionInfo).isClosed());

    TransitRegionStateProcedure assign = TransitRegionStateProcedure.assign(env, regionInfo, null);
    testRecoveryAndDoubleExcution(assign);

    HRegion region2 = UTIL.getMiniHBaseCluster().getRegions(tableName).get(0);
    long openSeqNum2 = region2.getOpenSeqNum();
    // confirm that the region is successfully opened
    assertTrue(openSeqNum2 > openSeqNum);
  }

  private static class BuggyStoreEngine extends DefaultStoreEngine {
    @Override
    protected void createComponents(Configuration conf, HStore store, CellComparator comparator)
      throws IOException {
      throw new IOException("I'm broken");
    }
  }

  private Set<String> getRegionStates() throws IOException {
    List<RegionInfo> regions = MetaTableAccessor.getTableRegions(UTIL.getConnection(), tableName);
    Set<String> regionStates = new HashSet<>();
    for (RegionInfo region : regions) {
      Result result = MetaTableAccessor.getRegionResult(UTIL.getConnection(), region);
      String state = Bytes.toString(
        result.getValue(HConstants.CATALOG_FAMILY, getStateColumn(region.getReplicaId())));
      regionStates.add(state);
    }
    return regionStates;
  }

  private static int getTotalRITs() throws IOException {
    final AssignmentManager am = UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    return am.computeRegionInTransitionStat().getTotalRITs();
  }

  @Test
  public void testDisableFailedOpenRegions() throws Exception {
    // Perform a faulty modification to put regions into FAILED_OPEN state
    Admin admin = UTIL.getAdmin();
    TableDescriptor td = admin.getDescriptor(tableName);
    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(td);
    tdb.setValue("hbase.hstore.engine.class", BuggyStoreEngine.class.getName());
    try {
      admin.modifyTable(tdb.build());
      fail("Should fail to modify the table");
    } catch (IOException e) {
      // expected
    }

    // Table is still "ENABLED"
    assertEquals(TableState.State.ENABLED,
      MetaTableAccessor.getTableState(UTIL.getConnection(), tableName).getState());

    // But the regions are in "FAILED_OPEN" state
    assertEquals(Collections.singleton("FAILED_OPEN"), getRegionStates());

    // We should be able to disable the table
    assertNotEquals(0, getTotalRITs());
    UTIL.getAdmin().disableTable(tableName);

    // The number of RITs should be 0 after disabling the table
    assertEquals(0, getTotalRITs());

    // The regions are now in "CLOSED" state
    assertEquals(Collections.singleton("CLOSED"), getRegionStates());

    // Fix the error in the table descriptor
    tdb = TableDescriptorBuilder.newBuilder(td);
    tdb.setValue("hbase.hstore.engine.class", DefaultStoreEngine.class.getName());
    admin.modifyTable(tdb.build());

    // We can then re-enable the table
    UTIL.getAdmin().enableTable(tableName);
    assertEquals(Collections.singleton("OPEN"), getRegionStates());
    assertEquals(0, getTotalRITs());
  }
}
