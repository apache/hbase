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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
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

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static byte[] CF = Bytes.toBytes("cf");

  @Rule
  public TestName name = new TestName();

  private TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
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
    region.setReadRequestsCount(1);
    region.setWriteRequestsCount(2);
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
}
