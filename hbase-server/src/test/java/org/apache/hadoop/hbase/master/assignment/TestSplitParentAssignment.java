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

import static org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil.insertData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Hbck;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Regression test for HBASE-30353: a split parent region must never be re-opened after master
 * failover.
 * <p>
 * The fix has two parts. {@code RegionStateStore.splitRegion} now writes {@code SPLIT} into
 * {@code info:state} for the parent; that is covered by {@code TestRegionStateStore}. This class
 * covers the second part, the {@code isSplit()} check in
 * {@code AssignmentManager.createAssignProcedure}. It matters for parents loaded from meta rows
 * written before the fix (state {@code CLOSED} with {@code regionInfo.isSplit()=true}), which pass
 * the expected-state check, and for forced assigns, which skip it. The tests go through the client
 * entry points, {@code Admin.assign} and {@code Hbck.assigns}.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestSplitParentAssignment {

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitParentAssignment.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final String CF = "cf";
  private static final int ROW_COUNT = 60;
  private static final int START_ROW = 11;

  private String testMethodName;

  @BeforeAll
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster(3);
  }

  @AfterAll
  public static void tearDownCluster() throws IOException {
    try {
      UTIL.shutdownMiniCluster();
    } catch (IOException e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @BeforeEach
  public void setup(TestInfo testInfo) throws Exception {
    testMethodName = testInfo.getTestMethod().get().getName();
    // Prevent CatalogJanitor from garbage collecting the split parent before the test can use it.
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(false);
    // Prevent compaction. If daughters compact away reference files, the parent becomes
    // eligible for garbage collection even with CatalogJanitor disabled.
    for (int i = 0; i < UTIL.getHBaseCluster().getLiveRegionServerThreads().size(); i++) {
      UTIL.getHBaseCluster().getRegionServer(i).getCompactSplitThread().switchCompaction(false);
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(true);
    for (TableDescriptor htd : UTIL.getAdmin().listTableDescriptors()) {
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
  public void testAdminAssignRejectsSplitParentLoadedAsClosed() throws Exception {
    TableName tableName = TableName.valueOf(testMethodName);
    AssignmentManager am = UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    RegionInfo parentInfo = splitTableAndGetParent(tableName);
    RegionStateNode rsn = simulatePostFailover(am, parentInfo, RegionState.State.CLOSED);
    RegionInfo splitParentInfo = rsn.getRegionInfo();

    Exception ex =
      assertThrows(Exception.class, () -> UTIL.getAdmin().assign(splitParentInfo.getRegionName()),
        "Admin.assign must reject a split parent even when its state is CLOSED");

    assertTrue(ex.toString().contains("is a split parent and cannot be assigned"),
      "Expected the split parent guard to reject the assign but got: " + ex);
    assertNull(rsn.getProcedure(), "No assign procedure may be attached to a split parent");
    assertEquals(RegionState.State.CLOSED, rsn.getState());
  }

  @Test
  public void testHbckForceAssignRejectsSplitParentLoadedAsClosed() throws Exception {
    TableName tableName = TableName.valueOf(testMethodName);
    AssignmentManager am = UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    RegionInfo parentInfo = splitTableAndGetParent(tableName);
    RegionStateNode rsn = simulatePostFailover(am, parentInfo, RegionState.State.CLOSED);
    RegionInfo splitParentInfo = rsn.getRegionInfo();

    List<Long> pids;
    try (Hbck hbck = UTIL.getConnection().getHbck()) {
      pids = hbck.assigns(Collections.singletonList(splitParentInfo.getEncodedName()), true, true);
    }

    assertEquals(Collections.singletonList(Procedure.NO_PROC_ID), pids,
      "Forced HBCK assign of a split parent must not submit a procedure");
    assertNull(rsn.getProcedure(), "No assign procedure may be attached to a split parent");
    assertEquals(RegionState.State.CLOSED, rsn.getState());
  }

  private RegionInfo splitTableAndGetParent(TableName tableName) throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(procExec, tableName, null, CF);
    insertData(UTIL, tableName, ROW_COUNT, START_ROW, CF);
    byte[] splitKey = Bytes.toBytes("" + (START_ROW + ROW_COUNT / 2));
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    return regions[0];
  }

  /**
   * Replaces the parent's {@link RegionStateNode} with one shaped like {@code loadMeta} would
   * rebuild it after a master failover: {@code regionInfo.isSplit()=true} and the given state. Pass
   * {@code CLOSED} to reproduce a meta row written before HBASE-30353, where the parent's
   * {@code info:state} was not {@code SPLIT}.
   */
  private RegionStateNode simulatePostFailover(AssignmentManager am, RegionInfo parentInfo,
    RegionState.State state) {
    am.getRegionStates().deleteRegion(parentInfo);
    RegionInfo splitParentInfo = RegionInfoBuilder.newBuilder(parentInfo.getTable())
      .setStartKey(parentInfo.getStartKey()).setEndKey(parentInfo.getEndKey())
      .setRegionId(parentInfo.getRegionId()).setSplit(true).setOffline(true).build();
    RegionStateNode freshRsn = am.getRegionStates().getOrCreateRegionStateNode(splitParentInfo);
    freshRsn.setState(state);
    return freshRsn;
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
