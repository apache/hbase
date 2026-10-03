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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
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
 * Regression test for HBASE-30353 / HBasePlanning #2627: a split parent region must never be
 * re-opened after master failover.
 * <p>
 * Fix has two parts: (1) {@code MetaTableAccessor.splitRegion} now writes {@code SPLIT} into
 * {@code info:state} for the parent, so after failover {@code loadMeta} reconstructs the
 * {@link RegionStateNode} with {@code state=SPLIT} rather than {@code CLOSED}. (2)
 * {@code AssignmentManager} checks {@code regionNode.isSplit()} in {@code createAssignProcedure},
 * throwing {@link DoNotRetryRegionException} before any assign can proceed regardless of the state
 * stored in meta.
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

  /**
   * Reproduces HBASE-30353: simulates the post-failover state where a new master reads the split
   * parent from meta ({@code regionInfo.isSplit()=true}, {@code state=SPLIT}), then asserts that
   * {@code assign()} rejects the parent.
   */
  @Test
  public void testAssignSplitParentIsRejected() throws Exception {
    TableName tableName = TableName.valueOf(testMethodName);
    AssignmentManager am = UTIL.getHBaseCluster().getMaster().getAssignmentManager();

    RegionInfo parentInfo = splitTableAndGetParent(tableName);
    RegionStateNode freshRsn = simulatePostFailover(am, parentInfo);
    RegionInfo splitParentInfo = freshRsn.getRegionInfo();

    assertTrue(splitParentInfo.isSplit(),
      "Precondition: regionInfo.isSplit() must be true on freshRsn");
    assertTrue(freshRsn.isSplit(), "Precondition: isSplit() must return true");
    assertEquals(RegionState.State.SPLIT, freshRsn.getState(),
      "Precondition: state must be SPLIT (written to meta since HBASE-30353)");

    DoNotRetryRegionException ex =
      assertThrows(DoNotRetryRegionException.class, () -> am.assign(splitParentInfo),
        "Expected DoNotRetryRegionException: split parent must not be assignable");
    assertTrue(ex.getMessage().contains(splitParentInfo.getEncodedName()),
      "Exception message must identify the region");
  }

  /**
   * Verifies that {@code MetaTableAccessor.splitRegion} writes {@code SPLIT} into
   * {@code info:state} for the parent, so that after a master failover {@code loadMeta} can
   * reconstruct the correct terminal state without relying solely on {@code regionInfo.isSplit()}.
   */
  @Test
  public void testSplitRegionWritesSplitStateToMeta() throws Exception {
    TableName tableName = TableName.valueOf(testMethodName);
    RegionInfo parentInfo = splitTableAndGetParent(tableName);

    Result metaRow = MetaTableAccessor.getRegionResult(UTIL.getConnection(), parentInfo);
    byte[] stateBytes = metaRow.getValue(HConstants.CATALOG_FAMILY,
      CatalogFamilyFormat.getRegionStateColumn(RegionInfo.DEFAULT_REPLICA_ID));
    RegionState.State stateInMeta = RegionState.State.valueOf(Bytes.toString(stateBytes));

    assertEquals(RegionState.State.SPLIT, stateInMeta,
      "info:state for split parent must be SPLIT in hbase:meta");
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
   * Reproduces the post-failover state: {@code loadMeta} rebuilds the parent's
   * {@link RegionStateNode} from meta with {@code regionInfo.isSplit()=true} and
   * {@code state=SPLIT}, because {@code MetaTableAccessor.splitRegion} now writes SPLIT into
   * {@code info:state} as part of the HBASE-30353 fix.
   */
  private RegionStateNode simulatePostFailover(AssignmentManager am, RegionInfo parentInfo) {
    am.getRegionStates().deleteRegion(parentInfo);
    RegionInfo splitParentInfo = RegionInfoBuilder.newBuilder(parentInfo.getTable())
      .setStartKey(parentInfo.getStartKey()).setEndKey(parentInfo.getEndKey())
      .setRegionId(parentInfo.getRegionId()).setSplit(true).setOffline(true).build();
    RegionStateNode freshRsn = am.getRegionStates().getOrCreateRegionStateNode(splitParentInfo);
    freshRsn.setState(RegionState.State.SPLIT);
    return freshRsn;
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
