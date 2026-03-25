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

import static org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_BACKOFF_MILLIS_DEFAULT;
import static org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY;
import static org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_SIZE_MAX_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Confirm that we will batch region reopens when reopening all table regions. This can avoid the
 * pain associated with reopening too many regions at once.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestReopenTableRegionsProcedureBatching {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReopenTableRegionsProcedureBatching.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final int BACKOFF_MILLIS_PER_RS = 0;
  private static final int REOPEN_BATCH_SIZE_MAX = 1;

  private static TableName TABLE_NAME = TableName.valueOf("Batching");

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    UTIL.startMiniCluster(1);
    UTIL.createMultiRegionTable(TABLE_NAME, CF);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSmallMaxBatchSize() throws IOException {
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(TABLE_NAME);
    assertTrue(2 <= regions.size());
    Set<StuckRegion> stuckRegions =
      regions.stream().map(r -> stickRegion(am, procExec, r)).collect(Collectors.toSet());
    ReopenTableRegionsProcedure proc =
      new ReopenTableRegionsProcedure(TABLE_NAME, BACKOFF_MILLIS_PER_RS, REOPEN_BATCH_SIZE_MAX);
    procExec.submitProcedure(proc);
    UTIL.waitFor(10000, () -> proc.getState() == ProcedureState.WAITING_TIMEOUT);

    // the first batch should be small
    confirmBatchSize(1, stuckRegions, proc);
    ProcedureSyncWait.waitForProcedureToComplete(procExec, proc, 60_000);

    // other batches should also be small
    assertTrue(proc.getBatchesProcessed() >= regions.size());

    // all regions should only be opened once
    assertEquals(proc.getRegionsReopened(), regions.size());
  }

  @Test
  public void testDefaultMaxBatchSize() throws IOException {
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(TABLE_NAME);
    assertTrue(2 <= regions.size());
    Set<StuckRegion> stuckRegions =
      regions.stream().map(r -> stickRegion(am, procExec, r)).collect(Collectors.toSet());
    ReopenTableRegionsProcedure proc = new ReopenTableRegionsProcedure(TABLE_NAME);
    procExec.submitProcedure(proc);
    UTIL.waitFor(10000, () -> proc.getState() == ProcedureState.WAITING_TIMEOUT);

    // the first batch should be large
    confirmBatchSize(regions.size(), stuckRegions, proc);
    ProcedureSyncWait.waitForProcedureToComplete(procExec, proc, 60_000);

    // all regions should only be opened once
    assertEquals(proc.getRegionsReopened(), regions.size());
  }

  @Test
  public void testNegativeBatchSizeDoesNotBreak() throws IOException {
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(TABLE_NAME);
    assertTrue(2 <= regions.size());
    Set<StuckRegion> stuckRegions =
      regions.stream().map(r -> stickRegion(am, procExec, r)).collect(Collectors.toSet());
    ReopenTableRegionsProcedure proc =
      new ReopenTableRegionsProcedure(TABLE_NAME, BACKOFF_MILLIS_PER_RS, -100);
    procExec.submitProcedure(proc);
    UTIL.waitFor(10000, () -> proc.getState() == ProcedureState.WAITING_TIMEOUT);

    // the first batch should be small
    confirmBatchSize(1, stuckRegions, proc);
    ProcedureSyncWait.waitForProcedureToComplete(procExec, proc, 60_000);

    // other batches should also be small
    assertTrue(proc.getBatchesProcessed() >= regions.size());

    // all regions should only be opened once
    assertEquals(proc.getRegionsReopened(), regions.size());
  }

  @Test
  public void testBatchSizeDoesNotOverflow() {
    ReopenTableRegionsProcedure proc =
      new ReopenTableRegionsProcedure(TABLE_NAME, BACKOFF_MILLIS_PER_RS, Integer.MAX_VALUE);
    int currentBatchSize = 1;
    while (currentBatchSize < Integer.MAX_VALUE) {
      currentBatchSize = proc.progressBatchSize();
      assertTrue(currentBatchSize > 0);
    }
  }

  @Test
  public void testBackoffConfigurationFromTableDescriptor() {
    Configuration conf = HBaseConfiguration.create();
    TableDescriptorBuilder tbd = TableDescriptorBuilder.newBuilder(TABLE_NAME);

    // Default (no batching, no backoff)
    ReopenTableRegionsProcedure proc = ReopenTableRegionsProcedure.throttled(conf, tbd.build());
    assertEquals(PROGRESSIVE_BATCH_BACKOFF_MILLIS_DEFAULT, proc.getReopenBatchBackoffMillis());
    assertEquals(Integer.MAX_VALUE, proc.progressBatchSize());

    // From Configuration (backoff: 100ms, max: 6)
    conf.setLong(PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY, 100);
    conf.setInt(PROGRESSIVE_BATCH_SIZE_MAX_KEY, 6);
    proc = ReopenTableRegionsProcedure.throttled(conf, tbd.build());
    assertEquals(100, proc.getReopenBatchBackoffMillis());
    assertEquals(2, proc.progressBatchSize());
    assertEquals(4, proc.progressBatchSize());
    assertEquals(6, proc.progressBatchSize());
    assertEquals(6, proc.progressBatchSize());

    // From TableDescriptor (backoff: 200ms, max: 7)
    tbd.setValue(PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY, "200");
    tbd.setValue(PROGRESSIVE_BATCH_SIZE_MAX_KEY, "7");
    proc = ReopenTableRegionsProcedure.throttled(conf, tbd.build());
    assertEquals(200, proc.getReopenBatchBackoffMillis());
    assertEquals(2, proc.progressBatchSize());
    assertEquals(4, proc.progressBatchSize());
    assertEquals(7, proc.progressBatchSize());
    assertEquals(7, proc.progressBatchSize());
  }

  private void confirmBatchSize(int expectedBatchSize, Set<StuckRegion> stuckRegions,
    ReopenTableRegionsProcedure proc) {
    while (true) {
      if (proc.getBatchesProcessed() == 0) {
        continue;
      }
      stuckRegions.forEach(this::unstickRegion);
      UTIL.waitFor(5000, () -> expectedBatchSize == proc.getRegionsReopened());
      break;
    }
  }

  static class StuckRegion {
    final TransitRegionStateProcedure trsp;
    final RegionStateNode regionNode;
    final long openSeqNum;

    public StuckRegion(TransitRegionStateProcedure trsp, RegionStateNode regionNode,
      long openSeqNum) {
      this.trsp = trsp;
      this.regionNode = regionNode;
      this.openSeqNum = openSeqNum;
    }
  }

  private StuckRegion stickRegion(AssignmentManager am,
    ProcedureExecutor<MasterProcedureEnv> procExec, RegionInfo regionInfo) {
    RegionStateNode regionNode = am.getRegionStates().getRegionStateNode(regionInfo);
    TransitRegionStateProcedure trsp =
      TransitRegionStateProcedure.unassign(procExec.getEnvironment(), regionInfo);
    regionNode.lock();
    long openSeqNum;
    try {
      openSeqNum = regionNode.getOpenSeqNum();
      regionNode.setState(State.OPENING);
      regionNode.setOpenSeqNum(-1L);
      regionNode.setProcedure(trsp);
    } finally {
      regionNode.unlock();
    }
    return new StuckRegion(trsp, regionNode, openSeqNum);
  }

  private void unstickRegion(StuckRegion stuckRegion) {
    stuckRegion.regionNode.lock();
    try {
      stuckRegion.regionNode.setState(State.OPEN);
      stuckRegion.regionNode.setOpenSeqNum(stuckRegion.openSeqNum);
      stuckRegion.regionNode.unsetProcedure(stuckRegion.trsp);
    } finally {
      stuckRegion.regionNode.unlock();
    }
  }
}
