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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

/**
 * A region open started by one active master must still complete after a failover to a master whose
 * clock is behind, i.e. whose active time is lower than the active time sent with the open request.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestOpenRegionReportAfterFailoverToMasterWithClockBehind {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName NAME = TableName.valueOf("ClockBehind");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static volatile CountDownLatch ARRIVE;

  /** The master that holds OPENED reports for the table until RESUME. */
  private static volatile MasterServices HOLDING_MASTER;

  private static volatile CountDownLatch RESUME;

  /** How far behind the clock of the next active master is, 0 for no skew. */
  private static volatile long SKEW_MS;

  private static final AtomicInteger SKEWED_CALLS = new AtomicInteger();

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master, MasterRegion masterRegion) {
      super(master, masterRegion);
    }

    @Override
    public ReportRegionStateTransitionResponse reportRegionStateTransition(
      ReportRegionStateTransitionRequest req) throws PleaseHoldException {
      RegionStateTransition transition = req.getTransition(0);
      if (
        getMaster() == HOLDING_MASTER && transition.getTransitionCode() == TransitionCode.OPENED
          && ProtobufUtil.toTableName(transition.getRegionInfo(0).getTableName()).equals(NAME)
      ) {
        // hold every OPENED report on the old active master until the next active master is up,
        // then fail it with PleaseHoldException, so the region server sends it again to the
        // active master
        ARRIVE.countDown();
        try {
          RESUME.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        throw new PleaseHoldException("master is gone");
      }
      return super.reportRegionStateTransition(req);
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected long getCurrentTimeForActiveTime() {
      long skew = SKEW_MS;
      if (skew > 0) {
        SKEWED_CALLS.incrementAndGet();
      }
      return super.getCurrentTimeForActiveTime() - skew;
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master,
      MasterRegion masterRegion) {
      return new AssignmentManagerForTest(master, masterRegion);
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);
    UTIL.startMiniCluster(
      StartTestingClusterOption.builder().numMasters(2).numRegionServers(2).build());
    UTIL.createTable(NAME, CF);
    UTIL.waitTableAvailable(NAME);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    SKEW_MS = 0;
    if (RESUME != null) {
      RESUME.countDown();
    }
    // The old active master still sees itself as active until it aborts, which can be after the
    // test. Stop it, or the mini cluster may send the cluster shutdown to it instead of the new
    // active master, and wait for the new active master forever.
    if (HOLDING_MASTER != null) {
      HOLDING_MASTER.stop("Stopping the old active master");
    }
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testOpenCompletesAfterFailoverToMasterWithClockBehind() throws Exception {
    HMaster oldMaster = UTIL.getMiniHBaseCluster().getMaster();
    long oldActiveTime = oldMaster.getMasterActiveTime();
    RegionInfo region = UTIL.getMiniHBaseCluster().getRegions(NAME).get(0).getRegionInfo();
    HRegionServer rs1 = UTIL.getRSForFirstRegionInTable(NAME);
    HRegionServer rs2 = UTIL.getOtherRegionServer(rs1);

    ARRIVE = new CountDownLatch(1);
    RESUME = new CountDownLatch(1);
    HOLDING_MASTER = oldMaster;
    oldMaster.getAssignmentManager()
      .moveAsync(new RegionPlan(region, rs1.getServerName(), rs2.getServerName()));
    // rs2 has opened the region and is reporting OPENED, with the old master's active time
    ARRIVE.await();

    // the next active master's clock is behind by more than the time since the old master became
    // active, so its active time will be lower than the one rs2 is reporting with
    SKEW_MS = EnvironmentEdgeManager.currentTime() - oldActiveTime + 60_000;
    oldMaster.getZooKeeper().close();
    UTIL.waitFor(120_000,
      () -> getNewActiveMaster(oldMaster) != null && getNewActiveMaster(oldMaster).isInitialized());
    SKEW_MS = 0;
    assertTrue(SKEWED_CALLS.get() > 0, "The new active master did not read the skewed clock");
    RESUME.countDown();

    HMaster newMaster = getNewActiveMaster(oldMaster);
    ProcedureExecutor<MasterProcedureEnv> procExec = newMaster.getMasterProcedureExecutor();
    UTIL.waitFor(60_000,
      () -> procExec.getProcedures().stream().anyMatch(p -> p instanceof OpenRegionProcedure
        && ((OpenRegionProcedure) p).region.getTable().equals(NAME)));
    long procId =
      procExec.getProcedures().stream()
        .filter(p -> p instanceof OpenRegionProcedure
          && ((OpenRegionProcedure) p).region.getTable().equals(NAME))
        .findFirst().get().getProcId();
    // rs2 keeps reporting OPENED with the old master's active time; the new master must accept it
    UTIL.waitFor(60_000, () -> procExec.isFinished(procId));
    UTIL.waitFor(60_000, () -> {
      RegionState state = newMaster.getAssignmentManager().getRegionStates().getRegionState(region);
      return state != null && state.isOpened() && rs2.getServerName().equals(state.getServerName());
    });
    assertTrue(newMaster.getMasterActiveTime() > oldActiveTime,
      "Active time went backwards: " + newMaster.getMasterActiveTime() + " <= " + oldActiveTime);
  }

  private static HMaster getNewActiveMaster(HMaster oldMaster) {
    for (MasterThread mt : UTIL.getMiniHBaseCluster().getMasterThreads()) {
      if (mt.getMaster() != oldMaster && mt.getMaster().isActiveMaster()) {
        return mt.getMaster();
      }
    }
    return null;
  }
}
