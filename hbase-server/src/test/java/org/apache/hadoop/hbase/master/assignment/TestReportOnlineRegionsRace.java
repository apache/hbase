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

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED_VALUE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IdLock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

@Category({ MasterTests.class, MediumTests.class })
public class TestReportOnlineRegionsRace {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReportOnlineRegionsRace.class);

  private static volatile CountDownLatch ARRIVE_RS_REPORT;
  private static volatile CountDownLatch RESUME_RS_REPORT;
  private static volatile CountDownLatch FINISH_RS_REPORT;

  private static volatile CountDownLatch RESUME_REPORT_STATE;

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master) {
      super(master);
    }

    @Override
    public void reportOnlineRegions(ServerName serverName, Set<byte[]> regionNames) {
      if (ARRIVE_RS_REPORT != null) {
        ARRIVE_RS_REPORT.countDown();
        try {
          RESUME_RS_REPORT.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      super.reportOnlineRegions(serverName, regionNames);
      if (FINISH_RS_REPORT != null) {
        FINISH_RS_REPORT.countDown();
      }
    }

    @Override
    public ReportRegionStateTransitionResponse reportRegionStateTransition(
        ReportRegionStateTransitionRequest req) throws PleaseHoldException {
      if (RESUME_REPORT_STATE != null) {
        try {
          RESUME_REPORT_STATE.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return super.reportRegionStateTransition(req);
    }

  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master) {
      return new AssignmentManagerForTest(master);
    }
  }

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName NAME = TableName.valueOf("Race");

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);
    UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 1000);
    UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT,
        HConstants.DEFAULT_REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT);
    UTIL.startMiniCluster(1);
    UTIL.createTable(NAME, CF);
    UTIL.waitTableAvailable(NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRace() throws Exception {
    RegionInfo region = UTIL.getMiniHBaseCluster().getRegions(NAME).get(0).getRegionInfo();
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    RegionStateNode rsn = am.getRegionStates().getRegionStateNode(region);

    // halt a regionServerReport
    RESUME_RS_REPORT = new CountDownLatch(1);
    ARRIVE_RS_REPORT = new CountDownLatch(1);
    FINISH_RS_REPORT = new CountDownLatch(1);

    ARRIVE_RS_REPORT.await();

    // schedule a TRSP to REOPEN the region
    RESUME_REPORT_STATE = new CountDownLatch(1);
    Future<byte[]> future =
      am.moveAsync(new RegionPlan(region, rsn.getRegionLocation(), rsn.getRegionLocation()));
    TransitRegionStateProcedure proc =
      procExec.getProcedures().stream().filter(p -> p instanceof TransitRegionStateProcedure)
        .filter(p -> !p.isFinished()).map(p -> (TransitRegionStateProcedure) p).findAny().get();
    IdLock procExecLock = procExec.getProcExecutionLock();
    // a CloseRegionProcedure and then the OpenRegionProcedure we want to block
    IdLock.Entry lockEntry = procExecLock.getLockEntry(proc.getProcId() + 2);
    // resume the reportRegionStateTransition to finish the CloseRegionProcedure
    RESUME_REPORT_STATE.countDown();
    // wait until we schedule the OpenRegionProcedure
    UTIL.waitFor(10000,
      () -> proc.getCurrentStateId() == REGION_STATE_TRANSITION_CONFIRM_OPENED_VALUE);
    // the region should be in OPENING state
    assertEquals(RegionState.State.OPENING, rsn.getState());
    // resume the region server report
    RESUME_RS_REPORT.countDown();
    // wait until it finishes, it will find that the region is opened on the rs
    FINISH_RS_REPORT.await();
    // let the OpenRegionProcedure go
    procExecLock.releaseLockEntry(lockEntry);
    // wait until the TRSP is done
    future.get();

    // confirm that the region can still be write, i.e, the regionServerReport method should not
    // change the region state to OPEN
    try (Table table = UTIL.getConnection().getTableBuilder(NAME, null).setWriteRpcTimeout(1000)
      .setOperationTimeout(2000).build()) {
      table.put(
        new Put(Bytes.toBytes("key")).addColumn(CF, Bytes.toBytes("cq"), Bytes.toBytes("val")));
    }
  }
}
