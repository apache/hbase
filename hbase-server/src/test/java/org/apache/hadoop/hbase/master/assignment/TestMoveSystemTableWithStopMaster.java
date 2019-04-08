/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.assignment;

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MoveRegionState.MOVE_REGION_ASSIGN;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;

@Category({LargeTests.class})
public class TestMoveSystemTableWithStopMaster {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestMoveSystemTableWithStopMaster.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMoveSystemTableWithStopMaster.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1, 2);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMoveMetaRegoinWithStopMaster() throws Exception {
    ClusterConnection conn = (ClusterConnection) UTIL.getConnection();
    MiniHBaseCluster miniHBaseCluster = UTIL.getHBaseCluster();

    List<HRegionLocation> namespaceRegionLocations = conn.locateRegions(TableName.META_TABLE_NAME);

    RegionInfo regionInfo = namespaceRegionLocations.get(0).getRegion();
    ServerName source = namespaceRegionLocations.get(0).getServerName();
    ServerName dstServerName = UTIL.getOtherRegionServer(
      miniHBaseCluster.getRegionServer(source)).getServerName();

    RegionPlan rp = new RegionPlan(regionInfo, source, dstServerName);

    HMaster master = UTIL.getHBaseCluster().getMaster();

    CountDownLatch moveRegionAssignLatch = new CountDownLatch(1);
    CountDownLatch masterAbortLatch = new CountDownLatch(1);

    MoveRegionProcedureHoldBeforeAssign proc = new MoveRegionProcedureHoldBeforeAssign(
      master.getMasterProcedureExecutor().getEnvironment(), rp, true);

    proc.moveRegionAssignLatch = moveRegionAssignLatch;
    proc.masterStoppedLatch = masterAbortLatch;

    ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), proc);

    moveRegionAssignLatch.await();
    master.abort("for test");
    // may not closed, and rs still conn to old master
    master.getEventLoopGroupConfig().group().shutdownGracefully();
    miniHBaseCluster.waitForMasterToStop(master.getServerName(), 30000);
    masterAbortLatch.countDown();

    UTIL.getMiniHBaseCluster().startMaster();

    // master should be initialized in 30 seconds
    Assert.assertTrue(miniHBaseCluster.waitForActiveAndReadyMaster(60000));
  }


  @Test
  public void testMoveNamespaceRegoinWithStopMaster() throws Exception {
    ClusterConnection conn = (ClusterConnection) UTIL.getConnection();
    MiniHBaseCluster miniHBaseCluster = UTIL.getHBaseCluster();

    List<HRegionLocation> namespaceRegionLocations = conn.locateRegions(
      TableName.NAMESPACE_TABLE_NAME);

    RegionInfo regionInfo = namespaceRegionLocations.get(0).getRegion();
    ServerName source = namespaceRegionLocations.get(0).getServerName();
    ServerName dstServerName = UTIL.getOtherRegionServer(
      miniHBaseCluster.getRegionServer(source)).getServerName();

    RegionPlan rp = new RegionPlan(regionInfo, source, dstServerName);

    HMaster master = UTIL.getHBaseCluster().getMaster();

    CountDownLatch moveRegionAssignLatch = new CountDownLatch(1);
    CountDownLatch masterAbortLatch = new CountDownLatch(1);

    MoveRegionProcedureHoldBeforeAssign proc = new MoveRegionProcedureHoldBeforeAssign(
      master.getMasterProcedureExecutor().getEnvironment(), rp, true);

    proc.moveRegionAssignLatch = moveRegionAssignLatch;
    proc.masterStoppedLatch = masterAbortLatch;

    ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), proc);

    moveRegionAssignLatch.await();
    master.abort("for test");
    miniHBaseCluster.waitForMasterToStop(master.getServerName(), 30000);
    masterAbortLatch.countDown();

    UTIL.getMiniHBaseCluster().startMaster();

    // master should be initialized in 60 seconds
    Assert.assertTrue(miniHBaseCluster.waitForActiveAndReadyMaster(60000));
  }

  public static class MoveRegionProcedureHoldBeforeAssign extends MoveRegionProcedure {
    public MoveRegionProcedureHoldBeforeAssign() {
    // Required by the Procedure framework to create the procedure on replay
      super();
    }

    CountDownLatch masterStoppedLatch;

    CountDownLatch moveRegionAssignLatch;

    public MoveRegionProcedureHoldBeforeAssign(MasterProcedureEnv env,
      RegionPlan plan, boolean check) throws HBaseIOException {
      super(env, plan, check);
    }

    protected Flow executeFromState(final MasterProcedureEnv env,
      final MasterProcedureProtos.MoveRegionState state) throws InterruptedException {
      if (state == MOVE_REGION_ASSIGN) {
        if (moveRegionAssignLatch != null) {
          moveRegionAssignLatch.countDown();
          masterStoppedLatch.await();
        }
      }
      return super.executeFromState(env, state);
    }
  }
}