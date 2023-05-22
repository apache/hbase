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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-23594.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestRaceBetweenSCPAndTRSP {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRaceBetweenSCPAndTRSP.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName NAME = TableName.valueOf("Race");

  private static byte[] CF = Bytes.toBytes("cf");

  private static CountDownLatch ARRIVE_REGION_OPENING;

  private static CountDownLatch RESUME_REGION_OPENING;

  private static CountDownLatch ARRIVE_GET_REGIONS_ON_SERVER;

  private static CountDownLatch RESUME_GET_REGIONS_ON_SERVER;

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master, MasterRegion masterRegion) {
      super(master, masterRegion);
    }

    @Override
    void regionOpening(RegionStateNode regionNode) throws IOException {
      super.regionOpening(regionNode);
      if (regionNode.getRegionInfo().getTable().equals(NAME) && ARRIVE_REGION_OPENING != null) {
        ARRIVE_REGION_OPENING.countDown();
        ARRIVE_REGION_OPENING = null;
        try {
          RESUME_REGION_OPENING.await();
        } catch (InterruptedException e) {
        }
      }
    }

    @Override
    public List<RegionInfo> getRegionsOnServer(ServerName serverName) {
      List<RegionInfo> regions = super.getRegionsOnServer(serverName);
      if (ARRIVE_GET_REGIONS_ON_SERVER != null) {
        ARRIVE_GET_REGIONS_ON_SERVER.countDown();
        ARRIVE_GET_REGIONS_ON_SERVER = null;
        try {
          RESUME_GET_REGIONS_ON_SERVER.await();
        } catch (InterruptedException e) {
        }
      }
      return regions;
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException, KeeperException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master,
      MasterRegion masterRegion) {
      return new AssignmentManagerForTest(master, masterRegion);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);
    UTIL.startMiniCluster(2);
    UTIL.createTable(NAME, CF);
    UTIL.waitTableAvailable(NAME);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    RegionInfo region = UTIL.getMiniHBaseCluster().getRegions(NAME).get(0).getRegionInfo();
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    ServerName sn = am.getRegionStates().getRegionState(region).getServerName();

    // Assign the CountDownLatches that get nulled in background threads else we NPE checking
    // the static.
    ARRIVE_REGION_OPENING = new CountDownLatch(1);
    CountDownLatch arriveRegionOpening = ARRIVE_REGION_OPENING;
    RESUME_REGION_OPENING = new CountDownLatch(1);
    ARRIVE_GET_REGIONS_ON_SERVER = new CountDownLatch(1);
    CountDownLatch arriveGetRegionsOnServer = ARRIVE_GET_REGIONS_ON_SERVER;
    RESUME_GET_REGIONS_ON_SERVER = new CountDownLatch(1);

    Future<byte[]> moveFuture = am.moveAsync(new RegionPlan(region, sn, sn));
    arriveRegionOpening.await();

    // Kill the region server and trigger a SCP
    UTIL.getMiniHBaseCluster().killRegionServer(sn);
    // Wait until the SCP reaches the getRegionsOnServer call
    arriveGetRegionsOnServer.await();
    RSProcedureDispatcher remoteDispatcher = UTIL.getMiniHBaseCluster().getMaster()
      .getMasterProcedureExecutor().getEnvironment().getRemoteDispatcher();
    // this is necessary for making the UT stable, the problem here is that, in
    // ServerManager.expireServer, we will submit the SCP and then the SCP will be executed in
    // another thread(the PEWorker), so when we reach the above getRegionsOnServer call in SCP, it
    // is still possible that the expireServer call has not been finished so the remote dispatcher
    // still think it can dispatcher the TRSP, in this way we will be in dead lock as the TRSP will
    // not schedule a new ORP since it relies on SCP to wake it up after everything is OK. This is
    // not what we want to test in this UT so we need to wait here to prevent this from happening.
    // See HBASE-27277 for more detailed analysis.
    UTIL.waitFor(15000, () -> !remoteDispatcher.hasNode(sn));

    // Resume the TRSP, it should be able to finish
    RESUME_REGION_OPENING.countDown();
    moveFuture.get();

    ProcedureExecutor<?> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    long scpProcId =
      procExec.getProcedures().stream().filter(p -> p instanceof ServerCrashProcedure)
        .map(p -> (ServerCrashProcedure) p).findAny().get().getProcId();
    // Resume the SCP and make sure it can finish too
    RESUME_GET_REGIONS_ON_SERVER.countDown();
    UTIL.waitFor(60000, () -> procExec.isFinished(scpProcId));
  }
}
