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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionServerList;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IdLock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

/**
 * Testcase for HBASE-22365.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestSCPGetRegionsRace {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSCPGetRegionsRace.class);

  private static final List<ServerName> EXCLUDE_SERVERS = new ArrayList<>();

  private static final class ServerManagerForTest extends ServerManager {

    public ServerManagerForTest(MasterServices master, RegionServerList storage) {
      super(master, storage);
    }

    @Override
    public List<ServerName> createDestinationServersList() {
      return super.createDestinationServersList(EXCLUDE_SERVERS);
    }
  }

  private static CountDownLatch ARRIVE_REPORT;

  private static CountDownLatch RESUME_REPORT;

  private static CountDownLatch ARRIVE_GET;

  private static CountDownLatch RESUME_GET;

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master, MasterRegion masterRegion) {
      super(master, masterRegion);
    }

    @Override
    public ReportRegionStateTransitionResponse reportRegionStateTransition(
        ReportRegionStateTransitionRequest req) throws PleaseHoldException {
      if (req.getTransition(0).getTransitionCode() == TransitionCode.CLOSED) {
        if (ARRIVE_REPORT != null) {
          ARRIVE_REPORT.countDown();
          try {
            RESUME_REPORT.await();
            RESUME_REPORT = null;
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
      return super.reportRegionStateTransition(req);
    }

    @Override
    public List<RegionInfo> getRegionsOnServer(ServerName serverName) {
      List<RegionInfo> regions = super.getRegionsOnServer(serverName);
      if (ARRIVE_GET != null) {
        ARRIVE_GET.countDown();
        try {
          RESUME_GET.await();
          RESUME_GET = null;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return regions;
    }

  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master,
      MasterRegion masterRegion) {
      return new AssignmentManagerForTest(master, masterRegion);
    }

    @Override
    protected ServerManager createServerManager(MasterServices master,
      RegionServerList storage) throws IOException {
      setupClusterConnection();
      return new ServerManagerForTest(master, storage);
    }
  }

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName NAME = TableName.valueOf("Assign");

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(StartTestingClusterOption.builder().masterClass(HMasterForTest.class)
      .numMasters(1).numRegionServers(3).build());
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
    RegionInfo region =
      Iterables.getOnlyElement(UTIL.getMiniHBaseCluster().getRegions(NAME)).getRegionInfo();
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    AssignmentManager am = master.getAssignmentManager();
    RegionStateNode rsn = am.getRegionStates().getRegionStateNode(region);
    ServerName source = rsn.getRegionLocation();
    ServerName dest =
      UTIL.getAdmin().getRegionServers().stream().filter(sn -> !sn.equals(source)).findAny().get();

    ARRIVE_REPORT = new CountDownLatch(1);
    RESUME_REPORT = new CountDownLatch(1);

    Future<?> future = am.moveAsync(new RegionPlan(region, source, dest));

    ARRIVE_REPORT.await();
    ARRIVE_REPORT = null;
    // let's get procedure lock to stop the TRSP
    IdLock procExecutionLock = master.getMasterProcedureExecutor().getProcExecutionLock();
    long procId = master.getProcedures().stream()
      .filter(p -> p instanceof RegionRemoteProcedureBase).findAny().get().getProcId();
    IdLock.Entry lockEntry = procExecutionLock.getLockEntry(procId);
    RESUME_REPORT.countDown();

    // kill the source region server
    ARRIVE_GET = new CountDownLatch(1);
    RESUME_GET = new CountDownLatch(1);
    UTIL.getMiniHBaseCluster().killRegionServer(source);

    // wait until we try to get the region list of the region server
    ARRIVE_GET.await();
    ARRIVE_GET = null;
    // release the procedure lock and let the TRSP to finish
    procExecutionLock.releaseLockEntry(lockEntry);
    future.get();

    // resume the SCP
    EXCLUDE_SERVERS.add(dest);
    RESUME_GET.countDown();
    // wait until there are no SCPs and TRSPs
    UTIL.waitFor(60000, () -> master.getProcedures().stream().allMatch(p -> p.isFinished() ||
      (!(p instanceof ServerCrashProcedure) && !(p instanceof TransitRegionStateProcedure))));

    // assert the region is only on the dest server.
    HRegionServer rs = UTIL.getMiniHBaseCluster().getRegionServer(dest);
    assertNotNull(rs.getRegion(region.getEncodedName()));
    assertNull(UTIL.getOtherRegionServer(rs).getRegion(region.getEncodedName()));
  }
}
