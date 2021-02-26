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

import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

@Category({ MasterTests.class, MediumTests.class })
public class TestReportRegionStateTransitionFromDeadServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReportRegionStateTransitionFromDeadServer.class);

  private static final List<ServerName> EXCLUDE_SERVERS = new ArrayList<>();

  private static CountDownLatch ARRIVE_GET_REGIONS;
  private static CountDownLatch RESUME_GET_REGIONS;
  private static CountDownLatch ARRIVE_REPORT;
  private static CountDownLatch RESUME_REPORT;

  private static final class ServerManagerForTest extends ServerManager {

    public ServerManagerForTest(MasterServices master) {
      super(master);
    }

    @Override
    public List<ServerName> createDestinationServersList() {
      return super.createDestinationServersList(EXCLUDE_SERVERS);
    }
  }

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master) {
      super(master);
    }

    @Override
    public List<RegionInfo> getRegionsOnServer(ServerName serverName) {
      List<RegionInfo> regions = super.getRegionsOnServer(serverName);
      if (ARRIVE_GET_REGIONS != null) {
        ARRIVE_GET_REGIONS.countDown();
        try {
          RESUME_GET_REGIONS.await();
        } catch (InterruptedException e) {
        }
      }
      return regions;
    }

    @Override
    public ReportRegionStateTransitionResponse reportRegionStateTransition(
        ReportRegionStateTransitionRequest req) throws PleaseHoldException {
      if (ARRIVE_REPORT != null && req.getTransitionList().stream()
        .allMatch(t -> !ProtobufUtil.toRegionInfo(t.getRegionInfo(0)).isMetaRegion())) {
        ARRIVE_REPORT.countDown();
        try {
          RESUME_REPORT.await();
        } catch (InterruptedException e) {
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

    @Override
    protected ServerManager createServerManager(MasterServices master) throws IOException {
      setupClusterConnection();
      return new ServerManagerForTest(master);
    }
  }

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName NAME = TableName.valueOf("Report");

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);
    UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 1000);
    UTIL.startMiniCluster(3);
    UTIL.getAdmin().balancerSwitch(false, true);
    UTIL.createTable(NAME, CF);
    UTIL.waitTableAvailable(NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws HBaseIOException, InterruptedException, ExecutionException {
    RegionInfo region = UTIL.getMiniHBaseCluster().getRegions(NAME).get(0).getRegionInfo();
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    RegionStateNode rsn = am.getRegionStates().getRegionStateNode(region);

    // move from rs0 to rs1, and then kill rs0. Later add rs1 to exclude servers, and at last verify
    // that the region should not be on rs1 and rs2 both.
    HRegionServer rs0 = UTIL.getMiniHBaseCluster().getRegionServer(rsn.getRegionLocation());
    HRegionServer rs1 = UTIL.getOtherRegionServer(rs0);
    HRegionServer rs2 = UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer()).filter(rs -> rs != rs0 && rs != rs1).findAny().get();

    RESUME_REPORT = new CountDownLatch(1);
    ARRIVE_REPORT = new CountDownLatch(1);
    Future<?> future =
      am.moveAsync(new RegionPlan(region, rs0.getServerName(), rs1.getServerName()));
    ARRIVE_REPORT.await();

    RESUME_GET_REGIONS = new CountDownLatch(1);
    ARRIVE_GET_REGIONS = new CountDownLatch(1);
    rs0.abort("For testing!");

    ARRIVE_GET_REGIONS.await();
    RESUME_REPORT.countDown();

    try {
      future.get(15, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // after the fix in HBASE-21508 we will get this exception as the TRSP can not be finished any
      // more before SCP interrupts it. It's OK.
    }

    EXCLUDE_SERVERS.add(rs1.getServerName());
    RESUME_GET_REGIONS.countDown();
    // wait until there are no running procedures, no SCP and no TRSP
    UTIL.waitFor(30000, () -> UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor()
      .getActiveProcIds().isEmpty());
    boolean onRS1 = !rs1.getRegions(NAME).isEmpty();
    boolean onRS2 = !rs2.getRegions(NAME).isEmpty();
    assertNotEquals(
      "should either be on rs1 or rs2, but onRS1 is " + onRS1 + " and on RS2 is " + onRS2, onRS1,
      onRS2);
  }
}
