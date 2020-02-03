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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
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
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

@Category({ MasterTests.class, MediumTests.class })
public class TestRegionAssignedToMultipleRegionServers {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionAssignedToMultipleRegionServers.class);

  private static final List<ServerName> EXCLUDE_SERVERS = new ArrayList<>();

  private static boolean HALT = false;

  private static boolean KILL = false;

  private static CountDownLatch ARRIVE;

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
    public ReportRegionStateTransitionResponse reportRegionStateTransition(
        ReportRegionStateTransitionRequest req) throws PleaseHoldException {
      if (req.getTransition(0).getTransitionCode() == TransitionCode.OPENED) {
        if (ARRIVE != null) {
          ARRIVE.countDown();
          ARRIVE = null;
        }
        while (HALT) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          if (KILL) {
            throw new PleaseHoldException("Inject error!");
          }
        }
      }
      return super.reportRegionStateTransition(req);
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException, KeeperException {
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

  private static TableName NAME = TableName.valueOf("Assign");

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);
    UTIL
      .startMiniCluster(StartMiniClusterOption.builder().numMasters(2).numRegionServers(2).build());
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
    RegionStateNode rsn = am.getRegionStates().getRegionStateNode(region);

    ServerName sn = rsn.getRegionLocation();
    ARRIVE = new CountDownLatch(1);
    HALT = true;
    am.moveAsync(new RegionPlan(region, sn, sn));
    ARRIVE.await();

    // let's restart the master
    EXCLUDE_SERVERS.add(rsn.getRegionLocation());
    KILL = true;
    HMaster activeMaster = UTIL.getMiniHBaseCluster().getMaster();
    activeMaster.abort("For testing");
    activeMaster.getThread().join();
    KILL = false;

    // sleep a while to reproduce the problem, as after the fix in HBASE-21472 the execution logic
    // is changed so the old code to reproduce the problem can not compile...
    Thread.sleep(10000);
    HALT = false;
    Thread.sleep(5000);

    HRegionServer rs = UTIL.getMiniHBaseCluster().getRegionServer(sn);
    assertNotNull(rs.getRegion(region.getEncodedName()));
    assertNull(UTIL.getOtherRegionServer(rs).getRegion(region.getEncodedName()));
  }
}
