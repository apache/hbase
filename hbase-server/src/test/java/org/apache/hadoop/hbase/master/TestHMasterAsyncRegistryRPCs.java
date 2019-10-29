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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetPortInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetPortInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMetaLocationsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetMetaLocationsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsActiveRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsActiveResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LargeTests.class, MasterTests.class})
public class TestHMasterAsyncRegistryRPCs {
  private static final Logger LOG = LoggerFactory.getLogger(TestHMasterAsyncRegistryRPCs.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static RpcClient rpcClient;
  private static HMaster activeMaster;
  private static List<HMaster> standByMasters;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHMasterAsyncRegistryRPCs.class);

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.MASTER_PORT, "0");
    conf.setStrings(HConstants.ZOOKEEPER_ZNODE_PARENT, "/metacachetest");
    TEST_UTIL.startMiniCluster(2);
    activeMaster = TEST_UTIL.getHBaseCluster().getMaster();
    standByMasters = new ArrayList<>();
    // Create a few standby masters.
    for (int i = 0; i < 2; i++) {
      standByMasters.add(TEST_UTIL.getHBaseCluster().startMaster().getMaster());
    }
    rpcClient = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (rpcClient != null) {
      rpcClient.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Verifies that getMetaReplicaIdFromPath() parses the full meta znode paths correctly.
   * @throws IOException
   */
  @Test
  public void TestGetMetaReplicaIdFromPath() throws IOException {
    ZKWatcher zk = TEST_UTIL.getZooKeeperWatcher();
    String metaZnode=
        ZNodePaths.joinZNode(zk.getZNodePaths().baseZNode, zk.getZNodePaths().metaZNodePrefix);
    assertEquals(0, zk.getZNodePaths().getMetaReplicaIdFromPath(metaZnode));
    for (int i = 1; i < 10; i++) {
      assertEquals(i, zk.getZNodePaths().getMetaReplicaIdFromPath(metaZnode + "-" +i));
    }
    for (String suffix : new String[]{"foo", "1234", "foo-123", "123-foo-234"}) {
      try {
        final String input = metaZnode + suffix;
        zk.getZNodePaths().getMetaReplicaIdFromZnode(input);
        fail("Exception not hit getMetaReplicaIdFromZnode(): " +  input);
      } catch (NumberFormatException e) {
        // Expected
      }
    }
  }

  private void verifyGetCachedMetadataLocations(HMaster master) throws IOException {
    try {
      ServerName sm = master.getServerName();
      BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sm, User.getCurrent(), 0);
      MasterProtos.MasterService.BlockingInterface stub =
          MasterProtos.MasterService.newBlockingStub(channel);
      GetMetaLocationsResponse response = stub.getMetaLocations(null,
          GetMetaLocationsRequest.getDefaultInstance());
      assertEquals(1, response.getLocationsCount());
      HBaseProtos.RegionLocation location = response.getLocations(0);
      assertEquals(sm.getHostname(), location.getServerName().getHostName());
    } catch (ServiceException e) {
      LOG.error(
          "Error in GetCachedMetadataLocations. Active master: {}", master.isActiveMaster(), e);
      fail("Error calling GetCachedMetadataLocations()");
    }
  }

  /**
   * Verifies that both active and standby masters
   * @throws IOException
   */
  @Test
  public void TestGetCachedMetadataLocations() throws IOException {
    // Verify that the active and standby HMasters start correctly.
    assertTrue(activeMaster.serviceStarted);
    assertTrue(activeMaster.isActiveMaster());
    verifyGetCachedMetadataLocations(activeMaster);
    for (HMaster standByMaster: standByMasters) {
      assertTrue(!standByMaster.serviceStarted);
      assertTrue(!standByMaster.isActiveMaster());
      verifyGetCachedMetadataLocations(standByMaster);
    }
  }

  private void verifyIsMasterActive(HMaster master, boolean expectedResult) throws Exception {
    ServerName sm = master.getServerName();
    BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sm, User.getCurrent(), 0);
    MasterProtos.MasterService.BlockingInterface stub =
        MasterProtos.MasterService.newBlockingStub(channel);
    IsActiveResponse response = stub.isActive(null, IsActiveRequest.getDefaultInstance());
    assertEquals(expectedResult, response.getIsMasterActive());
  }

  @Test
  public void TestIsActiveRPC() throws Exception {
    verifyIsMasterActive(activeMaster, true);
    for (HMaster master: standByMasters) verifyIsMasterActive(master, false);
  }

  private void verifyMasterPorts(HMaster master) throws Exception {
    ServerName sm = master.getServerName();
    BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sm, User.getCurrent(), 0);
    MasterProtos.MasterService.BlockingInterface stub =
        MasterProtos.MasterService.newBlockingStub(channel);
    GetPortInfoResponse response = stub.getPortInfo(null, GetPortInfoRequest.getDefaultInstance());
    Configuration conf = master.getConfiguration();
    assertEquals(response.getMasterPort(),
        conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT));
    assertEquals(response.getInfoPort(),
        conf.getInt(HConstants.MASTER_INFO_PORT, HConstants.DEFAULT_MASTER_INFOPORT));
  }

  @Test
  public void TestGetPortInfoRPC() throws Exception {
    verifyMasterPorts(activeMaster);
    for (HMaster master: standByMasters) verifyMasterPorts(master);
  }

  public String getClusterId(HMaster master) throws IOException, ServiceException {
    ServerName sm = master.getServerName();
    BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sm, User.getCurrent(), 0);
    MasterProtos.MasterService.BlockingInterface stub =
        MasterProtos.MasterService.newBlockingStub(channel);
    GetClusterIdResponse response =
        stub.getClusterId(null, GetClusterIdRequest.getDefaultInstance());
    return response.getClusterId();
  }

  @Test
  public void TestClusterIdRPC() throws Exception {
    assertEquals(activeMaster.getClusterId(), getClusterId(activeMaster));
    try {
      assertTrue(standByMasters.size() > 0);
      getClusterId(standByMasters.get(0));
      fail("No exception thrown while fetching ClusterId for standby master: "
          + standByMasters.get(0).getServerName().toString());
    } catch (ServiceException e) {
      // Expected.
      assertTrue(e.getMessage().contains("Server is not running yet"));
    }
  }
}
