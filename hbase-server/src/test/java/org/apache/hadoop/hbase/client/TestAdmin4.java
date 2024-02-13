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
package org.apache.hadoop.hbase.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.KeeperException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos.DrainedZNodeServerData;

@Category({ MediumTests.class, ClientTests.class })
public class TestAdmin4 extends TestAdminBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestAdmin4.class);

  // For HBASE-24208
  @Test
  public void testDecommissionAndStopRegionServers() throws Exception {
    List<ServerName> decommissionedRegionServers = ADMIN.listDecommissionedRegionServers();
    assertTrue(decommissionedRegionServers.isEmpty());

    ArrayList<ServerName> clusterRegionServers = new ArrayList<>(ADMIN.getRegionServers(true));

    List<ServerName> serversToDecommission = new ArrayList<>();
    serversToDecommission.add(clusterRegionServers.get(0));

    // Decommission
    ADMIN.decommissionRegionServers(serversToDecommission, true);
    assertEquals(1, ADMIN.listDecommissionedRegionServers().size());

    // Stop decommissioned region server and verify it is removed from draining znode
    ServerName serverName = serversToDecommission.get(0);
    ADMIN.stopRegionServer(serverName.getHostname() + ":" + serverName.getPort());
    assertNotEquals("RS not removed from decommissioned list", -1,
      TEST_UTIL.waitFor(10000, () -> ADMIN.listDecommissionedRegionServers().isEmpty()));
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    assertEquals(-1, ZKUtil.checkExists(zkw,
      ZNodePaths.joinZNode(zkw.getZNodePaths().drainingZNode, serverName.getServerName())));
  }

  /**
   * TestCase for HBASE-28342
   */
  @Test
  public void testAsyncDecommissionRegionServersByHostNamesPermanently() throws Exception {
    List<ServerName> decommissionedRegionServers = ADMIN.listDecommissionedRegionServers();
    assertTrue(decommissionedRegionServers.isEmpty());

    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 6);

    ArrayList<ServerName> clusterRegionServers =
      new ArrayList<>(ADMIN.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.LIVE_SERVERS))
        .getLiveServerMetrics().keySet());

    HashMap<ServerName, List<RegionInfo>> serversToDecommission = new HashMap<>();
    // Get a server that has meta online. We will decommission two of the servers,
    // leaving one online.
    int i;
    for (i = 0; i < clusterRegionServers.size(); i++) {
      List<RegionInfo> regionsOnServer = ADMIN.getRegions(clusterRegionServers.get(i));
      if (
        ADMIN.getRegions(clusterRegionServers.get(i)).stream().anyMatch(RegionInfo::isMetaRegion)
      ) {
        serversToDecommission.put(clusterRegionServers.get(i), regionsOnServer);
        break;
      }
    }

    clusterRegionServers.remove(i);
    // Get another server to decommission.
    serversToDecommission.put(clusterRegionServers.get(0),
      ADMIN.getRegions(clusterRegionServers.get(0)));

    ServerName remainingServer = clusterRegionServers.get(1);

    // Decommission the servers with `matchHostNameOnly` set to `true` so that the hostnames are
    // always maintained as decommissioned/drained
    boolean matchHostNameOnly = true;
    ADMIN.decommissionRegionServers(new ArrayList<>(serversToDecommission.keySet()), true,
      matchHostNameOnly);
    assertEquals(2, ADMIN.listDecommissionedRegionServers().size());

    // Verify the regions have been off the decommissioned servers, all on the one
    // remaining server.
    for (ServerName server : serversToDecommission.keySet()) {
      for (RegionInfo region : serversToDecommission.get(server)) {
        TEST_UTIL.assertRegionOnServer(region, remainingServer, 10000);
      }
    }

    // Try to recommission the servers and assert that they remain decommissioned
    // No regions should be loaded on them
    recommissionRegionServers(serversToDecommission);
    // Assert that the number of decommissioned servers is still 2!
    assertEquals(2, ADMIN.listDecommissionedRegionServers().size());

    // Verify that all regions are still on the remainingServer and not on the decommissioned
    // servers
    for (ServerName server : serversToDecommission.keySet()) {
      for (RegionInfo region : serversToDecommission.get(server)) {
        TEST_UTIL.assertRegionOnServer(region, remainingServer, 10000);
      }
    }

    // Cleanup ZooKeeper's state and recommission all servers for the rest of tests
    markZnodeAsRecommissionable(serversToDecommission.keySet());
    recommissionRegionServers(serversToDecommission);
  }

  /**
   * TestCase for HBASE-28342
   */
  @Test
  public void testAsyncDecommissionRegionServersSetsHostNameMatchDataFalseInZooKeeperAsExpected()
    throws Exception {
    assertTrue(ADMIN.listDecommissionedRegionServers().isEmpty());

    ArrayList<ServerName> clusterRegionServers =
      new ArrayList<>(ADMIN.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.LIVE_SERVERS))
        .getLiveServerMetrics().keySet());

    ServerName decommissionedRegionServer = clusterRegionServers.get(0);
    clusterRegionServers.remove(0);

    // Decommission the servers with `matchHostNameOnly` set to `false` so that the hostnames are
    // not always considered as decommissioned/drained
    boolean expectedMatchHostNameOnly = false;
    ADMIN.decommissionRegionServers(Collections.singletonList(decommissionedRegionServer), true,
      expectedMatchHostNameOnly);
    assertEquals(1, ADMIN.listDecommissionedRegionServers().size());

    // Read the node's data in ZooKeeper and assert that it was set as expected
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    String znodePath = ZNodePaths.joinZNode(zkw.getZNodePaths().drainingZNode,
      decommissionedRegionServer.getServerName());
    DrainedZNodeServerData data = DrainedZNodeServerData.parseFrom(ZKUtil.getData(zkw, znodePath));
    assertEquals(expectedMatchHostNameOnly, data.getMatchHostNameOnly());

    // Recommission the server
    ADMIN.recommissionRegionServer(decommissionedRegionServer, new ArrayList<>());
    assertEquals(0, ADMIN.listDecommissionedRegionServers().size());
  }

  /**
   * TestCase for HBASE-28342
   */
  @Test
  public void testAsyncDecommissionRegionServersSetsHostNameMatchToTrueInZooKeeperAsExpected()
    throws Exception {
    assertTrue(ADMIN.listDecommissionedRegionServers().isEmpty());

    ArrayList<ServerName> clusterRegionServers =
      new ArrayList<>(ADMIN.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.LIVE_SERVERS))
        .getLiveServerMetrics().keySet());

    ServerName decommissionedRegionServer = clusterRegionServers.get(0);
    clusterRegionServers.remove(0);

    // Decommission the servers with `matchHostNameOnly` set to `true` so that the hostnames are
    // always considered as decommissioned/drained
    boolean expectedMatchHostNameOnly = true;
    ADMIN.decommissionRegionServers(Collections.singletonList(decommissionedRegionServer), true,
      expectedMatchHostNameOnly);
    assertEquals(1, ADMIN.listDecommissionedRegionServers().size());

    // Read the node's data in ZooKeeper and assert that it was set as expected
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    String znodePath = ZNodePaths.joinZNode(zkw.getZNodePaths().drainingZNode,
      decommissionedRegionServer.getServerName());
    DrainedZNodeServerData data = DrainedZNodeServerData.parseFrom(ZKUtil.getData(zkw, znodePath));
    assertEquals(expectedMatchHostNameOnly, data.getMatchHostNameOnly());

    // Reset the node's data in ZooKeeper in order to be able to recommission the server
    markZnodeAsRecommissionable(Collections.singleton(decommissionedRegionServer));
    ADMIN.recommissionRegionServer(decommissionedRegionServer, new ArrayList<>());
    assertEquals(0, ADMIN.listDecommissionedRegionServers().size());
  }

  @Test
  public void testReplicationPeerModificationSwitch() throws Exception {
    assertTrue(ADMIN.isReplicationPeerModificationEnabled());
    try {
      // disable modification, should returns true as it is enabled by default and the above
      // assertion has confirmed it
      assertTrue(ADMIN.replicationPeerModificationSwitch(false));
      IOException error =
        assertThrows(IOException.class, () -> ADMIN.addReplicationPeer("peer", ReplicationPeerConfig
          .newBuilder().setClusterKey(TEST_UTIL.getClusterKey() + "-test").build()));
      assertThat(error.getCause().getMessage(),
        containsString("Replication peer modification disabled"));
      // enable again, and the previous value should be false
      assertFalse(ADMIN.replicationPeerModificationSwitch(true));
    } finally {
      // always reset to avoid mess up other tests
      ADMIN.replicationPeerModificationSwitch(true);
    }
  }

  private void recommissionRegionServers(
    HashMap<ServerName, List<RegionInfo>> decommissionedServers) throws IOException {
    for (ServerName server : decommissionedServers.keySet()) {
      List<byte[]> encodedRegionNames = decommissionedServers.get(server).stream()
        .map(RegionInfo::getEncodedNameAsBytes).collect(Collectors.toList());
      ADMIN.recommissionRegionServer(server, encodedRegionNames);
    }
  }

  private void markZnodeAsRecommissionable(Set<ServerName> decommissionedServers)
    throws IOException {
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    for (ServerName serverName : decommissionedServers) {
      String znodePath =
        ZNodePaths.joinZNode(zkw.getZNodePaths().drainingZNode, serverName.getServerName());
      byte[] newData =
        DrainedZNodeServerData.newBuilder().setMatchHostNameOnly(false).build().toByteArray();
      try {
        ZKUtil.setData(zkw, znodePath, newData);
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
