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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MediumTests.class, ClientTests.class })
public class TestAdmin4 extends TestAdminBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAdmin4.class);

  // For HBASE-24208
  @Test
  public void testDecommissionAndStopRegionServers() throws Exception {
    List<ServerName> decommissionedRegionServers = ADMIN.listDecommissionedRegionServers();
    assertTrue(decommissionedRegionServers.isEmpty());

    ArrayList<ServerName> clusterRegionServers =
      new ArrayList<>(ADMIN.getRegionServers(true));

    List<ServerName> serversToDecommission = new ArrayList<ServerName>();
    serversToDecommission.add(clusterRegionServers.get(0));

    // Decommission
    ADMIN.decommissionRegionServers(serversToDecommission, true);
    assertEquals(1, ADMIN.listDecommissionedRegionServers().size());

    // Stop decommissioned region server and verify it is removed from draining znode
    ServerName serverName = serversToDecommission.get(0);
    ADMIN.stopRegionServer(serverName.getHostname()+":"+serverName.getPort());
    assertNotEquals("RS not removed from decommissioned list", -1,
      TEST_UTIL.waitFor(10000, () -> ADMIN.listDecommissionedRegionServers().isEmpty()));
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    assertEquals(-1, ZKUtil.checkExists(zkw,
      ZNodePaths.joinZNode(zkw.getZNodePaths().drainingZNode, serverName.getServerName())));
  }
}
