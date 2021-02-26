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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncDecommissionAdminApi extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncDecommissionAdminApi.class);

  @Test
  public void testAsyncDecommissionRegionServers() throws Exception {
    admin.balancerSwitch(false, true);
    List<ServerName> decommissionedRegionServers = admin.listDecommissionedRegionServers().get();
    assertTrue(decommissionedRegionServers.isEmpty());

    TEST_UTIL.createMultiRegionTable(tableName, FAMILY, 4);

    ArrayList<ServerName> clusterRegionServers =
        new ArrayList<>(admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).get()
          .getLiveServerMetrics().keySet());

    assertEquals(TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size(),
      clusterRegionServers.size());

    HashMap<ServerName, List<RegionInfo>> serversToDecommssion = new HashMap<>();
    // Get a server that has regions. We will decommission one of the servers,
    // leaving one online.
    int i;
    for (i = 0; i < clusterRegionServers.size(); i++) {
      List<RegionInfo> regionsOnServer = admin.getRegions(clusterRegionServers.get(i)).get();
      if (regionsOnServer.size() > 0) {
        serversToDecommssion.put(clusterRegionServers.get(i), regionsOnServer);
        break;
      }
    }

    clusterRegionServers.remove(i);
    ServerName remainingServer = clusterRegionServers.get(0);

    // Decommission
    admin.decommissionRegionServers(new ArrayList<ServerName>(serversToDecommssion.keySet()),
        true).get();
    assertEquals(1, admin.listDecommissionedRegionServers().get().size());

    // Verify the regions have been off the decommissioned servers, all on the remaining server.
    for (ServerName server : serversToDecommssion.keySet()) {
      for (RegionInfo region : serversToDecommssion.get(server)) {
        TEST_UTIL.assertRegionOnServer(region, remainingServer, 10000);
      }
    }

    // Maybe the TRSP is still not finished at master side, since the reportRegionTransition just
    // updates the procedure store, and we still need to wake up the procedure and execute it in the
    // procedure executor, which is asynchronous
    TEST_UTIL.waitUntilNoRegionsInTransition(10000);

    // Recommission and load regions
    for (ServerName server : serversToDecommssion.keySet()) {
      List<byte[]> encodedRegionNames = serversToDecommssion.get(server).stream()
          .map(region -> region.getEncodedNameAsBytes()).collect(Collectors.toList());
      admin.recommissionRegionServer(server, encodedRegionNames).get();
    }
    assertTrue(admin.listDecommissionedRegionServers().get().isEmpty());
    // Verify the regions have been moved to the recommissioned servers
    for (ServerName server : serversToDecommssion.keySet()) {
      for (RegionInfo region : serversToDecommssion.get(server)) {
        TEST_UTIL.assertRegionOnServer(region, server, 10000);
      }
    }
  }
}
