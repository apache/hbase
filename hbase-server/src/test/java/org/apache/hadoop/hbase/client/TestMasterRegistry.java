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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@Category({ MediumTests.class, ClientTests.class })
public class TestMasterRegistry {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterRegistry.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    StartMiniClusterOption.Builder builder = StartMiniClusterOption.builder();
    builder.numMasters(3).numRegionServers(3);
    TEST_UTIL.startMiniCluster(builder.build());
    HBaseTestingUtility.setReplicas(TEST_UTIL.getAdmin(), TableName.META_TABLE_NAME, 3);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Generates a string of dummy master addresses in host:port format. Every other hostname won't
   * have a port number.
   */
  private static String generateDummyMastersList(int size) {
    List<String> masters = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      masters.add(" localhost" + (i % 2 == 0 ? ":" + (1000 + i) : ""));
    }
    return String.join(",", masters);
  }

  /**
   * Makes sure the master registry parses the master end points in the configuration correctly.
   */
  @Test
  public void testMasterAddressParsing() throws IOException {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    int numMasters = 10;
    conf.set(HConstants.MASTER_ADDRS_KEY, generateDummyMastersList(numMasters));
    try (MasterRegistry registry = new MasterRegistry(conf)) {
      List<ServerName> parsedMasters = new ArrayList<>(registry.getParsedMasterServers());
      // Half of them would be without a port, duplicates are removed.
      assertEquals(numMasters / 2 + 1, parsedMasters.size());
      // Sort in the increasing order of port numbers.
      Collections.sort(parsedMasters, Comparator.comparingInt(ServerName::getPort));
      for (int i = 0; i < parsedMasters.size(); i++) {
        ServerName sn = parsedMasters.get(i);
        assertEquals("localhost", sn.getHostname());
        if (i == parsedMasters.size() - 1) {
          // Last entry should be the one with default port.
          assertEquals(HConstants.DEFAULT_MASTER_PORT, sn.getPort());
        } else {
          assertEquals(1000 + (2 * i), sn.getPort());
        }
      }
    }
  }

  @Test
  public void testRegistryRPCs() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    HMaster activeMaster = TEST_UTIL.getHBaseCluster().getMaster();
    final int size =
      activeMaster.getMetaRegionLocationCache().getMetaRegionLocations().get().size();
    for (int numHedgedReqs = 1; numHedgedReqs <= size; numHedgedReqs++) {
      conf.setInt(MasterRegistry.MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY, numHedgedReqs);
      try (MasterRegistry registry = new MasterRegistry(conf)) {
        // Add wait on all replicas being assigned before proceeding w/ test. Failed on occasion
        // because not all replicas had made it up before test started.
        RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(TEST_UTIL, registry);
        assertEquals(registry.getClusterId().get(), activeMaster.getClusterId());
        assertEquals(registry.getActiveMaster().get(), activeMaster.getServerName());
        List<HRegionLocation> metaLocations =
          Arrays.asList(registry.getMetaRegionLocations().get().getRegionLocations());
        List<HRegionLocation> actualMetaLocations =
          activeMaster.getMetaRegionLocationCache().getMetaRegionLocations().get();
        Collections.sort(metaLocations);
        Collections.sort(actualMetaLocations);
        assertEquals(actualMetaLocations, metaLocations);
      }
    }
  }

  /**
   * Tests that the list of masters configured in the MasterRegistry is dynamically refreshed in the
   * event of errors.
   */
  @Test
  public void testDynamicMasterConfigurationRefresh() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    String currentMasterAddrs = Preconditions.checkNotNull(conf.get(HConstants.MASTER_ADDRS_KEY));
    HMaster activeMaster = TEST_UTIL.getHBaseCluster().getMaster();
    String clusterId = activeMaster.getClusterId();
    // Add a non-working master
    ServerName badServer = ServerName.valueOf("localhost", 1234, -1);
    conf.set(HConstants.MASTER_ADDRS_KEY, badServer.toShortString() + "," + currentMasterAddrs);
    // Set the hedging fan out so that all masters are queried.
    conf.setInt(MasterRegistry.MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY, 4);
    // Do not limit the number of refreshes during the test run.
    conf.setLong(MasterAddressRefresher.MIN_SECS_BETWEEN_REFRESHES, 0);
    try (MasterRegistry registry = new MasterRegistry(conf)) {
      final Set<ServerName> masters = registry.getParsedMasterServers();
      assertTrue(masters.contains(badServer));
      // Make a registry RPC, this should trigger a refresh since one of the hedged RPC fails.
      assertEquals(registry.getClusterId().get(), clusterId);
      // Wait for new set of masters to be populated.
      TEST_UTIL.waitFor(5000,
          (Waiter.Predicate<Exception>) () -> !registry.getParsedMasterServers().equals(masters));
      // new set of masters should not include the bad server
      final Set<ServerName> newMasters = registry.getParsedMasterServers();
      // Bad one should be out.
      assertEquals(3, newMasters.size());
      assertFalse(newMasters.contains(badServer));
      // Kill the active master
      activeMaster.stopMaster();
      TEST_UTIL.waitFor(10000,
        () -> TEST_UTIL.getMiniHBaseCluster().getLiveMasterThreads().size() == 2);
      TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster(10000);
      // Wait until the killed master de-registered. This should also trigger another refresh.
      TEST_UTIL.waitFor(10000, () -> registry.getMasters().get().size() == 2);
      TEST_UTIL.waitFor(20000, () -> registry.getParsedMasterServers().size() == 2);
      final Set<ServerName> newMasters2 = registry.getParsedMasterServers();
      assertEquals(2, newMasters2.size());
      assertFalse(newMasters2.contains(activeMaster.getServerName()));
    } finally {
      // Reset the state, add a killed master.
      TEST_UTIL.getMiniHBaseCluster().startMaster();
    }
  }
}
