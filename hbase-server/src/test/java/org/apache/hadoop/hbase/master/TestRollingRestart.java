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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Tests the restarting of everything as done during rolling restarts.
 */
@RunWith(Parameterized.class)
@Category({MasterTests.class, LargeTests.class})
public class TestRollingRestart {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRollingRestart.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRollingRestart.class);

  @Rule
  public TestName name = new TestName();

  @Parameterized.Parameter
  public boolean splitWALCoordinatedByZK;

  @Test
  public void testBasicRollingRestart() throws Exception {

    // Start a cluster with 2 masters and 4 regionservers
    final int NUM_MASTERS = 2;
    final int NUM_RS = 3;
    final int NUM_REGIONS_TO_CREATE = 20;

    int expectedNumRS = 3;

    // Start the cluster
    log("Starting cluster");
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK,
        splitWALCoordinatedByZK);
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(NUM_MASTERS).numRegionServers(NUM_RS).numDataNodes(NUM_RS).build();
    TEST_UTIL.startMiniCluster(option);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();

    // Create a table with regions
    final TableName tableName =
        TableName.valueOf(name.getMethodName().replaceAll("[\\[|\\]]", "-"));
    byte [] family = Bytes.toBytes("family");
    log("Creating table with " + NUM_REGIONS_TO_CREATE + " regions");
    Table ht = TEST_UTIL.createMultiRegionTable(tableName, family, NUM_REGIONS_TO_CREATE);
    int numRegions = -1;
    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      numRegions = r.getStartKeys().length;
    }
    numRegions += 1; // catalogs
    log("Waiting for no more RIT\n");
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    log("Disabling table\n");
    TEST_UTIL.getAdmin().disableTable(tableName);
    log("Waiting for no more RIT\n");
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    NavigableSet<String> regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    log("Verifying only catalog and namespace regions are assigned\n");
    if (regions.size() != 2) {
      for (String oregion : regions) log("Region still online: " + oregion);
    }
    assertEquals(2, regions.size());
    log("Enabling table\n");
    TEST_UTIL.getAdmin().enableTable(tableName);
    log("Waiting for no more RIT\n");
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    log("Verifying there are " + numRegions + " assigned on cluster\n");
    regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    assertRegionsAssigned(cluster, regions);
    assertEquals(expectedNumRS, cluster.getRegionServerThreads().size());

    // Add a new regionserver
    log("Adding a fourth RS");
    RegionServerThread restarted = cluster.startRegionServer();
    expectedNumRS++;
    restarted.waitForServerOnline();
    log("Additional RS is online");
    log("Waiting for no more RIT");
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    log("Verifying there are " + numRegions + " assigned on cluster");
    assertRegionsAssigned(cluster, regions);
    assertEquals(expectedNumRS, cluster.getRegionServerThreads().size());

    // Master Restarts
    List<MasterThread> masterThreads = cluster.getMasterThreads();
    MasterThread activeMaster = null;
    MasterThread backupMaster = null;
    assertEquals(2, masterThreads.size());
    if (masterThreads.get(0).getMaster().isActiveMaster()) {
      activeMaster = masterThreads.get(0);
      backupMaster = masterThreads.get(1);
    } else {
      activeMaster = masterThreads.get(1);
      backupMaster = masterThreads.get(0);
    }

    // Bring down the backup master
    log("Stopping backup master\n\n");
    backupMaster.getMaster().stop("Stop of backup during rolling restart");
    cluster.hbaseCluster.waitOnMaster(backupMaster);

    // Bring down the primary master
    log("Stopping primary master\n\n");
    activeMaster.getMaster().stop("Stop of active during rolling restart");
    cluster.hbaseCluster.waitOnMaster(activeMaster);

    // Start primary master
    log("Restarting primary master\n\n");
    activeMaster = cluster.startMaster();
    cluster.waitForActiveAndReadyMaster();

    // Start backup master
    log("Restarting backup master\n\n");
    backupMaster = cluster.startMaster();

    assertEquals(expectedNumRS, cluster.getRegionServerThreads().size());

    // RegionServer Restarts

    // Bring them down, one at a time, waiting between each to complete
    List<RegionServerThread> regionServers =
      cluster.getLiveRegionServerThreads();
    int num = 1;
    int total = regionServers.size();
    for (RegionServerThread rst : regionServers) {
      ServerName serverName = rst.getRegionServer().getServerName();
      log("Stopping region server " + num + " of " + total + " [ " +
          serverName + "]");
      rst.getRegionServer().stop("Stopping RS during rolling restart");
      cluster.hbaseCluster.waitOnRegionServer(rst);
      log("Waiting for RS shutdown to be handled by master");
      waitForRSShutdownToStartAndFinish(activeMaster, serverName);
      log("RS shutdown done, waiting for no more RIT");
      TEST_UTIL.waitUntilNoRegionsInTransition(60000);
      log("Verifying there are " + numRegions + " assigned on cluster");
      assertRegionsAssigned(cluster, regions);
      expectedNumRS--;
      assertEquals(expectedNumRS, cluster.getRegionServerThreads().size());
      log("Restarting region server " + num + " of " + total);
      restarted = cluster.startRegionServer();
      restarted.waitForServerOnline();
      expectedNumRS++;
      log("Region server " + num + " is back online");
      log("Waiting for no more RIT");
      TEST_UTIL.waitUntilNoRegionsInTransition(60000);
      log("Verifying there are " + numRegions + " assigned on cluster");
      assertRegionsAssigned(cluster, regions);
      assertEquals(expectedNumRS, cluster.getRegionServerThreads().size());
      num++;
    }
    Thread.sleep(1000);
    assertRegionsAssigned(cluster, regions);

    // TODO: Bring random 3 of 4 RS down at the same time

    ht.close();
    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
  }

  private void waitForRSShutdownToStartAndFinish(MasterThread activeMaster,
      ServerName serverName) throws InterruptedException {
    ServerManager sm = activeMaster.getMaster().getServerManager();
    // First wait for it to be in dead list
    while (!sm.getDeadServers().isDeadServer(serverName)) {
      log("Waiting for [" + serverName + "] to be listed as dead in master");
      Thread.sleep(1);
    }
    log("Server [" + serverName + "] marked as dead, waiting for it to " +
        "finish dead processing");
    while (sm.areDeadServersInProgress()) {
      log("Server [" + serverName + "] still being processed, waiting");
      Thread.sleep(100);
    }
    log("Server [" + serverName + "] done with server shutdown processing");
  }

  private void log(String msg) {
    LOG.debug("\n\nTRR: " + msg + "\n");
  }

  private int getNumberOfOnlineRegions(MiniHBaseCluster cluster) {
    int numFound = 0;
    for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
      numFound += rst.getRegionServer().getNumberOfOnlineRegions();
    }
    for (MasterThread mt : cluster.getMasterThreads()) {
      numFound += mt.getMaster().getNumberOfOnlineRegions();
    }
    return numFound;
  }

  private void assertRegionsAssigned(MiniHBaseCluster cluster,
      Set<String> expectedRegions) throws IOException {
    int numFound = getNumberOfOnlineRegions(cluster);
    if (expectedRegions.size() > numFound) {
      log("Expected to find " + expectedRegions.size() + " but only found"
          + " " + numFound);
      NavigableSet<String> foundRegions =
        HBaseTestingUtility.getAllOnlineRegions(cluster);
      for (String region : expectedRegions) {
        if (!foundRegions.contains(region)) {
          log("Missing region: " + region);
        }
      }
      assertEquals(expectedRegions.size(), numFound);
    } else if (expectedRegions.size() < numFound) {
      int doubled = numFound - expectedRegions.size();
      log("Expected to find " + expectedRegions.size() + " but found"
          + " " + numFound + " (" + doubled + " double assignments?)");
      NavigableSet<String> doubleRegions = getDoubleAssignedRegions(cluster);
      for (String region : doubleRegions) {
        log("Region is double assigned: " + region);
      }
      assertEquals(expectedRegions.size(), numFound);
    } else {
      log("Success!  Found expected number of " + numFound + " regions");
    }
  }

  private NavigableSet<String> getDoubleAssignedRegions(
      MiniHBaseCluster cluster) throws IOException {
    NavigableSet<String> online = new TreeSet<>();
    NavigableSet<String> doubled = new TreeSet<>();
    for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
      for (RegionInfo region : ProtobufUtil.getOnlineRegions(
          rst.getRegionServer().getRSRpcServices())) {
        if(!online.add(region.getRegionNameAsString())) {
          doubled.add(region.getRegionNameAsString());
        }
      }
    }
    return doubled;
  }


  @Parameterized.Parameters
  public static Collection coordinatedByZK() {
    return Arrays.asList(false, true);
  }
}

