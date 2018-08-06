/**
 *
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseTestingUtility;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Category(LargeTests.class)
public class TestMasterFailoverBalancerPersistence {
  // Start the cluster
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(4, 1);
    cluster = TEST_UTIL.getHBaseCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }


  /**
   * Test that if the master fails, the load balancer maintains its
   * state (running or not) when the next master takes over
   *
   * @throws Exception on failure
   */
  @Test(timeout = 240000)
  public void testMasterFailoverBalancerPersistence() throws Exception {

    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster active = cluster.getMaster();
    // check that the balancer is on by default for the active master
    ClusterStatus clusterStatus = active.getClusterStatus();
    assertTrue(clusterStatus.isBalancerOn());

    active = killActiveAndWaitForNewActive(cluster);

    // ensure the load balancer is still running on new master
    clusterStatus = active.getClusterStatus();
    assertTrue(clusterStatus.isBalancerOn());

    // turn off the load balancer
    active.balanceSwitch(false);

    // once more, kill active master and wait for new active master to show up
    active = killActiveAndWaitForNewActive(cluster);

    // ensure the load balancer is not running on the new master
    clusterStatus = active.getClusterStatus();
    assertFalse(clusterStatus.isBalancerOn());
  }

  /**
   * Test that if the master fails, the ReplicaMapping is rebuilt
   * by the new master.
   *
   * @throws Exception on failure
   */
  @Test(timeout = 100000)
  public void testReadReplicaMappingAfterMasterFailover() throws Exception {
    final byte [] FAMILY = Bytes.toBytes("testFamily");

    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster active = cluster.getMaster();

    final TableName tableName = TableName.valueOf("testReadReplicaMappingAfterMasterFailover");
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor(tableName.getNameAsString());
    hdt.setRegionReplication(2);
    Table ht = null;
    try {
      ht = TEST_UTIL.createTable(hdt, new byte[][] { FAMILY }, TEST_UTIL.getConfiguration());

      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
      List<HRegionLocation> allRegionLocations = locator.getAllRegionLocations();

      // There are two regions, one for primary, one for the replica.
      assertTrue(allRegionLocations.size() == 2);

      List<HRegionInfo> parentRegion = new ArrayList<>();
      parentRegion.add(allRegionLocations.get(0).getRegionInfo());
      Map<ServerName, List<HRegionInfo>> currentAssign =
          active.getAssignmentManager().getRegionStates().getRegionAssignments(parentRegion);
      Collection<List<HRegionInfo>> c = currentAssign.values();
      int count = 0;
      for (List<HRegionInfo> l : c) {
        count += l.size();
      }

      // Make sure that there are regions in the ReplicaMapping
      assertEquals(2, count);

      active = killActiveAndWaitForNewActive(cluster);

      Map<ServerName, List<HRegionInfo>> currentAssignNew =
          active.getAssignmentManager().getRegionStates().getRegionAssignments(parentRegion);
      Collection<List<HRegionInfo>> cNew = currentAssignNew.values();
      count = 0;
      for (List<HRegionInfo> l : cNew) {
        count += l.size();
      }

      // Make sure that there are regions in the ReplicaMapping when the new master takes over.
      assertEquals(2, count);
    } finally {
      if (ht != null) {
        TEST_UTIL.deleteTable(tableName.getName());
      }
    }
  }

  /**
   * Kill the master and wait for a new active master to show up
   *
   * @param cluster
   * @return the new active master
   * @throws InterruptedException
   * @throws java.io.IOException
   */
  private HMaster killActiveAndWaitForNewActive(MiniHBaseCluster cluster)
      throws InterruptedException, IOException {
    int activeIndex = getActiveMasterIndex(cluster);
    HMaster active = cluster.getMaster();
    cluster.stopMaster(activeIndex);
    cluster.waitOnMaster(activeIndex);
    assertTrue(cluster.waitForActiveAndReadyMaster());
    // double check this is actually a new master
    HMaster newActive = cluster.getMaster();
    assertFalse(active == newActive);
    return newActive;
  }

  /**
   * return the index of the active master in the cluster
   *
   * @throws org.apache.hadoop.hbase.MasterNotRunningException
   *          if no active master found
   */
  private int getActiveMasterIndex(MiniHBaseCluster cluster) throws MasterNotRunningException {
    // get all the master threads
    List<JVMClusterUtil.MasterThread> masterThreads = cluster.getMasterThreads();

    for (int i = 0; i < masterThreads.size(); i++) {
      if (masterThreads.get(i).getMaster().isActiveMaster()) {
        return i;
      }
    }
    throw new MasterNotRunningException();
  }

}
