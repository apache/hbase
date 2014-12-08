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

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(LargeTests.class)
public class TestMasterFailoverBalancerPersistence {

  /**
   * Test that if the master fails, the load balancer maintains its
   * state (running or not) when the next master takes over
   *
   * @throws Exception
   */
  @Test(timeout = 240000)
  public void testMasterFailoverBalancerPersistence() throws Exception {
    final int NUM_MASTERS = 3;
    final int NUM_RS = 1;

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

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

    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
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
