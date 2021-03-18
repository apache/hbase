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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, LargeTests.class})
public class TestMasterFailoverWithMetaReplica {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterFailoverWithMetaReplica.class);
  private static final int num_of_meta_replica = 2;

  /**
   * Test that during master failover, there is no state change for meta replica regions
   *
   * @throws Exception
   */
  @Test
  public void testMasterFailoverWithMetaReplica() throws Exception {
    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setInt(HConstants.META_REPLICAS_NUM, num_of_meta_replica);

    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(2).numRegionServers(num_of_meta_replica).build();
    TEST_UTIL.startMiniCluster(option);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster oldMaster = cluster.getMaster();

    // Make sure meta replica regions are assigned.
    for (int replicaId = 1; replicaId < num_of_meta_replica; replicaId++) {
      RegionInfo h = RegionReplicaUtil
        .getRegionInfoForReplica(RegionInfoBuilder.FIRST_META_REGIONINFO, replicaId);
      AssignmentTestingUtil.waitForAssignment(oldMaster.getAssignmentManager(), h);
    }

    int oldProcedureNum = oldMaster.getProcedures().size();

    int activeIndex = -1;
    List<JVMClusterUtil.MasterThread> masterThreads = cluster.getMasterThreads();

    for (int i = 0; i < masterThreads.size(); i++) {
      if (masterThreads.get(i).getMaster().isActiveMaster()) {
        activeIndex = i;
      }
    }

    // Stop the active master and wait for new master to come online.
    cluster.stopMaster(activeIndex);
    cluster.waitOnMaster(activeIndex);
    assertTrue(cluster.waitForActiveAndReadyMaster());
    // double check this is actually a new master
    HMaster newMaster = cluster.getMaster();
    assertFalse(oldMaster == newMaster);

    int newProcedureNum = newMaster.getProcedures().size();

    // Make sure all region servers report back and there is no new procedures.
    assertEquals(newMaster.getServerManager().getOnlineServers().size(), num_of_meta_replica);
    assertEquals(oldProcedureNum, newProcedureNum);

    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
}
