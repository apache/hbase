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
package org.apache.hadoop.hbase.master;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniClusterRule;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class, MasterTests.class})
public class TestAlwaysStandByHMaster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAlwaysStandByHMaster.class);

  private static final StartMiniClusterOption OPTION = StartMiniClusterOption.builder()
    .numAlwaysStandByMasters(1)
    .numMasters(1)
    .numRegionServers(1)
    .build();

  @ClassRule
  public static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder()
    .setMiniClusterOption(OPTION)
    .build();

  /**
   * Tests that the AlwaysStandByHMaster does not transition to active state even if no active
   * master exists.
   */
  @Test  public void testAlwaysStandBy() throws Exception {
    HBaseTestingUtility testUtil = miniClusterRule.getTestingUtility();
    // Make sure there is an active master.
    assertNotNull(testUtil.getMiniHBaseCluster().getMaster());
    assertEquals(2, testUtil.getMiniHBaseCluster().getMasterThreads().size());
    // Kill the only active master.
    testUtil.getMiniHBaseCluster().stopMaster(0).join();
    // Wait for 5s to make sure the always standby doesn't transition to active state.
    assertFalse(testUtil.getMiniHBaseCluster().waitForActiveAndReadyMaster(5000));
    // Add a new master.
    HMaster newActive = testUtil.getMiniHBaseCluster().startMaster().getMaster();
    assertTrue(testUtil.getMiniHBaseCluster().waitForActiveAndReadyMaster(5000));
    // Newly added master should be the active.
    assertEquals(newActive.getServerName(),
        testUtil.getMiniHBaseCluster().getMaster().getServerName());
  }
}
