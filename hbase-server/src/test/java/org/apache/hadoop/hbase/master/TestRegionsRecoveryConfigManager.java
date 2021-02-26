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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for Regions Recovery Config Manager
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestRegionsRecoveryConfigManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionsRecoveryConfigManager.class);

  private static final HBaseTestingUtility HBASE_TESTING_UTILITY = new HBaseTestingUtility();

  private MiniHBaseCluster cluster;

  private HMaster hMaster;

  private RegionsRecoveryConfigManager regionsRecoveryConfigManager;

  private Configuration conf;

  @Before
  public void setup() throws Exception {
    conf = HBASE_TESTING_UTILITY.getConfiguration();
    conf.unset("hbase.regions.recovery.store.file.ref.count");
    conf.unset("hbase.master.regions.recovery.check.interval");
    StartMiniClusterOption option = StartMiniClusterOption.builder().masterClass(TestHMaster.class)
      .numRegionServers(1).numDataNodes(1).build();
    HBASE_TESTING_UTILITY.startMiniCluster(option);
    cluster = HBASE_TESTING_UTILITY.getMiniHBaseCluster();
  }

  @After
  public void tearDown() throws Exception {
    HBASE_TESTING_UTILITY.shutdownMiniCluster();
  }

  @Test
  public void testChoreSchedule() throws Exception {
    this.hMaster = cluster.getMaster();

    this.regionsRecoveryConfigManager = new RegionsRecoveryConfigManager(this.hMaster);
    // not yet scheduled
    assertFalse(
      hMaster.getChoreService().isChoreScheduled(regionsRecoveryConfigManager.getChore()));

    this.regionsRecoveryConfigManager.onConfigurationChange(conf);
    // not yet scheduled
    assertFalse(
      hMaster.getChoreService().isChoreScheduled(regionsRecoveryConfigManager.getChore()));

    conf.setInt("hbase.master.regions.recovery.check.interval", 10);
    this.regionsRecoveryConfigManager.onConfigurationChange(conf);
    // not yet scheduled - missing config: hbase.regions.recovery.store.file.ref.count
    assertFalse(
      hMaster.getChoreService().isChoreScheduled(regionsRecoveryConfigManager.getChore()));

    conf.setInt("hbase.regions.recovery.store.file.ref.count", 10);
    this.regionsRecoveryConfigManager.onConfigurationChange(conf);
    // chore scheduled
    assertTrue(hMaster.getChoreService().isChoreScheduled(regionsRecoveryConfigManager.getChore()));

    conf.setInt("hbase.regions.recovery.store.file.ref.count", 20);
    this.regionsRecoveryConfigManager.onConfigurationChange(conf);
    // chore re-scheduled
    assertTrue(hMaster.getChoreService().isChoreScheduled(regionsRecoveryConfigManager.getChore()));

    conf.setInt("hbase.regions.recovery.store.file.ref.count", 20);
    this.regionsRecoveryConfigManager.onConfigurationChange(conf);
    // chore scheduling untouched
    assertTrue(hMaster.getChoreService().isChoreScheduled(regionsRecoveryConfigManager.getChore()));

    conf.unset("hbase.regions.recovery.store.file.ref.count");
    this.regionsRecoveryConfigManager.onConfigurationChange(conf);
    // chore un-scheduled
    assertFalse(
      hMaster.getChoreService().isChoreScheduled(regionsRecoveryConfigManager.getChore()));
  }

  // Make it public so that JVMClusterUtil can access it.
  public static class TestHMaster extends HMaster {
    public TestHMaster(Configuration conf) throws IOException {
      super(conf);
    }
  }
}
