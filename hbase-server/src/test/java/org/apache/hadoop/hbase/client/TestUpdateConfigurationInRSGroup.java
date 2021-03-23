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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MediumTests.class})
public class TestUpdateConfigurationInRSGroup extends TestAsyncAdminBase {
  protected static final Logger LOG = LoggerFactory.getLogger(TestUpdateConfigurationInRSGroup.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestUpdateConfigurationInRSGroup.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Admin ADMIN;
  private static final String TEST_GROUP = "test";
  private static final String TEST2_GROUP = "test2";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setUpConfigurationFiles(TEST_UTIL);
    RSGroupUtil.enableRSGroup(TEST_UTIL.getConfiguration());
    StartMiniClusterOption option = StartMiniClusterOption.builder().numRegionServers(3)
      .numMasters(2).build();
    TEST_UTIL.startMiniCluster(option);
    addResourceToRegionServerConfiguration(TEST_UTIL);
    ADMIN = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    deleteGroups();
  }

  @Test
  public void testOnlineConfigChangeInRSGroup() throws Exception {
    addGroup(TEST_GROUP, 1);
    ADMIN.updateConfiguration(TEST_GROUP);
  }

  @Test
  public void testNonexistentRSGroup() throws Exception {
    try {
      ADMIN.updateConfiguration(TEST2_GROUP);
      fail("Group does not exist: test2");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testCustomOnlineConfigChangeInRSGroup() throws Exception {
    // Check the default configuration of the RegionServers
    TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().forEach(thread -> {
      Configuration conf = thread.getRegionServer().getConfiguration();
      assertEquals(0, conf.getInt("hbase.custom.config", 0));
    });

    replaceHBaseSiteXML();
    RSGroupInfo testRSGroup = addGroup(TEST_GROUP, 1);
    RSGroupInfo test2RSGroup = addGroup(TEST2_GROUP, 1);
    ADMIN.updateConfiguration(TEST_GROUP);

    // Check the configuration of the RegionServer in test rsgroup, should be update
    Configuration regionServerConfiguration =
      TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
      .map(JVMClusterUtil.RegionServerThread::getRegionServer)
      .filter(regionServer ->
        (regionServer.getServerName().getAddress().equals(testRSGroup.getServers().first())))
      .collect(Collectors.toList()).get(0).getConfiguration();
    int custom = regionServerConfiguration.getInt("hbase.custom.config", 0);
    assertEquals(1000, custom);

    // Check the configuration of the RegionServer in test2 rsgroup, should not be update
    regionServerConfiguration =
      TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream()
        .map(JVMClusterUtil.RegionServerThread::getRegionServer)
        .filter(regionServer ->
          (regionServer.getServerName().getAddress().equals(test2RSGroup.getServers().first())))
        .collect(Collectors.toList()).get(0).getConfiguration();
    custom = regionServerConfiguration.getInt("hbase.custom.config", 0);
    assertEquals(0, custom);

    restoreHBaseSiteXML();
  }

  private RSGroupInfo addGroup(String groupName, int serverCount) throws IOException {
    RSGroupInfo defaultInfo = ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    ADMIN.addRSGroup(groupName);
    Set<Address> set = new HashSet<>();
    for (Address server : defaultInfo.getServers()) {
      if (set.size() == serverCount) {
        break;
      }
      set.add(server);
    }
    ADMIN.moveServersToRSGroup(set, groupName);
    return ADMIN.getRSGroup(groupName);
  }

  private void deleteGroups() throws IOException {
    for (RSGroupInfo groupInfo : ADMIN.listRSGroups()) {
      if (!groupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        removeGroup(groupInfo.getName());
      }
    }
  }

  private void removeGroup(String groupName) throws IOException {
    RSGroupInfo groupInfo = ADMIN.getRSGroup(groupName);
    ADMIN.moveServersToRSGroup(groupInfo.getServers(), RSGroupInfo.DEFAULT_GROUP);
    ADMIN.removeRSGroup(groupName);
  }
}
