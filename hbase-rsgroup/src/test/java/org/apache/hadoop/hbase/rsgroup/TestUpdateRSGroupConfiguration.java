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
package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

@Category({ MediumTests.class })
public class TestUpdateRSGroupConfiguration extends TestRSGroupsBase {
  protected static final Logger LOG = LoggerFactory.getLogger(TestUpdateRSGroupConfiguration.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestUpdateRSGroupConfiguration.class);
  private static final String TEST_GROUP = "test";
  private static final String TEST2_GROUP = "test2";

  @BeforeClass
  public static void setUp() throws Exception {
    setUpConfigurationFiles(TEST_UTIL);
    setUpTestBeforeClass();
    addResourceToRegionServerConfiguration(TEST_UTIL);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tearDownAfterClass();
  }

  @Before
  public void beforeMethod() throws Exception {
    setUpBeforeMethod();
  }

  @After
  public void afterMethod() throws Exception {
    tearDownAfterMethod();
  }

  @Test
  public void testNonexistentRSGroup() throws Exception {
    assertThrows(IllegalArgumentException.class,
      () -> rsGroupAdmin.updateConfiguration(TEST2_GROUP));
  }

  // This test relies on a disallowed API change in RSGroupInfo and was also found to be
  // flaky. REVERTED from branch-2.5 and branch-2.
  @Test
  @Ignore
  public void testCustomOnlineConfigChangeInRSGroup() throws Exception {
    RSGroupInfo testRSGroup = addGroup(TEST_GROUP, 1);
    RSGroupInfo test2RSGroup = addGroup(TEST2_GROUP, 1);
    // Check the default configuration of the RegionServers
    TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().forEach(thread -> {
      Configuration conf = thread.getRegionServer().getConfiguration();
      assertEquals(0, conf.getInt("hbase.custom.config", 0));
    });

    replaceHBaseSiteXML();
    try {
      rsGroupAdmin.updateConfiguration(TEST_GROUP);

      Address testServerAddr = Iterables.getOnlyElement(testRSGroup.getServers());
      LOG.info("Check hbase.custom.config for " + testServerAddr);
      Configuration testRsConf = TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()
        .stream().map(JVMClusterUtil.RegionServerThread::getRegionServer)
        .filter(rs -> rs.getServerName().getAddress().equals(testServerAddr)).findFirst().get()
        .getConfiguration();
      assertEquals(1000, testRsConf.getInt("hbase.custom.config", 0));

      Address test2ServerAddr = Iterables.getOnlyElement(test2RSGroup.getServers());
      LOG.info("Check hbase.custom.config for " + test2ServerAddr);
      // Check the configuration of the RegionServer in test2 rsgroup, should not be update
      Configuration test2RsConf = TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()
        .stream().map(JVMClusterUtil.RegionServerThread::getRegionServer)
        .filter(rs -> rs.getServerName().getAddress().equals(test2ServerAddr)).findFirst().get()
        .getConfiguration();
      assertEquals(0, test2RsConf.getInt("hbase.custom.config", 0));
    } finally {
      restoreHBaseSiteXML();
    }
  }
}
