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
package org.apache.hadoop.hbase.rsgroup;

import static org.apache.hadoop.hbase.rsgroup.RSGroupInfo.DEFAULT_GROUP;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

@Category({ RSGroupTests.class, MediumTests.class })
public class TestRSGroupAutoScale extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRSGroupAutoScale.class);

  private static final String GROUP1 = "g1";
  private static final String GROUP2 = "g2";

  private static RSGroupAutoScaleChore autoScaleChore;

  @BeforeClass
  public static void setUpTestBeforeClass() throws Exception {
    // 6 server in total
    NUM_SLAVES_BASE = 6;
    Configuration conf = TEST_UTIL.getConfiguration();
    // At least 2 servers in default group
    conf.setInt(RSGroupAutoScaleChore.MIN_NUM_OF_SERVER_IN_DEFAULT_GROUP_CONF_KEY, 2);
    conf.setInt(RSGroupAutoScaleChore.CONSECUTIVE_TIMES_CONF_KEY, 1);
    TestRSGroupsBase.setUpTestBeforeClass();

    autoScaleChore = new RSGroupAutoScaleChore(MASTER);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestRSGroupsBase.tearDownAfterClass();
  }

  @Before
  public void setUpBeforeMethod() throws Exception {
    super.setUpBeforeMethod();
  }

  @After
  public void tearDownAfterMethod() throws Exception {
    super.tearDownAfterMethod();
  }

  @Test
  public void testMoveServersBetweenNormalGroups() throws IOException, InterruptedException {
    // group1 has 2 servers, but requires 1 server
    addGroup(GROUP1, 2);
    updateGroupRequiredServerNum(GROUP1, 1);
    // group2 has 1 servers, but requires 2 servers
    addGroup(GROUP2, 1);
    updateGroupRequiredServerNum(GROUP2, 2);

    autoScaleChore.chore();
    // Move 1 server from group1 to group2
    assertEquals(1, ADMIN.getRSGroup(GROUP1).getServers().size());
    assertEquals(2, ADMIN.getRSGroup(GROUP2).getServers().size());
  }

  @Test
  public void testMoveServersFromDefaultGroups() throws IOException, InterruptedException {
    // group1 has 1 servers, but requires 2 servers
    addGroup(GROUP1, 1);
    updateGroupRequiredServerNum(GROUP1, 2);

    int numOfDefaultGroup = ADMIN.getRSGroup(DEFAULT_GROUP).getServers().size();

    autoScaleChore.chore();
    // Move 1 server from default group to group1
    assertEquals(2, ADMIN.getRSGroup(GROUP1).getServers().size());
    assertEquals(numOfDefaultGroup - 1, ADMIN.getRSGroup(DEFAULT_GROUP).getServers().size());
  }

  @Test
  public void testMoveServersToDefaultGroups() throws IOException, InterruptedException {
    // group1 has 2 servers, but requires 1 servers
    addGroup(GROUP1, 2);
    updateGroupRequiredServerNum(GROUP1, 1);

    int numOfDefaultGroup = ADMIN.getRSGroup(DEFAULT_GROUP).getServers().size();

    autoScaleChore.chore();
    // Move 1 server from group1 to default group
    assertEquals(1, ADMIN.getRSGroup(GROUP1).getServers().size());
    assertEquals(numOfDefaultGroup + 1, ADMIN.getRSGroup(DEFAULT_GROUP).getServers().size());
  }

  @Test
  public void testMinNumInDefaultGroup() throws IOException, InterruptedException {
    // group1 has 1 servers, but requires 5 servers
    addGroup(GROUP1, 1);
    updateGroupRequiredServerNum(GROUP1, 5);

    autoScaleChore.chore();
    // Move 3 server from default group to group1, keep at least 2 servers in default group.
    assertEquals(4, ADMIN.getRSGroup(GROUP1).getServers().size());
    assertEquals(2, ADMIN.getRSGroup(DEFAULT_GROUP).getServers().size());
  }

  private void updateGroupRequiredServerNum(String group, int num) throws IOException {
    HashMap<String, String> configuration =
      Maps.newHashMap(ADMIN.getRSGroup(group).getConfiguration());
    RSGroupConfigurationAccessor.setRequiredServerNum(configuration, num);
    ADMIN.updateRSGroupConfig(group, configuration);
  }
}
