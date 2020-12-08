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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RSGroupTests.class, MediumTests.class })
public class TestRSGroupsFallback extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRSGroupsFallback.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsFallback.class);

  private static final String FALLBACK_GROUP = "fallback";

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(RSGroupBasedLoadBalancer.FALLBACK_GROUP_ENABLE_KEY, true);
    conf.setInt(HConstants.HBASE_BALANCER_MAX_BALANCING, 0);
    setUpTestBeforeClass();
    MASTER.balanceSwitch(true);
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
  public void testFallback() throws Exception {
    // add fallback group
    addGroup(FALLBACK_GROUP, 1);
    // add test group
    String groupName = getGroupName(name.getMethodName());
    addGroup(groupName, 1);
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f")).build())
      .setRegionServerGroup(groupName)
      .build();
    ADMIN.createTable(desc, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
    // server of test group crash, regions move to default group
    crashRsInGroup(groupName);
    assertRegionsInGroup(tableName, RSGroupInfo.DEFAULT_GROUP);

    // server of default group crash, regions move to any other group
    crashRsInGroup(RSGroupInfo.DEFAULT_GROUP);
    assertRegionsInGroup(tableName, FALLBACK_GROUP);

    // add a new server to default group, regions move to default group
    TEST_UTIL.getMiniHBaseCluster().startRegionServerAndWait(60000);
    assertTrue(MASTER.balance());
    assertRegionsInGroup(tableName, RSGroupInfo.DEFAULT_GROUP);

    // add a new server to test group, regions move back
    JVMClusterUtil.RegionServerThread t =
      TEST_UTIL.getMiniHBaseCluster().startRegionServerAndWait(60000);
    ADMIN.moveServersToRSGroup(
      Collections.singleton(t.getRegionServer().getServerName().getAddress()), groupName);
    assertTrue(MASTER.balance());
    assertRegionsInGroup(tableName, groupName);

    TEST_UTIL.deleteTable(tableName);
  }

  private void assertRegionsInGroup(TableName table, String group) throws IOException {
    ProcedureTestingUtility.waitAllProcedures(
      TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor());
    RSGroupInfo rsGroup = ADMIN.getRSGroup(group);
    MASTER.getAssignmentManager().getRegionStates().getRegionsOfTable(table).forEach(region -> {
      Address regionOnServer = MASTER.getAssignmentManager().getRegionStates()
          .getRegionAssignments().get(region).getAddress();
      assertTrue(rsGroup.getServers().contains(regionOnServer));
    });
  }

  private void crashRsInGroup(String groupName) throws Exception {
    for (Address server : ADMIN.getRSGroup(groupName).getServers()) {
      AssignmentTestingUtil.crashRs(TEST_UTIL, getServerName(server), true);
    }
    Threads.sleep(1000);
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
  }
}
