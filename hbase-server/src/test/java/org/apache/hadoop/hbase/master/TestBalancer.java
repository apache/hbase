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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test balancer with disabled table
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestBalancer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBalancer.class);

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @Before
  public void before() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @After
  public void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAssignmentsForBalancer() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createMultiRegionTable(tableName, HConstants.CATALOG_FAMILY, 10);
    // disable table
    final TableName disableTableName = TableName.valueOf("testDisableTable");
    TEST_UTIL.createMultiRegionTable(disableTableName, HConstants.CATALOG_FAMILY, 10);
    TEST_UTIL.getAdmin().disableTable(disableTableName);

    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    AssignmentManager assignmentManager = master.getAssignmentManager();
    RegionStates regionStates = assignmentManager.getRegionStates();
    ServerName sn1 = ServerName.parseServerName("asf903.gq1.ygridcore.net,52690,1517835491385");
    regionStates.getOrCreateServer(sn1);

    TableStateManager tableStateManager = master.getTableStateManager();
    ServerManager serverManager = master.getServerManager();
    Map<TableName, Map<ServerName, List<RegionInfo>>> assignments =
      assignmentManager.getRegionStates()
        .getAssignmentsForBalancer(tableStateManager, serverManager.getOnlineServersList());
    assertFalse(assignments.containsKey(disableTableName));
    assertTrue(assignments.containsKey(tableName));
    assertFalse(assignments.get(tableName).containsKey(sn1));
  }
}
