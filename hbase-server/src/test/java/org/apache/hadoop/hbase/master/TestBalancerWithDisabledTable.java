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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
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
public class TestBalancerWithDisabledTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBalancerWithDisabledTable.class);

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
    TableStateManager tableStateManager = master.getTableStateManager();
    Map<TableName, Map<ServerName, List<RegionInfo>>> assignments =
      assignmentManager.getRegionStates().getAssignmentsForBalancer(tableStateManager, true);
    assertFalse(assignments.containsKey(disableTableName));
    assertTrue(assignments.containsKey(tableName));

    assignments =
      assignmentManager.getRegionStates().getAssignmentsForBalancer(tableStateManager, false);
    Map<TableName, Map<ServerName, List<RegionInfo>>> tableNameMap = new HashMap<>();
    for (Map.Entry<ServerName, List<RegionInfo>> entry : assignments
      .get(HConstants.ENSEMBLE_TABLE_NAME).entrySet()) {
      final ServerName serverName = entry.getKey();
      for (RegionInfo regionInfo : entry.getValue()) {
        Map<ServerName, List<RegionInfo>> tableResult =
          tableNameMap.computeIfAbsent(regionInfo.getTable(), t -> new HashMap<>());
        List<RegionInfo> serverResult =
          tableResult.computeIfAbsent(serverName, s -> new ArrayList<>());
        serverResult.add(regionInfo);
      }
    }
    assertFalse(tableNameMap.containsKey(disableTableName));
    assertTrue(tableNameMap.containsKey(tableName));
  }
}
