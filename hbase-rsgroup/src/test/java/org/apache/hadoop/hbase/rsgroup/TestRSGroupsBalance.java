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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({ MediumTests.class })
public class TestRSGroupsBalance extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupsBalance.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsBalance.class);

  @BeforeClass
  public static void setUp() throws Exception {
    setUpTestBeforeClass();
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
  public void testGroupBalance() throws Exception {
    LOG.info(name.getMethodName());
    String newGroupName = getGroupName(name.getMethodName());
    addGroup(newGroupName, 3);

    final TableName tableName = TableName.valueOf(tablePrefix + "_ns", name.getMethodName());
    admin.createNamespace(NamespaceDescriptor.create(tableName.getNamespaceAsString())
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, newGroupName).build());
    final TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
    byte[] startKey = Bytes.toBytes("aaaaa");
    byte[] endKey = Bytes.toBytes("zzzzz");
    admin.createTable(desc, startKey, endKey, 6);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }
        return regions.size() >= 6;
      }
    });

    // make assignment uneven, move all regions to one server
    Map<ServerName, List<String>> assignMap = getTableServerRegionMap().get(tableName);
    final ServerName first = assignMap.entrySet().iterator().next().getKey();
    for (RegionInfo region : admin.getRegions(tableName)) {
      if (!assignMap.get(first).contains(region.getRegionNameAsString())) {
        admin.move(region.getEncodedNameAsBytes(), first);
      }
    }
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Map<ServerName, List<String>> map = getTableServerRegionMap().get(tableName);
        if (map == null) {
          return true;
        }
        List<String> regions = map.get(first);
        if (regions == null) {
          return true;
        }
        return regions.size() >= 6;
      }
    });

    // balance the other group and make sure it doesn't affect the new group
    admin.balancerSwitch(true, true);
    rsGroupAdmin.balanceRSGroup(RSGroupInfo.DEFAULT_GROUP);
    assertEquals(6, getTableServerRegionMap().get(tableName).get(first).size());

    // disable balance, balancer will not be run and return false
    admin.balancerSwitch(false, true);
    assertFalse(rsGroupAdmin.balanceRSGroup(newGroupName));
    assertEquals(6, getTableServerRegionMap().get(tableName).get(first).size());

    // enable balance
    admin.balancerSwitch(true, true);
    rsGroupAdmin.balanceRSGroup(newGroupName);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        for (List<String> regions : getTableServerRegionMap().get(tableName).values()) {
          if (2 != regions.size()) {
            return false;
          }
        }
        return true;
      }
    });
    admin.balancerSwitch(false, true);
  }

  @Test
  public void testMisplacedRegions() throws Exception {
    final TableName tableName = TableName.valueOf(tablePrefix + "_testMisplacedRegions");
    LOG.info("testMisplacedRegions");

    final RSGroupInfo RSGroupInfo = addGroup("testMisplacedRegions", 1);

    TEST_UTIL.createMultiRegionTable(tableName, new byte[] { 'f' }, 15);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    rsGroupAdminEndpoint.getGroupInfoManager().moveTables(Sets.newHashSet(tableName),
      RSGroupInfo.getName());

    admin.balancerSwitch(true, true);
    assertTrue(rsGroupAdmin.balanceRSGroup(RSGroupInfo.getName()));
    admin.balancerSwitch(false, true);
    assertTrue(observer.preBalanceRSGroupCalled);
    assertTrue(observer.postBalanceRSGroupCalled);

    TEST_UTIL.waitFor(60000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        ServerName serverName =
          ServerName.valueOf(RSGroupInfo.getServers().iterator().next().toString(), 1);
        return admin.getConnection().getAdmin().getRegions(serverName).size() == 15;
      }
    });
  }

  @Test
  public void testGetRSGroupAssignmentsByTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createMultiRegionTable(tableName, HConstants.CATALOG_FAMILY, 10);
    // disable table
    final TableName disableTableName = TableName.valueOf("testDisableTable");
    TEST_UTIL.createMultiRegionTable(disableTableName, HConstants.CATALOG_FAMILY, 10);
    TEST_UTIL.getAdmin().disableTable(disableTableName);

    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    Map<TableName, Map<ServerName, List<RegionInfo>>> assignments =
        rsGroupAdminEndpoint.getGroupAdminServer()
            .getRSGroupAssignmentsByTable(master.getTableStateManager(), RSGroupInfo.DEFAULT_GROUP);
    assertFalse(assignments.containsKey(disableTableName));
    assertTrue(assignments.containsKey(tableName));
  }
}
