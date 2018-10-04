/**
 * Copyright The Apache Software Foundation
 *
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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category({MediumTests.class})
public class TestRSGroupsBalance extends TestRSGroupsBase {
  protected static final Log LOG = LogFactory.getLog(TestRSGroupsBalance.class);

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
  public void testRSGroupBalancerSwitch() throws IOException {
    //Balancer is initially off in the test, set to true and check
    assertFalse(admin.setBalancerRunning(true, true));
    assertTrue(admin.isBalancerEnabled());
    //Set balancer off and check if it actually turned off
    assertTrue(admin.setBalancerRunning(false,true));
    assertFalse(admin.isBalancerEnabled());
  }

  @Test
  public void testGroupBalance() throws Exception {
    LOG.info("testGroupBalance");
    String newGroupName = getGroupName("testGroupBalance");
    final RSGroupInfo newGroup = addGroup(rsGroupAdmin, newGroupName, 3);

    final TableName tableName = TableName.valueOf(tablePrefix+"_ns", "testGroupBalance");
    admin.createNamespace(
        NamespaceDescriptor.create(tableName.getNamespaceAsString())
            .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, newGroupName).build());
    final byte[] familyNameBytes = Bytes.toBytes("f");
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    byte [] startKey = Bytes.toBytes("aaaaa");
    byte [] endKey = Bytes.toBytes("zzzzz");
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

    //make assignment uneven, move all regions to one server
    Map<ServerName,List<String>> assignMap =
        getTableServerRegionMap().get(tableName);
    final ServerName first = assignMap.entrySet().iterator().next().getKey();
    for(HRegionInfo region: admin.getTableRegions(tableName)) {
      if(!assignMap.get(first).contains(region.getRegionNameAsString())) {
        admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(first.getServerName()));
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

    //balance the other group and make sure it doesn't affect the new group
    rsGroupAdmin.balanceRSGroup(RSGroupInfo.DEFAULT_GROUP);
    assertEquals(6, getTableServerRegionMap().get(tableName).get(first).size());

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

    assertTrue(observer.preBalanceRSGroupCalled);
    assertTrue(observer.postBalanceRSGroupCalled);
  }

  @Test
  public void testMisplacedRegions() throws Exception {
    final TableName tableName = TableName.valueOf(tablePrefix+"_testMisplacedRegions");
    LOG.info("testMisplacedRegions");

    final RSGroupInfo RSGroupInfo = addGroup(rsGroupAdmin, "testMisplacedRegions", 1);

    TEST_UTIL.createMultiRegionTable(tableName, new byte[]{'f'}, 15);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    RSGroupAdminEndpoint.getGroupInfoManager()
        .moveTables(Sets.newHashSet(tableName), RSGroupInfo.getName());

    assertTrue(rsGroupAdmin.balanceRSGroup(RSGroupInfo.getName()));

    TEST_UTIL.waitFor(60000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        ServerName serverName =
            ServerName.valueOf(RSGroupInfo.getServers().iterator().next().toString(), 1);
        return admin.getConnection().getAdmin()
            .getOnlineRegions(serverName).size() == 15;
      }
    });
  }

}