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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ MediumTests.class })
public class TestRSGroupsBasics extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupsBasics.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsBasics.class);

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
  public void testBasicStartUp() throws IOException {
    RSGroupInfo defaultInfo = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    assertEquals(NUM_SLAVES_BASE, defaultInfo.getServers().size());
    // Assignment of meta, namespace and rsgroup regions.
    int count = master.getAssignmentManager().getRegionStates().getRegionAssignments().size();
    // 3 (meta, namespace and rsgroup)
    assertEquals(3, count);
  }

  @Test
  public void testCreateAndDrop() throws Exception {
    TEST_UTIL.createTable(tableName, Bytes.toBytes("cf"));
    // wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(tableName) != null;
      }
    });
    TEST_UTIL.deleteTable(tableName);
  }

  @Test
  public void testCreateMultiRegion() throws IOException {
    byte[] end = { 1, 3, 5, 7, 9 };
    byte[] start = { 0, 2, 4, 6, 8 };
    byte[][] f = { Bytes.toBytes("f") };
    TEST_UTIL.createTable(tableName, f, 1, start, end, 10);
  }

  @Test
  public void testNamespaceCreateAndAssign() throws Exception {
    LOG.info("testNamespaceCreateAndAssign");
    String nsName = tablePrefix + "_foo";
    final TableName tableName = TableName.valueOf(nsName, tablePrefix + "_testCreateAndAssign");
    RSGroupInfo appInfo = addGroup("appInfo", 1);
    admin.createNamespace(NamespaceDescriptor.create(nsName)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, "appInfo").build());
    final TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
    admin.createTable(desc);
    // wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
    ServerName targetServer = getServerName(appInfo.getServers().iterator().next());
    // verify it was assigned to the right group
    Assert.assertEquals(1, admin.getRegions(targetServer).size());
  }

  @Test
  public void testCreateWhenRsgroupNoOnlineServers() throws Exception {
    LOG.info("testCreateWhenRsgroupNoOnlineServers");

    // set rsgroup has no online servers and test create table
    final RSGroupInfo appInfo = addGroup("appInfo", 1);
    Iterator<Address> iterator = appInfo.getServers().iterator();
    List<ServerName> serversToDecommission = new ArrayList<>();
    ServerName targetServer = getServerName(iterator.next());
    assertTrue(master.getServerManager().getOnlineServers().containsKey(targetServer));
    serversToDecommission.add(targetServer);
    admin.decommissionRegionServers(serversToDecommission, true);
    assertEquals(1, admin.listDecommissionedRegionServers().size());

    final TableName tableName = TableName.valueOf(tablePrefix + "_ns", name.getMethodName());
    admin.createNamespace(NamespaceDescriptor.create(tableName.getNamespaceAsString())
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, appInfo.getName()).build());
    final TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
    try {
      admin.createTable(desc);
      fail("Shouldn't create table successfully!");
    } catch (Exception e) {
      LOG.debug("create table error", e);
    }

    // recommission and test create table
    admin.recommissionRegionServer(targetServer, null);
    assertEquals(0, admin.listDecommissionedRegionServers().size());
    admin.createTable(desc);
    // wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
  }

  @Test
  public void testDefaultNamespaceCreateAndAssign() throws Exception {
    LOG.info("testDefaultNamespaceCreateAndAssign");
    String tableName = tablePrefix + "_testCreateAndAssign";
    admin.modifyNamespace(NamespaceDescriptor.create("default")
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, "default").build());
    final TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
    admin.createTable(desc);
    // wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
  }

  @Test
  public void testCloneSnapshot() throws Exception {
    byte[] FAMILY = Bytes.toBytes("test");
    String snapshotName = tableName.getNameAsString() + "_snap";
    TableName clonedTableName = TableName.valueOf(tableName.getNameAsString() + "_clone");

    // create base table
    TEST_UTIL.createTable(tableName, FAMILY);

    // create snapshot
    admin.snapshot(snapshotName, tableName);

    // clone
    admin.cloneSnapshot(snapshotName, clonedTableName);
  }

  @Test
  public void testClearDeadServers() throws Exception {
    LOG.info("testClearDeadServers");

    // move region servers from default group to new group
    final int serverCountToMoveToNewGroup = 3;
    final RSGroupInfo newGroup =
        addGroup(getGroupName(name.getMethodName()), serverCountToMoveToNewGroup);

    // get the existing dead servers
    NUM_DEAD_SERVERS = cluster.getClusterMetrics().getDeadServerNames().size();

    // stop 1 region server in new group
    ServerName serverToStop = getServerName(newGroup.getServers().iterator().next());
    try {
      // stopping may cause an exception
      // due to the connection loss
      admin.stopRegionServer(serverToStop.getAddress().toString());
      NUM_DEAD_SERVERS++;
    } catch (Exception e) {
    }

    // wait for the stopped region server to show up in the dead server list
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return cluster.getClusterMetrics().getDeadServerNames().size() == NUM_DEAD_SERVERS &&
          !master.getServerManager().areDeadServersInProgress();
      }
    });

    // verify
    assertFalse(cluster.getClusterMetrics().getLiveServerMetrics().containsKey(serverToStop));
    assertTrue(cluster.getClusterMetrics().getDeadServerNames().contains(serverToStop));
    assertTrue(newGroup.getServers().contains(serverToStop.getAddress()));

    // clear dead servers list
    List<ServerName> notClearedServers = admin.clearDeadServers(Lists.newArrayList(serverToStop));
    assertEquals(0, notClearedServers.size());

    // verify if the stopped region server gets cleared and removed from the group
    Set<Address> newGroupServers = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getServers();
    assertFalse(newGroupServers.contains(serverToStop.getAddress()));
    assertEquals(serverCountToMoveToNewGroup - 1 /* 1 stopped */, newGroupServers.size());
  }

  @Test
  public void testClearNotProcessedDeadServer() throws Exception {
    LOG.info("testClearNotProcessedDeadServer");

    // get the existing dead servers
    NUM_DEAD_SERVERS = cluster.getClusterMetrics().getDeadServerNames().size();

    // move region servers from default group to "dead server" group
    final int serverCountToMoveToDeadServerGroup = 1;
    RSGroupInfo deadServerGroup =
        addGroup("deadServerGroup", serverCountToMoveToDeadServerGroup);

    // stop 1 region server in "dead server" group
    ServerName serverToStop = getServerName(deadServerGroup.getServers().iterator().next());
    try {
      // stopping may cause an exception
      // due to the connection loss
      admin.stopRegionServer(serverToStop.getAddress().toString());
      NUM_DEAD_SERVERS++;
    } catch (Exception e) {
    }
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return cluster.getClusterMetrics().getDeadServerNames().size() == NUM_DEAD_SERVERS;
      }
    });

    Set<Address> ServersInDeadServerGroup =
        rsGroupAdmin.getRSGroupInfo(deadServerGroup.getName()).getServers();
    assertEquals(serverCountToMoveToDeadServerGroup, ServersInDeadServerGroup.size());
    assertTrue(ServersInDeadServerGroup.contains(serverToStop.getAddress()));
  }

  @Test
  public void testRSGroupsWithHBaseQuota() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    restartHBaseCluster();
    try {
      TEST_UTIL.waitFor(90000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return admin.isTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
        }
      });
    } finally {
      TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, false);
      restartHBaseCluster();
    }
  }

  private void restartHBaseCluster() throws Exception {
    LOG.info("\n\nShutting down cluster");
    TEST_UTIL.shutdownMiniHBaseCluster();
    LOG.info("\n\nSleeping a bit");
    Thread.sleep(2000);
    TEST_UTIL.restartHBaseCluster(NUM_SLAVES_BASE - 1);
    initialize();
  }
}
