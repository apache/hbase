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

import static org.apache.hadoop.hbase.util.Threads.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Version;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.VersionInfo;
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

@Category({ LargeTests.class })
public class TestRSGroupsKillRS extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupsKillRS.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsKillRS.class);

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
  public void testKillRS() throws Exception {
    RSGroupInfo appInfo = addGroup("appInfo", 1);
    final TableName tableName = TableName.valueOf(tablePrefix + "_ns", name.getMethodName());
    admin.createNamespace(NamespaceDescriptor.create(tableName.getNamespaceAsString())
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, appInfo.getName()).build());
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
    assertEquals(1, admin.getRegions(targetServer).size());

    try {
      // stopping may cause an exception
      // due to the connection loss
      admin.stopRegionServer(targetServer.getAddress().toString());
    } catch (Exception e) {
    }
    // wait until the server is actually down
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !cluster.getClusterMetrics().getLiveServerMetrics().containsKey(targetServer);
      }
    });
    // there is only one rs in the group and we killed it, so the region can not be online, until
    // later we add new servers to it.
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !cluster.getClusterMetrics().getRegionStatesInTransition().isEmpty();
      }
    });
    Set<Address> newServers = Sets.newHashSet();
    newServers
      .add(rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().iterator().next());
    rsGroupAdmin.moveServers(newServers, appInfo.getName());

    // Make sure all the table's regions get reassigned
    // disabling the table guarantees no conflicting assign/unassign (ie SSH) happens
    admin.disableTable(tableName);
    admin.enableTable(tableName);

    // wait for region to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return cluster.getClusterMetrics().getRegionStatesInTransition().isEmpty();
      }
    });

    ServerName targetServer1 = getServerName(newServers.iterator().next());
    assertEquals(1, admin.getRegions(targetServer1).size());
    assertEquals(tableName, admin.getRegions(targetServer1).get(0).getTable());
  }

  @Test
  public void testKillAllRSInGroup() throws Exception {
    // create a rsgroup and move two regionservers to it
    String groupName = "my_group";
    int groupRSCount = 2;
    addGroup(groupName, groupRSCount);

    // create a table, and move it to my_group
    Table t = TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 5);
    TEST_UTIL.loadTable(t, Bytes.toBytes("f"));
    Set<TableName> toAddTables = new HashSet<>();
    toAddTables.add(tableName);
    rsGroupAdmin.moveTables(toAddTables, groupName);
    assertTrue(rsGroupAdmin.getRSGroupInfo(groupName).getTables().contains(tableName));
    TEST_UTIL.waitTableAvailable(tableName, 30000);

    // check my_group servers and table regions
    Set<Address> servers = rsGroupAdmin.getRSGroupInfo(groupName).getServers();
    assertEquals(2, servers.size());
    LOG.debug("group servers {}", servers);
    for (RegionInfo tr :
        master.getAssignmentManager().getRegionStates().getRegionsOfTable(tableName)) {
      assertTrue(servers.contains(
          master.getAssignmentManager().getRegionStates().getRegionAssignments()
              .get(tr).getAddress()));
    }

    // Move a region, to ensure there exists a region whose 'lastHost' is in my_group
    // ('lastHost' of other regions are in 'default' group)
    // and check if all table regions are online
    List<ServerName> gsn = new ArrayList<>();
    for(Address addr : servers){
      gsn.add(getServerName(addr));
    }
    assertEquals(2, gsn.size());
    for(Map.Entry<RegionInfo, ServerName> entry :
        master.getAssignmentManager().getRegionStates().getRegionAssignments().entrySet()){
      if(entry.getKey().getTable().equals(tableName)){
        LOG.debug("move region {} from {} to {}", entry.getKey().getRegionNameAsString(),
            entry.getValue(), gsn.get(1 - gsn.indexOf(entry.getValue())));
        TEST_UTIL.moveRegionAndWait(entry.getKey(), gsn.get(1 - gsn.indexOf(entry.getValue())));
        break;
      }
    }
    TEST_UTIL.waitTableAvailable(tableName, 30000);

    // case 1: stop all the regionservers in my_group, and restart a regionserver in my_group,
    // and then check if all table regions are online
    for(Address addr : rsGroupAdmin.getRSGroupInfo(groupName).getServers()) {
      TEST_UTIL.getMiniHBaseCluster().stopRegionServer(getServerName(addr));
    }
    // better wait for a while for region reassign
    sleep(10000);
    assertEquals(NUM_SLAVES_BASE - gsn.size(),
        TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size());
    TEST_UTIL.getMiniHBaseCluster().startRegionServer(gsn.get(0).getHostname(),
        gsn.get(0).getPort());
    assertEquals(NUM_SLAVES_BASE - gsn.size() + 1,
        TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size());
    TEST_UTIL.waitTableAvailable(tableName, 30000);

    // case 2: stop all the regionservers in my_group, and move another
    // regionserver(from the 'default' group) to my_group,
    // and then check if all table regions are online
    for(JVMClusterUtil.RegionServerThread rst :
        TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()){
      if(rst.getRegionServer().getServerName().getAddress().equals(gsn.get(0).getAddress())){
        TEST_UTIL.getMiniHBaseCluster().stopRegionServer(rst.getRegionServer().getServerName());
        break;
      }
    }
    sleep(10000);
    assertEquals(NUM_SLAVES_BASE - gsn.size(),
        TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size());
    ServerName newServer = master.getServerManager().getOnlineServersList().get(0);
    rsGroupAdmin.moveServers(Sets.newHashSet(newServer.getAddress()), groupName);
    // wait and check if table regions are online
    TEST_UTIL.waitTableAvailable(tableName, 30000);
  }

  @Test
  public void testLowerMetaGroupVersion() throws Exception{
    // create a rsgroup and move one regionserver to it
    String groupName = "meta_group";
    int groupRSCount = 1;
    addGroup(groupName, groupRSCount);

    // move hbase:meta to meta_group
    Set<TableName> toAddTables = new HashSet<>();
    toAddTables.add(TableName.META_TABLE_NAME);
    rsGroupAdmin.moveTables(toAddTables, groupName);
    assertTrue(
        rsGroupAdmin.getRSGroupInfo(groupName).getTables().contains(TableName.META_TABLE_NAME));

    // restart the regionserver in meta_group, and lower its version
    String originVersion = "";
    Set<Address> servers = new HashSet<>();
    for(Address addr : rsGroupAdmin.getRSGroupInfo(groupName).getServers()) {
      servers.add(addr);
      TEST_UTIL.getMiniHBaseCluster().stopRegionServer(getServerName(addr));
      originVersion = master.getRegionServerVersion(getServerName(addr));
    }
    // better wait for a while for region reassign
    sleep(10000);
    assertEquals(NUM_SLAVES_BASE - groupRSCount,
        TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size());
    Address address = servers.iterator().next();
    int majorVersion = VersionInfo.getMajorVersion(originVersion);
    assertTrue(majorVersion >= 1);
    String lowerVersion = String.valueOf(majorVersion - 1) + originVersion.split("\\.")[1];
    setFinalStatic(Version.class.getField("version"), lowerVersion);
    TEST_UTIL.getMiniHBaseCluster().startRegionServer(address.getHostname(),
        address.getPort());
    assertEquals(NUM_SLAVES_BASE,
        TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size());
    assertTrue(VersionInfo.compareVersion(originVersion,
        master.getRegionServerVersion(getServerName(servers.iterator().next()))) > 0);
    LOG.debug("wait for META assigned...");
    // SCP finished, which means all regions assigned too.
    TEST_UTIL.waitFor(60000, () -> !TEST_UTIL.getHBaseCluster().getMaster().getProcedures().stream()
        .filter(p -> (p instanceof ServerCrashProcedure)).findAny().isPresent());
  }

  private static void setFinalStatic(Field field, Object newValue) throws Exception {
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.set(null, newValue);
  }
}
