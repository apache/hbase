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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
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

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

@Category({MediumTests.class})
public class TestRSGroups extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRSGroups.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroups.class);
  private static boolean INIT = false;
  private static RSGroupAdminEndpoint rsGroupAdminEndpoint;
  private static CPMasterObserver observer;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setFloat(
            "hbase.master.balancer.stochastic.tableSkewCost", 6000);
    TEST_UTIL.getConfiguration().set(
        HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        RSGroupAdminEndpoint.class.getName() + "," + CPMasterObserver.class.getName());
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setInt(
        ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
        NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    initialize();
  }

  private static void initialize() throws Exception {
    admin = TEST_UTIL.getAdmin();
    cluster = TEST_UTIL.getHBaseCluster();
    master = ((MiniHBaseCluster)cluster).getMaster();

    //wait for balancer to come online
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return master.isInitialized() &&
            ((RSGroupBasedLoadBalancer) master.getLoadBalancer()).isOnline();
      }
    });
    admin.setBalancerRunning(false,true);
    rsGroupAdmin = new VerifyingRSGroupAdminClient(
        new RSGroupAdminClient(TEST_UTIL.getConnection()), TEST_UTIL.getConfiguration());
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    observer = (CPMasterObserver) host.findCoprocessor(CPMasterObserver.class.getName());
    rsGroupAdminEndpoint = (RSGroupAdminEndpoint)
        host.findCoprocessor(RSGroupAdminEndpoint.class.getName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void beforeMethod() throws Exception {
    if (!INIT) {
      INIT = true;
      afterMethod();
    }

  }

  @After
  public void afterMethod() throws Exception {
    deleteTableIfNecessary();
    deleteNamespaceIfNecessary();
    deleteGroups();

    for(ServerName sn : admin.listDecommissionedRegionServers()){
      admin.recommissionRegionServer(sn, null);
    }
    assertTrue(admin.listDecommissionedRegionServers().isEmpty());

    int missing = NUM_SLAVES_BASE - getNumServers();
    LOG.info("Restoring servers: "+missing);
    for(int i=0; i<missing; i++) {
      ((MiniHBaseCluster)cluster).startRegionServer();
    }

    rsGroupAdmin.addRSGroup("master");
    ServerName masterServerName =
        ((MiniHBaseCluster)cluster).getMaster().getServerName();

    try {
      rsGroupAdmin.moveServers(Sets.newHashSet(masterServerName.getAddress()), "master");
    } catch (Exception ex) {
      LOG.warn("Got this on setup, FYI", ex);
    }
    assertTrue(observer.preMoveServersCalled);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        LOG.info("Waiting for cleanup to finish " + rsGroupAdmin.listRSGroups());
        //Might be greater since moving servers back to default
        //is after starting a server

        return rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size()
            == NUM_SLAVES_BASE;
      }
    });
  }

  @Test
  public void testBasicStartUp() throws IOException {
    RSGroupInfo defaultInfo = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    assertEquals(4, defaultInfo.getServers().size());
    // Assignment of root and meta regions.
    int count = master.getAssignmentManager().getRegionStates().getRegionAssignments().size();
    //3 meta,namespace, group
    assertEquals(3, count);
  }

  @Test
  public void testNamespaceCreateAndAssign() throws Exception {
    LOG.info("testNamespaceCreateAndAssign");
    String nsName = tablePrefix+"_foo";
    final TableName tableName = TableName.valueOf(nsName, tablePrefix + "_testCreateAndAssign");
    RSGroupInfo appInfo = addGroup("appInfo", 1);
    admin.createNamespace(NamespaceDescriptor.create(nsName)
        .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, "appInfo").build());
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    admin.createTable(desc);
    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
    ServerName targetServer =
        ServerName.parseServerName(appInfo.getServers().iterator().next().toString());
    AdminProtos.AdminService.BlockingInterface rs =
      ((ClusterConnection) admin.getConnection()).getAdmin(targetServer);
    //verify it was assigned to the right group
    Assert.assertEquals(1, ProtobufUtil.getOnlineRegions(rs).size());
  }

  @Test
  public void testDefaultNamespaceCreateAndAssign() throws Exception {
    LOG.info("testDefaultNamespaceCreateAndAssign");
    String tableName = tablePrefix + "_testCreateAndAssign";
    admin.modifyNamespace(NamespaceDescriptor.create("default")
        .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, "default").build());
    final HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor("f"));
    admin.createTable(desc);
    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
  }

  @Test
  public void testNamespaceConstraint() throws Exception {
    String nsName = tablePrefix+"_foo";
    String groupName = tablePrefix+"_foo";
    LOG.info("testNamespaceConstraint");
    rsGroupAdmin.addRSGroup(groupName);
    assertTrue(observer.preAddRSGroupCalled);
    assertTrue(observer.postAddRSGroupCalled);

    admin.createNamespace(NamespaceDescriptor.create(nsName)
        .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, groupName)
        .build());
    //test removing a referenced group
    try {
      rsGroupAdmin.removeRSGroup(groupName);
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
    //test modify group
    //changing with the same name is fine
    admin.modifyNamespace(
        NamespaceDescriptor.create(nsName)
          .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, groupName)
          .build());
    String anotherGroup = tablePrefix+"_anotherGroup";
    rsGroupAdmin.addRSGroup(anotherGroup);
    //test add non-existent group
    admin.deleteNamespace(nsName);
    rsGroupAdmin.removeRSGroup(groupName);
    assertTrue(observer.preRemoveRSGroupCalled);
    assertTrue(observer.postRemoveRSGroupCalled);
    try {
      admin.createNamespace(NamespaceDescriptor.create(nsName)
          .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, "foo")
          .build());
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
  }

  @Test
  public void testGroupInfoMultiAccessing() throws Exception {
    RSGroupInfoManager manager = rsGroupAdminEndpoint.getGroupInfoManager();
    RSGroupInfo defaultGroup = manager.getRSGroup("default");
    // getRSGroup updates default group's server list
    // this process must not affect other threads iterating the list
    Iterator<Address> it = defaultGroup.getServers().iterator();
    manager.getRSGroup("default");
    it.next();
  }

  public static class CPMasterObserver implements MasterCoprocessor, MasterObserver {
    boolean preBalanceRSGroupCalled = false;
    boolean postBalanceRSGroupCalled = false;
    boolean preMoveServersCalled = false;
    boolean postMoveServersCalled = false;
    boolean preMoveTablesCalled = false;
    boolean postMoveTablesCalled = false;
    boolean preAddRSGroupCalled = false;
    boolean postAddRSGroupCalled = false;
    boolean preRemoveRSGroupCalled = false;
    boolean postRemoveRSGroupCalled = false;
    boolean preRemoveServersCalled = false;
    boolean postRemoveServersCalled = false;
    boolean preMoveServersAndTables = false;
    boolean postMoveServersAndTables = false;
    boolean preGetRSGroupInfoCalled = false;
    boolean postGetRSGroupInfoCalled = false;
    boolean preGetRSGroupInfoOfTableCalled = false;
    boolean postGetRSGroupInfoOfTableCalled = false;
    boolean preListRSGroupsCalled = false;
    boolean postListRSGroupsCalled = false;
    boolean preGetRSGroupInfoOfServerCalled = false;
    boolean postGetRSGroupInfoOfServerCalled = false;

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }
    @Override
    public void preMoveServersAndTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
      preMoveServersAndTables = true;
    }
    @Override
    public void postMoveServersAndTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
      postMoveServersAndTables = true;
    }
    @Override
    public void preRemoveServers(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers) throws IOException {
      preRemoveServersCalled = true;
    }
    @Override
    public void postRemoveServers(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers) throws IOException {
      postRemoveServersCalled = true;
    }
    @Override
    public void preRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String name) throws IOException {
      preRemoveRSGroupCalled = true;
    }
    @Override
    public void postRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String name) throws IOException {
      postRemoveRSGroupCalled = true;
    }
    @Override
    public void preAddRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String name) throws IOException {
      preAddRSGroupCalled = true;
    }
    @Override
    public void postAddRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String name) throws IOException {
      postAddRSGroupCalled = true;
    }
    @Override
    public void preMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<TableName> tables, String targetGroup) throws IOException {
      preMoveTablesCalled = true;
    }
    @Override
    public void postMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<TableName> tables, String targetGroup) throws IOException {
      postMoveTablesCalled = true;
    }
    @Override
    public void preMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers, String targetGroup) throws IOException {
      preMoveServersCalled = true;
    }

    @Override
    public void postMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers, String targetGroup) throws IOException {
      postMoveServersCalled = true;
    }
    @Override
    public void preBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String groupName) throws IOException {
      preBalanceRSGroupCalled = true;
    }
    @Override
    public void postBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String groupName, boolean balancerRan) throws IOException {
      postBalanceRSGroupCalled = true;
    }

    @Override
    public void preGetRSGroupInfo(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final String groupName) throws IOException {
      preGetRSGroupInfoCalled = true;
    }

    @Override
    public void postGetRSGroupInfo(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final String groupName) throws IOException {
      postGetRSGroupInfoCalled = true;
    }

    @Override
    public void preGetRSGroupInfoOfTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final TableName tableName) throws IOException {
      preGetRSGroupInfoOfTableCalled = true;
    }

    @Override
    public void postGetRSGroupInfoOfTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final TableName tableName) throws IOException {
      postGetRSGroupInfoOfTableCalled = true;
    }

    @Override
    public void preListRSGroups(final ObserverContext<MasterCoprocessorEnvironment> ctx)
        throws IOException {
      preListRSGroupsCalled = true;
    }

    @Override
    public void postListRSGroups(final ObserverContext<MasterCoprocessorEnvironment> ctx)
        throws IOException {
      postListRSGroupsCalled = true;
    }

    @Override
    public void preGetRSGroupInfoOfServer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final Address server) throws IOException {
      preGetRSGroupInfoOfServerCalled = true;
    }

    @Override
    public void postGetRSGroupInfoOfServer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final Address server) throws IOException {
      postGetRSGroupInfoOfServerCalled = true;
    }
  }

  @Test
  public void testGetRSGroupInfoCPHookCalled() throws Exception {
    rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    assertTrue(observer.preGetRSGroupInfoCalled);
    assertTrue(observer.postGetRSGroupInfoCalled);
  }

  @Test
  public void testGetRSGroupInfoOfTableCPHookCalled() throws Exception {
    rsGroupAdmin.getRSGroupInfoOfTable(TableName.META_TABLE_NAME);
    assertTrue(observer.preGetRSGroupInfoOfTableCalled);
    assertTrue(observer.postGetRSGroupInfoOfTableCalled);
  }

  @Test
  public void testListRSGroupsCPHookCalled() throws Exception {
    rsGroupAdmin.listRSGroups();
    assertTrue(observer.preListRSGroupsCalled);
    assertTrue(observer.postListRSGroupsCalled);
  }

  @Test
  public void testGetRSGroupInfoOfServerCPHookCalled() throws Exception {
    ServerName masterServerName = ((MiniHBaseCluster) cluster).getMaster().getServerName();
    rsGroupAdmin.getRSGroupOfServer(masterServerName.getAddress());
    assertTrue(observer.preGetRSGroupInfoOfServerCalled);
    assertTrue(observer.postGetRSGroupInfoOfServerCalled);
  }

  @Test
  public void testMoveServersAndTables() throws Exception {
    super.testMoveServersAndTables();
    assertTrue(observer.preMoveServersAndTables);
    assertTrue(observer.postMoveServersAndTables);
  }
  @Test
  public void testTableMoveTruncateAndDrop() throws Exception {
    super.testTableMoveTruncateAndDrop();
    assertTrue(observer.preMoveTablesCalled);
    assertTrue(observer.postMoveTablesCalled);
  }

  @Test
  public void testRemoveServers() throws Exception {
    super.testRemoveServers();
    assertTrue(observer.preRemoveServersCalled);
    assertTrue(observer.postRemoveServersCalled);
  }

  @Test
  public void testMisplacedRegions() throws Exception {
    final TableName tableName = TableName.valueOf(tablePrefix+"_testMisplacedRegions");
    LOG.info("testMisplacedRegions");

    final RSGroupInfo RSGroupInfo = addGroup("testMisplacedRegions", 1);

    TEST_UTIL.createMultiRegionTable(tableName, new byte[]{'f'}, 15);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    rsGroupAdminEndpoint.getGroupInfoManager()
        .moveTables(Sets.newHashSet(tableName), RSGroupInfo.getName());

    admin.setBalancerRunning(true,true);
    assertTrue(rsGroupAdmin.balanceRSGroup(RSGroupInfo.getName()));
    admin.setBalancerRunning(false,true);
    assertTrue(observer.preBalanceRSGroupCalled);
    assertTrue(observer.postBalanceRSGroupCalled);

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

  @Test
  public void testRSGroupListDoesNotContainFailedTableCreation() throws Exception {
    toggleQuotaCheckAndRestartMiniCluster(true);
    String nsp = "np1";
    NamespaceDescriptor nspDesc =
        NamespaceDescriptor.create(nsp).addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "5")
            .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    admin.createNamespace(nspDesc);
    assertEquals(3, admin.listNamespaceDescriptors().length);
    HColumnDescriptor fam1 = new HColumnDescriptor("fam1");
    HTableDescriptor tableDescOne =
        new HTableDescriptor(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table1"));
    tableDescOne.addFamily(fam1);
    admin.createTable(tableDescOne);

    HTableDescriptor tableDescTwo =
        new HTableDescriptor(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table2"));
    tableDescTwo.addFamily(fam1);
    boolean constraintViolated = false;

    try {
      admin.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"),
          6);
      Assert.fail("Creation table should fail because of quota violation.");
    } catch (Exception exp) {
      assertTrue(exp instanceof IOException);
      constraintViolated = true;
    } finally {
      assertTrue("Constraint not violated for table " + tableDescTwo.getTableName(),
          constraintViolated);
    }
    List<RSGroupInfo> rsGroupInfoList = rsGroupAdmin.listRSGroups();
    boolean foundTable2 = false;
    boolean foundTable1 = false;
    for(int i = 0; i < rsGroupInfoList.size(); i++){
      if(rsGroupInfoList.get(i).getTables().contains(tableDescTwo.getTableName())){
        foundTable2 = true;
      }
      if(rsGroupInfoList.get(i).getTables().contains(tableDescOne.getTableName())){
        foundTable1 = true;
      }
    }
    assertFalse("Found table2 in rsgroup list.", foundTable2);
    assertTrue("Did not find table1 in rsgroup list", foundTable1);

    TEST_UTIL.deleteTable(tableDescOne.getTableName());
    admin.deleteNamespace(nspDesc.getName());
    toggleQuotaCheckAndRestartMiniCluster(false);

  }
  private void toggleQuotaCheckAndRestartMiniCluster(boolean enable) throws Exception{
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, enable);
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setInt(
        ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
        NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    initialize();
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
