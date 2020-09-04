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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.net.Address;
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

@Category({ MediumTests.class })
public class TestRSGroupsAdmin1 extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupsAdmin1.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsAdmin1.class);

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
  public void testValidGroupNames() throws IOException {
    String[] badNames = { "foo*", "foo@", "-" };
    String[] goodNames = { "foo_123" };

    for (String entry : badNames) {
      try {
        rsGroupAdmin.addRSGroup(entry);
        fail("Expected a constraint exception for: " + entry);
      } catch (ConstraintException ex) {
        // expected
      }
    }

    for (String entry : goodNames) {
      rsGroupAdmin.addRSGroup(entry);
    }
  }

  @Test
  public void testBogusArgs() throws Exception {
    assertNull(rsGroupAdmin.getRSGroupInfoOfTable(TableName.valueOf("nonexistent")));
    assertNull(rsGroupAdmin.getRSGroupOfServer(Address.fromParts("bogus", 123)));
    assertNull(rsGroupAdmin.getRSGroupInfo("bogus"));

    try {
      rsGroupAdmin.removeRSGroup("bogus");
      fail("Expected removing bogus group to fail");
    } catch (ConstraintException ex) {
      // expected
    }

    try {
      rsGroupAdmin.moveTables(Sets.newHashSet(TableName.valueOf("bogustable")), "bogus");
      fail("Expected move with bogus group to fail");
    } catch (ConstraintException | TableNotFoundException ex) {
      // expected
    }

    try {
      rsGroupAdmin.moveServers(Sets.newHashSet(Address.fromParts("bogus", 123)), "bogus");
      fail("Expected move with bogus group to fail");
    } catch (ConstraintException ex) {
      // expected
    }

    try {
      admin.balancerSwitch(true, true);
      rsGroupAdmin.balanceRSGroup("bogus");
      admin.balancerSwitch(false, true);
      fail("Expected move with bogus group to fail");
    } catch (ConstraintException ex) {
      // expected
    }
  }

  @Test
  public void testNamespaceConstraint() throws Exception {
    String nsName = tablePrefix + "_foo";
    String groupName = tablePrefix + "_foo";
    LOG.info("testNamespaceConstraint");
    rsGroupAdmin.addRSGroup(groupName);
    assertTrue(observer.preAddRSGroupCalled);
    assertTrue(observer.postAddRSGroupCalled);

    admin.createNamespace(NamespaceDescriptor.create(nsName)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, groupName).build());
    // test removing a referenced group
    try {
      rsGroupAdmin.removeRSGroup(groupName);
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
    // test modify group
    // changing with the same name is fine
    admin.modifyNamespace(NamespaceDescriptor.create(nsName)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, groupName).build());
    String anotherGroup = tablePrefix + "_anotherGroup";
    rsGroupAdmin.addRSGroup(anotherGroup);
    // test add non-existent group
    admin.deleteNamespace(nsName);
    rsGroupAdmin.removeRSGroup(groupName);
    assertTrue(observer.preRemoveRSGroupCalled);
    assertTrue(observer.postRemoveRSGroupCalled);
    try {
      admin.createNamespace(NamespaceDescriptor.create(nsName)
        .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, "foo").build());
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

  @Test
  public void testFailRemoveGroup() throws IOException, InterruptedException {
    int initNumGroups = rsGroupAdmin.listRSGroups().size();
    addGroup("bar", 3);
    TEST_UTIL.createTable(tableName, Bytes.toBytes("f"));
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName), "bar");
    RSGroupInfo barGroup = rsGroupAdmin.getRSGroupInfo("bar");
    // group is not empty therefore it should fail
    try {
      rsGroupAdmin.removeRSGroup(barGroup.getName());
      fail("Expected remove group to fail");
    } catch (IOException e) {
    }
    // group cannot lose all it's servers therefore it should fail
    try {
      rsGroupAdmin.moveServers(barGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);
      fail("Expected move servers to fail");
    } catch (IOException e) {
    }

    rsGroupAdmin.moveTables(barGroup.getTables(), RSGroupInfo.DEFAULT_GROUP);
    try {
      rsGroupAdmin.removeRSGroup(barGroup.getName());
      fail("Expected move servers to fail");
    } catch (IOException e) {
    }

    rsGroupAdmin.moveServers(barGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.removeRSGroup(barGroup.getName());

    Assert.assertEquals(initNumGroups, rsGroupAdmin.listRSGroups().size());
  }

  @Test
  public void testMultiTableMove() throws Exception {
    final TableName tableNameA = TableName.valueOf(tablePrefix + name.getMethodName() + "A");
    final TableName tableNameB = TableName.valueOf(tablePrefix + name.getMethodName() + "B");
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName(name.getMethodName());
    final RSGroupInfo newGroup = addGroup(newGroupName, 1);

    TEST_UTIL.createTable(tableNameA, familyNameBytes);
    TEST_UTIL.createTable(tableNameB, familyNameBytes);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regionsA = getTableRegionMap().get(tableNameA);
        if (regionsA == null) {
          return false;
        }
        List<String> regionsB = getTableRegionMap().get(tableNameB);
        if (regionsB == null) {
          return false;
        }
        return getTableRegionMap().get(tableNameA).size() >= 1 &&
          getTableRegionMap().get(tableNameB).size() >= 1;
      }
    });

    RSGroupInfo tableGrpA = rsGroupAdmin.getRSGroupInfoOfTable(tableNameA);
    assertTrue(tableGrpA.getName().equals(RSGroupInfo.DEFAULT_GROUP));

    RSGroupInfo tableGrpB = rsGroupAdmin.getRSGroupInfoOfTable(tableNameB);
    assertTrue(tableGrpB.getName().equals(RSGroupInfo.DEFAULT_GROUP));
    // change table's group
    LOG.info("Moving table [" + tableNameA + "," + tableNameB + "] to " + newGroup.getName());
    rsGroupAdmin.moveTables(Sets.newHashSet(tableNameA, tableNameB), newGroup.getName());

    // verify group change
    Assert.assertEquals(newGroup.getName(),
      rsGroupAdmin.getRSGroupInfoOfTable(tableNameA).getName());

    Assert.assertEquals(newGroup.getName(),
      rsGroupAdmin.getRSGroupInfoOfTable(tableNameB).getName());

    // verify tables' not exist in old group
    Set<TableName> DefaultTables =
      rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables();
    assertFalse(DefaultTables.contains(tableNameA));
    assertFalse(DefaultTables.contains(tableNameB));

    // verify tables' exist in new group
    Set<TableName> newGroupTables = rsGroupAdmin.getRSGroupInfo(newGroupName).getTables();
    assertTrue(newGroupTables.contains(tableNameA));
    assertTrue(newGroupTables.contains(tableNameB));
  }

  @Test
  public void testTableMoveTruncateAndDrop() throws Exception {
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName(name.getMethodName());
    final RSGroupInfo newGroup = addGroup(newGroupName, 2);

    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, 5);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }

        return getTableRegionMap().get(tableName).size() >= 5;
      }
    });

    RSGroupInfo tableGrp = rsGroupAdmin.getRSGroupInfoOfTable(tableName);
    assertTrue(tableGrp.getName().equals(RSGroupInfo.DEFAULT_GROUP));

    // change table's group
    LOG.info("Moving table " + tableName + " to " + newGroup.getName());
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName), newGroup.getName());

    // verify group change
    Assert.assertEquals(newGroup.getName(),
      rsGroupAdmin.getRSGroupInfoOfTable(tableName).getName());

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Map<ServerName, List<String>> serverMap = getTableServerRegionMap().get(tableName);
        int count = 0;
        if (serverMap != null) {
          for (ServerName rs : serverMap.keySet()) {
            if (newGroup.containsServer(rs.getAddress())) {
              count += serverMap.get(rs).size();
            }
          }
        }
        return count == 5;
      }
    });

    // test truncate
    admin.disableTable(tableName);
    admin.truncateTable(tableName, true);
    Assert.assertEquals(1, rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getTables().size());
    Assert.assertEquals(tableName,
      rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getTables().first());

    // verify removed table is removed from group
    TEST_UTIL.deleteTable(tableName);
    Assert.assertEquals(0, rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getTables().size());

    assertTrue(observer.preMoveTablesCalled);
    assertTrue(observer.postMoveTablesCalled);
  }

  @Test
  public void testDisabledTableMove() throws Exception {
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName(name.getMethodName());
    final RSGroupInfo newGroup = addGroup(newGroupName, 2);

    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, 5);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }
        return getTableRegionMap().get(tableName).size() >= 5;
      }
    });

    RSGroupInfo tableGrp = rsGroupAdmin.getRSGroupInfoOfTable(tableName);
    assertTrue(tableGrp.getName().equals(RSGroupInfo.DEFAULT_GROUP));

    // test disable table
    admin.disableTable(tableName);

    // change table's group
    LOG.info("Moving table " + tableName + " to " + newGroup.getName());
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName), newGroup.getName());

    // verify group change
    Assert.assertEquals(newGroup.getName(),
      rsGroupAdmin.getRSGroupInfoOfTable(tableName).getName());
  }

  @Test
  public void testNonExistentTableMove() throws Exception {
    TableName tableName = TableName.valueOf(tablePrefix + name.getMethodName());

    RSGroupInfo tableGrp = rsGroupAdmin.getRSGroupInfoOfTable(tableName);
    assertNull(tableGrp);

    // test if table exists already.
    boolean exist = admin.tableExists(tableName);
    assertFalse(exist);

    LOG.info("Moving table " + tableName + " to " + RSGroupInfo.DEFAULT_GROUP);
    try {
      rsGroupAdmin.moveTables(Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);
      fail("Table " + tableName + " shouldn't have been successfully moved.");
    } catch (IOException ex) {
      assertTrue(ex instanceof TableNotFoundException);
    }

    try {
      rsGroupAdmin.moveServersAndTables(Sets.newHashSet(Address.fromParts("bogus", 123)),
        Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);
      fail("Table " + tableName + " shouldn't have been successfully moved.");
    } catch (IOException ex) {
      assertTrue(ex instanceof TableNotFoundException);
    }
    // verify group change
    assertNull(rsGroupAdmin.getRSGroupInfoOfTable(tableName));
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
    ColumnFamilyDescriptor fam1 = ColumnFamilyDescriptorBuilder.of("fam1");
    TableDescriptor tableDescOne = TableDescriptorBuilder
      .newBuilder(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table1"))
      .setColumnFamily(fam1).build();
    admin.createTable(tableDescOne);

    TableDescriptor tableDescTwo = TableDescriptorBuilder
      .newBuilder(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table2"))
      .setColumnFamily(fam1).build();
    boolean constraintViolated = false;

    try {
      admin.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 6);
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
    for (int i = 0; i < rsGroupInfoList.size(); i++) {
      if (rsGroupInfoList.get(i).getTables().contains(tableDescTwo.getTableName())) {
        foundTable2 = true;
      }
      if (rsGroupInfoList.get(i).getTables().contains(tableDescOne.getTableName())) {
        foundTable1 = true;
      }
    }
    assertFalse("Found table2 in rsgroup list.", foundTable2);
    assertTrue("Did not find table1 in rsgroup list", foundTable1);

    TEST_UTIL.deleteTable(tableDescOne.getTableName());
    admin.deleteNamespace(nspDesc.getName());
    toggleQuotaCheckAndRestartMiniCluster(false);

  }

  @Test
  public void testNotMoveTableToNullRSGroupWhenCreatingExistingTable()
          throws Exception {
    // Trigger
    TableName tn1 = TableName.valueOf("t1");
    TEST_UTIL.createTable(tn1, "cf1");
    try {
      // Create an existing table to trigger HBASE-21866
      TEST_UTIL.createTable(tn1, "cf1");
    } catch (TableExistsException teex) {
      // Ignore
    }

    // Wait then verify
    //   Could not verify until the rollback of CreateTableProcedure is done
    //   (that is, the coprocessor finishes its work),
    //   or the table is still in the "default" rsgroup even though HBASE-21866
    //   is not fixed.
    TEST_UTIL.waitFor(5000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return
                (master.getMasterProcedureExecutor().getActiveExecutorCount() == 0);
      }
    });
    SortedSet<TableName> tables
            = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables();
    assertTrue("Table 't1' must be in 'default' rsgroup", tables.contains(tn1));

    // Cleanup
    TEST_UTIL.deleteTable(tn1);
  }

  private void toggleQuotaCheckAndRestartMiniCluster(boolean enable) throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, enable);
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
      NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    initialize();
  }

  @Test
  public void testRenameRSGroup() throws Exception {
    // Add rsgroup, and assign 2 servers and a table to it.
    RSGroupInfo oldgroup = addGroup("oldgroup", 2);
    final TableName tb1 = TableName.valueOf("testRename");
    TEST_UTIL.createTable(tb1, "tr");
    rsGroupAdmin.moveTables(Sets.newHashSet(tb1), oldgroup.getName());
    TEST_UTIL.waitFor(1000,
      (Waiter.Predicate<Exception>) () ->
        rsGroupAdmin.getRSGroupInfoOfTable(tb1).getServers().size() == 2);
    oldgroup = rsGroupAdmin.getRSGroupInfo(oldgroup.getName());
    assertEquals(2, oldgroup.getServers().size());
    assertEquals(oldgroup.getName(), rsGroupAdmin.getRSGroupInfoOfTable(tb1).getName());
    assertTrue(oldgroup.getTables().contains(tb1));

    // Another rsgroup and table for verification
    // that they are unchanged during we're renaming oldgroup.
    RSGroupInfo normal = addGroup("normal", 1);
    final TableName tb2 = TableName.valueOf("unmovedTable");
    TEST_UTIL.createTable(tb2, "ut");
    rsGroupAdmin.moveTables(Sets.newHashSet(tb2), normal.getName());
    TEST_UTIL.waitFor(1000,
      (Waiter.Predicate<Exception>) () ->
        rsGroupAdmin.getRSGroupInfoOfTable(tb2).getServers().size() == 1);
    normal = rsGroupAdmin.getRSGroupInfo(normal.getName());
    assertEquals(1, normal.getServers().size());
    assertEquals(normal.getName(), rsGroupAdmin.getRSGroupInfoOfTable(tb2).getName());
    assertTrue(normal.containsTable(tb2));


    // Rename rsgroup
    rsGroupAdmin.renameRSGroup(oldgroup.getName(), "newgroup");
    Set<Address> servers = oldgroup.getServers();
    RSGroupInfo newgroup = rsGroupAdmin.getRSGroupInfo("newgroup");
    assertEquals(servers.size(), newgroup.getServers().size());
    for (Address server : servers) {
      assertTrue(newgroup.containsServer(server));
    }
    assertEquals(newgroup.getName(), rsGroupAdmin.getRSGroupInfoOfTable(tb1).getName());
    assertTrue(newgroup.containsTable(tb1));
    assertEquals(normal.getName(), rsGroupAdmin.getRSGroupInfoOfTable(tb2).getName());
  }

  @Test
  public void testRenameRSGroupConstraints() throws Exception {
    // Add RSGroup, and assign 2 servers and a table to it.
    String oldGroupName = "oldGroup";
    RSGroupInfo oldGroup = addGroup(oldGroupName, 2);
    oldGroup = rsGroupAdmin.getRSGroupInfo(oldGroup.getName());
    assertNotNull(oldGroup);
    assertEquals(2, oldGroup.getServers().size());

    //Add another RSGroup
    String anotherRSGroupName = "anotherRSGroup";
    RSGroupInfo anotherGroup = addGroup(anotherRSGroupName, 1);
    anotherGroup = rsGroupAdmin.getRSGroupInfo(anotherGroup.getName());
    assertNotNull(anotherGroup);
    assertEquals(1, anotherGroup.getServers().size());

    //Rename a non existing RSGroup
    try {
      rsGroupAdmin.renameRSGroup("nonExistingRSGroup", "newRSGroup1");
      fail("ConstraintException was expected.");
    } catch (ConstraintException e) {
      assertTrue(e.getMessage().contains("does not exist"));
    }

    //Rename to existing group
    try {
      rsGroupAdmin.renameRSGroup(oldGroup.getName(), anotherRSGroupName);
      fail("ConstraintException was expected.");
    } catch (ConstraintException e) {
      assertTrue(e.getMessage().contains("Group already exists"));
    }

    //Rename default RSGroup
    try {
      rsGroupAdmin.renameRSGroup(RSGroupInfo.DEFAULT_GROUP, "newRSGroup2");
      fail("ConstraintException was expected.");
    } catch (ConstraintException e) {
      //Do nothing
    }

    //Rename to default RSGroup
    try {
      rsGroupAdmin.renameRSGroup(oldGroup.getName(), RSGroupInfo.DEFAULT_GROUP);
      fail("ConstraintException was expected.");
    } catch (ConstraintException e) {
      assertTrue(e.getMessage().contains("Group already exists"));
    }
  }
}
