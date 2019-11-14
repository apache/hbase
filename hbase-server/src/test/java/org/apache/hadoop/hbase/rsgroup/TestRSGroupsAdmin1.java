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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
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
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
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

@Category({ RSGroupTests.class, MediumTests.class })
public class TestRSGroupsAdmin1 extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupsAdmin1.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsAdmin1.class);

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
        ADMIN.addRSGroup(entry);
        fail("Expected a constraint exception for: " + entry);
      } catch (ConstraintException ex) {
        // expected
      }
    }

    for (String entry : goodNames) {
      ADMIN.addRSGroup(entry);
    }
  }

  @Test
  public void testBogusArgs() throws Exception {
    assertNull(ADMIN.getRSGroup(TableName.valueOf("nonexistent")));
    assertNull(ADMIN.getRSGroup(Address.fromParts("bogus", 123)));
    assertNull(ADMIN.getRSGroup("bogus"));

    try {
      ADMIN.removeRSGroup("bogus");
      fail("Expected removing bogus group to fail");
    } catch (ConstraintException ex) {
      // expected
    }

    try {
      ADMIN.setRSGroup(Sets.newHashSet(TableName.valueOf("bogustable")), "bogus");
      fail("Expected set table to bogus group fail");
    } catch (ConstraintException | TableNotFoundException ex) {
      // expected
    }

    try {
      ADMIN.moveServersToRSGroup(Sets.newHashSet(Address.fromParts("bogus", 123)), "bogus");
      fail("Expected move with bogus group to fail");
    } catch (ConstraintException ex) {
      // expected
    }

    try {
      ADMIN.balancerSwitch(true, true);
      ADMIN.balanceRSGroup("bogus");
      ADMIN.balancerSwitch(false, true);
      fail("Expected move with bogus group to fail");
    } catch (ConstraintException ex) {
      // expected
    }
  }

  @Test
  public void testNamespaceConstraint() throws Exception {
    String nsName = TABLE_PREFIX + "_foo";
    String groupName = TABLE_PREFIX + "_foo";
    LOG.info("testNamespaceConstraint");
    addGroup(groupName, 1);
    assertTrue(OBSERVER.preAddRSGroupCalled);
    assertTrue(OBSERVER.postAddRSGroupCalled);

    ADMIN.createNamespace(NamespaceDescriptor.create(nsName)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, groupName).build());
    RSGroupInfo rsGroupInfo = ADMIN.getRSGroup(groupName);
    ADMIN.moveServersToRSGroup(rsGroupInfo.getServers(), RSGroupInfo.DEFAULT_GROUP);
    // test removing a referenced group
    try {
      ADMIN.removeRSGroup(groupName);
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
    // test modify group
    // changing with the same name is fine
    ADMIN.modifyNamespace(NamespaceDescriptor.create(nsName)
      .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, groupName).build());
    String anotherGroup = TABLE_PREFIX + "_anotherGroup";
    ADMIN.addRSGroup(anotherGroup);
    // test add non-existent group
    ADMIN.deleteNamespace(nsName);
    ADMIN.removeRSGroup(groupName);
    assertTrue(OBSERVER.preRemoveRSGroupCalled);
    assertTrue(OBSERVER.postRemoveRSGroupCalled);
    try {
      ADMIN.createNamespace(NamespaceDescriptor.create(nsName)
        .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, "foo").build());
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
  }

  @Test
  public void testFailRemoveGroup() throws IOException, InterruptedException {
    int initNumGroups = ADMIN.listRSGroups().size();
    addGroup("bar", 3);
    TEST_UTIL.createTable(tableName, Bytes.toBytes("f"));
    ADMIN.setRSGroup(Sets.newHashSet(tableName), "bar");
    RSGroupInfo barGroup = RS_GROUP_ADMIN_CLIENT.getRSGroupInfo("bar");
    // group is not empty therefore it should fail
    try {
      ADMIN.removeRSGroup(barGroup.getName());
      fail("Expected remove group to fail");
    } catch (IOException e) {
    }
    // group cannot lose all it's servers therefore it should fail
    try {
      ADMIN.moveServersToRSGroup(barGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);
      fail("Expected move servers to fail");
    } catch (IOException e) {
    }

    ADMIN.setRSGroup(barGroup.getTables(), RSGroupInfo.DEFAULT_GROUP);
    try {
      ADMIN.removeRSGroup(barGroup.getName());
      fail("Expected move servers to fail");
    } catch (IOException e) {
    }

    ADMIN.moveServersToRSGroup(barGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);
    ADMIN.removeRSGroup(barGroup.getName());

    assertEquals(initNumGroups, ADMIN.listRSGroups().size());
  }

  @Test
  public void testMultiTableMove() throws Exception {
    final TableName tableNameA =
      TableName.valueOf(TABLE_PREFIX + getNameWithoutIndex(name.getMethodName()) + "A");
    final TableName tableNameB =
      TableName.valueOf(TABLE_PREFIX + getNameWithoutIndex(name.getMethodName()) + "B");
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName(getNameWithoutIndex(name.getMethodName()));
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

    RSGroupInfo tableGrpA = ADMIN.getRSGroup(tableNameA);
    assertTrue(tableGrpA.getName().equals(RSGroupInfo.DEFAULT_GROUP));

    RSGroupInfo tableGrpB = ADMIN.getRSGroup(tableNameB);
    assertTrue(tableGrpB.getName().equals(RSGroupInfo.DEFAULT_GROUP));
    // change table's group
    LOG.info("Moving table [" + tableNameA + "," + tableNameB + "] to " + newGroup.getName());
    ADMIN.setRSGroup(Sets.newHashSet(tableNameA, tableNameB), newGroup.getName());

    // verify group change
    assertEquals(newGroup.getName(), ADMIN.getRSGroup(tableNameA).getName());

    assertEquals(newGroup.getName(), ADMIN.getRSGroup(tableNameB).getName());

    // verify tables' not exist in old group
    Set<TableName> defaultTables =
      RS_GROUP_ADMIN_CLIENT.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables();
    assertFalse(defaultTables.contains(tableNameA));
    assertFalse(defaultTables.contains(tableNameB));

    // verify tables' exist in new group
    Set<TableName> newGroupTables = RS_GROUP_ADMIN_CLIENT.getRSGroupInfo(newGroupName).getTables();
    assertTrue(newGroupTables.contains(tableNameA));
    assertTrue(newGroupTables.contains(tableNameB));
  }

  @Test
  public void testTableMoveTruncateAndDrop() throws Exception {
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName(getNameWithoutIndex(name.getMethodName()));
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

    RSGroupInfo tableGrp = ADMIN.getRSGroup(tableName);
    LOG.info("got table group info is {}", tableGrp);
    assertTrue(tableGrp.getName().equals(RSGroupInfo.DEFAULT_GROUP));

    // change table's group
    LOG.info("Moving table " + tableName + " to " + newGroup.getName());
    ADMIN.setRSGroup(Sets.newHashSet(tableName), newGroup.getName());

    // verify group change
    assertEquals(newGroup.getName(), ADMIN.getRSGroup(tableName).getName());

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
    ADMIN.disableTable(tableName);
    ADMIN.truncateTable(tableName, true);
    assertEquals(1, RS_GROUP_ADMIN_CLIENT.getRSGroupInfo(newGroup.getName()).getTables().size());
    assertEquals(tableName,
      RS_GROUP_ADMIN_CLIENT.getRSGroupInfo(newGroup.getName()).getTables().first());

    // verify removed table is removed from group
    TEST_UTIL.deleteTable(tableName);
    assertEquals(0, RS_GROUP_ADMIN_CLIENT.getRSGroupInfo(newGroup.getName()).getTables().size());
  }

  @Test
  public void testDisabledTableMove() throws Exception {
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName(getNameWithoutIndex(name.getMethodName()));
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

    RSGroupInfo tableGrp = ADMIN.getRSGroup(tableName);
    assertTrue(tableGrp.getName().equals(RSGroupInfo.DEFAULT_GROUP));

    // test disable table
    ADMIN.disableTable(tableName);

    // change table's group
    LOG.info("Moving table " + tableName + " to " + newGroup.getName());
    ADMIN.setRSGroup(Sets.newHashSet(tableName), newGroup.getName());

    // verify group change
    assertEquals(newGroup.getName(), ADMIN.getRSGroup(tableName).getName());
  }

  @Test
  public void testNonExistentTableMove() throws Exception {
    TableName tableName =
      TableName.valueOf(TABLE_PREFIX + getNameWithoutIndex(name.getMethodName()));
    RSGroupInfo tableGrp = ADMIN.getRSGroup(tableName);
    assertNull(tableGrp);

    // test if table exists already.
    boolean exist = ADMIN.tableExists(tableName);
    assertFalse(exist);

    LOG.info("Moving table " + tableName + " to " + RSGroupInfo.DEFAULT_GROUP);
    try {
      ADMIN.setRSGroup(Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);
      fail("Table " + tableName + " shouldn't have been successfully moved.");
    } catch (IOException ex) {
      assertTrue(ex instanceof TableNotFoundException);
    }

    try {
      ADMIN.setRSGroup(Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);
      ADMIN.moveServersToRSGroup(Sets.newHashSet(Address.fromParts("bogus", 123)),
        RSGroupInfo.DEFAULT_GROUP);
      fail("Table " + tableName + " shouldn't have been successfully moved.");
    } catch (IOException ex) {
      assertTrue(ex instanceof TableNotFoundException);
    }
    // verify group change
    assertNull(ADMIN.getRSGroup(tableName));
  }

  @Test
  public void testRSGroupListDoesNotContainFailedTableCreation() throws Exception {
    toggleQuotaCheckAndRestartMiniCluster(true);
    String nsp = "np1";
    NamespaceDescriptor nspDesc =
      NamespaceDescriptor.create(nsp).addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "5")
        .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    ADMIN.createNamespace(nspDesc);
    assertEquals(3, ADMIN.listNamespaceDescriptors().length);
    ColumnFamilyDescriptor fam1 = ColumnFamilyDescriptorBuilder.of("fam1");
    TableDescriptor tableDescOne = TableDescriptorBuilder
      .newBuilder(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table1"))
      .setColumnFamily(fam1).build();
    ADMIN.createTable(tableDescOne);

    TableDescriptor tableDescTwo = TableDescriptorBuilder
      .newBuilder(TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table2"))
      .setColumnFamily(fam1).build();
    boolean constraintViolated = false;

    try {
      ADMIN.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 6);
      fail("Creation table should fail because of quota violation.");
    } catch (Exception exp) {
      assertTrue(exp instanceof IOException);
      constraintViolated = true;
    } finally {
      assertTrue("Constraint not violated for table " + tableDescTwo.getTableName(),
        constraintViolated);
    }
    List<RSGroupInfo> rsGroupInfoList = RS_GROUP_ADMIN_CLIENT.listRSGroups();
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
    ADMIN.deleteNamespace(nspDesc.getName());
    toggleQuotaCheckAndRestartMiniCluster(false);

  }

  @Test
  public void testNotMoveTableToNullRSGroupWhenCreatingExistingTable() throws Exception {
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
    // Could not verify until the rollback of CreateTableProcedure is done
    // (that is, the coprocessor finishes its work),
    // or the table is still in the "default" rsgroup even though HBASE-21866
    // is not fixed.
    TEST_UTIL.waitFor(5000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (MASTER.getMasterProcedureExecutor().getActiveExecutorCount() == 0);
      }
    });
    SortedSet<TableName> tables =
      RS_GROUP_ADMIN_CLIENT.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables();
    assertTrue("Table 't1' must be in 'default' rsgroup", tables.contains(tn1));

    // Cleanup
    TEST_UTIL.deleteTable(tn1);
  }
}
