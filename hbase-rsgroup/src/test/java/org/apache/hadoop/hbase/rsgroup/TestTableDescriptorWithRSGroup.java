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

package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({ LargeTests.class })
public class TestTableDescriptorWithRSGroup extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableDescriptorWithRSGroup.class);
  private final byte[] familyNameBytes = Bytes.toBytes("f1");

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
  public void testCreateTableInTableDescriptorSpecificRSGroup() throws Exception {
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 1);
    assertEquals(0, newGroup.getTables().size());

    // assertion is done in createTableInRSGroup
    createTableWithRSGroupDetail(newGroup.getName());

    // Create table should fail if specified rs group does not exist.
    try {
      TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName.getNameAsString() + "_2"))
          .setRegionServerGroup("nonExistingRSGroup")
          .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f1")).build())
          .build();
      admin.createTable(desc, getSpitKeys(6));
      fail("Should have thrown ConstraintException but no exception thrown.");
    } catch (ConstraintException e) {
      assertEquals(e.getMessage(), "Region server group nonExistingRSGroup does not exist.");
    }
  }

  private void createTableWithRSGroupDetail(String newGroup) throws Exception {
    // Create table

    ColumnFamilyDescriptor f1 = ColumnFamilyDescriptorBuilder.newBuilder(familyNameBytes).build();
    TableDescriptor desc =
      TableDescriptorBuilder.newBuilder(tableName).setRegionServerGroup(newGroup)
        .setColumnFamily(f1).build();
    admin.createTable(desc, getSpitKeys(5));

    TEST_UTIL.waitFor(WAIT_TIMEOUT, (Waiter.Predicate<Exception>) () -> {
      List<String> regions = getTableRegionMap().get(tableName);
      if (regions == null) {
        return false;
      }

      return getTableRegionMap().get(tableName).size() >= 5;
    });
    TableDescriptor descriptor = admin.getConnection().getTable(tableName).getDescriptor();
    Optional<String> regionServerGroup = descriptor.getRegionServerGroup();
    assertTrue("RSGroup info is not updated into TableDescriptor when table is created.",
      regionServerGroup.isPresent());
    assertEquals(newGroup, regionServerGroup.get());
  }

  // moveTables should update rs group info in table descriptor
  @Test
  public void testMoveTablesShouldUpdateTableDescriptor() throws Exception {
    RSGroupInfo rsGroup1 = addGroup("rsGroup1", 1);
    assertEquals(0, rsGroup1.getTables().size());

    createTableWithRSGroupDetail(rsGroup1.getName());
    TableDescriptor descriptor = admin.getConnection().getTable(tableName).getDescriptor();
    Optional<String> regionServerGroup = descriptor.getRegionServerGroup();
    assertTrue("RSGroup info is not updated into TableDescriptor when table created",
      regionServerGroup.isPresent());
    assertEquals(rsGroup1.getName(), regionServerGroup.get());

    // moveTables
    RSGroupInfo rsGroup2 = addGroup("rsGroup2", 1);
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName), rsGroup2.getName());
    descriptor = admin.getConnection().getTable(tableName).getDescriptor();
    regionServerGroup = descriptor.getRegionServerGroup();
    assertTrue("RSGroup info is not updated into TableDescriptor when table moved",
      regionServerGroup.isPresent());
    assertEquals(rsGroup2.getName(), regionServerGroup.get());
  }

  // moveServersAndTables should update rs group info in table descriptor
  @Test
  public void testMoveServersAndTablesShouldUpdateTableDescriptor() throws Exception {
    RSGroupInfo rsGroup1 = addGroup("rsGroup1", 2);
    assertEquals(0, rsGroup1.getTables().size());

    createTableWithRSGroupDetail(rsGroup1.getName());
    TableDescriptor descriptor = admin.getConnection().getTable(tableName).getDescriptor();
    Optional<String> regionServerGroup = descriptor.getRegionServerGroup();
    assertTrue("RSGroup info is not updated into TableDescriptor when table created",
      regionServerGroup.isPresent());
    assertEquals(rsGroup1.getName(), regionServerGroup.get());

    // moveServersAndTables
    rsGroupAdmin.moveServersAndTables(Sets.newHashSet(rsGroup1.getServers().iterator().next()),
      Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);

    descriptor = admin.getConnection().getTable(tableName).getDescriptor();
    regionServerGroup = descriptor.getRegionServerGroup();
    assertTrue("RSGroup info is not updated into TableDescriptor when table moved",
      regionServerGroup.isPresent());
    assertEquals(RSGroupInfo.DEFAULT_GROUP, regionServerGroup.get());

  }

  @Test
  public void testRenameRSGroupUpdatesTableDescriptor() throws Exception {
    RSGroupInfo oldGroup = addGroup("oldGroup", 1);
    assertEquals(0, oldGroup.getTables().size());

    createTableWithRSGroupDetail(oldGroup.getName());

    final String newGroupName = "newGroup";
    rsGroupAdmin.renameRSGroup(oldGroup.getName(), newGroupName);
    TableDescriptor descriptor = admin.getConnection().getTable(tableName).getDescriptor();
    Optional<String> regionServerGroup = descriptor.getRegionServerGroup();
    assertTrue("RSGroup info is not updated into TableDescriptor when rs group renamed",
      regionServerGroup.isPresent());
    assertEquals(newGroupName, regionServerGroup.get());
  }

  @Test
  public void testCloneSnapshotWithRSGroup() throws Exception {
    RSGroupInfo rsGroup1 = addGroup("rsGroup1", 1);
    assertEquals(0, rsGroup1.getTables().size());
    // Creates table in rsGroup1
    createTableWithRSGroupDetail(rsGroup1.getName());

    // Create snapshot
    final String snapshotName = "snapShot2";
    admin.snapshot(snapshotName, tableName);
    final List<SnapshotDescription> snapshotDescriptions =
      admin.listSnapshots(Pattern.compile("snapShot2"));
    assertEquals(1, snapshotDescriptions.size());
    assertEquals(snapshotName, snapshotDescriptions.get(0).getName());

    // Move table to different rs group then delete the old rs group
    RSGroupInfo rsGroup2 = addGroup("rsGroup2", 1);
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName), rsGroup2.getName());
    rsGroup2 = rsGroupAdmin.getRSGroupInfo(rsGroup2.getName());
    assertEquals(1, rsGroup2.getTables().size());
    assertEquals(rsGroup2.getTables().first(), tableName);

    // Clone Snapshot
    final TableName clonedTable1 = TableName.valueOf(tableName.getNameAsString() + "_1");
    admin.cloneSnapshot(Bytes.toBytes(snapshotName), clonedTable1);

    // Verify that cloned table is created into old rs group
    final RSGroupInfo rsGroupInfoOfTable = rsGroupAdmin.getRSGroupInfoOfTable(clonedTable1);
    assertEquals(rsGroup1.getName(), rsGroupInfoOfTable.getName());
    TableDescriptor descriptor = admin.getConnection().getTable(clonedTable1).getDescriptor();
    Optional<String> regionServerGroup = descriptor.getRegionServerGroup();
    assertTrue("RSGroup info is not updated into TableDescriptor when table is cloned.",
      regionServerGroup.isPresent());
    assertEquals(rsGroup1.getName(), regionServerGroup.get());

    // Delete table's original rs group, clone should fail.
    rsGroupAdmin
      .moveServersAndTables(Sets.newHashSet(rsGroup1.getServers()), Sets.newHashSet(clonedTable1),
        rsGroup2.getName());
    rsGroupAdmin.removeRSGroup(rsGroup1.getName());
    // Clone Snapshot
    final TableName clonedTable2 = TableName.valueOf(tableName.getNameAsString() + "_2");
    try {
      admin.cloneSnapshot(Bytes.toBytes(snapshotName), clonedTable2);
      fail("Should have thrown ConstraintException but no exception thrown.");
    } catch (ConstraintException e) {
      assertTrue(
        e.getCause().getMessage().contains("Region server group rsGroup1 does not exist."));
    }
  }

  // Modify table should validate rs group existence and move rs group if required
  @Test
  public void testMoveTablesWhenModifyTableWithNewRSGroup() throws Exception {
    RSGroupInfo rsGroup1 = addGroup("rsGroup1", 1);
    assertEquals(0, rsGroup1.getTables().size());

    createTableWithRSGroupDetail(rsGroup1.getName());
    TableDescriptor descriptor = admin.getConnection().getTable(tableName).getDescriptor();
    Optional<String> regionServerGroup = descriptor.getRegionServerGroup();
    assertTrue("RSGroup info is not updated into TableDescriptor when table created",
      regionServerGroup.isPresent());
    assertEquals(rsGroup1.getName(), regionServerGroup.get());

    final TableDescriptor newTableDescriptor =
      TableDescriptorBuilder.newBuilder(descriptor).setRegionServerGroup("rsGroup2").build();

    // ConstraintException as rsGroup2 does not exits
    try {
      admin.modifyTable(newTableDescriptor);
      fail("Should have thrown ConstraintException but no exception thrown.");
    } catch (ConstraintException e) {
      assertTrue(
        e.getCause().getMessage().contains("Region server group rsGroup2 does not exist."));
    }

    addGroup("rsGroup2", 1);
    // Table creation should be successful as rsGroup2 exists.
    admin.modifyTable(newTableDescriptor);

    // old group should not have table mapping now
    rsGroup1 = rsGroupAdmin.getRSGroupInfo("rsGroup1");
    assertEquals(0, rsGroup1.getTables().size());

    RSGroupInfo rsGroup2 = rsGroupAdmin.getRSGroupInfo("rsGroup2");
    assertEquals(1, rsGroup2.getTables().size());
    descriptor = admin.getConnection().getTable(tableName).getDescriptor();
    regionServerGroup = descriptor.getRegionServerGroup();
    assertTrue("RSGroup info is not updated into TableDescriptor when table is modified.",
      regionServerGroup.isPresent());
    assertEquals("rsGroup2", regionServerGroup.get());
  }

  @Test
  public void testTableShouldNotAddedIntoRSGroup_WhenModifyTableFails() throws Exception {
    RSGroupInfo rsGroup1 = addGroup("rsGroup1", 1);
    assertEquals(0, rsGroup1.getTables().size());

    createTableWithRSGroupDetail(rsGroup1.getName());
    TableDescriptor descriptor = admin.getConnection().getTable(tableName).getDescriptor();

    RSGroupInfo rsGroup2 = addGroup("rsGroup2", 1);
    final TableDescriptor newTableDescriptor =
      TableDescriptorBuilder.newBuilder(descriptor).setRegionServerGroup(rsGroup2.getName())
        .removeColumnFamily(familyNameBytes).build();

    // Removed family to fail pre-check validation
    try {
      admin.modifyTable(newTableDescriptor);
      fail("Should have thrown DoNotRetryIOException but no exception thrown.");
    } catch (DoNotRetryIOException e) {
      assertTrue(
        e.getCause().getMessage().contains("Table should have at least one column family"));
    }
    rsGroup2 = rsGroupAdmin.getRSGroupInfo(rsGroup2.getName());
    assertEquals("Table must not have moved to RSGroup as table modify failed", 0,
      rsGroup2.getTables().size());
  }

  @Test
  public void testTableShouldNotAddedIntoRSGroup_WhenTableCreationFails() throws Exception {
    RSGroupInfo rsGroup1 = addGroup("rsGroup1", 1);
    assertEquals(0, rsGroup1.getTables().size());

    // Create TableDescriptor without a family so creation fails
    TableDescriptor desc =
      TableDescriptorBuilder.newBuilder(tableName).setRegionServerGroup(rsGroup1.getName())
        .build();
    try {
      admin.createTable(desc, getSpitKeys(5));
      fail("Should have thrown DoNotRetryIOException but no exception thrown.");
    } catch (DoNotRetryIOException e) {
      assertTrue(
        e.getCause().getMessage().contains("Table should have at least one column family"));
    }
    rsGroup1 = rsGroupAdmin.getRSGroupInfo(rsGroup1.getName());
    assertEquals("Table must not have moved to RSGroup as table create operation failed", 0,
      rsGroup1.getTables().size());
  }

  @Test
  public void testSystemTablesCanBeMovedToNewRSGroupByModifyingTable() throws Exception {
    RSGroupInfo rsGroup1 = addGroup("rsGroup1", 1);
    assertEquals(0, rsGroup1.getTables().size());
    final TableName tableName = TableName.valueOf("hbase:meta");
    final TableDescriptor descriptor = admin.getConnection().getTable(tableName).getDescriptor();
    final TableDescriptor newTableDescriptor =
      TableDescriptorBuilder.newBuilder(descriptor).setRegionServerGroup(rsGroup1.getName())
        .build();
    admin.modifyTable(newTableDescriptor);
    final RSGroupInfo rsGroupInfoOfTable = rsGroupAdmin.getRSGroupInfoOfTable(tableName);
    assertEquals(rsGroup1.getName(), rsGroupInfoOfTable.getName());
  }

  @Test
  public void testUpdateTableDescriptorOnlyIfRSGroupInfoWasStoredInTableDescriptor()
    throws Exception {
    RSGroupInfo rsGroup1 = addGroup("rsGroup1", 1);
    assertEquals(0, rsGroup1.getTables().size());
    // create table with rs group info stored in table descriptor
    createTable(tableName, rsGroup1.getName());
    final TableName table2 = TableName.valueOf(tableName.getNameAsString() + "_2");
    // create table with no rs group info
    createTable(table2, null);
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName), rsGroup1.getName());
    assertTrue("RSGroup info is not updated into TableDescriptor when table created",
      admin.getConnection().getTable(tableName).getDescriptor().getRegionServerGroup()
        .isPresent());
    assertFalse("Table descriptor should not have been updated "
        + "as rs group info was not stored in table descriptor.",
      admin.getConnection().getTable(table2).getDescriptor().getRegionServerGroup().isPresent());

    final String rsGroup2 = "rsGroup2";
    rsGroupAdmin.renameRSGroup(rsGroup1.getName(), rsGroup2);
    assertEquals(rsGroup2,
      admin.getConnection().getTable(tableName).getDescriptor().getRegionServerGroup().get());
    assertFalse("Table descriptor should not have been updated "
        + "as rs group info was not stored in table descriptor.",
      admin.getConnection().getTable(table2).getDescriptor().getRegionServerGroup().isPresent());
  }

  @Test
  public void testModifyAndMoveTableScenario() throws Exception {
    RSGroupInfo rsGroup1 = addGroup("rsGroup1", 1);
    assertEquals(0, rsGroup1.getTables().size());
    // create table with rs group info stored in table descriptor
    createTable(tableName, rsGroup1.getName());
    final TableName table2 = TableName.valueOf(tableName.getNameAsString() + "_2");
    // create table with no rs group info
    createTable(table2, null);
    rsGroupAdmin.moveTables(Sets.newHashSet(table2), rsGroup1.getName());

    RSGroupInfo rsGroup2 = addGroup("rsGroup2", 1);
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName, table2), rsGroup2.getName());
    rsGroup2 = rsGroupAdmin.getRSGroupInfo(rsGroup2.getName());
    assertEquals("Table movement failed.", 2, rsGroup2.getTables().size());
  }

  private void createTable(TableName tName, String rsGroupName) throws Exception {
    ColumnFamilyDescriptor f1 = ColumnFamilyDescriptorBuilder.newBuilder(familyNameBytes).build();
    final TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(tName).setColumnFamily(f1);
    if (rsGroupName != null) {
      builder.setRegionServerGroup(rsGroupName);
    }
    TableDescriptor desc = builder.build();
    admin.createTable(desc, getSpitKeys(10));
    TEST_UTIL.waitFor(WAIT_TIMEOUT, (Waiter.Predicate<Exception>) () -> {
      List<String> regions = getTableRegionMap().get(tName);
      if (regions == null) {
        return false;
      }

      return getTableRegionMap().get(tName).size() >= 5;
    });
  }

  private byte[][] getSpitKeys(int numRegions) throws IOException {
    if (numRegions < 3) {
      throw new IOException("Must create at least 3 regions");
    }
    byte[] startKey = Bytes.toBytes("aaaaa");
    byte[] endKey = Bytes.toBytes("zzzzz");
    return Bytes.split(startKey, endKey, numRegions - 3);
  }
}
