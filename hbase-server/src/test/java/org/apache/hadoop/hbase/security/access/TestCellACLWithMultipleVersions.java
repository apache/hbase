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
package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({SecurityTests.class, MediumTests.class})
public class TestCellACLWithMultipleVersions extends SecureTestUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCellACLWithMultipleVersions.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCellACLWithMultipleVersions.class);

  @Rule
  public TableNameTestRule testTable = new TableNameTestRule();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] TEST_FAMILY1 = Bytes.toBytes("f1");
  private static final byte[] TEST_FAMILY2 = Bytes.toBytes("f2");
  private static final byte[] TEST_ROW = Bytes.toBytes("cellpermtest");
  private static final byte[] TEST_Q1 = Bytes.toBytes("q1");
  private static final byte[] TEST_Q2 = Bytes.toBytes("q2");
  private static final byte[] ZERO = Bytes.toBytes(0L);
  private static final byte[] ONE = Bytes.toBytes(1L);
  private static final byte[] TWO = Bytes.toBytes(2L);

  private static Configuration conf;

  private static final String GROUP = "group";
  private static User GROUP_USER;
  private static User USER_OWNER;
  private static User USER_OTHER;
  private static User USER_OTHER2;

  private static String[] usersAndGroups;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    // Enable security
    enableSecurity(conf);
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);

    // We expect 0.98 cell ACL semantics
    conf.setBoolean(AccessControlConstants.CF_ATTRIBUTE_EARLY_OUT, false);

    TEST_UTIL.startMiniCluster();
    MasterCoprocessorHost cpHost = TEST_UTIL.getMiniHBaseCluster().getMaster()
        .getMasterCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    AccessController ac = cpHost.findCoprocessor(AccessController.class);
    cpHost.createEnvironment(ac, Coprocessor.PRIORITY_HIGHEST, 1, conf);
    RegionServerCoprocessorHost rsHost = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getRegionServerCoprocessorHost();
    rsHost.createEnvironment(ac, Coprocessor.PRIORITY_HIGHEST, 1, conf);

    // Wait for the ACL table to become available
    TEST_UTIL.waitTableEnabled(PermissionStorage.ACL_TABLE_NAME);

    // create a set of test users
    USER_OWNER = User.createUserForTesting(conf, "owner", new String[0]);
    USER_OTHER = User.createUserForTesting(conf, "other", new String[0]);
    USER_OTHER2 = User.createUserForTesting(conf, "other2", new String[0]);
    GROUP_USER = User.createUserForTesting(conf, "group_user", new String[] { GROUP });

    usersAndGroups = new String[] { USER_OTHER.getShortName(), AuthUtil.toGroupEntry(GROUP) };
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(testTable.getTableName());
    ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor familyDescriptor =
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(TEST_FAMILY1);
    familyDescriptor.setMaxVersions(4);
    tableDescriptor.setOwner(USER_OWNER);
    tableDescriptor.setColumnFamily(familyDescriptor);
    familyDescriptor =
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(TEST_FAMILY2);
    familyDescriptor.setMaxVersions(4);
    tableDescriptor.setOwner(USER_OWNER);
    tableDescriptor.setColumnFamily(familyDescriptor);
    // Create the test table (owner added to the _acl_ table)
    try (Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      try (Admin admin = connection.getAdmin()) {
        admin.createTable(tableDescriptor, new byte[][] { Bytes.toBytes("s") });
      }
    }
    TEST_UTIL.waitTableEnabled(testTable.getTableName());
    LOG.info("Sleeping a second because of HBASE-12581");
    Threads.sleep(1000);
  }

  @Test
  public void testCellPermissionwithVersions() throws Exception {
    // store two sets of values, one store with a cell level ACL, and one
    // without
    final Map<String, Permission> writePerms = prepareCellPermissions(usersAndGroups, Action.WRITE);
    final Map<String, Permission> readPerms = prepareCellPermissions(usersAndGroups, Action.READ);
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection connection = ConnectionFactory.createConnection(conf);
            Table t = connection.getTable(testTable.getTableName())) {
          Put p;
          // with ro ACL
          p = new Put(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(writePerms);
          t.put(p);
          // with ro ACL
          p = new Put(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(readPerms);
          t.put(p);
          p = new Put(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(writePerms);
          t.put(p);
          p = new Put(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(readPerms);
          t.put(p);
          p = new Put(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(writePerms);
          t.put(p);
        }
        return null;
      }
    }, USER_OWNER);

    /* ---- Gets ---- */

    AccessTestAction getQ1 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW);
        get.readVersions(10);
        try(Connection connection = ConnectionFactory.createConnection(conf);
            Table t = connection.getTable(testTable.getTableName())) {
          return t.get(get).listCells();
        }
      }
    };

    AccessTestAction get2 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW);
        get.readVersions(10);
        try(Connection connection = ConnectionFactory.createConnection(conf);
            Table t = connection.getTable(testTable.getTableName())) {
          return t.get(get).listCells();
        }
      }
    };
    // Confirm special read access set at cell level

    verifyAllowed(GROUP_USER, getQ1, 2);
    verifyAllowed(USER_OTHER, getQ1, 2);

    // store two sets of values, one store with a cell level ACL, and one
    // without
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection connection = ConnectionFactory.createConnection(conf);
            Table t = connection.getTable(testTable.getTableName())) {
          Put p;
          p = new Put(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(writePerms);
          t.put(p);
          p = new Put(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(readPerms);
          t.put(p);
          p = new Put(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(writePerms);
          t.put(p);
        }
        return null;
      }
    }, USER_OWNER);
    // Confirm special read access set at cell level

    verifyAllowed(USER_OTHER, get2, 1);
    verifyAllowed(GROUP_USER, get2, 1);
  }

  private Map<String, Permission> prepareCellPermissions(String[] users, Action... action) {
    Map<String, Permission> perms = new HashMap<>(2);
    for (String user : users) {
      perms.put(user, new Permission(action));
    }
    return perms;
  }

  @Test
  public void testCellPermissionsWithDeleteMutipleVersions() throws Exception {
    // table/column/qualifier level permissions
    final byte[] TEST_ROW1 = Bytes.toBytes("r1");
    final byte[] TEST_ROW2 = Bytes.toBytes("r2");
    final byte[] TEST_Q1 = Bytes.toBytes("q1");
    final byte[] TEST_Q2 = Bytes.toBytes("q2");
    final byte[] ZERO = Bytes.toBytes(0L);

    // additional test user
    final User user1 = User.createUserForTesting(conf, "user1", new String[0]);
    final User user2 = User.createUserForTesting(conf, "user2", new String[0]);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            // with rw ACL for "user1"
            Put p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
            p.addColumn(TEST_FAMILY1, TEST_Q2, ZERO);
            p.setACL(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            t.put(p);
            // with rw ACL for "user1"
            p = new Put(TEST_ROW2);
            p.addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
            p.addColumn(TEST_FAMILY1, TEST_Q2, ZERO);
            p.setACL(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            // with rw ACL for "user1", "user2" and "@group"
            Put p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
            p.addColumn(TEST_FAMILY1, TEST_Q2, ZERO);
            Map<String, Permission> perms =
                prepareCellPermissions(new String[] { user1.getShortName(), user2.getShortName(),
                    AuthUtil.toGroupEntry(GROUP) }, Action.READ, Action.WRITE);
            p.setACL(perms);
            t.put(p);
            // with rw ACL for "user1", "user2" and "@group"
            p = new Put(TEST_ROW2);
            p.addColumn(TEST_FAMILY1, TEST_Q1, ZERO);
            p.addColumn(TEST_FAMILY1, TEST_Q2, ZERO);
            p.setACL(perms);
            t.put(p);
          }
        }
        return null;
      }
    }, user1);

    // user1 should be allowed to delete TEST_ROW1 as he is having write permission on both
    // versions of the cells
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Delete d = new Delete(TEST_ROW1);
            d.addColumns(TEST_FAMILY1, TEST_Q1);
            d.addColumns(TEST_FAMILY1, TEST_Q2);
            t.delete(d);
          }
        }
        return null;
      }
    });
    // user2 should not be allowed to delete TEST_ROW2 as he is having write permission only on one
    // version of the cells.
    verifyUserDeniedForDeleteMultipleVersions(user2, TEST_ROW2, TEST_Q1, TEST_Q2);

    // GROUP_USER should not be allowed to delete TEST_ROW2 as he is having write permission only on
    // one version of the cells.
    verifyUserDeniedForDeleteMultipleVersions(GROUP_USER, TEST_ROW2, TEST_Q1, TEST_Q2);

    // user1 should be allowed to delete the cf. (All data under cf for a row)
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Delete d = new Delete(TEST_ROW2);
            d.addFamily(TEST_FAMILY1);
            t.delete(d);
          }
        }
        return null;
      }
    });
  }

  private void verifyUserDeniedForDeleteMultipleVersions(final User user, final byte[] row,
      final byte[] q1, final byte[] q2) throws IOException, InterruptedException {
    user.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Delete d = new Delete(row);
            d.addColumns(TEST_FAMILY1, q1);
            d.addColumns(TEST_FAMILY1, q2);
            t.delete(d);
            fail(user.getShortName() + " should not be allowed to delete the row");
          } catch (Exception e) {

          }
        }
        return null;
      }
    });
  }


  @Test
  public void testDeleteWithFutureTimestamp() throws Exception {
    // Store two values, one in the future

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            // Store a read write ACL without a timestamp, server will use current time
            Put p = new Put(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q2, ONE);
            Map<String, Permission> readAndWritePerms =
                prepareCellPermissions(usersAndGroups, Action.READ, Action.WRITE);
            p.setACL(readAndWritePerms);
            t.put(p);
            p = new Put(TEST_ROW).addColumn(TEST_FAMILY2, TEST_Q2, ONE);
            p.setACL(readAndWritePerms);
            t.put(p);
            LOG.info("Stored at current time");
            // Store read only ACL at a future time
            p = new Put(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1,
                EnvironmentEdgeManager.currentTime() + 1000000, ZERO);
            p.setACL(prepareCellPermissions(new String[]{ USER_OTHER.getShortName(),
                AuthUtil.toGroupEntry(GROUP)}, Action.READ));
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    // Confirm stores are visible

    AccessTestAction getQ1 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            return t.get(get).listCells();
          }
        }
      }
    };

    AccessTestAction getQ2 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q2);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            return t.get(get).listCells();
          }
        }
      }
    };

    verifyAllowed(getQ1, USER_OWNER, USER_OTHER, GROUP_USER);
    verifyAllowed(getQ2, USER_OWNER, USER_OTHER, GROUP_USER);


    // Issue a DELETE for the family, should succeed because the future ACL is
    // not considered
    AccessTestAction deleteFamily1 = getDeleteFamilyAction(TEST_FAMILY1);
    AccessTestAction deleteFamily2 = getDeleteFamilyAction(TEST_FAMILY2);

    verifyAllowed(deleteFamily1, USER_OTHER);
    verifyAllowed(deleteFamily2, GROUP_USER);

    // The future put should still exist

    verifyAllowed(getQ1, USER_OWNER, USER_OTHER,GROUP_USER);

    // The other put should be covered by the tombstone

    verifyIfNull(getQ2, USER_OTHER, GROUP_USER);
  }

  private AccessTestAction getDeleteFamilyAction(final byte[] fam) {
    AccessTestAction deleteFamilyAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Delete delete = new Delete(TEST_ROW).addFamily(fam);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            t.delete(delete);
          }
        }
        return null;
      }
    };
    return deleteFamilyAction;
  }

  @Test
  public void testCellPermissionsWithDeleteWithUserTs() throws Exception {
    USER_OWNER.runAs(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            // This version (TS = 123) with rw ACL for USER_OTHER and USER_OTHER2
            Put p = new Put(TEST_ROW);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 123L, ZERO);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 123L, ZERO);
            p.setACL(prepareCellPermissions(
              new String[] { USER_OTHER.getShortName(), AuthUtil.toGroupEntry(GROUP),
                  USER_OTHER2.getShortName() }, Permission.Action.READ, Permission.Action.WRITE));
            t.put(p);

            // This version (TS = 125) with rw ACL for USER_OTHER
            p = new Put(TEST_ROW);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 125L, ONE);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 125L, ONE);
            p.setACL(prepareCellPermissions(
              new String[] { USER_OTHER.getShortName(), AuthUtil.toGroupEntry(GROUP) },
              Action.READ, Action.WRITE));
            t.put(p);

            // This version (TS = 127) with rw ACL for USER_OTHER
            p = new Put(TEST_ROW);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 127L, TWO);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 127L, TWO);
            p.setACL(prepareCellPermissions(
              new String[] { USER_OTHER.getShortName(), AuthUtil.toGroupEntry(GROUP) },
              Action.READ, Action.WRITE));
            t.put(p);

            return null;
          }
        }
      }
    });

    // USER_OTHER2 should be allowed to delete the column f1:q1 versions older than TS 124L
    USER_OTHER2.runAs(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Delete d = new Delete(TEST_ROW, 124L);
            d.addColumns(TEST_FAMILY1, TEST_Q1);
            t.delete(d);
          }
        }
        return null;
      }
    });

    // USER_OTHER2 should be allowed to delete the column f1:q2 versions older than TS 124L
    USER_OTHER2.runAs(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Delete d = new Delete(TEST_ROW);
            d.addColumns(TEST_FAMILY1, TEST_Q2, 124L);
            t.delete(d);
          }
        }
        return null;
      }
    });
  }

  @Test
  public void testCellPermissionsWithDeleteExactVersion() throws Exception {
    final byte[] TEST_ROW1 = Bytes.toBytes("r1");
    final byte[] TEST_Q1 = Bytes.toBytes("q1");
    final byte[] TEST_Q2 = Bytes.toBytes("q2");
    final byte[] ZERO = Bytes.toBytes(0L);

    final User user1 = User.createUserForTesting(conf, "user1", new String[0]);
    final User user2 = User.createUserForTesting(conf, "user2", new String[0]);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Map<String, Permission> permsU1andOwner =
                prepareCellPermissions(
                  new String[] { user1.getShortName(), USER_OWNER.getShortName() }, Action.READ,
                  Action.WRITE);
            Map<String, Permission> permsU2andGUandOwner =
                prepareCellPermissions(
                  new String[] { user2.getShortName(), AuthUtil.toGroupEntry(GROUP),
                      USER_OWNER.getShortName() }, Action.READ, Action.WRITE);
            Put p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 123, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 123, ZERO);
            p.setACL(permsU2andGUandOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY2, TEST_Q1, 123, ZERO);
            p.addColumn(TEST_FAMILY2, TEST_Q2, 123, ZERO);
            p.setACL(permsU2andGUandOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY2, TEST_Q1, 125, ZERO);
            p.addColumn(TEST_FAMILY2, TEST_Q2, 125, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 127, ZERO);
            p.setACL(permsU2andGUandOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 127, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY2, TEST_Q1, 129, ZERO);
            p.addColumn(TEST_FAMILY2, TEST_Q2, 129, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    // user1 should be allowed to delete TEST_ROW1 as he is having write permission on both
    // versions of the cells
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Delete d = new Delete(TEST_ROW1);
            d.addColumn(TEST_FAMILY1, TEST_Q1, 123);
            d.addColumn(TEST_FAMILY1, TEST_Q2);
            d.addFamilyVersion(TEST_FAMILY2, 125);
            t.delete(d);
          }
        }
        return null;
      }
    });

    verifyUserDeniedForDeleteExactVersion(user2, TEST_ROW1, TEST_Q1, TEST_Q2);
    verifyUserDeniedForDeleteExactVersion(GROUP_USER, TEST_ROW1, TEST_Q1, TEST_Q2);
  }

  private void verifyUserDeniedForDeleteExactVersion(final User user, final byte[] row,
      final byte[] q1, final byte[] q2) throws IOException, InterruptedException {
    user.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Delete d = new Delete(row, 127);
            d.addColumns(TEST_FAMILY1, q1);
            d.addColumns(TEST_FAMILY1, q2);
            d.addFamily(TEST_FAMILY2, 129);
            t.delete(d);
            fail(user.getShortName() + " can not do the delete");
          } catch (Exception e) {

          }
        }
        return null;
      }
    });
  }

  @Test
  public void testCellPermissionsForIncrementWithMultipleVersions() throws Exception {
    final byte[] TEST_ROW1 = Bytes.toBytes("r1");
    final byte[] TEST_Q1 = Bytes.toBytes("q1");
    final byte[] TEST_Q2 = Bytes.toBytes("q2");
    final byte[] ZERO = Bytes.toBytes(0L);

    final User user1 = User.createUserForTesting(conf, "user1", new String[0]);
    final User user2 = User.createUserForTesting(conf, "user2", new String[0]);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Map<String, Permission> permsU1andOwner =
                prepareCellPermissions(
                  new String[] { user1.getShortName(), USER_OWNER.getShortName() }, Action.READ,
                  Action.WRITE);
            Map<String, Permission> permsU2andGUandOwner =
                prepareCellPermissions(
                  new String[] { user2.getShortName(), AuthUtil.toGroupEntry(GROUP),
                      USER_OWNER.getShortName() }, Action.READ, Action.WRITE);
            Put p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 123, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 123, ZERO);
            p.setACL(permsU2andGUandOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 127, ZERO);
            p.setACL(permsU2andGUandOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 127, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    // Increment considers the TimeRange set on it.
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Increment inc = new Increment(TEST_ROW1);
            inc.setTimeRange(0, 123);
            inc.addColumn(TEST_FAMILY1, TEST_Q1, 2L);
            t.increment(inc);
            t.incrementColumnValue(TEST_ROW1, TEST_FAMILY1, TEST_Q2, 1L);
          }
        }
        return null;
      }
    });

    verifyUserDeniedForIncrementMultipleVersions(user2, TEST_ROW1, TEST_Q2);
    verifyUserDeniedForIncrementMultipleVersions(GROUP_USER, TEST_ROW1, TEST_Q2);
  }

  private void verifyUserDeniedForIncrementMultipleVersions(final User user, final byte[] row,
      final byte[] q1) throws IOException, InterruptedException {
    user.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Increment inc = new Increment(row);
            inc.setTimeRange(0, 127);
            inc.addColumn(TEST_FAMILY1, q1, 2L);
            t.increment(inc);
            fail(user.getShortName() + " cannot do the increment.");
          } catch (Exception e) {

          }
        }
        return null;
      }
    });
  }

  @Test
  public void testCellPermissionsForPutWithMultipleVersions() throws Exception {
    final byte[] TEST_ROW1 = Bytes.toBytes("r1");
    final byte[] TEST_Q1 = Bytes.toBytes("q1");
    final byte[] TEST_Q2 = Bytes.toBytes("q2");
    final byte[] ZERO = Bytes.toBytes(0L);

    final User user1 = User.createUserForTesting(conf, "user1", new String[0]);
    final User user2 = User.createUserForTesting(conf, "user2", new String[0]);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Map<String, Permission> permsU1andOwner =
                prepareCellPermissions(
                  new String[] { user1.getShortName(), USER_OWNER.getShortName() }, Action.READ,
                  Action.WRITE);
            Map<String, Permission> permsU2andGUandOwner =
                prepareCellPermissions(
                  new String[] { user1.getShortName(), AuthUtil.toGroupEntry(GROUP),
                      USER_OWNER.getShortName() }, Action.READ, Action.WRITE);
            permsU2andGUandOwner.put(user2.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU2andGUandOwner.put(USER_OWNER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            Put p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 123, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 123, ZERO);
            p.setACL(permsU2andGUandOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 127, ZERO);
            p.setACL(permsU2andGUandOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 127, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    // new Put with TEST_Q1 column having TS=125. This covers old cell with TS 123 and user1 is
    // having RW permission. While TEST_Q2 is with latest TS and so it covers old cell with TS 127.
    // User1 is having RW permission on that too.
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Put p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 125, ZERO);
            p.addColumn(TEST_FAMILY1, TEST_Q2, ZERO);
            p.setACL(user2.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            t.put(p);
          }
        }
        return null;
      }
    });

    verifyUserDeniedForPutMultipleVersions(user2, TEST_ROW1, TEST_Q1, TEST_Q2, ZERO);
    verifyUserDeniedForPutMultipleVersions(GROUP_USER, TEST_ROW1, TEST_Q1, TEST_Q2, ZERO);
  }

  private void verifyUserDeniedForPutMultipleVersions(final User user, final byte[] row,
      final byte[] q1, final byte[] q2, final byte[] value) throws IOException,
      InterruptedException {
    user.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Put p = new Put(row);
            // column Q1 covers version at 123 fr which user2 do not have permission
            p.addColumn(TEST_FAMILY1, q1, 124, value);
            p.addColumn(TEST_FAMILY1, q2, value);
            t.put(p);
            fail(user.getShortName() + " cannot do the put.");
          } catch (Exception e) {

          }
        }
        return null;
      }
    });
  }

  @Test
  public void testCellPermissionsForCheckAndDelete() throws Exception {
    final byte[] TEST_ROW1 = Bytes.toBytes("r1");
    final byte[] TEST_Q3 = Bytes.toBytes("q3");
    final byte[] ZERO = Bytes.toBytes(0L);

    final User user1 = User.createUserForTesting(conf, "user1", new String[0]);
    final User user2 = User.createUserForTesting(conf, "user2", new String[0]);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Map<String, Permission> permsU1andOwner =
                prepareCellPermissions(
                  new String[] { user1.getShortName(), USER_OWNER.getShortName() }, Action.READ,
                  Action.WRITE);
            Map<String, Permission> permsU1andU2andGUandOwner =
                prepareCellPermissions(new String[] { user1.getShortName(), user2.getShortName(),
                    AuthUtil.toGroupEntry(GROUP), USER_OWNER.getShortName() }, Action.READ,
                  Action.WRITE);
            Map<String, Permission> permsU1_U2andGU =
                prepareCellPermissions(new String[] { user1.getShortName(), user2.getShortName(),
                    AuthUtil.toGroupEntry(GROUP) }, Action.READ, Action.WRITE);

            Put p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 120, ZERO);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 120, ZERO);
            p.addColumn(TEST_FAMILY1, TEST_Q3, 120, ZERO);
            p.setACL(permsU1andU2andGUandOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 123, ZERO);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 123, ZERO);
            p.addColumn(TEST_FAMILY1, TEST_Q3, 123, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q1, 127, ZERO);
            p.setACL(permsU1_U2andGU);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q2, 127, ZERO);
            p.setACL(user2.getShortName(), new Permission(Permission.Action.READ));
            t.put(p);

            p = new Put(TEST_ROW1);
            p.addColumn(TEST_FAMILY1, TEST_Q3, 127, ZERO);
            p.setACL(AuthUtil.toGroupEntry(GROUP), new Permission(Permission.Action.READ));
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    // user1 should be allowed to do the checkAndDelete. user1 having read permission on the latest
    // version cell and write permission on all versions
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Delete d = new Delete(TEST_ROW1);
            d.addColumns(TEST_FAMILY1, TEST_Q1, 120);
            t.checkAndMutate(TEST_ROW1, TEST_FAMILY1).qualifier(TEST_Q1)
                .ifEquals(ZERO).thenDelete(d);
          }
        }
        return null;
      }
    });
    // user2 shouldn't be allowed to do the checkAndDelete. user2 having RW permission on the latest
    // version cell but not on cell version TS=123
    verifyUserDeniedForCheckAndDelete(user2, TEST_ROW1, ZERO);

    // GROUP_USER shouldn't be allowed to do the checkAndDelete. GROUP_USER having RW permission on
    // the latest
    // version cell but not on cell version TS=123
    verifyUserDeniedForCheckAndDelete(GROUP_USER, TEST_ROW1, ZERO);

    // user2 should be allowed to do the checkAndDelete when delete tries to delete the old version
    // TS=120. user2 having R permission on the latest version(no W permission) cell
    // and W permission on cell version TS=120.
    verifyUserAllowedforCheckAndDelete(user2, TEST_ROW1, TEST_Q2, ZERO);

    // GROUP_USER should be allowed to do the checkAndDelete when delete tries to delete the old
    // version
    // TS=120. user2 having R permission on the latest version(no W permission) cell
    // and W permission on cell version TS=120.
    verifyUserAllowedforCheckAndDelete(GROUP_USER, TEST_ROW1, TEST_Q3, ZERO);
  }

  private void verifyUserAllowedforCheckAndDelete(final User user, final byte[] row,
      final byte[] q1, final byte[] value) throws IOException, InterruptedException {
    user.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Delete d = new Delete(row);
            d.addColumn(TEST_FAMILY1, q1, 120);
            t.checkAndMutate(row, TEST_FAMILY1).qualifier(q1).ifEquals(value).thenDelete(d);
          }
        }
        return null;
      }
    });
  }

  private void verifyUserDeniedForCheckAndDelete(final User user, final byte[] row,
      final byte[] value) throws IOException, InterruptedException {
    user.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(testTable.getTableName())) {
            Delete d = new Delete(row);
            d.addColumns(TEST_FAMILY1, TEST_Q1);
            t.checkAndMutate(row, TEST_FAMILY1).qualifier(TEST_Q1).ifEquals(value).thenDelete(d);
            fail(user.getShortName() + " should not be allowed to do checkAndDelete");
          } catch (Exception e) {
          }
        }
        return null;
      }
    });
  }

  @After
  public void tearDown() throws Exception {
    // Clean the _acl_ table
    try {
      TEST_UTIL.deleteTable(testTable.getTableName());
    } catch (TableNotFoundException ex) {
      // Test deleted the table, no problem
      LOG.info("Test deleted table " + testTable.getTableName());
    }
    assertEquals(0, PermissionStorage.getTablePermissions(conf, testTable.getTableName()).size());
  }
}
