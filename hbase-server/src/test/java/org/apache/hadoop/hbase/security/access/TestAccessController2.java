/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TestTableName;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestAccessController2 extends SecureTestUtil {
  private static final Log LOG = LogFactory.getLog(TestAccessController2.class);

  private static final byte[] TEST_ROW = Bytes.toBytes("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("q");
  private static final byte[] TEST_VALUE = Bytes.toBytes("value");

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  private final static byte[] Q1 = Bytes.toBytes("q1");
  private final static byte[] value1 = Bytes.toBytes("value1");

  private static byte[] TEST_FAMILY_2 = Bytes.toBytes("f2");
  private static byte[] TEST_ROW_2 = Bytes.toBytes("r2");
  private final static byte[] Q2 = Bytes.toBytes("q2");
  private final static byte[] value2 = Bytes.toBytes("value2");

  private static byte[] TEST_ROW_3 = Bytes.toBytes("r3");

  private static final String TESTGROUP_1 = "testgroup_1";
  private static final String TESTGROUP_2 = "testgroup_2";

  private static User TESTGROUP1_USER1;
  private static User TESTGROUP2_USER1;

  @Rule
  public TestTableName TEST_TABLE = new TestTableName();
  private String namespace = "testNamespace";
  private String tname = namespace + ":testtable1";
  private byte[] tableName = Bytes.toBytes(tname);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Enable security
    enableSecurity(conf);
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);
    TEST_UTIL.startMiniCluster();
    // Wait for the ACL table to become available
    TEST_UTIL.waitUntilAllRegionsAssigned(AccessControlLists.ACL_TABLE_NAME);

    TESTGROUP1_USER1 =
        User.createUserForTesting(conf, "testgroup1_user1", new String[] { TESTGROUP_1 });
    TESTGROUP2_USER1 =
        User.createUserForTesting(conf, "testgroup2_user2", new String[] { TESTGROUP_2 });
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(namespace).build());
    HTable table = null;
    try {
      table =
          TEST_UTIL.createTable(TableName.valueOf(tableName),
            new String[] { Bytes.toString(TEST_FAMILY), Bytes.toString(TEST_FAMILY_2) });

      // Ingesting test data.
      table.put(Arrays.asList(new Put(TEST_ROW).add(TEST_FAMILY, Q1, value1),
          new Put(TEST_ROW_2).add(TEST_FAMILY, Q2, value2),
          new Put(TEST_ROW_3).add(TEST_FAMILY_2, Q1, value1)));
    } finally {
      table.close();
    }

    assertEquals(1, AccessControlLists.getTablePermissions(conf, TableName.valueOf(tableName))
        .size());
    try {
      assertEquals(1, AccessControlClient.getUserPermissions(conf, tableName.toString()).size());
    } catch (Throwable e) {
      LOG.error("Error during call of AccessControlClient.getUserPermissions. ", e);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    // Clean the _acl_ table
    try {
      TEST_UTIL.deleteTable(tableName);
    } catch (TableNotFoundException ex) {
      // Test deleted the table, no problem
      LOG.info("Test deleted table " + tableName);
    }
    TEST_UTIL.getHBaseAdmin().deleteNamespace(namespace);
    // Verify all table/namespace permissions are erased
    assertEquals(0, AccessControlLists.getTablePermissions(conf, TableName.valueOf(tableName))
        .size());
    assertEquals(0, AccessControlLists.getNamespacePermissions(conf, namespace).size());
  }

  @Test
  public void testCreateWithCorrectOwner() throws Exception {
    // Create a test user
    User testUser =
        User.createUserForTesting(TEST_UTIL.getConfiguration(), "TestUser", new String[0]);
    // Grant the test user the ability to create tables
    SecureTestUtil.grantGlobal(TEST_UTIL, testUser.getShortName(), Action.CREATE);
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor desc = new HTableDescriptor(TEST_TABLE.getTableName());
        desc.addFamily(new HColumnDescriptor(TEST_FAMILY));
        HBaseAdmin admin = new HBaseAdmin(conf);
        try {
          admin.createTable(desc);
        } finally {
          admin.close();
        }
        return null;
      }
    }, testUser);
    TEST_UTIL.waitTableEnabled(TEST_TABLE.getTableName().getName());
    // Verify that owner permissions have been granted to the test user on the
    // table just created
    List<TablePermission> perms =
        AccessControlLists.getTablePermissions(conf, TEST_TABLE.getTableName()).get(
          testUser.getShortName());
    assertNotNull(perms);
    assertFalse(perms.isEmpty());
    // Should be RWXCA
    assertTrue(perms.get(0).implies(Permission.Action.READ));
    assertTrue(perms.get(0).implies(Permission.Action.WRITE));
    assertTrue(perms.get(0).implies(Permission.Action.EXEC));
    assertTrue(perms.get(0).implies(Permission.Action.CREATE));
    assertTrue(perms.get(0).implies(Permission.Action.ADMIN));
  }

  @Test
  public void testCreateTableWithGroupPermissions() throws Exception {
    grantGlobal(TEST_UTIL, convertToGroup(TESTGROUP_1), Action.CREATE);
    AccessTestAction createAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor desc = new HTableDescriptor(TEST_TABLE.getTableName());
        desc.addFamily(new HColumnDescriptor(TEST_FAMILY));
        try (Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
          try (Admin admin = connection.getAdmin()) {
            admin.createTable(desc);
          }
        }
        return null;
      }
    };
    verifyAllowed(createAction, TESTGROUP1_USER1);
    verifyDenied(createAction, TESTGROUP2_USER1);
    revokeGlobal(TEST_UTIL, convertToGroup(TESTGROUP_1), Action.CREATE);
  }

  @Test
  public void testACLTableAccess() throws Exception {
    final Configuration conf = TEST_UTIL.getConfiguration();

    // Superuser
    User superUser = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });

    // Global users
    User globalRead = User.createUserForTesting(conf, "globalRead", new String[0]);
    User globalWrite = User.createUserForTesting(conf, "globalWrite", new String[0]);
    User globalCreate = User.createUserForTesting(conf, "globalCreate", new String[0]);
    User globalAdmin = User.createUserForTesting(conf, "globalAdmin", new String[0]);
    SecureTestUtil.grantGlobal(TEST_UTIL, globalRead.getShortName(), Action.READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, globalWrite.getShortName(), Action.WRITE);
    SecureTestUtil.grantGlobal(TEST_UTIL, globalCreate.getShortName(), Action.CREATE);
    SecureTestUtil.grantGlobal(TEST_UTIL, globalAdmin.getShortName(), Action.ADMIN);

    // Namespace users
    User nsRead = User.createUserForTesting(conf, "nsRead", new String[0]);
    User nsWrite = User.createUserForTesting(conf, "nsWrite", new String[0]);
    User nsCreate = User.createUserForTesting(conf, "nsCreate", new String[0]);
    User nsAdmin = User.createUserForTesting(conf, "nsAdmin", new String[0]);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsRead.getShortName(),
      TEST_TABLE.getTableName().getNamespaceAsString(), Action.READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsWrite.getShortName(),
      TEST_TABLE.getTableName().getNamespaceAsString(), Action.WRITE);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsCreate.getShortName(),
      TEST_TABLE.getTableName().getNamespaceAsString(), Action.CREATE);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsAdmin.getShortName(),
      TEST_TABLE.getTableName().getNamespaceAsString(), Action.ADMIN);

    // Table users
    User tableRead = User.createUserForTesting(conf, "tableRead", new String[0]);
    User tableWrite = User.createUserForTesting(conf, "tableWrite", new String[0]);
    User tableCreate = User.createUserForTesting(conf, "tableCreate", new String[0]);
    User tableAdmin = User.createUserForTesting(conf, "tableAdmin", new String[0]);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableRead.getShortName(),
      TEST_TABLE.getTableName(), null, null, Action.READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableWrite.getShortName(),
      TEST_TABLE.getTableName(), null, null, Action.WRITE);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableCreate.getShortName(),
      TEST_TABLE.getTableName(), null, null, Action.CREATE);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableAdmin.getShortName(),
      TEST_TABLE.getTableName(), null, null, Action.ADMIN);

    // Write tests

    AccessTestAction writeAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTable t = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
        try {
          t.put(new Put(TEST_ROW).add(AccessControlLists.ACL_LIST_FAMILY, TEST_QUALIFIER,
            TEST_VALUE));
          return null;
        } finally {
          t.close();
        }
      }
    };

    // All writes to ACL table denied except for GLOBAL WRITE permission and superuser

    verifyDenied(writeAction, globalAdmin, globalCreate, globalRead);
    verifyDenied(writeAction, nsAdmin, nsCreate, nsRead, nsWrite);
    verifyDenied(writeAction, tableAdmin, tableCreate, tableRead, tableWrite);
    verifyAllowed(writeAction, superUser, globalWrite);

    // Read tests

    AccessTestAction scanAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTable t = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
        try {
          ResultScanner s = t.getScanner(new Scan());
          try {
            for (Result r = s.next(); r != null; r = s.next()) {
              // do nothing
            }
          } finally {
            s.close();
          }
          return null;
        } finally {
          t.close();
        }
      }
    };

    // All reads from ACL table denied except for GLOBAL READ and superuser

    verifyDenied(scanAction, globalAdmin, globalCreate, globalWrite);
    verifyDenied(scanAction, nsCreate, nsAdmin, nsRead, nsWrite);
    verifyDenied(scanAction, tableCreate, tableAdmin, tableRead, tableWrite);
    verifyAllowed(scanAction, superUser, globalRead);
  }

  /*
   * Test table scan operation at table, column family and column qualifier level.
   */
  @Test(timeout = 300000)
  public void testPostGrantAndRevokeScanAction() throws Exception {
    TableName name = TableName.valueOf(tableName);
    AccessTestAction scanTableActionForGroupWithTableLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan s1 = new Scan();
        ResultScanner scanner1 = table.getScanner(s1);
        try {
          Result[] next1 = scanner1.next(5);
          assertTrue("User having table level access should be able to scan all "
              + "the data in the table.", next1.length == 3);
        } finally {
          scanner1.close();
          table.close();
        }
        return null;
      }
    };

    AccessTestAction scanTableActionForGroupWithFamilyLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan s1 = new Scan();
        ResultScanner scanner1 = table.getScanner(s1);
        try {
          Result[] next1 = scanner1.next(5);
          assertTrue("User having column family level access should be able to scan all "
              + "the data belonging to that family.", next1.length == 2);
        } finally {
          scanner1.close();
          table.close();
        }
        return null;
      }
    };

    AccessTestAction scanFamilyActionForGroupWithFamilyLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan s1 = new Scan();
        s1.addFamily(TEST_FAMILY_2);
        ResultScanner scanner1 = null;
        try {
          scanner1 = table.getScanner(s1);
        } finally {
          if (scanner1 != null) {
            scanner1.close();
          }
          table.close();
        }
        return null;
      }
    };

    AccessTestAction scanTableActionForGroupWithQualifierLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan s1 = new Scan();
        ResultScanner scanner1 = table.getScanner(s1);
        try {
          Result[] next1 = scanner1.next(5);
          assertTrue("User having column qualifier level access should be able to scan "
              + "that column family qualifier data.", next1.length == 1);
        } finally {
          scanner1.close();
          table.close();
        }
        return null;
      }
    };

    AccessTestAction scanFamilyActionForGroupWithQualifierLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {

        HTable table = new HTable(conf, tableName);
        Scan s1 = new Scan();
        s1.addFamily(TEST_FAMILY_2);
        ResultScanner scanner1 = null;
        try {
          scanner1 = table.getScanner(s1);
        } finally {
          if (scanner1 != null) {
            scanner1.close();
          }
          table.close();
        }
        return null;
      }
    };

    AccessTestAction scanQualifierActionForGroupWithQualifierLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan s1 = new Scan();
        s1.addColumn(TEST_FAMILY, Q2);
        ResultScanner scanner1 = null;
        try {
          scanner1 = table.getScanner(s1);
        } finally {
          if (scanner1 != null) {
            scanner1.close();
          }
          table.close();
        }
        return null;
      }
    };

    // Verify user from a group which has table level access can read all the data and group which
    // has no access can't read any data.
    grantOnTable(TEST_UTIL, '@' + TESTGROUP_1, name, null, null, Permission.Action.READ);
    verifyAllowed(TESTGROUP1_USER1, scanTableActionForGroupWithTableLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanTableActionForGroupWithTableLevelAccess);

    // Verify user from a group whose table level access has been revoked can't read any data.
    revokeFromTable(TEST_UTIL, '@' + TESTGROUP_1, name, null, null);
    verifyDenied(TESTGROUP1_USER1, scanTableActionForGroupWithTableLevelAccess);

    // Verify user from a group which has column family level access can read all the data
    // belonging to that family and group which has no access can't read any data.
    grantOnTable(TEST_UTIL, '@' + TESTGROUP_1, name, TEST_FAMILY, null, Permission.Action.READ);
    verifyAllowed(TESTGROUP1_USER1, scanTableActionForGroupWithFamilyLevelAccess);
    verifyDenied(TESTGROUP1_USER1, scanFamilyActionForGroupWithFamilyLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanTableActionForGroupWithFamilyLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanFamilyActionForGroupWithFamilyLevelAccess);

    // Verify user from a group whose column family level access has been revoked can't read any
    // data from that family.
    revokeFromTable(TEST_UTIL, '@' + TESTGROUP_1, name, TEST_FAMILY, null);
    verifyDenied(TESTGROUP1_USER1, scanTableActionForGroupWithFamilyLevelAccess);

    // Verify user from a group which has column qualifier level access can read data that has this
    // family and qualifier, and group which has no access can't read any data.
    grantOnTable(TEST_UTIL, '@' + TESTGROUP_1, name, TEST_FAMILY, Q1, Permission.Action.READ);
    verifyAllowed(TESTGROUP1_USER1, scanTableActionForGroupWithQualifierLevelAccess);
    verifyDenied(TESTGROUP1_USER1, scanFamilyActionForGroupWithQualifierLevelAccess);
    verifyDenied(TESTGROUP1_USER1, scanQualifierActionForGroupWithQualifierLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanTableActionForGroupWithQualifierLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanFamilyActionForGroupWithQualifierLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanQualifierActionForGroupWithQualifierLevelAccess);

    // Verify user from a group whose column qualifier level access has been revoked can't read the
    // data having this column family and qualifier.
    revokeFromTable(TEST_UTIL, '@' + TESTGROUP_1, name, TEST_FAMILY, Q1);
    verifyDenied(TESTGROUP1_USER1, scanTableActionForGroupWithQualifierLevelAccess);
  }

  @Test
  public void testACLZNodeDeletion() throws Exception {
    String baseAclZNode = "/hbase/acl/";
    String ns = "testACLZNodeDeletionNamespace";
    NamespaceDescriptor desc = NamespaceDescriptor.create(ns).build();
    createNamespace(TEST_UTIL, desc);

    final TableName table = TableName.valueOf(ns, "testACLZNodeDeletionTable");
    final byte[] family = Bytes.toBytes("f1");
    HTableDescriptor htd = new HTableDescriptor(table);
    htd.addFamily(new HColumnDescriptor(family));
    TEST_UTIL.getHBaseAdmin().createTable(htd);

    // Namespace needs this, as they follow the lazy creation of ACL znode.
    grantOnNamespace(TEST_UTIL, TESTGROUP1_USER1.getShortName(), ns, Action.ADMIN);
    ZooKeeperWatcher zkw = TEST_UTIL.getMiniHBaseCluster().getMaster().getZooKeeper();
    assertTrue("The acl znode for table should exist",  ZKUtil.checkExists(zkw, baseAclZNode +
        table.getNameAsString()) != -1);
    assertTrue("The acl znode for namespace should exist", ZKUtil.checkExists(zkw, baseAclZNode +
        convertToNamespace(ns)) != -1);

    revokeFromNamespace(TEST_UTIL, TESTGROUP1_USER1.getShortName(), ns, Action.ADMIN);
    TEST_UTIL.deleteTable(table);
    deleteNamespace(TEST_UTIL, ns);

    assertTrue("The acl znode for table should have been deleted",
        ZKUtil.checkExists(zkw, baseAclZNode + table.getNameAsString()) == -1);
    assertTrue( "The acl znode for namespace should have been deleted",
        ZKUtil.checkExists(zkw, baseAclZNode + convertToNamespace(ns)) == -1);
  }
}
