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

import static org.apache.hadoop.hbase.AuthUtil.toGroupEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
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
public class TestAccessController2 extends SecureTestUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAccessController2.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAccessController2.class);

  private static final byte[] TEST_ROW = Bytes.toBytes("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("q");
  private static final byte[] TEST_VALUE = Bytes.toBytes("value");

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  /** The systemUserConnection created here is tied to the system user. In case, you are planning
   * to create AccessTestAction, DON'T use this systemUserConnection as the 'doAs' user
   * gets  eclipsed by the system user. */
  private static Connection systemUserConnection;

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
  public TableNameTestRule testTable = new TableNameTestRule();
  private String namespace = "testNamespace";
  private String tname = namespace + ":testtable1";
  private TableName tableName = TableName.valueOf(tname);
  private static String TESTGROUP_1_NAME;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Up the handlers; this test needs more than usual.
    conf.setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    // Enable security
    enableSecurity(conf);
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);
    TEST_UTIL.startMiniCluster();
    // Wait for the ACL table to become available
    TEST_UTIL.waitUntilAllRegionsAssigned(PermissionStorage.ACL_TABLE_NAME);

    TESTGROUP_1_NAME = toGroupEntry(TESTGROUP_1);
    TESTGROUP1_USER1 =
        User.createUserForTesting(conf, "testgroup1_user1", new String[] { TESTGROUP_1 });
    TESTGROUP2_USER1 =
        User.createUserForTesting(conf, "testgroup2_user2", new String[] { TESTGROUP_2 });

    systemUserConnection = ConnectionFactory.createConnection(conf);
  }

  @Before
  public void setUp() throws Exception {
    createNamespace(TEST_UTIL, NamespaceDescriptor.create(namespace).build());
    try (Table table = createTable(TEST_UTIL, tableName,
          new byte[][] { TEST_FAMILY, TEST_FAMILY_2 })) {
      TEST_UTIL.waitTableEnabled(tableName);

      // Ingesting test data.
      table.put(Arrays.asList(new Put(TEST_ROW).addColumn(TEST_FAMILY, Q1, value1),
          new Put(TEST_ROW_2).addColumn(TEST_FAMILY, Q2, value2),
          new Put(TEST_ROW_3).addColumn(TEST_FAMILY_2, Q1, value1)));
    }

    assertEquals(1, PermissionStorage.getTablePermissions(conf, tableName).size());
    try {
      assertEquals(1, AccessControlClient.getUserPermissions(systemUserConnection,
          tableName.toString()).size());
    } catch (Throwable e) {
      LOG.error("Error during call of AccessControlClient.getUserPermissions. ", e);
    }

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    systemUserConnection.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    // Clean the _acl_ table
    try {
      deleteTable(TEST_UTIL, tableName);
    } catch (TableNotFoundException ex) {
      // Test deleted the table, no problem
      LOG.info("Test deleted table " + tableName);
    }
    deleteNamespace(TEST_UTIL, namespace);
    // Verify all table/namespace permissions are erased
    assertEquals(0, PermissionStorage.getTablePermissions(conf, tableName).size());
    assertEquals(0, PermissionStorage.getNamespacePermissions(conf, namespace).size());
  }

  @Test
  public void testCreateWithCorrectOwner() throws Exception {
    // Create a test user
    final User testUser = User.createUserForTesting(TEST_UTIL.getConfiguration(), "TestUser",
      new String[0]);
    // Grant the test user the ability to create tables
    SecureTestUtil.grantGlobal(TEST_UTIL, testUser.getShortName(), Action.CREATE);
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor desc = new HTableDescriptor(testTable.getTableName());
        desc.addFamily(new HColumnDescriptor(TEST_FAMILY));
        try (Connection connection =
            ConnectionFactory.createConnection(TEST_UTIL.getConfiguration(), testUser)) {
          try (Admin admin = connection.getAdmin()) {
            createTable(TEST_UTIL, admin, desc);
          }
        }
        return null;
      }
    }, testUser);
    TEST_UTIL.waitTableAvailable(testTable.getTableName());
    // Verify that owner permissions have been granted to the test user on the
    // table just created
    List<UserPermission> perms = PermissionStorage
        .getTablePermissions(conf, testTable.getTableName()).get(testUser.getShortName());
    assertNotNull(perms);
    assertFalse(perms.isEmpty());
    // Should be RWXCA
    assertTrue(perms.get(0).getPermission().implies(Permission.Action.READ));
    assertTrue(perms.get(0).getPermission().implies(Permission.Action.WRITE));
    assertTrue(perms.get(0).getPermission().implies(Permission.Action.EXEC));
    assertTrue(perms.get(0).getPermission().implies(Permission.Action.CREATE));
    assertTrue(perms.get(0).getPermission().implies(Permission.Action.ADMIN));
  }

  @Test
  public void testCreateTableWithGroupPermissions() throws Exception {
    grantGlobal(TEST_UTIL, TESTGROUP_1_NAME, Action.CREATE);
    try {
      AccessTestAction createAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          HTableDescriptor desc = new HTableDescriptor(testTable.getTableName());
          desc.addFamily(new HColumnDescriptor(TEST_FAMILY));
          try (Connection connection =
              ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
            try (Admin admin = connection.getAdmin()) {
              admin.createTable(desc);
            }
          }
          return null;
        }
      };
      verifyAllowed(createAction, TESTGROUP1_USER1);
      verifyDenied(createAction, TESTGROUP2_USER1);
    } finally {
      revokeGlobal(TEST_UTIL, TESTGROUP_1_NAME, Action.CREATE);
    }
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
      testTable.getTableName().getNamespaceAsString(), Action.READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsWrite.getShortName(),
      testTable.getTableName().getNamespaceAsString(), Action.WRITE);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsCreate.getShortName(),
      testTable.getTableName().getNamespaceAsString(), Action.CREATE);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsAdmin.getShortName(),
      testTable.getTableName().getNamespaceAsString(), Action.ADMIN);

    // Table users
    User tableRead = User.createUserForTesting(conf, "tableRead", new String[0]);
    User tableWrite = User.createUserForTesting(conf, "tableWrite", new String[0]);
    User tableCreate = User.createUserForTesting(conf, "tableCreate", new String[0]);
    User tableAdmin = User.createUserForTesting(conf, "tableAdmin", new String[0]);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableRead.getShortName(),
      testTable.getTableName(), null, null, Action.READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableWrite.getShortName(),
      testTable.getTableName(), null, null, Action.WRITE);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableCreate.getShortName(),
      testTable.getTableName(), null, null, Action.CREATE);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableAdmin.getShortName(),
      testTable.getTableName(), null, null, Action.ADMIN);

    grantGlobal(TEST_UTIL, TESTGROUP_1_NAME, Action.WRITE);
    try {
      // Write tests

      AccessTestAction writeAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {

          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(PermissionStorage.ACL_TABLE_NAME)) {
            t.put(new Put(TEST_ROW).addColumn(PermissionStorage.ACL_LIST_FAMILY, TEST_QUALIFIER,
              TEST_VALUE));
            return null;
          } finally {
          }
        }
      };

      // All writes to ACL table denied except for GLOBAL WRITE permission and superuser

      verifyDenied(writeAction, globalAdmin, globalCreate, globalRead, TESTGROUP2_USER1);
      verifyDenied(writeAction, nsAdmin, nsCreate, nsRead, nsWrite);
      verifyDenied(writeAction, tableAdmin, tableCreate, tableRead, tableWrite);
      verifyAllowed(writeAction, superUser, globalWrite, TESTGROUP1_USER1);
    } finally {
      revokeGlobal(TEST_UTIL, TESTGROUP_1_NAME, Action.WRITE);
    }

    grantGlobal(TEST_UTIL, TESTGROUP_1_NAME, Action.READ);
    try {
      // Read tests

      AccessTestAction scanAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(PermissionStorage.ACL_TABLE_NAME)) {
            ResultScanner s = t.getScanner(new Scan());
            try {
              for (Result r = s.next(); r != null; r = s.next()) {
                // do nothing
              }
            } finally {
              s.close();
            }
            return null;
          }
        }
      };

      // All reads from ACL table denied except for GLOBAL READ and superuser

      verifyDenied(scanAction, globalAdmin, globalCreate, globalWrite, TESTGROUP2_USER1);
      verifyDenied(scanAction, nsCreate, nsAdmin, nsRead, nsWrite);
      verifyDenied(scanAction, tableCreate, tableAdmin, tableRead, tableWrite);
      verifyAllowed(scanAction, superUser, globalRead, TESTGROUP1_USER1);
    } finally {
      revokeGlobal(TEST_UTIL, TESTGROUP_1_NAME, Action.READ);
    }
  }

  /*
   * Test table scan operation at table, column family and column qualifier level.
   */
  @Test
  public void testPostGrantAndRevokeScanAction() throws Exception {
    AccessTestAction scanTableActionForGroupWithTableLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName);) {
          Scan s1 = new Scan();
          try (ResultScanner scanner1 = table.getScanner(s1);) {
            Result[] next1 = scanner1.next(5);
            assertTrue("User having table level access should be able to scan all "
                + "the data in the table.", next1.length == 3);
          }
        }
        return null;
      }
    };

    AccessTestAction scanTableActionForGroupWithFamilyLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName);) {
          Scan s1 = new Scan();
          try (ResultScanner scanner1 = table.getScanner(s1);) {
            Result[] next1 = scanner1.next(5);
            assertTrue("User having column family level access should be able to scan all "
                + "the data belonging to that family.", next1.length == 2);
          }
        }
        return null;
      }
    };

    AccessTestAction scanFamilyActionForGroupWithFamilyLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName);) {
          Scan s1 = new Scan();
          s1.addFamily(TEST_FAMILY_2);
          try (ResultScanner scanner1 = table.getScanner(s1);) {
            scanner1.next();
          }
        }
        return null;
      }
    };

    AccessTestAction scanTableActionForGroupWithQualifierLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName);) {
          Scan s1 = new Scan();
          try (ResultScanner scanner1 = table.getScanner(s1);) {
            Result[] next1 = scanner1.next(5);
            assertTrue("User having column qualifier level access should be able to scan "
                + "that column family qualifier data.", next1.length == 1);
          }
        }
        return null;
      }
    };

    AccessTestAction scanFamilyActionForGroupWithQualifierLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName);) {
          Scan s1 = new Scan();
          s1.addFamily(TEST_FAMILY_2);
          try (ResultScanner scanner1 = table.getScanner(s1);) {
            scanner1.next();
          }
        }
        return null;
      }
    };

    AccessTestAction scanQualifierActionForGroupWithQualifierLevelAccess = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(tableName);) {
          Scan s1 = new Scan();
          s1.addColumn(TEST_FAMILY, Q2);
          try (ResultScanner scanner1 = table.getScanner(s1);) {
            scanner1.next();
          }
        }
        return null;
      }
    };

    // Verify user from a group which has table level access can read all the data and group which
    // has no access can't read any data.
    grantOnTable(TEST_UTIL, TESTGROUP_1_NAME, tableName, null, null, Action.READ);
    verifyAllowed(TESTGROUP1_USER1, scanTableActionForGroupWithTableLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanTableActionForGroupWithTableLevelAccess);

    // Verify user from a group whose table level access has been revoked can't read any data.
    revokeFromTable(TEST_UTIL, TESTGROUP_1_NAME, tableName, null, null);
    verifyDenied(TESTGROUP1_USER1, scanTableActionForGroupWithTableLevelAccess);

    // Verify user from a group which has column family level access can read all the data
    // belonging to that family and group which has no access can't read any data.
    grantOnTable(TEST_UTIL, TESTGROUP_1_NAME, tableName, TEST_FAMILY, null,
      Permission.Action.READ);
    verifyAllowed(TESTGROUP1_USER1, scanTableActionForGroupWithFamilyLevelAccess);
    verifyDenied(TESTGROUP1_USER1, scanFamilyActionForGroupWithFamilyLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanTableActionForGroupWithFamilyLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanFamilyActionForGroupWithFamilyLevelAccess);

    // Verify user from a group whose column family level access has been revoked can't read any
    // data from that family.
    revokeFromTable(TEST_UTIL, TESTGROUP_1_NAME, tableName, TEST_FAMILY, null);
    verifyDenied(TESTGROUP1_USER1, scanTableActionForGroupWithFamilyLevelAccess);

    // Verify user from a group which has column qualifier level access can read data that has this
    // family and qualifier, and group which has no access can't read any data.
    grantOnTable(TEST_UTIL, TESTGROUP_1_NAME, tableName, TEST_FAMILY, Q1, Action.READ);
    verifyAllowed(TESTGROUP1_USER1, scanTableActionForGroupWithQualifierLevelAccess);
    verifyDenied(TESTGROUP1_USER1, scanFamilyActionForGroupWithQualifierLevelAccess);
    verifyDenied(TESTGROUP1_USER1, scanQualifierActionForGroupWithQualifierLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanTableActionForGroupWithQualifierLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanFamilyActionForGroupWithQualifierLevelAccess);
    verifyDenied(TESTGROUP2_USER1, scanQualifierActionForGroupWithQualifierLevelAccess);

    // Verify user from a group whose column qualifier level access has been revoked can't read the
    // data having this column family and qualifier.
    revokeFromTable(TEST_UTIL, TESTGROUP_1_NAME, tableName, TEST_FAMILY, Q1);
    verifyDenied(TESTGROUP1_USER1, scanTableActionForGroupWithQualifierLevelAccess);
  }

  public static class MyAccessController extends AccessController {
  }

  @Test
  public void testCoprocessorLoading() throws Exception {
    MasterCoprocessorHost cpHost =
        TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterCoprocessorHost();
    cpHost.load(MyAccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    AccessController ACCESS_CONTROLLER = cpHost.findCoprocessor(MyAccessController.class);
    MasterCoprocessorEnvironment CP_ENV = cpHost.createEnvironment(
      ACCESS_CONTROLLER, Coprocessor.PRIORITY_HIGHEST, 1, conf);
    RegionServerCoprocessorHost rsHost = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getRegionServerCoprocessorHost();
    RegionServerCoprocessorEnvironment RSCP_ENV = rsHost.createEnvironment(
      ACCESS_CONTROLLER, Coprocessor.PRIORITY_HIGHEST, 1, conf);
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
    createTable(TEST_UTIL, htd);

    // Namespace needs this, as they follow the lazy creation of ACL znode.
    grantOnNamespace(TEST_UTIL, TESTGROUP1_USER1.getShortName(), ns, Action.ADMIN);
    ZKWatcher zkw = TEST_UTIL.getMiniHBaseCluster().getMaster().getZooKeeper();
    assertTrue("The acl znode for table should exist",  ZKUtil.checkExists(zkw, baseAclZNode +
        table.getNameAsString()) != -1);
    assertTrue("The acl znode for namespace should exist", ZKUtil.checkExists(zkw, baseAclZNode +
        convertToNamespace(ns)) != -1);

    revokeFromNamespace(TEST_UTIL, TESTGROUP1_USER1.getShortName(), ns, Action.ADMIN);
    deleteTable(TEST_UTIL, table);
    deleteNamespace(TEST_UTIL, ns);

    assertTrue("The acl znode for table should have been deleted",
        ZKUtil.checkExists(zkw, baseAclZNode + table.getNameAsString()) == -1);
    assertTrue( "The acl znode for namespace should have been deleted",
        ZKUtil.checkExists(zkw, baseAclZNode + convertToNamespace(ns)) == -1);
  }
}
