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

import static org.apache.hadoop.hbase.AuthUtil.toGroupEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs authorization checks for rsgroup operations, according to different
 * levels of authorized users.
 */
@Category({SecurityTests.class, MediumTests.class})
public class TestRSGroupsWithACL extends SecureTestUtil{

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRSGroupsWithACL.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsWithACL.class);
  private static TableName TEST_TABLE = TableName.valueOf("testtable1");
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  private static Connection systemUserConnection;
  // user with all permissions
  private static User SUPERUSER;
  // user granted with all global permission
  private static User USER_ADMIN;
  // user with rw permissions on column family.
  private static User USER_RW;
  // user with read-only permissions
  private static User USER_RO;
  // user is table owner. will have all permissions on table
  private static User USER_OWNER;
  // user with create table permissions alone
  private static User USER_CREATE;
  // user with no permissions
  private static User USER_NONE;

  private static final String GROUP_ADMIN = "group_admin";
  private static final String GROUP_CREATE = "group_create";
  private static final String GROUP_READ = "group_read";
  private static final String GROUP_WRITE = "group_write";

  private static User USER_GROUP_ADMIN;
  private static User USER_GROUP_CREATE;
  private static User USER_GROUP_READ;
  private static User USER_GROUP_WRITE;

  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static RSGroupAdminEndpoint rsGroupAdminEndpoint;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        RSGroupBasedLoadBalancer.class.getName());
    // Enable security
    enableSecurity(conf);
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);
    // Enable rsgroup
    configureRSGroupAdminEndpoint(conf);

    TEST_UTIL.startMiniCluster();

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    TEST_UTIL.waitFor(60000, (Predicate<Exception>) () ->
        master.isInitialized() && ((RSGroupBasedLoadBalancer) master.getLoadBalancer()).isOnline());

    rsGroupAdminEndpoint = (RSGroupAdminEndpoint) TEST_UTIL.getMiniHBaseCluster().getMaster().
        getMasterCoprocessorHost().findCoprocessor(RSGroupAdminEndpoint.class.getName());
    // Wait for the ACL table to become available
    TEST_UTIL.waitUntilAllRegionsAssigned(PermissionStorage.ACL_TABLE_NAME);

    // create a set of test users
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER_ADMIN = User.createUserForTesting(conf, "admin2", new String[0]);
    USER_RW = User.createUserForTesting(conf, "rwuser", new String[0]);
    USER_RO = User.createUserForTesting(conf, "rouser", new String[0]);
    USER_OWNER = User.createUserForTesting(conf, "owner", new String[0]);
    USER_CREATE = User.createUserForTesting(conf, "tbl_create", new String[0]);
    USER_NONE = User.createUserForTesting(conf, "nouser", new String[0]);

    USER_GROUP_ADMIN =
        User.createUserForTesting(conf, "user_group_admin", new String[] { GROUP_ADMIN });
    USER_GROUP_CREATE =
        User.createUserForTesting(conf, "user_group_create", new String[] { GROUP_CREATE });
    USER_GROUP_READ =
        User.createUserForTesting(conf, "user_group_read", new String[] { GROUP_READ });
    USER_GROUP_WRITE =
        User.createUserForTesting(conf, "user_group_write", new String[] { GROUP_WRITE });

    systemUserConnection = TEST_UTIL.getConnection();
    setUpTableAndUserPermissions();
  }

  private static void setUpTableAndUserPermissions() throws Exception {
    TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(TEST_TABLE);
    ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(TEST_FAMILY);
    cfd.setMaxVersions(100);
    tableBuilder.setColumnFamily(cfd.build());
    tableBuilder.setValue(TableDescriptorBuilder.OWNER, USER_OWNER.getShortName());
    createTable(TEST_UTIL, tableBuilder.build(),
        new byte[][] { Bytes.toBytes("s") });

    // Set up initial grants
    grantGlobal(TEST_UTIL, USER_ADMIN.getShortName(),
        Permission.Action.ADMIN,
        Permission.Action.CREATE,
        Permission.Action.READ,
        Permission.Action.WRITE);

    grantOnTable(TEST_UTIL, USER_RW.getShortName(),
        TEST_TABLE, TEST_FAMILY, null,
        Permission.Action.READ,
        Permission.Action.WRITE);

    // USER_CREATE is USER_RW plus CREATE permissions
    grantOnTable(TEST_UTIL, USER_CREATE.getShortName(),
        TEST_TABLE, null, null,
        Permission.Action.CREATE,
        Permission.Action.READ,
        Permission.Action.WRITE);

    grantOnTable(TEST_UTIL, USER_RO.getShortName(),
        TEST_TABLE, TEST_FAMILY, null,
        Permission.Action.READ);

    grantGlobal(TEST_UTIL, toGroupEntry(GROUP_ADMIN), Permission.Action.ADMIN);
    grantGlobal(TEST_UTIL, toGroupEntry(GROUP_CREATE), Permission.Action.CREATE);
    grantGlobal(TEST_UTIL, toGroupEntry(GROUP_READ), Permission.Action.READ);
    grantGlobal(TEST_UTIL, toGroupEntry(GROUP_WRITE), Permission.Action.WRITE);

    assertEquals(4, PermissionStorage.getTablePermissions(conf, TEST_TABLE).size());
    try {
      assertEquals(4, AccessControlClient.getUserPermissions(systemUserConnection,
          TEST_TABLE.toString()).size());
    } catch (AssertionError e) {
      fail(e.getMessage());
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.getUserPermissions. ", e);
    }
  }

  private static void cleanUp() throws Exception {
    // Clean the _acl_ table
    try {
      deleteTable(TEST_UTIL, TEST_TABLE);
    } catch (TableNotFoundException ex) {
      // Test deleted the table, no problem
      LOG.info("Test deleted table " + TEST_TABLE);
    }
    // Verify all table/namespace permissions are erased
    assertEquals(0, PermissionStorage.getTablePermissions(conf, TEST_TABLE).size());
    assertEquals(0,
      PermissionStorage.getNamespacePermissions(conf, TEST_TABLE.getNamespaceAsString()).size());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cleanUp();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void configureRSGroupAdminEndpoint(Configuration conf) {
    String currentCoprocessors = conf.get(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
    String coprocessors = RSGroupAdminEndpoint.class.getName();
    if (currentCoprocessors != null) {
      coprocessors += "," + currentCoprocessors;
    }
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, coprocessors);
    conf.set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        RSGroupBasedLoadBalancer.class.getName());
  }

  @Test
  public void testGetRSGroupInfo() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("getRSGroupInfo");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testGetRSGroupInfoOfTable() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("getRSGroupInfoOfTable");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testMoveServers() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("moveServers");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testMoveTables() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("moveTables");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testAddRSGroup() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("addRSGroup");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testRemoveRSGroup() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("removeRSGroup");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testBalanceRSGroup() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("balanceRSGroup");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testListRSGroup() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("listRSGroup");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testGetRSGroupInfoOfServer() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("getRSGroupInfoOfServer");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testMoveServersAndTables() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("moveServersAndTables");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testRenameRSGroup() throws Exception {
    AccessTestAction action = () -> {
      rsGroupAdminEndpoint.checkPermission("renameRSGroup");
      return null;
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
      USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }
}
