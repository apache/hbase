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
import static org.junit.Assert.assertTrue;

import com.google.protobuf.BlockingRpcChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContextImpl;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

@Category({SecurityTests.class, MediumTests.class})
public class TestNamespaceCommands extends SecureTestUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNamespaceCommands.class);

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Logger LOG = LoggerFactory.getLogger(TestNamespaceCommands.class);
  private static String TEST_NAMESPACE = "ns1";
  private static String TEST_NAMESPACE2 = "ns2";
  private static Configuration conf;
  private static MasterCoprocessorEnvironment CP_ENV;
  private static AccessController ACCESS_CONTROLLER;

  // user with all permissions
  private static User SUPERUSER;

  // user with A permission on global
  private static User USER_GLOBAL_ADMIN;
  // user with C permission on global
  private static User USER_GLOBAL_CREATE;
  // user with W permission on global
  private static User USER_GLOBAL_WRITE;
  // user with R permission on global
  private static User USER_GLOBAL_READ;
  // user with X permission on global
  private static User USER_GLOBAL_EXEC;

  // user with A permission on namespace
  private static User USER_NS_ADMIN;
  // user with C permission on namespace
  private static User USER_NS_CREATE;
  // user with W permission on namespace
  private static User USER_NS_WRITE;
  // user with R permission on namespace.
  private static User USER_NS_READ;
  // user with X permission on namespace.
  private static User USER_NS_EXEC;

  // user with rw permissions
  private static User USER_TABLE_WRITE;  // TODO: WE DO NOT GIVE ANY PERMS TO THIS USER
  //user with create table permissions alone
  private static User USER_TABLE_CREATE; // TODO: WE DO NOT GIVE ANY PERMS TO THIS USER

  private static final String GROUP_ADMIN = "group_admin";
  private static final String GROUP_NS_ADMIN = "group_ns_admin";
  private static final String GROUP_CREATE = "group_create";
  private static final String GROUP_READ = "group_read";
  private static final String GROUP_WRITE = "group_write";

  private static User USER_GROUP_ADMIN;
  private static User USER_GROUP_NS_ADMIN;
  private static User USER_GROUP_CREATE;
  private static User USER_GROUP_READ;
  private static User USER_GROUP_WRITE;

  private static String TEST_TABLE = TEST_NAMESPACE + ":testtable";
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = UTIL.getConfiguration();
    enableSecurity(conf);

    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    // Users with global permissions
    USER_GLOBAL_ADMIN = User.createUserForTesting(conf, "global_admin", new String[0]);
    USER_GLOBAL_CREATE = User.createUserForTesting(conf, "global_create", new String[0]);
    USER_GLOBAL_WRITE = User.createUserForTesting(conf, "global_write", new String[0]);
    USER_GLOBAL_READ = User.createUserForTesting(conf, "global_read", new String[0]);
    USER_GLOBAL_EXEC = User.createUserForTesting(conf, "global_exec", new String[0]);

    USER_NS_ADMIN = User.createUserForTesting(conf, "namespace_admin", new String[0]);
    USER_NS_CREATE = User.createUserForTesting(conf, "namespace_create", new String[0]);
    USER_NS_WRITE = User.createUserForTesting(conf, "namespace_write", new String[0]);
    USER_NS_READ = User.createUserForTesting(conf, "namespace_read", new String[0]);
    USER_NS_EXEC = User.createUserForTesting(conf, "namespace_exec", new String[0]);

    USER_TABLE_CREATE = User.createUserForTesting(conf, "table_create", new String[0]);
    USER_TABLE_WRITE = User.createUserForTesting(conf, "table_write", new String[0]);

    USER_GROUP_ADMIN =
        User.createUserForTesting(conf, "user_group_admin", new String[] { GROUP_ADMIN });
    USER_GROUP_NS_ADMIN =
        User.createUserForTesting(conf, "user_group_ns_admin", new String[] { GROUP_NS_ADMIN });
    USER_GROUP_CREATE =
        User.createUserForTesting(conf, "user_group_create", new String[] { GROUP_CREATE });
    USER_GROUP_READ =
        User.createUserForTesting(conf, "user_group_read", new String[] { GROUP_READ });
    USER_GROUP_WRITE =
        User.createUserForTesting(conf, "user_group_write", new String[] { GROUP_WRITE });
    // TODO: other table perms

    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    UTIL.startMiniCluster();
    // Wait for the ACL table to become available
    UTIL.waitTableAvailable(AccessControlLists.ACL_TABLE_NAME.getName(), 30 * 1000);

    // Find the Access Controller CP. Could be on master or if master is not serving regions, is
    // on an arbitrary server.
    for (JVMClusterUtil.RegionServerThread rst:
        UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()) {
      ACCESS_CONTROLLER = rst.getRegionServer().getRegionServerCoprocessorHost().
        findCoprocessor(AccessController.class);
      if (ACCESS_CONTROLLER != null) break;
    }
    if (ACCESS_CONTROLLER == null) throw new NullPointerException();

    UTIL.getAdmin().createNamespace(NamespaceDescriptor.create(TEST_NAMESPACE).build());
    UTIL.getAdmin().createNamespace(NamespaceDescriptor.create(TEST_NAMESPACE2).build());

    // grants on global
    grantGlobal(UTIL, USER_GLOBAL_ADMIN.getShortName(),  Permission.Action.ADMIN);
    grantGlobal(UTIL, USER_GLOBAL_CREATE.getShortName(), Permission.Action.CREATE);
    grantGlobal(UTIL, USER_GLOBAL_WRITE.getShortName(),  Permission.Action.WRITE);
    grantGlobal(UTIL, USER_GLOBAL_READ.getShortName(),   Permission.Action.READ);
    grantGlobal(UTIL, USER_GLOBAL_EXEC.getShortName(),   Permission.Action.EXEC);

    // grants on namespace
    grantOnNamespace(UTIL, USER_NS_ADMIN.getShortName(),  TEST_NAMESPACE, Permission.Action.ADMIN);
    grantOnNamespace(UTIL, USER_NS_CREATE.getShortName(), TEST_NAMESPACE, Permission.Action.CREATE);
    grantOnNamespace(UTIL, USER_NS_WRITE.getShortName(),  TEST_NAMESPACE, Permission.Action.WRITE);
    grantOnNamespace(UTIL, USER_NS_READ.getShortName(),   TEST_NAMESPACE, Permission.Action.READ);
    grantOnNamespace(UTIL, USER_NS_EXEC.getShortName(),   TEST_NAMESPACE, Permission.Action.EXEC);
    grantOnNamespace(UTIL, toGroupEntry(GROUP_NS_ADMIN), TEST_NAMESPACE, Permission.Action.ADMIN);

    grantOnNamespace(UTIL, USER_NS_ADMIN.getShortName(), TEST_NAMESPACE2, Permission.Action.ADMIN);

    grantGlobal(UTIL, toGroupEntry(GROUP_ADMIN), Permission.Action.ADMIN);
    grantGlobal(UTIL, toGroupEntry(GROUP_CREATE), Permission.Action.CREATE);
    grantGlobal(UTIL, toGroupEntry(GROUP_READ), Permission.Action.READ);
    grantGlobal(UTIL, toGroupEntry(GROUP_WRITE), Permission.Action.WRITE);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.getAdmin().deleteNamespace(TEST_NAMESPACE);
    UTIL.getAdmin().deleteNamespace(TEST_NAMESPACE2);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAclTableEntries() throws Exception {
    String userTestNamespace = "userTestNsp";
    Table acl = UTIL.getConnection().getTable(AccessControlLists.ACL_TABLE_NAME);
    try {
      ListMultimap<String, TablePermission> perms =
          AccessControlLists.getNamespacePermissions(conf, TEST_NAMESPACE);

      perms = AccessControlLists.getNamespacePermissions(conf, TEST_NAMESPACE);
      for (Map.Entry<String, TablePermission> entry : perms.entries()) {
        LOG.debug(Objects.toString(entry));
      }
      assertEquals(6, perms.size());

      // Grant and check state in ACL table
      grantOnNamespace(UTIL, userTestNamespace, TEST_NAMESPACE,
        Permission.Action.WRITE);

      Result result = acl.get(new Get(Bytes.toBytes(userTestNamespace)));
      assertTrue(result != null);
      perms = AccessControlLists.getNamespacePermissions(conf, TEST_NAMESPACE);
      assertEquals(7, perms.size());
      List<TablePermission> namespacePerms = perms.get(userTestNamespace);
      assertTrue(perms.containsKey(userTestNamespace));
      assertEquals(1, namespacePerms.size());
      assertEquals(TEST_NAMESPACE,
        namespacePerms.get(0).getNamespace());
      assertEquals(null, namespacePerms.get(0).getFamily());
      assertEquals(null, namespacePerms.get(0).getQualifier());
      assertEquals(1, namespacePerms.get(0).getActions().length);
      assertEquals(Permission.Action.WRITE, namespacePerms.get(0).getActions()[0]);

      // Revoke and check state in ACL table
      revokeFromNamespace(UTIL, userTestNamespace, TEST_NAMESPACE,
        Permission.Action.WRITE);

      perms = AccessControlLists.getNamespacePermissions(conf, TEST_NAMESPACE);
      assertEquals(6, perms.size());
    } finally {
      acl.close();
    }
  }

  @Test
  public void testModifyNamespace() throws Exception {
    AccessTestAction modifyNamespace = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preModifyNamespace(ObserverContextImpl.createAndPrepare(CP_ENV),
          null, // not needed by AccessController
          NamespaceDescriptor.create(TEST_NAMESPACE).addConfiguration("abc", "156").build());
        return null;
      }
    };

    // modifyNamespace: superuser | global(A) | NS(A)
    verifyAllowed(modifyNamespace, SUPERUSER, USER_GLOBAL_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(modifyNamespace, USER_GLOBAL_CREATE, USER_GLOBAL_WRITE, USER_GLOBAL_READ,
      USER_GLOBAL_EXEC, USER_NS_ADMIN, USER_NS_CREATE, USER_NS_WRITE, USER_NS_READ, USER_NS_EXEC,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testCreateAndDeleteNamespace() throws Exception {
    AccessTestAction createNamespace = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCreateNamespace(ObserverContextImpl.createAndPrepare(CP_ENV),
          NamespaceDescriptor.create(TEST_NAMESPACE2).build());
        return null;
      }
    };

    AccessTestAction deleteNamespace = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteNamespace(ObserverContextImpl.createAndPrepare(CP_ENV),
          TEST_NAMESPACE2);
        return null;
      }
    };

    // createNamespace: superuser | global(A)
    verifyAllowed(createNamespace, SUPERUSER, USER_GLOBAL_ADMIN, USER_GROUP_ADMIN);
    // all others should be denied
    verifyDenied(createNamespace, USER_GLOBAL_CREATE, USER_GLOBAL_WRITE, USER_GLOBAL_READ,
      USER_GLOBAL_EXEC, USER_NS_ADMIN, USER_NS_CREATE, USER_NS_WRITE, USER_NS_READ, USER_NS_EXEC,
      USER_TABLE_CREATE, USER_TABLE_WRITE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    // deleteNamespace: superuser | global(A) | NS(A)
    verifyAllowed(deleteNamespace, SUPERUSER, USER_GLOBAL_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(deleteNamespace, USER_GLOBAL_CREATE, USER_GLOBAL_WRITE, USER_GLOBAL_READ,
      USER_GLOBAL_EXEC, USER_NS_ADMIN, USER_NS_CREATE, USER_NS_WRITE, USER_NS_READ, USER_NS_EXEC,
      USER_TABLE_CREATE, USER_TABLE_WRITE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testGetNamespaceDescriptor() throws Exception {
    AccessTestAction getNamespaceAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preGetNamespaceDescriptor(ObserverContextImpl.createAndPrepare(CP_ENV),
          TEST_NAMESPACE);
        return null;
      }
    };
    // getNamespaceDescriptor : superuser | global(A) | NS(A)
    verifyAllowed(getNamespaceAction, SUPERUSER, USER_GLOBAL_ADMIN, USER_NS_ADMIN,
      USER_GROUP_ADMIN);
    verifyDenied(getNamespaceAction, USER_GLOBAL_CREATE, USER_GLOBAL_WRITE, USER_GLOBAL_READ,
      USER_GLOBAL_EXEC, USER_NS_CREATE, USER_NS_WRITE, USER_NS_READ, USER_NS_EXEC,
      USER_TABLE_CREATE, USER_TABLE_WRITE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testListNamespaces() throws Exception {
    AccessTestAction listAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Connection unmanagedConnection =
            ConnectionFactory.createConnection(UTIL.getConfiguration());
        Admin admin = unmanagedConnection.getAdmin();
        try {
          return Arrays.asList(admin.listNamespaceDescriptors());
        } finally {
          admin.close();
          unmanagedConnection.close();
        }
      }
    };

    // listNamespaces         : All access*
    // * Returned list will only show what you can call getNamespaceDescriptor()

    verifyAllowed(listAction, SUPERUSER, USER_GLOBAL_ADMIN, USER_NS_ADMIN, USER_GROUP_ADMIN);

    // we have 3 namespaces: [default, hbase, TEST_NAMESPACE, TEST_NAMESPACE2]
    assertEquals(4, ((List)SUPERUSER.runAs(listAction)).size());
    assertEquals(4, ((List)USER_GLOBAL_ADMIN.runAs(listAction)).size());
    assertEquals(4, ((List)USER_GROUP_ADMIN.runAs(listAction)).size());

    assertEquals(2, ((List)USER_NS_ADMIN.runAs(listAction)).size());

    assertEquals(0, ((List)USER_GLOBAL_CREATE.runAs(listAction)).size());
    assertEquals(0, ((List)USER_GLOBAL_WRITE.runAs(listAction)).size());
    assertEquals(0, ((List)USER_GLOBAL_READ.runAs(listAction)).size());
    assertEquals(0, ((List)USER_GLOBAL_EXEC.runAs(listAction)).size());
    assertEquals(0, ((List)USER_NS_CREATE.runAs(listAction)).size());
    assertEquals(0, ((List)USER_NS_WRITE.runAs(listAction)).size());
    assertEquals(0, ((List)USER_NS_READ.runAs(listAction)).size());
    assertEquals(0, ((List)USER_NS_EXEC.runAs(listAction)).size());
    assertEquals(0, ((List)USER_TABLE_CREATE.runAs(listAction)).size());
    assertEquals(0, ((List)USER_TABLE_WRITE.runAs(listAction)).size());
    assertEquals(0, ((List)USER_GROUP_CREATE.runAs(listAction)).size());
    assertEquals(0, ((List)USER_GROUP_READ.runAs(listAction)).size());
    assertEquals(0, ((List)USER_GROUP_WRITE.runAs(listAction)).size());
  }

  @Test
  public void testGrantRevoke() throws Exception{
    final String testUser = "testUser";

    // Test if client API actions are authorized

    AccessTestAction grantAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table acl = connection.getTable(AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service =
              acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          AccessControlUtil.grant(null, protocol, testUser, TEST_NAMESPACE, false, Action.WRITE);
        } finally {
          acl.close();
          connection.close();
        }
        return null;
      }
    };

    AccessTestAction grantNamespaceAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table acl = conn.getTable(AccessControlLists.ACL_TABLE_NAME)) {
          BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          AccessControlUtil.grant(null, protocol, USER_GROUP_NS_ADMIN.getShortName(),
            TEST_NAMESPACE, false, Action.READ);
        }
        return null;
      }
    };

    AccessTestAction revokeAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table acl = connection.getTable(AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service =
              acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          AccessControlUtil.revoke(null, protocol, testUser, TEST_NAMESPACE, Action.WRITE);
        } finally {
          acl.close();
          connection.close();
        }
        return null;
      }
    };

    AccessTestAction revokeNamespaceAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table acl = connection.getTable(AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service =
              acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          AccessControlUtil.revoke(null, protocol, USER_GROUP_NS_ADMIN.getShortName(),
            TEST_NAMESPACE, Action.READ);
        } finally {
          acl.close();
          connection.close();
        }
        return null;
      }
    };

    AccessTestAction getPermissionsAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table acl = connection.getTable(AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          AccessControlUtil.getUserPermissions(null, protocol, Bytes.toBytes(TEST_NAMESPACE));
        } finally {
          acl.close();
          connection.close();
        }
        return null;
      }
    };

    verifyAllowed(grantAction, SUPERUSER, USER_GLOBAL_ADMIN, USER_GROUP_ADMIN, USER_NS_ADMIN);
    verifyDenied(grantAction, USER_GLOBAL_CREATE, USER_GLOBAL_WRITE, USER_GLOBAL_READ,
      USER_GLOBAL_EXEC, USER_NS_CREATE, USER_NS_WRITE, USER_NS_READ, USER_NS_EXEC,
      USER_TABLE_CREATE, USER_TABLE_WRITE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(grantNamespaceAction, SUPERUSER, USER_GLOBAL_ADMIN, USER_GROUP_ADMIN,
      USER_NS_ADMIN, USER_GROUP_NS_ADMIN);
    verifyDenied(grantNamespaceAction, USER_GLOBAL_CREATE, USER_GLOBAL_WRITE, USER_GLOBAL_READ,
      USER_GLOBAL_EXEC, USER_NS_CREATE, USER_NS_WRITE, USER_NS_READ, USER_NS_EXEC,
      USER_TABLE_CREATE, USER_TABLE_WRITE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(revokeAction, SUPERUSER, USER_GLOBAL_ADMIN, USER_GROUP_ADMIN, USER_NS_ADMIN);
    verifyDenied(revokeAction, USER_GLOBAL_CREATE, USER_GLOBAL_WRITE, USER_GLOBAL_READ,
      USER_GLOBAL_EXEC, USER_NS_CREATE, USER_NS_WRITE, USER_NS_READ, USER_NS_EXEC,
      USER_TABLE_CREATE, USER_TABLE_WRITE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(revokeNamespaceAction, SUPERUSER, USER_GLOBAL_ADMIN, USER_GROUP_ADMIN,
      USER_NS_ADMIN, USER_GROUP_NS_ADMIN);
    verifyDenied(revokeNamespaceAction, USER_GLOBAL_CREATE, USER_GLOBAL_WRITE, USER_GLOBAL_READ,
      USER_GLOBAL_EXEC, USER_NS_CREATE, USER_NS_WRITE, USER_NS_READ, USER_NS_EXEC,
      USER_TABLE_CREATE, USER_TABLE_WRITE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(getPermissionsAction, SUPERUSER, USER_GLOBAL_ADMIN, USER_NS_ADMIN,
      USER_GROUP_ADMIN);
    verifyDenied(getPermissionsAction, USER_GLOBAL_CREATE, USER_GLOBAL_WRITE, USER_GLOBAL_READ,
      USER_GLOBAL_EXEC, USER_NS_CREATE, USER_NS_WRITE, USER_NS_READ, USER_NS_EXEC,
      USER_TABLE_CREATE, USER_TABLE_WRITE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testCreateTableWithNamespace() throws Exception {
    AccessTestAction createTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TEST_TABLE));
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        ACCESS_CONTROLLER.preCreateTable(ObserverContextImpl.createAndPrepare(CP_ENV), htd, null);
        return null;
      }
    };

    //createTable            : superuser | global(C) | NS(C)
    verifyAllowed(createTable, SUPERUSER, USER_GLOBAL_CREATE, USER_NS_CREATE, USER_GROUP_CREATE);
    verifyDenied(createTable, USER_GLOBAL_ADMIN, USER_GLOBAL_WRITE, USER_GLOBAL_READ,
      USER_GLOBAL_EXEC, USER_NS_ADMIN, USER_NS_WRITE, USER_NS_READ, USER_NS_EXEC,
      USER_TABLE_CREATE, USER_TABLE_WRITE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_ADMIN);
  }
}
