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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.ListMultimap;
import com.google.protobuf.BlockingRpcChannel;

@Category(MediumTests.class)
public class TestNamespaceCommands extends SecureTestUtil {
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static String TEST_NAMESPACE = "ns1";
  private static String TEST_NAMESPACE2 = "ns2";
  private static Configuration conf;
  private static MasterCoprocessorEnvironment CP_ENV;
  private static AccessController ACCESS_CONTROLLER;

  // user with all permissions
  private static User SUPERUSER;
  // user with rw permissions
  private static User USER_RW;
  // user with create table permissions alone
  private static User USER_CREATE;
  // user with permission on namespace for testing all operations.
  private static User USER_NSP_WRITE;
  // user with admin permission on namespace.
  private static User USER_NSP_ADMIN;

  private static String TEST_TABLE = TEST_NAMESPACE + ":testtable";
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = UTIL.getConfiguration();
    enableSecurity(conf);

    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER_RW = User.createUserForTesting(conf, "rw_user", new String[0]);
    USER_CREATE = User.createUserForTesting(conf, "create_user", new String[0]);
    USER_NSP_WRITE = User.createUserForTesting(conf, "namespace_write", new String[0]);
    USER_NSP_ADMIN = User.createUserForTesting(conf, "namespace_admin", new String[0]);

    UTIL.startMiniCluster();
    // Wait for the ACL table to become available
    UTIL.waitTableAvailable(AccessControlLists.ACL_TABLE_NAME.getName(), 30 * 1000);

    ACCESS_CONTROLLER = (AccessController) UTIL.getMiniHBaseCluster().getMaster()
      .getCoprocessorHost()
        .findCoprocessor(AccessController.class.getName());

    UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(TEST_NAMESPACE).build());
    UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(TEST_NAMESPACE2).build());

    grantOnNamespace(UTIL, USER_NSP_WRITE.getShortName(),
      TEST_NAMESPACE, Permission.Action.WRITE, Permission.Action.CREATE);

    grantOnNamespace(UTIL, USER_NSP_ADMIN.getShortName(), TEST_NAMESPACE, Permission.Action.ADMIN);
    grantOnNamespace(UTIL, USER_NSP_ADMIN.getShortName(), TEST_NAMESPACE2, Permission.Action.ADMIN);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.getHBaseAdmin().deleteNamespace(TEST_NAMESPACE);
    UTIL.getHBaseAdmin().deleteNamespace(TEST_NAMESPACE2);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAclTableEntries() throws Exception {
    String userTestNamespace = "userTestNsp";
    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      // Grant and check state in ACL table
      grantOnNamespace(UTIL, userTestNamespace, TEST_NAMESPACE,
        Permission.Action.WRITE);

      Result result = acl.get(new Get(Bytes.toBytes(userTestNamespace)));
      assertTrue(result != null);
      ListMultimap<String, TablePermission> perms =
          AccessControlLists.getNamespacePermissions(conf, TEST_NAMESPACE);
      assertEquals(3, perms.size());
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
      assertEquals(2, perms.size());
    } finally {
      acl.close();
    }
  }

  @Test
  public void testModifyNamespace() throws Exception {
    AccessTestAction modifyNamespace = new AccessTestAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preModifyNamespace(ObserverContext.createAndPrepare(CP_ENV, null),
          NamespaceDescriptor.create(TEST_NAMESPACE).addConfiguration("abc", "156").build());
        return null;
      }
    };
    // verify that superuser or hbase admin can modify namespaces.
    verifyAllowed(modifyNamespace, SUPERUSER, USER_NSP_ADMIN);
    // all others should be denied
    verifyDenied(modifyNamespace, USER_NSP_WRITE, USER_CREATE, USER_RW);
  }

  @Test
  public void testCreateAndDeleteNamespace() throws Exception {
    AccessTestAction createNamespace = new AccessTestAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCreateNamespace(ObserverContext.createAndPrepare(CP_ENV, null),
          NamespaceDescriptor.create(TEST_NAMESPACE2).build());
        return null;
      }
    };

    AccessTestAction deleteNamespace = new AccessTestAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteNamespace(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_NAMESPACE2);
        return null;
      }
    };

    // verify that only superuser can create namespaces.
    verifyAllowed(createNamespace, SUPERUSER);
 // verify that superuser or hbase admin can delete namespaces.
    verifyAllowed(deleteNamespace, SUPERUSER, USER_NSP_ADMIN);

    // all others should be denied
    verifyDenied(createNamespace, USER_NSP_WRITE, USER_CREATE, USER_RW, USER_NSP_ADMIN);
    verifyDenied(deleteNamespace, USER_NSP_WRITE, USER_CREATE, USER_RW);
  }

  @Test
  public void testGrantRevoke() throws Exception{
    final String testUser = "testUser";

    // Test if client API actions are authorized

    AccessTestAction grantAction = new AccessTestAction() {
      public Object run() throws Exception {
        HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service =
              acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          ProtobufUtil.grant(protocol, testUser, TEST_NAMESPACE, Action.WRITE);
        } finally {
          acl.close();
        }
        return null;
      }
    };

    AccessTestAction revokeAction = new AccessTestAction() {
      public Object run() throws Exception {
        HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service =
              acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          ProtobufUtil.revoke(protocol, testUser, TEST_NAMESPACE, Action.WRITE);
        } finally {
          acl.close();
        }
        return null;
      }
    };

    AccessTestAction getPermissionsAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          ProtobufUtil.getUserPermissions(protocol, Bytes.toBytes(TEST_NAMESPACE));
        } finally {
          acl.close();
        }
        return null;
      }
    };

    // Only HBase super user should be able to grant and revoke permissions to
    // namespaces
    verifyAllowed(grantAction, SUPERUSER, USER_NSP_ADMIN);
    verifyDenied(grantAction, USER_CREATE, USER_RW);
    verifyAllowed(revokeAction, SUPERUSER, USER_NSP_ADMIN);
    verifyDenied(revokeAction, USER_CREATE, USER_RW);

    // Only an admin should be able to get the user permission
    verifyAllowed(revokeAction, SUPERUSER, USER_NSP_ADMIN);
    verifyDeniedWithException(revokeAction, USER_CREATE, USER_RW);
  }

  @Test
  public void testCreateTableWithNamespace() throws Exception {
    AccessTestAction createTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TEST_TABLE));
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        ACCESS_CONTROLLER.preCreateTable(ObserverContext.createAndPrepare(CP_ENV, null), htd, null);
        return null;
      }
    };

    // Only users with create permissions on namespace should be able to create a new table
    verifyAllowed(createTable, SUPERUSER, USER_NSP_WRITE);

    // all others should be denied
    verifyDenied(createTable, USER_CREATE, USER_RW);
  }
}
