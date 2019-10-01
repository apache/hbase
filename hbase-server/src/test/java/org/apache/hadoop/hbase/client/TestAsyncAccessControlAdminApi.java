/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.access.SecureTestUtil.AccessTestAction;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@RunWith(Parameterized.class)
@Category({ ClientTests.class, SmallTests.class })
public class TestAsyncAccessControlAdminApi extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncAccessControlAdminApi.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    SecureTestUtil.enableSecurity(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(PermissionStorage.ACL_TABLE_NAME);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @Test
  public void test() throws Exception {
    TableName tableName = TableName.valueOf("test-table");
    String userName1 = "user1";
    String userName2 = "user2";
    User user2 = User.createUserForTesting(TEST_UTIL.getConfiguration(), userName2, new String[0]);
    Permission permission =
        Permission.newBuilder(tableName).withActions(Permission.Action.READ).build();
    UserPermission userPermission = new UserPermission(userName1, permission);

    // grant user1 table permission
    admin.grant(userPermission, false).get();

    // get table permissions
    List<UserPermission> userPermissions =
        admin.getUserPermissions(GetUserPermissionsRequest.newBuilder(tableName).build()).get();
    assertEquals(1, userPermissions.size());
    assertEquals(userPermission, userPermissions.get(0));

    // get table permissions
    userPermissions =
        admin
            .getUserPermissions(
              GetUserPermissionsRequest.newBuilder(tableName).withUserName(userName1).build())
            .get();
    assertEquals(1, userPermissions.size());
    assertEquals(userPermission, userPermissions.get(0));

    userPermissions =
        admin
            .getUserPermissions(
              GetUserPermissionsRequest.newBuilder(tableName).withUserName(userName2).build())
            .get();
    assertEquals(0, userPermissions.size());

    // has user permission
    List<Permission> permissions = Lists.newArrayList(permission);
    boolean hasPermission =
        admin.hasUserPermissions(userName1, permissions).get().get(0).booleanValue();
    assertTrue(hasPermission);
    hasPermission = admin.hasUserPermissions(userName2, permissions).get().get(0).booleanValue();
    assertFalse(hasPermission);

    AccessTestAction hasPermissionAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (AsyncConnection conn =
            ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get()) {
          return conn.getAdmin().hasUserPermissions(userName1, permissions).get().get(0);
        }
      }
    };
    try {
      user2.runAs(hasPermissionAction);
      fail("Should not come here");
    } catch (Exception e) {
      LOG.error("Call has permission error", e);
    }

    // check permission
    admin.hasUserPermissions(permissions);
    AccessTestAction checkPermissionsAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (AsyncConnection conn =
            ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get()) {
          return conn.getAdmin().hasUserPermissions(permissions).get().get(0);
        }
      }
    };
    assertFalse((Boolean) user2.runAs(checkPermissionsAction));
  }
}
