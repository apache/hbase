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

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
    TEST_UTIL.waitTableAvailable(AccessControlLists.ACL_TABLE_NAME);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @Test
  public void testGrant() throws Exception {
    TableName tableName = TableName.valueOf("test-table");
    String user = "test-user";
    UserPermission userPermission = new UserPermission(user,
        Permission.newBuilder(tableName).withActions(Permission.Action.READ).build());
    // grant user table permission
    admin.grant(userPermission, false).get();

    // get table permissions
    List<UserPermission> userPermissions =
        admin.getUserPermissions(GetUserPermissionsRequest.newBuilder(tableName).build()).get();
    assertEquals(1, userPermissions.size());
    assertEquals(userPermission, userPermissions.get(0));

    // get user table permissions
    userPermissions = admin.getUserPermissions(
      GetUserPermissionsRequest.newBuilder(tableName).withUserName(user).build()).get();
    assertEquals(1, userPermissions.size());
    assertEquals(userPermission, userPermissions.get(0));

    userPermissions = admin.getUserPermissions(
      GetUserPermissionsRequest.newBuilder(tableName).withUserName("u").build()).get();
    assertEquals(0, userPermissions.size());
  }
}
