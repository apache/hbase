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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SecurityTests.class, SmallTests.class })
public class TestUnloadAccessController extends SecureTestUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestUnloadAccessController.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static TableName TEST_TABLE = TableName.valueOf("TestUnloadAccessController");
  private static Permission permission =
      Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build();
  private static Admin admin;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.waitUntilAllSystemRegionsAssigned();
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGrant() {
    try {
      admin.grant(new UserPermission("user", permission), false);
      fail("Expected UnsupportedOperationException but not found");
    } catch (Throwable e) {
      checkException(e);
    }
  }

  @Test
  public void testRevoke() {
    try {
      admin.revoke(new UserPermission("user", permission));
      fail("Expected UnsupportedOperationException but not found");
    } catch (Throwable e) {
      e.printStackTrace();
      checkException(e);
    }
  }

  @Test
  public void testGetUserPermissions() {
    try {
      admin.getUserPermissions(GetUserPermissionsRequest.newBuilder().build());
      fail("Expected UnsupportedOperationException but not found");
    } catch (Throwable e) {
      checkException(e);
    }
  }

  @Test
  public void testHasUserPermission() {
    try {
      List<Permission> permissionList = new ArrayList<>();
      permissionList.add(permission);
      admin.hasUserPermissions(permissionList);
      fail("Expected UnsupportedOperationException but not found");
    } catch (Throwable e) {
      checkException(e);
    }
  }

  private void checkException(Throwable e) {
    if (e instanceof DoNotRetryIOException
        && e.getMessage().contains(UnsupportedOperationException.class.getName())) {
      return;
    }
    fail("Expected UnsupportedOperationException but found " + e.getMessage());
  }
}
