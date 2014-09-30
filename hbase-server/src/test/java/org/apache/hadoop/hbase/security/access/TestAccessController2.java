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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TestTableName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestAccessController2 extends SecureTestUtil {

  private static final byte[] TEST_FAMILY = Bytes.toBytes("f");
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  @Rule public TestTableName TEST_TABLE = new TestTableName();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Enable security
    enableSecurity(conf);
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);
    TEST_UTIL.startMiniCluster();
    // Wait for the ACL table to become available
    TEST_UTIL.waitTableEnabled(AccessControlLists.ACL_TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateWithCorrectOwner() throws Exception {
    // Create a test user
    User testUser = User.createUserForTesting(TEST_UTIL.getConfiguration(), "TestUser",
      new String[0]);
    // Grant the test user the ability to create tables
    SecureTestUtil.grantGlobal(TEST_UTIL, testUser.getShortName(), Action.CREATE);
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor desc = new HTableDescriptor(TEST_TABLE.getTableName());
        desc.addFamily(new HColumnDescriptor(TEST_FAMILY));
        Admin admin = new HBaseAdmin(conf);
        try {
          admin.createTable(desc);
        } finally {
          admin.close();
        }
        return null;
      }
    }, testUser);
    TEST_UTIL.waitTableEnabled(TEST_TABLE.getTableName());
    // Verify that owner permissions have been granted to the test user on the
    // table just created
    List<TablePermission> perms = AccessControlLists.getTablePermissions(conf, TEST_TABLE.getTableName())
       .get(testUser.getShortName());
    assertNotNull(perms);
    assertFalse(perms.isEmpty());
    // Should be RWXCA
    assertTrue(perms.get(0).implies(Permission.Action.READ));
    assertTrue(perms.get(0).implies(Permission.Action.WRITE));
    assertTrue(perms.get(0).implies(Permission.Action.EXEC));
    assertTrue(perms.get(0).implies(Permission.Action.CREATE));
    assertTrue(perms.get(0).implies(Permission.Action.ADMIN));
  }

}
