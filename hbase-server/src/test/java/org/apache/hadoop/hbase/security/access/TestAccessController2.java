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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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

  private static final byte[] TEST_ROW = Bytes.toBytes("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("q");
  private static final byte[] TEST_VALUE = Bytes.toBytes("value");

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

}
