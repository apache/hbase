/*
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Tests around the table permissions which are not covered by {@link TestPermissionStorage}: the
 * survival of the stored permissions across table splits, the equality of the {@link Permission}s,
 * and the {@link AuthManager} permission cache.
 */
@Tag(SecurityTests.TAG)
@Tag(MediumTests.TAG)
public class TestTablePermissions {

  private static final Logger LOG = LoggerFactory.getLogger(TestTablePermissions.class);
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static String TEST_NAMESPACE = "perms_test_ns";
  private static String TEST_NAMESPACE2 = "perms_test_ns2";
  private static TableName TEST_TABLE = TableName.valueOf("perms_test");
  private static TableName TEST_TABLE2 = TableName.valueOf("perms_test2");
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static byte[] TEST_QUALIFIER = Bytes.toBytes("col1");

  @BeforeAll
  public static void beforeClass() throws Exception {
    // setup configuration
    Configuration conf = UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);

    UTIL.startMiniCluster();

    // Wait for the ACL table to become available
    UTIL.waitTableEnabled(PermissionStorage.ACL_TABLE_NAME);
    UTIL.waitTableAvailable(TableName.valueOf("hbase:acl"));

    UTIL.createTable(TEST_TABLE, TEST_FAMILY);
  }

  @AfterAll
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @AfterEach
  public void tearDown() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      PermissionStorage.removeTablePermissions(conn, TEST_TABLE);
      // testAuthManager writes global permissions
      PermissionStorage.removeTablePermissions(conn, PermissionStorage.ACL_TABLE_NAME);
    }
  }

  private ListMultimap<String, UserPermission> getTablePerms(Connection conn, TableName tableName)
    throws IOException {
    ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
    PermissionStorage.getTablePermissions(conn, tableName)
      .forEach(up -> perms.put(up.getUser(), up));
    return perms;
  }

  @Test
  public void testPersistence() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      PermissionStorage.addUserPermission(connection, new UserPermission("albert",
        Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build()), false);
      PermissionStorage.addUserPermission(connection,
        new UserPermission("betty", Permission.newBuilder(TEST_TABLE)
          .withActions(Permission.Action.READ, Permission.Action.WRITE).build()),
        false);
      PermissionStorage.addUserPermission(connection,
        new UserPermission("clark", Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
          .withActions(Permission.Action.READ).build()),
        false);
      PermissionStorage.addUserPermission(connection,
        new UserPermission("dwight", Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
          .withQualifier(TEST_QUALIFIER).withActions(Permission.Action.WRITE).build()),
        false);
      // verify permissions survive changes in table metadata
      ListMultimap<String, UserPermission> preperms = getTablePerms(connection, TEST_TABLE);

      Table table = UTIL.getConnection().getTable(TEST_TABLE);
      table.put(
        new Put(Bytes.toBytes("row1")).addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes("v1")));
      table.put(
        new Put(Bytes.toBytes("row2")).addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes("v2")));
      Admin admin = UTIL.getAdmin();
      try {
        admin.split(TEST_TABLE);
      } catch (IOException e) {
        // although split fail, this may not affect following check
        // In old Split API without AM2, if region's best split key is not found,
        // there are not exception thrown. But in current API, exception
        // will be thrown.
        LOG.debug("region is not splittable, because " + e);
      }

      // wait for split
      Thread.sleep(10000);

      ListMultimap<String, UserPermission> postperms = getTablePerms(connection, TEST_TABLE);

      checkMultimapEqual(preperms, postperms);
    }
  }

  private void checkMultimapEqual(ListMultimap<String, UserPermission> first,
    ListMultimap<String, UserPermission> second) {
    assertEquals(first.size(), second.size());
    for (String key : first.keySet()) {
      List<UserPermission> firstPerms = first.get(key);
      List<UserPermission> secondPerms = second.get(key);
      assertNotNull(secondPerms);
      assertEquals(firstPerms.size(), secondPerms.size());
      LOG.info("First permissions: " + firstPerms.toString());
      LOG.info("Second permissions: " + secondPerms.toString());
      for (UserPermission p : firstPerms) {
        assertTrue(secondPerms.contains(p), "Permission " + p.toString() + " not found");
      }
    }
  }

  @Test
  public void testEquals() throws Exception {
    Permission p1 = Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build();
    Permission p2 = Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build();
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = Permission.newBuilder(TEST_TABLE)
      .withActions(TablePermission.Action.READ, TablePermission.Action.WRITE).build();
    p2 = Permission.newBuilder(TEST_TABLE)
      .withActions(TablePermission.Action.WRITE, TablePermission.Action.READ).build();
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
      .withActions(TablePermission.Action.READ, TablePermission.Action.WRITE).build();
    p2 = Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
      .withActions(TablePermission.Action.WRITE, TablePermission.Action.READ).build();
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY).withQualifier(TEST_QUALIFIER)
      .withActions(TablePermission.Action.READ, TablePermission.Action.WRITE).build();
    p2 = Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY).withQualifier(TEST_QUALIFIER)
      .withActions(TablePermission.Action.WRITE, TablePermission.Action.READ).build();
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = Permission.newBuilder(TEST_TABLE).withActions(TablePermission.Action.READ).build();
    p2 = Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
      .withActions(TablePermission.Action.READ).build();
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p1 = Permission.newBuilder(TEST_TABLE).withActions(TablePermission.Action.READ).build();
    p2 = Permission.newBuilder(TEST_TABLE).withActions(TablePermission.Action.WRITE).build();
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));
    p2 = Permission.newBuilder(TEST_TABLE)
      .withActions(TablePermission.Action.READ, TablePermission.Action.WRITE).build();
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p1 = Permission.newBuilder(TEST_TABLE).withActions(TablePermission.Action.READ).build();
    p2 = Permission.newBuilder(TEST_TABLE2).withActions(TablePermission.Action.READ).build();
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p1 = Permission.newBuilder(TEST_NAMESPACE).withActions(TablePermission.Action.READ).build();
    p2 = Permission.newBuilder(TEST_NAMESPACE).withActions(TablePermission.Action.READ).build();
    assertEquals(p1, p2);

    p1 = Permission.newBuilder(TEST_NAMESPACE).withActions(TablePermission.Action.READ).build();
    p2 = Permission.newBuilder(TEST_NAMESPACE2).withActions(TablePermission.Action.READ).build();
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));
  }

  @Test
  public void testAuthManager() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    /**
     * test a race condition causing AuthManager to sometimes fail global permissions checks when
     * the global cache is being updated
     */
    AuthManager authManager = new AuthManager(conf);
    // currently running user is the system user and should have global admin perms
    User currentUser = User.getCurrent();
    assertTrue(authManager.authorizeUserGlobal(currentUser, Permission.Action.ADMIN));
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      for (int i = 1; i <= 50; i++) {
        PermissionStorage.addUserPermission(connection,
          new UserPermission("testauth" + i,
            Permission.newBuilder()
              .withActions(Permission.Action.ADMIN, Permission.Action.READ, Permission.Action.WRITE)
              .build()),
          false);
        // make sure the system user still shows as authorized
        assertTrue(authManager.authorizeUserGlobal(currentUser, Permission.Action.ADMIN),
          "Failed current user auth check on iter " + i);
      }
    }
  }
}
