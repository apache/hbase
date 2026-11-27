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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.access.Permission.Action;
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
 * Tests for {@link PermissionStorage}, the reading and writing of the access permissions in the
 * {@code hbase:acl} table, including the single user permission lookup methods. The user tables and
 * namespaces used below do not need to exist, {@link PermissionStorage} only reads and writes the
 * {@code hbase:acl} table.
 */
@Tag(SecurityTests.TAG)
@Tag(MediumTests.TAG)
public class TestPermissionStorage {

  private static final Logger LOG = LoggerFactory.getLogger(TestPermissionStorage.class);
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final String USER1 = "perm_user1";
  private static final String USER2 = "perm_user2";
  private static final String USER3 = "perm_user3";
  // a group name is stored with a leading '@'
  private static final String GROUP = "@perm_group";

  private static final String TEST_NAMESPACE = "perm_storage_ns";
  private static final String TEST_NAMESPACE2 = "perm_storage_ns2";
  private static final TableName TEST_TABLE = TableName.valueOf("perm_storage_test");
  private static final TableName TEST_TABLE2 = TableName.valueOf("perm_storage_test2");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("col1");

  @BeforeAll
  public static void beforeClass() throws Exception {
    // setup configuration
    Configuration conf = UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);

    UTIL.startMiniCluster();

    // Wait for the ACL table to become available
    UTIL.waitTableEnabled(PermissionStorage.ACL_TABLE_NAME);
    UTIL.waitTableAvailable(TableName.valueOf("hbase:acl"));
  }

  @AfterAll
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @AfterEach
  public void tearDown() throws Exception {
    // the tests use arbitrary user names, clean up whole entries instead of revoking permission by
    // permission
    try (Connection conn = ConnectionFactory.createConnection(UTIL.getConfiguration())) {
      PermissionStorage.removeTablePermissions(conn, TEST_TABLE);
      PermissionStorage.removeTablePermissions(conn, TEST_TABLE2);
      PermissionStorage.removeNamespacePermissions(conn, TEST_NAMESPACE);
      PermissionStorage.removeNamespacePermissions(conn, TEST_NAMESPACE2);
      // the global permissions are stored in the hbase:acl entry
      PermissionStorage.removeTablePermissions(conn, PermissionStorage.ACL_TABLE_NAME);
    }
  }

  private Connection getConnection() throws Exception {
    return ConnectionFactory.createConnection(UTIL.getConfiguration());
  }

  private ListMultimap<String, UserPermission> getTablePerms(Connection conn, TableName tableName)
    throws IOException {
    ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
    PermissionStorage.getTablePermissions(conn, tableName)
      .forEach(up -> perms.put(up.getUser(), up));
    return perms;
  }

  private ListMultimap<String, UserPermission> getGlobalPerms(Connection conn) throws IOException {
    ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
    PermissionStorage.getGlobalPermissions(conn).forEach(up -> perms.put(up.getUser(), up));
    return perms;
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
  public void testGetUserTablePermission() throws Exception {
    try (Connection conn = getConnection()) {
      // not granted yet
      assertNull(PermissionStorage.getUserTablePermission(conn, TEST_TABLE, USER1));

      PermissionStorage.addUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_TABLE).withActions(Action.READ, Action.WRITE).build()), false);
      UserPermission userPerm = PermissionStorage.getUserTablePermission(conn, TEST_TABLE, USER1);
      assertNotNull(userPerm);
      assertEquals(USER1, userPerm.getUser());
      assertEquals(Permission.Scope.TABLE, userPerm.getAccessScope());
      TablePermission tablePerm = (TablePermission) userPerm.getPermission();
      assertEquals(TEST_TABLE, tablePerm.getTableName());
      assertNull(tablePerm.getFamily());
      assertNull(tablePerm.getQualifier());
      assertArrayEquals(new Action[] { Action.READ, Action.WRITE }, tablePerm.getActions());

      // only matches the exact user, and only on the exact table
      assertNull(PermissionStorage.getUserTablePermission(conn, TEST_TABLE, USER2));
      assertNull(PermissionStorage.getUserTablePermission(conn, TEST_TABLE2, USER1));
    }
  }

  @Test
  public void testGetUserTablePermissionMatchesExactEntry() throws Exception {
    try (Connection conn = getConnection()) {
      // family and qualifier level permissions are stored under separate qualifiers, they must
      // not show up when looking for the table level permission
      PermissionStorage.addUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY).withActions(Action.READ).build()),
        false);
      PermissionStorage.addUserPermission(conn,
        new UserPermission(USER1, Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
          .withQualifier(TEST_QUALIFIER).withActions(Action.WRITE).build()),
        false);
      assertNull(PermissionStorage.getUserTablePermission(conn, TEST_TABLE, USER1));

      // and the table level permission is still matched exactly, without family and qualifier
      PermissionStorage.addUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_TABLE).withActions(Action.READ, Action.WRITE).build()), false);
      assertEquals(
        new UserPermission(USER1,
          Permission.newBuilder(TEST_TABLE).withActions(Action.READ, Action.WRITE).build()),
        PermissionStorage.getUserTablePermission(conn, TEST_TABLE, USER1));
    }
  }

  @Test
  public void testGetUserTablePermissionAfterRemove() throws Exception {
    try (Connection conn = getConnection()) {
      PermissionStorage.addUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_TABLE).withActions(Action.READ, Action.WRITE).build()), false);

      // removing an action which is not granted keeps the permission untouched
      PermissionStorage.removeUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_TABLE).withActions(Action.EXEC).build()));
      assertEquals(
        new UserPermission(USER1,
          Permission.newBuilder(TEST_TABLE).withActions(Action.READ, Action.WRITE).build()),
        PermissionStorage.getUserTablePermission(conn, TEST_TABLE, USER1));

      // removing one of the granted actions leaves the others behind
      PermissionStorage.removeUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_TABLE).withActions(Action.WRITE).build()));
      UserPermission userPerm = PermissionStorage.getUserTablePermission(conn, TEST_TABLE, USER1);
      assertNotNull(userPerm);
      assertArrayEquals(new Action[] { Action.READ }, userPerm.getPermission().getActions());

      // removing the last granted action removes the whole entry
      PermissionStorage.removeUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_TABLE).withActions(Action.READ).build()));
      assertNull(PermissionStorage.getUserTablePermission(conn, TEST_TABLE, USER1));
    }
  }

  @Test
  public void testGetUserTablePermissionForGroup() throws Exception {
    try (Connection conn = getConnection()) {
      PermissionStorage.addUserPermission(conn, new UserPermission(GROUP,
        Permission.newBuilder(TEST_TABLE).withActions(Action.READ).build()), false);
      assertEquals(
        new UserPermission(GROUP,
          Permission.newBuilder(TEST_TABLE).withActions(Action.READ).build()),
        PermissionStorage.getUserTablePermission(conn, TEST_TABLE, GROUP));
      // the group is stored with a leading '@', only the exact name matches
      assertNull(PermissionStorage.getUserTablePermission(conn, TEST_TABLE, "perm_group"));
    }
  }

  @Test
  public void testGetUserNamespacePermission() throws Exception {
    try (Connection conn = getConnection()) {
      // not granted yet
      assertNull(PermissionStorage.getUserNamespacePermission(conn, TEST_NAMESPACE, USER1));

      PermissionStorage.addUserPermission(conn,
        new UserPermission(USER1,
          Permission.newBuilder(TEST_NAMESPACE).withActions(Action.READ, Action.CREATE).build()),
        false);
      UserPermission userPerm =
        PermissionStorage.getUserNamespacePermission(conn, TEST_NAMESPACE, USER1);
      assertNotNull(userPerm);
      assertEquals(USER1, userPerm.getUser());
      assertEquals(Permission.Scope.NAMESPACE, userPerm.getAccessScope());
      NamespacePermission nsPerm = (NamespacePermission) userPerm.getPermission();
      assertEquals(TEST_NAMESPACE, nsPerm.getNamespace());
      assertArrayEquals(new Action[] { Action.READ, Action.CREATE }, nsPerm.getActions());

      // only matches the exact user, and only on the exact namespace
      assertNull(PermissionStorage.getUserNamespacePermission(conn, TEST_NAMESPACE, USER2));
      assertNull(PermissionStorage.getUserNamespacePermission(conn, TEST_NAMESPACE2, USER1));
    }
  }

  @Test
  public void testGetUserNamespacePermissionAfterRemove() throws Exception {
    try (Connection conn = getConnection()) {
      PermissionStorage.addUserPermission(conn,
        new UserPermission(USER1,
          Permission.newBuilder(TEST_NAMESPACE).withActions(Action.READ, Action.WRITE).build()),
        false);

      // removing one of the granted actions leaves the others behind
      PermissionStorage.removeUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_NAMESPACE).withActions(Action.WRITE).build()));
      assertEquals(
        new UserPermission(USER1,
          Permission.newBuilder(TEST_NAMESPACE).withActions(Action.READ).build()),
        PermissionStorage.getUserNamespacePermission(conn, TEST_NAMESPACE, USER1));

      // removing the last granted action removes the whole entry
      PermissionStorage.removeUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_NAMESPACE).withActions(Action.READ).build()));
      assertNull(PermissionStorage.getUserNamespacePermission(conn, TEST_NAMESPACE, USER1));
    }
  }

  @Test
  public void testGetUserGlobalPermission() throws Exception {
    try (Connection conn = getConnection()) {
      // table and namespace permissions do not show up when looking for the global permission
      PermissionStorage.addUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_TABLE).withActions(Action.ADMIN).build()), false);
      PermissionStorage.addUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder(TEST_NAMESPACE).withActions(Action.ADMIN).build()), false);
      assertNull(PermissionStorage.getUserGlobalPermission(conn, USER1));

      PermissionStorage.addUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder().withActions(Action.ADMIN, Action.READ).build()), false);
      UserPermission userPerm = PermissionStorage.getUserGlobalPermission(conn, USER1);
      assertNotNull(userPerm);
      assertEquals(USER1, userPerm.getUser());
      assertEquals(Permission.Scope.GLOBAL, userPerm.getAccessScope());
      assertArrayEquals(new Action[] { Action.READ, Action.ADMIN },
        userPerm.getPermission().getActions());

      // only matches the exact user
      assertNull(PermissionStorage.getUserGlobalPermission(conn, USER2));
    }
  }

  @Test
  public void testGetUserGlobalPermissionAfterRemove() throws Exception {
    try (Connection conn = getConnection()) {
      PermissionStorage.addUserPermission(conn, new UserPermission(USER1,
        Permission.newBuilder().withActions(Action.READ, Action.WRITE).build()), false);

      // removing one of the granted actions leaves the others behind
      PermissionStorage.removeUserPermission(conn,
        new UserPermission(USER1, Permission.newBuilder().withActions(Action.WRITE).build()));
      assertEquals(
        new UserPermission(USER1, Permission.newBuilder().withActions(Action.READ).build()),
        PermissionStorage.getUserGlobalPermission(conn, USER1));

      // removing the last granted action removes the whole entry
      PermissionStorage.removeUserPermission(conn,
        new UserPermission(USER1, Permission.newBuilder().withActions(Action.READ).build()));
      assertNull(PermissionStorage.getUserGlobalPermission(conn, USER1));
    }
  }

  @Test
  public void testBasicWrite() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      // add some permissions
      PermissionStorage.addUserPermission(connection,
        new UserPermission(USER1, Permission.newBuilder(TEST_TABLE)
          .withActions(Permission.Action.READ, Permission.Action.WRITE).build()),
        false);
      PermissionStorage.addUserPermission(connection, new UserPermission(USER2,
        Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build()), false);
      PermissionStorage.addUserPermission(connection,
        new UserPermission(USER3, Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
          .withQualifier(TEST_QUALIFIER).withActions(Permission.Action.READ).build()),
        false);
      // retrieve the same
      ListMultimap<String, UserPermission> perms = getTablePerms(connection, TEST_TABLE);
      List<UserPermission> userPerms = perms.get(USER1);
      assertNotNull(userPerms, "Should have permissions for " + USER1);
      assertEquals(1, userPerms.size(), "Should have 1 permission for " + USER1);
      assertEquals(Permission.Scope.TABLE, userPerms.get(0).getAccessScope());
      TablePermission permission = (TablePermission) userPerms.get(0).getPermission();
      assertEquals(TEST_TABLE, permission.getTableName(), "Permission should be for " + TEST_TABLE);
      assertNull(permission.getFamily(), "Column family should be empty");

      // check actions
      assertNotNull(permission.getActions());
      assertEquals(2, permission.getActions().length);
      List<Permission.Action> actions = Arrays.asList(permission.getActions());
      assertTrue(actions.contains(TablePermission.Action.READ));
      assertTrue(actions.contains(TablePermission.Action.WRITE));

      userPerms = perms.get(USER2);
      assertNotNull(userPerms, "Should have permissions for " + USER2);
      assertEquals(1, userPerms.size(), "Should have 1 permission for " + USER2);
      assertEquals(Permission.Scope.TABLE, userPerms.get(0).getAccessScope());
      permission = (TablePermission) userPerms.get(0).getPermission();
      assertEquals(TEST_TABLE, permission.getTableName(), "Permission should be for " + TEST_TABLE);
      assertNull(permission.getFamily(), "Column family should be empty");

      // check actions
      assertNotNull(permission.getActions());
      assertEquals(1, permission.getActions().length);
      actions = Arrays.asList(permission.getActions());
      assertTrue(actions.contains(TablePermission.Action.READ));
      assertFalse(actions.contains(TablePermission.Action.WRITE));

      userPerms = perms.get(USER3);
      assertNotNull(userPerms, "Should have permissions for " + USER3);
      assertEquals(1, userPerms.size(), "Should have 1 permission for " + USER3);
      assertEquals(Permission.Scope.TABLE, userPerms.get(0).getAccessScope());
      permission = (TablePermission) userPerms.get(0).getPermission();
      assertEquals(TEST_TABLE, permission.getTableName(), "Permission should be for " + TEST_TABLE);
      assertTrue(Bytes.equals(TEST_FAMILY, permission.getFamily()),
        "Permission should be for family " + Bytes.toString(TEST_FAMILY));
      assertTrue(Bytes.equals(TEST_QUALIFIER, permission.getQualifier()),
        "Permission should be for qualifier " + Bytes.toString(TEST_QUALIFIER));

      // check actions
      assertNotNull(permission.getActions());
      assertEquals(1, permission.getActions().length);
      actions = Arrays.asList(permission.getActions());
      assertTrue(actions.contains(TablePermission.Action.READ));
      assertFalse(actions.contains(TablePermission.Action.WRITE));

      // table 2 permissions
      PermissionStorage.addUserPermission(connection,
        new UserPermission(USER2, Permission.newBuilder(TEST_TABLE2)
          .withActions(Permission.Action.READ, Permission.Action.WRITE).build()),
        false);

      // check full load
      Map<byte[], ListMultimap<String, UserPermission>> allPerms =
        PermissionStorage.loadAll(connection);
      assertEquals(2, allPerms.size(),
        "Full permission map should have entries for both test tables");

      userPerms = allPerms.get(TEST_TABLE.getName()).get(USER2);
      assertNotNull(userPerms);
      assertEquals(1, userPerms.size());
      assertEquals(Permission.Scope.TABLE, userPerms.get(0).getAccessScope());
      permission = (TablePermission) userPerms.get(0).getPermission();
      assertEquals(TEST_TABLE, permission.getTableName());
      assertEquals(1, permission.getActions().length);
      assertEquals(Permission.Action.READ, permission.getActions()[0]);

      userPerms = allPerms.get(TEST_TABLE2.getName()).get(USER2);
      assertNotNull(userPerms);
      assertEquals(1, userPerms.size());
      assertEquals(Permission.Scope.TABLE, userPerms.get(0).getAccessScope());
      permission = (TablePermission) userPerms.get(0).getPermission();
      assertEquals(TEST_TABLE2, permission.getTableName());
      assertEquals(2, permission.getActions().length);
      actions = Arrays.asList(permission.getActions());
      assertTrue(actions.contains(Permission.Action.READ));
      assertTrue(actions.contains(Permission.Action.WRITE));
    }
  }

  @Test
  public void testRevokeFamilyPermissionKeepsTableLevelPermission() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      // table level READ for USER1
      PermissionStorage.addUserPermission(connection, new UserPermission(USER1,
        Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build()), false);
      // family level WRITE for USER1 on the same table
      PermissionStorage.addUserPermission(connection,
        new UserPermission(USER1, Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
          .withActions(Permission.Action.WRITE).build()),
        false);

      // revoke only the family level WRITE
      PermissionStorage.removeUserPermission(connection,
        new UserPermission(USER1, Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
          .withActions(Permission.Action.WRITE).build()));

      List<UserPermission> userPerms = getTablePerms(connection, TEST_TABLE).get(USER1);
      assertEquals(1, userPerms.size(),
        "family level WRITE should be revoked, only the table level READ should remain");
      TablePermission permission = (TablePermission) userPerms.get(0).getPermission();
      assertNull(permission.getFamily(), "table level permission should be kept untouched");
      assertArrayEquals(new Permission.Action[] { Permission.Action.READ }, permission.getActions(),
        "table level READ should be kept untouched");
    }
  }

  @Test
  public void testRevokeFamilyPermissionKeepsTableLevelActions() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      // table level READ,WRITE for USER1
      PermissionStorage.addUserPermission(connection,
        new UserPermission(USER1, Permission.newBuilder(TEST_TABLE)
          .withActions(Permission.Action.READ, Permission.Action.WRITE).build()),
        false);
      // family level WRITE for USER1 on the same table
      PermissionStorage.addUserPermission(connection,
        new UserPermission(USER1, Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
          .withActions(Permission.Action.WRITE).build()),
        false);

      // revoke only the family level WRITE
      PermissionStorage.removeUserPermission(connection,
        new UserPermission(USER1, Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
          .withActions(Permission.Action.WRITE).build()));

      List<UserPermission> userPerms = getTablePerms(connection, TEST_TABLE).get(USER1);
      assertEquals(1, userPerms.size(),
        "family level WRITE should be revoked, only the table level permission should remain");
      TablePermission permission = (TablePermission) userPerms.get(0).getPermission();
      assertNull(permission.getFamily(), "table level permission should be kept untouched");
      assertArrayEquals(new Permission.Action[] { Permission.Action.READ, Permission.Action.WRITE },
        permission.getActions(), "table level actions should be kept untouched");
    }
  }

  @Test
  public void testSerialization() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    ListMultimap<String, UserPermission> permissions = createPermissions();
    byte[] permsData = PermissionStorage.writePermissionsAsBytes(permissions, conf);

    ListMultimap<String, UserPermission> copy =
      PermissionStorage.readUserPermission(permsData, conf);

    checkMultimapEqual(permissions, copy);
  }

  private ListMultimap<String, UserPermission> createPermissions() {
    ListMultimap<String, UserPermission> permissions = ArrayListMultimap.create();
    permissions.put(USER1, new UserPermission(USER1,
      Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build()));
    permissions.put(USER1, new UserPermission(USER1, Permission.newBuilder(TEST_TABLE)
      .withFamily(TEST_FAMILY).withActions(Permission.Action.WRITE).build()));
    permissions.put(USER1, new UserPermission(USER1,
      Permission.newBuilder(TEST_TABLE2).withActions(Permission.Action.READ).build()));
    permissions.put(USER2, new UserPermission(USER2, Permission.newBuilder(TEST_TABLE2)
      .withActions(Permission.Action.READ, Permission.Action.WRITE).build()));
    permissions.put(USER3, new UserPermission(USER3,
      Permission.newBuilder(TEST_NAMESPACE).withActions(Permission.Action.READ).build()));
    return permissions;
  }

  @Test
  public void testGlobalPermission() throws Exception {
    Configuration conf = UTIL.getConfiguration();

    // add some permissions
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      PermissionStorage.addUserPermission(connection, new UserPermission(USER1, Permission
        .newBuilder().withActions(Permission.Action.READ, Permission.Action.WRITE).build()), false);
      PermissionStorage.addUserPermission(connection, new UserPermission(USER2,
        Permission.newBuilder().withActions(Permission.Action.CREATE).build()), false);
      PermissionStorage.addUserPermission(connection,
        new UserPermission(USER3,
          Permission.newBuilder()
            .withActions(Permission.Action.ADMIN, Permission.Action.READ, Permission.Action.CREATE)
            .build()),
        false);

      ListMultimap<String, UserPermission> perms = getGlobalPerms(connection);
      List<UserPermission> user1Perms = perms.get(USER1);
      assertEquals(1, user1Perms.size(), "Should have 1 permission for " + USER1);
      assertArrayEquals(new Permission.Action[] { Permission.Action.READ, Permission.Action.WRITE },
        user1Perms.get(0).getPermission().getActions(), USER1 + " should have WRITE permission");

      List<UserPermission> user2Perms = perms.get(USER2);
      assertEquals(1, user2Perms.size(), "Should have 1 permission for " + USER2);
      assertArrayEquals(new Permission.Action[] { Permission.Action.CREATE },
        user2Perms.get(0).getPermission().getActions(), USER2 + " should have CREATE permission");

      List<UserPermission> user3Perms = perms.get(USER3);
      assertEquals(1, user3Perms.size(), "Should have 1 permission for " + USER3);
      assertArrayEquals(
        new Permission.Action[] { Permission.Action.READ, Permission.Action.CREATE,
          Permission.Action.ADMIN },
        user3Perms.get(0).getPermission().getActions(),
        USER3 + " should have ADMIN, READ, CREATE permission");
    }
  }

  private void testAddUserPermissionWithInvalidUser0(String invalidUser, String errorMsg)
    throws Exception {
    Permission tablePerm =
      Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build();
    // the check is performed before using the Connection instance, so we just pass null
    IOException e = assertThrows(
      IOException.class, () -> PermissionStorage.addUserPermission(null,
        new UserPermission(invalidUser, tablePerm), false),
      "Should reject user name [" + invalidUser + "]");
    assertThat(e.getMessage(), containsString(errorMsg));
  }

  @Test
  public void testAddUserPermissionWithInvalidUser() throws Exception {
    testAddUserPermissionWithInvalidUser0(null, "null user name is not allowed");
    testAddUserPermissionWithInvalidUser0("", "Blank user name '' is not allowed");
    testAddUserPermissionWithInvalidUser0(" ", "Blank user name ' ' is not allowed");
    testAddUserPermissionWithInvalidUser0("\t", "Blank user name '\t' is not allowed");
    testAddUserPermissionWithInvalidUser0(" user",
      "Leading or trailing whitespace in user name ' user' is not allowed");
    testAddUserPermissionWithInvalidUser0("user ",
      "Leading or trailing whitespace in user name 'user ' is not allowed");
    testAddUserPermissionWithInvalidUser0(" user ",
      "Leading or trailing whitespace in user name ' user ' is not allowed");
    testAddUserPermissionWithInvalidUser0("user,user",
      "',' in user name 'user,user' is not allowed");

    testAddUserPermissionWithInvalidUser0("@", "Blank group name '@' is not allowed");
    testAddUserPermissionWithInvalidUser0("@ ", "Blank group name '@ ' is not allowed");
    testAddUserPermissionWithInvalidUser0("@\t", "Blank group name '@\t' is not allowed");
    testAddUserPermissionWithInvalidUser0("@ group",
      "Leading or trailing whitespace in group name '@ group' is not allowed");
    testAddUserPermissionWithInvalidUser0("@group ",
      "Leading or trailing whitespace in group name '@group ' is not allowed");
    testAddUserPermissionWithInvalidUser0("@ group ",
      "Leading or trailing whitespace in group name '@ group ' is not allowed");
    testAddUserPermissionWithInvalidUser0("@group,group",
      "',' in group name '@group,group' is not allowed");
  }

  @Test
  public void testAddUserPermissionWithInvalidAction() throws Exception {
    // no actions associated with the permission
    IOException e = assertThrows(IOException.class, () -> PermissionStorage.addUserPermission(null,
      new UserPermission(USER1, Permission.newBuilder(TEST_TABLE).build()), false));
    assertThat(e.getMessage(), containsString("No actions associated with user"));
  }

  private void testRemoveUserPermissionWithInvalidUser0(String invalidUser, String errorMsg)
    throws Exception {
    Permission tablePerm =
      Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build();
    // the check is performed before using the Connection instance, so we just pass null
    IOException e = assertThrows(IOException.class,
      () -> PermissionStorage.removeUserPermission(null,
        new UserPermission(invalidUser, tablePerm)),
      "Should reject user name [" + invalidUser + "]");
    assertThat(e.getMessage(), containsString(errorMsg));
  }

  @Test
  public void testRemoveUserPermissionWithInvalidUser() throws Exception {
    testRemoveUserPermissionWithInvalidUser0(null, "null user name is not allowed");
    testRemoveUserPermissionWithInvalidUser0("", "Blank user name '' is not allowed");
    testRemoveUserPermissionWithInvalidUser0(" ", "Blank user name ' ' is not allowed");
    testRemoveUserPermissionWithInvalidUser0("\t", "Blank user name '\t' is not allowed");
    testRemoveUserPermissionWithInvalidUser0(" user",
      "Leading or trailing whitespace in user name ' user' is not allowed");
    testRemoveUserPermissionWithInvalidUser0("user ",
      "Leading or trailing whitespace in user name 'user ' is not allowed");
    testRemoveUserPermissionWithInvalidUser0(" user ",
      "Leading or trailing whitespace in user name ' user ' is not allowed");
    testRemoveUserPermissionWithInvalidUser0("user,user",
      "',' in user name 'user,user' is not allowed");

    testRemoveUserPermissionWithInvalidUser0("@", "Blank group name '@' is not allowed");
    testRemoveUserPermissionWithInvalidUser0("@ ", "Blank group name '@ ' is not allowed");
    testRemoveUserPermissionWithInvalidUser0("@\t", "Blank group name '@\t' is not allowed");
    testRemoveUserPermissionWithInvalidUser0("@ group",
      "Leading or trailing whitespace in group name '@ group' is not allowed");
    testRemoveUserPermissionWithInvalidUser0("@group ",
      "Leading or trailing whitespace in group name '@group ' is not allowed");
    testRemoveUserPermissionWithInvalidUser0("@ group ",
      "Leading or trailing whitespace in group name '@ group ' is not allowed");
    testRemoveUserPermissionWithInvalidUser0("@group,group",
      "',' in group name '@group,group' is not allowed");
  }

  @Test
  public void testRemoveWithNoActionsRemovesAllActions() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      PermissionStorage.addUserPermission(connection,
        new UserPermission(USER1, Permission.newBuilder(TEST_TABLE)
          .withActions(Permission.Action.READ, Permission.Action.WRITE).build()),
        false);

      // revoking without specifying actions is valid and removes the whole permission
      PermissionStorage.removeUserPermission(connection,
        new UserPermission(USER1, Permission.newBuilder(TEST_TABLE).build()));

      assertTrue(getTablePerms(connection, TEST_TABLE).get(USER1).isEmpty(),
        "table level permission should be fully revoked");
    }
  }

  @Test
  public void testAddGroupPermission() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      // a group name is stored as "@group", only a bare "@" is invalid
      PermissionStorage.addUserPermission(connection, new UserPermission(GROUP,
        Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build()), false);

      ListMultimap<String, UserPermission> perms = getTablePerms(connection, TEST_TABLE);
      List<UserPermission> groupPerms = perms.get(GROUP);
      assertEquals(1, groupPerms.size(), "Should have 1 permission for " + GROUP);
      assertEquals(Permission.Scope.TABLE, groupPerms.get(0).getAccessScope());
      TablePermission permission = (TablePermission) groupPerms.get(0).getPermission();
      assertEquals(TEST_TABLE, permission.getTableName());
      assertArrayEquals(new Permission.Action[] { Permission.Action.READ },
        permission.getActions());
    }
  }
}
