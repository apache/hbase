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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

@InterfaceAudience.Private
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestAuthManager {

  private static final TableName TEST_TABLE = TableName.valueOf("authManagerTable");
  private static final String TEST_NAMESPACE = "authManagerNs";
  private static final TableName NS_TABLE = TableName.valueOf(TEST_NAMESPACE, "t");

  private Configuration conf;
  private AuthManager authManager;
  private User george;

  @BeforeEach
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "admin");
    Superusers.initialize(conf);
    authManager = new AuthManager(conf);
    george = User.createUserForTesting(conf, "george", new String[0]);
  }

  @Test
  public void testMetaFixedRules() throws Exception {
    // READ is open to any user, even before any grant exists
    assertTrue(
      authManager.authorizeUserTable(george, TableName.META_TABLE_NAME, Permission.Action.READ));
    assertTrue(
      authManager.accessUserTable(george, TableName.META_TABLE_NAME, Permission.Action.READ));
    assertTrue(
      authManager.authorizeUserTable(null, TableName.META_TABLE_NAME, Permission.Action.READ));
    // other actions are superuser only
    assertFalse(
      authManager.authorizeUserTable(george, TableName.META_TABLE_NAME, Permission.Action.WRITE));
    assertTrue(authManager.authorizeUserTable(User.getCurrent(), TableName.META_TABLE_NAME,
      Permission.Action.WRITE));

    // global, namespace and table level grants must not penetrate
    authManager.applyFullSnapshot(
      snapshot(globalEntry(), userPerms("george", globalPerm(Permission.Action.WRITE)),
        namespaceEntry("hbase"), userPerms("george", nsPerm("hbase", Permission.Action.WRITE)),
        TableName.META_TABLE_NAME.getName(),
        userPerms("george", tablePerm(TableName.META_TABLE_NAME, Permission.Action.WRITE))));
    assertFalse(
      authManager.authorizeUserTable(george, TableName.META_TABLE_NAME, Permission.Action.WRITE));
    assertFalse(
      authManager.accessUserTable(george, TableName.META_TABLE_NAME, Permission.Action.WRITE));
    assertTrue(
      authManager.authorizeUserTable(george, TableName.META_TABLE_NAME, Permission.Action.READ));
  }

  @Test
  public void testAclFixedRules() throws Exception {
    // grants of every scope must not give access to hbase:acl
    authManager.applyFullSnapshot(
      snapshot(globalEntry(), userPerms("george", globalPerm(Permission.Action.READ)),
        namespaceEntry("hbase"), userPerms("george", nsPerm("hbase", Permission.Action.READ)),
        PermissionStorage.ACL_TABLE_NAME.getName(),
        userPerms("george", tablePerm(PermissionStorage.ACL_TABLE_NAME, Permission.Action.READ))));
    for (Permission.Action action : Permission.Action.values()) {
      assertFalse(authManager.authorizeUserTable(george, PermissionStorage.ACL_TABLE_NAME, action));
      assertFalse(authManager.accessUserTable(george, PermissionStorage.ACL_TABLE_NAME, action));
    }
    assertFalse(authManager.authorizeUserFamily(george, PermissionStorage.ACL_TABLE_NAME,
      Bytes.toBytes("l"), Permission.Action.READ));
    assertFalse(authManager.authorizeCell(george, PermissionStorage.ACL_TABLE_NAME, null,
      Permission.Action.READ));
    // superuser passes
    assertTrue(authManager.authorizeUserTable(User.getCurrent(), PermissionStorage.ACL_TABLE_NAME,
      Permission.Action.READ));
  }

  @Test
  public void testNullTableKeepsGlobalScopeSemantics() throws Exception {
    // table == null is the historical "global scope" call, it must not hit the acl fixed rules
    assertFalse(authManager.authorizeUserTable(george, null, Permission.Action.READ));
    authManager.applyFullSnapshot(
      snapshot(globalEntry(), userPerms("george", globalPerm(Permission.Action.READ))));
    assertTrue(authManager.authorizeUserTable(george, null, Permission.Action.READ));
    assertTrue(authManager.accessUserTable(george, null, Permission.Action.READ));
  }

  @Test
  public void testApplyUpdates() throws Exception {
    ListMultimap<String, UserPermission> readPerms =
      userPerms("george", tablePerm(TEST_TABLE, Permission.Action.READ));
    authManager.applyUpdates(
      Collections.singletonList(AuthManager.EntryUpdate.replace(TEST_TABLE.getName(), readPerms)));
    assertTrue(authManager.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ));
    assertFalse(authManager.authorizeUserTable(george, TEST_TABLE, Permission.Action.WRITE));

    // the update replaces the whole entry, it does not merge into it
    authManager
      .applyUpdates(Collections.singletonList(AuthManager.EntryUpdate.replace(TEST_TABLE.getName(),
        userPerms("george", tablePerm(TEST_TABLE, Permission.Action.WRITE)))));
    assertFalse(authManager.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ));
    assertTrue(authManager.authorizeUserTable(george, TEST_TABLE, Permission.Action.WRITE));

    authManager.applyUpdates(
      Collections.singletonList(AuthManager.EntryUpdate.deleted(TEST_TABLE.getName())));
    assertFalse(authManager.authorizeUserTable(george, TEST_TABLE, Permission.Action.WRITE));

    // namespace and global entries
    authManager.applyUpdates(Arrays.asList(
      AuthManager.EntryUpdate.replace(namespaceEntry(TEST_NAMESPACE),
        userPerms("george", nsPerm(TEST_NAMESPACE, Permission.Action.READ))),
      AuthManager.EntryUpdate.replace(globalEntry(),
        userPerms("george", globalPerm(Permission.Action.ADMIN)))));
    assertTrue(authManager.authorizeUserTable(george, NS_TABLE, Permission.Action.READ));
    assertTrue(authManager.authorizeUserGlobal(george, Permission.Action.ADMIN));

    authManager
      .applyUpdates(Arrays.asList(AuthManager.EntryUpdate.deleted(namespaceEntry(TEST_NAMESPACE)),
        AuthManager.EntryUpdate.deleted(globalEntry())));
    assertFalse(authManager.authorizeUserTable(george, NS_TABLE, Permission.Action.READ));
    assertFalse(authManager.authorizeUserGlobal(george, Permission.Action.ADMIN));
  }

  @Test
  public void testApplyFullSnapshotSwaps() throws Exception {
    authManager.applyFullSnapshot(snapshot(TEST_TABLE.getName(),
      userPerms("george", tablePerm(TEST_TABLE, Permission.Action.READ))));
    assertTrue(authManager.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ));

    authManager.applyFullSnapshot(snapshot(NS_TABLE.getName(),
      userPerms("george", tablePerm(NS_TABLE, Permission.Action.WRITE))));
    // the previous entry is gone after the swap
    assertFalse(authManager.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ));
    assertTrue(authManager.authorizeUserTable(george, NS_TABLE, Permission.Action.WRITE));
  }

  private static byte[] globalEntry() {
    return PermissionStorage.ACL_GLOBAL_NAME;
  }

  private static byte[] namespaceEntry(String namespace) {
    return Bytes.toBytes(PermissionStorage.toNamespaceEntry(namespace));
  }

  private static Permission tablePerm(TableName table, Permission.Action action) {
    return Permission.newBuilder(table).withActions(action).build();
  }

  private static Permission nsPerm(String namespace, Permission.Action action) {
    return Permission.newBuilder(namespace).withActions(action).build();
  }

  private static Permission globalPerm(Permission.Action action) {
    return Permission.newBuilder().withActions(action).build();
  }

  private static ListMultimap<String, UserPermission> userPerms(String user, Permission perm) {
    ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
    perms.put(user, new UserPermission(user, perm));
    return perms;
  }

  private static Map<byte[], ListMultimap<String, UserPermission>> snapshot(byte[] entry,
    ListMultimap<String, UserPermission> perms) {
    Map<byte[], ListMultimap<String, UserPermission>> map =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    map.put(entry, perms);
    return map;
  }

  private static Map<byte[], ListMultimap<String, UserPermission>> snapshot(byte[] entry1,
    ListMultimap<String, UserPermission> perms1, byte[] entry2,
    ListMultimap<String, UserPermission> perms2) {
    Map<byte[], ListMultimap<String, UserPermission>> map = snapshot(entry1, perms1);
    map.put(entry2, perms2);
    return map;
  }

  private static Map<byte[], ListMultimap<String, UserPermission>> snapshot(byte[] entry1,
    ListMultimap<String, UserPermission> perms1, byte[] entry2,
    ListMultimap<String, UserPermission> perms2, byte[] entry3,
    ListMultimap<String, UserPermission> perms3) {
    Map<byte[], ListMultimap<String, UserPermission>> map =
      snapshot(entry1, perms1, entry2, perms2);
    map.put(entry3, perms3);
    return map;
  }

}
