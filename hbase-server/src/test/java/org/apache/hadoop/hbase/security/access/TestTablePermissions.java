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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Test the reading and writing of access permissions on {@code _acl_} table.
 */
@Category({SecurityTests.class, LargeTests.class})
public class TestTablePermissions {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTablePermissions.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestTablePermissions.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static ZKWatcher ZKW;
  private final static Abortable ABORTABLE = new Abortable() {
    private final AtomicBoolean abort = new AtomicBoolean(false);

    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
      abort.set(true);
    }

    @Override
    public boolean isAborted() {
      return abort.get();
    }
  };

  private static String TEST_NAMESPACE = "perms_test_ns";
  private static String TEST_NAMESPACE2 = "perms_test_ns2";
  private static TableName TEST_TABLE =
      TableName.valueOf("perms_test");
  private static TableName TEST_TABLE2 =
      TableName.valueOf("perms_test2");
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static byte[] TEST_QUALIFIER = Bytes.toBytes("col1");

  @BeforeClass
  public static void beforeClass() throws Exception {
    // setup configuration
    Configuration conf = UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);

    UTIL.startMiniCluster();

    // Wait for the ACL table to become available
    UTIL.waitTableEnabled(AccessControlLists.ACL_TABLE_NAME);

    ZKW = new ZKWatcher(UTIL.getConfiguration(),
      "TestTablePermissions", ABORTABLE);

    UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    UTIL.createTable(TEST_TABLE2, TEST_FAMILY);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(AccessControlLists.ACL_TABLE_NAME)) {
      AccessControlLists.removeTablePermissions(conf, TEST_TABLE, table);
      AccessControlLists.removeTablePermissions(conf, TEST_TABLE2, table);
      AccessControlLists.removeTablePermissions(conf, AccessControlLists.ACL_TABLE_NAME, table);
    }
  }

  /**
   * The AccessControlLists.addUserPermission may throw exception before closing the table.
   */
  private void addUserPermission(Configuration conf, UserPermission userPerm, Table t) throws IOException {
    try {
      AccessControlLists.addUserPermission(conf, userPerm, t);
    } finally {
      t.close();
    }
  }

  @Test
  public void testBasicWrite() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      // add some permissions
      addUserPermission(conf,
          new UserPermission(Bytes.toBytes("george"), TEST_TABLE, null, (byte[])null,
              UserPermission.Action.READ, UserPermission.Action.WRITE),
              connection.getTable(AccessControlLists.ACL_TABLE_NAME));
      addUserPermission(conf,
          new UserPermission(Bytes.toBytes("hubert"), TEST_TABLE, null, (byte[])null,
              UserPermission.Action.READ),
          connection.getTable(AccessControlLists.ACL_TABLE_NAME));
      addUserPermission(conf,
          new UserPermission(Bytes.toBytes("humphrey"),
              TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER,
              UserPermission.Action.READ),
          connection.getTable(AccessControlLists.ACL_TABLE_NAME));
    }
    // retrieve the same
    ListMultimap<String,TablePermission> perms =
        AccessControlLists.getTablePermissions(conf, TEST_TABLE);
    List<TablePermission> userPerms = perms.get("george");
    assertNotNull("Should have permissions for george", userPerms);
    assertEquals("Should have 1 permission for george", 1, userPerms.size());
    TablePermission permission = userPerms.get(0);
    assertEquals("Permission should be for " + TEST_TABLE,
        TEST_TABLE, permission.getTableName());
    assertNull("Column family should be empty", permission.getFamily());

    // check actions
    assertNotNull(permission.getActions());
    assertEquals(2, permission.getActions().length);
    List<TablePermission.Action> actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertTrue(actions.contains(TablePermission.Action.WRITE));

    userPerms = perms.get("hubert");
    assertNotNull("Should have permissions for hubert", userPerms);
    assertEquals("Should have 1 permission for hubert", 1, userPerms.size());
    permission = userPerms.get(0);
    assertEquals("Permission should be for " + TEST_TABLE,
        TEST_TABLE, permission.getTableName());
    assertNull("Column family should be empty", permission.getFamily());

    // check actions
    assertNotNull(permission.getActions());
    assertEquals(1, permission.getActions().length);
    actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertFalse(actions.contains(TablePermission.Action.WRITE));

    userPerms = perms.get("humphrey");
    assertNotNull("Should have permissions for humphrey", userPerms);
    assertEquals("Should have 1 permission for humphrey", 1, userPerms.size());
    permission = userPerms.get(0);
    assertEquals("Permission should be for " + TEST_TABLE,
        TEST_TABLE, permission.getTableName());
    assertTrue("Permission should be for family " + Bytes.toString(TEST_FAMILY),
        Bytes.equals(TEST_FAMILY, permission.getFamily()));
    assertTrue("Permission should be for qualifier " + Bytes.toString(TEST_QUALIFIER),
        Bytes.equals(TEST_QUALIFIER, permission.getQualifier()));

    // check actions
    assertNotNull(permission.getActions());
    assertEquals(1, permission.getActions().length);
    actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertFalse(actions.contains(TablePermission.Action.WRITE));

    // table 2 permissions
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(AccessControlLists.ACL_TABLE_NAME)) {
      AccessControlLists.addUserPermission(conf,
          new UserPermission(Bytes.toBytes("hubert"), TEST_TABLE2, null, (byte[])null,
              TablePermission.Action.READ, TablePermission.Action.WRITE), table);
    }
    // check full load
    Map<byte[], ListMultimap<String,TablePermission>> allPerms =
        AccessControlLists.loadAll(conf);
    assertEquals("Full permission map should have entries for both test tables",
        2, allPerms.size());

    userPerms = allPerms.get(TEST_TABLE.getName()).get("hubert");
    assertNotNull(userPerms);
    assertEquals(1, userPerms.size());
    permission = userPerms.get(0);
    assertEquals(TEST_TABLE, permission.getTableName());
    assertEquals(1, permission.getActions().length);
    assertEquals(TablePermission.Action.READ, permission.getActions()[0]);

    userPerms = allPerms.get(TEST_TABLE2.getName()).get("hubert");
    assertNotNull(userPerms);
    assertEquals(1, userPerms.size());
    permission = userPerms.get(0);
    assertEquals(TEST_TABLE2, permission.getTableName());
    assertEquals(2, permission.getActions().length);
    actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertTrue(actions.contains(TablePermission.Action.WRITE));
  }

  @Test
  public void testPersistence() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      addUserPermission(conf,
          new UserPermission(Bytes.toBytes("albert"), TEST_TABLE, null,
              (byte[])null, TablePermission.Action.READ), connection.getTable(AccessControlLists.ACL_TABLE_NAME));
      addUserPermission(conf,
          new UserPermission(Bytes.toBytes("betty"), TEST_TABLE, null,
              (byte[])null, TablePermission.Action.READ,
              TablePermission.Action.WRITE), connection.getTable(AccessControlLists.ACL_TABLE_NAME));
      addUserPermission(conf,
          new UserPermission(Bytes.toBytes("clark"),
              TEST_TABLE, TEST_FAMILY,
              TablePermission.Action.READ), connection.getTable(AccessControlLists.ACL_TABLE_NAME));
      addUserPermission(conf,
          new UserPermission(Bytes.toBytes("dwight"),
              TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER,
              TablePermission.Action.WRITE), connection.getTable(AccessControlLists.ACL_TABLE_NAME));
    }
    // verify permissions survive changes in table metadata
    ListMultimap<String,TablePermission> preperms =
        AccessControlLists.getTablePermissions(conf, TEST_TABLE);

    Table table = UTIL.getConnection().getTable(TEST_TABLE);
    table.put(new Put(Bytes.toBytes("row1"))
            .addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes("v1")));
    table.put(new Put(Bytes.toBytes("row2"))
            .addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes("v2")));
    Admin admin = UTIL.getAdmin();
    try {
      admin.split(TEST_TABLE);
    }
    catch (IOException e) {
      //although split fail, this may not affect following check
      //In old Split API without AM2, if region's best split key is not found,
      //there are not exception thrown. But in current API, exception
      //will be thrown.
      LOG.debug("region is not splittable, because " + e);
    }

    // wait for split
    Thread.sleep(10000);

    ListMultimap<String,TablePermission> postperms =
        AccessControlLists.getTablePermissions(conf, TEST_TABLE);

    checkMultimapEqual(preperms, postperms);
  }

  @Test
  public void testSerialization() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    ListMultimap<String,TablePermission> permissions = createPermissions();
    byte[] permsData = AccessControlLists.writePermissionsAsBytes(permissions, conf);

    ListMultimap<String, TablePermission> copy =
        AccessControlLists.readPermissions(permsData, conf);

    checkMultimapEqual(permissions, copy);
  }

  private ListMultimap<String,TablePermission> createPermissions() {
    ListMultimap<String,TablePermission> permissions = ArrayListMultimap.create();
    permissions.put("george", new TablePermission(TEST_TABLE, null,
        TablePermission.Action.READ));
    permissions.put("george", new TablePermission(TEST_TABLE, TEST_FAMILY,
        TablePermission.Action.WRITE));
    permissions.put("george", new TablePermission(TEST_TABLE2, null,
        TablePermission.Action.READ));
    permissions.put("hubert", new TablePermission(TEST_TABLE2, null,
        TablePermission.Action.READ, TablePermission.Action.WRITE));
    permissions.put("bruce",new TablePermission(TEST_NAMESPACE,
        TablePermission.Action.READ));
    return permissions;
  }

  public void checkMultimapEqual(ListMultimap<String,TablePermission> first,
      ListMultimap<String,TablePermission> second) {
    assertEquals(first.size(), second.size());
    for (String key : first.keySet()) {
      List<TablePermission> firstPerms = first.get(key);
      List<TablePermission> secondPerms = second.get(key);
      assertNotNull(secondPerms);
      assertEquals(firstPerms.size(), secondPerms.size());
      LOG.info("First permissions: "+firstPerms.toString());
      LOG.info("Second permissions: "+secondPerms.toString());
      for (TablePermission p : firstPerms) {
        assertTrue("Permission "+p.toString()+" not found", secondPerms.contains(p));
      }
    }
  }

  @Test
  public void testEquals() throws Exception {
    TablePermission p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    TablePermission p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ, TablePermission.Action.WRITE);
    p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.WRITE, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, TEST_FAMILY, TablePermission.Action.READ, TablePermission.Action.WRITE);
    p2 = new TablePermission(TEST_TABLE, TEST_FAMILY, TablePermission.Action.WRITE, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER, TablePermission.Action.READ, TablePermission.Action.WRITE);
    p2 = new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER, TablePermission.Action.WRITE, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_TABLE, TEST_FAMILY, TablePermission.Action.READ);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.WRITE);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));
    p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ, TablePermission.Action.WRITE);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_TABLE2, null, TablePermission.Action.READ);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p2 = new TablePermission(TEST_TABLE, null);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p1 = new TablePermission(TEST_NAMESPACE, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_NAMESPACE, TablePermission.Action.READ);
    assertEquals(p1, p2);

    p1 = new TablePermission(TEST_NAMESPACE, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_NAMESPACE2, TablePermission.Action.READ);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));
  }

  @Test
  public void testGlobalPermission() throws Exception {
    Configuration conf = UTIL.getConfiguration();

    // add some permissions
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      addUserPermission(conf,
          new UserPermission(Bytes.toBytes("user1"),
              Permission.Action.READ, Permission.Action.WRITE), connection.getTable(AccessControlLists.ACL_TABLE_NAME));
      addUserPermission(conf,
          new UserPermission(Bytes.toBytes("user2"),
              Permission.Action.CREATE), connection.getTable(AccessControlLists.ACL_TABLE_NAME));
      addUserPermission(conf,
          new UserPermission(Bytes.toBytes("user3"),
              Permission.Action.ADMIN, Permission.Action.READ, Permission.Action.CREATE),
          connection.getTable(AccessControlLists.ACL_TABLE_NAME));
    }
    ListMultimap<String,TablePermission> perms = AccessControlLists.getTablePermissions(conf, null);
    List<TablePermission> user1Perms = perms.get("user1");
    assertEquals("Should have 1 permission for user1", 1, user1Perms.size());
    assertEquals("user1 should have WRITE permission",
                 new Permission.Action[] { Permission.Action.READ, Permission.Action.WRITE },
                 user1Perms.get(0).getActions());

    List<TablePermission> user2Perms = perms.get("user2");
    assertEquals("Should have 1 permission for user2", 1, user2Perms.size());
    assertEquals("user2 should have CREATE permission",
                 new Permission.Action[] { Permission.Action.CREATE },
                 user2Perms.get(0).getActions());

    List<TablePermission> user3Perms = perms.get("user3");
    assertEquals("Should have 1 permission for user3", 1, user3Perms.size());
    assertEquals("user3 should have ADMIN, READ, CREATE permission",
                 new Permission.Action[] {
                    Permission.Action.READ, Permission.Action.CREATE, Permission.Action.ADMIN
                 },
                 user3Perms.get(0).getActions());
  }

  @Test
  public void testAuthManager() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    /* test a race condition causing TableAuthManager to sometimes fail global permissions checks
     * when the global cache is being updated
     */
    TableAuthManager authManager = TableAuthManager.getOrCreate(ZKW, conf);
    // currently running user is the system user and should have global admin perms
    User currentUser = User.getCurrent();
    assertTrue(authManager.authorize(currentUser, Permission.Action.ADMIN));
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      for (int i=1; i<=50; i++) {
        addUserPermission(conf, new UserPermission(Bytes.toBytes("testauth"+i),
            Permission.Action.ADMIN, Permission.Action.READ, Permission.Action.WRITE),
            connection.getTable(AccessControlLists.ACL_TABLE_NAME));
        // make sure the system user still shows as authorized
        assertTrue("Failed current user auth check on iter "+i,
            authManager.authorize(currentUser, Permission.Action.ADMIN));
      }
    }
  }
}
