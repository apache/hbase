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

import static org.apache.hadoop.hbase.security.access.Permission.Action.READ;
import static org.apache.hadoop.hbase.security.access.Permission.Action.WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableSnapshotScanner;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ SecurityTests.class, LargeTests.class })
public class TestSnapshotScannerHDFSAclController {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotScannerHDFSAclController.class);
  @Rule
  public TestName name = new TestName();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSnapshotScannerHDFSAclController.class);

  private static final String UN_GRANT_USER = "un_grant_user";
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static Admin admin = null;
  private static FileSystem fs = null;
  private static Path rootDir = null;
  private static User unGrantUser = null;
  private static SnapshotScannerHDFSAclHelper helper;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // enable hdfs acl and set umask to 027
    conf.setBoolean("dfs.namenode.acls.enabled", true);
    conf.set("fs.permissions.umask-mode", "027");
    // enable hbase hdfs acl feature
    conf.setBoolean(SnapshotScannerHDFSAclHelper.USER_SCAN_SNAPSHOT_ENABLE, true);
    // enable secure
    conf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    conf.set(SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_TMP_DIR,
      SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT);
    SecureTestUtil.enableSecurity(conf);
    // add SnapshotScannerHDFSAclController coprocessor
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      conf.get(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY) + ","
          + SnapshotScannerHDFSAclController.class.getName());
    // set hfile cleaner plugin
    conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
      SnapshotScannerHDFSAclCleaner.class.getName());

    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getAdmin();
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    fs = rootDir.getFileSystem(conf);
    unGrantUser = User.createUserForTesting(conf, UN_GRANT_USER, new String[] {});
    helper = new SnapshotScannerHDFSAclHelper(conf, admin.getConnection());

    // set hbase directory permission
    FsPermission commonDirectoryPermission =
        new FsPermission(conf.get(SnapshotScannerHDFSAclHelper.COMMON_DIRECTORY_PERMISSION,
          SnapshotScannerHDFSAclHelper.COMMON_DIRECTORY_PERMISSION_DEFAULT));
    Path path = rootDir;
    while (path != null) {
      fs.setPermission(path, commonDirectoryPermission);
      path = path.getParent();
    }
    // set restore directory permission
    Path restoreDir = new Path(SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT);
    if (!fs.exists(restoreDir)) {
      fs.mkdirs(restoreDir);
      fs.setPermission(restoreDir,
        new FsPermission(
            conf.get(SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_DIRECTORY_PERMISSION,
              SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_DIRECTORY_PERMISSION_DEFAULT)));
    }
    path = restoreDir.getParent();
    while (path != null) {
      fs.setPermission(path, commonDirectoryPermission);
      path = path.getParent();
    }

    SnapshotScannerHDFSAclController coprocessor = TEST_UTIL.getHBaseCluster().getMaster()
        .getMasterCoprocessorHost().findCoprocessor(SnapshotScannerHDFSAclController.class);
    TEST_UTIL.waitFor(1200000, () -> coprocessor.checkInitialized("check initialized"));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGrantGlobal() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace1 = name.getMethodName();
    String namespace2 = namespace1 + "2";
    String namespace3 = namespace1 + "3";
    TableName table1 = TableName.valueOf(namespace1, "t1");
    TableName table12 = TableName.valueOf(namespace1, "t2");
    TableName table21 = TableName.valueOf(namespace2, "t21");
    TableName table3 = TableName.valueOf(namespace3, "t3");
    TableName table31 = TableName.valueOf(namespace3, "t31");
    String snapshot1 = namespace1 + "t1";
    String snapshot12 = namespace1 + "t12";
    String snapshot2 = namespace1 + "t2";
    String snapshot21 = namespace2 + "t21";
    String snapshot3 = namespace1 + "t3";
    String snapshot31 = namespace1 + "t31";

    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);

    // case 1: grant G(R) -> grant G(W) -> grant G(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    admin.grant(
      new UserPermission(grantUserName, Permission.newBuilder().withActions(WRITE).build()), true);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, WRITE);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, -1);
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    admin.snapshot(snapshot12, table1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot12, 6);

    // case 2: grant G(R),N(R) -> G(W)
    admin.grant(new UserPermission(grantUserName,
        Permission.newBuilder(namespace1).withActions(READ).build()),
      false);
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, WRITE);
    // table in ns1
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table12);
    admin.snapshot(snapshot2, table12);
    // table in ns2
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table21);
    admin.snapshot(snapshot21, table21);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot21, -1);

    // case 3: grant G(R),T(R) -> G(W)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table3);
    admin.snapshot(snapshot3, table3);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table3, READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, WRITE);
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table31);
    admin.snapshot(snapshot31, table31);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot3, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot31, -1);
  }

  @Test
  public void testGrantNamespace() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    TableName table2 = TableName.valueOf(namespace, "t2");
    TableName table3 = TableName.valueOf(namespace, "t3");
    String snapshot = namespace + "t1";
    String snapshot2 = namespace + "t2";
    String snapshot3 = namespace + "t3";

    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    admin.snapshot(snapshot, table);

    // case 1: grant N(R) -> grant N(W) -> grant N(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table3);
    admin.snapshot(snapshot3, table3);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot3, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, unGrantUser, snapshot, -1);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, WRITE);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);

    // case 2: grant T(R) -> N(W)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, WRITE);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table2);
    admin.snapshot(snapshot2, table2);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, -1);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, WRITE);

    // case 3: grant G(R) -> N(W)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, WRITE);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot3, 6);
  }

  @Test
  public void testGrantTable() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    TableName table2 = TableName.valueOf(namespace, "t2");
    String snapshot = namespace + "t1";
    String snapshot2 = namespace + "t1-2";
    String snapshot3 = namespace + "t2";

    try (Table t = TestHDFSAclHelper.createTable(TEST_UTIL, table)) {
      TestHDFSAclHelper.put(t);
      admin.snapshot(snapshot, table);
      // table owner can scan table snapshot
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL,
        User.createUserForTesting(conf, "owner", new String[] {}), snapshot, 6);
      // case 1: grant table family(R)
      SecureTestUtil.grantOnTable(TEST_UTIL, grantUserName, table, TestHDFSAclHelper.COLUMN1, null,
        READ);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);
      // case 2: grant T(R)
      TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
      TestHDFSAclHelper.put2(t);
      admin.snapshot(snapshot2, table);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 10);
    }
    // create t2 and snapshot
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table2);
    admin.snapshot(snapshot3, table2);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot3, -1);

    // case 3: grant T(R) -> grant T(W) with merging existing permissions
    TEST_UTIL.getAdmin().grant(
      new UserPermission(grantUserName, Permission.newBuilder(table).withActions(WRITE).build()),
      true);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);

    // case 4: grant T(R) -> grant T(W) without merging existing permissions
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, WRITE);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, -1);
  }

  @Test
  public void testRevokeGlobal() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace, "t1");
    TableName table2 = TableName.valueOf(namespace, "t2");
    TableName table3 = TableName.valueOf(namespace, "t3");
    String snapshot1 = namespace + "t1";
    String snapshot2 = namespace + "t2";
    String snapshot3 = namespace + "t3";

    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);
    // case 1: grant G(R) -> revoke G(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    SecureTestUtil.revokeGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, -1);

    // case 2: grant G(R), grant N(R), grant T(R) -> revoke G(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table1, READ);
    SecureTestUtil.revokeGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table2);
    admin.snapshot(snapshot2, table2);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 6);
    SecureTestUtil.revokeFromNamespace(TEST_UTIL, grantUserName, namespace, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, -1);

    // case 3: grant G(R), grant T(R) -> revoke G(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    SecureTestUtil.revokeGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, -1);
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table3);
    admin.snapshot(snapshot3, table3);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot3, -1);
  }

  @Test
  public void testRevokeNamespace() throws Exception {
    String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace, "t1");
    TableName table2 = TableName.valueOf(namespace, "t2");
    TableName table3 = TableName.valueOf(namespace, "t3");
    TableName table4 = TableName.valueOf(namespace, "t4");
    String snapshot1 = namespace + "t1";
    String snapshot2 = namespace + "t2";
    String snapshot3 = namespace + "t3";
    String snapshot4 = namespace + "t4";

    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);

    // case 1: grant N(R) -> revoke N(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    admin.revoke(new UserPermission(grantUserName, Permission.newBuilder(namespace).build()));
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table3);
    admin.snapshot(snapshot3, table3);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot3, -1);

    // case 2: grant N(R), grant G(R) -> revoke N(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    admin.revoke(new UserPermission(grantUserName, Permission.newBuilder(namespace).build()));
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table4);
    admin.snapshot(snapshot4, table4);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot4, 6);
    SecureTestUtil.revokeGlobal(TEST_UTIL, grantUserName, READ);

    // case 3: grant N(R), grant T(R) -> revoke N(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table1, READ);
    SecureTestUtil.revokeFromNamespace(TEST_UTIL, grantUserName, namespace, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    TestHDFSAclHelper.createTable(TEST_UTIL, table2);
    admin.snapshot(snapshot2, table2);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, -1);
  }

  @Test
  public void testRevokeTable() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";

    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    admin.snapshot(snapshot, table);

    // case 1: grant T(R) -> revoke table family
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    SecureTestUtil.revokeFromTable(TEST_UTIL, grantUserName, table, TestHDFSAclHelper.COLUMN1, null,
      READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);

    // case 2: grant T(R) -> revoke T(R)
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    admin.revoke(new UserPermission(grantUserName, Permission.newBuilder(table).build()));
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);

    // case 3: grant T(R), grant N(R) -> revoke T(R)
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    admin.revoke(new UserPermission(grantUserName, Permission.newBuilder(table).build()));
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    SecureTestUtil.revokeFromNamespace(TEST_UTIL, grantUserName, namespace, READ);

    // case 4: grant T(R), grant G(R) -> revoke T(R)
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    admin.revoke(new UserPermission(grantUserName, Permission.newBuilder(table).build()));
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    SecureTestUtil.revokeGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);
  }

  @Test
  public void testTruncateTable() throws Exception {
    String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String grantUserName2 = grantUserName + "2";
    User grantUser2 = User.createUserForTesting(conf, grantUserName2, new String[] {});

    String namespace = name.getMethodName();
    TableName tableName = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";
    String snapshot2 = namespace + "t1-2";
    try (Table t = TestHDFSAclHelper.createTable(TEST_UTIL, tableName)) {
      TestHDFSAclHelper.put(t);
      // snapshot
      admin.snapshot(snapshot, tableName);
      // grant user2 namespace permission
      SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName2, namespace, READ);
      // grant user table permission
      TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, tableName, READ);
      // truncate table
      admin.disableTable(tableName);
      admin.truncateTable(tableName, true);
      TestHDFSAclHelper.put2(t);
      // snapshot
      admin.snapshot(snapshot2, tableName);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser2, snapshot, 6);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 9);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser2, snapshot2, 9);
    }
  }

  @Test
  public void testRestoreSnapshot() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";
    String snapshot2 = namespace + "t1-2";
    String snapshot3 = namespace + "t1-3";

    try (Table t = TestHDFSAclHelper.createTable(TEST_UTIL, table)) {
      TestHDFSAclHelper.put(t);
      // grant t1, snapshot
      TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
      admin.snapshot(snapshot, table);
      // delete
      admin.disableTable(table);
      admin.deleteTable(table);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);

      // restore snapshot and restore acl
      admin.restoreSnapshot(snapshot, true, true);
      TestHDFSAclHelper.put2(t);
      // snapshot
      admin.snapshot(snapshot2, table);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 10);

      // delete
      admin.disableTable(table);
      admin.deleteTable(table);
      // restore snapshot and skip restore acl
      admin.restoreSnapshot(snapshot);
      admin.snapshot(snapshot3, table);

      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, -1);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot3, -1);
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    String namespace = name.getMethodName();
    String grantUserName1 = namespace + "1";
    String grantUserName2 = namespace + "2";
    String grantUserName3 = namespace + "3";
    User grantUser1 = User.createUserForTesting(conf, grantUserName1, new String[] {});
    User grantUser2 = User.createUserForTesting(conf, grantUserName2, new String[] {});
    User grantUser3 = User.createUserForTesting(conf, grantUserName3, new String[] {});

    TableName tableName1 = TableName.valueOf(namespace, "t1");
    TableName tableName2 = TableName.valueOf(namespace, "t2");
    String snapshot1 = namespace + "t1";
    String snapshot2 = namespace + "t2";
    try (Table t = TestHDFSAclHelper.createTable(TEST_UTIL, tableName1);
        Table t2 = TestHDFSAclHelper.createTable(TEST_UTIL, tableName2)) {
      TestHDFSAclHelper.put(t);
      TestHDFSAclHelper.put(t2);
      // snapshot
      admin.snapshot(snapshot1, tableName1);
      admin.snapshot(snapshot2, tableName2);
      // grant user table permission
      TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName1, tableName1, READ);
      TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName2, tableName2, READ);
      SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName3, namespace, READ);
      // delete table
      admin.disableTable(tableName1);
      admin.deleteTable(tableName1);
      // grantUser2 and grantUser3 should have data/ns acl
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser1, snapshot1, -1);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser2, snapshot2, 6);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser3, snapshot2, 6);
    }
  }

  @Test
  public void testDeleteNamespace() throws Exception {
    String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace = name.getMethodName();
    TableName tableName = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";
    try (Table t = TestHDFSAclHelper.createTable(TEST_UTIL, tableName)) {
      TestHDFSAclHelper.put(t);
      // snapshot
      admin.snapshot(snapshot, tableName);
      // grant user2 namespace permission
      SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
      // truncate table
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      // snapshot
      admin.deleteNamespace(namespace);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    }
  }

  @Test
  public void testCleanArchiveTableDir() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";

    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    admin.snapshot(snapshot, table);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);

    // HFileCleaner will not delete archive table directory even if it's a empty directory
    HFileCleaner cleaner = TEST_UTIL.getHBaseCluster().getMaster().getHFileCleaner();
    cleaner.choreForTesting();
    Path archiveTableDir = HFileArchiveUtil.getTableArchivePath(rootDir, table);
    assertTrue(fs.exists(archiveTableDir));

    // Check SnapshotScannerHDFSAclCleaner method
    assertTrue(SnapshotScannerHDFSAclCleaner.isArchiveTableDir(archiveTableDir));
    assertTrue(SnapshotScannerHDFSAclCleaner.isArchiveNamespaceDir(archiveTableDir.getParent()));
    assertTrue(
      SnapshotScannerHDFSAclCleaner.isArchiveDataDir(archiveTableDir.getParent().getParent()));
    assertFalse(SnapshotScannerHDFSAclCleaner
        .isArchiveDataDir(archiveTableDir.getParent().getParent().getParent()));
  }

  @Test
  public void testGrantMobTable() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";

    try (Table t = TestHDFSAclHelper.createMobTable(TEST_UTIL, table)) {
      TestHDFSAclHelper.put(t);
      admin.snapshot(snapshot, table);
      TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    }
  }

  @Test
  public void testModifyTable() throws Exception {
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";
    TableName table2 = TableName.valueOf(namespace, "t2");

    String tableUserName = name.getMethodName();
    User tableUser = User.createUserForTesting(conf, tableUserName, new String[] {});
    String tableUserName2 = tableUserName + "2";
    User tableUser2 = User.createUserForTesting(conf, tableUserName2, new String[] {});
    String tableUserName3 = tableUserName + "3";
    User tableUser3 = User.createUserForTesting(conf, tableUserName3, new String[] {});
    String nsUserName = tableUserName + "-ns";
    User nsUser = User.createUserForTesting(conf, nsUserName, new String[] {});
    String globalUserName = tableUserName + "-global";
    User globalUser = User.createUserForTesting(conf, globalUserName, new String[] {});
    String globalUserName2 = tableUserName + "-global-2";
    User globalUser2 = User.createUserForTesting(conf, globalUserName2, new String[] {});

    SecureTestUtil.grantGlobal(TEST_UTIL, globalUserName, READ);
    TestHDFSAclHelper.createNamespace(TEST_UTIL, namespace);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsUserName, namespace, READ);
    TableDescriptor td = TestHDFSAclHelper.createUserScanSnapshotDisabledTable(TEST_UTIL, table);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantGlobal(TEST_UTIL, globalUserName2, READ);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, tableUserName, table, READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableUserName2, table, TestHDFSAclHelper.COLUMN1, null,
      READ);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, tableUserName3, table, WRITE);

    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser2, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser3, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, nsUser, snapshot, -1);
    // Global permission is set before table is created, the acl is inherited
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, globalUser, snapshot, 6);
    // Global permission is set after table is created, the table dir acl is skip
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, globalUser2, snapshot, -1);

    // enable user scan snapshot
    admin.modifyTable(TableDescriptorBuilder.newBuilder(td)
        .setValue(SnapshotScannerHDFSAclHelper.USER_SCAN_SNAPSHOT_ENABLE, "true").build());
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser, snapshot, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser2, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser3, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, nsUser, snapshot, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, globalUser, snapshot, 6);
    // disable user scan snapshot
    SecureTestUtil.grantOnNamespace(TEST_UTIL, tableUserName2, namespace, READ);
    TestHDFSAclHelper.createTable(TEST_UTIL, table2);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, tableUserName3, table2, READ);
    admin.modifyTable(TableDescriptorBuilder.newBuilder(td)
        .setValue(SnapshotScannerHDFSAclHelper.USER_SCAN_SNAPSHOT_ENABLE, "false").build());
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser2, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser3, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, nsUser, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, globalUser, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, globalUser2, snapshot, -1);
    List<Path> namespaceRootPaths = helper.getNamespaceRootPaths(namespace);
    List<Path> tableRootPaths = helper.getTableRootPaths(table, false);
    // check access
    for (Path path : tableRootPaths) {
      checkUserAclEntry(path, tableUserName, false, false);
      checkUserAclEntry(path, tableUserName2, false, false);
      checkUserAclEntry(path, tableUserName3, false, false);
      checkUserAclEntry(path, nsUserName, false, false);
      checkUserAclEntry(path, globalUserName, false, false);
      checkUserAclEntry(path, globalUserName2, false, false);
    }
    for (Path path : namespaceRootPaths) {
      checkUserAclEntry(path, tableUserName, false, false);
      checkUserAclEntry(path, tableUserName2, true, true);
      checkUserAclEntry(path, tableUserName3, true, false);
      checkUserAclEntry(path, nsUserName, true, true);
      checkUserAclEntry(path, globalUserName, true, true);
      checkUserAclEntry(path, globalUserName2, true, true);
    }
  }

  private void checkUserAclEntry(Path path, String userName, boolean requireAccessAcl,
      boolean requireDefaultAcl) throws IOException {
    boolean accessAclEntry = false;
    boolean defaultAclEntry = false;
    if (fs.exists(path)) {
      for (AclEntry aclEntry : fs.getAclStatus(path).getEntries()) {
        String user = aclEntry.getName();
        if (user != null && user.equals(userName)) {
          if (aclEntry.getScope() == AclEntryScope.DEFAULT) {
            defaultAclEntry = true;
          } else if (aclEntry.getScope() == AclEntryScope.ACCESS) {
            accessAclEntry = true;
          }
        }
      }
    }
    String message = "require user: " + userName + ", path: " + path.toString() + " acl";
    Assert.assertEquals(message, requireAccessAcl, accessAclEntry);
    Assert.assertEquals(message, requireDefaultAcl, defaultAclEntry);
  }
}

final class TestHDFSAclHelper {
  private static final Logger LOG = LoggerFactory.getLogger(TestHDFSAclHelper.class);

  private TestHDFSAclHelper() {
  }

  static void grantOnTable(HBaseTestingUtility util, String user, TableName tableName,
      Permission.Action... actions) throws Exception {
    SecureTestUtil.grantOnTable(util, user, tableName, null, null, actions);
  }

  static void createNamespace(HBaseTestingUtility util, String namespace) throws IOException {
    if (Arrays.stream(util.getAdmin().listNamespaceDescriptors())
        .noneMatch(ns -> ns.getName().equals(namespace))) {
      NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
      util.getAdmin().createNamespace(namespaceDescriptor);
    }
  }

  static Table createTable(HBaseTestingUtility util, TableName tableName) throws IOException {
    createNamespace(util, tableName.getNamespaceAsString());
    TableDescriptor td = getTableDescriptorBuilder(util, tableName)
        .setValue(SnapshotScannerHDFSAclHelper.USER_SCAN_SNAPSHOT_ENABLE, "true").build();
    byte[][] splits = new byte[][] { Bytes.toBytes("2"), Bytes.toBytes("4") };
    return util.createTable(td, splits);
  }

  static Table createMobTable(HBaseTestingUtility util, TableName tableName) throws IOException {
    createNamespace(util, tableName.getNamespaceAsString());
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN1).setMobEnabled(true)
            .setMobThreshold(0).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN2).setMobEnabled(true)
            .setMobThreshold(0).build())
        .setOwner(User.createUserForTesting(util.getConfiguration(), "owner", new String[] {}))
        .setValue(SnapshotScannerHDFSAclHelper.USER_SCAN_SNAPSHOT_ENABLE, "true").build();
    byte[][] splits = new byte[][] { Bytes.toBytes("2"), Bytes.toBytes("4") };
    return util.createTable(td, splits);
  }

  static TableDescriptor createUserScanSnapshotDisabledTable(HBaseTestingUtility util,
      TableName tableName) throws IOException {
    createNamespace(util, tableName.getNamespaceAsString());
    TableDescriptor td = getTableDescriptorBuilder(util, tableName).build();
    byte[][] splits = new byte[][] { Bytes.toBytes("2"), Bytes.toBytes("4") };
    try (Table t = util.createTable(td, splits)) {
      put(t);
    }
    return td;
  }

  private static TableDescriptorBuilder getTableDescriptorBuilder(HBaseTestingUtility util,
      TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN1).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN2).build())
        .setOwner(User.createUserForTesting(util.getConfiguration(), "owner", new String[] {}));
  }

  static void createTableAndPut(HBaseTestingUtility util, TableName tableNam) throws IOException {
    try (Table t = createTable(util, tableNam)) {
      put(t);
    }
  }

  static final byte[] COLUMN1 = Bytes.toBytes("A");
  static final byte[] COLUMN2 = Bytes.toBytes("B");

  static void put(Table hTable) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(COLUMN1, null, Bytes.toBytes(i));
      put.addColumn(COLUMN2, null, Bytes.toBytes(i + 1));
      puts.add(put);
    }
    hTable.put(puts);
  }

  static void put2(Table hTable) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        continue;
      }
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(COLUMN1, null, Bytes.toBytes(i + 2));
      put.addColumn(COLUMN2, null, Bytes.toBytes(i + 3));
      puts.add(put);
    }
    hTable.put(puts);
  }

  /**
   * Check if user is able to read expected rows from the specific snapshot
   * @param user the specific user
   * @param snapshot the snapshot to be scanned
   * @param expectedRowCount expected row count read from snapshot, -1 if expects
   *          AccessControlException
   * @throws IOException user scan snapshot error
   * @throws InterruptedException user scan snapshot error
   */
  static void canUserScanSnapshot(HBaseTestingUtility util, User user, String snapshot,
      int expectedRowCount) throws IOException, InterruptedException {
    PrivilegedExceptionAction<Void> action =
        getScanSnapshotAction(util.getConfiguration(), snapshot, expectedRowCount);
    user.runAs(action);
  }

  private static PrivilegedExceptionAction<Void> getScanSnapshotAction(Configuration conf,
      String snapshotName, long expectedRowCount) {
    return () -> {
      try {
        Path restoreDir = new Path(SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT);
        Scan scan = new Scan();
        TableSnapshotScanner scanner =
            new TableSnapshotScanner(conf, restoreDir, snapshotName, scan);
        int rowCount = 0;
        while (true) {
          Result result = scanner.next();
          if (result == null) {
            break;
          }
          rowCount++;
        }
        scanner.close();
        assertEquals(expectedRowCount, rowCount);
      } catch (Exception e) {
        LOG.debug("Scan snapshot error, snapshot {}", snapshotName, e);
        assertEquals(expectedRowCount, -1);
      }
      return null;
    };
  }
}