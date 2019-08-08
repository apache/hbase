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
import static org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclController.SnapshotScannerHDFSAclStorage.hasUserGlobalHdfsAcl;
import static org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclController.SnapshotScannerHDFSAclStorage.hasUserNamespaceHdfsAcl;
import static org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclController.SnapshotScannerHDFSAclStorage.hasUserTableHdfsAcl;
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
  private static Table aclTable;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // enable hdfs acl and set umask to 027
    conf.setBoolean("dfs.namenode.acls.enabled", true);
    conf.set("fs.permissions.umask-mode", "027");
    // enable hbase hdfs acl feature
    conf.setBoolean(SnapshotScannerHDFSAclHelper.ACL_SYNC_TO_HDFS_ENABLE, true);
    // enable secure
    conf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    conf.set(SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_TMP_DIR,
      SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT);
    SecureTestUtil.enableSecurity(conf);
    // add SnapshotScannerHDFSAclController coprocessor
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      conf.get(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY) + ","
          + SnapshotScannerHDFSAclController.class.getName());

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
    aclTable = admin.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGrantGlobal1() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot1 = namespace + "s1";
    String snapshot2 = namespace + "s2";

    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    admin.snapshot(snapshot1, table);
    // grant G(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    assertTrue(hasUserGlobalHdfsAcl(aclTable, grantUserName));
    // grant G(W) with merging existing permissions
    admin.grant(
      new UserPermission(grantUserName, Permission.newBuilder().withActions(WRITE).build()), true);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    assertTrue(hasUserGlobalHdfsAcl(aclTable, grantUserName));
    // grant G(W) without merging
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, WRITE);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, -1);
    assertFalse(hasUserGlobalHdfsAcl(aclTable, grantUserName));
    // grant G(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    // take a snapshot and ACLs are inherited automatically
    admin.snapshot(snapshot2, table);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 6);
    assertTrue(hasUserGlobalHdfsAcl(aclTable, grantUserName));
  }

  @Test
  public void testGrantGlobal2() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace1 = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace1, "t1");
    String namespace2 = namespace1 + "2";
    TableName table2 = TableName.valueOf(namespace2, "t2");
    String snapshot1 = namespace1 + "s1";
    String snapshot2 = namespace2 + "s2";

    // grant G(R), grant namespace1(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    // create table in namespace1 and snapshot
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);
    admin.grant(new UserPermission(grantUserName,
        Permission.newBuilder(namespace1).withActions(READ).build()),
      false);
    // grant G(W)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, WRITE);
    // create table in namespace2 and snapshot
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table2);
    admin.snapshot(snapshot2, table2);
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, -1);
    assertFalse(hasUserGlobalHdfsAcl(aclTable, grantUserName));
    assertTrue(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace1));
    assertFalse(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace2));
    checkUserAclEntry(helper.getGlobalRootPaths(), grantUserName, false, false);
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace1), grantUserName, true, true);
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace2), grantUserName, false, false);
  }

  @Test
  public void testGrantGlobal3() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace, "t1");
    TableName table2 = TableName.valueOf(namespace, "t2");
    String snapshot1 = namespace + "s1";
    String snapshot2 = namespace + "s2";
    // grant G(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    // grant table1(R)
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table1, READ);
    // grant G(W)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, WRITE);
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table2);
    admin.snapshot(snapshot2, table2);
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, -1);
    assertFalse(hasUserGlobalHdfsAcl(aclTable, grantUserName));
    assertFalse(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    assertTrue(hasUserTableHdfsAcl(aclTable, grantUserName, table1));
    assertFalse(hasUserTableHdfsAcl(aclTable, grantUserName, table2));
    checkUserAclEntry(helper.getGlobalRootPaths(), grantUserName, false, false);
    checkUserAclEntry(helper.getTableRootPaths(table2, false), grantUserName, false, false);
    checkUserAclEntry(helper.getTableRootPaths(table1, false), grantUserName, true, true);
  }

  @Test
  public void testGrantNamespace1() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace, "t1");
    TableName table2 = TableName.valueOf(namespace, "t2");
    String snapshot1 = namespace + "s1";
    String snapshot2 = namespace + "s2";

    // create table1 and snapshot
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);
    // grant N(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    // create table2 and snapshot, ACLs can be inherited automatically
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table2);
    admin.snapshot(snapshot2, table2);
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, unGrantUser, snapshot1, -1);
    assertTrue(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    assertFalse(hasUserTableHdfsAcl(aclTable, grantUserName, table1));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, true, true);
    // grant N(W)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, WRITE);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, -1);
    assertFalse(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, false, false);
  }

  @Test
  public void testGrantNamespace2() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace, "t1");
    String snapshot1 = namespace + "s1";

    // create table1 and snapshot
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);

    // grant N(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    // grant table1(R)
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table1, READ);
    // grant N(W)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, WRITE);
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    assertFalse(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, true, false);
    assertTrue(hasUserTableHdfsAcl(aclTable, grantUserName, table1));
    checkUserAclEntry(helper.getTableRootPaths(table1, false), grantUserName, true, true);
  }

  @Test
  public void testGrantNamespace3() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";

    // create table1 and snapshot
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    admin.snapshot(snapshot, table);
    // grant namespace(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    // grant global(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    // grant namespace(W)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, WRITE);
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    assertFalse(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, true, true);
    assertTrue(hasUserGlobalHdfsAcl(aclTable, grantUserName));
    checkUserAclEntry(helper.getGlobalRootPaths(), grantUserName, true, true);
  }

  @Test
  public void testGrantTable() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace, "t1");
    String snapshot1 = namespace + "s1";
    String snapshot2 = namespace + "s2";

    try (Table t = TestHDFSAclHelper.createTable(TEST_UTIL, table1)) {
      TestHDFSAclHelper.put(t);
      admin.snapshot(snapshot1, table1);
      // table owner can scan table snapshot
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL,
        User.createUserForTesting(conf, "owner", new String[] {}), snapshot1, 6);
      // grant table1 family(R)
      SecureTestUtil.grantOnTable(TEST_UTIL, grantUserName, table1, TestHDFSAclHelper.COLUMN1, null,
        READ);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, -1);

      // grant table1(R)
      TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table1, READ);
      TestHDFSAclHelper.put2(t);
      admin.snapshot(snapshot2, table1);
      // check scan snapshot
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 10);
      assertTrue(hasUserTableHdfsAcl(aclTable, grantUserName, table1));
      checkUserAclEntry(helper.getTableRootPaths(table1, false), grantUserName, true, true);
    }

    // grant table1(W) with merging existing permissions
    admin.grant(
      new UserPermission(grantUserName, Permission.newBuilder(table1).withActions(WRITE).build()),
      true);
    assertTrue(hasUserTableHdfsAcl(aclTable, grantUserName, table1));
    checkUserAclEntry(helper.getTableRootPaths(table1, false), grantUserName, true, true);

    // grant table1(W) without merging existing permissions
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table1, WRITE);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, -1);
    assertFalse(hasUserTableHdfsAcl(aclTable, grantUserName, table1));
    checkUserAclEntry(helper.getTableRootPaths(table1, false), grantUserName, false, false);
  }

  @Test
  public void testGrantMobTable() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "s1";

    try (Table t = TestHDFSAclHelper.createMobTable(TEST_UTIL, table)) {
      TestHDFSAclHelper.put(t);
      admin.snapshot(snapshot, table);
      TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
      assertTrue(hasUserTableHdfsAcl(aclTable, grantUserName, table));
      checkUserAclEntry(helper.getTableRootPaths(table, false), grantUserName, true, true);
    }
  }

  @Test
  public void testRevokeGlobal1() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace, "t1");
    String snapshot1 = namespace + "t1";

    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    SecureTestUtil.revokeGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, -1);
    assertFalse(hasUserGlobalHdfsAcl(aclTable, grantUserName));
    checkUserAclEntry(helper.getGlobalRootPaths(), grantUserName, false, false);
  }

  @Test
  public void testRevokeGlobal2() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace, "t1");
    String snapshot1 = namespace + "s1";
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);

    // grant G(R), grant N(R), grant T(R) -> revoke G(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table1, READ);
    SecureTestUtil.revokeGlobal(TEST_UTIL, grantUserName, READ);
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    assertFalse(hasUserGlobalHdfsAcl(aclTable, grantUserName));
    checkUserAclEntry(helper.getGlobalRootPaths(), grantUserName, false, false);
    assertTrue(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, true, true);
  }

  @Test
  public void testRevokeGlobal3() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});

    String namespace = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace, "t1");
    String snapshot1 = namespace + "t1";
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);

    // grant G(R), grant T(R) -> revoke G(R)
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table1, READ);
    SecureTestUtil.revokeGlobal(TEST_UTIL, grantUserName, READ);
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    assertFalse(hasUserGlobalHdfsAcl(aclTable, grantUserName));
    checkUserAclEntry(helper.getGlobalRootPaths(), grantUserName, false, false);
    assertFalse(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, true, false);
    assertTrue(hasUserTableHdfsAcl(aclTable, grantUserName, table1));
    checkUserAclEntry(helper.getTableRootPaths(table1, false), grantUserName, true, true);
  }

  @Test
  public void testRevokeNamespace1() throws Exception {
    String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table1 = TableName.valueOf(namespace, "t1");
    String snapshot1 = namespace + "s1";
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table1);
    admin.snapshot(snapshot1, table1);

    // revoke N(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    admin.revoke(new UserPermission(grantUserName, Permission.newBuilder(namespace).build()));
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, -1);
    assertFalse(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, false, false);

    // grant N(R), grant G(R) -> revoke N(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    admin.revoke(new UserPermission(grantUserName, Permission.newBuilder(namespace).build()));
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot1, 6);
    assertFalse(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, true, true);
  }

  @Test
  public void testRevokeNamespace2() throws Exception {
    String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "s1";
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    admin.snapshot(snapshot, table);

    // grant N(R), grant T(R) -> revoke N(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    SecureTestUtil.revokeFromNamespace(TEST_UTIL, grantUserName, namespace, READ);
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    assertFalse(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, true, false);
    assertTrue(hasUserTableHdfsAcl(aclTable, grantUserName, table));
    checkUserAclEntry(helper.getTableRootPaths(table, false), grantUserName, true, true);
  }

  @Test
  public void testRevokeTable1() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    admin.snapshot(snapshot, table);

    // grant T(R) -> revoke table family
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    SecureTestUtil.revokeFromTable(TEST_UTIL, grantUserName, table, TestHDFSAclHelper.COLUMN1, null,
      READ);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);

    // grant T(R) -> revoke T(R)
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    admin.revoke(new UserPermission(grantUserName, Permission.newBuilder(table).build()));
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);
    assertFalse(hasUserTableHdfsAcl(aclTable, grantUserName, table));
    checkUserAclEntry(helper.getTableRootPaths(table, false), grantUserName, false, false);
  }

  @Test
  public void testRevokeTable2() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    admin.snapshot(snapshot, table);

    // grant T(R), grant N(R) -> revoke T(R)
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    admin.revoke(new UserPermission(grantUserName, Permission.newBuilder(table).build()));
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    assertFalse(hasUserTableHdfsAcl(aclTable, grantUserName, table));
    checkUserAclEntry(helper.getTableRootPaths(table, false), grantUserName, true, true);
    assertTrue(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, true, true);
  }

  @Test
  public void testRevokeTable3() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    admin.snapshot(snapshot, table);

    // grant T(R), grant G(R) -> revoke T(R)
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, grantUserName, READ);
    admin.revoke(new UserPermission(grantUserName, Permission.newBuilder(table).build()));
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    assertFalse(hasUserTableHdfsAcl(aclTable, grantUserName, table));
    checkUserAclEntry(helper.getTableRootPaths(table, false), grantUserName, true, true);
    assertTrue(hasUserGlobalHdfsAcl(aclTable, grantUserName));
    checkUserAclEntry(helper.getGlobalRootPaths(), grantUserName, true, true);
  }

  @Test
  public void testTruncateTable() throws Exception {
    String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String grantUserName2 = grantUserName + "2";
    User grantUser2 = User.createUserForTesting(conf, grantUserName2, new String[] {});

    String namespace = name.getMethodName();
    TableName tableName = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "s1";
    String snapshot2 = namespace + "s2";
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
      // check scan snapshot
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser2, snapshot, 6);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 9);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser2, snapshot2, 9);
      assertTrue(hasUserNamespaceHdfsAcl(aclTable, grantUserName2, namespace));
      checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName2, true, true);
      assertTrue(hasUserTableHdfsAcl(aclTable, grantUserName, tableName));
      checkUserAclEntry(helper.getTableRootPaths(tableName, false), grantUserName, true, true);
      checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName, true, false);
    }
  }

  @Test
  public void testRestoreSnapshot() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "s1";
    String snapshot2 = namespace + "s2";
    String snapshot3 = namespace + "s3";

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
      assertTrue(hasUserTableHdfsAcl(aclTable, grantUserName, table));
      checkUserAclEntry(helper.getTableRootPaths(table, false), grantUserName, true, true);

      // delete
      admin.disableTable(table);
      admin.deleteTable(table);
      // restore snapshot and skip restore acl
      admin.restoreSnapshot(snapshot);
      admin.snapshot(snapshot3, table);

      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, -1);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot3, -1);
      assertFalse(hasUserTableHdfsAcl(aclTable, grantUserName, table));
      checkUserAclEntry(helper.getPathHelper().getDataTableDir(table), grantUserName, false, false);
      checkUserAclEntry(helper.getPathHelper().getArchiveTableDir(table), grantUserName, true,
        false);
    }
  }

  @Test
  public void testDeleteTable() throws Exception {
    String namespace = name.getMethodName();
    String grantUserName1 = namespace + "1";
    String grantUserName2 = namespace + "2";
    User grantUser1 = User.createUserForTesting(conf, grantUserName1, new String[] {});
    User grantUser2 = User.createUserForTesting(conf, grantUserName2, new String[] {});
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot1 = namespace + "t1";

    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    // snapshot
    admin.snapshot(snapshot1, table);
    // grant user table permission
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName1, table, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName2, namespace, READ);
    // delete table
    admin.disableTable(table);
    admin.deleteTable(table);
    // grantUser2 and grantUser3 should have data/ns acl
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser1, snapshot1, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser2, snapshot1, 6);
    assertTrue(hasUserNamespaceHdfsAcl(aclTable, grantUserName2, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), grantUserName2, true, true);
    assertFalse(hasUserTableHdfsAcl(aclTable, grantUserName1, table));
    checkUserAclEntry(helper.getPathHelper().getDataTableDir(table), grantUserName1, false, false);
    checkUserAclEntry(helper.getPathHelper().getMobTableDir(table), grantUserName1, false, false);
    checkUserAclEntry(helper.getPathHelper().getArchiveTableDir(table), grantUserName1, true,
      false);

    // check tmp table directory does not exist
    Path tmpTableDir = helper.getPathHelper().getTmpTableDir(table);
    assertFalse(fs.exists(tmpTableDir));
  }

  @Test
  public void testDeleteNamespace() throws Exception {
    String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    // snapshot
    admin.snapshot(snapshot, table);
    // grant namespace permission
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    // delete table
    admin.disableTable(table);
    admin.deleteTable(table);
    // delete namespace
    admin.deleteNamespace(namespace);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
    assertFalse(hasUserNamespaceHdfsAcl(aclTable, grantUserName, namespace));
    checkUserAclEntry(helper.getPathHelper().getArchiveNsDir(namespace), grantUserName, true,
      false);

    // check tmp namespace dir does not exist
    assertFalse(fs.exists(helper.getPathHelper().getTmpNsDir(namespace)));
    assertFalse(fs.exists(helper.getPathHelper().getDataNsDir(namespace)));
    // assertFalse(fs.exists(helper.getPathHelper().getMobDataNsDir(namespace)));
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
    checkUserAclEntry(helper.getTableRootPaths(table, false), grantUserName, true, true);

    // Check SnapshotScannerHDFSAclCleaner method
    assertTrue(SnapshotScannerHDFSAclCleaner.isArchiveTableDir(archiveTableDir));
    assertTrue(SnapshotScannerHDFSAclCleaner.isArchiveNamespaceDir(archiveTableDir.getParent()));
    assertTrue(
      SnapshotScannerHDFSAclCleaner.isArchiveDataDir(archiveTableDir.getParent().getParent()));
    assertFalse(SnapshotScannerHDFSAclCleaner
        .isArchiveDataDir(archiveTableDir.getParent().getParent().getParent()));
  }

  @Test
  public void testModifyTable1() throws Exception {
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";

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
        .setValue(SnapshotScannerHDFSAclHelper.ACL_SYNC_TO_HDFS_ENABLE, "true").build());
    // check scan snapshot
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser, snapshot, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser2, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser3, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, nsUser, snapshot, 6);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, globalUser, snapshot, 6);
    // check acl table storage and ACLs in dirs
    assertTrue(hasUserGlobalHdfsAcl(aclTable, globalUserName));
    checkUserAclEntry(helper.getGlobalRootPaths(), globalUserName, true, true);
    assertTrue(hasUserNamespaceHdfsAcl(aclTable, nsUserName, namespace));
    checkUserAclEntry(helper.getNamespaceRootPaths(namespace), nsUserName, true, true);
    assertTrue(hasUserTableHdfsAcl(aclTable, tableUserName, table));
    checkUserAclEntry(helper.getTableRootPaths(table, false), tableUserName, true, true);
    for (String user : new String[] { tableUserName2, tableUserName3 }) {
      assertFalse(hasUserTableHdfsAcl(aclTable, user, table));
      checkUserAclEntry(helper.getTableRootPaths(table, false), user, false, false);
    }
  }

  @Test
  public void testModifyTable2() throws Exception {
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

    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    SecureTestUtil.grantGlobal(TEST_UTIL, globalUserName, READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, globalUserName2, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsUserName, namespace, READ);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, tableUserName, table, READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableUserName2, table, TestHDFSAclHelper.COLUMN1, null,
      READ);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, tableUserName3, table, WRITE);

    SecureTestUtil.grantOnNamespace(TEST_UTIL, tableUserName2, namespace, READ);
    TestHDFSAclHelper.createTable(TEST_UTIL, table2);
    TestHDFSAclHelper.grantOnTable(TEST_UTIL, tableUserName3, table2, READ);
    // disable user scan snapshot
    admin.modifyTable(TableDescriptorBuilder.newBuilder(admin.getDescriptor(table))
        .setValue(SnapshotScannerHDFSAclHelper.ACL_SYNC_TO_HDFS_ENABLE, "false").build());
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser2, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, tableUser3, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, nsUser, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, globalUser, snapshot, -1);
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, globalUser2, snapshot, -1);
    // check access
    String[] users = new String[] { globalUserName, globalUserName2, nsUserName, tableUserName,
      tableUserName2, tableUserName3 };
    for (Path path : helper.getTableRootPaths(table, false)) {
      for (String user : users) {
        checkUserAclEntry(path, user, false, false);
      }
    }
    String[] nsUsers = new String[] { globalUserName, globalUserName2, nsUserName };
    for (Path path : helper.getNamespaceRootPaths(namespace)) {
      checkUserAclEntry(path, tableUserName, false, false);
      checkUserAclEntry(path, tableUserName2, true, true);
      checkUserAclEntry(path, tableUserName3, true, false);
      for (String user : nsUsers) {
        checkUserAclEntry(path, user, true, true);
      }
    }
    assertTrue(hasUserNamespaceHdfsAcl(aclTable, nsUserName, namespace));
    assertTrue(hasUserNamespaceHdfsAcl(aclTable, tableUserName2, namespace));
    assertFalse(hasUserTableHdfsAcl(aclTable, tableUserName, table));
  }

  @Test
  public void testRestartMaster() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, "t1");
    String snapshot = namespace + "t1";
    admin.createNamespace(NamespaceDescriptor.create(namespace).build());

    // grant N(R)
    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUserName, namespace, READ);
    // restart cluster and tmp directory will not be deleted
    TEST_UTIL.getMiniHBaseCluster().shutdown();
    TEST_UTIL.restartHBaseCluster(1);
    TEST_UTIL.waitUntilNoRegionsInTransition();

    Path tmpNsDir = helper.getPathHelper().getTmpNsDir(namespace);
    assertFalse(fs.exists(tmpNsDir));

    // create table2 and snapshot
    TestHDFSAclHelper.createTableAndPut(TEST_UTIL, table);
    admin = TEST_UTIL.getAdmin();
    aclTable = TEST_UTIL.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME);
    admin.snapshot(snapshot, table);
    // TODO fix it in another patch
    TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);
  }

  private void checkUserAclEntry(List<Path> paths, String user, boolean requireAccessAcl,
      boolean requireDefaultAcl) throws Exception {
    for (Path path : paths) {
      checkUserAclEntry(path, user, requireAccessAcl, requireDefaultAcl);
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
    assertEquals(message, requireAccessAcl, accessAclEntry);
    assertEquals(message, requireDefaultAcl, defaultAclEntry);
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
        .setValue(SnapshotScannerHDFSAclHelper.ACL_SYNC_TO_HDFS_ENABLE, "true").build();
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
        .setValue(SnapshotScannerHDFSAclHelper.ACL_SYNC_TO_HDFS_ENABLE, "true").build();
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