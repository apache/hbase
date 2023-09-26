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
import static org.apache.hadoop.hbase.security.access.TestSnapshotScannerHDFSAclController.checkUserAclEntry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Separated from {@link TestSnapshotScannerHDFSAclController}. Uses facility from that class.
 * @see TestSnapshotScannerHDFSAclController
 */
@Category({ SecurityTests.class, LargeTests.class })
public class TestSnapshotScannerHDFSAclController3 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotScannerHDFSAclController3.class);

  @Rule
  public TestName name = new TestName();
  private static final Logger LOG =
    LoggerFactory.getLogger(TestSnapshotScannerHDFSAclController3.class);

  private static final String UN_GRANT_USER = "un_grant_user";
  private static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static Admin admin = null;
  private static SnapshotScannerHDFSAclHelper helper;
  private static Table aclTable;
  private static FileSystem FS;

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
    SnapshotScannerHDFSAclController coprocessor = TEST_UTIL.getHBaseCluster().getMaster()
      .getMasterCoprocessorHost().findCoprocessor(SnapshotScannerHDFSAclController.class);
    TEST_UTIL.waitFor(30000, () -> coprocessor.checkInitialized("check initialized"));
    TEST_UTIL.waitTableAvailable(PermissionStorage.ACL_TABLE_NAME);

    admin = TEST_UTIL.getAdmin();
    Path rootDir = TEST_UTIL.getDefaultRootDirPath();
    FS = rootDir.getFileSystem(conf);
    User unGrantUser = User.createUserForTesting(conf, UN_GRANT_USER, new String[] {});
    helper = new SnapshotScannerHDFSAclHelper(conf, admin.getConnection());

    // set hbase directory permission
    FsPermission commonDirectoryPermission =
      new FsPermission(conf.get(SnapshotScannerHDFSAclHelper.COMMON_DIRECTORY_PERMISSION,
        SnapshotScannerHDFSAclHelper.COMMON_DIRECTORY_PERMISSION_DEFAULT));
    Path path = rootDir;
    while (path != null) {
      FS.setPermission(path, commonDirectoryPermission);
      path = path.getParent();
    }
    // set restore directory permission
    Path restoreDir = new Path(SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT);
    if (!FS.exists(restoreDir)) {
      FS.mkdirs(restoreDir);
      FS.setPermission(restoreDir,
        new FsPermission(
          conf.get(SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_DIRECTORY_PERMISSION,
            SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_DIRECTORY_PERMISSION_DEFAULT)));
    }
    path = restoreDir.getParent();
    while (path != null) {
      FS.setPermission(path, commonDirectoryPermission);
      path = path.getParent();
    }
    aclTable = admin.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testModifyTable() throws Exception {
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, name.getMethodName());
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
    snapshotAndWait(snapshot, table);
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
    checkUserAclEntry(FS, helper.getGlobalRootPaths(), globalUserName, true, true);
    assertTrue(hasUserNamespaceHdfsAcl(aclTable, nsUserName, namespace));
    checkUserAclEntry(FS, helper.getNamespaceRootPaths(namespace), nsUserName, true, true);
    assertTrue(hasUserTableHdfsAcl(aclTable, tableUserName, table));
    checkUserAclEntry(FS, helper.getTableRootPaths(table, false), tableUserName, true, true);
    for (String user : new String[] { tableUserName2, tableUserName3 }) {
      assertFalse(hasUserTableHdfsAcl(aclTable, user, table));
      checkUserAclEntry(FS, helper.getTableRootPaths(table, false), user, false, false);
    }
    deleteTable(table);
  }

  private static void deleteTable(TableName tableName) {
    try {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    } catch (IOException e) {
      LOG.warn("Failed to delete table: {}", tableName);
    }
  }

  private void snapshotAndWait(final String snapShotName, final TableName tableName)
    throws Exception {
    admin.snapshot(snapShotName, tableName);
    LOG.info("Sleep for three seconds, waiting for HDFS Acl setup");
    Threads.sleep(3000);
  }

}
