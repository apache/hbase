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
import static org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclController.SnapshotScannerHDFSAclStorage.hasUserTableHdfsAcl;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
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
public class TestSnapshotScannerHDFSAclController2 {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotScannerHDFSAclController2.class);
  @Rule
  public TestName name = new TestName();
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSnapshotScannerHDFSAclController2.class);

  private static final String UN_GRANT_USER = "un_grant_user";
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
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
  public void testRestoreSnapshot() throws Exception {
    final String grantUserName = name.getMethodName();
    User grantUser = User.createUserForTesting(conf, grantUserName, new String[] {});
    String namespace = name.getMethodName();
    TableName table = TableName.valueOf(namespace, name.getMethodName());
    String snapshot = namespace + "s1";
    String snapshot2 = namespace + "s2";
    String snapshot3 = namespace + "s3";

    LOG.info("Create {}", table);
    try (Table t = TestHDFSAclHelper.createTable(TEST_UTIL, table)) {
      TestHDFSAclHelper.put(t);
      // grant t1, snapshot
      LOG.info("Grant {}", table);
      TestHDFSAclHelper.grantOnTable(TEST_UTIL, grantUserName, table, READ);
      admin.snapshot(snapshot, table);
      // delete
      admin.disableTable(table);
      admin.deleteTable(table);
      LOG.info("Before scan of shapshot! {}", table);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);

      // restore snapshot and restore acl
      admin.restoreSnapshot(snapshot, true, true);
      TestHDFSAclHelper.put2(t);
      // snapshot
      admin.snapshot(snapshot2, table);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, 6);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, 10);
      assertTrue(hasUserTableHdfsAcl(aclTable, grantUserName, table));
      TestSnapshotScannerHDFSAclController.
        checkUserAclEntry(FS, helper.getTableRootPaths(table, false),
          grantUserName, true, true);

      // delete
      admin.disableTable(table);
      admin.deleteTable(table);
      // restore snapshot and skip restore acl
      admin.restoreSnapshot(snapshot);
      admin.snapshot(snapshot3, table);

      LOG.info("CHECK");
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot, -1);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot2, -1);
      TestHDFSAclHelper.canUserScanSnapshot(TEST_UTIL, grantUser, snapshot3, -1);
      assertFalse(hasUserTableHdfsAcl(aclTable, grantUserName, table));
      TestSnapshotScannerHDFSAclController.
        checkUserAclEntry(FS, helper.getPathHelper().getDataTableDir(table),
          grantUserName, false, false);
      TestSnapshotScannerHDFSAclController.
        checkUserAclEntry(FS, helper.getPathHelper().getArchiveTableDir(table),
          grantUserName, true, false);
    }
  }
}
