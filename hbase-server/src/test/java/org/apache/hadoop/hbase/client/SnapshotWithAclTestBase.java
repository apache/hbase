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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlConstants;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class SnapshotWithAclTestBase extends SecureTestUtil {

  private TableName TEST_TABLE = TableName.valueOf(TEST_UTIL.getRandomUUID().toString());

  private static final int ROW_COUNT = 30000;

  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static byte[] TEST_QUALIFIER = Bytes.toBytes("cq");
  private static byte[] TEST_ROW = Bytes.toBytes(0);

  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // user is table owner. will have all permissions on table
  private static User USER_OWNER;
  // user with rw permissions on column family.
  private static User USER_RW;
  // user with read-only permissions
  private static User USER_RO;
  // user with none permissions
  private static User USER_NONE;

  static class AccessReadAction implements AccessTestAction {

    private TableName tableName;

    public AccessReadAction(TableName tableName) {
      this.tableName = tableName;
    }

    @Override
    public Object run() throws Exception {
      Get g = new Get(TEST_ROW);
      g.addFamily(TEST_FAMILY);
      try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Table t = conn.getTable(tableName)) {
        t.get(g);
      }
      return null;
    }
  }

  static class AccessWriteAction implements AccessTestAction {
    private TableName tableName;

    public AccessWriteAction(TableName tableName) {
      this.tableName = tableName;
    }

    @Override
    public Object run() throws Exception {
      Put p = new Put(TEST_ROW);
      p.addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(0));
      try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Table t = conn.getTable(tableName)) {
        t.put(p);
      }
      return null;
    }
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Enable security
    enableSecurity(conf);
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);
    // Enable EXEC permission checking
    conf.setBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY, true);
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.waitUntilAllRegionsAssigned(PermissionStorage.ACL_TABLE_NAME);
    MasterCoprocessorHost cpHost =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);

    USER_OWNER = User.createUserForTesting(conf, "owner", new String[0]);
    USER_RW = User.createUserForTesting(conf, "rwuser", new String[0]);
    USER_RO = User.createUserForTesting(conf, "rouser", new String[0]);
    USER_NONE = User.createUserForTesting(conf, "usernone", new String[0]);
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.createTable(TableDescriptorBuilder.newBuilder(TEST_TABLE)
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(TEST_FAMILY).setMaxVersions(100).build())
      .setOwner(USER_OWNER).build(), new byte[][] { Bytes.toBytes("s") });
    TEST_UTIL.waitTableEnabled(TEST_TABLE);

    grantOnTable(TEST_UTIL, USER_RW.getShortName(), TEST_TABLE, TEST_FAMILY, null,
      Permission.Action.READ, Permission.Action.WRITE);

    grantOnTable(TEST_UTIL, USER_RO.getShortName(), TEST_TABLE, TEST_FAMILY, null,
      Permission.Action.READ);
  }

  private void loadData() throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      try (Table t = conn.getTable(TEST_TABLE)) {
        for (int i = 0; i < ROW_COUNT; i++) {
          Put put = new Put(Bytes.toBytes(i));
          put.addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
          t.put(put);
        }
      }
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void verifyRows(TableName tableName) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
      Table t = conn.getTable(tableName); ResultScanner scanner = t.getScanner(new Scan())) {
      Result result;
      int rowCount = 0;
      while ((result = scanner.next()) != null) {
        byte[] value = result.getValue(TEST_FAMILY, TEST_QUALIFIER);
        Assert.assertArrayEquals(value, Bytes.toBytes(rowCount++));
      }
      Assert.assertEquals(ROW_COUNT, rowCount);
    }
  }

  protected abstract void snapshot(String snapshotName, TableName tableName) throws Exception;

  protected abstract void cloneSnapshot(String snapshotName, TableName tableName,
      boolean restoreAcl) throws Exception;

  protected abstract void restoreSnapshot(String snapshotName, boolean restoreAcl) throws Exception;

  @Test
  public void testRestoreSnapshot() throws Exception {
    verifyAllowed(new AccessReadAction(TEST_TABLE), USER_OWNER, USER_RO, USER_RW);
    verifyDenied(new AccessReadAction(TEST_TABLE), USER_NONE);
    verifyAllowed(new AccessWriteAction(TEST_TABLE), USER_OWNER, USER_RW);
    verifyDenied(new AccessWriteAction(TEST_TABLE), USER_RO, USER_NONE);

    loadData();
    verifyRows(TEST_TABLE);

    String snapshotName1 = TEST_UTIL.getRandomUUID().toString();
    snapshot(snapshotName1, TEST_TABLE);

    // clone snapshot with restoreAcl true.
    TableName tableName1 = TableName.valueOf(TEST_UTIL.getRandomUUID().toString());
    cloneSnapshot(snapshotName1, tableName1, true);
    verifyRows(tableName1);
    verifyAllowed(new AccessReadAction(tableName1), USER_OWNER, USER_RO, USER_RW);
    verifyDenied(new AccessReadAction(tableName1), USER_NONE);
    verifyAllowed(new AccessWriteAction(tableName1), USER_OWNER, USER_RW);
    verifyDenied(new AccessWriteAction(tableName1), USER_RO, USER_NONE);

    // clone snapshot with restoreAcl false.
    TableName tableName2 = TableName.valueOf(TEST_UTIL.getRandomUUID().toString());
    cloneSnapshot(snapshotName1, tableName2, false);
    verifyRows(tableName2);
    verifyAllowed(new AccessReadAction(tableName2), USER_OWNER);
    verifyDenied(new AccessReadAction(tableName2), USER_NONE, USER_RO, USER_RW);
    verifyAllowed(new AccessWriteAction(tableName2), USER_OWNER);
    verifyDenied(new AccessWriteAction(tableName2), USER_RO, USER_RW, USER_NONE);

    // remove read permission for USER_RO.
    revokeFromTable(TEST_UTIL, USER_RO.getShortName(), TEST_TABLE, TEST_FAMILY, null,
      Permission.Action.READ);
    verifyAllowed(new AccessReadAction(TEST_TABLE), USER_OWNER, USER_RW);
    verifyDenied(new AccessReadAction(TEST_TABLE), USER_RO, USER_NONE);
    verifyAllowed(new AccessWriteAction(TEST_TABLE), USER_OWNER, USER_RW);
    verifyDenied(new AccessWriteAction(TEST_TABLE), USER_RO, USER_NONE);

    // restore snapshot with restoreAcl false.
    TEST_UTIL.getAdmin().disableTable(TEST_TABLE);
    restoreSnapshot(snapshotName1, false);
    TEST_UTIL.getAdmin().enableTable(TEST_TABLE);
    verifyAllowed(new AccessReadAction(TEST_TABLE), USER_OWNER, USER_RW);
    verifyDenied(new AccessReadAction(TEST_TABLE), USER_RO, USER_NONE);
    verifyAllowed(new AccessWriteAction(TEST_TABLE), USER_OWNER, USER_RW);
    verifyDenied(new AccessWriteAction(TEST_TABLE), USER_RO, USER_NONE);

    // restore snapshot with restoreAcl true.
    TEST_UTIL.getAdmin().disableTable(TEST_TABLE);
    restoreSnapshot(snapshotName1, true);
    TEST_UTIL.getAdmin().enableTable(TEST_TABLE);
    verifyAllowed(new AccessReadAction(TEST_TABLE), USER_OWNER, USER_RO, USER_RW);
    verifyDenied(new AccessReadAction(TEST_TABLE), USER_NONE);
    verifyAllowed(new AccessWriteAction(TEST_TABLE), USER_OWNER, USER_RW);
    verifyDenied(new AccessWriteAction(TEST_TABLE), USER_RO, USER_NONE);
  }


  final class AccessSnapshotAction implements AccessTestAction {
    private String snapshotName;
    private AccessSnapshotAction(String snapshotName) {
      this.snapshotName = snapshotName;
    }
    @Override
    public Object run() throws Exception {
      try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Admin admin = conn.getAdmin()) {
        admin.snapshot(this.snapshotName, TEST_TABLE);
      }
      return null;
    }
  }

  @Test
  public void testDeleteSnapshot() throws Exception {
    String testSnapshotName = HBaseCommonTestingUtility.getRandomUUID().toString();
    verifyAllowed(new AccessSnapshotAction(testSnapshotName), USER_OWNER);
    verifyDenied(new AccessSnapshotAction(HBaseCommonTestingUtility.getRandomUUID().toString()),
      USER_RO, USER_RW, USER_NONE);
    List<SnapshotDescription> snapshotDescriptions = TEST_UTIL.getAdmin().listSnapshots(
      Pattern.compile(testSnapshotName));
    Assert.assertEquals(1, snapshotDescriptions.size());
    Assert.assertEquals(USER_OWNER.getShortName(), snapshotDescriptions.get(0).getOwner());
    AccessTestAction deleteSnapshotAction = () -> {
      try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Admin admin = conn.getAdmin()) {
        admin.deleteSnapshot(testSnapshotName);
      }
      return null;
    };
    verifyDenied(deleteSnapshotAction, USER_RO, USER_RW, USER_NONE);
    verifyAllowed(deleteSnapshotAction, USER_OWNER);

    List<SnapshotDescription> snapshotsAfterDelete = TEST_UTIL.getAdmin().listSnapshots(
      Pattern.compile(testSnapshotName));
    Assert.assertEquals(0, snapshotsAfterDelete.size());
  }
}
