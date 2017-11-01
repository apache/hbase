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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlConstants;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.UUID;

@Category(MediumTests.class)
public class TestSnapshotWithAcl extends SecureTestUtil {

  public TableName TEST_TABLE = TableName.valueOf(UUID.randomUUID().toString());

  private static final int ROW_COUNT = 30000;

  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static byte[] TEST_QUALIFIER = Bytes.toBytes("cq");
  private static byte[] TEST_ROW = Bytes.toBytes(0);
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;
  private static HBaseAdmin admin = null;

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
      HTable t = new HTable(conf, tableName);
      try {
        t.get(g);
      } finally {
        t.close();
      }
      return null;
    }
  };

  static class AccessWriteAction implements AccessTestAction {
    private TableName tableName;

    public AccessWriteAction(TableName tableName) {
      this.tableName = tableName;
    }

    @Override
    public Object run() throws Exception {
      Put p = new Put(TEST_ROW);
      p.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(1));
      HTable t = new HTable(conf, tableName);
      try {
        t.put(p);
      } finally {
        t.close();
      }
      return null;
    }
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Enable security
    enableSecurity(conf);
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);
    // Enable EXEC permission checking
    conf.setBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY, true);
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.waitUntilAllRegionsAssigned(AccessControlLists.ACL_TABLE_NAME);
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
    admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    hcd.setMaxVersions(100);
    htd.addFamily(hcd);
    htd.setOwner(USER_OWNER);
    admin.createTable(htd, new byte[][] { Bytes.toBytes("s") });
    TEST_UTIL.waitTableEnabled(TEST_TABLE);

    grantOnTable(TEST_UTIL, USER_RW.getShortName(), TEST_TABLE, TEST_FAMILY, null,
      Permission.Action.READ, Permission.Action.WRITE);

    grantOnTable(TEST_UTIL, USER_RO.getShortName(), TEST_TABLE, TEST_FAMILY, null,
      Permission.Action.READ);
  }

  private void loadData() throws IOException {
    HTable hTable = new HTable(conf, TEST_TABLE);
    try {
      for (int i = 0; i < ROW_COUNT; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
        hTable.put(put);
      }
      hTable.flushCommits();
    } finally {
      hTable.close();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void verifyRows(TableName tableName) throws IOException {
    HTable hTable = new HTable(conf, tableName);
    try {
      Scan scan = new Scan();
      ResultScanner scanner = hTable.getScanner(scan);
      Result result;
      int rowCount = 0;
      while ((result = scanner.next()) != null) {
        byte[] value = result.getValue(TEST_FAMILY, TEST_QUALIFIER);
        Assert.assertArrayEquals(value, Bytes.toBytes(rowCount++));
      }
      Assert.assertEquals(rowCount, ROW_COUNT);
    } finally {
      hTable.close();
    }
  }

  @Test
  public void testRestoreSnapshot() throws Exception {
    verifyAllowed(new AccessReadAction(TEST_TABLE), USER_OWNER, USER_RO, USER_RW);
    verifyDenied(new AccessReadAction(TEST_TABLE), USER_NONE);
    verifyAllowed(new AccessWriteAction(TEST_TABLE), USER_OWNER, USER_RW);
    verifyDenied(new AccessWriteAction(TEST_TABLE), USER_RO, USER_NONE);

    loadData();
    verifyRows(TEST_TABLE);

    String snapshotName1 = UUID.randomUUID().toString();
    admin.snapshot(snapshotName1, TEST_TABLE);

    // clone snapshot with restoreAcl true.
    TableName tableName1 = TableName.valueOf(UUID.randomUUID().toString());
    admin.cloneSnapshot(snapshotName1, tableName1, true);
    verifyRows(tableName1);
    verifyAllowed(new AccessReadAction(tableName1), USER_OWNER, USER_RO, USER_RW);
    verifyDenied(new AccessReadAction(tableName1), USER_NONE);
    verifyAllowed(new AccessWriteAction(tableName1), USER_OWNER, USER_RW);
    verifyDenied(new AccessWriteAction(tableName1), USER_RO, USER_NONE);

    // clone snapshot with restoreAcl false.
    TableName tableName2 = TableName.valueOf(UUID.randomUUID().toString());
    admin.cloneSnapshot(snapshotName1, tableName2, false);
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
    admin.disableTable(TEST_TABLE);
    admin.restoreSnapshot(snapshotName1, false, false);
    admin.enableTable(TEST_TABLE);
    verifyAllowed(new AccessReadAction(TEST_TABLE), USER_OWNER, USER_RW);
    verifyDenied(new AccessReadAction(TEST_TABLE), USER_RO, USER_NONE);
    verifyAllowed(new AccessWriteAction(TEST_TABLE), USER_OWNER, USER_RW);
    verifyDenied(new AccessWriteAction(TEST_TABLE), USER_RO, USER_NONE);

    // restore snapshot with restoreAcl true.
    admin.disableTable(TEST_TABLE);
    admin.restoreSnapshot(snapshotName1, false, true);
    admin.enableTable(TEST_TABLE);
    verifyAllowed(new AccessReadAction(TEST_TABLE), USER_OWNER, USER_RO, USER_RW);
    verifyDenied(new AccessReadAction(TEST_TABLE), USER_NONE);
    verifyAllowed(new AccessWriteAction(TEST_TABLE), USER_OWNER, USER_RW);
    verifyDenied(new AccessWriteAction(TEST_TABLE), USER_RO, USER_NONE);
  }
}
