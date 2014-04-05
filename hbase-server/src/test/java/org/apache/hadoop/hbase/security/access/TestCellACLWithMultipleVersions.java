/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TestTableName;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
@Category(MediumTests.class)
public class TestCellACLWithMultipleVersions extends SecureTestUtil {
  private static final Log LOG = LogFactory.getLog(TestCellACLWithMultipleVersions.class);

  static {
    Logger.getLogger(AccessController.class).setLevel(Level.TRACE);
    Logger.getLogger(AccessControlFilter.class).setLevel(Level.TRACE);
    Logger.getLogger(TableAuthManager.class).setLevel(Level.TRACE);
  }

  @Rule
  public TestTableName TEST_TABLE = new TestTableName();
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  // user is table owner. will have all permissions on table
  private static User USER_OWNER;
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static AccessController ACCESS_CONTROLLER;

  static void verifyConfiguration(Configuration conf) {
    if (!(conf.get(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY).contains(
        AccessController.class.getName())
        && conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY).contains(
            AccessController.class.getName()) && conf.get(
        CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY).contains(
        AccessController.class.getName()))) {
      throw new RuntimeException("AccessController is missing from a system coprocessor list");
    }
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    conf.set("hbase.master.hfilecleaner.plugins",
        "org.apache.hadoop.hbase.master.cleaner.HFileLinkCleaner,"
            + "org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner");
    conf.set("hbase.master.logcleaner.plugins",
        "org.apache.hadoop.hbase.master.snapshot.SnapshotLogCleaner");
    // Enable security
    SecureTestUtil.enableSecurity(conf);
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);

    // Enable EXEC permission checking
    conf.setBoolean(AccessController.EXEC_PERMISSION_CHECKS_KEY, true);

    TEST_UTIL.startMiniCluster();
    MasterCoprocessorHost cpHost = TEST_UTIL.getMiniHBaseCluster()
        .getMaster().getCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = (AccessController) cpHost.findCoprocessor(AccessController.class.getName());
    cpHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
        Coprocessor.PRIORITY_HIGHEST, 1, conf);
    RegionServerCoprocessorHost rsHost = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getCoprocessorHost();
    rsHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
        Coprocessor.PRIORITY_HIGHEST, 1, conf);

    // Wait for the ACL table to become available
    TEST_UTIL.waitTableEnabled(AccessControlLists.ACL_TABLE_NAME.getName());

    // create a set of test users
    USER_OWNER = User.createUserForTesting(conf, "owner", new String[0]);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    // Create the test table (owner added to the _acl_ table)
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE.getTableName());
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    hcd.setMaxVersions(4);
    htd.setOwner(USER_OWNER);
    htd.addFamily(hcd);
    admin.createTable(htd, new byte[][] { Bytes.toBytes("s") });
    TEST_UTIL.waitTableEnabled(TEST_TABLE.getTableName().getName());
  }

  @Test
  public void testCellPermissionwithVersions() throws Exception {
    // table/column/qualifier level permissions
    final byte[] TEST_ROW = Bytes.toBytes("cellpermtest");
    final byte[] TEST_ROW1 = Bytes.toBytes("cellpermtest1");
    final byte[] TEST_Q1 = Bytes.toBytes("q1");
    // test value
    final byte[] ZERO = Bytes.toBytes(0L);

    /* ---- Setup ---- */

    // additional test user
    final User userOther = User.createUserForTesting(conf, "user_check_cell_perms_other",
        new String[0]);

    // store two sets of values, one store with a cell level ACL, and one
    // without
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTable t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          Put p;
          // with ro ACL
          p = new Put(TEST_ROW).add(TEST_FAMILY, TEST_Q1, ZERO);
          p.setACL(userOther.getShortName(), new Permission(Permission.Action.WRITE));
          t.put(p);
          // with ro ACL
          p = new Put(TEST_ROW).add(TEST_FAMILY, TEST_Q1, ZERO);
          p.setACL(userOther.getShortName(), new Permission(Permission.Action.READ));
          t.put(p);
          p = new Put(TEST_ROW).add(TEST_FAMILY, TEST_Q1, ZERO);
          p.setACL(userOther.getShortName(), new Permission(Permission.Action.WRITE));
          t.put(p);
          p = new Put(TEST_ROW).add(TEST_FAMILY, TEST_Q1, ZERO);
          p.setACL(userOther.getShortName(), new Permission(Permission.Action.READ));
          t.put(p);
          p = new Put(TEST_ROW).add(TEST_FAMILY, TEST_Q1, ZERO);
          p.setACL(userOther.getShortName(), new Permission(Permission.Action.WRITE));
          t.put(p);
        } finally {
          t.close();
        }
        return null;
      }
    }, USER_OWNER);

    /* ---- Gets ---- */

    AccessTestAction getQ1 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW);
        get.setMaxVersions(10);
        HTable t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          return t.get(get).listCells();
        } finally {
          t.close();
        }
      }
    };

    AccessTestAction get2 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW1);
        get.setMaxVersions(10);
        HTable t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          return t.get(get).listCells();
        } finally {
          t.close();
        }
      }
    };
    // Confirm special read access set at cell level

    verifyAllowed(userOther, getQ1, 2);

    // store two sets of values, one store with a cell level ACL, and one
    // without
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTable t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          Put p;
          p = new Put(TEST_ROW1).add(TEST_FAMILY, TEST_Q1, ZERO);
          p.setACL(userOther.getShortName(), new Permission(Permission.Action.WRITE));
          t.put(p);
          p = new Put(TEST_ROW1).add(TEST_FAMILY, TEST_Q1, ZERO);
          p.setACL(userOther.getShortName(), new Permission(Permission.Action.READ));
          t.put(p);
          p = new Put(TEST_ROW1).add(TEST_FAMILY, TEST_Q1, ZERO);
          p.setACL(userOther.getShortName(), new Permission(Permission.Action.WRITE));
          t.put(p);
        } finally {
          t.close();
        }
        return null;
      }
    }, USER_OWNER);
    // Confirm special read access set at cell level

    verifyAllowed(userOther, get2, 1);
  }

  public void verifyAllowed(User user, AccessTestAction action, int count) throws Exception {
    try {
      Object obj = user.runAs(action);
      if (obj != null && obj instanceof List<?>) {
        List<?> results = (List<?>) obj;
        if (results != null && results.isEmpty()) {
          fail("Empty non null results from action for user '" + user.getShortName() + "'");
        }
        assertEquals(results.size(), count);
      }
    } catch (AccessDeniedException ade) {
      fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
    }
  }

  @After
  public void tearDown() throws Exception {
    // Clean the _acl_ table
    try {
      TEST_UTIL.deleteTable(TEST_TABLE.getTableName());
    } catch (TableNotFoundException ex) {
      // Test deleted the table, no problem
      LOG.info("Test deleted table " + TEST_TABLE.getTableName());
    }
    assertEquals(0, AccessControlLists.getTablePermissions(conf, TEST_TABLE.getTableName()).size());
  }
}
