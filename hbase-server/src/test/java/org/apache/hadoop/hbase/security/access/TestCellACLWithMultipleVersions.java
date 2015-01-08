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

import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.TestTableName;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SecurityTests.class, MediumTests.class})
public class TestCellACLWithMultipleVersions extends SecureTestUtil {
  private static final Log LOG = LogFactory.getLog(TestCellACLWithMultipleVersions.class);

  static {
    Logger.getLogger(AccessController.class).setLevel(Level.TRACE);
    Logger.getLogger(AccessControlFilter.class).setLevel(Level.TRACE);
    Logger.getLogger(TableAuthManager.class).setLevel(Level.TRACE);
  }

  @Rule
  public TestTableName TEST_TABLE = new TestTableName();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] TEST_FAMILY1 = Bytes.toBytes("f1");
  private static final byte[] TEST_FAMILY2 = Bytes.toBytes("f2");
  private static final byte[] TEST_ROW = Bytes.toBytes("cellpermtest");
  private static final byte[] TEST_Q1 = Bytes.toBytes("q1");
  private static final byte[] TEST_Q2 = Bytes.toBytes("q2");
  private static final byte[] ZERO = Bytes.toBytes(0L);
  private static final byte[] ONE = Bytes.toBytes(1L);
  private static final byte[] TWO = Bytes.toBytes(2L);

  private static Configuration conf;

  private static User USER_OWNER;
  private static User USER_OTHER;
  private static User USER_OTHER2;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    // Enable security
    enableSecurity(conf);
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);

    // We expect 0.98 cell ACL semantics
    conf.setBoolean(AccessControlConstants.CF_ATTRIBUTE_EARLY_OUT, false);

    TEST_UTIL.startMiniCluster();
    MasterCoprocessorHost cpHost = TEST_UTIL.getMiniHBaseCluster().getMaster()
        .getMasterCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    AccessController ac = (AccessController)
      cpHost.findCoprocessor(AccessController.class.getName());
    cpHost.createEnvironment(AccessController.class, ac, Coprocessor.PRIORITY_HIGHEST, 1, conf);
    RegionServerCoprocessorHost rsHost = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getRegionServerCoprocessorHost();
    rsHost.createEnvironment(AccessController.class, ac, Coprocessor.PRIORITY_HIGHEST, 1, conf);

    // Wait for the ACL table to become available
    TEST_UTIL.waitTableEnabled(AccessControlLists.ACL_TABLE_NAME);

    // create a set of test users
    USER_OWNER = User.createUserForTesting(conf, "owner", new String[0]);
    USER_OTHER = User.createUserForTesting(conf, "other", new String[0]);
    USER_OTHER2 = User.createUserForTesting(conf, "other2", new String[0]);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE.getTableName());
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY1);
    hcd.setMaxVersions(4);
    htd.setOwner(USER_OWNER);
    htd.addFamily(hcd);
    hcd = new HColumnDescriptor(TEST_FAMILY2);
    hcd.setMaxVersions(4);
    htd.setOwner(USER_OWNER);
    htd.addFamily(hcd);
    // Create the test table (owner added to the _acl_ table)
    try (Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      try (Admin admin = connection.getAdmin()) {
        admin.createTable(htd, new byte[][] { Bytes.toBytes("s") });
      }
    }
    TEST_UTIL.waitTableEnabled(TEST_TABLE.getTableName());
    LOG.info("Sleeping a second because of HBASE-12581");
    Threads.sleep(1000);
  }

  @Test
  public void testCellPermissionwithVersions() throws Exception {
    // store two sets of values, one store with a cell level ACL, and one
    // without
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table t = connection.getTable(TEST_TABLE.getTableName());
        try {
          Put p;
          // with ro ACL
          p = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(USER_OTHER.getShortName(), new Permission(Permission.Action.WRITE));
          t.put(p);
          // with ro ACL
          p = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(USER_OTHER.getShortName(), new Permission(Permission.Action.READ));
          t.put(p);
          p = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(USER_OTHER.getShortName(), new Permission(Permission.Action.WRITE));
          t.put(p);
          p = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(USER_OTHER.getShortName(), new Permission(Permission.Action.READ));
          t.put(p);
          p = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(USER_OTHER.getShortName(), new Permission(Permission.Action.WRITE));
          t.put(p);
        } finally {
          t.close();
          connection.close();
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
        Connection connection = ConnectionFactory.createConnection(conf);
        Table t = connection.getTable(TEST_TABLE.getTableName());
        try {
          return t.get(get).listCells();
        } finally {
          t.close();
          connection.close();
        }
      }
    };

    AccessTestAction get2 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW);
        get.setMaxVersions(10);
        Connection connection = ConnectionFactory.createConnection(conf);
        Table t = connection.getTable(TEST_TABLE.getTableName());
        try {
          return t.get(get).listCells();
        } finally {
          t.close();
          connection.close();
        }
      }
    };
    // Confirm special read access set at cell level

    verifyAllowed(USER_OTHER, getQ1, 2);

    // store two sets of values, one store with a cell level ACL, and one
    // without
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table t = connection.getTable(TEST_TABLE.getTableName());
        try {
          Put p;
          p = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(USER_OTHER.getShortName(), new Permission(Permission.Action.WRITE));
          t.put(p);
          p = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(USER_OTHER.getShortName(), new Permission(Permission.Action.READ));
          t.put(p);
          p = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q1, ZERO);
          p.setACL(USER_OTHER.getShortName(), new Permission(Permission.Action.WRITE));
          t.put(p);
        } finally {
          t.close();
          connection.close();
        }
        return null;
      }
    }, USER_OWNER);
    // Confirm special read access set at cell level

    verifyAllowed(USER_OTHER, get2, 1);
  }

  @Test
  public void testCellPermissionsWithDeleteMutipleVersions() throws Exception {
    // table/column/qualifier level permissions
    final byte[] TEST_ROW1 = Bytes.toBytes("r1");
    final byte[] TEST_ROW2 = Bytes.toBytes("r2");
    final byte[] TEST_Q1 = Bytes.toBytes("q1");
    final byte[] TEST_Q2 = Bytes.toBytes("q2");
    final byte[] ZERO = Bytes.toBytes(0L);

    // additional test user
    final User user1 = User.createUserForTesting(conf, "user1", new String[0]);
    final User user2 = User.createUserForTesting(conf, "user2", new String[0]);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            // with rw ACL for "user1"
            Put p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, ZERO);
            p.add(TEST_FAMILY1, TEST_Q2, ZERO);
            p.setACL(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            t.put(p);
            // with rw ACL for "user1"
            p = new Put(TEST_ROW2);
            p.add(TEST_FAMILY1, TEST_Q1, ZERO);
            p.add(TEST_FAMILY1, TEST_Q2, ZERO);
            p.setACL(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            // with rw ACL for "user1" and "user2"
            Put p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, ZERO);
            p.add(TEST_FAMILY1, TEST_Q2, ZERO);
            Map<String, Permission> perms = new HashMap<String, Permission>();
            perms.put(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            perms.put(user2.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            p.setACL(perms);
            t.put(p);
            // with rw ACL for "user1" and "user2"
            p = new Put(TEST_ROW2);
            p.add(TEST_FAMILY1, TEST_Q1, ZERO);
            p.add(TEST_FAMILY1, TEST_Q2, ZERO);
            p.setACL(perms);
            t.put(p);
          }
        }
        return null;
      }
    }, user1);

    // user1 should be allowed to delete TEST_ROW1 as he is having write permission on both
    // versions of the cells
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Delete d = new Delete(TEST_ROW1);
            d.deleteColumns(TEST_FAMILY1, TEST_Q1);
            d.deleteColumns(TEST_FAMILY1, TEST_Q2);
            t.delete(d);
          }
        }
        return null;
      }
    });
    // user2 should not be allowed to delete TEST_ROW2 as he is having write permission only on one
    // version of the cells.
    user2.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Delete d = new Delete(TEST_ROW2);
            d.deleteColumns(TEST_FAMILY1, TEST_Q1);
            d.deleteColumns(TEST_FAMILY1, TEST_Q2);
            t.delete(d);
            fail("user2 should not be allowed to delete the row");
          } catch (Exception e) {

          }
        }
        return null;
      }
    });
    // user1 should be allowed to delete the cf. (All data under cf for a row)
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Delete d = new Delete(TEST_ROW2);
            d.deleteFamily(TEST_FAMILY1);
            t.delete(d);
          }
        }
        return null;
      }
    });
  }


  @Test
  public void testDeleteWithFutureTimestamp() throws Exception {
    // Store two values, one in the future

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            // Store a read write ACL without a timestamp, server will use current time
            Put p = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q2, ONE);
            p.setACL(USER_OTHER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            t.put(p);
            LOG.info("Stored at current time");
            // Store read only ACL at a future time
            p = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q1,
                EnvironmentEdgeManager.currentTime() + 1000000, ZERO);
            p.setACL(USER_OTHER.getShortName(), new Permission(Permission.Action.READ));
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    // Confirm stores are visible

    AccessTestAction getQ1 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q1);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            return t.get(get).listCells();
          }
        }
      }
    };

    AccessTestAction getQ2 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW).addColumn(TEST_FAMILY1, TEST_Q2);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            return t.get(get).listCells();
          }
        }
      }
    };

    verifyAllowed(getQ1, USER_OWNER, USER_OTHER);
    verifyAllowed(getQ2, USER_OWNER, USER_OTHER);


    // Issue a DELETE for the family, should succeed because the future ACL is
    // not considered

    AccessTestAction deleteFamily = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Delete delete = new Delete(TEST_ROW).deleteFamily(TEST_FAMILY1);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            t.delete(delete);
          }
        }
        return null;
      }
    };

    verifyAllowed(deleteFamily, USER_OTHER);

    // The future put should still exist
    
    verifyAllowed(getQ1, USER_OWNER, USER_OTHER);
    
    // The other put should be covered by the tombstone

    verifyDenied(getQ2, USER_OTHER);
  }

  @Test
  public void testCellPermissionsWithDeleteWithUserTs() throws Exception {
    USER_OWNER.runAs(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            // This version (TS = 123) with rw ACL for USER_OTHER and USER_OTHER2
            Put p = new Put(TEST_ROW);
            p.add(TEST_FAMILY1, TEST_Q1, 123L, ZERO);
            p.add(TEST_FAMILY1, TEST_Q2, 123L, ZERO);
            Map<String, Permission> perms = new HashMap<String, Permission>();
            perms.put(USER_OTHER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            perms.put(USER_OTHER2.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            p.setACL(perms);
            t.put(p);

            // This version (TS = 125) with rw ACL for USER_OTHER
            p = new Put(TEST_ROW);
            p.add(TEST_FAMILY1, TEST_Q1, 125L, ONE);
            p.add(TEST_FAMILY1, TEST_Q2, 125L, ONE);
            perms = new HashMap<String, Permission>();
            perms.put(USER_OTHER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            p.setACL(perms);
            t.put(p);

            // This version (TS = 127) with rw ACL for USER_OTHER
            p = new Put(TEST_ROW);
            p.add(TEST_FAMILY1, TEST_Q1, 127L, TWO);
            p.add(TEST_FAMILY1, TEST_Q2, 127L, TWO);
            perms = new HashMap<String, Permission>();
            perms.put(USER_OTHER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            p.setACL(perms);
            t.put(p);

            return null;
          }
        }
      }
    });

    // USER_OTHER2 should be allowed to delete the column f1:q1 versions older than TS 124L
    USER_OTHER2.runAs(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Delete d = new Delete(TEST_ROW, 124L);
            d.deleteColumns(TEST_FAMILY1, TEST_Q1);
            t.delete(d);
          }
        }
        return null;
      }
    });

    // USER_OTHER2 should be allowed to delete the column f1:q2 versions older than TS 124L
    USER_OTHER2.runAs(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Delete d = new Delete(TEST_ROW);
            d.deleteColumns(TEST_FAMILY1, TEST_Q2, 124L);
            t.delete(d);
          }
        }
        return null;
      }
    });
  }

  @Test
  public void testCellPermissionsWithDeleteExactVersion() throws Exception {
    final byte[] TEST_ROW1 = Bytes.toBytes("r1");
    final byte[] TEST_Q1 = Bytes.toBytes("q1");
    final byte[] TEST_Q2 = Bytes.toBytes("q2");
    final byte[] ZERO = Bytes.toBytes(0L);

    final User user1 = User.createUserForTesting(conf, "user1", new String[0]);
    final User user2 = User.createUserForTesting(conf, "user2", new String[0]);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Map<String, Permission> permsU1andOwner = new HashMap<String, Permission>();
            permsU1andOwner.put(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU1andOwner.put(USER_OWNER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            Map<String, Permission> permsU2andOwner = new HashMap<String, Permission>();
            permsU2andOwner.put(user2.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU2andOwner.put(USER_OWNER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            Put p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, 123, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q2, 123, ZERO);
            p.setACL(permsU2andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY2, TEST_Q1, 123, ZERO);
            p.add(TEST_FAMILY2, TEST_Q2, 123, ZERO);
            p.setACL(permsU2andOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY2, TEST_Q1, 125, ZERO);
            p.add(TEST_FAMILY2, TEST_Q2, 125, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, 127, ZERO);
            p.setACL(permsU2andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q2, 127, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY2, TEST_Q1, 129, ZERO);
            p.add(TEST_FAMILY2, TEST_Q2, 129, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    // user1 should be allowed to delete TEST_ROW1 as he is having write permission on both
    // versions of the cells
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Delete d = new Delete(TEST_ROW1);
            d.deleteColumn(TEST_FAMILY1, TEST_Q1, 123);
            d.deleteColumn(TEST_FAMILY1, TEST_Q2);
            d.deleteFamilyVersion(TEST_FAMILY2, 125);
            t.delete(d);
          }
        }
        return null;
      }
    });

    user2.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Delete d = new Delete(TEST_ROW1, 127);
            d.deleteColumns(TEST_FAMILY1, TEST_Q1);
            d.deleteColumns(TEST_FAMILY1, TEST_Q2);
            d.deleteFamily(TEST_FAMILY2, 129);
            t.delete(d);
            fail("user2 can not do the delete");
          } catch (Exception e) {

          }
        }
        return null;
      }
    });
  }

  @Test
  public void testCellPermissionsForIncrementWithMultipleVersions() throws Exception {
    final byte[] TEST_ROW1 = Bytes.toBytes("r1");
    final byte[] TEST_Q1 = Bytes.toBytes("q1");
    final byte[] TEST_Q2 = Bytes.toBytes("q2");
    final byte[] ZERO = Bytes.toBytes(0L);

    final User user1 = User.createUserForTesting(conf, "user1", new String[0]);
    final User user2 = User.createUserForTesting(conf, "user2", new String[0]);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Map<String, Permission> permsU1andOwner = new HashMap<String, Permission>();
            permsU1andOwner.put(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU1andOwner.put(USER_OWNER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            Map<String, Permission> permsU2andOwner = new HashMap<String, Permission>();
            permsU2andOwner.put(user2.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU2andOwner.put(USER_OWNER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            Put p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, 123, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q2, 123, ZERO);
            p.setACL(permsU2andOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, 127, ZERO);
            p.setACL(permsU2andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q2, 127, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    // Increment considers the TimeRange set on it.
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Increment inc = new Increment(TEST_ROW1);
            inc.setTimeRange(0, 123);
            inc.addColumn(TEST_FAMILY1, TEST_Q1, 2L);
            t.increment(inc);
            t.incrementColumnValue(TEST_ROW1, TEST_FAMILY1, TEST_Q2, 1L);
          }
        }
        return null;
      }
    });

    user2.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Increment inc = new Increment(TEST_ROW1);
            inc.setTimeRange(0, 127);
            inc.addColumn(TEST_FAMILY1, TEST_Q2, 2L);
            t.increment(inc);
            fail();
          } catch (Exception e) {

          }
        }
        return null;
      }
    });
  }

  @Test
  public void testCellPermissionsForPutWithMultipleVersions() throws Exception {
    final byte[] TEST_ROW1 = Bytes.toBytes("r1");
    final byte[] TEST_Q1 = Bytes.toBytes("q1");
    final byte[] TEST_Q2 = Bytes.toBytes("q2");
    final byte[] ZERO = Bytes.toBytes(0L);

    final User user1 = User.createUserForTesting(conf, "user1", new String[0]);
    final User user2 = User.createUserForTesting(conf, "user2", new String[0]);

    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Map<String, Permission> permsU1andOwner = new HashMap<String, Permission>();
            permsU1andOwner.put(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU1andOwner.put(USER_OWNER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            Map<String, Permission> permsU2andOwner = new HashMap<String, Permission>();
            permsU2andOwner.put(user2.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU2andOwner.put(USER_OWNER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            Put p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, 123, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q2, 123, ZERO);
            p.setACL(permsU2andOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, 127, ZERO);
            p.setACL(permsU2andOwner);
            t.put(p);
            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q2, 127, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    // new Put with TEST_Q1 column having TS=125. This covers old cell with TS 123 and user1 is
    // having RW permission. While TEST_Q2 is with latest TS and so it covers old cell with TS 127.
    // User1 is having RW permission on that too.
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Put p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, 125, ZERO);
            p.add(TEST_FAMILY1, TEST_Q2, ZERO);
            p.setACL(user2.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            t.put(p);
          }
        }
        return null;
      }
    });

    // Should be denied.
    user2.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Put p = new Put(TEST_ROW1);
            // column Q1 covers version at 123 fr which user2 do not have permission
            p.add(TEST_FAMILY1, TEST_Q1, 124, ZERO);
            p.add(TEST_FAMILY1, TEST_Q2, ZERO);
            t.put(p);
            fail();
          } catch (Exception e) {

          }
        }
        return null;
      }
    });
  }

  @Test
  public void testCellPermissionsForCheckAndDelete() throws Exception {
    final byte[] TEST_ROW1 = Bytes.toBytes("r1");
    final byte[] ZERO = Bytes.toBytes(0L);

    final User user1 = User.createUserForTesting(conf, "user1", new String[0]);
    final User user2 = User.createUserForTesting(conf, "user2", new String[0]);
    
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Map<String, Permission> permsU1andOwner = new HashMap<String, Permission>();
            permsU1andOwner.put(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU1andOwner.put(USER_OWNER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            Map<String, Permission> permsU1andU2andOwner = new HashMap<String, Permission>();
            permsU1andU2andOwner.put(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU1andU2andOwner.put(user2.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU1andU2andOwner.put(USER_OWNER.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            Map<String, Permission> permsU1andU2 = new HashMap<String, Permission>();
            permsU1andU2.put(user1.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));
            permsU1andU2.put(user2.getShortName(), new Permission(Permission.Action.READ,
                Permission.Action.WRITE));

            Put p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, 120, ZERO);
            p.add(TEST_FAMILY1, TEST_Q2, 120, ZERO);
            p.setACL(permsU1andU2andOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, 123, ZERO);
            p.add(TEST_FAMILY1, TEST_Q2, 123, ZERO);
            p.setACL(permsU1andOwner);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q1, 127, ZERO);
            p.setACL(permsU1andU2);
            t.put(p);

            p = new Put(TEST_ROW1);
            p.add(TEST_FAMILY1, TEST_Q2, 127, ZERO);
            p.setACL(user2.getShortName(), new Permission(Permission.Action.READ));
            t.put(p);
          }
        }
        return null;
      }
    }, USER_OWNER);

    // user1 should be allowed to do the checkAndDelete. user1 having read permission on the latest
    // version cell and write permission on all versions
    user1.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Delete d = new Delete(TEST_ROW1);
            d.deleteColumns(TEST_FAMILY1, TEST_Q1, 120);
            t.checkAndDelete(TEST_ROW1, TEST_FAMILY1, TEST_Q1, ZERO, d);
          }
        }
        return null;
      }
    });
    // user2 shouldn't be allowed to do the checkAndDelete. user2 having RW permission on the latest
    // version cell but not on cell version TS=123
    user2.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Delete d = new Delete(TEST_ROW1);
            d.deleteColumns(TEST_FAMILY1, TEST_Q1);
            t.checkAndDelete(TEST_ROW1, TEST_FAMILY1, TEST_Q1, ZERO, d);
            fail("user2 should not be allowed to do checkAndDelete");
          } catch (Exception e) {
          }
        }
        return null;
      }
    });
    // user2 should be allowed to do the checkAndDelete when delete tries to delete the old version
    // TS=120. user2 having R permission on the latest version(no W permission) cell
    // and W permission on cell version TS=120.
    user2.runAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
          try (Table t = connection.getTable(TEST_TABLE.getTableName())) {
            Delete d = new Delete(TEST_ROW1);
            d.deleteColumn(TEST_FAMILY1, TEST_Q2, 120);
            t.checkAndDelete(TEST_ROW1, TEST_FAMILY1, TEST_Q2, ZERO, d);
          }
        }
        return null;
      }
    });
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