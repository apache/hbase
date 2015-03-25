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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
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

import com.google.common.collect.Lists;

@Category(MediumTests.class)
public class TestCellACLs extends SecureTestUtil {
  private static final Log LOG = LogFactory.getLog(TestCellACLs.class);

  static {
    Logger.getLogger(AccessController.class).setLevel(Level.TRACE);
    Logger.getLogger(AccessControlFilter.class).setLevel(Level.TRACE);
    Logger.getLogger(TableAuthManager.class).setLevel(Level.TRACE);
  }

  @Rule
  public TestTableName TEST_TABLE = new TestTableName();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static final byte[] TEST_ROW = Bytes.toBytes("cellpermtest");
  private static final byte[] TEST_Q1 = Bytes.toBytes("q1");
  private static final byte[] TEST_Q2 = Bytes.toBytes("q2");
  private static final byte[] TEST_Q3 = Bytes.toBytes("q3");
  private static final byte[] TEST_Q4 = Bytes.toBytes("q4");
  private static final byte[] ZERO = Bytes.toBytes(0L);
  private static final byte[] ONE = Bytes.toBytes(1L);

  private static Configuration conf;

  private static User USER_OWNER;
  private static User USER_OTHER;

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
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    // Create the test table (owner added to the _acl_ table)
    Admin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE.getTableName());
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    hcd.setMaxVersions(4);
    htd.setOwner(USER_OWNER);
    htd.addFamily(hcd);
    admin.createTable(htd, new byte[][] { Bytes.toBytes("s") });
    TEST_UTIL.waitTableEnabled(TEST_TABLE.getTableName());
    LOG.info("Sleeping a second because of HBASE-12581");
    Threads.sleep(1000);
  }

  @Test
  public void testCellPermissions() throws Exception {
    // store two sets of values, one store with a cell level ACL, and one without
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          Put p;
          // with ro ACL
          p = new Put(TEST_ROW).add(TEST_FAMILY, TEST_Q1, ZERO);
          p.setACL(USER_OTHER.getShortName(), new Permission(Action.READ));
          t.put(p);
          // with rw ACL
          p = new Put(TEST_ROW).add(TEST_FAMILY, TEST_Q2, ZERO);
          p.setACL(USER_OTHER.getShortName(), new Permission(Action.READ, Action.WRITE));
          t.put(p);
          // no ACL
          p = new Put(TEST_ROW)
            .add(TEST_FAMILY, TEST_Q3, ZERO)
            .add(TEST_FAMILY, TEST_Q4, ZERO);
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
        Get get = new Get(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q1);
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          return t.get(get).listCells();
        } finally {
          t.close();
        }
      }
    };

    AccessTestAction getQ2 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q2);
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          return t.get(get).listCells();
        } finally {
          t.close();
        }
      }
    };

    AccessTestAction getQ3 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q3);
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          return t.get(get).listCells();
        } finally {
          t.close();
        }
      }
    };

    AccessTestAction getQ4 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get get = new Get(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q4);
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          return t.get(get).listCells();
        } finally {
          t.close();
        }
      }
    };

    // Confirm special read access set at cell level

    verifyAllowed(getQ1, USER_OTHER);
    verifyAllowed(getQ2, USER_OTHER);

    // Confirm this access does not extend to other cells

    verifyIfNull(getQ3, USER_OTHER);
    verifyIfNull(getQ4, USER_OTHER);

    /* ---- Scans ---- */

    // check that a scan over the test data returns the expected number of KVs

    final List<Cell> scanResults = Lists.newArrayList();

    AccessTestAction scanAction = new AccessTestAction() {
      @Override
      public List<Cell> run() throws Exception {
        Scan scan = new Scan();
        scan.setStartRow(TEST_ROW);
        scan.setStopRow(Bytes.add(TEST_ROW, new byte[]{ 0 } ));
        scan.addFamily(TEST_FAMILY);
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          ResultScanner scanner = t.getScanner(scan);
          Result result = null;
          do {
            result = scanner.next();
            if (result != null) {
              scanResults.addAll(result.listCells());
            }
          } while (result != null);
        } finally {
          t.close();
        }
        return scanResults;
      }
    };

    // owner will see all values
    scanResults.clear();
    verifyAllowed(scanAction, USER_OWNER);
    assertEquals(4, scanResults.size());

    // other user will see 2 values
    scanResults.clear();
    verifyAllowed(scanAction, USER_OTHER);
    assertEquals(2, scanResults.size());

    /* ---- Increments ---- */

    AccessTestAction incrementQ1 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Increment i = new Increment(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q1, 1L);
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          t.increment(i);
        } finally {
          t.close();
        }
        return null;
      }
    };

    AccessTestAction incrementQ2 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Increment i = new Increment(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q2, 1L);
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          t.increment(i);
        } finally {
          t.close();
        }
        return null;
      }
    };

    AccessTestAction incrementQ2newDenyACL = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Increment i = new Increment(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q2, 1L);
        // Tag this increment with an ACL that denies write permissions to USER_OTHER
        i.setACL(USER_OTHER.getShortName(), new Permission(Action.READ));
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          t.increment(i);
        } finally {
          t.close();
        }
        return null;
      }
    };

    AccessTestAction incrementQ3 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Increment i = new Increment(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q3, 1L);
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          t.increment(i);
        } finally {
          t.close();
        }
        return null;
      }
    };

    verifyDenied(incrementQ1, USER_OTHER);
    verifyDenied(incrementQ3, USER_OTHER);

    // We should be able to increment Q2 twice, the previous ACL will be
    // carried forward
    verifyAllowed(incrementQ2, USER_OTHER);
    verifyAllowed(incrementQ2newDenyACL, USER_OTHER);
    // But not again after we denied ourselves write permission with an ACL
    // update
    verifyDenied(incrementQ2, USER_OTHER);

    /* ---- Deletes ---- */

    AccessTestAction deleteFamily = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Delete delete = new Delete(TEST_ROW).deleteFamily(TEST_FAMILY);
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          t.delete(delete);
        } finally {
          t.close();
        }
        return null;
      }
    };

    AccessTestAction deleteQ1 = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Delete delete = new Delete(TEST_ROW).deleteColumn(TEST_FAMILY, TEST_Q1);
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          t.delete(delete);
        } finally {
          t.close();
        }
        return null;
      }
    };

    verifyDenied(deleteFamily, USER_OTHER);
    verifyDenied(deleteQ1, USER_OTHER);
    verifyAllowed(deleteQ1, USER_OWNER);
  }

  /**
   * Insure we are not granting access in the absence of any cells found
   * when scanning for covered cells.
   */
  @Test
  public void testCoveringCheck() throws Exception {
    // Grant read access to USER_OTHER
    grantOnTable(TEST_UTIL, USER_OTHER.getShortName(), TEST_TABLE.getTableName(),
      TEST_FAMILY, null, Action.READ);

    // A write by USER_OTHER should be denied.
    // This is where we could have a big problem if there is an error in the
    // covering check logic.
    verifyDenied(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          Put p;
          p = new Put(TEST_ROW).add(TEST_FAMILY, TEST_Q1, ZERO);
          t.put(p);
        } finally {
          t.close();
        }
        return null;
      }
    }, USER_OTHER);

    // Add the cell
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          Put p;
          p = new Put(TEST_ROW).add(TEST_FAMILY, TEST_Q1, ZERO);
          t.put(p);
        } finally {
          t.close();
        }
        return null;
      }
    }, USER_OWNER);

    // A write by USER_OTHER should still be denied, just to make sure
    verifyDenied(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          Put p;
          p = new Put(TEST_ROW).add(TEST_FAMILY, TEST_Q1, ONE);
          t.put(p);
        } finally {
          t.close();
        }
        return null;
      }
    }, USER_OTHER);

    // A read by USER_OTHER should be allowed, just to make sure
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Table t = new HTable(conf, TEST_TABLE.getTableName());
        try {
          return t.get(new Get(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q1));
        } finally {
          t.close();
        }
      }
    }, USER_OTHER);
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
