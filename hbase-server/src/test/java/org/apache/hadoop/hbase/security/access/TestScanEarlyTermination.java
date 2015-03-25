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

import static org.junit.Assert.*;

import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
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
public class TestScanEarlyTermination extends SecureTestUtil {
  private static final Log LOG = LogFactory.getLog(TestScanEarlyTermination.class);

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
  private static final byte[] TEST_ROW = Bytes.toBytes("testrow");
  private static final byte[] TEST_Q1 = Bytes.toBytes("q1");
  private static final byte[] TEST_Q2 = Bytes.toBytes("q2");
  private static final byte[] ZERO = Bytes.toBytes(0L);

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
    Admin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE.getTableName());
    htd.setOwner(USER_OWNER);
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY1);
    hcd.setMaxVersions(10);
    htd.addFamily(hcd);
    hcd = new HColumnDescriptor(TEST_FAMILY2);
    hcd.setMaxVersions(10);
    htd.addFamily(hcd);

    // Enable backwards compatible early termination behavior in the HTD. We
    // want to confirm that the per-table configuration is properly picked up.
    htd.setConfiguration(AccessControlConstants.CF_ATTRIBUTE_EARLY_OUT, "true");

    admin.createTable(htd);
    TEST_UTIL.waitUntilAllRegionsAssigned(TEST_TABLE.getTableName());
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

  @Test
  public void testEarlyScanTermination() throws Exception {
    // Grant USER_OTHER access to TEST_FAMILY1 only
    grantOnTable(TEST_UTIL, USER_OTHER.getShortName(), TEST_TABLE.getTableName(), TEST_FAMILY1,
      null, Action.READ);

    // Set up test data
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        // force a new RS connection
        conf.set("testkey", UUID.randomUUID().toString());
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table t = connection.getTable(TEST_TABLE.getTableName())) {
          Put put = new Put(TEST_ROW).add(TEST_FAMILY1, TEST_Q1, ZERO);
          t.put(put);
          // Set a READ cell ACL for USER_OTHER on this value in FAMILY2
          put = new Put(TEST_ROW).add(TEST_FAMILY2, TEST_Q1, ZERO);
          put.setACL(USER_OTHER.getShortName(), new Permission(Action.READ));
          t.put(put);
          // Set an empty cell ACL for USER_OTHER on this other value in FAMILY2
          put = new Put(TEST_ROW).add(TEST_FAMILY2, TEST_Q2, ZERO);
          put.setACL(USER_OTHER.getShortName(), new Permission());
          t.put(put);
        }
        return null;
      }
    }, USER_OWNER);

    // A scan of FAMILY1 will be allowed
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        // force a new RS connection
        conf.set("testkey", UUID.randomUUID().toString());
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table t = connection.getTable(TEST_TABLE.getTableName())) {
          Scan scan = new Scan().addFamily(TEST_FAMILY1);
          Result result = t.getScanner(scan).next();
          if (result != null) {
            assertTrue("Improper exclusion", result.containsColumn(TEST_FAMILY1, TEST_Q1));
            assertFalse("Improper inclusion", result.containsColumn(TEST_FAMILY2, TEST_Q1));
            return result.listCells();
          }
          return null;
        }
      }
    }, USER_OTHER);

    // A scan of FAMILY1 and FAMILY2 will produce results for FAMILY1 without
    // throwing an exception, however no cells from FAMILY2 will be returned
    // because we early out checks at the CF level.
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        // force a new RS connection
        conf.set("testkey", UUID.randomUUID().toString());
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table t = connection.getTable(TEST_TABLE.getTableName())) {
          Scan scan = new Scan();
          Result result = t.getScanner(scan).next();
          if (result != null) {
            assertTrue("Improper exclusion", result.containsColumn(TEST_FAMILY1, TEST_Q1));
            assertFalse("Improper inclusion", result.containsColumn(TEST_FAMILY2, TEST_Q1));
            return result.listCells();
          }
          return null;
        }
      }
    }, USER_OTHER);

    // A scan of FAMILY2 will throw an AccessDeniedException
    verifyDenied(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        // force a new RS connection
        conf.set("testkey", UUID.randomUUID().toString());
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table t = connection.getTable(TEST_TABLE.getTableName())) {
          Scan scan = new Scan().addFamily(TEST_FAMILY2);
          Result result = t.getScanner(scan).next();
          if (result != null) {
            return result.listCells();
          }
          return null;
        }
      }
    }, USER_OTHER);

    // Now grant USER_OTHER access to TEST_FAMILY2:TEST_Q2
    grantOnTable(TEST_UTIL, USER_OTHER.getShortName(), TEST_TABLE.getTableName(), TEST_FAMILY2,
      TEST_Q2, Action.READ);

    // A scan of FAMILY1 and FAMILY2 will produce combined results. In FAMILY2
    // we have access granted to Q2 at the CF level. Because we early out
    // checks at the CF level the cell ACL on Q1 also granting access is ignored.
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        // force a new RS connection
        conf.set("testkey", UUID.randomUUID().toString());
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table t = connection.getTable(TEST_TABLE.getTableName())) {
          Scan scan = new Scan();
          Result result = t.getScanner(scan).next();
          if (result != null) {
            assertTrue("Improper exclusion", result.containsColumn(TEST_FAMILY1, TEST_Q1));
            assertFalse("Improper inclusion", result.containsColumn(TEST_FAMILY2, TEST_Q1));
            assertTrue("Improper exclusion", result.containsColumn(TEST_FAMILY2, TEST_Q2));
            return result.listCells();
          }
          return null;
        }
      }
    }, USER_OTHER);
  }
}
