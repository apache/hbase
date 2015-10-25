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
import static org.mockito.Mockito.*;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
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

import com.google.common.collect.Lists;

@Category({SecurityTests.class, LargeTests.class})
public class TestWithDisabledAuthorization extends SecureTestUtil {
  private static final Log LOG = LogFactory.getLog(TestWithDisabledAuthorization.class);

  static {
    Logger.getLogger(AccessController.class).setLevel(Level.TRACE);
    Logger.getLogger(AccessControlFilter.class).setLevel(Level.TRACE);
    Logger.getLogger(TableAuthManager.class).setLevel(Level.TRACE);
  }

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static final byte[] TEST_FAMILY2 = Bytes.toBytes("f2");
  private static final byte[] TEST_ROW = Bytes.toBytes("testrow");
  private static final byte[] TEST_Q1 = Bytes.toBytes("q1");
  private static final byte[] TEST_Q2 = Bytes.toBytes("q2");
  private static final byte[] TEST_Q3 = Bytes.toBytes("q3");
  private static final byte[] TEST_Q4 = Bytes.toBytes("q4");
  private static final byte[] ZERO = Bytes.toBytes(0L);

  private static MasterCoprocessorEnvironment CP_ENV;
  private static AccessController ACCESS_CONTROLLER;
  private static RegionServerCoprocessorEnvironment RSCP_ENV;
  private RegionCoprocessorEnvironment RCP_ENV;

  @Rule public TestTableName TEST_TABLE = new TestTableName();

  // default users
  
  // superuser
  private static User SUPERUSER;
  // user granted with all global permission
  private static User USER_ADMIN;
  // user with rw permissions on column family.
  private static User USER_RW;
  // user with read-only permissions
  private static User USER_RO;
  // user is table owner. will have all permissions on table
  private static User USER_OWNER;
  // user with create table permissions alone
  private static User USER_CREATE;
  // user with no permissions
  private static User USER_NONE;
  // user with only partial read-write perms (on family:q1 only)
  private static User USER_QUAL;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Up the handlers; this test needs more than usual.
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    // Enable security
    enableSecurity(conf);
    // We expect 0.98 cell ACL semantics
    conf.setBoolean(AccessControlConstants.CF_ATTRIBUTE_EARLY_OUT, false);
    // Enable EXEC permission checking
    conf.setBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY, true);
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);

    // Now, DISABLE only active authorization
    conf.setBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, false);

    // Start the minicluster
    TEST_UTIL.startMiniCluster();
    MasterCoprocessorHost cpHost =
        TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = (AccessController) cpHost.findCoprocessor(AccessController.class.getName());
    CP_ENV = cpHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
      Coprocessor.PRIORITY_HIGHEST, 1, conf);
    RegionServerCoprocessorHost rsHost = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
      .getRegionServerCoprocessorHost();
    RSCP_ENV = rsHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
      Coprocessor.PRIORITY_HIGHEST, 1, conf);

    // Wait for the ACL table to become available
    TEST_UTIL.waitUntilAllRegionsAssigned(AccessControlLists.ACL_TABLE_NAME);

    // create a set of test users
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER_ADMIN = User.createUserForTesting(conf, "admin2", new String[0]);
    USER_OWNER = User.createUserForTesting(conf, "owner", new String[0]);
    USER_CREATE = User.createUserForTesting(conf, "tbl_create", new String[0]);
    USER_RW = User.createUserForTesting(conf, "rwuser", new String[0]);
    USER_RO = User.createUserForTesting(conf, "rouser", new String[0]);
    USER_QUAL = User.createUserForTesting(conf, "rwpartial", new String[0]);
    USER_NONE = User.createUserForTesting(conf, "nouser", new String[0]);
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
    hcd.setMaxVersions(100);
    htd.addFamily(hcd);
    htd.setOwner(USER_OWNER);
    admin.createTable(htd, new byte[][] { Bytes.toBytes("s") });
    TEST_UTIL.waitUntilAllRegionsAssigned(TEST_TABLE.getTableName());

    Region region = TEST_UTIL.getHBaseCluster().getRegions(TEST_TABLE.getTableName()).get(0);
    RegionCoprocessorHost rcpHost = region.getCoprocessorHost();
    RCP_ENV = rcpHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
      Coprocessor.PRIORITY_HIGHEST, 1, TEST_UTIL.getConfiguration());

    // Set up initial grants

    grantGlobal(TEST_UTIL, USER_ADMIN.getShortName(),
      Permission.Action.ADMIN,
      Permission.Action.CREATE,
      Permission.Action.READ,
      Permission.Action.WRITE);

    grantOnTable(TEST_UTIL, USER_RW.getShortName(),
      TEST_TABLE.getTableName(), TEST_FAMILY, null,
      Permission.Action.READ,
      Permission.Action.WRITE);

    // USER_CREATE is USER_RW plus CREATE permissions
    grantOnTable(TEST_UTIL, USER_CREATE.getShortName(),
      TEST_TABLE.getTableName(), null, null,
      Permission.Action.CREATE,
      Permission.Action.READ,
      Permission.Action.WRITE);

    grantOnTable(TEST_UTIL, USER_RO.getShortName(),
      TEST_TABLE.getTableName(), TEST_FAMILY, null,
      Permission.Action.READ);

    grantOnTable(TEST_UTIL, USER_QUAL.getShortName(),
      TEST_TABLE.getTableName(), TEST_FAMILY, TEST_Q1,
      Permission.Action.READ,
      Permission.Action.WRITE);

    assertEquals(5, AccessControlLists.getTablePermissions(TEST_UTIL.getConfiguration(),
      TEST_TABLE.getTableName()).size());
  }

  @After
  public void tearDown() throws Exception {
    // Clean the _acl_ table
    try {
      deleteTable(TEST_UTIL, TEST_TABLE.getTableName());
    } catch (TableNotFoundException ex) {
      // Test deleted the table, no problem
      LOG.info("Test deleted table " + TEST_TABLE.getTableName());
    }
    // Verify all table/namespace permissions are erased
    assertEquals(0, AccessControlLists.getTablePermissions(TEST_UTIL.getConfiguration(),
      TEST_TABLE.getTableName()).size());
    assertEquals(0, AccessControlLists.getNamespacePermissions(TEST_UTIL.getConfiguration(),
      TEST_TABLE.getTableName().getNamespaceAsString()).size());
  }

  @Test
  public void testCheckPermissions() throws Exception {

    AccessTestAction checkGlobalAdmin = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkGlobalPerms(TEST_UTIL, Permission.Action.ADMIN);
        return null;
      }
    };

    verifyAllowed(checkGlobalAdmin, SUPERUSER, USER_ADMIN);
    verifyDenied(checkGlobalAdmin, USER_OWNER, USER_CREATE, USER_RW, USER_RO, USER_QUAL,
      USER_NONE);

    AccessTestAction checkGlobalRead = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkGlobalPerms(TEST_UTIL, Permission.Action.READ);
        return null;
      }
    };

    verifyAllowed(checkGlobalRead, SUPERUSER, USER_ADMIN);
    verifyDenied(checkGlobalRead, USER_OWNER, USER_CREATE, USER_RW, USER_RO, USER_QUAL,
      USER_NONE);

    AccessTestAction checkGlobalReadWrite = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkGlobalPerms(TEST_UTIL, Permission.Action.READ, Permission.Action.WRITE);
        return null;
      }
    };

    verifyAllowed(checkGlobalReadWrite, SUPERUSER, USER_ADMIN);
    verifyDenied(checkGlobalReadWrite, USER_OWNER, USER_CREATE, USER_RW, USER_RO, USER_QUAL,
      USER_NONE);

    AccessTestAction checkTableAdmin = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), null, null,
          Permission.Action.ADMIN);
        return null;
      }
    };

    verifyAllowed(checkTableAdmin, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(checkTableAdmin, USER_CREATE, USER_RW, USER_RO, USER_QUAL, USER_NONE);

    AccessTestAction checkTableCreate = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), null, null,
          Permission.Action.CREATE);
        return null;
      }
    };

    verifyAllowed(checkTableCreate, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE);
    verifyDenied(checkTableCreate, USER_RW, USER_RO, USER_QUAL, USER_NONE);

    AccessTestAction checkTableRead = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), null, null,
          Permission.Action.READ);
        return null;
      }
    };

    verifyAllowed(checkTableRead, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE);
    verifyDenied(checkTableRead, USER_RW, USER_RO, USER_QUAL, USER_NONE);

    AccessTestAction checkTableReadWrite = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), null, null,
          Permission.Action.READ, Permission.Action.WRITE);
        return null;
      }
    };

    verifyAllowed(checkTableReadWrite, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE);
    verifyDenied(checkTableReadWrite, USER_RW, USER_RO, USER_QUAL, USER_NONE);

    AccessTestAction checkColumnRead = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), TEST_FAMILY, null,
          Permission.Action.READ);
        return null;
      }
    };

    verifyAllowed(checkColumnRead, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE, USER_RW,
      USER_RO);
    verifyDenied(checkColumnRead, USER_QUAL, USER_NONE);

    AccessTestAction checkColumnReadWrite = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), TEST_FAMILY, null,
          Permission.Action.READ, Permission.Action.WRITE);
        return null;
      }
    };

    verifyAllowed(checkColumnReadWrite, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE,
      USER_RW);
    verifyDenied(checkColumnReadWrite, USER_RO, USER_QUAL, USER_NONE);

    AccessTestAction checkQualifierRead = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), TEST_FAMILY, TEST_Q1,
          Permission.Action.READ);
        return null;
      }
    };

    verifyAllowed(checkQualifierRead, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE, USER_RW,
      USER_RO, USER_QUAL);
    verifyDenied(checkQualifierRead, USER_NONE);

    AccessTestAction checkQualifierReadWrite = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), TEST_FAMILY, TEST_Q1,
          Permission.Action.READ, Permission.Action.WRITE);
        return null;
      }
    };

    verifyAllowed(checkQualifierReadWrite, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE,
      USER_RW, USER_QUAL);
    verifyDenied(checkQualifierReadWrite, USER_RO, USER_NONE);

    AccessTestAction checkMultiQualifierRead = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), new Permission[] {
          new TablePermission(TEST_TABLE.getTableName(), TEST_FAMILY, TEST_Q1,
            Permission.Action.READ),
          new TablePermission(TEST_TABLE.getTableName(), TEST_FAMILY, TEST_Q2,
            Permission.Action.READ), });
        return null;
      }
    };

    verifyAllowed(checkMultiQualifierRead, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE,
      USER_RW, USER_RO);
    verifyDenied(checkMultiQualifierRead, USER_QUAL, USER_NONE);

    AccessTestAction checkMultiQualifierReadWrite = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), new Permission[] {
            new TablePermission(TEST_TABLE.getTableName(), TEST_FAMILY, TEST_Q1,
              Permission.Action.READ, Permission.Action.WRITE),
            new TablePermission(TEST_TABLE.getTableName(), TEST_FAMILY, TEST_Q2,
              Permission.Action.READ, Permission.Action.WRITE), });
        return null;
      }
    };

    verifyAllowed(checkMultiQualifierReadWrite, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE,
      USER_RW);
    verifyDenied(checkMultiQualifierReadWrite, USER_RO, USER_QUAL, USER_NONE);
  }

  /** Test grants and revocations with authorization disabled */
  @Test
  public void testPassiveGrantRevoke() throws Exception {

    // Add a test user

    User tblUser = User.createUserForTesting(TEST_UTIL.getConfiguration(), "tbluser",
      new String[0]);

    // If we check now, the test user won't have permissions

    AccessTestAction checkTableRead = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_UTIL, TEST_TABLE.getTableName(), TEST_FAMILY, null,
          Permission.Action.READ);
        return null;
      }
    };

    verifyDenied(tblUser, checkTableRead);

    // An actual read won't be denied

    AccessTestAction tableRead = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
             Table t = conn.getTable(TEST_TABLE.getTableName())) {
          t.get(new Get(TEST_ROW).addFamily(TEST_FAMILY));
        }
        return null;
      }
    };

    verifyAllowed(tblUser, tableRead);

    // Grant read perms to the test user

    grantOnTable(TEST_UTIL, tblUser.getShortName(), TEST_TABLE.getTableName(), TEST_FAMILY,
      null, Permission.Action.READ);

    // Now both the permission check and actual op will succeed

    verifyAllowed(tblUser, checkTableRead);
    verifyAllowed(tblUser, tableRead);

    // Revoke read perms from the test user

    revokeFromTable(TEST_UTIL, tblUser.getShortName(), TEST_TABLE.getTableName(), TEST_FAMILY,
      null, Permission.Action.READ);

    // Now the permission check will indicate revocation but the actual op will still succeed

    verifyDenied(tblUser, checkTableRead);
    verifyAllowed(tblUser, tableRead);
  }

  /** Test master observer */
  @Test
  public void testPassiveMasterOperations() throws Exception {

    // preCreateTable
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE.getTableName());
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        ACCESS_CONTROLLER.preCreateTable(ObserverContext.createAndPrepare(CP_ENV, null), htd,
          null);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preModifyTable
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE.getTableName());
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY2));
        ACCESS_CONTROLLER.preModifyTable(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE.getTableName(), htd);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preDeleteTable
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteTable(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE.getTableName());
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preTruncateTable
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preTruncateTable(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE.getTableName());
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preAddColumnFamily
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY2);
        ACCESS_CONTROLLER.preAddColumnFamily(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE.getTableName(), hcd);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preModifyColumnFamily
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY2);
        ACCESS_CONTROLLER.preModifyColumnFamily(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE.getTableName(), hcd);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preDeleteColumnFamily
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteColumnFamily(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE.getTableName(), TEST_FAMILY2);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preEnableTable
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preEnableTable(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE.getTableName());
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preDisableTable
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDisableTable(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE.getTableName());
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preMove
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HRegionInfo region = new HRegionInfo(TEST_TABLE.getTableName());
        ServerName srcServer = ServerName.valueOf("1.1.1.1", 1, 0);
        ServerName destServer = ServerName.valueOf("2.2.2.2", 2, 0);
        ACCESS_CONTROLLER.preMove(ObserverContext.createAndPrepare(CP_ENV, null), region,
          srcServer, destServer);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preAssign
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HRegionInfo region = new HRegionInfo(TEST_TABLE.getTableName());
        ACCESS_CONTROLLER.preAssign(ObserverContext.createAndPrepare(CP_ENV, null), region);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preUnassign
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HRegionInfo region = new HRegionInfo(TEST_TABLE.getTableName());
        ACCESS_CONTROLLER.preUnassign(ObserverContext.createAndPrepare(CP_ENV, null), region,
          true);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preBalance
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalance(ObserverContext.createAndPrepare(CP_ENV, null));
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preBalanceSwitch
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalanceSwitch(ObserverContext.createAndPrepare(CP_ENV, null),
          true);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preSnapshot
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        SnapshotDescription snapshot = SnapshotDescription.newBuilder()
          .setName("foo")
          .build();
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE.getTableName());
        ACCESS_CONTROLLER.preSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          snapshot, htd);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preListSnapshot
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        SnapshotDescription snapshot = SnapshotDescription.newBuilder()
          .setName("foo")
          .build();
        ACCESS_CONTROLLER.preListSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          snapshot);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preCloneSnapshot
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        SnapshotDescription snapshot = SnapshotDescription.newBuilder()
          .setName("foo")
          .build();
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE.getTableName());
        ACCESS_CONTROLLER.preCloneSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          snapshot, htd);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preRestoreSnapshot
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        SnapshotDescription snapshot = SnapshotDescription.newBuilder()
          .setName("foo")
          .build();
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE.getTableName());
        ACCESS_CONTROLLER.preRestoreSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          snapshot, htd);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preDeleteSnapshot
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        SnapshotDescription snapshot = SnapshotDescription.newBuilder()
          .setName("foo")
          .build();
        ACCESS_CONTROLLER.preDeleteSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          snapshot);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preGetTableDescriptors
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        List<TableName> tableNamesList = Lists.newArrayList();
        tableNamesList.add(TEST_TABLE.getTableName());
        List<HTableDescriptor> descriptors = Lists.newArrayList();
        ACCESS_CONTROLLER.preGetTableDescriptors(ObserverContext.createAndPrepare(CP_ENV, null),
          tableNamesList, descriptors, ".+");
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preGetTableNames
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        List<HTableDescriptor> descriptors = Lists.newArrayList();
        ACCESS_CONTROLLER.preGetTableNames(ObserverContext.createAndPrepare(CP_ENV, null),
          descriptors, ".+");
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preCreateNamespace
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        NamespaceDescriptor ns = NamespaceDescriptor.create("test").build();
        ACCESS_CONTROLLER.preCreateNamespace(ObserverContext.createAndPrepare(CP_ENV, null),
          ns);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preDeleteNamespace
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteNamespace(ObserverContext.createAndPrepare(CP_ENV, null),
          "test");
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preModifyNamespace
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        NamespaceDescriptor ns = NamespaceDescriptor.create("test").build();
        ACCESS_CONTROLLER.preModifyNamespace(ObserverContext.createAndPrepare(CP_ENV, null),
          ns);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preGetNamespaceDescriptor
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preGetNamespaceDescriptor(ObserverContext.createAndPrepare(CP_ENV,
            null),
          "test");
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preListNamespaceDescriptors
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        List<NamespaceDescriptor> descriptors = Lists.newArrayList();
        ACCESS_CONTROLLER.preListNamespaceDescriptors(ObserverContext.createAndPrepare(CP_ENV,
            null),
          descriptors);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preSetUserQuota
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Quotas quotas = Quotas.newBuilder().build();
        ACCESS_CONTROLLER.preSetUserQuota(ObserverContext.createAndPrepare(CP_ENV, null),
          "testuser", quotas);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preSetTableQuota
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Quotas quotas = Quotas.newBuilder().build();
        ACCESS_CONTROLLER.preSetTableQuota(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE.getTableName(), quotas);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preSetNamespaceQuota
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Quotas quotas = Quotas.newBuilder().build();
        ACCESS_CONTROLLER.preSetNamespaceQuota(ObserverContext.createAndPrepare(CP_ENV, null),
          "test", quotas);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

  }

  /** Test region server observer */
  @Test
  public void testPassiveRegionServerOperations() throws Exception {
    // preStopRegionServer
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preStopRegionServer(ObserverContext.createAndPrepare(RSCP_ENV, null));
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preMerge
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE.getTableName());
        Region region_a = mock(Region.class);
        when(region_a.getTableDesc()).thenReturn(htd);
        Region region_b = mock(Region.class);
        when(region_b.getTableDesc()).thenReturn(htd);
        ACCESS_CONTROLLER.preMerge(ObserverContext.createAndPrepare(RSCP_ENV, null), region_a,
          region_b);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preRollWALWriterRequest
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preRollWALWriterRequest(ObserverContext.createAndPrepare(RSCP_ENV,
          null));
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

  }

  /** Test region observer */
  @Test
  public void testPassiveRegionOperations() throws Exception {

    // preOpen
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preOpen(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preFlush
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preFlush(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preSplit
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSplit(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preGetOp
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        List<Cell> cells = Lists.newArrayList();
        ACCESS_CONTROLLER.preGetOp(ObserverContext.createAndPrepare(RCP_ENV, null),
          new Get(TEST_ROW), cells);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preExists
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preExists(ObserverContext.createAndPrepare(RCP_ENV, null),
          new Get(TEST_ROW), true);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // prePut
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.prePut(ObserverContext.createAndPrepare(RCP_ENV, null),
          new Put(TEST_ROW), new WALEdit(), Durability.USE_DEFAULT);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preDelete
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDelete(ObserverContext.createAndPrepare(RCP_ENV, null),
          new Delete(TEST_ROW), new WALEdit(), Durability.USE_DEFAULT);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preBatchMutate
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBatchMutate(ObserverContext.createAndPrepare(RCP_ENV, null),
          new MiniBatchOperationInProgress<Mutation>(null, null, null, 0, 0));
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preCheckAndPut
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCheckAndPut(ObserverContext.createAndPrepare(RCP_ENV, null),
          TEST_ROW, TEST_FAMILY, TEST_Q1, CompareFilter.CompareOp.EQUAL,
          new BinaryComparator("foo".getBytes()), new Put(TEST_ROW), true);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preCheckAndDelete
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCheckAndDelete(ObserverContext.createAndPrepare(RCP_ENV, null),
          TEST_ROW, TEST_FAMILY, TEST_Q1, CompareFilter.CompareOp.EQUAL,
          new BinaryComparator("foo".getBytes()), new Delete(TEST_ROW), true);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preAppend
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAppend(ObserverContext.createAndPrepare(RCP_ENV, null),
          new Append(TEST_ROW));
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preIncrement
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preIncrement(ObserverContext.createAndPrepare(RCP_ENV, null),
          new Increment(TEST_ROW));
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preScannerOpen
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preScannerOpen(ObserverContext.createAndPrepare(RCP_ENV, null),
          new Scan(), mock(RegionScanner.class));
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

    // preBulkLoadHFile
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        List<Pair<byte[], String>> paths = Lists.newArrayList();
        ACCESS_CONTROLLER.preBulkLoadHFile(ObserverContext.createAndPrepare(RCP_ENV, null),
          paths);
        return null;
      }
    }, SUPERUSER, USER_ADMIN, USER_RW, USER_RO, USER_OWNER, USER_CREATE, USER_QUAL, USER_NONE);

  }

  @Test
  public void testPassiveCellPermissions() throws Exception {
    final Configuration conf = TEST_UTIL.getConfiguration();

    // store two sets of values, one store with a cell level ACL, and one without
    verifyAllowed(new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection connection = ConnectionFactory.createConnection(conf);
            Table t = connection.getTable(TEST_TABLE.getTableName())) {
          Put p;
          // with ro ACL
          p = new Put(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q1, ZERO);
          p.setACL(USER_NONE.getShortName(), new Permission(Action.READ));
          t.put(p);
          // with rw ACL
          p = new Put(TEST_ROW).addColumn(TEST_FAMILY, TEST_Q2, ZERO);
          p.setACL(USER_NONE.getShortName(), new Permission(Action.READ, Action.WRITE));
          t.put(p);
          // no ACL
          p = new Put(TEST_ROW)
              .addColumn(TEST_FAMILY, TEST_Q3, ZERO)
              .addColumn(TEST_FAMILY, TEST_Q4, ZERO);
          t.put(p);
        }
        return null;
      }
    }, USER_OWNER);

    // check that a scan over the test data returns the expected number of KVs

    final List<Cell> scanResults = Lists.newArrayList();

    AccessTestAction scanAction = new AccessTestAction() {
      @Override
      public List<Cell> run() throws Exception {
        Scan scan = new Scan();
        scan.setStartRow(TEST_ROW);
        scan.setStopRow(Bytes.add(TEST_ROW, new byte[]{ 0 } ));
        scan.addFamily(TEST_FAMILY);
        Connection connection = ConnectionFactory.createConnection(conf);
        Table t = connection.getTable(TEST_TABLE.getTableName());
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
          connection.close();
        }
        return scanResults;
      }
    };

    // owner will see all values
    scanResults.clear();
    verifyAllowed(scanAction, USER_OWNER);
    assertEquals(4, scanResults.size());

    // other user will also see 4 values
    // if cell filtering was active, we would only see 2 values
    scanResults.clear();
    verifyAllowed(scanAction, USER_NONE);
    assertEquals(4, scanResults.size());
  }

}
