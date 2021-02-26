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
package org.apache.hadoop.hbase.security.access;

import static org.apache.hadoop.hbase.AuthUtil.toGroupEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContextImpl;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
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
 * Performs checks for reference counting w.r.t. AuthManager which is used by
 * AccessController.
 *
 * NOTE: Only one test in  here. In AMv2, there is problem deleting because
 * we are missing auth. For now disabled. See the cleanup method.
 */
@Category({SecurityTests.class, MediumTests.class})
public class TestAccessController3 extends SecureTestUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAccessController3.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAccessController.class);
  private static TableName TEST_TABLE = TableName.valueOf("testtable1");
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  /** The systemUserConnection created here is tied to the system user. In case, you are planning
   * to create AccessTestAction, DON'T use this systemUserConnection as the 'doAs' user
   * gets  eclipsed by the system user. */
  private static Connection systemUserConnection;


  // user with all permissions
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
  // user with admin rights on the column family
  private static User USER_ADMIN_CF;

  private static final String GROUP_ADMIN = "group_admin";
  private static final String GROUP_CREATE = "group_create";
  private static final String GROUP_READ = "group_read";
  private static final String GROUP_WRITE = "group_write";

  private static User USER_GROUP_ADMIN;
  private static User USER_GROUP_CREATE;
  private static User USER_GROUP_READ;
  private static User USER_GROUP_WRITE;

  // TODO: convert this test to cover the full matrix in
  // https://hbase.apache.org/book/appendix_acl_matrix.html
  // creating all Scope x Permission combinations

  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static MasterCoprocessorEnvironment CP_ENV;
  private static AccessController ACCESS_CONTROLLER;
  private static RegionServerCoprocessorEnvironment RSCP_ENV;
  private static RegionCoprocessorEnvironment RCP_ENV;

  private static boolean callSuperTwice = true;

  @Rule
  public TestName name = new TestName();

  // class with faulty stop() method, controlled by flag
  public static class FaultyAccessController extends AccessController {
    public FaultyAccessController() {
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
      super.stop(env);
      if (callSuperTwice) {
        super.stop(env);
      }
    }
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    // Enable security
    enableSecurity(conf);
    String accessControllerClassName = FaultyAccessController.class.getName();
    // In this particular test case, we can't use SecureBulkLoadEndpoint because its doAs will fail
    // to move a file for a random user
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, accessControllerClassName);
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);

    // Enable EXEC permission checking
    conf.setBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY, true);

    TEST_UTIL.startMiniCluster();
    MasterCoprocessorHost cpHost =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterCoprocessorHost();
    cpHost.load(FaultyAccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = (AccessController) cpHost.findCoprocessor(accessControllerClassName);
    CP_ENV = cpHost.createEnvironment(ACCESS_CONTROLLER, Coprocessor.PRIORITY_HIGHEST, 1, conf);
    RegionServerCoprocessorHost rsHost;
    do {
      rsHost = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
          .getRegionServerCoprocessorHost();
    } while (rsHost == null);
    RSCP_ENV = rsHost.createEnvironment(ACCESS_CONTROLLER, Coprocessor.PRIORITY_HIGHEST, 1, conf);

    // Wait for the ACL table to become available
    TEST_UTIL.waitUntilAllRegionsAssigned(PermissionStorage.ACL_TABLE_NAME);

    // create a set of test users
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER_ADMIN = User.createUserForTesting(conf, "admin2", new String[0]);
    USER_RW = User.createUserForTesting(conf, "rwuser", new String[0]);
    USER_RO = User.createUserForTesting(conf, "rouser", new String[0]);
    USER_OWNER = User.createUserForTesting(conf, "owner", new String[0]);
    USER_CREATE = User.createUserForTesting(conf, "tbl_create", new String[0]);
    USER_NONE = User.createUserForTesting(conf, "nouser", new String[0]);
    USER_ADMIN_CF = User.createUserForTesting(conf, "col_family_admin", new String[0]);

    USER_GROUP_ADMIN =
        User.createUserForTesting(conf, "user_group_admin", new String[] { GROUP_ADMIN });
    USER_GROUP_CREATE =
        User.createUserForTesting(conf, "user_group_create", new String[] { GROUP_CREATE });
    USER_GROUP_READ =
        User.createUserForTesting(conf, "user_group_read", new String[] { GROUP_READ });
    USER_GROUP_WRITE =
        User.createUserForTesting(conf, "user_group_write", new String[] { GROUP_WRITE });

    systemUserConnection = TEST_UTIL.getConnection();
    setUpTableAndUserPermissions();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    assertEquals(1, TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().size());
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().get(0).
      getRegionServer();
    // Strange place for an assert.
    assertFalse("RegionServer should have ABORTED (FaultyAccessController)", rs.isAborted());
    cleanUp();
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void setUpTableAndUserPermissions() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    hcd.setMaxVersions(100);
    htd.addFamily(hcd);
    htd.setOwner(USER_OWNER);
    createTable(TEST_UTIL, htd, new byte[][] { Bytes.toBytes("s") });

    HRegion region = TEST_UTIL.getHBaseCluster().getRegions(TEST_TABLE).get(0);
    RegionCoprocessorHost rcpHost = region.getCoprocessorHost();
    RCP_ENV = rcpHost.createEnvironment(ACCESS_CONTROLLER, Coprocessor.PRIORITY_HIGHEST, 1, conf);

    // Set up initial grants

    grantGlobal(TEST_UTIL, USER_ADMIN.getShortName(),
      Permission.Action.ADMIN,
      Permission.Action.CREATE,
      Permission.Action.READ,
      Permission.Action.WRITE);

    grantOnTable(TEST_UTIL, USER_RW.getShortName(),
      TEST_TABLE, TEST_FAMILY, null,
      Permission.Action.READ,
      Permission.Action.WRITE);

    // USER_CREATE is USER_RW plus CREATE permissions
    grantOnTable(TEST_UTIL, USER_CREATE.getShortName(),
      TEST_TABLE, null, null,
      Permission.Action.CREATE,
      Permission.Action.READ,
      Permission.Action.WRITE);

    grantOnTable(TEST_UTIL, USER_RO.getShortName(),
      TEST_TABLE, TEST_FAMILY, null,
      Permission.Action.READ);

    grantOnTable(TEST_UTIL, USER_ADMIN_CF.getShortName(),
      TEST_TABLE, TEST_FAMILY,
      null, Permission.Action.ADMIN, Permission.Action.CREATE);

    grantGlobal(TEST_UTIL, toGroupEntry(GROUP_ADMIN), Permission.Action.ADMIN);
    grantGlobal(TEST_UTIL, toGroupEntry(GROUP_CREATE), Permission.Action.CREATE);
    grantGlobal(TEST_UTIL, toGroupEntry(GROUP_READ), Permission.Action.READ);
    grantGlobal(TEST_UTIL, toGroupEntry(GROUP_WRITE), Permission.Action.WRITE);

    assertEquals(5, PermissionStorage.getTablePermissions(conf, TEST_TABLE).size());
    try {
      assertEquals(5, AccessControlClient.getUserPermissions(systemUserConnection,
          TEST_TABLE.toString()).size());
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.getUserPermissions. ", e);
    }
  }

  private static void cleanUp() throws Exception {
    // Clean the _acl_ table
    // TODO: Skipping delete because of access issues w/ AMv2.
    // AMv1 seems to crash servers on exit too for same lack of
    // auth perms but it gets hung up.
    try {
      deleteTable(TEST_UTIL, TEST_TABLE);
    } catch (TableNotFoundException ex) {
      // Test deleted the table, no problem
      LOG.info("Test deleted table " + TEST_TABLE);
    }
    // Verify all table/namespace permissions are erased
    assertEquals(0, PermissionStorage.getTablePermissions(conf, TEST_TABLE).size());
    assertEquals(0,
      PermissionStorage.getNamespacePermissions(conf, TEST_TABLE.getNamespaceAsString()).size());
  }

  @Test
  public void testTableCreate() throws Exception {
    AccessTestAction createTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        ACCESS_CONTROLLER.preCreateTable(ObserverContextImpl.createAndPrepare(CP_ENV), htd, null);
        return null;
      }
    };

    // verify that superuser can create tables
    verifyAllowed(createTable, SUPERUSER, USER_ADMIN, USER_GROUP_CREATE, USER_GROUP_ADMIN);

    // all others should be denied
    verifyDenied(createTable, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE);
  }

}
