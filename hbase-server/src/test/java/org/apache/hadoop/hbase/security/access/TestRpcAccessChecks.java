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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * This class tests operations in MasterRpcServices which require ADMIN access.
 * It doesn't test all operations which require ADMIN access, only those which get vetted within
 * MasterRpcServices at the point of entry itself (unlike old approach of using
 * hooks in AccessController).
 *
 * Sidenote:
 * There is one big difference between how security tests for AccessController hooks work, and how
 * the tests in this class for security in MasterRpcServices work.
 * The difference arises because of the way AC & MasterRpcServices get the user.
 *
 * In AccessController, it first checks if there is an active rpc user in ObserverContext. If not,
 * it uses UserProvider for current user. This *might* make sense in the context of coprocessors,
 * because they can be called outside the context of RPCs.
 * But in the context of MasterRpcServices, only one way makes sense - RPCServer.getRequestUser().
 *
 * In AC tests, when we do FooUser.runAs on AccessController instance directly, it bypasses
 * the rpc framework completely, but works because UserProvider provides the correct user, i.e.
 * FooUser in this case.
 *
 * But this doesn't work for the tests here, so we go around by doing complete RPCs.
 */
@Category({SecurityTests.class, MediumTests.class})
public class TestRpcAccessChecks {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRpcAccessChecks.class);

  @Rule
  public final TestName TEST_NAME = new TestName();

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  // user granted with all global permission
  private static User USER_ADMIN;
  // user without admin permissions
  private static User USER_NON_ADMIN;
  // user in supergroup
  private static User USER_IN_SUPERGROUPS;
  // user with global permission but not a superuser
  private static User USER_ADMIN_NOT_SUPER;

  private static final String GROUP_ADMIN = "admin_group";
  private static User USER_GROUP_ADMIN;

  // Dummy service to test execService calls. Needs to be public so can be loaded as Coprocessor.
  public static class DummyCpService implements MasterCoprocessor, RegionServerCoprocessor {
    public DummyCpService() {}

    @Override
    public Iterable<Service> getServices() {
      return Collections.singleton(mock(TestRpcServiceProtos.TestProtobufRpcProto.class));
    }
  }

  private static void enableSecurity(Configuration conf) throws IOException {
    conf.set("hadoop.security.authorization", "false");
    conf.set("hadoop.security.authentication", "simple");
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, AccessController.class.getName() +
      "," + DummyCpService.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, AccessController.class.getName() +
      "," + DummyCpService.class.getName());
    conf.set(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, "true");
    SecureTestUtil.configureSuperuser(conf);
  }

  @BeforeClass
  public static void setup() throws Exception {
    conf = TEST_UTIL.getConfiguration();

    // Enable security
    enableSecurity(conf);

    // Create users
    // admin is superuser as well.
    USER_ADMIN = User.createUserForTesting(conf, "admin", new String[0]);
    USER_NON_ADMIN = User.createUserForTesting(conf, "non_admin", new String[0]);
    USER_GROUP_ADMIN =
        User.createUserForTesting(conf, "user_group_admin", new String[] { GROUP_ADMIN });
    USER_IN_SUPERGROUPS =
        User.createUserForTesting(conf, "user_in_supergroup", new String[] { "supergroup" });
    USER_ADMIN_NOT_SUPER = User.createUserForTesting(conf, "normal_admin", new String[0]);

    TEST_UTIL.startMiniCluster();
    // Wait for the ACL table to become available
    TEST_UTIL.waitUntilAllRegionsAssigned(PermissionStorage.ACL_TABLE_NAME);

    // Assign permissions to groups
    SecureTestUtil.grantGlobal(TEST_UTIL, toGroupEntry(GROUP_ADMIN),
      Permission.Action.ADMIN, Permission.Action.CREATE);
    SecureTestUtil.grantGlobal(TEST_UTIL, USER_ADMIN_NOT_SUPER.getShortName(),
      Permission.Action.ADMIN);
  }

  interface Action {
    void run(Admin admin) throws Exception;
  }

  private void verifyAllowed(User user, Action action) throws Exception {
    user.runAs((PrivilegedExceptionAction<?>) () -> {
      try (Connection conn = ConnectionFactory.createConnection(conf);
          Admin admin = conn.getAdmin()) {
        action.run(admin);
      } catch (IOException e) {
        fail(e.toString());
      }
      return null;
    });
  }

  private void verifyDenied(User user, Action action) throws Exception {
    user.runAs((PrivilegedExceptionAction<?>) () -> {
      boolean accessDenied = false;
      try (Connection conn = ConnectionFactory.createConnection(conf);
          Admin admin = conn.getAdmin()) {
        action.run(admin);
      } catch (AccessDeniedException e) {
        accessDenied = true;
      }
      assertTrue("Expected access to be denied", accessDenied);
      return null;
    });
  }

  private void verifiedDeniedServiceException(User user, Action action) throws Exception {
    user.runAs((PrivilegedExceptionAction<?>) () -> {
      boolean accessDenied = false;
      try (Connection conn = ConnectionFactory.createConnection(conf);
          Admin admin = conn.getAdmin()) {
        action.run(admin);
      } catch (ServiceException e) {
        // For MasterRpcServices.execService.
        if (e.getCause() instanceof AccessDeniedException) {
          accessDenied = true;
        }
      }
      assertTrue("Expected access to be denied", accessDenied);
      return null;
    });

  }

  private void verifyAdminCheckForAction(Action action) throws Exception {
    verifyAllowed(USER_ADMIN, action);
    verifyAllowed(USER_GROUP_ADMIN, action);
    verifyDenied(USER_NON_ADMIN, action);
  }

  @Test
  public void testEnableCatalogJanitor() throws Exception {
    verifyAdminCheckForAction((admin) -> admin.enableCatalogJanitor(true));
  }

  @Test
  public void testRunCatalogJanitor() throws Exception {
    verifyAdminCheckForAction((admin) -> admin.runCatalogJanitor());
  }

  @Test
  public void testCleanerChoreRunning() throws Exception {
    verifyAdminCheckForAction((admin) -> admin.cleanerChoreSwitch(true));
  }

  @Test
  public void testRunCleanerChore() throws Exception {
    verifyAdminCheckForAction((admin) -> admin.runCleanerChore());
  }

  @Test
  public void testExecProcedure() throws Exception {
    verifyAdminCheckForAction((admin) -> {
      // Using existing table instead of creating a new one.
      admin.execProcedure("flush-table-proc", TableName.META_TABLE_NAME.getNameAsString(),
          new HashMap<>());
    });
  }

  @Test
  public void testExecService() throws Exception {
    Action action = (admin) -> {
      TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface service =
          TestRpcServiceProtos.TestProtobufRpcProto.newBlockingStub(admin.coprocessorService());
      service.ping(null, TestProtos.EmptyRequestProto.getDefaultInstance());
    };

    verifyAllowed(USER_ADMIN, action);
    verifyAllowed(USER_GROUP_ADMIN, action);
    // This is same as above verifyAccessDenied
    verifiedDeniedServiceException(USER_NON_ADMIN, action);
  }

  @Test
  public void testExecProcedureWithRet() throws Exception {
    verifyAdminCheckForAction((admin) -> {
      // Using existing table instead of creating a new one.
      admin.execProcedureWithReturn("flush-table-proc", TableName.META_TABLE_NAME.getNameAsString(),
          new HashMap<>());
    });
  }

  @Test
  public void testNormalize() throws Exception {
    verifyAdminCheckForAction((admin) -> admin.normalize());
  }

  @Test
  public void testSetNormalizerRunning() throws Exception {
    verifyAdminCheckForAction((admin) -> admin.normalizerSwitch(true));
  }

  @Test
  public void testExecRegionServerService() throws Exception {
    Action action = (admin) -> {
      ServerName serverName = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
      TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface service =
          TestRpcServiceProtos.TestProtobufRpcProto.newBlockingStub(
              admin.coprocessorService(serverName));
      service.ping(null, TestProtos.EmptyRequestProto.getDefaultInstance());
    };

    verifyAllowed(USER_ADMIN, action);
    verifyAllowed(USER_GROUP_ADMIN, action);
    verifiedDeniedServiceException(USER_NON_ADMIN, action);
  }

  @Test
  public void testTableFlush() throws Exception {
    TableName tn = TableName.valueOf(TEST_NAME.getMethodName());
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tn)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f1")).build();
    Action adminAction = (admin) -> {
      admin.createTable(desc);
      // Avoid giving a global permission which may screw up other tests
      SecureTestUtil.grantOnTable(
          TEST_UTIL, USER_NON_ADMIN.getShortName(), tn, null, null, Permission.Action.READ,
          Permission.Action.WRITE, Permission.Action.CREATE);
    };
    verifyAllowed(USER_ADMIN, adminAction);

    Action userAction = (admin) -> {
      Connection conn = admin.getConnection();
      final byte[] rowKey = Bytes.toBytes("row1");
      final byte[] col = Bytes.toBytes("q1");
      final byte[] val = Bytes.toBytes("v1");
      try (Table table = conn.getTable(tn)) {
        // Write a value
        Put p = new Put(rowKey);
        p.addColumn(Bytes.toBytes("f1"), col, val);
        table.put(p);
        // Flush should not require ADMIN permission
        admin.flush(tn);
        // Nb: ideally, we would verify snapshot permission too (as that was fixed in the
        //   regression HBASE-20185) but taking a snapshot requires ADMIN permission which
        //   masks the root issue.
        // Make sure we read the value
        Result result = table.get(new Get(rowKey));
        assertFalse(result.isEmpty());
        Cell c = result.getColumnLatestCell(Bytes.toBytes("f1"), col);
        assertArrayEquals(val, CellUtil.cloneValue(c));
      }
    };
    verifyAllowed(USER_NON_ADMIN, userAction);
  }

  @Test
  public void testTableFlushAndSnapshot() throws Exception {
    TableName tn = TableName.valueOf(TEST_NAME.getMethodName());
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tn)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f1")).build();
    Action adminAction = (admin) -> {
      admin.createTable(desc);
      // Giving ADMIN here, but only on this table, *not* globally
      SecureTestUtil.grantOnTable(
          TEST_UTIL, USER_NON_ADMIN.getShortName(), tn, null, null, Permission.Action.READ,
          Permission.Action.WRITE, Permission.Action.CREATE, Permission.Action.ADMIN);
    };
    verifyAllowed(USER_ADMIN, adminAction);

    Action userAction = (admin) -> {
      Connection conn = admin.getConnection();
      final byte[] rowKey = Bytes.toBytes("row1");
      final byte[] col = Bytes.toBytes("q1");
      final byte[] val = Bytes.toBytes("v1");
      try (Table table = conn.getTable(tn)) {
        // Write a value
        Put p = new Put(rowKey);
        p.addColumn(Bytes.toBytes("f1"), col, val);
        table.put(p);
        // Flush should not require ADMIN permission
        admin.flush(tn);
        // Table admin should be sufficient to snapshot this table
        admin.snapshot(tn.getNameAsString() + "_snapshot1", tn);
        // Read the value just because
        Result result = table.get(new Get(rowKey));
        assertFalse(result.isEmpty());
        Cell c = result.getColumnLatestCell(Bytes.toBytes("f1"), col);
        assertArrayEquals(val, CellUtil.cloneValue(c));
      }
    };
    verifyAllowed(USER_NON_ADMIN, userAction);
  }

  @Test
  public void testGrantDeniedOnSuperUsersGroups() {
    /** User */
    try {
      // Global
      SecureTestUtil.grantGlobal(USER_ADMIN_NOT_SUPER, TEST_UTIL, USER_ADMIN.getShortName(),
        Permission.Action.ADMIN, Permission.Action.CREATE);
      fail("Granting superuser's global permissions is not allowed.");
    } catch (Exception e) {
    }
    try {
      // Namespace
      SecureTestUtil.grantOnNamespace(USER_ADMIN_NOT_SUPER, TEST_UTIL, USER_ADMIN.getShortName(),
        TEST_NAME.getMethodName(),
        Permission.Action.ADMIN, Permission.Action.CREATE);
      fail("Granting superuser's namespace permissions is not allowed.");
    } catch (Exception e) {
    }
    try {
      // Table
      SecureTestUtil.grantOnTable(USER_ADMIN_NOT_SUPER, TEST_UTIL, USER_ADMIN.getName(),
        TableName.valueOf(TEST_NAME.getMethodName()), null, null,
        Permission.Action.ADMIN, Permission.Action.CREATE);
      fail("Granting superuser's table permissions is not allowed.");
    } catch (Exception e) {
    }

    /** Group */
    try {
      SecureTestUtil.grantGlobal(USER_ADMIN_NOT_SUPER, TEST_UTIL,
        USER_IN_SUPERGROUPS.getShortName(), Permission.Action.ADMIN, Permission.Action.CREATE);
      fail("Granting superuser's global permissions is not allowed.");
    } catch (Exception e) {
    }
  }

  @Test
  public void testRevokeDeniedOnSuperUsersGroups() {
    /** User */
    try {
      // Global
      SecureTestUtil.revokeGlobal(USER_ADMIN_NOT_SUPER, TEST_UTIL, USER_ADMIN.getShortName(),
        Permission.Action.ADMIN);
      fail("Revoking superuser's global permissions is not allowed.");
    } catch (Exception e) {
    }
    try {
      // Namespace
      SecureTestUtil.revokeFromNamespace(USER_ADMIN_NOT_SUPER, TEST_UTIL, USER_ADMIN.getShortName(),
        TEST_NAME.getMethodName(), Permission.Action.ADMIN);
      fail("Revoking superuser's namespace permissions is not allowed.");
    } catch (Exception e) {
    }
    try {
      // Table
      SecureTestUtil.revokeFromTable(USER_ADMIN_NOT_SUPER, TEST_UTIL, USER_ADMIN.getName(),
        TableName.valueOf(TEST_NAME.getMethodName()), null, null,
        Permission.Action.ADMIN);
      fail("Revoking superuser's table permissions is not allowed.");
    } catch (Exception e) {
    }

    /** Group */
    try {
      // Global revoke
      SecureTestUtil.revokeGlobal(USER_ADMIN_NOT_SUPER, TEST_UTIL,
        AuthUtil.toGroupEntry("supergroup"),
        Permission.Action.ADMIN, Permission.Action.CREATE);
      fail("Revoking supergroup's permissions is not allowed.");
    } catch (Exception e) {
    }
  }
}
