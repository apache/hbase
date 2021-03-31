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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Hbck;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.client.security.SecurityCapability;

import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContextImpl;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.CountRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.CountResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.HelloRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.HelloResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.IncrementCountRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.IncrementCountResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.NoopRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.NoopResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.PingRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.PingResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.PingService;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsRequest;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Performs authorization checks for common operations, according to different
 * levels of authorized users.
 */
@Category({SecurityTests.class, LargeTests.class})
public class TestAccessController extends SecureTestUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAccessController.class);

  private static final FsPermission FS_PERMISSION_ALL = FsPermission.valueOf("-rwxrwxrwx");
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

  private static TableName TEST_TABLE2 = TableName.valueOf("testtable2");
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static byte[] TEST_QUALIFIER = Bytes.toBytes("q1");
  private static byte[] TEST_ROW = Bytes.toBytes("r1");

  private static MasterCoprocessorEnvironment CP_ENV;
  private static AccessController ACCESS_CONTROLLER;
  private static RegionServerCoprocessorEnvironment RSCP_ENV;
  private static RegionCoprocessorEnvironment RCP_ENV;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    // Up the handlers; this test needs more than usual.
    conf.setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);

    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
      MyShellBasedUnixGroupsMapping.class.getName());
    UserGroupInformation.setConfiguration(conf);

    // Enable security
    enableSecurity(conf);
    // In this particular test case, we can't use SecureBulkLoadEndpoint because its doAs will fail
    // to move a file for a random user
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    // Verify enableSecurity sets up what we require
    verifyConfiguration(conf);

    // Enable EXEC permission checking
    conf.setBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY, true);

    TEST_UTIL.startMiniCluster();
    MasterCoprocessorHost masterCpHost =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterCoprocessorHost();
    masterCpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = masterCpHost.findCoprocessor(AccessController.class);
    CP_ENV = masterCpHost.createEnvironment(
        ACCESS_CONTROLLER, Coprocessor.PRIORITY_HIGHEST, 1, conf);
    RegionServerCoprocessorHost rsCpHost = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
      .getRegionServerCoprocessorHost();
    RSCP_ENV = rsCpHost.createEnvironment(ACCESS_CONTROLLER, Coprocessor.PRIORITY_HIGHEST, 1, conf);

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
    int size = 0;
    try {
      size = AccessControlClient.getUserPermissions(systemUserConnection, TEST_TABLE.toString())
          .size();
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.getUserPermissions. ", e);
      fail("error during call of AccessControlClient.getUserPermissions.");
    }
    assertEquals(5, size);
  }

  private static void cleanUp() throws Exception {
    // Clean the _acl_ table
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
  public void testUnauthorizedShutdown() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override public Object run() throws Exception {
        HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
        master.shutdown();
        return null;
      }
    };
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testUnauthorizedStopMaster() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override public Object run() throws Exception {
        HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
        master.stopMaster();
        return null;
      }
    };

    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testUnauthorizedSetTableStateInMeta() throws Exception {
    AccessTestAction action = () -> {
      try(Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Hbck hbck = conn.getHbck()){
        hbck.setTableStateInMeta(new TableState(TEST_TABLE, TableState.State.DISABLED));
      }
      return null;
    };

    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testUnauthorizedSetRegionStateInMeta() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    final List<RegionInfo> regions = admin.getRegions(TEST_TABLE);
    RegionInfo closeRegion = regions.get(0);
    Map<String, RegionState.State> newStates = new HashMap<>();
    newStates.put(closeRegion.getEncodedName(), RegionState.State.CLOSED);
    AccessTestAction action = () -> {
      try(Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Hbck hbck = conn.getHbck()){
        hbck.setRegionStateInMeta(newStates);
      }
      return null;
    };

    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testUnauthorizedFixMeta() throws Exception {
    AccessTestAction action = () -> {
      try(Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Hbck hbck = conn.getHbck()){
        hbck.fixMeta();
      }
      return null;
    };

    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testSecurityCapabilities() throws Exception {
    List<SecurityCapability> capabilities = TEST_UTIL.getConnection().getAdmin()
      .getSecurityCapabilities();
    assertTrue("AUTHORIZATION capability is missing",
      capabilities.contains(SecurityCapability.AUTHORIZATION));
    assertTrue("CELL_AUTHORIZATION capability is missing",
      capabilities.contains(SecurityCapability.CELL_AUTHORIZATION));
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

  @Test
  public void testTableModify() throws Exception {
    AccessTestAction modifyTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        htd.addFamily(new HColumnDescriptor("fam_" + User.getCurrent().getShortName()));
        ACCESS_CONTROLLER.preModifyTable(ObserverContextImpl.createAndPrepare(CP_ENV), TEST_TABLE,
          null, htd);
        return null;
      }
    };

    verifyAllowed(modifyTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_GROUP_CREATE,
      USER_GROUP_ADMIN);
    verifyDenied(modifyTable, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testTableDelete() throws Exception {
    AccessTestAction deleteTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER
            .preDeleteTable(ObserverContextImpl.createAndPrepare(CP_ENV), TEST_TABLE);
        return null;
      }
    };

    verifyAllowed(deleteTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_GROUP_CREATE,
      USER_GROUP_ADMIN);
    verifyDenied(deleteTable, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testTableTruncate() throws Exception {
    AccessTestAction truncateTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER
            .preTruncateTable(ObserverContextImpl.createAndPrepare(CP_ENV),
              TEST_TABLE);
        return null;
      }
    };

    verifyAllowed(truncateTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_GROUP_CREATE,
      USER_GROUP_ADMIN);
    verifyDenied(truncateTable, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testTableDisable() throws Exception {
    AccessTestAction disableTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDisableTable(ObserverContextImpl.createAndPrepare(CP_ENV),
          TEST_TABLE);
        return null;
      }
    };

    AccessTestAction disableAclTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDisableTable(ObserverContextImpl.createAndPrepare(CP_ENV),
          PermissionStorage.ACL_TABLE_NAME);
        return null;
      }
    };

    verifyAllowed(disableTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_GROUP_CREATE,
      USER_GROUP_ADMIN);
    verifyDenied(disableTable, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);

    // No user should be allowed to disable _acl_ table
    verifyDenied(disableAclTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
      USER_GROUP_CREATE, USER_GROUP_ADMIN, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testTableEnable() throws Exception {
    AccessTestAction enableTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER
            .preEnableTable(ObserverContextImpl.createAndPrepare(CP_ENV), TEST_TABLE);
        return null;
      }
    };

    verifyAllowed(enableTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_GROUP_CREATE,
      USER_GROUP_ADMIN);
    verifyDenied(enableTable, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  public static class TestTableDDLProcedure extends Procedure<MasterProcedureEnv>
  implements TableProcedureInterface {
    private TableName tableName;

    public TestTableDDLProcedure() {
    }

    public TestTableDDLProcedure(final MasterProcedureEnv env, final TableName tableName)
        throws IOException {
      this.tableName = tableName;
      this.setTimeout(180000); // Timeout in 3 minutes
      this.setOwner(env.getRequestUser());
    }

    @Override
    public TableName getTableName() {
      return tableName;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.EDIT;
    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return true;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
      TestProcedureProtos.TestTableDDLStateData.Builder testTableDDLMsg =
          TestProcedureProtos.TestTableDDLStateData.newBuilder()
          .setTableName(tableName.getNameAsString());
      serializer.serialize(testTableDDLMsg.build());
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
      TestProcedureProtos.TestTableDDLStateData testTableDDLMsg =
          serializer.deserialize(TestProcedureProtos.TestTableDDLStateData.class);
      tableName = TableName.valueOf(testTableDDLMsg.getTableName());
    }

    @Override
    protected Procedure[] execute(MasterProcedureEnv env) throws ProcedureYieldException,
        InterruptedException {
      // Not letting the procedure to complete until timed out
      setState(ProcedureState.WAITING_TIMEOUT);
      return null;
    }

    @Override
    protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    }
  }

  @Test
  public void testAbortProcedure() throws Exception {
    long procId = 1;
    AccessTestAction abortProcedureAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAbortProcedure(ObserverContextImpl.createAndPrepare(CP_ENV), procId);
       return null;
      }
    };

    verifyAllowed(abortProcedureAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
  }

  @Test
  public void testGetProcedures() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec =
        TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    Procedure proc = new TestTableDDLProcedure(procExec.getEnvironment(), tableName);
    proc.setOwner(USER_OWNER);
    procExec.submitProcedure(proc);
    final List<Procedure<MasterProcedureEnv>> procList = procExec.getProcedures();

    AccessTestAction getProceduresAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER
        .postGetProcedures(ObserverContextImpl.createAndPrepare(CP_ENV));
       return null;
      }
    };

    verifyAllowed(getProceduresAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyAllowed(getProceduresAction, USER_OWNER);
    verifyIfNull(
      getProceduresAction, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testGetLocks() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preGetLocks(ObserverContextImpl.createAndPrepare(CP_ENV));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testMove() throws Exception {
    List<HRegionLocation> regions;
    try (RegionLocator locator = systemUserConnection.getRegionLocator(TEST_TABLE)) {
      regions = locator.getAllRegionLocations();
    }
    HRegionLocation location = regions.get(0);
    final HRegionInfo hri = location.getRegionInfo();
    final ServerName server = location.getServerName();
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preMove(ObserverContextImpl.createAndPrepare(CP_ENV),
          hri, server, server);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testAssign() throws Exception {
    List<HRegionLocation> regions;
    try (RegionLocator locator = systemUserConnection.getRegionLocator(TEST_TABLE)) {
      regions = locator.getAllRegionLocations();
    }
    HRegionLocation location = regions.get(0);
    final HRegionInfo hri = location.getRegionInfo();
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAssign(ObserverContextImpl.createAndPrepare(CP_ENV), hri);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testUnassign() throws Exception {
    List<HRegionLocation> regions;
    try (RegionLocator locator = systemUserConnection.getRegionLocator(TEST_TABLE)) {
      regions = locator.getAllRegionLocations();
    }
    HRegionLocation location = regions.get(0);
    final HRegionInfo hri = location.getRegionInfo();
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preUnassign(ObserverContextImpl.createAndPrepare(CP_ENV), hri);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testRegionOffline() throws Exception {
    List<HRegionLocation> regions;
    try (RegionLocator locator = systemUserConnection.getRegionLocator(TEST_TABLE)) {
      regions = locator.getAllRegionLocations();
    }
    HRegionLocation location = regions.get(0);
    final HRegionInfo hri = location.getRegionInfo();
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preRegionOffline(ObserverContextImpl.createAndPrepare(CP_ENV), hri);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testSetSplitOrMergeEnabled() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetSplitOrMergeEnabled(ObserverContextImpl.createAndPrepare(CP_ENV),
          true, MasterSwitchType.MERGE);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testBalance() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalance(ObserverContextImpl.createAndPrepare(CP_ENV));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testBalanceSwitch() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalanceSwitch(ObserverContextImpl.createAndPrepare(CP_ENV), true);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testShutdown() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preShutdown(ObserverContextImpl.createAndPrepare(CP_ENV));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testStopMaster() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preStopMaster(ObserverContextImpl.createAndPrepare(CP_ENV));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  private void verifyWrite(AccessTestAction action) throws Exception {
    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE, USER_RW,
      USER_GROUP_WRITE);
    verifyDenied(action, USER_NONE, USER_RO, USER_GROUP_ADMIN, USER_GROUP_READ, USER_GROUP_CREATE);
  }

  @Test
  public void testSplitWithSplitRow() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    createTestTable(tableName);
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSplitRegion(
            ObserverContextImpl.createAndPrepare(CP_ENV),
            tableName,
            TEST_ROW);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testFlush() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preFlush(ObserverContextImpl.createAndPrepare(RCP_ENV),
          FlushLifeCycleTracker.DUMMY);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE, USER_GROUP_CREATE,
      USER_GROUP_ADMIN);
    verifyDenied(action, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testCompact() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCompact(ObserverContextImpl.createAndPrepare(RCP_ENV), null, null,
          ScanType.COMPACT_RETAIN_DELETES, null, null);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE, USER_GROUP_CREATE,
      USER_GROUP_ADMIN);
    verifyDenied(action, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  private void verifyRead(AccessTestAction action) throws Exception {
    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE, USER_RW, USER_RO,
      USER_GROUP_READ);
    verifyDenied(action, USER_NONE, USER_GROUP_CREATE, USER_GROUP_ADMIN, USER_GROUP_WRITE);
  }

  private void verifyReadWrite(AccessTestAction action) throws Exception {
    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE, USER_RW);
    verifyDenied(action, USER_NONE, USER_RO, USER_GROUP_ADMIN, USER_GROUP_CREATE, USER_GROUP_READ,
        USER_GROUP_WRITE);
  }

  @Test
  public void testRead() throws Exception {
    // get action
    AccessTestAction getAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Get g = new Get(TEST_ROW);
        g.addFamily(TEST_FAMILY);
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          t.get(g);
        }
        return null;
      }
    };
    verifyRead(getAction);

    // action for scanning
    AccessTestAction scanAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Scan s = new Scan();
        s.addFamily(TEST_FAMILY);
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table table = conn.getTable(TEST_TABLE)) {
          ResultScanner scanner = table.getScanner(s);
          try {
            for (Result r = scanner.next(); r != null; r = scanner.next()) {
              // do nothing
            }
          } finally {
            scanner.close();
          }
        }
        return null;
      }
    };
    verifyRead(scanAction);
  }

  @Test
  // test put, delete, increment
  public void testWrite() throws Exception {
    // put action
    AccessTestAction putAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Put p = new Put(TEST_ROW);
        p.addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(1));
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          t.put(p);
        }
        return null;
      }
    };
    verifyWrite(putAction);

    // delete action
    AccessTestAction deleteAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Delete d = new Delete(TEST_ROW);
        d.addFamily(TEST_FAMILY);
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          t.delete(d);
        }
        return null;
      }
    };
    verifyWrite(deleteAction);

    // increment action
    AccessTestAction incrementAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Increment inc = new Increment(TEST_ROW);
        inc.addColumn(TEST_FAMILY, TEST_QUALIFIER, 1);
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE);) {
          t.increment(inc);
        }
        return null;
      }
    };
    verifyWrite(incrementAction);
  }

  @Test
  public void testReadWrite() throws Exception {
    // action for checkAndDelete
    AccessTestAction checkAndDeleteAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Delete d = new Delete(TEST_ROW);
        d.addFamily(TEST_FAMILY);
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE);) {
          t.checkAndMutate(TEST_ROW, TEST_FAMILY).qualifier(TEST_QUALIFIER)
              .ifEquals(Bytes.toBytes("test_value")).thenDelete(d);
        }
        return null;
      }
    };
    verifyReadWrite(checkAndDeleteAction);

    // action for checkAndPut()
    AccessTestAction checkAndPut = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Put p = new Put(TEST_ROW);
        p.addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(1));
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          t.checkAndMutate(TEST_ROW, TEST_FAMILY).qualifier(TEST_QUALIFIER)
              .ifEquals(Bytes.toBytes("test_value")).thenPut(p);
        }
        return null;
      }
    };
    verifyReadWrite(checkAndPut);
  }

  @Test
  public void testBulkLoad() throws Exception {
    try {
      FileSystem fs = TEST_UTIL.getTestFileSystem();
      final Path dir = TEST_UTIL.getDataTestDirOnTestFS("testBulkLoad");
      fs.mkdirs(dir);
      // need to make it globally writable
      // so users creating HFiles have write permissions
      fs.setPermission(dir, FS_PERMISSION_ALL);

      AccessTestAction bulkLoadAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          int numRows = 3;

          // Making the assumption that the test table won't split between the range
          byte[][][] hfileRanges = { { { (byte) 0 }, { (byte) 9 } } };

          Path bulkLoadBasePath = new Path(dir, new Path(User.getCurrent().getName()));
          new BulkLoadHelper(bulkLoadBasePath).initHFileData(TEST_FAMILY, TEST_QUALIFIER,
            hfileRanges, numRows, FS_PERMISSION_ALL).bulkLoadHFile(TEST_TABLE);
          return null;
        }
      };

      // User performing bulk loads must have privilege to read table metadata
      // (ADMIN or CREATE)
      verifyAllowed(bulkLoadAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE,
        USER_GROUP_CREATE, USER_GROUP_ADMIN);
      verifyDenied(bulkLoadAction, USER_RW, USER_NONE, USER_RO, USER_GROUP_READ, USER_GROUP_WRITE);
    } finally {
      // Reinit after the bulk upload
      TEST_UTIL.getAdmin().disableTable(TEST_TABLE);
      TEST_UTIL.getAdmin().enableTable(TEST_TABLE);
    }
  }

  private class BulkLoadAccessTestAction implements AccessTestAction {
    private FsPermission filePermission;
    private Path testDataDir;

    public BulkLoadAccessTestAction(FsPermission perm, Path testDataDir) {
      this.filePermission = perm;
      this.testDataDir = testDataDir;
    }

    @Override
    public Object run() throws Exception {
      FileSystem fs = TEST_UTIL.getTestFileSystem();
      fs.mkdirs(testDataDir);
      fs.setPermission(testDataDir, FS_PERMISSION_ALL);
      // Making the assumption that the test table won't split between the range
      byte[][][] hfileRanges = { { { (byte) 0 }, { (byte) 9 } } };
      Path bulkLoadBasePath = new Path(testDataDir, new Path(User.getCurrent().getName()));
      new BulkLoadHelper(bulkLoadBasePath)
          .initHFileData(TEST_FAMILY, TEST_QUALIFIER, hfileRanges, 3, filePermission)
          .bulkLoadHFile(TEST_TABLE);
      return null;
    }
  }

  @Test
  public void testBulkLoadWithoutWritePermission() throws Exception {
    // Use the USER_CREATE to initialize the source directory.
    Path testDataDir0 = TEST_UTIL.getDataTestDirOnTestFS("testBulkLoadWithoutWritePermission0");
    Path testDataDir1 = TEST_UTIL.getDataTestDirOnTestFS("testBulkLoadWithoutWritePermission1");
    AccessTestAction bulkLoadAction1 =
        new BulkLoadAccessTestAction(FsPermission.valueOf("-r-xr-xr-x"), testDataDir0);
    AccessTestAction bulkLoadAction2 =
        new BulkLoadAccessTestAction(FS_PERMISSION_ALL, testDataDir1);
    // Test the incorrect case.
    BulkLoadHelper.setPermission(TEST_UTIL.getTestFileSystem(),
      TEST_UTIL.getTestFileSystem().getWorkingDirectory(), FS_PERMISSION_ALL);
    try {
      USER_CREATE.runAs(bulkLoadAction1);
      fail("Should fail because the hbase user has no write permission on hfiles.");
    } catch (IOException e) {
    }
    // Ensure the correct case.
    USER_CREATE.runAs(bulkLoadAction2);
  }

  public static class BulkLoadHelper {
    private final FileSystem fs;
    private final Path loadPath;
    private final Configuration conf;

    public BulkLoadHelper(Path loadPath) throws IOException {
      fs = TEST_UTIL.getTestFileSystem();
      conf = TEST_UTIL.getConfiguration();
      loadPath = loadPath.makeQualified(fs);
      this.loadPath = loadPath;
    }

    private void createHFile(Path path,
        byte[] family, byte[] qualifier,
        byte[] startKey, byte[] endKey, int numRows) throws IOException {
      HFile.Writer writer = null;
      long now = System.currentTimeMillis();
      try {
        HFileContext context = new HFileContextBuilder().build();
        writer = HFile.getWriterFactory(conf, new CacheConfig(conf)).withPath(fs, path)
            .withFileContext(context).create();
        // subtract 2 since numRows doesn't include boundary keys
        for (byte[] key : Bytes.iterateOnSplits(startKey, endKey, true, numRows - 2)) {
          KeyValue kv = new KeyValue(key, family, qualifier, now, key);
          writer.append(kv);
        }
      } finally {
        if (writer != null) {
          writer.close();
        }
      }
    }

    private BulkLoadHelper initHFileData(byte[] family, byte[] qualifier, byte[][][] hfileRanges,
        int numRowsPerRange, FsPermission filePermission) throws Exception {
      Path familyDir = new Path(loadPath, Bytes.toString(family));
      fs.mkdirs(familyDir);
      int hfileIdx = 0;
      List<Path> hfiles = new ArrayList<>();
      for (byte[][] range : hfileRanges) {
        byte[] from = range[0];
        byte[] to = range[1];
        Path hfile = new Path(familyDir, "hfile_" + (hfileIdx++));
        hfiles.add(hfile);
        createHFile(hfile, family, qualifier, from, to, numRowsPerRange);
      }
      // set global read so RegionServer can move it
      setPermission(fs, loadPath, FS_PERMISSION_ALL);
      // Ensure the file permission as requested.
      for (Path hfile : hfiles) {
        setPermission(fs, hfile, filePermission);
      }
      return this;
    }

    private void bulkLoadHFile(TableName tableName) throws Exception {
      try (Connection conn = ConnectionFactory.createConnection(conf);
          Admin admin = conn.getAdmin();
          RegionLocator locator = conn.getRegionLocator(tableName);
          Table table = conn.getTable(tableName)) {
        TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(loadPath, admin, table, locator);
      }
    }

    private static void setPermission(FileSystem fs, Path dir, FsPermission perm)
        throws IOException {
      if (!fs.getFileStatus(dir).isDirectory()) {
        fs.setPermission(dir, perm);
      } else {
        for (FileStatus el : fs.listStatus(dir)) {
          fs.setPermission(el.getPath(), perm);
          setPermission(fs, el.getPath(), perm);
        }
      }
    }
  }

  @Test
  public void testAppend() throws Exception {

    AccessTestAction appendAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        byte[] row = TEST_ROW;
        byte[] qualifier = TEST_QUALIFIER;
        Put put = new Put(row);
        put.addColumn(TEST_FAMILY, qualifier, Bytes.toBytes(1));
        Append append = new Append(row);
        append.addColumn(TEST_FAMILY, qualifier, Bytes.toBytes(2));
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          t.put(put);
          t.append(append);
        }
        return null;
      }
    };

    verifyAllowed(appendAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE, USER_RW,
      USER_GROUP_WRITE);
    verifyDenied(appendAction, USER_RO, USER_NONE, USER_GROUP_CREATE, USER_GROUP_READ,
      USER_GROUP_ADMIN);
  }

  @Test
  public void testGrantRevoke() throws Exception {
    AccessTestAction grantAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          conn.getAdmin().grant(new UserPermission(USER_RO.getShortName(), Permission
              .newBuilder(TEST_TABLE).withFamily(TEST_FAMILY).withActions(Action.READ).build()),
            false);
        }
        return null;
      }
    };

    AccessTestAction revokeAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          conn.getAdmin().revoke(new UserPermission(USER_RO.getShortName(), Permission
              .newBuilder(TEST_TABLE).withFamily(TEST_FAMILY).withActions(Action.READ).build()));
        }
        return null;
      }
    };

    AccessTestAction getTablePermissionsAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          conn.getAdmin()
              .getUserPermissions(GetUserPermissionsRequest.newBuilder(TEST_TABLE).build());
        }
        return null;
      }
    };

    AccessTestAction getGlobalPermissionsAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          conn.getAdmin().getUserPermissions(GetUserPermissionsRequest.newBuilder().build());
        }
        return null;
      }
    };

    AccessTestAction preGrantAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preGrant(ObserverContextImpl.createAndPrepare(CP_ENV),
          new UserPermission(USER_RO.getShortName(), Permission.newBuilder(TEST_TABLE)
              .withFamily(TEST_FAMILY).withActions(Action.READ).build()),
          false);
        return null;
      }
    };

    AccessTestAction preRevokeAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preRevoke(ObserverContextImpl.createAndPrepare(CP_ENV),
          new UserPermission(USER_RO.getShortName(), Permission.newBuilder(TEST_TABLE)
              .withFamily(TEST_FAMILY).withActions(Action.READ).build()));
        return null;
      }
    };

    AccessTestAction grantCPAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf);
            Table acl = conn.getTable(PermissionStorage.ACL_TABLE_NAME)) {
          BlockingRpcChannel service = acl.coprocessorService(TEST_TABLE.getName());
          AccessControlService.BlockingInterface protocol =
              AccessControlService.newBlockingStub(service);
          AccessControlUtil.grant(null, protocol, USER_RO.getShortName(), TEST_TABLE, TEST_FAMILY,
            null, false, Action.READ);
        }
        return null;
      }
    };

    AccessTestAction revokeCPAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf);
            Table acl = conn.getTable(PermissionStorage.ACL_TABLE_NAME)) {
          BlockingRpcChannel service = acl.coprocessorService(TEST_TABLE.getName());
          AccessControlService.BlockingInterface protocol =
              AccessControlService.newBlockingStub(service);
          AccessControlUtil.revoke(null, protocol, USER_RO.getShortName(), TEST_TABLE, TEST_FAMILY,
            null, Action.READ);
        }
        return null;
      }
    };

    verifyAllowed(grantAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(grantAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);
    try {
      verifyAllowed(revokeAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
      verifyDenied(revokeAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);

      verifyAllowed(getTablePermissionsAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
      verifyDenied(getTablePermissionsAction, USER_CREATE, USER_RW, USER_RO, USER_NONE,
        USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

      verifyAllowed(getGlobalPermissionsAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
      verifyDenied(getGlobalPermissionsAction, USER_CREATE, USER_OWNER, USER_RW, USER_RO,
        USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

      verifyAllowed(preGrantAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
      verifyDenied(preGrantAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);

      verifyAllowed(preRevokeAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
      verifyDenied(preRevokeAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);

      verifyAllowed(grantCPAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
      verifyDenied(grantCPAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);

      verifyAllowed(revokeCPAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
      verifyDenied(revokeCPAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);
    } finally {
      // Cleanup, Grant the revoked permission back to the user
      grantOnTable(TEST_UTIL, USER_RO.getShortName(), TEST_TABLE, TEST_FAMILY, null,
        Permission.Action.READ);
    }
  }

  @Test
  public void testPostGrantRevoke() throws Exception {
    final TableName tableName =
        TableName.valueOf("TempTable");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");

    // create table
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(tableName)) {
      deleteTable(TEST_UTIL, tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    createTable(TEST_UTIL, htd);
    try {
      // create temp users
      User tblUser =
          User.createUserForTesting(TEST_UTIL.getConfiguration(), "tbluser", new String[0]);
      User gblUser =
          User.createUserForTesting(TEST_UTIL.getConfiguration(), "gbluser", new String[0]);

      // prepare actions:
      AccessTestAction putActionAll = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Put p = new Put(Bytes.toBytes("a"));
          p.addColumn(family1, qualifier, Bytes.toBytes("v1"));
          p.addColumn(family2, qualifier, Bytes.toBytes("v2"));

          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName);) {
            t.put(p);
          }
          return null;
        }
      };

      AccessTestAction putAction1 = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Put p = new Put(Bytes.toBytes("a"));
          p.addColumn(family1, qualifier, Bytes.toBytes("v1"));

          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
            t.put(p);
          }
          return null;
        }
      };

      AccessTestAction putAction2 = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Put p = new Put(Bytes.toBytes("a"));
          p.addColumn(family2, qualifier, Bytes.toBytes("v2"));
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName);) {
            t.put(p);
          }
          return null;
        }
      };

      AccessTestAction getActionAll = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Get g = new Get(TEST_ROW);
          g.addFamily(family1);
          g.addFamily(family2);
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName);) {
            t.get(g);
          }
          return null;
        }
      };

      AccessTestAction getAction1 = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Get g = new Get(TEST_ROW);
          g.addFamily(family1);
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
            t.get(g);
          }
          return null;
        }
      };

      AccessTestAction getAction2 = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Get g = new Get(TEST_ROW);
          g.addFamily(family2);
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
            t.get(g);
          }
          return null;
        }
      };

      AccessTestAction deleteActionAll = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Delete d = new Delete(TEST_ROW);
          d.addFamily(family1);
          d.addFamily(family2);
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
            t.delete(d);
          }
          return null;
        }
      };

      AccessTestAction deleteAction1 = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Delete d = new Delete(TEST_ROW);
          d.addFamily(family1);
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
            t.delete(d);
          }
          return null;
        }
      };

      AccessTestAction deleteAction2 = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Delete d = new Delete(TEST_ROW);
          d.addFamily(family2);
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
            t.delete(d);
          }
          return null;
        }
      };

      // initial check:
      verifyDenied(tblUser, getActionAll, getAction1, getAction2);
      verifyDenied(tblUser, putActionAll, putAction1, putAction2);
      verifyDenied(tblUser, deleteActionAll, deleteAction1, deleteAction2);

      verifyDenied(gblUser, getActionAll, getAction1, getAction2);
      verifyDenied(gblUser, putActionAll, putAction1, putAction2);
      verifyDenied(gblUser, deleteActionAll, deleteAction1, deleteAction2);

      // grant table read permission
      grantGlobal(TEST_UTIL, gblUser.getShortName(), Permission.Action.READ);
      grantOnTable(TEST_UTIL, tblUser.getShortName(), tableName, null, null, Permission.Action.READ);

      // check
      verifyAllowed(tblUser, getActionAll, getAction1, getAction2);
      verifyDenied(tblUser, putActionAll, putAction1, putAction2);
      verifyDenied(tblUser, deleteActionAll, deleteAction1, deleteAction2);

      verifyAllowed(gblUser, getActionAll, getAction1, getAction2);
      verifyDenied(gblUser, putActionAll, putAction1, putAction2);
      verifyDenied(gblUser, deleteActionAll, deleteAction1, deleteAction2);

      // grant table write permission while revoking read permissions
      grantGlobal(TEST_UTIL, gblUser.getShortName(), Permission.Action.WRITE);
      grantOnTable(TEST_UTIL, tblUser.getShortName(), tableName, null, null,
        Permission.Action.WRITE);

      verifyDenied(tblUser, getActionAll, getAction1, getAction2);
      verifyAllowed(tblUser, putActionAll, putAction1, putAction2);
      verifyAllowed(tblUser, deleteActionAll, deleteAction1, deleteAction2);

      verifyDenied(gblUser, getActionAll, getAction1, getAction2);
      verifyAllowed(gblUser, putActionAll, putAction1, putAction2);
      verifyAllowed(gblUser, deleteActionAll, deleteAction1, deleteAction2);

      // revoke table permissions
      revokeGlobal(TEST_UTIL, gblUser.getShortName());
      revokeFromTable(TEST_UTIL, tblUser.getShortName(), tableName, null, null);

      verifyDenied(tblUser, getActionAll, getAction1, getAction2);
      verifyDenied(tblUser, putActionAll, putAction1, putAction2);
      verifyDenied(tblUser, deleteActionAll, deleteAction1, deleteAction2);

      verifyDenied(gblUser, getActionAll, getAction1, getAction2);
      verifyDenied(gblUser, putActionAll, putAction1, putAction2);
      verifyDenied(gblUser, deleteActionAll, deleteAction1, deleteAction2);

      // grant column family read permission
      grantGlobal(TEST_UTIL, gblUser.getShortName(), Permission.Action.READ);
      grantOnTable(TEST_UTIL, tblUser.getShortName(), tableName, family1, null,
        Permission.Action.READ);

      // Access should be denied for family2
      verifyAllowed(tblUser, getActionAll, getAction1);
      verifyDenied(tblUser, getAction2);
      verifyDenied(tblUser, putActionAll, putAction1, putAction2);
      verifyDenied(tblUser, deleteActionAll, deleteAction1, deleteAction2);

      verifyAllowed(gblUser, getActionAll, getAction1, getAction2);
      verifyDenied(gblUser, putActionAll, putAction1, putAction2);
      verifyDenied(gblUser, deleteActionAll, deleteAction1, deleteAction2);

      // grant column family write permission
      grantGlobal(TEST_UTIL, gblUser.getShortName(), Permission.Action.WRITE);
      grantOnTable(TEST_UTIL, tblUser.getShortName(), tableName, family2, null,
        Permission.Action.WRITE);

      // READ from family1, WRITE to family2 are allowed
      verifyAllowed(tblUser, getActionAll, getAction1);
      verifyAllowed(tblUser, putAction2, deleteAction2);
      verifyDenied(tblUser, getAction2);
      verifyDenied(tblUser, putActionAll, putAction1);
      verifyDenied(tblUser, deleteActionAll, deleteAction1);

      verifyDenied(gblUser, getActionAll, getAction1, getAction2);
      verifyAllowed(gblUser, putActionAll, putAction1, putAction2);
      verifyAllowed(gblUser, deleteActionAll, deleteAction1, deleteAction2);

      // revoke column family permission
      revokeGlobal(TEST_UTIL, gblUser.getShortName());
      revokeFromTable(TEST_UTIL, tblUser.getShortName(), tableName, family2, null);

      // Revoke on family2 should not have impact on family1 permissions
      verifyAllowed(tblUser, getActionAll, getAction1);
      verifyDenied(tblUser, getAction2);
      verifyDenied(tblUser, putActionAll, putAction1, putAction2);
      verifyDenied(tblUser, deleteActionAll, deleteAction1, deleteAction2);

      // Should not have access as global permissions are completely revoked
      verifyDenied(gblUser, getActionAll, getAction1, getAction2);
      verifyDenied(gblUser, putActionAll, putAction1, putAction2);
      verifyDenied(gblUser, deleteActionAll, deleteAction1, deleteAction2);
    } finally {
      // delete table
      deleteTable(TEST_UTIL, tableName);
    }
  }

  private boolean hasFoundUserPermission(List<UserPermission> userPermissions,
                                         List<UserPermission> perms) {
    return perms.containsAll(userPermissions);
  }

  private boolean hasFoundUserPermission(UserPermission userPermission, List<UserPermission> perms) {
    return perms.contains(userPermission);
  }

  @Test
  public void testPostGrantRevokeAtQualifierLevel() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");

    // create table
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(tableName)) {
      deleteTable(TEST_UTIL, tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    createTable(TEST_UTIL, htd);

    try {
      // create temp users
      User user = User.createUserForTesting(TEST_UTIL.getConfiguration(), "user", new String[0]);

      AccessTestAction getQualifierAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Get g = new Get(TEST_ROW);
          g.addColumn(family1, qualifier);
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
            t.get(g);
          }
          return null;
        }
      };

      AccessTestAction putQualifierAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Put p = new Put(TEST_ROW);
          p.addColumn(family1, qualifier, Bytes.toBytes("v1"));
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
            t.put(p);
          }
          return null;
        }
      };

      AccessTestAction deleteQualifierAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Delete d = new Delete(TEST_ROW);
          d.addColumn(family1, qualifier);
          // d.deleteFamily(family1);
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
            t.delete(d);
          }
          return null;
        }
      };

      revokeFromTable(TEST_UTIL, user.getShortName(), tableName, family1, null);

      verifyDenied(user, getQualifierAction);
      verifyDenied(user, putQualifierAction);
      verifyDenied(user, deleteQualifierAction);

      grantOnTable(TEST_UTIL, user.getShortName(), tableName, family1, qualifier,
        Permission.Action.READ);

      verifyAllowed(user, getQualifierAction);
      verifyDenied(user, putQualifierAction);
      verifyDenied(user, deleteQualifierAction);

      // only grant write permission
      // TODO: comment this portion after HBASE-3583
      grantOnTable(TEST_UTIL, user.getShortName(), tableName, family1, qualifier,
        Permission.Action.WRITE);

      verifyDenied(user, getQualifierAction);
      verifyAllowed(user, putQualifierAction);
      verifyAllowed(user, deleteQualifierAction);

      // grant both read and write permission
      grantOnTable(TEST_UTIL, user.getShortName(), tableName, family1, qualifier,
        Permission.Action.READ, Permission.Action.WRITE);

      verifyAllowed(user, getQualifierAction);
      verifyAllowed(user, putQualifierAction);
      verifyAllowed(user, deleteQualifierAction);

      // revoke family level permission won't impact column level
      revokeFromTable(TEST_UTIL, user.getShortName(), tableName, family1, qualifier);

      verifyDenied(user, getQualifierAction);
      verifyDenied(user, putQualifierAction);
      verifyDenied(user, deleteQualifierAction);
    } finally {
      // delete table
      deleteTable(TEST_UTIL, tableName);
    }
  }

  @Test
  public void testPermissionList() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");

    // create table
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(tableName)) {
      deleteTable(TEST_UTIL, tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    htd.setOwner(USER_OWNER);
    createTable(TEST_UTIL, htd);
    try {
      List<UserPermission> perms =
          admin.getUserPermissions(GetUserPermissionsRequest.newBuilder(tableName).build());
      UserPermission ownerperm = new UserPermission(USER_OWNER.getName(),
          Permission.newBuilder(tableName).withActions(Action.values()).build());
      assertTrue("Owner should have all permissions on table",
        hasFoundUserPermission(ownerperm, perms));

      User user = User.createUserForTesting(TEST_UTIL.getConfiguration(), "user", new String[0]);
      String userName = user.getShortName();

      UserPermission up =
          new UserPermission(userName, Permission.newBuilder(tableName).withFamily(family1)
              .withQualifier(qualifier).withActions(Permission.Action.READ).build());
      assertFalse("User should not be granted permission: " + up.toString(),
        hasFoundUserPermission(up, perms));

      // grant read permission
      grantOnTable(TEST_UTIL, user.getShortName(), tableName, family1, qualifier,
        Permission.Action.READ);

      perms = admin.getUserPermissions(GetUserPermissionsRequest.newBuilder(tableName).build());
      UserPermission upToVerify =
          new UserPermission(userName, Permission.newBuilder(tableName).withFamily(family1)
              .withQualifier(qualifier).withActions(Permission.Action.READ).build());
      assertTrue("User should be granted permission: " + upToVerify.toString(),
        hasFoundUserPermission(upToVerify, perms));

      upToVerify = new UserPermission(userName, Permission.newBuilder(tableName).withFamily(family1)
          .withQualifier(qualifier).withActions(Permission.Action.WRITE).build());
      assertFalse("User should not be granted permission: " + upToVerify.toString(),
        hasFoundUserPermission(upToVerify, perms));

      // grant read+write
      grantOnTable(TEST_UTIL, user.getShortName(), tableName, family1, qualifier,
        Permission.Action.WRITE, Permission.Action.READ);

      perms = admin.getUserPermissions(GetUserPermissionsRequest.newBuilder(tableName).build());
      upToVerify = new UserPermission(userName,
          Permission.newBuilder(tableName).withFamily(family1).withQualifier(qualifier)
              .withActions(Permission.Action.WRITE, Permission.Action.READ).build());
      assertTrue("User should be granted permission: " + upToVerify.toString(),
        hasFoundUserPermission(upToVerify, perms));

      // revoke
      revokeFromTable(TEST_UTIL, user.getShortName(), tableName, family1, qualifier,
        Permission.Action.WRITE, Permission.Action.READ);

      perms = admin.getUserPermissions(GetUserPermissionsRequest.newBuilder(tableName).build());
      assertFalse("User should not be granted permission: " + upToVerify.toString(),
        hasFoundUserPermission(upToVerify, perms));

      // disable table before modification
      admin.disableTable(tableName);

      User newOwner = User.createUserForTesting(conf, "new_owner", new String[] {});
      htd.setOwner(newOwner);
      admin.modifyTable(tableName, htd);

      perms = admin.getUserPermissions(GetUserPermissionsRequest.newBuilder(tableName).build());
      UserPermission newOwnerperm = new UserPermission(newOwner.getName(),
          Permission.newBuilder(tableName).withActions(Action.values()).build());
      assertTrue("New owner should have all permissions on table",
        hasFoundUserPermission(newOwnerperm, perms));
    } finally {
      // delete table
      deleteTable(TEST_UTIL, tableName);
    }
  }

  @Test
  public void testGlobalPermissionList() throws Exception {
    List<UserPermission> perms = systemUserConnection.getAdmin()
        .getUserPermissions(GetUserPermissionsRequest.newBuilder().build());

    Collection<String> superUsers = Superusers.getSuperUsers();
    List<UserPermission> adminPerms = new ArrayList<>(superUsers.size() + 1);
    adminPerms.add(new UserPermission(USER_ADMIN.getShortName(), Permission.newBuilder()
        .withActions(Action.ADMIN, Action.CREATE, Action.READ, Action.WRITE).build()));
    for (String user : superUsers) {
      // Global permission
      adminPerms.add(
        new UserPermission(user, Permission.newBuilder().withActions(Action.values()).build()));
    }
    assertTrue("Only super users, global users and user admin has permission on table hbase:acl " +
        "per setup", perms.size() == 5 + superUsers.size() &&
        hasFoundUserPermission(adminPerms, perms));
  }

  /** global operations */
  private void verifyGlobal(AccessTestAction action) throws Exception {
    verifyAllowed(action, SUPERUSER);

    verifyDenied(action, USER_CREATE, USER_RW, USER_NONE, USER_RO);
  }

  @Test
  public void testCheckPermissions() throws Exception {
    // --------------------------------------
    // test global permissions
    AccessTestAction globalAdmin = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkGlobalPerms(TEST_UTIL, Permission.Action.ADMIN);
        return null;
      }
    };
    // verify that only superuser can admin
    verifyGlobal(globalAdmin);

    // --------------------------------------
    // test multiple permissions
    AccessTestAction globalReadWrite = new AccessTestAction() {
      @Override
      public Void run() throws Exception {
        checkGlobalPerms(TEST_UTIL, Permission.Action.READ, Permission.Action.WRITE);
        return null;
      }
    };

    verifyGlobal(globalReadWrite);

    // --------------------------------------
    // table/column/qualifier level permissions
    final byte[] TEST_Q1 = Bytes.toBytes("q1");
    final byte[] TEST_Q2 = Bytes.toBytes("q2");

    User userTable = User.createUserForTesting(conf, "user_check_perms_table", new String[0]);
    User userColumn = User.createUserForTesting(conf, "user_check_perms_family", new String[0]);
    User userQualifier = User.createUserForTesting(conf, "user_check_perms_q", new String[0]);

    grantOnTable(TEST_UTIL, userTable.getShortName(),
      TEST_TABLE, null, null,
      Permission.Action.READ);
    grantOnTable(TEST_UTIL, userColumn.getShortName(),
      TEST_TABLE, TEST_FAMILY, null,
      Permission.Action.READ);
    grantOnTable(TEST_UTIL, userQualifier.getShortName(),
      TEST_TABLE, TEST_FAMILY, TEST_Q1,
      Permission.Action.READ);

    try {
      AccessTestAction tableRead = new AccessTestAction() {
        @Override
        public Void run() throws Exception {
          checkTablePerms(TEST_UTIL, TEST_TABLE, null, null, Permission.Action.READ);
          return null;
        }
      };

      AccessTestAction columnRead = new AccessTestAction() {
        @Override
        public Void run() throws Exception {
          checkTablePerms(TEST_UTIL, TEST_TABLE, TEST_FAMILY, null, Permission.Action.READ);
          return null;
        }
      };

      AccessTestAction qualifierRead = new AccessTestAction() {
        @Override
        public Void run() throws Exception {
          checkTablePerms(TEST_UTIL, TEST_TABLE, TEST_FAMILY, TEST_Q1, Permission.Action.READ);
          return null;
        }
      };

      AccessTestAction multiQualifierRead = new AccessTestAction() {
        @Override
        public Void run() throws Exception {
          checkTablePerms(TEST_UTIL,
            new Permission[] {
                Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY).withQualifier(TEST_Q1)
                    .withActions(Permission.Action.READ).build(),
                Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY).withQualifier(TEST_Q2)
                    .withActions(Permission.Action.READ).build(), });
          return null;
        }
      };

      AccessTestAction globalAndTableRead = new AccessTestAction() {
        @Override
        public Void run() throws Exception {
          checkTablePerms(TEST_UTIL, new Permission[] { new Permission(Permission.Action.READ),
              Permission.newBuilder(TEST_TABLE).withActions(Permission.Action.READ).build() });
          return null;
        }
      };

      AccessTestAction noCheck = new AccessTestAction() {
        @Override
        public Void run() throws Exception {
          checkTablePerms(TEST_UTIL, new Permission[0]);
          return null;
        }
      };

      verifyAllowed(tableRead, SUPERUSER, userTable);
      verifyDenied(tableRead, userColumn, userQualifier);

      verifyAllowed(columnRead, SUPERUSER, userTable, userColumn);
      verifyDenied(columnRead, userQualifier);

      verifyAllowed(qualifierRead, SUPERUSER, userTable, userColumn, userQualifier);

      verifyAllowed(multiQualifierRead, SUPERUSER, userTable, userColumn);
      verifyDenied(multiQualifierRead, userQualifier);

      verifyAllowed(globalAndTableRead, SUPERUSER);
      verifyDenied(globalAndTableRead, userTable, userColumn, userQualifier);

      verifyAllowed(noCheck, SUPERUSER, userTable, userColumn, userQualifier);

      // --------------------------------------
      // test family level multiple permissions
      AccessTestAction familyReadWrite = new AccessTestAction() {
        @Override
        public Void run() throws Exception {
          checkTablePerms(TEST_UTIL, TEST_TABLE, TEST_FAMILY, null, Permission.Action.READ,
            Permission.Action.WRITE);
          return null;
        }
      };

      verifyAllowed(familyReadWrite, SUPERUSER, USER_OWNER, USER_CREATE, USER_RW);
      verifyDenied(familyReadWrite, USER_NONE, USER_RO);

      // --------------------------------------
      // check for wrong table region
      CheckPermissionsRequest checkRequest =
          CheckPermissionsRequest
              .newBuilder()
              .addPermission(
                AccessControlProtos.Permission
                    .newBuilder()
                    .setType(AccessControlProtos.Permission.Type.Table)
                    .setTablePermission(
                      AccessControlProtos.TablePermission.newBuilder()
                          .setTableName(ProtobufUtil.toProtoTableName(TEST_TABLE))
                          .addAction(AccessControlProtos.Permission.Action.CREATE))).build();
      Table acl = systemUserConnection.getTable(PermissionStorage.ACL_TABLE_NAME);
      try {
        BlockingRpcChannel channel = acl.coprocessorService(new byte[0]);
        AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(channel);
        try {
          // but ask for TablePermissions for TEST_TABLE
          protocol.checkPermissions(null, checkRequest);
          fail("this should have thrown CoprocessorException");
        } catch (ServiceException ex) {
          // expected
        }
      } finally {
        acl.close();
      }

    } finally {
      revokeFromTable(TEST_UTIL, userTable.getShortName(), TEST_TABLE, null, null,
        Permission.Action.READ);
      revokeFromTable(TEST_UTIL, userColumn.getShortName(), TEST_TABLE, TEST_FAMILY, null,
        Permission.Action.READ);
      revokeFromTable(TEST_UTIL, userQualifier.getShortName(), TEST_TABLE, TEST_FAMILY, TEST_Q1,
        Permission.Action.READ);
    }
  }

  @Test
  public void testStopRegionServer() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preStopRegionServer(ObserverContextImpl.createAndPrepare(RSCP_ENV));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testRollWALWriterRequest() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preRollWALWriterRequest(ObserverContextImpl.createAndPrepare(RSCP_ENV));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testOpenRegion() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preOpen(ObserverContextImpl.createAndPrepare(RCP_ENV));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER, USER_GROUP_CREATE,
      USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testCloseRegion() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preClose(ObserverContextImpl.createAndPrepare(RCP_ENV), false);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER, USER_GROUP_CREATE,
      USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testSnapshot() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    final HTableDescriptor htd = admin.getTableDescriptor(TEST_TABLE);
    final SnapshotDescription snapshot = new SnapshotDescription(
        TEST_TABLE.getNameAsString() + "-snapshot", TEST_TABLE);
    AccessTestAction snapshotAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSnapshot(ObserverContextImpl.createAndPrepare(CP_ENV),
          snapshot, htd);
        return null;
      }
    };

    AccessTestAction deleteAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteSnapshot(ObserverContextImpl.createAndPrepare(CP_ENV),
          snapshot);
        return null;
      }
    };

    AccessTestAction restoreAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preRestoreSnapshot(ObserverContextImpl.createAndPrepare(CP_ENV),
          snapshot, htd);
        return null;
      }
    };

    AccessTestAction cloneAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCloneSnapshot(ObserverContextImpl.createAndPrepare(CP_ENV),
          snapshot, null);
        return null;
      }
    };

    verifyAllowed(snapshotAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(snapshotAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(cloneAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(deleteAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(restoreAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(restoreAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(deleteAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(cloneAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testSnapshotWithOwner() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    final HTableDescriptor htd = admin.getTableDescriptor(TEST_TABLE);
    final SnapshotDescription snapshot = new SnapshotDescription(
        TEST_TABLE.getNameAsString() + "-snapshot", TEST_TABLE, null, USER_OWNER.getName());

    AccessTestAction snapshotAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSnapshot(ObserverContextImpl.createAndPrepare(CP_ENV),
            snapshot, htd);
        return null;
      }
    };
    verifyAllowed(snapshotAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(snapshotAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);

    AccessTestAction deleteAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteSnapshot(ObserverContextImpl.createAndPrepare(CP_ENV),
          snapshot);
        return null;
      }
    };
    verifyAllowed(deleteAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(deleteAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);

    AccessTestAction restoreAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preRestoreSnapshot(ObserverContextImpl.createAndPrepare(CP_ENV),
          snapshot, htd);
        return null;
      }
    };
    verifyAllowed(restoreAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(restoreAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);

    AccessTestAction cloneAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCloneSnapshot(ObserverContextImpl.createAndPrepare(CP_ENV),
          snapshot, htd);
        return null;
      }
    };
    verifyAllowed(cloneAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN, USER_OWNER);
    verifyDenied(cloneAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testGlobalAuthorizationForNewRegisteredRS() throws Exception {
    LOG.debug("Test for global authorization for a new registered RegionServer.");
    MiniHBaseCluster hbaseCluster = TEST_UTIL.getHBaseCluster();

    final Admin admin = TEST_UTIL.getAdmin();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE2);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
    createTable(TEST_UTIL, htd);

    // Starting a new RegionServer.
    JVMClusterUtil.RegionServerThread newRsThread = hbaseCluster
        .startRegionServer();
    final HRegionServer newRs = newRsThread.getRegionServer();

    // Move region to the new RegionServer.
    List<HRegionLocation> regions;
    try (RegionLocator locator = systemUserConnection.getRegionLocator(TEST_TABLE2)) {
      regions = locator.getAllRegionLocations();
    }
    HRegionLocation location = regions.get(0);
    final HRegionInfo hri = location.getRegionInfo();
    final ServerName server = location.getServerName();
    try (Table table = systemUserConnection.getTable(TEST_TABLE2)) {
      AccessTestAction moveAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          admin.move(hri.getEncodedNameAsBytes(), newRs.getServerName());
          return null;
        }
      };
      SUPERUSER.runAs(moveAction);

      final int RETRIES_LIMIT = 10;
      int retries = 0;
      while (newRs.getRegions(TEST_TABLE2).size() < 1 && retries < RETRIES_LIMIT) {
        LOG.debug("Waiting for region to be opened. Already retried " + retries
            + " times.");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        retries++;
        if (retries == RETRIES_LIMIT - 1) {
          fail("Retry exhaust for waiting region to be opened.");
        }
      }
      // Verify write permission for user "admin2" who has the global
      // permissions.
      AccessTestAction putAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Put put = new Put(Bytes.toBytes("test"));
          put.addColumn(TEST_FAMILY, Bytes.toBytes("qual"), Bytes.toBytes("value"));
          table.put(put);
          return null;
        }
      };
      USER_ADMIN.runAs(putAction);
    }
  }

  @Test
  public void testTableDescriptorsEnumeration() throws Exception {
    User TABLE_ADMIN = User.createUserForTesting(conf, "UserA", new String[0]);

    // Grant TABLE ADMIN privs
    grantOnTable(TEST_UTIL, TABLE_ADMIN.getShortName(), TEST_TABLE, null, null,
      Permission.Action.ADMIN);
    try {
      AccessTestAction listTablesAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
              Admin admin = conn.getAdmin()) {
            return Arrays.asList(admin.listTables());
          }
        }
      };

      AccessTestAction getTableDescAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
              Admin admin = conn.getAdmin();) {
            return admin.getTableDescriptor(TEST_TABLE);
          }
        }
      };

      verifyAllowed(listTablesAction, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, TABLE_ADMIN,
        USER_GROUP_CREATE, USER_GROUP_ADMIN);
      verifyIfEmptyList(listTablesAction, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE);

      verifyAllowed(getTableDescAction, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER,
        TABLE_ADMIN, USER_GROUP_CREATE, USER_GROUP_ADMIN);
      verifyDenied(getTableDescAction, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE);
    } finally {
      // Cleanup, revoke TABLE ADMIN privs
      revokeFromTable(TEST_UTIL, TABLE_ADMIN.getShortName(), TEST_TABLE, null, null,
        Permission.Action.ADMIN);
    }
  }

  @Test
  public void testTableNameEnumeration() throws Exception {
    AccessTestAction listTablesAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Connection unmanagedConnection =
            ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Admin admin = unmanagedConnection.getAdmin();
        try {
          return Arrays.asList(admin.listTableNames());
        } finally {
          admin.close();
          unmanagedConnection.close();
        }
      }
    };

    verifyAllowed(listTablesAction, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_RW,
      USER_RO, USER_GROUP_CREATE, USER_GROUP_ADMIN, USER_GROUP_READ, USER_GROUP_WRITE);
    verifyIfEmptyList(listTablesAction, USER_NONE);
  }

  @Test
  public void testTableDeletion() throws Exception {
    User TABLE_ADMIN = User.createUserForTesting(conf, "TestUser", new String[0]);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    createTestTable(tableName);

    // Grant TABLE ADMIN privs
    grantOnTable(TEST_UTIL, TABLE_ADMIN.getShortName(), tableName, null, null, Permission.Action.ADMIN);

    AccessTestAction deleteTableAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Connection unmanagedConnection =
            ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Admin admin = unmanagedConnection.getAdmin();
        try {
          deleteTable(TEST_UTIL, admin, tableName);
        } finally {
          admin.close();
          unmanagedConnection.close();
        }
        return null;
      }
    };

    verifyDenied(deleteTableAction, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE);
    verifyAllowed(deleteTableAction, TABLE_ADMIN);
  }

  private void createTestTable(TableName tname) throws Exception {
    createTestTable(tname, TEST_FAMILY);
  }

  private void createTestTable(TableName tname, byte[] cf) throws Exception {
    HTableDescriptor htd = new HTableDescriptor(tname);
    HColumnDescriptor hcd = new HColumnDescriptor(cf);
    hcd.setMaxVersions(100);
    htd.addFamily(hcd);
    htd.setOwner(USER_OWNER);
    createTable(TEST_UTIL, htd, new byte[][] { Bytes.toBytes("s") });
  }

  @Test
  public void testNamespaceUserGrant() throws Exception {
    AccessTestAction getAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE);) {
          return t.get(new Get(TEST_ROW));
        }
      }
    };

    String namespace = TEST_TABLE.getNamespaceAsString();

    // Grant namespace READ to USER_NONE, this should supersede any table permissions
    grantOnNamespace(TEST_UTIL, USER_NONE.getShortName(), namespace, Permission.Action.READ);
    // Now USER_NONE should be able to read
    verifyAllowed(getAction, USER_NONE);

    // Revoke namespace READ to USER_NONE
    revokeFromNamespace(TEST_UTIL, USER_NONE.getShortName(), namespace, Permission.Action.READ);
    verifyDenied(getAction, USER_NONE);
  }

  @Test
  public void testAccessControlClientGrantRevoke() throws Exception {
    // Create user for testing, who has no READ privileges by default.
    User testGrantRevoke = User.createUserForTesting(conf, "testGrantRevoke", new String[0]);
    AccessTestAction getAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE);) {
          return t.get(new Get(TEST_ROW));
        }
      }
    };

    verifyDenied(getAction, testGrantRevoke);

    // Grant table READ permissions to testGrantRevoke.
    try {
      grantOnTableUsingAccessControlClient(TEST_UTIL, systemUserConnection,
        testGrantRevoke.getShortName(), TEST_TABLE, null, null, Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }

    // Now testGrantRevoke should be able to read also
    verifyAllowed(getAction, testGrantRevoke);

    // Revoke table READ permission to testGrantRevoke.
    try {
      revokeFromTableUsingAccessControlClient(TEST_UTIL, systemUserConnection,
        testGrantRevoke.getShortName(), TEST_TABLE, null, null, Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.revoke ", e);
    }

    // Now testGrantRevoke shouldn't be able read
    verifyDenied(getAction, testGrantRevoke);
  }

  @Test
  public void testAccessControlClientGlobalGrantRevoke() throws Exception {
    // Create user for testing, who has no READ privileges by default.
    User testGlobalGrantRevoke = User.createUserForTesting(conf,
      "testGlobalGrantRevoke", new String[0]);
    AccessTestAction getAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          return t.get(new Get(TEST_ROW));
        }
      }
    };

    verifyDenied(getAction, testGlobalGrantRevoke);

    // Grant table READ permissions to testGlobalGrantRevoke.
    String userName = testGlobalGrantRevoke.getShortName();
    try {
      grantGlobalUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
        Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    try {
      // Now testGlobalGrantRevoke should be able to read also
      verifyAllowed(getAction, testGlobalGrantRevoke);
    } catch (Exception e) {
      revokeGlobal(TEST_UTIL, userName, Permission.Action.READ);
      throw e;
    }

    // Revoke table READ permission to testGlobalGrantRevoke.
    try {
      revokeGlobalUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
        Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.revoke ", e);
    }

    // Now testGlobalGrantRevoke shouldn't be able read
    verifyDenied(getAction, testGlobalGrantRevoke);

  }

  @Test
  public void testAccessControlClientMultiGrantRevoke() throws Exception {
    User testGrantRevoke =
        User.createUserForTesting(conf, "testGrantRevoke", new String[0]);
    AccessTestAction getAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          return t.get(new Get(TEST_ROW));
        }
      }
    };

    AccessTestAction putAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Put p = new Put(TEST_ROW);
        p.addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(1));
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          t.put(p);
          return null;
        }
      }
    };

    verifyDenied(getAction, testGrantRevoke);
    verifyDenied(putAction, testGrantRevoke);

    // Grant global READ permissions to testGrantRevoke.
    String userName = testGrantRevoke.getShortName();
    try {
      grantGlobalUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
        Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    verifyAllowed(getAction, testGrantRevoke);
    verifyDenied(putAction, testGrantRevoke);

    // Grant global WRITE permissions to testGrantRevoke.
    try {
      grantGlobalUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
              Permission.Action.WRITE);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    verifyAllowed(getAction, testGrantRevoke);
    verifyAllowed(putAction, testGrantRevoke);

    // Revoke global READ permission to testGrantRevoke.
    try {
      revokeGlobalUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
              Permission.Action.READ, Permission.Action.WRITE);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.revoke ", e);
    }
    verifyDenied(getAction, testGrantRevoke);
    verifyDenied(putAction, testGrantRevoke);

    // Grant table READ & WRITE permissions to testGrantRevoke
    try {
      grantOnTableUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName, TEST_TABLE,
        null, null, Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    verifyAllowed(getAction, testGrantRevoke);
    verifyDenied(putAction, testGrantRevoke);

    // Grant table WRITE permissions to testGrantRevoke
    try {
      grantOnTableUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName, TEST_TABLE,
        null, null, Action.WRITE);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    verifyAllowed(getAction, testGrantRevoke);
    verifyAllowed(putAction, testGrantRevoke);

    // Revoke table READ & WRITE permission to testGrantRevoke.
    try {
      revokeFromTableUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName, TEST_TABLE, null, null,
              Permission.Action.READ, Permission.Action.WRITE);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.revoke ", e);
    }
    verifyDenied(getAction, testGrantRevoke);
    verifyDenied(putAction, testGrantRevoke);

    // Grant Namespace READ permissions to testGrantRevoke
    String namespace = TEST_TABLE.getNamespaceAsString();
    try {
      grantOnNamespaceUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
        namespace, Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    verifyAllowed(getAction, testGrantRevoke);
    verifyDenied(putAction, testGrantRevoke);

    // Grant Namespace WRITE permissions to testGrantRevoke
    try {
      grantOnNamespaceUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
              namespace, Permission.Action.WRITE);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    verifyAllowed(getAction, testGrantRevoke);
    verifyAllowed(putAction, testGrantRevoke);

    // Revoke table READ & WRITE permission to testGrantRevoke.
    try {
      revokeFromNamespaceUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
              TEST_TABLE.getNamespaceAsString(), Permission.Action.READ, Permission.Action.WRITE);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.revoke ", e);
    }
    verifyDenied(getAction, testGrantRevoke);
    verifyDenied(putAction, testGrantRevoke);
  }

  @Test
  public void testAccessControlClientGrantRevokeOnNamespace() throws Exception {
    // Create user for testing, who has no READ privileges by default.
    User testNS = User.createUserForTesting(conf, "testNS", new String[0]);
    AccessTestAction getAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE);) {
          return t.get(new Get(TEST_ROW));
        }
      }
    };

    verifyDenied(getAction, testNS);

    String userName = testNS.getShortName();
    String namespace = TEST_TABLE.getNamespaceAsString();
    // Grant namespace READ to testNS, this should supersede any table permissions
    try {
      grantOnNamespaceUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName, namespace,
        Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    try {
      // Now testNS should be able to read also
      verifyAllowed(getAction, testNS);
    } catch (Exception e) {
      revokeFromNamespace(TEST_UTIL, userName, namespace, Permission.Action.READ);
      throw e;
    }

    // Revoke namespace READ to testNS, this should supersede any table permissions
    try {
      revokeFromNamespaceUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
        namespace, Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.revoke ", e);
    }

    // Now testNS shouldn't be able read
    verifyDenied(getAction, testNS);
  }


  public static class PingCoprocessor extends PingService implements RegionCoprocessor {

    @Override
    public void start(CoprocessorEnvironment env) throws IOException { }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException { }

    @Override
    public Iterable<Service> getServices() {
      return Collections.singleton(this);
    }

    @Override
    public void ping(RpcController controller, PingRequest request,
        RpcCallback<PingResponse> callback) {
      callback.run(PingResponse.newBuilder().setPong("Pong!").build());
    }

    @Override
    public void count(RpcController controller, CountRequest request,
        RpcCallback<CountResponse> callback) {
      callback.run(CountResponse.newBuilder().build());
    }

    @Override
    public void increment(RpcController controller, IncrementCountRequest requet,
        RpcCallback<IncrementCountResponse> callback) {
      callback.run(IncrementCountResponse.newBuilder().build());
    }

    @Override
    public void hello(RpcController controller, HelloRequest request,
        RpcCallback<HelloResponse> callback) {
      callback.run(HelloResponse.newBuilder().setResponse("Hello!").build());
    }

    @Override
    public void noop(RpcController controller, NoopRequest request,
        RpcCallback<NoopResponse> callback) {
      callback.run(NoopResponse.newBuilder().build());
    }
  }

  @Test
  public void testCoprocessorExec() throws Exception {
    // Set up our ping endpoint service on all regions of our test table
    for (JVMClusterUtil.RegionServerThread thread:
        TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      HRegionServer rs = thread.getRegionServer();
      for (HRegion region: rs.getRegions(TEST_TABLE)) {
        region.getCoprocessorHost().load(PingCoprocessor.class,
          Coprocessor.PRIORITY_USER, conf);
      }
    }

    // Create users for testing, and grant EXEC privileges on our test table
    // only to user A
    User userA = User.createUserForTesting(conf, "UserA", new String[0]);
    User userB = User.createUserForTesting(conf, "UserB", new String[0]);

    grantOnTable(TEST_UTIL, userA.getShortName(),
      TEST_TABLE, null, null,
      Permission.Action.EXEC);
    try {
      // Create an action for invoking our test endpoint
      AccessTestAction execEndpointAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(TEST_TABLE);) {
            BlockingRpcChannel service = t.coprocessorService(HConstants.EMPTY_BYTE_ARRAY);
            PingCoprocessor.newBlockingStub(service).noop(null, NoopRequest.newBuilder().build());
          }
          return null;
        }
      };

      String namespace = TEST_TABLE.getNamespaceAsString();
      // Now grant EXEC to the entire namespace to user B
      grantOnNamespace(TEST_UTIL, userB.getShortName(), namespace, Permission.Action.EXEC);
      // User B should now be allowed also
      verifyAllowed(execEndpointAction, userA, userB);

      revokeFromNamespace(TEST_UTIL, userB.getShortName(), namespace, Permission.Action.EXEC);
      // Verify that EXEC permission is checked correctly
      verifyDenied(execEndpointAction, userB);
      verifyAllowed(execEndpointAction, userA);
    } finally {
      // Cleanup, revoke the userA privileges
      revokeFromTable(TEST_UTIL, userA.getShortName(), TEST_TABLE, null, null,
        Permission.Action.EXEC);
    }
  }

  @Test
  public void testSetQuota() throws Exception {
    AccessTestAction setUserQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetUserQuota(ObserverContextImpl.createAndPrepare(CP_ENV),
          null, null);
        return null;
      }
    };

    AccessTestAction setUserTableQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetUserQuota(ObserverContextImpl.createAndPrepare(CP_ENV), null,
          TEST_TABLE, null);
        return null;
      }
    };

    AccessTestAction setUserNamespaceQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetUserQuota(ObserverContextImpl.createAndPrepare(CP_ENV),
          null, (String)null, null);
        return null;
      }
    };

    AccessTestAction setTableQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetTableQuota(ObserverContextImpl.createAndPrepare(CP_ENV),
          TEST_TABLE, null);
        return null;
      }
    };

    AccessTestAction setNamespaceQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetNamespaceQuota(ObserverContextImpl.createAndPrepare(CP_ENV),
          null, null);
        return null;
      }
    };

    AccessTestAction setRegionServerQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetRegionServerQuota(ObserverContextImpl.createAndPrepare(CP_ENV),
          null, null);
        return null;
      }
    };

    verifyAllowed(setUserQuotaAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(setUserQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(setUserTableQuotaAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(setUserTableQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(setUserNamespaceQuotaAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(setUserNamespaceQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(setTableQuotaAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(setTableQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE);

    verifyAllowed(setNamespaceQuotaAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(setNamespaceQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);

    verifyAllowed(setRegionServerQuotaAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(setRegionServerQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testGetNamespacePermission() throws Exception {
    String namespace = "testGetNamespacePermission";
    NamespaceDescriptor desc = NamespaceDescriptor.create(namespace).build();
    createNamespace(TEST_UTIL, desc);
    grantOnNamespace(TEST_UTIL, USER_NONE.getShortName(), namespace, Permission.Action.READ);

    // Test 1: A specific namespace
    getNamespacePermissionsAndVerify(namespace, 1, namespace);

    // Test 2: '@.*'
    getNamespacePermissionsAndVerify(".*", 1, namespace);

    // Test 3: A more complex regex
    getNamespacePermissionsAndVerify("^test[a-zA-Z]*", 1, namespace);

    deleteNamespace(TEST_UTIL, namespace);
  }

  /**
   * List all user permissions match the given regular expression for namespace
   * and verify each of them.
   * @param namespaceRegexWithoutPrefix the regualar expression for namespace, without NAMESPACE_PREFIX
   * @param expectedAmount the expected amount of user permissions returned
   * @param expectedNamespace the expected namespace of each user permission returned
   * @throws HBaseException in the case of any HBase exception when accessing hbase:acl table
   */
  private void getNamespacePermissionsAndVerify(String namespaceRegexWithoutPrefix,
      int expectedAmount, String expectedNamespace) throws HBaseException {
    try {
      List<UserPermission> namespacePermissions = AccessControlClient.getUserPermissions(
        systemUserConnection, PermissionStorage.toNamespaceEntry(namespaceRegexWithoutPrefix));
      assertTrue(namespacePermissions != null);
      assertEquals(expectedAmount, namespacePermissions.size());
      for (UserPermission namespacePermission : namespacePermissions) {
        // Verify it is not a global user permission
        assertFalse(namespacePermission.getAccessScope() == Permission.Scope.GLOBAL);
        // Verify namespace is set
        NamespacePermission nsPerm = (NamespacePermission) namespacePermission.getPermission();
        assertEquals(expectedNamespace, nsPerm.getNamespace());
      }
    } catch (Throwable thw) {
      throw new HBaseException(thw);
    }
  }

  @Test
  public void testTruncatePerms() throws Exception {
    try {
      List<UserPermission> existingPerms = AccessControlClient.getUserPermissions(
          systemUserConnection, TEST_TABLE.getNameAsString());
      assertTrue(existingPerms != null);
      assertTrue(existingPerms.size() > 1);
      TEST_UTIL.getAdmin().disableTable(TEST_TABLE);
      TEST_UTIL.truncateTable(TEST_TABLE);
      TEST_UTIL.waitTableAvailable(TEST_TABLE);
      List<UserPermission> perms = AccessControlClient.getUserPermissions(
          systemUserConnection, TEST_TABLE.getNameAsString());
      assertTrue(perms != null);
      assertEquals(existingPerms.size(), perms.size());
    } catch (Throwable e) {
      throw new HBaseIOException(e);
    }
  }

  private PrivilegedAction<List<UserPermission>> getPrivilegedAction(final String regex) {
    return new PrivilegedAction<List<UserPermission>>() {
      @Override
      public List<UserPermission> run() {
        try(Connection conn = ConnectionFactory.createConnection(conf);) {
          return AccessControlClient.getUserPermissions(conn, regex);
        } catch (Throwable e) {
          LOG.error("error during call of AccessControlClient.getUserPermissions.", e);
          return null;
        }
      }
    };
  }

  @Test
  public void testAccessControlClientUserPerms() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    createTestTable(tableName);
    try {
      final String regex = tableName.getNameWithNamespaceInclAsString();
      User testUserPerms = User.createUserForTesting(conf, "testUserPerms", new String[0]);
      assertEquals(0, testUserPerms.runAs(getPrivilegedAction(regex)).size());
      // Grant TABLE ADMIN privs to testUserPerms
      grantOnTable(TEST_UTIL, testUserPerms.getShortName(), tableName, null, null, Action.ADMIN);
      List<UserPermission> perms = testUserPerms.runAs(getPrivilegedAction(regex));
      assertNotNull(perms);
      // Superuser, testUserPerms
      assertEquals(2, perms.size());
    } finally {
      deleteTable(TEST_UTIL, tableName);
    }
  }

  @Test
  public void testAccessControllerUserPermsRegexHandling() throws Exception {
    User testRegexHandler = User.createUserForTesting(conf, "testRegexHandling", new String[0]);

    final String REGEX_ALL_TABLES = ".*";
    final String tableName = name.getMethodName();
    final TableName table1 = TableName.valueOf(tableName);
    final byte[] family = Bytes.toBytes("f1");

    // create table in default ns
    Admin admin = TEST_UTIL.getAdmin();
    HTableDescriptor htd = new HTableDescriptor(table1);
    htd.addFamily(new HColumnDescriptor(family));
    createTable(TEST_UTIL, htd);

    // creating the ns and table in it
    String ns = "testNamespace";
    NamespaceDescriptor desc = NamespaceDescriptor.create(ns).build();
    final TableName table2 = TableName.valueOf(ns, tableName);
    createNamespace(TEST_UTIL, desc);
    htd = new HTableDescriptor(table2);
    htd.addFamily(new HColumnDescriptor(family));
    createTable(TEST_UTIL, htd);

    // Verify that we can read sys-tables
    String aclTableName = PermissionStorage.ACL_TABLE_NAME.getNameAsString();
    assertEquals(5, SUPERUSER.runAs(getPrivilegedAction(aclTableName)).size());
    assertEquals(0, testRegexHandler.runAs(getPrivilegedAction(aclTableName)).size());

    // Grant TABLE ADMIN privs to testUserPerms
    assertEquals(0, testRegexHandler.runAs(getPrivilegedAction(REGEX_ALL_TABLES)).size());
    grantOnTable(TEST_UTIL, testRegexHandler.getShortName(), table1, null, null, Action.ADMIN);
    assertEquals(2, testRegexHandler.runAs(getPrivilegedAction(REGEX_ALL_TABLES)).size());
    grantOnTable(TEST_UTIL, testRegexHandler.getShortName(), table2, null, null, Action.ADMIN);
    assertEquals(4, testRegexHandler.runAs(getPrivilegedAction(REGEX_ALL_TABLES)).size());

    // USER_ADMIN, testUserPerms must have a row each.
    assertEquals(2, testRegexHandler.runAs(getPrivilegedAction(tableName)).size());
    assertEquals(2, testRegexHandler.runAs(getPrivilegedAction(
          NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR + TableName.NAMESPACE_DELIM + tableName)
        ).size());
    assertEquals(2, testRegexHandler.runAs(getPrivilegedAction(
        ns + TableName.NAMESPACE_DELIM + tableName)).size());
    assertEquals(0, testRegexHandler.runAs(getPrivilegedAction("notMatchingAny")).size());

    deleteTable(TEST_UTIL, table1);
    deleteTable(TEST_UTIL, table2);
    deleteNamespace(TEST_UTIL, ns);
  }

  private void verifyAnyCreate(AccessTestAction action) throws Exception {
    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE, USER_ADMIN_CF,
      USER_GROUP_CREATE, USER_GROUP_ADMIN);
    verifyDenied(action, USER_NONE, USER_RO, USER_RW, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testPrepareAndCleanBulkLoad() throws Exception {
    AccessTestAction prepareBulkLoadAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.prePrepareBulkLoad(ObserverContextImpl.createAndPrepare(RCP_ENV));
        return null;
      }
    };
    AccessTestAction cleanupBulkLoadAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCleanupBulkLoad(ObserverContextImpl.createAndPrepare(RCP_ENV));
        return null;
      }
    };
    verifyAnyCreate(prepareBulkLoadAction);
    verifyAnyCreate(cleanupBulkLoadAction);
  }

  @Test
  public void testReplicateLogEntries() throws Exception {
    AccessTestAction replicateLogEntriesAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preReplicateLogEntries(ObserverContextImpl.createAndPrepare(RSCP_ENV));
        ACCESS_CONTROLLER.postReplicateLogEntries(ObserverContextImpl.createAndPrepare(RSCP_ENV));
        return null;
      }
    };

    verifyAllowed(replicateLogEntriesAction, SUPERUSER, USER_ADMIN, USER_GROUP_WRITE);
    verifyDenied(replicateLogEntriesAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER,
      USER_GROUP_READ, USER_GROUP_ADMIN, USER_GROUP_CREATE);
  }

  @Test
  public void testAddReplicationPeer() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAddReplicationPeer(ObserverContextImpl.createAndPrepare(CP_ENV),
          "test", null);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testRemoveReplicationPeer() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preRemoveReplicationPeer(ObserverContextImpl.createAndPrepare(CP_ENV),
          "test");
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testEnableReplicationPeer() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preEnableReplicationPeer(ObserverContextImpl.createAndPrepare(CP_ENV),
          "test");
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testDisableReplicationPeer() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDisableReplicationPeer(ObserverContextImpl.createAndPrepare(CP_ENV),
          "test");
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testGetReplicationPeerConfig() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preGetReplicationPeerConfig(
          ObserverContextImpl.createAndPrepare(CP_ENV), "test");
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testUpdateReplicationPeerConfig() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preUpdateReplicationPeerConfig(
          ObserverContextImpl.createAndPrepare(CP_ENV), "test", new ReplicationPeerConfig());
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testListReplicationPeers() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preListReplicationPeers(ObserverContextImpl.createAndPrepare(CP_ENV),
          "test");
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testRemoteLocks() throws Exception {
    String namespace = "preQueueNs";
    final TableName tableName = TableName.valueOf(namespace, name.getMethodName());
    HRegionInfo[] regionInfos = new HRegionInfo[] {new HRegionInfo(tableName)};

    // Setup Users
    // User will be granted ADMIN and CREATE on namespace. Should be denied before grant.
    User namespaceUser = User.createUserForTesting(conf, "qLNSUser", new String[0]);
    // User will be granted ADMIN and CREATE on table. Should be denied before grant.
    User tableACUser = User.createUserForTesting(conf, "qLTableACUser", new String[0]);
    // User will be granted READ, WRITE, EXECUTE on table. Should be denied.
    User tableRWXUser = User.createUserForTesting(conf, "qLTableRWXUser", new String[0]);
    grantOnTable(TEST_UTIL, tableRWXUser.getShortName(), tableName, null, null,
        Action.READ, Action.WRITE, Action.EXEC);
    // User with global READ, WRITE, EXECUTE should be denied lock access.
    User globalRWXUser = User.createUserForTesting(conf, "qLGlobalRWXUser", new String[0]);
    grantGlobal(TEST_UTIL, globalRWXUser.getShortName(), Action.READ, Action.WRITE, Action.EXEC);

    AccessTestAction namespaceLockAction = new AccessTestAction() {
      @Override public Object run() throws Exception {
        ACCESS_CONTROLLER.preRequestLock(ObserverContextImpl.createAndPrepare(CP_ENV), namespace,
            null, null, null);
        return null;
      }
    };
    verifyAllowed(namespaceLockAction, SUPERUSER, USER_ADMIN);
    verifyDenied(namespaceLockAction, globalRWXUser, tableACUser, namespaceUser, tableRWXUser);
    grantOnNamespace(TEST_UTIL, namespaceUser.getShortName(), namespace, Action.ADMIN);
    // Why I need this pause? I don't need it elsewhere.
    Threads.sleep(1000);
    verifyAllowed(namespaceLockAction, namespaceUser);

    AccessTestAction tableLockAction = new AccessTestAction() {
      @Override public Object run() throws Exception {
        ACCESS_CONTROLLER.preRequestLock(ObserverContextImpl.createAndPrepare(CP_ENV),
            null, tableName, null, null);
        return null;
      }
    };
    verifyAllowed(tableLockAction, SUPERUSER, USER_ADMIN, namespaceUser);
    verifyDenied(tableLockAction, globalRWXUser, tableACUser, tableRWXUser);
    grantOnTable(TEST_UTIL, tableACUser.getShortName(), tableName, null, null,
        Action.ADMIN, Action.CREATE);
    // See if this can fail (flakie) because grant hasn't propagated yet.
    for (int i = 0; i < 10; i++) {
      try {
        verifyAllowed(tableLockAction, tableACUser);
      } catch (AssertionError e) {
        LOG.warn("Retrying assertion error", e);
        Threads.sleep(1000);
        continue;
      }
    }

    AccessTestAction regionsLockAction = new AccessTestAction() {
      @Override public Object run() throws Exception {
        ACCESS_CONTROLLER.preRequestLock(ObserverContextImpl.createAndPrepare(CP_ENV),
            null, null, regionInfos, null);
        return null;
      }
    };
    verifyAllowed(regionsLockAction, SUPERUSER, USER_ADMIN, namespaceUser, tableACUser);
    verifyDenied(regionsLockAction, globalRWXUser, tableRWXUser);

    // Test heartbeats
    // Create a lock procedure and try sending heartbeat to it. It doesn't matter how the lock
    // was created, we just need namespace from the lock's tablename.
    LockProcedure proc = new LockProcedure(conf, tableName, LockType.EXCLUSIVE, "test", null);
    AccessTestAction regionLockHeartbeatAction = new AccessTestAction() {
      @Override public Object run() throws Exception {
        ACCESS_CONTROLLER.preLockHeartbeat(ObserverContextImpl.createAndPrepare(CP_ENV),
            proc.getTableName(), proc.getDescription());
        return null;
      }
    };
    verifyAllowed(regionLockHeartbeatAction, SUPERUSER, USER_ADMIN, namespaceUser, tableACUser);
    verifyDenied(regionLockHeartbeatAction, globalRWXUser, tableRWXUser);
  }

  @Test
  public void testAccessControlRevokeOnlyFewPermission() throws Throwable {
    TableName tname = TableName.valueOf("revoke");
    try {
      TEST_UTIL.createTable(tname, TEST_FAMILY);
      User testUserPerms = User.createUserForTesting(conf, "revokePerms", new String[0]);
      Permission.Action[] actions = { Action.READ, Action.WRITE };
      AccessControlClient.grant(TEST_UTIL.getConnection(), tname, testUserPerms.getShortName(),
        null, null, actions);

      List<UserPermission> userPermissions = AccessControlClient
          .getUserPermissions(TEST_UTIL.getConnection(), tname.getNameAsString());
      assertEquals(2, userPermissions.size());

      AccessControlClient.revoke(TEST_UTIL.getConnection(), tname, testUserPerms.getShortName(),
        null, null, Action.WRITE);

      userPermissions = AccessControlClient.getUserPermissions(TEST_UTIL.getConnection(),
        tname.getNameAsString());
      assertEquals(2, userPermissions.size());

      Permission.Action[] expectedAction = { Action.READ };
      boolean userFound = false;
      for (UserPermission p : userPermissions) {
        if (testUserPerms.getShortName().equals(p.getUser())) {
          assertArrayEquals(expectedAction, p.getPermission().getActions());
          userFound = true;
          break;
        }
      }
      assertTrue(userFound);
    } finally {
      TEST_UTIL.deleteTable(tname);
    }
  }

  @Test
  public void testGetClusterStatus() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preGetClusterMetrics(ObserverContextImpl.createAndPrepare(CP_ENV));
        return null;
      }
    };

    verifyAllowed(
        action, SUPERUSER, USER_ADMIN, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testExecuteProcedures() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preExecuteProcedures(ObserverContextImpl.createAndPrepare(RSCP_ENV));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER, USER_ADMIN);
  }

  @Test
  public void testGetUserPermissions() throws Throwable {
    Connection conn = null;
    try {
      conn = ConnectionFactory.createConnection(conf);
      User nSUser1 = User.createUserForTesting(conf, "nsuser1", new String[0]);
      User nSUser2 = User.createUserForTesting(conf, "nsuser2", new String[0]);
      User nSUser3 = User.createUserForTesting(conf, "nsuser3", new String[0]);

      // Global access groups
      User globalGroupUser1 =
          User.createUserForTesting(conf, "globalGroupUser1", new String[] { "group_admin" });
      User globalGroupUser2 = User.createUserForTesting(conf, "globalGroupUser2",
        new String[] { "group_admin", "group_create" });
      // Namespace access groups
      User nsGroupUser1 =
          User.createUserForTesting(conf, "nsGroupUser1", new String[] { "ns_group1" });
      User nsGroupUser2 =
          User.createUserForTesting(conf, "nsGroupUser2", new String[] { "ns_group2" });
      // table Access groups
      User tableGroupUser1 =
          User.createUserForTesting(conf, "tableGroupUser1", new String[] { "table_group1" });
      User tableGroupUser2 =
          User.createUserForTesting(conf, "tableGroupUser2", new String[] { "table_group2" });

      // Create namespaces
      String nsPrefix = "testNS";
      final String namespace1 = nsPrefix + "1";
      NamespaceDescriptor desc1 = NamespaceDescriptor.create(namespace1).build();
      createNamespace(TEST_UTIL, desc1);
      String namespace2 = nsPrefix + "2";
      NamespaceDescriptor desc2 = NamespaceDescriptor.create(namespace2).build();
      createNamespace(TEST_UTIL, desc2);

      // Grant namespace permission
      grantOnNamespace(TEST_UTIL, nSUser1.getShortName(), namespace1, Permission.Action.ADMIN);
      grantOnNamespace(TEST_UTIL, nSUser3.getShortName(), namespace1, Permission.Action.READ);
      grantOnNamespace(TEST_UTIL, toGroupEntry("ns_group1"), namespace1, Permission.Action.ADMIN);
      grantOnNamespace(TEST_UTIL, nSUser2.getShortName(), namespace2, Permission.Action.ADMIN);
      grantOnNamespace(TEST_UTIL, nSUser3.getShortName(), namespace2, Permission.Action.ADMIN);
      grantOnNamespace(TEST_UTIL, toGroupEntry("ns_group2"), namespace2, Permission.Action.READ,
        Permission.Action.WRITE);

      // Create tables
      TableName table1 = TableName.valueOf(namespace1 + TableName.NAMESPACE_DELIM + "t1");
      TableName table2 = TableName.valueOf(namespace2 + TableName.NAMESPACE_DELIM + "t2");
      byte[] TEST_FAMILY2 = Bytes.toBytes("f2");
      byte[] TEST_QUALIFIER2 = Bytes.toBytes("q2");
      createTestTable(table1, TEST_FAMILY);
      createTestTable(table2, TEST_FAMILY2);

      // Grant table permissions
      grantOnTable(TEST_UTIL, toGroupEntry("table_group1"), table1, null, null,
        Permission.Action.ADMIN);
      grantOnTable(TEST_UTIL, USER_ADMIN.getShortName(), table1, null, null,
        Permission.Action.ADMIN);
      grantOnTable(TEST_UTIL, USER_ADMIN_CF.getShortName(), table1, TEST_FAMILY, null,
        Permission.Action.ADMIN);
      grantOnTable(TEST_UTIL, USER_RW.getShortName(), table1, TEST_FAMILY, TEST_QUALIFIER,
        Permission.Action.READ);
      grantOnTable(TEST_UTIL, USER_RW.getShortName(), table1, TEST_FAMILY, TEST_QUALIFIER2,
        Permission.Action.WRITE);

      grantOnTable(TEST_UTIL, toGroupEntry("table_group2"), table2, null, null,
        Permission.Action.ADMIN);
      grantOnTable(TEST_UTIL, USER_ADMIN.getShortName(), table2, null, null,
        Permission.Action.ADMIN);
      grantOnTable(TEST_UTIL, USER_ADMIN_CF.getShortName(), table2, TEST_FAMILY2, null,
        Permission.Action.ADMIN);
      grantOnTable(TEST_UTIL, USER_RW.getShortName(), table2, TEST_FAMILY2, TEST_QUALIFIER,
        Permission.Action.READ);
      grantOnTable(TEST_UTIL, USER_RW.getShortName(), table2, TEST_FAMILY2, TEST_QUALIFIER2,
        Permission.Action.WRITE);

      List<UserPermission> userPermissions = null;
      Collection<String> superUsers = Superusers.getSuperUsers();
      int superUserCount = superUsers.size();

      // Global User ACL
      validateGlobalUserACLForGetUserPermissions(conn, nSUser1, globalGroupUser1, globalGroupUser2,
        superUsers, superUserCount);

      // Namespace ACL
      validateNamespaceUserACLForGetUserPermissions(conn, nSUser1, nSUser3, nsGroupUser1,
        nsGroupUser2, nsPrefix, namespace1, namespace2);

      // Table + Users
      validateTableACLForGetUserPermissions(conn, nSUser1, tableGroupUser1, tableGroupUser2,
        nsPrefix, table1, table2, TEST_QUALIFIER2, superUsers);

      // exception scenarios

      try {
        // test case with table name as null
        assertEquals(3, AccessControlClient.getUserPermissions(conn, null, TEST_FAMILY).size());
        fail("this should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException ex) {
        // expected
      }
      try {
        // test case with table name as emplty
        assertEquals(3, AccessControlClient
            .getUserPermissions(conn, HConstants.EMPTY_STRING, TEST_FAMILY).size());
        fail("this should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException ex) {
        // expected
      }
      try {
        // test case with table name as namespace name
        assertEquals(3,
          AccessControlClient.getUserPermissions(conn, "@" + namespace2, TEST_FAMILY).size());
        fail("this should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException ex) {
        // expected
      }

      // Clean the table and namespace
      deleteTable(TEST_UTIL, table1);
      deleteTable(TEST_UTIL, table2);
      deleteNamespace(TEST_UTIL, namespace1);
      deleteNamespace(TEST_UTIL, namespace2);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  @Test
  public void testHasPermission() throws Throwable {
    Connection conn = null;
    try {
      conn = ConnectionFactory.createConnection(conf);
      // Create user and set namespace ACL
      User user1 = User.createUserForTesting(conf, "testHasPermissionUser1", new String[0]);
      // Grant namespace permission
      grantOnNamespaceUsingAccessControlClient(TEST_UTIL, conn, user1.getShortName(),
        NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), Permission.Action.ADMIN,
        Permission.Action.CREATE, Permission.Action.READ);

      // Create user and set table ACL
      User user2 = User.createUserForTesting(conf, "testHasPermissionUser2", new String[0]);
      // Grant namespace permission
      grantOnTableUsingAccessControlClient(TEST_UTIL, conn, user2.getShortName(), TEST_TABLE,
        TEST_FAMILY, TEST_QUALIFIER, Permission.Action.READ, Permission.Action.WRITE);

      // Verify action privilege
      AccessTestAction hasPermissionActionCP = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table acl = conn.getTable(PermissionStorage.ACL_TABLE_NAME)) {
            BlockingRpcChannel service = acl.coprocessorService(TEST_TABLE.getName());
            AccessControlService.BlockingInterface protocol =
                AccessControlService.newBlockingStub(service);
            Permission.Action[] actions = { Permission.Action.READ, Permission.Action.WRITE };
            AccessControlUtil.hasPermission(null, protocol, TEST_TABLE, TEST_FAMILY,
              HConstants.EMPTY_BYTE_ARRAY, "dummy", actions);
          }
          return null;
        }
      };
      AccessTestAction hasPermissionAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          try (Connection conn = ConnectionFactory.createConnection(conf)) {
            Permission.Action[] actions = { Permission.Action.READ, Permission.Action.WRITE };
            conn.getAdmin().hasUserPermissions("dummy",
              Arrays.asList(Permission.newBuilder(TEST_TABLE).withFamily(TEST_FAMILY)
                  .withQualifier(HConstants.EMPTY_BYTE_ARRAY).withActions(actions).build()));
          }
          return null;
        }
      };
      verifyAllowed(hasPermissionActionCP, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN, USER_OWNER,
        USER_ADMIN_CF, user1);
      verifyDenied(hasPermissionActionCP, USER_CREATE, USER_RW, USER_RO, USER_NONE, user2);
      verifyAllowed(hasPermissionAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN, USER_OWNER,
        USER_ADMIN_CF, user1);
      verifyDenied(hasPermissionAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, user2);

      // Check for global user
      assertTrue(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, USER_ADMIN.getShortName(),
        Permission.Action.READ, Permission.Action.WRITE, Permission.Action.CREATE,
        Permission.Action.ADMIN));
      assertFalse(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, USER_ADMIN.getShortName(),
        Permission.Action.READ, Permission.Action.WRITE, Permission.Action.CREATE,
        Permission.Action.ADMIN, Permission.Action.EXEC));

      // Check for namespace access user
      assertTrue(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, user1.getShortName(),
        Permission.Action.ADMIN, Permission.Action.CREATE));
      assertFalse(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, user1.getShortName(),
        Permission.Action.ADMIN, Permission.Action.READ, Permission.Action.EXEC));

      // Check for table owner
      assertTrue(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, USER_OWNER.getShortName(),
        Permission.Action.READ, Permission.Action.WRITE, Permission.Action.EXEC,
        Permission.Action.CREATE, Permission.Action.ADMIN));

      // Check for table user
      assertTrue(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, USER_CREATE.getShortName(),
        Permission.Action.READ, Permission.Action.WRITE));
      assertFalse(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, USER_RO.getShortName(),
        Permission.Action.READ, Permission.Action.WRITE));

      // Check for family access user
      assertTrue(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(), TEST_FAMILY,
        HConstants.EMPTY_BYTE_ARRAY, USER_RO.getShortName(), Permission.Action.READ));
      assertTrue(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(), TEST_FAMILY,
        HConstants.EMPTY_BYTE_ARRAY, USER_RW.getShortName(), Permission.Action.READ,
        Permission.Action.WRITE));
      assertFalse(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, USER_ADMIN_CF.getShortName(),
        Permission.Action.ADMIN, Permission.Action.CREATE));
      assertTrue(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(), TEST_FAMILY,
        HConstants.EMPTY_BYTE_ARRAY, USER_ADMIN_CF.getShortName(), Permission.Action.ADMIN,
        Permission.Action.CREATE));
      assertFalse(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(), TEST_FAMILY,
        HConstants.EMPTY_BYTE_ARRAY, USER_ADMIN_CF.getShortName(), Permission.Action.READ));

      // Check for qualifier access user
      assertTrue(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(), TEST_FAMILY,
        TEST_QUALIFIER, user2.getShortName(), Permission.Action.READ, Permission.Action.WRITE));
      assertFalse(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(), TEST_FAMILY,
        TEST_QUALIFIER, user2.getShortName(), Permission.Action.EXEC, Permission.Action.READ));
      assertFalse(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(),
        HConstants.EMPTY_BYTE_ARRAY, TEST_QUALIFIER, USER_RW.getShortName(),
        Permission.Action.WRITE, Permission.Action.READ));

      // exception scenarios
      try {
        // test case with table name as null
        assertTrue(AccessControlClient.hasPermission(conn, null, HConstants.EMPTY_BYTE_ARRAY,
          HConstants.EMPTY_BYTE_ARRAY, null, Permission.Action.READ));
        fail("this should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException ex) {
        // expected
      }
      try {
        // test case with username as null
        assertTrue(AccessControlClient.hasPermission(conn, TEST_TABLE.getNameAsString(),
          HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, null, Permission.Action.READ));
        fail("this should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException ex) {
        // expected
      }

      revokeFromNamespaceUsingAccessControlClient(TEST_UTIL, conn, user1.getShortName(),
        NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), Permission.Action.ADMIN,
        Permission.Action.CREATE, Permission.Action.READ);
      revokeFromTableUsingAccessControlClient(TEST_UTIL, conn, user2.getShortName(), TEST_TABLE,
        TEST_FAMILY, TEST_QUALIFIER, Permission.Action.READ, Permission.Action.WRITE);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  @Test
  public void testSwitchRpcThrottle() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSwitchRpcThrottle(ObserverContextImpl.createAndPrepare(CP_ENV), true);
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testIsRpcThrottleEnabled() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preIsRpcThrottleEnabled(ObserverContextImpl.createAndPrepare(CP_ENV));
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testSwitchExceedThrottleQuota() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSwitchExceedThrottleQuota(ObserverContextImpl.createAndPrepare(CP_ENV),
          true);
        return null;
      }
    };
    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  /*
   * Validate Global User ACL
   */
  private void validateGlobalUserACLForGetUserPermissions(final Connection conn, User nSUser1,
      User globalGroupUser1, User globalGroupUser2, Collection<String> superUsers,
      int superUserCount) throws Throwable {
    // Verify action privilege
    AccessTestAction globalUserPermissionAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          conn.getAdmin().getUserPermissions(
            GetUserPermissionsRequest.newBuilder().withUserName("dummy").build());
        }
        return null;
      }
    };
    verifyAllowed(globalUserPermissionAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(globalUserPermissionAction, USER_GROUP_CREATE, USER_GROUP_READ, USER_GROUP_WRITE);

    // Validate global user permission
    List<UserPermission> userPermissions;
    assertEquals(5 + superUserCount, AccessControlClient.getUserPermissions(conn, null).size());
    assertEquals(5 + superUserCount,
      AccessControlClient.getUserPermissions(conn, HConstants.EMPTY_STRING).size());
    assertEquals(5 + superUserCount,
      AccessControlClient.getUserPermissions(conn, null, HConstants.EMPTY_STRING).size());
    userPermissions = AccessControlClient.getUserPermissions(conn, null, USER_ADMIN.getName());
    verifyGetUserPermissionResult(userPermissions, 1, null, null, USER_ADMIN.getName(), superUsers);
    assertEquals(0, AccessControlClient.getUserPermissions(conn, null, nSUser1.getName()).size());
    // Global group user ACL
    assertEquals(1,
      AccessControlClient.getUserPermissions(conn, null, globalGroupUser1.getName()).size());
    assertEquals(2,
      AccessControlClient.getUserPermissions(conn, null, globalGroupUser2.getName()).size());
  }

  /*
   * Validate Namespace User ACL
   */
  private void validateNamespaceUserACLForGetUserPermissions(final Connection conn, User nSUser1,
      User nSUser3, User nsGroupUser1, User nsGroupUser2, String nsPrefix, final String namespace1,
      String namespace2) throws Throwable {
    AccessTestAction namespaceUserPermissionAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          conn.getAdmin().getUserPermissions(
            GetUserPermissionsRequest.newBuilder(namespace1).withUserName("dummy").build());
        }
        return null;
      }
    };
    verifyAllowed(namespaceUserPermissionAction, SUPERUSER, USER_GROUP_ADMIN, USER_ADMIN, nSUser1,
      nsGroupUser1);
    verifyDenied(namespaceUserPermissionAction, USER_GROUP_CREATE, USER_GROUP_READ,
      USER_GROUP_WRITE, nSUser3, nsGroupUser2);

    List<UserPermission> userPermissions;
    assertEquals(6, AccessControlClient.getUserPermissions(conn, "@" + nsPrefix + ".*").size());
    assertEquals(3, AccessControlClient.getUserPermissions(conn, "@" + namespace1).size());
    assertEquals(3, AccessControlClient
        .getUserPermissions(conn, "@" + namespace1, HConstants.EMPTY_STRING).size());
    userPermissions =
        AccessControlClient.getUserPermissions(conn, "@" + namespace1, nSUser1.getName());
    verifyGetUserPermissionResult(userPermissions, 1, null, null, nSUser1.getName(), null);
    userPermissions =
        AccessControlClient.getUserPermissions(conn, "@" + namespace1, nSUser3.getName());
    verifyGetUserPermissionResult(userPermissions, 1, null, null, nSUser3.getName(), null);
    assertEquals(0,
      AccessControlClient.getUserPermissions(conn, "@" + namespace1, USER_ADMIN.getName()).size());
    // Namespace group user ACL
    assertEquals(1, AccessControlClient
        .getUserPermissions(conn, "@" + namespace1, nsGroupUser1.getName()).size());
    assertEquals(1, AccessControlClient
        .getUserPermissions(conn, "@" + namespace2, nsGroupUser2.getName()).size());
  }

  /*
   * Validate Table User ACL
   */
  private void validateTableACLForGetUserPermissions(final Connection conn, User nSUser1,
      User tableGroupUser1, User tableGroupUser2, String nsPrefix, TableName table1,
      TableName table2, byte[] TEST_QUALIFIER2, Collection<String> superUsers) throws Throwable {
    AccessTestAction tableUserPermissionAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
          conn.getAdmin().getUserPermissions(GetUserPermissionsRequest.newBuilder(TEST_TABLE)
              .withFamily(TEST_FAMILY).withQualifier(TEST_QUALIFIER).withUserName("dummy").build());
        }
        return null;
      }
    };
    verifyAllowed(tableUserPermissionAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_ADMIN_CF);
    verifyDenied(tableUserPermissionAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_CREATE);

    List<UserPermission> userPermissions;
    assertEquals(12, AccessControlClient.getUserPermissions(conn, nsPrefix + ".*").size());
    assertEquals(6, AccessControlClient.getUserPermissions(conn, table1.getNameAsString()).size());
    assertEquals(6, AccessControlClient
        .getUserPermissions(conn, table1.getNameAsString(), HConstants.EMPTY_STRING).size());
    userPermissions = AccessControlClient.getUserPermissions(conn, table1.getNameAsString(),
      USER_ADMIN_CF.getName());
    verifyGetUserPermissionResult(userPermissions, 1, null, null, USER_ADMIN_CF.getName(), null);
    assertEquals(0, AccessControlClient
        .getUserPermissions(conn, table1.getNameAsString(), nSUser1.getName()).size());
    // Table group user ACL
    assertEquals(1, AccessControlClient
        .getUserPermissions(conn, table1.getNameAsString(), tableGroupUser1.getName()).size());
    assertEquals(1, AccessControlClient
        .getUserPermissions(conn, table2.getNameAsString(), tableGroupUser2.getName()).size());

    // Table Users + CF
    assertEquals(12, AccessControlClient
        .getUserPermissions(conn, nsPrefix + ".*", HConstants.EMPTY_BYTE_ARRAY).size());
    userPermissions = AccessControlClient.getUserPermissions(conn, nsPrefix + ".*", TEST_FAMILY);
    verifyGetUserPermissionResult(userPermissions, 3, TEST_FAMILY, null, null, null);
    assertEquals(0, AccessControlClient
        .getUserPermissions(conn, table1.getNameAsString(), Bytes.toBytes("dummmyCF")).size());

    // Table Users + CF + User
    assertEquals(3,
      AccessControlClient
          .getUserPermissions(conn, table1.getNameAsString(), TEST_FAMILY, HConstants.EMPTY_STRING)
          .size());
    userPermissions = AccessControlClient.getUserPermissions(conn, table1.getNameAsString(),
      TEST_FAMILY, USER_ADMIN_CF.getName());
    verifyGetUserPermissionResult(userPermissions, 1, null, null, USER_ADMIN_CF.getName(),
      superUsers);
    assertEquals(0, AccessControlClient
        .getUserPermissions(conn, table1.getNameAsString(), TEST_FAMILY, nSUser1.getName()).size());

    // Table Users + CF + CQ
    assertEquals(3, AccessControlClient.getUserPermissions(conn, table1.getNameAsString(),
      TEST_FAMILY, HConstants.EMPTY_BYTE_ARRAY).size());
    assertEquals(1, AccessControlClient
        .getUserPermissions(conn, table1.getNameAsString(), TEST_FAMILY, TEST_QUALIFIER).size());
    assertEquals(1, AccessControlClient
        .getUserPermissions(conn, table1.getNameAsString(), TEST_FAMILY, TEST_QUALIFIER2).size());
    assertEquals(2, AccessControlClient.getUserPermissions(conn, table1.getNameAsString(),
      HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, USER_RW.getName()).size());
    assertEquals(0, AccessControlClient
        .getUserPermissions(conn, table1.getNameAsString(), TEST_FAMILY, Bytes.toBytes("dummmyCQ"))
        .size());

    // Table Users + CF + CQ + User
    assertEquals(3, AccessControlClient.getUserPermissions(conn, table1.getNameAsString(),
      TEST_FAMILY, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_STRING).size());
    assertEquals(1, AccessControlClient.getUserPermissions(conn, table1.getNameAsString(),
      TEST_FAMILY, TEST_QUALIFIER, USER_RW.getName()).size());
    assertEquals(1, AccessControlClient.getUserPermissions(conn, table1.getNameAsString(),
      TEST_FAMILY, TEST_QUALIFIER2, USER_RW.getName()).size());
    assertEquals(0, AccessControlClient.getUserPermissions(conn, table1.getNameAsString(),
      TEST_FAMILY, TEST_QUALIFIER2, nSUser1.getName()).size());
  }

  /*
   * Validate the user permission against the specified column family, column qualifier and user
   * name.
   */
  private void verifyGetUserPermissionResult(List<UserPermission> userPermissions, int resultCount,
      byte[] cf, byte[] cq, String userName, Collection<String> superUsers) {
    assertEquals(resultCount, userPermissions.size());

    for (UserPermission perm : userPermissions) {
      if (perm.getPermission() instanceof TablePermission) {
        TablePermission tablePerm = (TablePermission) perm.getPermission();
        if (cf != null) {
          assertTrue(Bytes.equals(cf, tablePerm.getFamily()));
        }
        if (cq != null) {
          assertTrue(Bytes.equals(cq, tablePerm.getQualifier()));
        }
        if (userName != null
          && (superUsers == null || !superUsers.contains(perm.getUser()))) {
          assertTrue(userName.equals(perm.getUser()));
        }
      } else if (perm.getPermission() instanceof NamespacePermission ||
          perm.getPermission() instanceof GlobalPermission) {
        if (userName != null &&
          (superUsers == null || !superUsers.contains(perm.getUser()))) {
          assertTrue(userName.equals(perm.getUser()));
        }
      }
    }
  }

  /*
   * Dummy ShellBasedUnixGroupsMapping class to retrieve the groups for the test users.
   */
  public static class MyShellBasedUnixGroupsMapping extends ShellBasedUnixGroupsMapping
      implements GroupMappingServiceProvider {
    @Override
    public List<String> getGroups(String user) throws IOException {
      if (user.equals("globalGroupUser1")) {
        return Arrays.asList(new String[] { "group_admin" });
      } else if (user.equals("globalGroupUser2")) {
        return Arrays.asList(new String[] { "group_admin", "group_create" });
      } else if (user.equals("nsGroupUser1")) {
        return Arrays.asList(new String[] { "ns_group1" });
      } else if (user.equals("nsGroupUser2")) {
        return Arrays.asList(new String[] { "ns_group2" });
      } else if (user.equals("tableGroupUser1")) {
        return Arrays.asList(new String[] { "table_group1" });
      } else if (user.equals("tableGroupUser2")) {
        return Arrays.asList(new String[] { "table_group2" });
      } else {
        return super.getGroups(user);
      }
    }
  }
}
