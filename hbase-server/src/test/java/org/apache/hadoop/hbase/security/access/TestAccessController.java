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

import static org.apache.hadoop.hbase.AuthUtil.toGroupEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
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
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProcedureProtos;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * Performs authorization checks for common operations, according to different
 * levels of authorized users.
 */
@Category(LargeTests.class)
public class TestAccessController extends SecureTestUtil {
  private static final Log LOG = LogFactory.getLog(TestAccessController.class);

  static {
    Logger.getLogger(AccessController.class).setLevel(Level.TRACE);
    Logger.getLogger(AccessControlFilter.class).setLevel(Level.TRACE);
    Logger.getLogger(TableAuthManager.class).setLevel(Level.TRACE);
  }

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

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
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

    Region region = TEST_UTIL.getHBaseCluster().getRegions(TEST_TABLE).get(0);
    RegionCoprocessorHost rcpHost = region.getCoprocessorHost();
    RCP_ENV = rcpHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
      Coprocessor.PRIORITY_HIGHEST, 1, conf);

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

    assertEquals(5, AccessControlLists.getTablePermissions(conf, TEST_TABLE).size());
    try {
      assertEquals(5, AccessControlClient.getUserPermissions(systemUserConnection,
          TEST_TABLE.toString()).size());
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.getUserPermissions. ", e);
    }
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
    assertEquals(0, AccessControlLists.getTablePermissions(conf, TEST_TABLE).size());
    assertEquals(
      0,
      AccessControlLists.getNamespacePermissions(conf,
        TEST_TABLE.getNamespaceAsString()).size());
  }

  @Test
  public void testTableCreate() throws Exception {
    AccessTestAction createTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testnewtable"));
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        ACCESS_CONTROLLER.preCreateTable(ObserverContext.createAndPrepare(CP_ENV, null), htd, null);
        return null;
      }
    };

    // verify that superuser can create tables
    verifyAllowed(createTable, SUPERUSER, USER_ADMIN, USER_GROUP_CREATE);

    // all others should be denied
    verifyDenied(createTable, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_ADMIN,
      USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testTableModify() throws Exception {
    AccessTestAction modifyTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        htd.addFamily(new HColumnDescriptor("fam_" + User.getCurrent().getShortName()));
        ACCESS_CONTROLLER.preModifyTable(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE, htd);
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
            .preDeleteTable(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE);
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
            .preTruncateTable(ObserverContext.createAndPrepare(CP_ENV, null),
              TEST_TABLE);
        return null;
      }
    };

    verifyAllowed(truncateTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_GROUP_CREATE,
      USER_GROUP_ADMIN);
    verifyDenied(truncateTable, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testAddColumn() throws Exception {
    final HColumnDescriptor hcd = new HColumnDescriptor("fam_new");
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAddColumn(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE,
          hcd);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_GROUP_CREATE,
      USER_GROUP_ADMIN);
    verifyDenied(action, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testModifyColumn() throws Exception {
    final HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    hcd.setMaxVersions(10);
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preModifyColumn(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE, hcd);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_ADMIN_CF,
      USER_GROUP_CREATE, USER_GROUP_ADMIN);
    verifyDenied(action, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testDeleteColumn() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteColumn(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE, TEST_FAMILY);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_ADMIN_CF,
      USER_GROUP_CREATE, USER_GROUP_ADMIN);
    verifyDenied(action, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testTableDisable() throws Exception {
    AccessTestAction disableTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDisableTable(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE);
        return null;
      }
    };

    AccessTestAction disableAclTable = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDisableTable(ObserverContext.createAndPrepare(CP_ENV, null),
            AccessControlLists.ACL_TABLE_NAME);
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
            .preEnableTable(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE);
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
      this.setOwner(env.getRequestUser().getUGI().getShortUserName());
    }

    @Override
    public TableName getTableName() {
      return tableName;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return null;
    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return true;
    }

    @Override
    protected void serializeStateData(OutputStream stream) throws IOException {
      TestProcedureProtos.TestTableDDLStateData.Builder testTableDDLMsg =
          TestProcedureProtos.TestTableDDLStateData.newBuilder()
          .setTableName(tableName.getNameAsString());
      testTableDDLMsg.build().writeDelimitedTo(stream);
    }

    @Override
    protected void deserializeStateData(InputStream stream) throws IOException {
      TestProcedureProtos.TestTableDDLStateData testTableDDLMsg =
          TestProcedureProtos.TestTableDDLStateData.parseDelimitedFrom(stream);
      tableName = TableName.valueOf(testTableDDLMsg.getTableName());
    }

    @Override
    protected Procedure[] execute(MasterProcedureEnv env) {
      // Not letting the procedure to complete until timed out
      setState(ProcedureState.WAITING_TIMEOUT);
      return null;
    }

    @Override
    protected void rollback(MasterProcedureEnv env) {
    }
  }

  @Test
  public void testAbortProcedure() throws Exception {
    final TableName tableName = TableName.valueOf("testAbortProcedure");
    final ProcedureExecutor<MasterProcedureEnv> procExec =
        TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    Procedure proc = new TestTableDDLProcedure(procExec.getEnvironment(), tableName);
    proc.setOwner(USER_OWNER.getShortName());
    final long procId = procExec.submitProcedure(proc);

    AccessTestAction abortProcedureAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER
        .preAbortProcedure(ObserverContext.createAndPrepare(CP_ENV, null), procExec, procId);
       return null;
      }
    };

    verifyAllowed(abortProcedureAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyAllowed(abortProcedureAction, USER_OWNER);
    verifyDenied(
      abortProcedureAction, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testListProcedures() throws Exception {
    final TableName tableName = TableName.valueOf("testAbortProcedure");
    final ProcedureExecutor<MasterProcedureEnv> procExec =
        TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    Procedure proc = new TestTableDDLProcedure(procExec.getEnvironment(), tableName);
    proc.setOwner(USER_OWNER.getShortName());
    final long procId = procExec.submitProcedure(proc);
    final List<ProcedureInfo> procInfoList = procExec.listProcedures();

    AccessTestAction listProceduresAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        List<ProcedureInfo> procInfoListClone = new ArrayList<ProcedureInfo>(procInfoList.size());
        for(ProcedureInfo pi : procInfoList) {
          procInfoListClone.add(pi.clone());
        }
        ACCESS_CONTROLLER
        .postListProcedures(ObserverContext.createAndPrepare(CP_ENV, null), procInfoListClone);
       return null;
      }
    };

    verifyAllowed(listProceduresAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyAllowed(listProceduresAction, USER_OWNER);
    verifyIfNull(
      listProceduresAction, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test (timeout=180000)
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
        ACCESS_CONTROLLER.preMove(ObserverContext.createAndPrepare(CP_ENV, null),
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
        ACCESS_CONTROLLER.preAssign(ObserverContext.createAndPrepare(CP_ENV, null), hri);
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
        ACCESS_CONTROLLER.preUnassign(ObserverContext.createAndPrepare(CP_ENV, null), hri, false);
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
        ACCESS_CONTROLLER.preRegionOffline(ObserverContext.createAndPrepare(CP_ENV, null), hri);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testBalance() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalance(ObserverContext.createAndPrepare(CP_ENV, null));
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
        ACCESS_CONTROLLER.preBalanceSwitch(ObserverContext.createAndPrepare(CP_ENV, null), true);
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
        ACCESS_CONTROLLER.preShutdown(ObserverContext.createAndPrepare(CP_ENV, null));
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
        ACCESS_CONTROLLER.preStopMaster(ObserverContext.createAndPrepare(CP_ENV, null));
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
  public void testSplit() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSplit(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testSplitWithSplitRow() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSplit(
            ObserverContext.createAndPrepare(RCP_ENV, null),
            TEST_ROW);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
      USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testMergeRegions() throws Exception {
    final TableName tname = TableName.valueOf("testMergeRegions");
    createTestTable(tname);
    try {
      final List<HRegion> regions = TEST_UTIL.getHBaseCluster().findRegionsForTable(tname);
      assertTrue("not enough regions: " + regions.size(), regions.size() >= 2);
      AccessTestAction action = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          ACCESS_CONTROLLER.preMerge(ObserverContext.createAndPrepare(RSCP_ENV, null),
            regions.get(0), regions.get(1));
          return null;
        }
      };

      verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_GROUP_ADMIN);
      verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_GROUP_READ,
        USER_GROUP_WRITE, USER_GROUP_CREATE);
    } finally {
      TEST_UTIL.deleteTable(tname);
    }
  }

  @Test
  public void testFlush() throws Exception {
    AccessTestAction action = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preFlush(ObserverContext.createAndPrepare(RCP_ENV, null));
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
        ACCESS_CONTROLLER.preCompact(ObserverContext.createAndPrepare(RCP_ENV, null), null, null,
          ScanType.COMPACT_RETAIN_DELETES);
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
            Table t = conn.getTable(TEST_TABLE)) {
          ResultScanner scanner = t.getScanner(s);
          try {
            for (Result r = scanner.next(); r != null; r = scanner.next()) {
              // do nothing
            }
          } catch (IOException e) {
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
        p.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(1));
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
        d.deleteFamily(TEST_FAMILY);
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
            Table t = conn.getTable(TEST_TABLE)) {
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
        d.deleteFamily(TEST_FAMILY);
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          t.checkAndDelete(TEST_ROW, TEST_FAMILY, TEST_QUALIFIER,
              Bytes.toBytes("test_value"), d);
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
        p.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(1));
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          t.checkAndPut(TEST_ROW, TEST_FAMILY, TEST_QUALIFIER,
              Bytes.toBytes("test_value"), p);
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
      fs.setPermission(dir, FsPermission.valueOf("-rwxrwxrwx"));

      AccessTestAction bulkLoadAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          int numRows = 3;

          // Making the assumption that the test table won't split between the range
          byte[][][] hfileRanges = { { { (byte) 0 }, { (byte) 9 } } };

          Path bulkLoadBasePath = new Path(dir, new Path(User.getCurrent().getName()));
          new BulkLoadHelper(bulkLoadBasePath).bulkLoadHFile(TEST_TABLE, TEST_FAMILY,
            TEST_QUALIFIER, hfileRanges, numRows);

          return null;
        }
      };

      // User performing bulk loads must have privilege to read table metadata
      // (ADMIN or CREATE)
      verifyAllowed(bulkLoadAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE,
        USER_GROUP_CREATE);
      verifyDenied(bulkLoadAction, USER_RW, USER_NONE, USER_RO, USER_GROUP_READ, USER_GROUP_WRITE,
        USER_GROUP_ADMIN);
    } finally {
      // Reinit after the bulk upload
      TEST_UTIL.getHBaseAdmin().disableTable(TEST_TABLE);
      TEST_UTIL.getHBaseAdmin().enableTable(TEST_TABLE);
    }
  }

  public class BulkLoadHelper {
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
        writer = HFile.getWriterFactory(conf, new CacheConfig(conf))
            .withPath(fs, path)
            .withFileContext(context)
            .create();
        // subtract 2 since numRows doesn't include boundary keys
        for (byte[] key : Bytes.iterateOnSplits(startKey, endKey, true, numRows-2)) {
          KeyValue kv = new KeyValue(key, family, qualifier, now, key);
          writer.append(kv);
        }
      } finally {
        if(writer != null)
          writer.close();
      }
    }

    private void bulkLoadHFile(
        TableName tableName,
        byte[] family,
        byte[] qualifier,
        byte[][][] hfileRanges,
        int numRowsPerRange) throws Exception {

      Path familyDir = new Path(loadPath, Bytes.toString(family));
      fs.mkdirs(familyDir);
      int hfileIdx = 0;
      for (byte[][] range : hfileRanges) {
        byte[] from = range[0];
        byte[] to = range[1];
        createHFile(new Path(familyDir, "hfile_"+(hfileIdx++)),
            family, qualifier, from, to, numRowsPerRange);
      }
      //set global read so RegionServer can move it
      setPermission(loadPath, FsPermission.valueOf("-rwxrwxrwx"));

      try (Connection conn = ConnectionFactory.createConnection(conf);
           HTable table = (HTable)conn.getTable(tableName)) {
        TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(loadPath, table);
      }
    }

    public void setPermission(Path dir, FsPermission perm) throws IOException {
      if(!fs.getFileStatus(dir).isDirectory()) {
        fs.setPermission(dir,perm);
      }
      else {
        for(FileStatus el : fs.listStatus(dir)) {
          fs.setPermission(el.getPath(), perm);
          setPermission(el.getPath() , perm);
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
        put.add(TEST_FAMILY, qualifier, Bytes.toBytes(1));
        Append append = new Append(row);
        append.add(TEST_FAMILY, qualifier, Bytes.toBytes(2));
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
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table acl = conn.getTable(AccessControlLists.ACL_TABLE_NAME)) {
          BlockingRpcChannel service = acl.coprocessorService(TEST_TABLE.getName());
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          ProtobufUtil.grant(protocol, USER_RO.getShortName(), TEST_TABLE,
            TEST_FAMILY, null, Action.READ);
        }
        return null;
      }
    };

    AccessTestAction revokeAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table acl = conn.getTable(AccessControlLists.ACL_TABLE_NAME)) {
          BlockingRpcChannel service = acl.coprocessorService(TEST_TABLE.getName());
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          ProtobufUtil.revoke(protocol, USER_RO.getShortName(), TEST_TABLE, TEST_FAMILY, null,
            Action.READ);
        }
        return null;
      }
    };

    AccessTestAction getTablePermissionsAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table acl = conn.getTable(AccessControlLists.ACL_TABLE_NAME)) {
          BlockingRpcChannel service = acl.coprocessorService(TEST_TABLE.getName());
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          ProtobufUtil.getUserPermissions(protocol, TEST_TABLE);
        }
        return null;
      }
    };

    AccessTestAction getGlobalPermissionsAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table acl = conn.getTable(AccessControlLists.ACL_TABLE_NAME)) {
          BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          ProtobufUtil.getUserPermissions(protocol);
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
    Admin admin = TEST_UTIL.getHBaseAdmin();
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
          p.add(family1, qualifier, Bytes.toBytes("v1"));
          p.add(family2, qualifier, Bytes.toBytes("v2"));
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
            t.put(p);
          }
          return null;
        }
      };

      AccessTestAction putAction1 = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          Put p = new Put(Bytes.toBytes("a"));
          p.add(family1, qualifier, Bytes.toBytes("v1"));
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
          p.add(family2, qualifier, Bytes.toBytes("v2"));
          try (Connection conn = ConnectionFactory.createConnection(conf);
              Table t = conn.getTable(tableName)) {
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
              Table t = conn.getTable(tableName)) {
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
          d.deleteFamily(family1);
          d.deleteFamily(family2);
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
          d.deleteFamily(family1);
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
          d.deleteFamily(family2);
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
      grantOnTable(TEST_UTIL, tblUser.getShortName(), tableName, null, null,
        Permission.Action.READ);

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

  private boolean hasFoundUserPermission(UserPermission userPermission, List<UserPermission> perms) {
    return perms.contains(userPermission);
  }

  @Test
  public void testPostGrantRevokeAtQualifierLevel() throws Exception {
    final TableName tableName =
        TableName.valueOf("testGrantRevokeAtQualifierLevel");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");

    // create table
    Admin admin = TEST_UTIL.getHBaseAdmin();
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
          p.add(family1, qualifier, Bytes.toBytes("v1"));
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
          d.deleteColumn(family1, qualifier);
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
    final TableName tableName =
        TableName.valueOf("testPermissionList");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");

    // create table
    Admin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      deleteTable(TEST_UTIL, tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    htd.setOwner(USER_OWNER);
    createTable(TEST_UTIL, htd);

    List<UserPermission> perms;
    try {
      Table acl = systemUserConnection.getTable(AccessControlLists.ACL_TABLE_NAME);
      try {
        BlockingRpcChannel service = acl.coprocessorService(tableName.getName());
        AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
        perms = ProtobufUtil.getUserPermissions(protocol, tableName);
      } finally {
        acl.close();
      }

      UserPermission ownerperm =
          new UserPermission(Bytes.toBytes(USER_OWNER.getName()), tableName, null, Action.values());
      assertTrue("Owner should have all permissions on table",
        hasFoundUserPermission(ownerperm, perms));

      User user = User.createUserForTesting(TEST_UTIL.getConfiguration(), "user", new String[0]);
      byte[] userName = Bytes.toBytes(user.getShortName());

      UserPermission up =
          new UserPermission(userName, tableName, family1, qualifier, Permission.Action.READ);
      assertFalse("User should not be granted permission: " + up.toString(),
        hasFoundUserPermission(up, perms));

      // grant read permission
      grantOnTable(TEST_UTIL, user.getShortName(), tableName, family1, qualifier,
        Permission.Action.READ);

      acl = systemUserConnection.getTable(AccessControlLists.ACL_TABLE_NAME);
      try {
        BlockingRpcChannel service = acl.coprocessorService(tableName.getName());
        AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
        perms = ProtobufUtil.getUserPermissions(protocol, tableName);
      } finally {
        acl.close();
      }

      UserPermission upToVerify =
          new UserPermission(userName, tableName, family1, qualifier, Permission.Action.READ);
      assertTrue("User should be granted permission: " + upToVerify.toString(),
        hasFoundUserPermission(upToVerify, perms));

      upToVerify =
          new UserPermission(userName, tableName, family1, qualifier, Permission.Action.WRITE);
      assertFalse("User should not be granted permission: " + upToVerify.toString(),
        hasFoundUserPermission(upToVerify, perms));

      // grant read+write
      grantOnTable(TEST_UTIL, user.getShortName(), tableName, family1, qualifier,
        Permission.Action.WRITE, Permission.Action.READ);

      acl = systemUserConnection.getTable(AccessControlLists.ACL_TABLE_NAME);
      try {
        BlockingRpcChannel service = acl.coprocessorService(tableName.getName());
        AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
        perms = ProtobufUtil.getUserPermissions(protocol, tableName);
      } finally {
        acl.close();
      }

      upToVerify =
          new UserPermission(userName, tableName, family1, qualifier, Permission.Action.WRITE,
              Permission.Action.READ);
      assertTrue("User should be granted permission: " + upToVerify.toString(),
        hasFoundUserPermission(upToVerify, perms));

      // revoke
      revokeFromTable(TEST_UTIL, user.getShortName(), tableName, family1, qualifier,
        Permission.Action.WRITE, Permission.Action.READ);

      acl = systemUserConnection.getTable(AccessControlLists.ACL_TABLE_NAME);
      try {
        BlockingRpcChannel service = acl.coprocessorService(tableName.getName());
        AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
        perms = ProtobufUtil.getUserPermissions(protocol, tableName);
      } finally {
        acl.close();
      }

      assertFalse("User should not be granted permission: " + upToVerify.toString(),
        hasFoundUserPermission(upToVerify, perms));

      // disable table before modification
      admin.disableTable(tableName);

      User newOwner = User.createUserForTesting(conf, "new_owner", new String[] {});
      htd.setOwner(newOwner);
      admin.modifyTable(tableName, htd);

      acl = systemUserConnection.getTable(AccessControlLists.ACL_TABLE_NAME);
      try {
        BlockingRpcChannel service = acl.coprocessorService(tableName.getName());
        AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
        perms = ProtobufUtil.getUserPermissions(protocol, tableName);
      } finally {
        acl.close();
      }

      UserPermission newOwnerperm =
          new UserPermission(Bytes.toBytes(newOwner.getName()), tableName, null, Action.values());
      assertTrue("New owner should have all permissions on table",
        hasFoundUserPermission(newOwnerperm, perms));
    } finally {
      // delete table
      deleteTable(TEST_UTIL, tableName);
    }
  }

  @Test
  public void testGlobalPermissionList() throws Exception {
    List<UserPermission> perms;
    Table acl = systemUserConnection.getTable(AccessControlLists.ACL_TABLE_NAME);
    try {
      BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
      AccessControlService.BlockingInterface protocol =
        AccessControlService.newBlockingStub(service);
      perms = ProtobufUtil.getUserPermissions(protocol);
    } finally {
      acl.close();
    }
    UserPermission adminPerm = new UserPermission(Bytes.toBytes(USER_ADMIN.getShortName()),
      AccessControlLists.ACL_TABLE_NAME, null, null, Bytes.toBytes("ACRW"));
    assertTrue("Only global users and user admin has permission on table _acl_ per setup",
      perms.size() == 5 && hasFoundUserPermission(adminPerm, perms));
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
          checkTablePerms(TEST_UTIL, TEST_TABLE, new Permission[] {
              new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_Q1, Permission.Action.READ),
              new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_Q2, Permission.Action.READ), });
          return null;
        }
      };

      AccessTestAction globalAndTableRead = new AccessTestAction() {
        @Override
        public Void run() throws Exception {
          checkTablePerms(TEST_UTIL, TEST_TABLE, new Permission[] {
              new Permission(Permission.Action.READ),
              new TablePermission(TEST_TABLE, null, (byte[]) null, Permission.Action.READ), });
          return null;
        }
      };

      AccessTestAction noCheck = new AccessTestAction() {
        @Override
        public Void run() throws Exception {
          checkTablePerms(TEST_UTIL, TEST_TABLE, new Permission[0]);
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
      Table acl = systemUserConnection.getTable(AccessControlLists.ACL_TABLE_NAME);
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
        ACCESS_CONTROLLER.preStopRegionServer(ObserverContext.createAndPrepare(RSCP_ENV, null));
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
        ACCESS_CONTROLLER.preRollWALWriterRequest(ObserverContext.createAndPrepare(RSCP_ENV, null));
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
        ACCESS_CONTROLLER.preOpen(ObserverContext.createAndPrepare(RCP_ENV, null));
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
        ACCESS_CONTROLLER.preClose(ObserverContext.createAndPrepare(RCP_ENV, null), false);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER, USER_GROUP_CREATE,
      USER_GROUP_READ, USER_GROUP_WRITE);
  }

  @Test
  public void testSnapshot() throws Exception {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    final HTableDescriptor htd = admin.getTableDescriptor(TEST_TABLE);
    SnapshotDescription.Builder builder = SnapshotDescription.newBuilder();
    builder.setName(TEST_TABLE.getNameAsString() + "-snapshot");
    builder.setTable(TEST_TABLE.getNameAsString());
    final SnapshotDescription snapshot = builder.build();
    AccessTestAction snapshotAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          snapshot, htd);
        return null;
      }
    };

    AccessTestAction deleteAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          snapshot);
        return null;
      }
    };

    AccessTestAction restoreAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preRestoreSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          snapshot, htd);
        return null;
      }
    };

    AccessTestAction cloneAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCloneSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          null, null);
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
    Admin admin = TEST_UTIL.getHBaseAdmin();
    final HTableDescriptor htd = admin.getTableDescriptor(TEST_TABLE);
    SnapshotDescription.Builder builder = SnapshotDescription.newBuilder();
    builder.setName(TEST_TABLE.getNameAsString() + "-snapshot");
    builder.setTable(TEST_TABLE.getNameAsString());
    builder.setOwner(USER_OWNER.getName());
    final SnapshotDescription snapshot = builder.build();
    AccessTestAction snapshotAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
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
        ACCESS_CONTROLLER.preDeleteSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
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
        ACCESS_CONTROLLER.preRestoreSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
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
        ACCESS_CONTROLLER.preCloneSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          null, null);
        return null;
      }
    };
    // Clone by snapshot owner is not allowed , because clone operation creates a new table,
    // which needs global admin permission.
    verifyAllowed(cloneAction, SUPERUSER, USER_ADMIN, USER_GROUP_ADMIN);
    verifyDenied(cloneAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER,
      USER_GROUP_READ, USER_GROUP_WRITE, USER_GROUP_CREATE);
  }

  @Test
  public void testGlobalAuthorizationForNewRegisteredRS() throws Exception {
    LOG.debug("Test for global authorization for a new registered RegionServer.");
    MiniHBaseCluster hbaseCluster = TEST_UTIL.getHBaseCluster();

    final Admin admin = TEST_UTIL.getHBaseAdmin();
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
    try (HTable table = (HTable) systemUserConnection.getTable(TEST_TABLE2)) {
      AccessTestAction moveAction = new AccessTestAction() {
        @Override
        public Object run() throws Exception {
          admin.move(hri.getEncodedNameAsBytes(),
            Bytes.toBytes(newRs.getServerName().getServerName()));
          return null;
        }
      };
      SUPERUSER.runAs(moveAction);

      final int RETRIES_LIMIT = 10;
      int retries = 0;
      while (newRs.getOnlineRegions(TEST_TABLE2).size() < 1 && retries < RETRIES_LIMIT) {
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
          put.add(TEST_FAMILY, Bytes.toBytes("qual"), Bytes.toBytes("value"));
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
    grantOnTable(TEST_UTIL, TABLE_ADMIN.getShortName(),
      TEST_TABLE, null, null,
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
    final TableName tname = TableName.valueOf("testTableDeletion");
    createTestTable(tname);

    // Grant TABLE ADMIN privs
    grantOnTable(TEST_UTIL, TABLE_ADMIN.getShortName(), tname, null, null,
      Permission.Action.ADMIN);

    AccessTestAction deleteTableAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Connection unmanagedConnection =
            ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Admin admin = unmanagedConnection.getAdmin();
        try {
          deleteTable(TEST_UTIL, admin, tname);
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
    HTableDescriptor htd = new HTableDescriptor(tname);
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    hcd.setMaxVersions(100);
    htd.addFamily(hcd);
    htd.setOwner(USER_OWNER);
    TEST_UTIL.createTable(htd, new byte[][] { Bytes.toBytes("s") });
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
            Table t = conn.getTable(TEST_TABLE)) {
          return t.get(new Get(TEST_ROW));
        }
      }
    };

    verifyDenied(getAction, testGrantRevoke);

    // Grant table READ permissions to testGrantRevoke.
    String userName = testGrantRevoke.getShortName();
    try {
      grantOnTableUsingAccessControlClient(TEST_UTIL, systemUserConnection,
        userName, TEST_TABLE, null, null, Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    try {
      // Now testGrantRevoke should be able to read also
      verifyAllowed(getAction, testGrantRevoke);

      // Revoke table READ permission to testGrantRevoke.
      try {
        revokeFromTableUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
          TEST_TABLE, null, null, Permission.Action.READ);
      } catch (Throwable e) {
        LOG.error("error during call of AccessControlClient.revoke ", e);
      }

      // Now testGrantRevoke shouldn't be able read
      verifyDenied(getAction, testGrantRevoke);
    } finally {
      revokeGlobal(TEST_UTIL, userName, Permission.Action.READ);
    }
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
      grantGlobalUsingAccessControlClient(TEST_UTIL, systemUserConnection,
          userName, Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    try {
      // Now testGlobalGrantRevoke should be able to read also
      verifyAllowed(getAction, testGlobalGrantRevoke);

      // Revoke table READ permission to testGlobalGrantRevoke.
      try {
        revokeGlobalUsingAccessControlClient(TEST_UTIL, systemUserConnection,
          userName, Permission.Action.READ);
      } catch (Throwable e) {
        LOG.error("error during call of AccessControlClient.revoke ", e);
      }

      // Now testGlobalGrantRevoke shouldn't be able read
      verifyDenied(getAction, testGlobalGrantRevoke);
    } finally {
      revokeGlobal(TEST_UTIL, userName, Permission.Action.READ);
    }
  }

  @Test
  public void testAccessControlClientGrantRevokeOnNamespace() throws Exception {
    // Create user for testing, who has no READ privileges by default.
    User testNS = User.createUserForTesting(conf, "testNS", new String[0]);
    AccessTestAction getAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        try(Connection conn = ConnectionFactory.createConnection(conf);
            Table t = conn.getTable(TEST_TABLE)) {
          return t.get(new Get(TEST_ROW));
        }
      }
    };

    verifyDenied(getAction, testNS);

    String userName = testNS.getShortName();
    String namespace = TEST_TABLE.getNamespaceAsString();
    // Grant namespace READ to testNS, this should supersede any table permissions
    try {
      grantOnNamespaceUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
        namespace, Permission.Action.READ);
    } catch (Throwable e) {
      LOG.error("error during call of AccessControlClient.grant. ", e);
    }
    try {
      // Now testNS should be able to read also
      verifyAllowed(getAction, testNS);

      // Revoke namespace READ to testNS, this should supersede any table permissions
      try {
        revokeFromNamespaceUsingAccessControlClient(TEST_UTIL, systemUserConnection, userName,
          namespace, Permission.Action.READ);
      } catch (Throwable e) {
        LOG.error("error during call of AccessControlClient.revoke ", e);
      }

      // Now testNS shouldn't be able read
      verifyDenied(getAction, testNS);
    } finally {
      revokeFromNamespace(TEST_UTIL, userName, namespace, Permission.Action.READ);
    }
  }


  public static class PingCoprocessor extends PingService implements Coprocessor,
      CoprocessorService {

    @Override
    public void start(CoprocessorEnvironment env) throws IOException { }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException { }

    @Override
    public Service getService() {
      return this;
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
      for (Region region: rs.getOnlineRegions(TEST_TABLE)) {
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
  public void testReservedCellTags() throws Exception {
    AccessTestAction putWithReservedTag = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        Table t = new HTable(conf, TEST_TABLE);
        try {
          KeyValue kv = new KeyValue(TEST_ROW, TEST_FAMILY, TEST_QUALIFIER,
            HConstants.LATEST_TIMESTAMP, HConstants.EMPTY_BYTE_ARRAY,
            new Tag[] { new Tag(AccessControlLists.ACL_TAG_TYPE,
              ProtobufUtil.toUsersAndPermissions(USER_OWNER.getShortName(),
                new Permission(Permission.Action.READ)).toByteArray()) });
          t.put(new Put(TEST_ROW).add(kv));
        } finally {
          t.close();
        }
        return null;
      }
    };

    // Current user is superuser
    verifyAllowed(putWithReservedTag, User.getCurrent());
    // No other user should be allowed
    verifyDenied(putWithReservedTag, USER_OWNER, USER_ADMIN, USER_CREATE, USER_RW, USER_RO);
  }

  @Test
  public void testGetNamespacePermission() throws Exception {
    String namespace = "testGetNamespacePermission";
    NamespaceDescriptor desc = NamespaceDescriptor.create(namespace).build();
    createNamespace(TEST_UTIL, desc);
    grantOnNamespace(TEST_UTIL, USER_NONE.getShortName(), namespace, Permission.Action.READ);
    try {
      List<UserPermission> namespacePermissions = AccessControlClient.getUserPermissions(
          systemUserConnection, AccessControlLists.toNamespaceEntry(namespace));
      assertTrue(namespacePermissions != null);
      assertTrue(namespacePermissions.size() == 1);
    } catch (Throwable thw) {
      throw new HBaseException(thw);
    }
    deleteNamespace(TEST_UTIL, namespace);
  }

  @Test
  public void testTruncatePerms() throws Throwable {
    List<UserPermission> existingPerms =
        AccessControlClient.getUserPermissions(systemUserConnection,
            TEST_TABLE.getNameAsString());
    assertTrue(existingPerms != null);
    assertTrue(existingPerms.size() > 1);
    try (Admin admin = systemUserConnection.getAdmin()) {
      admin.disableTable(TEST_TABLE);
      admin.truncateTable(TEST_TABLE, true);
    }
    List<UserPermission> perms = AccessControlClient.getUserPermissions(systemUserConnection,
        TEST_TABLE.getNameAsString());
    assertTrue(perms != null);
    assertEquals(existingPerms.size(), perms.size());
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
    TableName tname = TableName.valueOf("testAccessControlClientUserPerms");
    createTestTable(tname);
    try {
      final String regex = tname.getNameWithNamespaceInclAsString();
      User testUserPerms = User.createUserForTesting(conf, "testUserPerms", new String[0]);
      assertEquals(0, testUserPerms.runAs(getPrivilegedAction(regex)).size());
      // Grant TABLE ADMIN privs to testUserPerms
      grantOnTable(TEST_UTIL, testUserPerms.getShortName(), tname, null, null, Action.ADMIN);
      List<UserPermission> perms = testUserPerms.runAs(getPrivilegedAction(regex));
      assertNotNull(perms);
      // Superuser, testUserPerms
      assertEquals(2, perms.size());
    } finally {
      deleteTable(TEST_UTIL, tname);
    }
  }

  @Test
  public void testAccessControllerUserPermsRegexHandling() throws Exception {
    User testRegexHandler = User.createUserForTesting(conf, "testRegexHandling", new String[0]);

    final String REGEX_ALL_TABLES = ".*";
    final String tableName = "testRegex";
    final TableName table1 = TableName.valueOf(tableName);
    final byte[] family = Bytes.toBytes("f1");

    // create table in default ns
    Admin admin = TEST_UTIL.getHBaseAdmin();
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
    String aclTableName = AccessControlLists.ACL_TABLE_NAME.getNameAsString();
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
      USER_GROUP_CREATE);
    verifyDenied(action, USER_NONE, USER_RO, USER_RW, USER_GROUP_READ, USER_GROUP_WRITE,
      USER_GROUP_ADMIN);
  }

  @Test
  public void testPrepareAndCleanBulkLoad() throws Exception {
    AccessTestAction prepareBulkLoadAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.prePrepareBulkLoad(ObserverContext.createAndPrepare(RCP_ENV, null),
          null);
        return null;
      }
    };
    AccessTestAction cleanupBulkLoadAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCleanupBulkLoad(ObserverContext.createAndPrepare(RCP_ENV, null),
          null);
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
        ACCESS_CONTROLLER.preReplicateLogEntries(ObserverContext.createAndPrepare(RSCP_ENV, null),
          null, null);
        ACCESS_CONTROLLER.postReplicateLogEntries(ObserverContext.createAndPrepare(RSCP_ENV, null),
          null, null);
        return null;
      }
    };

    verifyAllowed(replicateLogEntriesAction, SUPERUSER, USER_ADMIN, USER_GROUP_WRITE);
    verifyDenied(replicateLogEntriesAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER,
      USER_GROUP_READ, USER_GROUP_ADMIN, USER_GROUP_CREATE);
  }

  @Test
  public void testSetQuota() throws Exception {
    AccessTestAction setUserQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetUserQuota(ObserverContext.createAndPrepare(CP_ENV, null),
          null, null);
        return null;
      }
    };

    AccessTestAction setUserTableQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetUserQuota(ObserverContext.createAndPrepare(CP_ENV, null),
          null, TEST_TABLE, null);
        return null;
      }
    };

    AccessTestAction setUserNamespaceQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetUserQuota(ObserverContext.createAndPrepare(CP_ENV, null),
          null, (String)null, null);
        return null;
      }
    };

    AccessTestAction setTableQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetTableQuota(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE, null);
        return null;
      }
    };

    AccessTestAction setNamespaceQuotaAction = new AccessTestAction() {
      @Override
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSetNamespaceQuota(ObserverContext.createAndPrepare(CP_ENV, null),
          null, null);
        return null;
      }
    };

    verifyAllowed(setUserQuotaAction, SUPERUSER, USER_ADMIN);
    verifyDenied(setUserQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);

    verifyAllowed(setUserTableQuotaAction, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(setUserTableQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE);

    verifyAllowed(setUserNamespaceQuotaAction, SUPERUSER, USER_ADMIN);
    verifyDenied(setUserNamespaceQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);

    verifyAllowed(setTableQuotaAction, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(setTableQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE);

    verifyAllowed(setNamespaceQuotaAction, SUPERUSER, USER_ADMIN);
    verifyDenied(setNamespaceQuotaAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }
}
