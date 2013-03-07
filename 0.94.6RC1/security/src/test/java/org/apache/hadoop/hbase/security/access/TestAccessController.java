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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.UnknownRowLockException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Performs authorization checks for common operations, according to different
 * levels of authorized users.
 */
@Category(LargeTests.class)
@SuppressWarnings("rawtypes")
public class TestAccessController {
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  // user with all permissions
  private static User SUPERUSER;
  // user granted with all global permission
  private static User USER_ADMIN;
  // user with rw permissions
  private static User USER_RW;
  // user with rw permissions on table.
  private static User USER_RW_ON_TABLE;
  // user with read-only permissions
  private static User USER_RO;
  // user is table owner. will have all permissions on table
  private static User USER_OWNER;
  // user with create table permissions alone
  private static User USER_CREATE;
  // user with no permissions
  private static User USER_NONE;

  private static byte[] TEST_TABLE = Bytes.toBytes("testtable");
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static MasterCoprocessorEnvironment CP_ENV;
  private static RegionCoprocessorEnvironment RCP_ENV;
  private static RegionServerCoprocessorEnvironment RSCP_ENV;
  private static AccessController ACCESS_CONTROLLER;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    conf.set("hbase.master.hfilecleaner.plugins",
      "org.apache.hadoop.hbase.master.cleaner.HFileLinkCleaner," +
      "org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner");
    conf.set("hbase.master.logcleaner.plugins",
      "org.apache.hadoop.hbase.master.snapshot.SnapshotLogCleaner");
    SecureTestUtil.enableSecurity(conf);

    TEST_UTIL.startMiniCluster();
    MasterCoprocessorHost cpHost = TEST_UTIL.getMiniHBaseCluster().getMaster().getCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = (AccessController) cpHost.findCoprocessor(AccessController.class.getName());
    CP_ENV = cpHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
      Coprocessor.PRIORITY_HIGHEST, 1, conf);
    RegionServerCoprocessorHost rsHost = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getCoprocessorHost();
    RSCP_ENV = rsHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER, 
      Coprocessor.PRIORITY_HIGHEST, 1, conf);

    // Wait for the ACL table to become available
    TEST_UTIL.waitTableAvailable(AccessControlLists.ACL_TABLE_NAME, 5000);

    // create a set of test users
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER_ADMIN = User.createUserForTesting(conf, "admin2", new String[0]);
    USER_RW = User.createUserForTesting(conf, "rwuser", new String[0]);
    USER_RO = User.createUserForTesting(conf, "rouser", new String[0]);
    USER_RW_ON_TABLE = User.createUserForTesting(conf, "rwuser_1", new String[0]);
    USER_OWNER = User.createUserForTesting(conf, "owner", new String[0]);
    USER_CREATE = User.createUserForTesting(conf, "tbl_create", new String[0]);
    USER_NONE = User.createUserForTesting(conf, "nouser", new String[0]);

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
    htd.setOwner(USER_OWNER);
    admin.createTable(htd);

    HRegion region = TEST_UTIL.getHBaseCluster().getRegions(TEST_TABLE).get(0);
    RegionCoprocessorHost rcpHost = region.getCoprocessorHost();
    RCP_ENV = rcpHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
      Coprocessor.PRIORITY_HIGHEST, 1, conf);

    // initilize access control
    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        TEST_TABLE);

     protocol.grant(new UserPermission(Bytes.toBytes(USER_ADMIN.getShortName()),
        Permission.Action.ADMIN, Permission.Action.CREATE, Permission.Action.READ,
        Permission.Action.WRITE));

      protocol.grant(new UserPermission(Bytes.toBytes(USER_RW.getShortName()), TEST_TABLE,
        TEST_FAMILY, Permission.Action.READ, Permission.Action.WRITE));

      protocol.grant(new UserPermission(Bytes.toBytes(USER_RO.getShortName()), TEST_TABLE,
        TEST_FAMILY, Permission.Action.READ));

      protocol.grant(new UserPermission(Bytes.toBytes(USER_CREATE.getShortName()), TEST_TABLE, null,
        Permission.Action.CREATE));
    
      protocol.grant(new UserPermission(Bytes.toBytes(USER_RW_ON_TABLE.getShortName()), TEST_TABLE,
        null, Permission.Action.READ, Permission.Action.WRITE));
    } finally {
      acl.close();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void verifyAllowed(User user, PrivilegedExceptionAction... actions) throws Exception {
    for (PrivilegedExceptionAction action : actions) {
      try {
        user.runAs(action);
      } catch (AccessDeniedException ade) {
        fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
      } catch (UnknownRowLockException exp){
        //expected
      }
    }
  }

  public void verifyAllowed(PrivilegedExceptionAction action, User... users) throws Exception {
    for (User user : users) {
      verifyAllowed(user, action);
    }
  }

  public void verifyDenied(User user, PrivilegedExceptionAction... actions) throws Exception {
    for (PrivilegedExceptionAction action : actions) {
      try {
        user.runAs(action);
        fail("Expected AccessDeniedException for user '" + user.getShortName() + "'");
      } catch (AccessDeniedException ade) {
        // expected result
      } catch (IOException e) {
        boolean isAccessDeniedException = false;
        if(e instanceof RetriesExhaustedWithDetailsException) {
          // in case of batch operations, and put, the client assembles a
          // RetriesExhaustedWithDetailsException instead of throwing an
          // AccessDeniedException
          for(Throwable ex : ((RetriesExhaustedWithDetailsException) e).getCauses()) {
            if (ex instanceof AccessDeniedException) {
              isAccessDeniedException = true;
              break;
            }
          }
        }
        else {
          // For doBulkLoad calls AccessDeniedException
          // is buried in the stack trace
          Throwable ex = e;
          do {
            if (ex instanceof AccessDeniedException) {
              isAccessDeniedException = true;
              break;
            }
          } while((ex = ex.getCause()) != null);
        }
        if (!isAccessDeniedException) {
          fail("Not receiving AccessDeniedException for user '" + user.getShortName() + "'");
        }
      }
    }
  }

  public void verifyDenied(PrivilegedExceptionAction action, User... users) throws Exception {
    for (User user : users) {
      verifyDenied(user, action);
    }
  }

  @Test
  public void testTableCreate() throws Exception {
    PrivilegedExceptionAction createTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor("testnewtable");
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        ACCESS_CONTROLLER.preCreateTable(ObserverContext.createAndPrepare(CP_ENV, null), htd, null);
        return null;
      }
    };

    // verify that superuser can create tables
    verifyAllowed(createTable, SUPERUSER, USER_ADMIN);

    // all others should be denied
    verifyDenied(createTable, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testTableModify() throws Exception {
    PrivilegedExceptionAction modifyTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        htd.addFamily(new HColumnDescriptor("fam_" + User.getCurrent().getShortName()));
        ACCESS_CONTROLLER.preModifyTable(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE, htd);
        return null;
      }
    };

    verifyAllowed(modifyTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER);
    verifyDenied(modifyTable, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testTableDelete() throws Exception {
    PrivilegedExceptionAction deleteTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER
            .preDeleteTable(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE);
        return null;
      }
    };

    verifyAllowed(deleteTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER);
    verifyDenied(deleteTable, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testAddColumn() throws Exception {
    final HColumnDescriptor hcd = new HColumnDescriptor("fam_new");
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAddColumn(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE,
          hcd);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER);
    verifyDenied(action, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testModifyColumn() throws Exception {
    final HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    hcd.setMaxVersions(10);
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preModifyColumn(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE, hcd);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER);
    verifyDenied(action, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testDeleteColumn() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteColumn(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE, TEST_FAMILY);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER);
    verifyDenied(action, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testTableDisable() throws Exception {
    PrivilegedExceptionAction disableTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDisableTable(ObserverContext.createAndPrepare(CP_ENV, null),
          TEST_TABLE);
        return null;
      }
    };

    PrivilegedExceptionAction disableAclTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDisableTable(ObserverContext.createAndPrepare(CP_ENV, null),
            AccessControlLists.ACL_TABLE_NAME);
        return null;
      }
    };

    verifyAllowed(disableTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER);
    verifyDenied(disableTable, USER_RW, USER_RO, USER_NONE);
    
    // No user should be allowed to disable _acl_ table
    verifyDenied(disableAclTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER, USER_RW, USER_RO);
  }

  @Test
  public void testTableEnable() throws Exception {
    PrivilegedExceptionAction enableTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER
            .preEnableTable(ObserverContext.createAndPrepare(CP_ENV, null), TEST_TABLE);
        return null;
      }
    };

    verifyAllowed(enableTable, SUPERUSER, USER_ADMIN, USER_CREATE, USER_OWNER);
    verifyDenied(enableTable, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testMove() throws Exception {
    Map<HRegionInfo, HServerAddress> regions;
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TEST_TABLE);
    try {
      regions = table.getRegionsInfo();
    } finally {
      table.close();
    }
    final Map.Entry<HRegionInfo, HServerAddress> firstRegion = regions.entrySet().iterator().next();
    final ServerName server = TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preMove(ObserverContext.createAndPrepare(CP_ENV, null),
          firstRegion.getKey(), server, server);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testAssign() throws Exception {
    Map<HRegionInfo, HServerAddress> regions;
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TEST_TABLE);
    try {
      regions = table.getRegionsInfo();
    } finally {
      table.close();
    }
    final Map.Entry<HRegionInfo, HServerAddress> firstRegion = regions.entrySet().iterator().next();

    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAssign(ObserverContext.createAndPrepare(CP_ENV, null),
          firstRegion.getKey());
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testUnassign() throws Exception {
    Map<HRegionInfo, HServerAddress> regions;
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TEST_TABLE);
    try {
      regions = table.getRegionsInfo();
    } finally {
      table.close();
    }
    final Map.Entry<HRegionInfo, HServerAddress> firstRegion = regions.entrySet().iterator().next();

    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preUnassign(ObserverContext.createAndPrepare(CP_ENV, null),
          firstRegion.getKey(), false);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testBalance() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalance(ObserverContext.createAndPrepare(CP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testBalanceSwitch() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalanceSwitch(ObserverContext.createAndPrepare(CP_ENV, null), true);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testShutdown() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preShutdown(ObserverContext.createAndPrepare(CP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testStopMaster() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preStopMaster(ObserverContext.createAndPrepare(CP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE);
  }

  private void verifyWrite(PrivilegedExceptionAction action) throws Exception {
    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_RW);
    verifyDenied(action, USER_NONE, USER_CREATE, USER_RO);
  }

  @Test
  public void testSplit() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSplit(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testFlush() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preFlush(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testCompact() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCompact(ObserverContext.createAndPrepare(RCP_ENV, null), null, null);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testPreCompactSelection() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCompactSelection(ObserverContext.createAndPrepare(RCP_ENV, null), null, null);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  private void verifyRead(PrivilegedExceptionAction action) throws Exception {
    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_RW, USER_RO);
    verifyDenied(action, USER_NONE, USER_CREATE);
  }

  private void verifyReadWrite(PrivilegedExceptionAction action) throws Exception {
    verifyAllowed(action, SUPERUSER, USER_ADMIN, USER_OWNER, USER_RW);
    verifyDenied(action, USER_NONE, USER_CREATE, USER_RO);
  }

  @Test
  public void testRead() throws Exception {
    // get action
    PrivilegedExceptionAction getAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Get g = new Get(Bytes.toBytes("random_row"));
        g.addFamily(TEST_FAMILY);
        HTable t = new HTable(conf, TEST_TABLE);
        try {
          t.get(g);
        } finally {
          t.close();
        }
        return null;
      }
    };
    verifyRead(getAction);

    // action for scanning
    PrivilegedExceptionAction scanAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Scan s = new Scan();
        s.addFamily(TEST_FAMILY);

        HTable table = new HTable(conf, TEST_TABLE);
        try {
          ResultScanner scanner = table.getScanner(s);
          try {
            for (Result r = scanner.next(); r != null; r = scanner.next()) {
              // do nothing
            }
          } catch (IOException e) {
          } finally {
            scanner.close();
          }
        } finally {
          table.close();
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
    PrivilegedExceptionAction putAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("random_row"));
        p.add(TEST_FAMILY, Bytes.toBytes("Qualifier"), Bytes.toBytes(1));
        HTable t = new HTable(conf, TEST_TABLE);
        try {
          t.put(p);
        } finally {
          t.close();
        }
        return null;
      }
    };
    verifyWrite(putAction);

    // delete action
    PrivilegedExceptionAction deleteAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteFamily(TEST_FAMILY);
        HTable t = new HTable(conf, TEST_TABLE);
        try {
          t.delete(d);
        } finally {
          t.close();
        }
        return null;
      }
    };
    verifyWrite(deleteAction);

    // increment action
    PrivilegedExceptionAction incrementAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Increment inc = new Increment(Bytes.toBytes("random_row"));
        inc.addColumn(TEST_FAMILY, Bytes.toBytes("Qualifier"), 1);
        HTable t = new HTable(conf, TEST_TABLE);
        try {
          t.increment(inc);
        } finally {
          t.close();
        }
        return null;
      }
    };
    verifyWrite(incrementAction);
  }

  @Test
  public void testReadWrite() throws Exception {
    // action for checkAndDelete
    PrivilegedExceptionAction checkAndDeleteAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteFamily(TEST_FAMILY);
        HTable t = new HTable(conf, TEST_TABLE);
        try {
          t.checkAndDelete(Bytes.toBytes("random_row"), TEST_FAMILY, Bytes.toBytes("q"),
            Bytes.toBytes("test_value"), d);
        } finally {
          t.close();
        }
        return null;
      }
    };
    verifyReadWrite(checkAndDeleteAction);

    // action for checkAndPut()
    PrivilegedExceptionAction checkAndPut = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("random_row"));
        p.add(TEST_FAMILY, Bytes.toBytes("Qualifier"), Bytes.toBytes(1));
        HTable t = new HTable(conf, TEST_TABLE);
        try {
          t.checkAndPut(Bytes.toBytes("random_row"), TEST_FAMILY, Bytes.toBytes("q"),
           Bytes.toBytes("test_value"), p);
        } finally {
          t.close();
        }
        return null;
      }
    };
    verifyReadWrite(checkAndPut);
  }

  @Test
  public void testBulkLoad() throws Exception {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    final Path dir = TEST_UTIL.getDataTestDir("testBulkLoad");
    fs.mkdirs(dir);
    //need to make it globally writable
    //so users creating HFiles have write permissions
    fs.setPermission(dir, FsPermission.valueOf("-rwxrwxrwx"));

    PrivilegedExceptionAction bulkLoadAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        int numRows = 3;

        //Making the assumption that the test table won't split between the range
        byte[][][] hfileRanges = {{{(byte)0}, {(byte)9}}};

        Path bulkLoadBasePath = new Path(dir, new Path(User.getCurrent().getName()));
        new BulkLoadHelper(bulkLoadBasePath)
            .bulkLoadHFile(TEST_TABLE, TEST_FAMILY, Bytes.toBytes("q"), hfileRanges, numRows);

        return null;
      }
    };
    verifyWrite(bulkLoadAction);

    // Reinit after the bulk upload
    TEST_UTIL.getHBaseAdmin().disableTable(TEST_TABLE);
    TEST_UTIL.getHBaseAdmin().enableTable(TEST_TABLE);
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
        writer = HFile.getWriterFactory(conf, new CacheConfig(conf))
            .withPath(fs, path)
            .withComparator(KeyValue.KEY_COMPARATOR)
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
        byte[] tableName,
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

      HTable table = new HTable(conf, tableName);
      try {
        TEST_UTIL.waitTableAvailable(tableName, 30000);
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(loadPath, table);
      } finally {
        table.close();
      }
    }

    public void setPermission(Path dir, FsPermission perm) throws IOException {
      if(!fs.getFileStatus(dir).isDir()) {
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

    PrivilegedExceptionAction appendAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        byte[] row = Bytes.toBytes("random_row");
        byte[] qualifier = Bytes.toBytes("q");
        Put put = new Put(row);
        put.add(TEST_FAMILY, qualifier, Bytes.toBytes(1));
        Append append = new Append(row);
        append.add(TEST_FAMILY, qualifier, Bytes.toBytes(2));
        HTable t = new HTable(conf, TEST_TABLE);
        try {
          t.put(put);
          t.append(append);
        } finally {
          t.close();
        }
        return null;
      }
    };

    verifyAllowed(appendAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_RW);
    verifyDenied(appendAction, USER_CREATE, USER_RO, USER_NONE);
  }

  @Test
  public void testGrantRevoke() throws Exception {

    PrivilegedExceptionAction grantAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
        try {
          AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
            TEST_TABLE);
          protocol.grant(new UserPermission(Bytes.toBytes(USER_RO.getShortName()), TEST_TABLE,
            TEST_FAMILY, (byte[]) null, Action.READ));
        } finally {
          acl.close();
        }
        return null;
      }
    };

    PrivilegedExceptionAction revokeAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
        try {
          AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
            TEST_TABLE);
          protocol.revoke(new UserPermission(Bytes.toBytes(USER_RO.getShortName()), TEST_TABLE,
            TEST_FAMILY, (byte[]) null, Action.READ));
        } finally {
          acl.close();
        }
        return null;
      }
    };

    PrivilegedExceptionAction getPermissionsAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
        try {
          AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
            TEST_TABLE);
          protocol.getUserPermissions(TEST_TABLE);
        } finally {
          acl.close();
        }
        return null;
      }
    };

    verifyAllowed(grantAction, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(grantAction, USER_CREATE, USER_RW, USER_RO, USER_NONE);

    verifyAllowed(revokeAction, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(revokeAction, USER_CREATE, USER_RW, USER_RO, USER_NONE);

    verifyAllowed(getPermissionsAction, SUPERUSER, USER_ADMIN, USER_OWNER);
    verifyDenied(getPermissionsAction, USER_CREATE, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testPostGrantRevoke() throws Exception {
    final byte[] tableName = Bytes.toBytes("TempTable");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");

    // create table
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    admin.createTable(htd);

    // create temp users
    User tblUser = User
        .createUserForTesting(TEST_UTIL.getConfiguration(), "tbluser", new String[0]);
    User gblUser = User
        .createUserForTesting(TEST_UTIL.getConfiguration(), "gbluser", new String[0]);

    // prepare actions:
    PrivilegedExceptionAction putActionAll = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("a"));
        p.add(family1, qualifier, Bytes.toBytes("v1"));
        p.add(family2, qualifier, Bytes.toBytes("v2"));
        HTable t = new HTable(conf, tableName);
        try {
          t.put(p);
        } finally {
          t.close();
        }
        return null;
      }
    };
    PrivilegedExceptionAction putAction1 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("a"));
        p.add(family1, qualifier, Bytes.toBytes("v1"));
        HTable t = new HTable(conf, tableName);
        try {
          t.put(p);
        } finally {
          t.close();
        }
        return null;
      }
    };
    PrivilegedExceptionAction putAction2 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("a"));
        p.add(family2, qualifier, Bytes.toBytes("v2"));
        HTable t = new HTable(conf, tableName);
        try {
          t.put(p);
        } finally {
          t.close();
        }
        return null;
      }
    };
    PrivilegedExceptionAction getActionAll = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Get g = new Get(Bytes.toBytes("random_row"));
        g.addFamily(family1);
        g.addFamily(family2);
        HTable t = new HTable(conf, tableName);
        try {
          t.get(g);
        } finally {
          t.close();
        }
        return null;
      }
    };
    PrivilegedExceptionAction getAction1 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Get g = new Get(Bytes.toBytes("random_row"));
        g.addFamily(family1);
        HTable t = new HTable(conf, tableName);
        try {
          t.get(g);
        } finally {
          t.close();
        }
        return null;
      }
    };
    PrivilegedExceptionAction getAction2 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Get g = new Get(Bytes.toBytes("random_row"));
        g.addFamily(family2);
        HTable t = new HTable(conf, tableName);
        try {
          t.get(g);
        } finally {
          t.close();
        }
        return null;
      }
    };
    PrivilegedExceptionAction deleteActionAll = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteFamily(family1);
        d.deleteFamily(family2);
        HTable t = new HTable(conf, tableName);
        try {
          t.delete(d);
        } finally {
          t.close();
        }
        return null;
      }
    };
    PrivilegedExceptionAction deleteAction1 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteFamily(family1);
        HTable t = new HTable(conf, tableName);
        try {
          t.delete(d);
        } finally {
          t.close();
        }
        return null;
      }
    };
    PrivilegedExceptionAction deleteAction2 = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteFamily(family2);
        HTable t = new HTable(conf, tableName);
        try {
          t.delete(d);
        } finally {
          t.close();
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
    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.grant(new UserPermission(Bytes.toBytes(tblUser.getShortName()), tableName, null,
        Permission.Action.READ));
      protocol.grant(new UserPermission(Bytes.toBytes(gblUser.getShortName()),
        Permission.Action.READ));
    } finally {
      acl.close();
    }

    Thread.sleep(100);

    // check
    verifyAllowed(tblUser, getActionAll, getAction1, getAction2);
    verifyDenied(tblUser, putActionAll, putAction1, putAction2);
    verifyDenied(tblUser, deleteActionAll, deleteAction1, deleteAction2);

    verifyAllowed(gblUser, getActionAll, getAction1, getAction2);
    verifyDenied(gblUser, putActionAll, putAction1, putAction2);
    verifyDenied(gblUser, deleteActionAll, deleteAction1, deleteAction2);

    // grant table write permission
    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.grant(new UserPermission(Bytes.toBytes(tblUser.getShortName()), tableName, null,
        Permission.Action.WRITE));
      protocol.grant(new UserPermission(Bytes.toBytes(gblUser.getShortName()),
        Permission.Action.WRITE));
    } finally {
      acl.close();
    }

    Thread.sleep(100);

    verifyDenied(tblUser, getActionAll, getAction1, getAction2);
    verifyAllowed(tblUser, putActionAll, putAction1, putAction2);
    verifyAllowed(tblUser, deleteActionAll, deleteAction1, deleteAction2);

    verifyDenied(gblUser, getActionAll, getAction1, getAction2);
    verifyAllowed(gblUser, putActionAll, putAction1, putAction2);
    verifyAllowed(gblUser, deleteActionAll, deleteAction1, deleteAction2);

    // revoke table permission
    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.grant(new UserPermission(Bytes.toBytes(tblUser.getShortName()), tableName, null,
        Permission.Action.READ, Permission.Action.WRITE));
      protocol.revoke(new UserPermission(Bytes.toBytes(tblUser.getShortName()), tableName, null));
      protocol.revoke(new UserPermission(Bytes.toBytes(gblUser.getShortName())));
    } finally {
      acl.close();
    }

    Thread.sleep(100);

    verifyDenied(tblUser, getActionAll, getAction1, getAction2);
    verifyDenied(tblUser, putActionAll, putAction1, putAction2);
    verifyDenied(tblUser, deleteActionAll, deleteAction1, deleteAction2);

    verifyDenied(gblUser, getActionAll, getAction1, getAction2);
    verifyDenied(gblUser, putActionAll, putAction1, putAction2);
    verifyDenied(gblUser, deleteActionAll, deleteAction1, deleteAction2);

    // grant column family read permission
    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.grant(new UserPermission(Bytes.toBytes(tblUser.getShortName()), tableName, family1,
        Permission.Action.READ));
      protocol.grant(new UserPermission(Bytes.toBytes(gblUser.getShortName()),
        Permission.Action.READ));
    } finally {
      acl.close();
    }

    Thread.sleep(100);

    // Access should be denied for family2
    verifyAllowed(tblUser, getActionAll, getAction1);
    verifyDenied(tblUser, getAction2);
    verifyDenied(tblUser, putActionAll, putAction1, putAction2);
    verifyDenied(tblUser, deleteActionAll, deleteAction1, deleteAction2);

    verifyAllowed(gblUser, getActionAll, getAction1, getAction2);
    verifyDenied(gblUser, putActionAll, putAction1, putAction2);
    verifyDenied(gblUser, deleteActionAll, deleteAction1, deleteAction2);

    // grant column family write permission
    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.grant(new UserPermission(Bytes.toBytes(tblUser.getShortName()), tableName, family2,
        Permission.Action.WRITE));
      protocol.grant(new UserPermission(Bytes.toBytes(gblUser.getShortName()),
        Permission.Action.WRITE));
    } finally {
      acl.close();
    }

    Thread.sleep(100);

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
    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.revoke(new UserPermission(Bytes.toBytes(tblUser.getShortName()), tableName, family2));
      protocol.revoke(new UserPermission(Bytes.toBytes(gblUser.getShortName())));
    } finally {
      acl.close();
    }

    Thread.sleep(100);

    // Revoke on family2 should not have impact on family1 permissions
    verifyAllowed(tblUser, getActionAll, getAction1);
    verifyDenied(tblUser, getAction2);
    verifyDenied(tblUser, putActionAll, putAction1, putAction2);
    verifyDenied(tblUser, deleteActionAll, deleteAction1, deleteAction2);

    // Should not have access as global permissions are completely revoked
    verifyDenied(gblUser, getActionAll, getAction1, getAction2);
    verifyDenied(gblUser, putActionAll, putAction1, putAction2);
    verifyDenied(gblUser, deleteActionAll, deleteAction1, deleteAction2);

    // delete table
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  private boolean hasFoundUserPermission(UserPermission userPermission, List<UserPermission> perms) {
    return perms.contains(userPermission);
  }

  @Test
  public void testPostGrantRevokeAtQualifierLevel() throws Exception {
    final byte[] tableName = Bytes.toBytes("testGrantRevokeAtQualifierLevel");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");

    // create table
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    admin.createTable(htd);

    // create temp users
    User user = User.createUserForTesting(TEST_UTIL.getConfiguration(), "user", new String[0]);

    PrivilegedExceptionAction getQualifierAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Get g = new Get(Bytes.toBytes("random_row"));
        g.addColumn(family1, qualifier);
        HTable t = new HTable(conf, tableName);
        try {
          t.get(g);
        } finally {
          t.close();
        }
        return null;
      }
    };
    PrivilegedExceptionAction putQualifierAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Put p = new Put(Bytes.toBytes("random_row"));
        p.add(family1, qualifier, Bytes.toBytes("v1"));
        HTable t = new HTable(conf, tableName);
        try {
          t.put(p);
        } finally {
          t.close();
        }
        return null;
      }
    };
    PrivilegedExceptionAction deleteQualifierAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        Delete d = new Delete(Bytes.toBytes("random_row"));
        d.deleteColumn(family1, qualifier);
        // d.deleteFamily(family1);
        HTable t = new HTable(conf, tableName);
        try {
          t.delete(d);
        } finally {
          t.close();
        }
        return null;
      }
    };

    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.revoke(new UserPermission(Bytes.toBytes(user.getShortName()), tableName, family1));
    } finally {
      acl.close();
    }

    verifyDenied(user, getQualifierAction);
    verifyDenied(user, putQualifierAction);
    verifyDenied(user, deleteQualifierAction);

    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.grant(new UserPermission(Bytes.toBytes(user.getShortName()), tableName, family1,
        qualifier, Permission.Action.READ));
    } finally {
      acl.close();
    }

    Thread.sleep(100);

    verifyAllowed(user, getQualifierAction);
    verifyDenied(user, putQualifierAction);
    verifyDenied(user, deleteQualifierAction);

    // only grant write permission
    // TODO: comment this portion after HBASE-3583
    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.grant(new UserPermission(Bytes.toBytes(user.getShortName()), tableName, family1,
        qualifier, Permission.Action.WRITE));
    } finally {
      acl.close();
    }

    Thread.sleep(100);

    verifyDenied(user, getQualifierAction);
    verifyAllowed(user, putQualifierAction);
    verifyAllowed(user, deleteQualifierAction);

    // grant both read and write permission.
    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.grant(new UserPermission(Bytes.toBytes(user.getShortName()), tableName, family1,
        qualifier, Permission.Action.READ, Permission.Action.WRITE));
    } finally {
      acl.close();
    }

    Thread.sleep(100);

    verifyAllowed(user, getQualifierAction);
    verifyAllowed(user, putQualifierAction);
    verifyAllowed(user, deleteQualifierAction);

    // revoke family level permission won't impact column level.
    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.revoke(new UserPermission(Bytes.toBytes(user.getShortName()), tableName, family1,
        qualifier));
    } finally {
      acl.close();
    }

    Thread.sleep(100);

    verifyDenied(user, getQualifierAction);
    verifyDenied(user, putQualifierAction);
    verifyDenied(user, deleteQualifierAction);

    // delete table
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test
  public void testPermissionList() throws Exception {
    final byte[] tableName = Bytes.toBytes("testPermissionList");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier = Bytes.toBytes("q");
    final byte[] user = Bytes.toBytes("user");

    // create table
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family1));
    htd.addFamily(new HColumnDescriptor(family2));
    htd.setOwner(USER_OWNER);
    admin.createTable(htd);

    List<UserPermission> perms;
    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      perms = protocol.getUserPermissions(tableName);
    } finally {
      acl.close();
    }

    UserPermission ownerperm = new UserPermission(Bytes.toBytes(USER_OWNER.getName()), tableName,
        null, Action.values());
    assertTrue("Owner should have all permissions on table",
      hasFoundUserPermission(ownerperm, perms));

    UserPermission up = new UserPermission(user, tableName, family1, qualifier,
        Permission.Action.READ);
    assertFalse("User should not be granted permission: " + up.toString(),
      hasFoundUserPermission(up, perms));

    // grant read permission
    UserPermission upToSet = new UserPermission(user, tableName, family1, qualifier,
        Permission.Action.READ);

    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.grant(upToSet);
      perms = protocol.getUserPermissions(tableName);
    } finally {
      acl.close();
    }

    UserPermission upToVerify = new UserPermission(user, tableName, family1, qualifier,
        Permission.Action.READ);
    assertTrue("User should be granted permission: " + upToVerify.toString(),
      hasFoundUserPermission(upToVerify, perms));

    upToVerify = new UserPermission(user, tableName, family1, qualifier, Permission.Action.WRITE);
    assertFalse("User should not be granted permission: " + upToVerify.toString(),
      hasFoundUserPermission(upToVerify, perms));

    // grant read+write
    upToSet = new UserPermission(user, tableName, family1, qualifier, Permission.Action.WRITE,
        Permission.Action.READ);
    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.grant(upToSet);
      perms = protocol.getUserPermissions(tableName);
    } finally {
      acl.close();
    }

    upToVerify = new UserPermission(user, tableName, family1, qualifier, Permission.Action.WRITE,
        Permission.Action.READ);
    assertTrue("User should be granted permission: " + upToVerify.toString(),
      hasFoundUserPermission(upToVerify, perms));

    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      protocol.revoke(upToSet);
      perms = protocol.getUserPermissions(tableName);
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

    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        tableName);
      perms = protocol.getUserPermissions(tableName);
    } finally {
      acl.close();
    }

    UserPermission newOwnerperm = new UserPermission(Bytes.toBytes(newOwner.getName()), tableName,
        null, Action.values());
    assertTrue("New owner should have all permissions on table",
      hasFoundUserPermission(newOwnerperm, perms));

    // delete table
    admin.deleteTable(tableName);
  }

  /** global operations */
  private void verifyGlobal(PrivilegedExceptionAction<?> action) throws Exception {
    verifyAllowed(action, SUPERUSER);

    verifyDenied(action, USER_CREATE, USER_RW, USER_NONE, USER_RO);
  }

  public void checkGlobalPerms(Permission.Action... actions) throws IOException {
    Permission[] perms = new Permission[actions.length];
    for (int i = 0; i < actions.length; i++) {
      perms[i] = new Permission(actions[i]);
    }
    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        new byte[0]);
      protocol.checkPermissions(perms);
    } finally {
      acl.close();
    }
  }

  public void checkTablePerms(byte[] table, byte[] family, byte[] column,
      Permission.Action... actions) throws IOException {
    Permission[] perms = new Permission[actions.length];
    for (int i = 0; i < actions.length; i++) {
      perms[i] = new TablePermission(table, family, column, actions[i]);
    }

    checkTablePerms(table, perms);
  }

  public void checkTablePerms(byte[] table, Permission... perms) throws IOException {
    HTable acl = new HTable(conf, table);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        new byte[0]);
      protocol.checkPermissions(perms);
    } finally {
      acl.close();
    }
  }

  public void grant(AccessControllerProtocol protocol, User user, byte[] t, byte[] f, byte[] q,
      Permission.Action... actions) throws IOException {
    protocol.grant(new UserPermission(Bytes.toBytes(user.getShortName()), t, f, q, actions));
  }

  @Test
  public void testCheckPermissions() throws Exception {
    // --------------------------------------
    // test global permissions
    PrivilegedExceptionAction<Void> globalAdmin = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkGlobalPerms(Permission.Action.ADMIN);
        return null;
      }
    };
    // verify that only superuser can admin
    verifyGlobal(globalAdmin);

    // --------------------------------------
    // test multiple permissions
    PrivilegedExceptionAction<Void> globalReadWrite = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkGlobalPerms(Permission.Action.READ, Permission.Action.WRITE);
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

    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        TEST_TABLE);
      grant(protocol, userTable, TEST_TABLE, null, null, Permission.Action.READ);
      grant(protocol, userColumn, TEST_TABLE, TEST_FAMILY, null, Permission.Action.READ);
      grant(protocol, userQualifier, TEST_TABLE, TEST_FAMILY, TEST_Q1, Permission.Action.READ);
    } finally {
      acl.close();
    }

    PrivilegedExceptionAction<Void> tableRead = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_TABLE, null, null, Permission.Action.READ);
        return null;
      }
    };

    PrivilegedExceptionAction<Void> columnRead = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_TABLE, TEST_FAMILY, null, Permission.Action.READ);
        return null;
      }
    };

    PrivilegedExceptionAction<Void> qualifierRead = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_TABLE, TEST_FAMILY, TEST_Q1, Permission.Action.READ);
        return null;
      }
    };

    PrivilegedExceptionAction<Void> multiQualifierRead = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_TABLE, new Permission[] {
            new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_Q1, Permission.Action.READ),
            new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_Q2, Permission.Action.READ), });
        return null;
      }
    };

    PrivilegedExceptionAction<Void> globalAndTableRead = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_TABLE, new Permission[] { new Permission(Permission.Action.READ),
            new TablePermission(TEST_TABLE, null, (byte[]) null, Permission.Action.READ), });
        return null;
      }
    };

    PrivilegedExceptionAction<Void> noCheck = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_TABLE, new Permission[0]);
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
    PrivilegedExceptionAction<Void> familyReadWrite = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTablePerms(TEST_TABLE, TEST_FAMILY, null, Permission.Action.READ,
          Permission.Action.WRITE);
        return null;
      }
    };

    verifyAllowed(familyReadWrite, SUPERUSER, USER_OWNER, USER_RW);
    verifyDenied(familyReadWrite, USER_NONE, USER_CREATE, USER_RO);

    // --------------------------------------
    // check for wrong table region
    acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      AccessControllerProtocol protocol = acl.coprocessorProxy(AccessControllerProtocol.class,
        TEST_TABLE);
      try {
        // but ask for TablePermissions for TEST_TABLE
        protocol.checkPermissions(new Permission[] { (Permission) new TablePermission(TEST_TABLE,
          null, (byte[]) null, Permission.Action.CREATE) });
        fail("this should have thrown CoprocessorException");
      } catch (CoprocessorException ex) {
        // expected
      }
    } finally {
      acl.close();
    }
  }

  @Test
  public void testLockAction() throws Exception {
    PrivilegedExceptionAction lockAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preLockRow(ObserverContext.createAndPrepare(RCP_ENV, null), null,
          Bytes.toBytes("random_row"));
        return null;
      }
    };
    verifyAllowed(lockAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_CREATE, USER_RW_ON_TABLE);
    verifyDenied(lockAction, USER_RO, USER_RW, USER_NONE);
  }

  @Test
  public void testUnLockAction() throws Exception {
    PrivilegedExceptionAction unLockAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preUnlockRow(ObserverContext.createAndPrepare(RCP_ENV, null), null,
          123456);
        return null;
      }
    };
    verifyAllowed(unLockAction, SUPERUSER, USER_ADMIN, USER_OWNER, USER_RW_ON_TABLE);
    verifyDenied(unLockAction, USER_NONE, USER_RO, USER_RW);
  }

  @Test
  public void testStopRegionServer() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preStopRegionServer(ObserverContext.createAndPrepare(RSCP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_OWNER, USER_RW, USER_RO, USER_NONE);
  }

  @Test
  public void testOpenRegion() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preOpen(ObserverContext.createAndPrepare(RCP_ENV, null));
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }

  @Test
  public void testCloseRegion() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preClose(ObserverContext.createAndPrepare(RCP_ENV, null), false);
        return null;
      }
    };

    verifyAllowed(action, SUPERUSER, USER_ADMIN);
    verifyDenied(action, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }


  @Test
  public void testSnapshot() throws Exception {
    PrivilegedExceptionAction snapshotAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          null, null);
        return null;
      }
    };

    PrivilegedExceptionAction deleteAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          null);
        return null;
      }
    };

    PrivilegedExceptionAction restoreAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preRestoreSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          null, null);
        return null;
      }
    };

    PrivilegedExceptionAction cloneAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preCloneSnapshot(ObserverContext.createAndPrepare(CP_ENV, null),
          null, null);
        return null;
      }
    };

    verifyAllowed(snapshotAction, SUPERUSER, USER_ADMIN);
    verifyDenied(snapshotAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);

    verifyAllowed(cloneAction, SUPERUSER, USER_ADMIN);
    verifyDenied(deleteAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);

    verifyAllowed(restoreAction, SUPERUSER, USER_ADMIN);
    verifyDenied(restoreAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);

    verifyAllowed(deleteAction, SUPERUSER, USER_ADMIN);
    verifyDenied(cloneAction, USER_CREATE, USER_RW, USER_RO, USER_NONE, USER_OWNER);
  }
}
