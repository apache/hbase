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

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsRequest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.ServiceException;

/**
 * Utility methods for testing security
 */
public class SecureTestUtil {
  
  private static final Log LOG = LogFactory.getLog(SecureTestUtil.class);
  private static final int WAIT_TIME = 10000;

  public static void enableSecurity(Configuration conf) throws IOException {
    conf.set("hadoop.security.authorization", "false");
    conf.set("hadoop.security.authentication", "simple");
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, AccessController.class.getName() +
      "," + SecureBulkLoadEndpoint.class.getName());
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    // The secure minicluster creates separate service principals based on the
    // current user's name, one for each slave. We need to add all of these to
    // the superuser list or security won't function properly. We expect the
    // HBase service account(s) to have superuser privilege.
    String currentUser = User.getCurrent().getName();
    StringBuffer sb = new StringBuffer();
    sb.append("admin,");
    sb.append(currentUser);
    // Assumes we won't ever have a minicluster with more than 5 slaves
    for (int i = 0; i < 5; i++) {
      sb.append(',');
      sb.append(currentUser); sb.append(".hfs."); sb.append(i);
    }
    conf.set("hbase.superuser", sb.toString());
    // Need HFile V3 for tags for security features
    conf.setInt(HFile.FORMAT_VERSION_KEY, 3);
  }

  public static void verifyConfiguration(Configuration conf) {
    if (!(conf.get(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY).contains(
        AccessController.class.getName())
        && conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY).contains(
            AccessController.class.getName()) && conf.get(
        CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY).contains(
        AccessController.class.getName()))) {
      throw new RuntimeException("AccessController is missing from a system coprocessor list");
    }
    if (conf.getInt(HFile.FORMAT_VERSION_KEY, 2) < HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
      throw new RuntimeException("Post 0.96 security features require HFile version >= 3");
    }
  }

  public static void checkTablePerms(Configuration conf, byte[] table, byte[] family, byte[] column,
      Permission.Action... actions) throws IOException {
    Permission[] perms = new Permission[actions.length];
    for (int i = 0; i < actions.length; i++) {
      perms[i] = new TablePermission(TableName.valueOf(table), family, column, actions[i]);
    }

    checkTablePerms(conf, table, perms);
  }

  public static void checkTablePerms(Configuration conf, byte[] table, Permission... perms) throws IOException {
    CheckPermissionsRequest.Builder request = CheckPermissionsRequest.newBuilder();
    for (Permission p : perms) {
      request.addPermission(ProtobufUtil.toPermission(p));
    }
    HTable acl = new HTable(conf, table);
    try {
      AccessControlService.BlockingInterface protocol =
        AccessControlService.newBlockingStub(acl.coprocessorService(new byte[0]));
      try {
        protocol.checkPermissions(null, request.build());
      } catch (ServiceException se) {
        ProtobufUtil.toIOException(se);
      }
    } finally {
      acl.close();
    }
  }

  /**
   * An AccessTestAction performs an action that will be examined to confirm
   * the results conform to expected access rights.
   * <p>
   * To indicate an action was allowed, return null or a non empty list of
   * KeyValues.
   * <p>
   * To indicate the action was not allowed, either throw an AccessDeniedException
   * or return an empty list of KeyValues.
   */
  static interface AccessTestAction extends PrivilegedExceptionAction<Object> { }

  public static void verifyAllowed(User user, AccessTestAction... actions) throws Exception {
    for (AccessTestAction action : actions) {
      try {
        Object obj = user.runAs(action);
        if (obj != null && obj instanceof List<?>) {
          List<?> results = (List<?>) obj;
          if (results != null && results.isEmpty()) {
            fail("Empty non null results from action for user '" + user.getShortName() + "'");
          }
        }
      } catch (AccessDeniedException ade) {
        fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
      }
    }
  }

  public static void verifyAllowed(AccessTestAction action, User... users) throws Exception {
    for (User user : users) {
      verifyAllowed(user, action);
    }
  }

  public static void verifyAllowed(User user, AccessTestAction action, int count) throws Exception {
    try {
      Object obj = user.runAs(action);
      if (obj != null && obj instanceof List<?>) {
        List<?> results = (List<?>) obj;
        if (results != null && results.isEmpty()) {
          fail("Empty non null results from action for user '" + user.getShortName() + "'");
        }
        assertEquals(count, results.size());
      }
    } catch (AccessDeniedException ade) {
      fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
    }
  }

  public static void verifyDeniedWithException(User user, AccessTestAction... actions)
      throws Exception {
    verifyDenied(user, true, actions);
  }

  public static void verifyDeniedWithException(AccessTestAction action, User... users)
      throws Exception {
    for (User user : users) {
      verifyDenied(user, true, action);
    }
  }

  public static void verifyDenied(User user, AccessTestAction... actions) throws Exception {
    verifyDenied(user, false, actions);
  }

  public static void verifyDenied(User user, boolean requireException,
      AccessTestAction... actions) throws Exception {
    for (AccessTestAction action : actions) {
      try {
        Object obj = user.runAs(action);
        if (requireException) {
          fail("Expected exception was not thrown for user '" + user.getShortName() + "'");
        }
        if (obj != null && obj instanceof List<?>) {
          List<?> results = (List<?>) obj;
          if (results != null && !results.isEmpty()) {
            fail("Unexpected results for user '" + user.getShortName() + "'");
          }
        }
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
          fail("Expected exception was not thrown for user '" + user.getShortName() + "'");
        }
      } catch (UndeclaredThrowableException ute) {
        // TODO why we get a PrivilegedActionException, which is unexpected?
        Throwable ex = ute.getUndeclaredThrowable();
        if (ex instanceof PrivilegedActionException) {
          ex = ((PrivilegedActionException) ex).getException();
        }
        if (ex instanceof ServiceException) {
          ServiceException se = (ServiceException)ex;
          if (se.getCause() != null && se.getCause() instanceof AccessDeniedException) {
            // expected result
            return;
          }
        }
        fail("Expected exception was not thrown for user '" + user.getShortName() + "'");
      }
    }
  }

  public static void verifyDenied(AccessTestAction action, User... users) throws Exception {
    for (User user : users) {
      verifyDenied(user, action);
    }
  }

  private static List<AccessController> getAccessControllers(MiniHBaseCluster cluster) {
    List<AccessController> result = Lists.newArrayList();
    for (RegionServerThread t: cluster.getLiveRegionServerThreads()) {
      for (HRegion region: t.getRegionServer().getOnlineRegionsLocalContext()) {
        Coprocessor cp = region.getCoprocessorHost()
          .findCoprocessor(AccessController.class.getName());
        if (cp != null) {
          result.add((AccessController)cp);
        }
      }
    }
    return result;
  }

  private static Map<AccessController,Long> getAuthManagerMTimes(MiniHBaseCluster cluster) {
    Map<AccessController,Long> result = Maps.newHashMap();
    for (AccessController ac: getAccessControllers(cluster)) {
      result.put(ac, ac.getAuthManager().getMTime());
    }
    return result;
  }

  @SuppressWarnings("rawtypes")
  private static void updateACLs(final HBaseTestingUtility util, Callable c) throws Exception {
    // Get the current mtimes for all access controllers
    final Map<AccessController,Long> oldMTimes = getAuthManagerMTimes(util.getHBaseCluster());

    // Run the update action
    c.call();

    // Wait until mtimes for all access controllers have incremented
    util.waitFor(WAIT_TIME, 100, new Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        Map<AccessController,Long> mtimes = getAuthManagerMTimes(util.getHBaseCluster());
        for (Map.Entry<AccessController,Long> e: mtimes.entrySet()) {
          if (!oldMTimes.containsKey(e.getKey())) {
            LOG.error("Snapshot of AccessController state does not include instance on region " +
              e.getKey().getRegion().getRegionNameAsString());
            // Error out the predicate, we will try again
            return false;
          }
          long old = oldMTimes.get(e.getKey());
          long now = e.getValue();
          if (now <= old) {
            LOG.info("AccessController on region " +
              e.getKey().getRegion().getRegionNameAsString() + " has not updated: mtime=" +
              now);
            return false;
          }
        }
        return true;
      }
    });
  }

  /**
   * Grant permissions globally to the given user. Will wait until all active
   * AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void grantGlobal(final HBaseTestingUtility util, final String user,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HTable acl = new HTable(util.getConfiguration(), AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
              AccessControlService.newBlockingStub(service);
          ProtobufUtil.grant(protocol, user, actions);
        } finally {
          acl.close();
        }
        return null;
      }
    });
  }

  /**
   * Revoke permissions globally from the given user. Will wait until all active
   * AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void revokeGlobal(final HBaseTestingUtility util, final String user,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HTable acl = new HTable(util.getConfiguration(), AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
              AccessControlService.newBlockingStub(service);
          ProtobufUtil.revoke(protocol, user, actions);
        } finally {
          acl.close();
        }
        return null;
      }
    });
  }

  /**
   * Grant permissions on a namespace to the given user. Will wait until all active
   * AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void grantOnNamespace(final HBaseTestingUtility util, final String user,
      final String namespace, final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HTable acl = new HTable(util.getConfiguration(), AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
              AccessControlService.newBlockingStub(service);
          ProtobufUtil.grant(protocol, user, namespace, actions);
        } finally {
          acl.close();
        }
        return null;
      }
    });
  }

  /**
   * Grant permissions on a namespace to the given user using AccessControl Client.
   * Will wait until all active AccessController instances have updated their permissions caches
   * or will throw an exception upon timeout (10 seconds).
   */
  public static void grantOnNamespaceUsingAccessControlClient(final HBaseTestingUtility util,
      final Configuration conf, final String user, final String namespace,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.grant(conf, namespace, user, actions);
        } catch (Throwable t) {
          t.printStackTrace();
        }
        return null;
      }
    });
  }

  /**
   * Revoke permissions on a namespace from the given user using AccessControl Client.
   * Will wait until all active AccessController instances have updated their permissions caches
   * or will throw an exception upon timeout (10 seconds).
   */
  public static void revokeFromNamespaceUsingAccessControlClient(final HBaseTestingUtility util,
      final Configuration conf, final String user, final String namespace,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.revoke(conf, namespace, user, actions);
        } catch (Throwable t) {
          t.printStackTrace();
        }
        return null;
      }
    });
  }

  /**
   * Revoke permissions on a namespace from the given user. Will wait until all active
   * AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void revokeFromNamespace(final HBaseTestingUtility util, final String user,
      final String namespace, final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HTable acl = new HTable(util.getConfiguration(), AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
              AccessControlService.newBlockingStub(service);
          ProtobufUtil.revoke(protocol, user, namespace, actions);
        } finally {
          acl.close();
        }
        return null;
      }
    });
  }

  /**
   * Grant permissions on a table to the given user. Will wait until all active
   * AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void grantOnTable(final HBaseTestingUtility util, final String user,
      final TableName table, final byte[] family, final byte[] qualifier,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HTable acl = new HTable(util.getConfiguration(), AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
              AccessControlService.newBlockingStub(service);
          ProtobufUtil.grant(protocol, user, table, family, qualifier, actions);
        } finally {
          acl.close();
        }
        return null;
      }
    });
  }

  /**
   * Grant permissions on a table to the given user using AccessControlClient. Will wait until all
   * active AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void grantOnTableUsingAccessControlClient(final HBaseTestingUtility util,
      final Configuration conf, final String user, final TableName table, final byte[] family,
      final byte[] qualifier, final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.grant(conf, table, user, family, qualifier, actions);
        } catch (Throwable t) {
          t.printStackTrace();
        }
        return null;
      }
    });
  }

  /**
   * Grant global permissions to the given user using AccessControlClient. Will wait until all
   * active AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void grantGlobalUsingAccessControlClient(final HBaseTestingUtility util,
      final Configuration conf, final String user, final Permission.Action... actions)
      throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.grant(conf, user, actions);
        } catch (Throwable t) {
          t.printStackTrace();
        }
        return null;
      }
    });
  }

  /**
   * Revoke permissions on a table from the given user. Will wait until all active
   * AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void revokeFromTable(final HBaseTestingUtility util, final String user,
      final TableName table, final byte[] family, final byte[] qualifier,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HTable acl = new HTable(util.getConfiguration(), AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service = acl.coprocessorService(HConstants.EMPTY_START_ROW);
          AccessControlService.BlockingInterface protocol =
              AccessControlService.newBlockingStub(service);
          ProtobufUtil.revoke(protocol, user, table, family, qualifier, actions);
        } finally {
          acl.close();
        }
        return null;
      }
    });
  }

  /**
   * Revoke permissions on a table from the given user using AccessControlClient. Will wait until
   * all active AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void revokeFromTableUsingAccessControlClient(final HBaseTestingUtility util,
      final Configuration conf, final String user, final TableName table, final byte[] family,
      final byte[] qualifier, final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.revoke(conf, table, user, family, qualifier, actions);
        } catch (Throwable t) {
          t.printStackTrace();
        }
        return null;
      }
    });
  }

  /**
   * Revoke global permissions from the given user using AccessControlClient. Will wait until
   * all active AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void revokeGlobalUsingAccessControlClient(final HBaseTestingUtility util,
      final Configuration conf, final String user,final Permission.Action... actions)
      throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.revoke(conf, user, actions);
        } catch (Throwable t) {
          t.printStackTrace();
        }
        return null;
      }
    });
  }
}
