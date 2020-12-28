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

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Utility methods for testing security
 */
public class SecureTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SecureTestUtil.class);
  private static final int WAIT_TIME = 10000;

  public static void configureSuperuser(Configuration conf) throws IOException {
    // The secure minicluster creates separate service principals based on the
    // current user's name, one for each slave. We need to add all of these to
    // the superuser list or security won't function properly. We expect the
    // HBase service account(s) to have superuser privilege.
    String currentUser = User.getCurrent().getName();
    StringBuilder sb = new StringBuilder();
    sb.append("admin,");
    sb.append(currentUser);
    // Assumes we won't ever have a minicluster with more than 5 slaves
    for (int i = 0; i < 5; i++) {
      sb.append(',');
      sb.append(currentUser); sb.append(".hfs."); sb.append(i);
    }
    // Add a supergroup for improving test coverage.
    sb.append(',').append("@supergroup");
    conf.set("hbase.superuser", sb.toString());
    // hbase.group.service.for.test.only is used in test only.
    conf.set(User.TestingGroups.TEST_CONF, "true");
  }

  public static void enableSecurity(Configuration conf) throws IOException {
    conf.set("hadoop.security.authorization", "false");
    conf.set("hadoop.security.authentication", "simple");
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, AccessController.class.getName() +
      "," + MasterSyncObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    // Need HFile V3 for tags for security features
    conf.setInt(HFile.FORMAT_VERSION_KEY, 3);
    conf.set(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, "true");
    configureSuperuser(conf);
  }

  public static void verifyConfiguration(Configuration conf) {
    String coprocs = conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY);
    boolean accessControllerLoaded = false;
    for (String coproc : coprocs.split(",")) {
      try {
        accessControllerLoaded = AccessController.class.isAssignableFrom(Class.forName(coproc));
        if (accessControllerLoaded) break;
      } catch (ClassNotFoundException cnfe) {
      }
    }
    if (!(conf.get(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY).contains(
        AccessController.class.getName())
        && accessControllerLoaded && conf.get(
        CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY).contains(
        AccessController.class.getName()))) {
      throw new RuntimeException("AccessController is missing from a system coprocessor list");
    }
    if (conf.getInt(HFile.FORMAT_VERSION_KEY, 2) < HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
      throw new RuntimeException("Post 0.96 security features require HFile version >= 3");
    }

    if (!conf.getBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, false)) {
      throw new RuntimeException("Post 2.0.0 security features require set "
          + User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY + " to true");
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
  public interface AccessTestAction extends PrivilegedExceptionAction<Object> { }

  /** This fails only in case of ADE or empty list for any of the actions. */
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

  /** This fails only in case of ADE or empty list for any of the users. */
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

  /** This passes only in case of ADE for all users. */
  public static void verifyDenied(AccessTestAction action, User... users) throws Exception {
    for (User user : users) {
      verifyDenied(user, action);
    }
  }

  /** This passes only in case of empty list for all users. */
  public static void verifyIfEmptyList(AccessTestAction action, User... users) throws Exception {
    for (User user : users) {
      try {
        Object obj = user.runAs(action);
        if (obj != null && obj instanceof List<?>) {
          List<?> results = (List<?>) obj;
          if (results != null && !results.isEmpty()) {
            fail("Unexpected action results: " +  results + " for user '"
                + user.getShortName() + "'");
          }
        } else {
          fail("Unexpected results for user '" + user.getShortName() + "'");
        }
      } catch (AccessDeniedException ade) {
        fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
      }
    }
  }

  /** This passes only in case of null for all users. */
  public static void verifyIfNull(AccessTestAction  action, User... users) throws Exception {
    for (User user : users) {
      try {
        Object obj = user.runAs(action);
        if (obj != null) {
          fail("Non null results from action for user '" + user.getShortName() + "' : " + obj);
        }
      } catch (AccessDeniedException ade) {
        fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
      }
    }
  }

  /** This passes only in case of ADE for all actions. */
  public static void verifyDenied(User user, AccessTestAction... actions) throws Exception {
    for (AccessTestAction action : actions) {
      try {
        user.runAs(action);
        fail("Expected exception was not thrown for user '" + user.getShortName() + "'");
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
            if (ex instanceof RemoteWithExtrasException) {
              ex = ((RemoteWithExtrasException) ex).unwrapRemoteException();
            }
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

  private static List<AccessController> getAccessControllers(MiniHBaseCluster cluster) {
    List<AccessController> result = Lists.newArrayList();
    for (RegionServerThread t: cluster.getLiveRegionServerThreads()) {
      for (HRegion region: t.getRegionServer().getOnlineRegionsLocalContext()) {
        Coprocessor cp = region.getCoprocessorHost().findCoprocessor(AccessController.class);
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
              e.getKey().getRegion().getRegionInfo().getRegionNameAsString());
            // Error out the predicate, we will try again
            return false;
          }
          long old = oldMTimes.get(e.getKey());
          long now = e.getValue();
          if (now <= old) {
            LOG.info("AccessController on region " +
              e.getKey().getRegion().getRegionInfo().getRegionNameAsString() +
              " has not updated: mtime=" + now);
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
        try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
          connection.getAdmin().grant(
            new UserPermission(user, Permission.newBuilder().withActions(actions).build()), false);
        }
        return null;
      }
    });
  }

  /**
   * Grant permissions globally to the given user. Will wait until all active
   * AccessController instances have updated their permissions caches or will
   * throw an exception upon timeout (10 seconds).
   */
  public static void grantGlobal(final User caller, final HBaseTestingUtility util,
      final String user, final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Configuration conf = util.getConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(conf, caller)) {
          connection.getAdmin().grant(
            new UserPermission(user, Permission.newBuilder().withActions(actions).build()), false);
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
        try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
          connection.getAdmin().revoke(
            new UserPermission(user, Permission.newBuilder().withActions(actions).build()));
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
  public static void revokeGlobal(final User caller, final HBaseTestingUtility util,
      final String user, final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Configuration conf = util.getConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(conf, caller)) {
          connection.getAdmin().revoke(
            new UserPermission(user, Permission.newBuilder().withActions(actions).build()));
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
        try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
          connection.getAdmin().grant(
            new UserPermission(user, Permission.newBuilder(namespace).withActions(actions).build()),
            false);
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
  public static void grantOnNamespace(final User caller, final HBaseTestingUtility util,
      final String user, final String namespace,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Configuration conf = util.getConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(conf, caller)) {
          connection.getAdmin().grant(
            new UserPermission(user, Permission.newBuilder(namespace).withActions(actions).build()),
            false);
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
      final Connection connection, final String user, final String namespace,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.grant(connection, namespace, user, actions);
        } catch (Throwable t) {
          LOG.error("grant failed: ", t);
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
      final Connection connection, final String user, final String namespace,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.revoke(connection, namespace, user, actions);
        } catch (Throwable t) {
          LOG.error("revoke failed: ", t);
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
        try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
          connection.getAdmin().revoke(new UserPermission(user,
              Permission.newBuilder(namespace).withActions(actions).build()));
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
  public static void revokeFromNamespace(final User caller, final HBaseTestingUtility util,
      final String user, final String namespace,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Configuration conf = util.getConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(conf, caller)) {
          connection.getAdmin().revoke(new UserPermission(user,
              Permission.newBuilder(namespace).withActions(actions).build()));
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
        try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
          connection.getAdmin().grant(new UserPermission(user, Permission.newBuilder(table)
              .withFamily(family).withQualifier(qualifier).withActions(actions).build()),
            false);
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
  public static void grantOnTable(final User caller, final HBaseTestingUtility util,
      final String user, final TableName table, final byte[] family, final byte[] qualifier,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Configuration conf = util.getConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(conf, caller)) {
          connection.getAdmin().grant(new UserPermission(user, Permission.newBuilder(table)
              .withFamily(family).withQualifier(qualifier).withActions(actions).build()),
            false);
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
      final Connection connection, final String user, final TableName table, final byte[] family,
      final byte[] qualifier, final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.grant(connection, table, user, family, qualifier, actions);
        } catch (Throwable t) {
          LOG.error("grant failed: ", t);
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
      final Connection connection, final String user, final Permission.Action... actions)
      throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.grant(connection, user, actions);
        } catch (Throwable t) {
          LOG.error("grant failed: ", t);
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
        try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
          connection.getAdmin().revoke(new UserPermission(user, Permission.newBuilder(table)
              .withFamily(family).withQualifier(qualifier).withActions(actions).build()));
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
  public static void revokeFromTable(final User caller, final HBaseTestingUtility util,
      final String user, final TableName table, final byte[] family, final byte[] qualifier,
      final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Configuration conf = util.getConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(conf, caller)) {
          connection.getAdmin().revoke(new UserPermission(user, Permission.newBuilder(table)
              .withFamily(family).withQualifier(qualifier).withActions(actions).build()));
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
      final Connection connection, final String user, final TableName table, final byte[] family,
      final byte[] qualifier, final Permission.Action... actions) throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.revoke(connection, table, user, family, qualifier, actions);
        } catch (Throwable t) {
          LOG.error("revoke failed: ", t);
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
      final Connection connection, final String user,final Permission.Action... actions)
      throws Exception {
    SecureTestUtil.updateACLs(util, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          AccessControlClient.revoke(connection, user, actions);
        } catch (Throwable t) {
          LOG.error("revoke failed: ", t);
        }
        return null;
      }
    });
  }

  public static class MasterSyncObserver implements MasterCoprocessor, MasterObserver {
    volatile CountDownLatch tableCreationLatch = null;
    volatile CountDownLatch tableDeletionLatch = null;

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void postCompletedCreateTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        TableDescriptor desc, RegionInfo[] regions) throws IOException {
      // the AccessController test, some times calls only and directly the
      // postCompletedCreateTableAction()
      if (tableCreationLatch != null) {
        tableCreationLatch.countDown();
      }
    }

    @Override
    public void postCompletedDeleteTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final TableName tableName) throws IOException {
      // the AccessController test, some times calls only and directly the
      // postCompletedDeleteTableAction()
      if (tableDeletionLatch != null) {
        tableDeletionLatch.countDown();
      }
    }
  }

  public static Table createTable(HBaseTestingUtility testUtil, TableName tableName,
      byte[][] families) throws Exception {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    for (byte[] family : families) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    createTable(testUtil, testUtil.getAdmin(), builder.build());
    return testUtil.getConnection().getTable(tableName);
  }

  public static void createTable(HBaseTestingUtility testUtil, TableDescriptor htd)
      throws Exception {
    createTable(testUtil, testUtil.getAdmin(), htd);
  }

  public static void createTable(HBaseTestingUtility testUtil, TableDescriptor htd,
      byte[][] splitKeys) throws Exception {
    createTable(testUtil, testUtil.getAdmin(), htd, splitKeys);
  }

  public static void createTable(HBaseTestingUtility testUtil, Admin admin, TableDescriptor htd)
      throws Exception {
    createTable(testUtil, admin, htd, null);
  }

  public static void createTable(HBaseTestingUtility testUtil, Admin admin, TableDescriptor htd,
      byte[][] splitKeys) throws Exception {
    // NOTE: We need a latch because admin is not sync,
    // so the postOp coprocessor method may be called after the admin operation returned.
    MasterSyncObserver observer = testUtil.getHBaseCluster().getMaster()
      .getMasterCoprocessorHost().findCoprocessor(MasterSyncObserver.class);
    observer.tableCreationLatch = new CountDownLatch(1);
    if (splitKeys != null) {
      admin.createTable(htd, splitKeys);
    } else {
      admin.createTable(htd);
    }
    observer.tableCreationLatch.await();
    observer.tableCreationLatch = null;
    testUtil.waitUntilAllRegionsAssigned(htd.getTableName());
  }

  public static void deleteTable(HBaseTestingUtility testUtil, TableName tableName)
      throws Exception {
    deleteTable(testUtil, testUtil.getAdmin(), tableName);
  }

  public static void createNamespace(HBaseTestingUtility testUtil, NamespaceDescriptor nsDesc)
      throws Exception {
    testUtil.getAdmin().createNamespace(nsDesc);
  }

  public static void deleteNamespace(HBaseTestingUtility testUtil, String namespace)
      throws Exception {
    testUtil.getAdmin().deleteNamespace(namespace);
  }

  public static void deleteTable(HBaseTestingUtility testUtil, Admin admin, TableName tableName)
      throws Exception {
    // NOTE: We need a latch because admin is not sync,
    // so the postOp coprocessor method may be called after the admin operation returned.
    MasterSyncObserver observer = testUtil.getHBaseCluster().getMaster()
      .getMasterCoprocessorHost().findCoprocessor(MasterSyncObserver.class);
    observer.tableDeletionLatch = new CountDownLatch(1);
    try {
      admin.disableTable(tableName);
    } catch (TableNotEnabledException e) {
      LOG.debug("Table: " + tableName + " already disabled, so just deleting it.");
    }
    admin.deleteTable(tableName);
    observer.tableDeletionLatch.await();
    observer.tableDeletionLatch = null;
  }

  public static String convertToNamespace(String namespace) {
    return PermissionStorage.NAMESPACE_PREFIX + namespace;
  }

  public static void checkGlobalPerms(HBaseTestingUtility testUtil, Permission.Action... actions)
      throws IOException {
    Permission[] perms = new Permission[actions.length];
    for (int i = 0; i < actions.length; i++) {
      perms[i] = new Permission(actions[i]);
    }
    checkPermissions(testUtil.getConfiguration(), perms);
  }

  public static void checkTablePerms(HBaseTestingUtility testUtil, TableName table, byte[] family,
      byte[] column, Permission.Action... actions) throws IOException {
    Permission[] perms = new Permission[actions.length];
    for (int i = 0; i < actions.length; i++) {
      perms[i] = Permission.newBuilder(table).withFamily(family).withQualifier(column)
          .withActions(actions[i]).build();
    }
    checkTablePerms(testUtil, perms);
  }

  public static void checkTablePerms(HBaseTestingUtility testUtil, Permission... perms)
      throws IOException {
    checkPermissions(testUtil.getConfiguration(), perms);
  }

  private static void checkPermissions(Configuration conf, Permission... perms) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      List<Boolean> hasUserPermissions =
          conn.getAdmin().hasUserPermissions(Lists.newArrayList(perms));
      for (int i = 0; i < hasUserPermissions.size(); i++) {
        if (!hasUserPermissions.get(i).booleanValue()) {
          throw new AccessDeniedException("Insufficient permissions " + perms[i]);
        }
      }
    }
  }
}
