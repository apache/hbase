/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.UserProvider;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Provides basic authorization checks for data access and administrative
 * operations.
 *
 * <p>
 * {@code AccessController} performs authorization checks for HBase operations
 * based on:
 * <ul>
 *   <li>the identity of the user performing the operation</li>
 *   <li>the scope over which the operation is performed, in increasing
 *   specificity: global, table, column family, or qualifier</li>
 *   <li>the type of action being performed (as mapped to
 *   {@link Permission.Action} values)</li>
 * </ul>
 * If the authorization check fails, an {@link AccessDeniedException}
 * will be thrown for the operation.
 * </p>
 *
 * <p>
 * To perform authorization checks, {@code AccessController} relies on the
 * {@link org.apache.hadoop.hbase.ipc.SecureRpcEngine} being loaded to provide
 * the user identities for remote requests.
 * </p>
 *
 * <p>
 * The access control lists used for authorization can be manipulated via the
 * exposed {@link AccessControllerProtocol} implementation, and the associated
 * {@code grant}, {@code revoke}, and {@code user_permission} HBase shell
 * commands.
 * </p>
 */
public class AccessController extends BaseRegionObserver
    implements MasterObserver, RegionServerObserver, AccessControllerProtocol {
  /**
   * Represents the result of an authorization check for logging and error
   * reporting.
   */
  private static class AuthResult {
    private final boolean allowed;
    private final byte[] table;
    private final byte[] family;
    private final byte[] qualifier;
    private final Permission.Action action;
    private final String request;
    private final String reason;
    private final User user;

    public AuthResult(boolean allowed, String request, String reason,  User user,
        Permission.Action action, byte[] table, byte[] family, byte[] qualifier) {
      this.allowed = allowed;
      this.request = request;
      this.reason = reason;
      this.user = user;
      this.table = table;
      this.family = family;
      this.qualifier = qualifier;
      this.action = action;
    }

    public boolean isAllowed() { return allowed; }

    public User getUser() { return user; }

    public String getReason() { return reason; }

    public String getRequest() { return request; }

    public String toContextString() {
      return "(user=" + (user != null ? user.getName() : "UNKNOWN") + ", " +
          "scope=" + (table == null ? "GLOBAL" : Bytes.toString(table)) + ", " +
          "family=" + (family != null ? Bytes.toString(family) : "") + ", " +
          "qualifer=" + (qualifier != null ? Bytes.toString(qualifier) : "") + ", " +
          "action=" + (action != null ? action.toString() : "") + ")";
    }

    public String toString() {
      return new StringBuilder("AuthResult")
          .append(toContextString()).toString();
    }

    public static AuthResult allow(String request, String reason, User user, Permission.Action action,
        byte[] table, byte[] family, byte[] qualifier) {
      return new AuthResult(true, request, reason, user, action, table, family, qualifier);
    }

    public static AuthResult allow(String request, String reason, User user, Permission.Action action, byte[] table) {
      return new AuthResult(true, request, reason, user, action, table, null, null);
    }

    public static AuthResult deny(String request, String reason, User user,
        Permission.Action action, byte[] table) {
      return new AuthResult(false, request, reason, user, action, table, null, null);
    }

    public static AuthResult deny(String request, String reason, User user,
        Permission.Action action, byte[] table, byte[] family, byte[] qualifier) {
      return new AuthResult(false, request, reason, user, action, table, family, qualifier);
    }
  }

  public static final Log LOG = LogFactory.getLog(AccessController.class);

  private static final Log AUDITLOG =
    LogFactory.getLog("SecurityLogger."+AccessController.class.getName());

  /**
   * Version number for AccessControllerProtocol
   */
  private static final long PROTOCOL_VERSION = 1L;

  TableAuthManager authManager = null;

  // flags if we are running on a region of the _acl_ table
  boolean aclRegion = false;

  // defined only for Endpoint implementation, so it can have way to
  // access region services.
  private RegionCoprocessorEnvironment regionEnv;

  /** Mapping of scanner instances to the user who created them */
  private Map<InternalScanner,String> scannerOwners =
      new MapMaker().weakKeys().makeMap();

  private UserProvider userProvider;

  void initialize(RegionCoprocessorEnvironment e) throws IOException {
    final HRegion region = e.getRegion();
    Map<byte[],ListMultimap<String,TablePermission>> tables =
        AccessControlLists.loadAll(region);
    // For each table, write out the table's permissions to the respective
    // znode for that table.
    for (Map.Entry<byte[],ListMultimap<String,TablePermission>> t:
      tables.entrySet()) {
      byte[] table = t.getKey();
      ListMultimap<String,TablePermission> perms = t.getValue();
      byte[] serialized = AccessControlLists.writePermissionsAsBytes(perms,
          regionEnv.getConfiguration());
      this.authManager.getZKPermissionWatcher().writeToZookeeper(table, serialized);
    }
  }

  /**
   * Writes all table ACLs for the tables in the given Map up into ZooKeeper
   * znodes.  This is called to synchronize ACL changes following {@code _acl_}
   * table updates.
   */
  void updateACL(RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap) {
    Set<byte[]> tableSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], List<KeyValue>> f : familyMap.entrySet()) {
      List<KeyValue> kvs = f.getValue();
      for (KeyValue kv: kvs) {
        if (Bytes.equals(kv.getBuffer(), kv.getFamilyOffset(),
            kv.getFamilyLength(), AccessControlLists.ACL_LIST_FAMILY, 0,
            AccessControlLists.ACL_LIST_FAMILY.length)) {
          tableSet.add(kv.getRow());
        }
      }
    }

    ZKPermissionWatcher zkw = this.authManager.getZKPermissionWatcher();
    Configuration conf = regionEnv.getConfiguration();
    for (byte[] tableName: tableSet) {
      try {
        ListMultimap<String,TablePermission> perms =
          AccessControlLists.getTablePermissions(conf, tableName);
        byte[] serialized = AccessControlLists.writePermissionsAsBytes(perms, conf);
        zkw.writeToZookeeper(tableName, serialized);
      } catch (IOException ex) {
        LOG.error("Failed updating permissions mirror for '" + tableName + "'", ex);
      }
    }
  }

  /**
   * Check the current user for authorization to perform a specific action
   * against the given set of row data.
   *
   * <p>Note: Ordering of the authorization checks
   * has been carefully optimized to short-circuit the most common requests
   * and minimize the amount of processing required.</p>
   *
   * @param permRequest the action being requested
   * @param e the coprocessor environment
   * @param families the map of column families to qualifiers present in
   * the request
   * @return
   */
  AuthResult permissionGranted(String request, User user, TablePermission.Action permRequest,
      RegionCoprocessorEnvironment e,
      Map<byte [], ? extends Collection<?>> families) {
    HRegionInfo hri = e.getRegion().getRegionInfo();
    byte[] tableName = hri.getTableName();

    // 1. All users need read access to .META. and -ROOT- tables.
    // this is a very common operation, so deal with it quickly.
    if (hri.isRootRegion() || hri.isMetaRegion()) {
      if (permRequest == TablePermission.Action.READ) {
        return AuthResult.allow(request, "All users allowed", user, permRequest, tableName);
      }
    }

    if (user == null) {
      return AuthResult.deny(request, "No user associated with request!", null, permRequest, tableName);
    }

    // 2. check for the table-level, if successful we can short-circuit
    if (authManager.authorize(user, tableName, (byte[])null, permRequest)) {
      return AuthResult.allow(request, "Table permission granted", user, permRequest, tableName);
    }

    // 3. check permissions against the requested families
    if (families != null && families.size() > 0) {
      // all families must pass
      for (Map.Entry<byte [], ? extends Collection<?>> family : families.entrySet()) {
        // a) check for family level access
        if (authManager.authorize(user, tableName, family.getKey(),
            permRequest)) {
          continue;  // family-level permission overrides per-qualifier
        }

        // b) qualifier level access can still succeed
        if ((family.getValue() != null) && (family.getValue().size() > 0)) {
          if (family.getValue() instanceof Set) {
            // for each qualifier of the family
            Set<byte[]> familySet = (Set<byte[]>)family.getValue();
            for (byte[] qualifier : familySet) {
              if (!authManager.authorize(user, tableName, family.getKey(),
                                         qualifier, permRequest)) {
                return AuthResult.deny(request, "Failed qualifier check", user,
                    permRequest, tableName, family.getKey(), qualifier);
              }
            }
          } else if (family.getValue() instanceof List) { // List<KeyValue>
            List<KeyValue> kvList = (List<KeyValue>)family.getValue();
            for (KeyValue kv : kvList) {
              if (!authManager.authorize(user, tableName, family.getKey(),
                      kv.getQualifier(), permRequest)) {
                return AuthResult.deny(request, "Failed qualifier check", user,
                    permRequest, tableName, family.getKey(), kv.getQualifier());
              }
            }
          }
        } else {
          // no qualifiers and family-level check already failed
          return AuthResult.deny(request, "Failed family check", user, permRequest,
              tableName, family.getKey(), null);
        }
      }

      // all family checks passed
      return AuthResult.allow(request, "All family checks passed", user, permRequest,
          tableName);
    }

    // 4. no families to check and table level access failed
    return AuthResult.deny(request, "No families to check and table permission failed",
        user, permRequest, tableName);
  }

  private void logResult(AuthResult result) {
    if (AUDITLOG.isTraceEnabled()) {
      InetAddress remoteAddr = null;
      RequestContext ctx = RequestContext.get();
      if (ctx != null) {
        remoteAddr = ctx.getRemoteAddress();
      }
      AUDITLOG.trace("Access " + (result.isAllowed() ? "allowed" : "denied") +
          " for user " + (result.getUser() != null ? result.getUser().getShortName() : "UNKNOWN") +
          "; reason: " + result.getReason() +
          "; remote address: " + (remoteAddr != null ? remoteAddr : "") +
          "; request: " + result.getRequest() +
          "; context: " + result.toContextString());
    }
  }

  /**
   * Returns the active user to which authorization checks should be applied.
   * If we are in the context of an RPC call, the remote user is used,
   * otherwise the currently logged in user is used.
   */
  private User getActiveUser() throws IOException {
    User user = RequestContext.getRequestUser();
    if (!RequestContext.isInRequestContext()) {
      // for non-rpc handling, fallback to system user
      user = userProvider.getCurrent();
    }

    return user;
  }

  /**
   * Authorizes that the current user has any of the given permissions for the
   * given table, column family and column qualifier.
   * @param tableName Table requested
   * @param family Column family requested
   * @param qualifier Column qualifier requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if user has no authorization
   */
  private void requirePermission(String request, byte[] tableName, byte[] family, byte[] qualifier,
      Action... permissions) throws IOException {
    User user = getActiveUser();
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorize(user, tableName, family, qualifier, permission)) {
        result = AuthResult.allow(request, "Table permission granted", user,
                                  permission, tableName, family, qualifier);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
                                 permission, tableName, family, qualifier);
      }
    }
    logResult(result);
    if (!result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has global privileges for the given action.
   * @param perm The action being requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if authorization is denied
   */
  private void requirePermission(String request, Permission.Action perm) throws IOException {
    User user = getActiveUser();
    if (authManager.authorize(user, perm)) {
      logResult(AuthResult.allow(request, "Global check allowed", user, perm, null));
    } else {
      logResult(AuthResult.deny(request, "Global check failed", user, perm, null));
      throw new AccessDeniedException("Insufficient permissions for user '" +
          (user != null ? user.getShortName() : "null") +"' (global, action=" +
          perm.toString() + ")");
    }
  }

  /**
   * Authorizes that the current user has permission to perform the given
   * action on the set of table column families.
   * @param perm Action that is required
   * @param env The current coprocessor environment
   * @param families The set of column families present/required in the request
   * @throws AccessDeniedException if the authorization check failed
   */
  private void requirePermission(String request, Permission.Action perm,
        RegionCoprocessorEnvironment env, Collection<byte[]> families)
      throws IOException {
    // create a map of family-qualifier
    HashMap<byte[], Set<byte[]>> familyMap = new HashMap<byte[], Set<byte[]>>();
    for (byte[] family : families) {
      familyMap.put(family, null);
    }
    requirePermission(request, perm, env, familyMap);
  }

  /**
   * Authorizes that the current user has permission to perform the given
   * action on the set of table column families.
   * @param perm Action that is required
   * @param env The current coprocessor environment
   * @param families The map of column families-qualifiers.
   * @throws AccessDeniedException if the authorization check failed
   */
  public void requirePermission(String request, Permission.Action perm,
        RegionCoprocessorEnvironment env,
        Map<byte[], ? extends Collection<?>> families)
      throws IOException {
    User user = getActiveUser();
    AuthResult result = permissionGranted(request, user, perm, env, families);
    logResult(result);

    if (!result.isAllowed()) {
      StringBuffer sb = new StringBuffer("");
      if ((families != null && families.size() > 0)) {
        for (byte[] familyName : families.keySet()) {
          if (sb.length() != 0) {
            sb.append(", ");
          }
          sb.append(Bytes.toString(familyName));
        }
      }
      throw new AccessDeniedException("Insufficient permissions (table=" +
        env.getRegion().getTableDesc().getNameAsString()+
        ((families != null && families.size() > 0) ? ", family: " +
        sb.toString() : "") + ", action=" +
        perm.toString() + ")");
    }
  }

  /**
   * Returns <code>true</code> if the current user is allowed the given action
   * over at least one of the column qualifiers in the given column families.
   */
  private boolean hasFamilyQualifierPermission(User user,
      TablePermission.Action perm,
      RegionCoprocessorEnvironment env,
      Map<byte[], ? extends Set<byte[]>> familyMap)
    throws IOException {
    HRegionInfo hri = env.getRegion().getRegionInfo();
    byte[] tableName = hri.getTableName();

    if (user == null) {
      return false;
    }

    if (familyMap != null && familyMap.size() > 0) {
      // at least one family must be allowed
      for (Map.Entry<byte[], ? extends Set<byte[]>> family :
          familyMap.entrySet()) {
        if (family.getValue() != null && !family.getValue().isEmpty()) {
          for (byte[] qualifier : family.getValue()) {
            if (authManager.matchPermission(user, tableName,
                family.getKey(), qualifier, perm)) {
              return true;
            }
          }
        } else {
          if (authManager.matchPermission(user, tableName, family.getKey(),
              perm)) {
            return true;
          }
        }
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Empty family map passed for permission check");
    }

    return false;
  }

  /* ---- MasterObserver implementation ---- */
  public void start(CoprocessorEnvironment env) throws IOException {

    ZooKeeperWatcher zk = null;
    if (env instanceof MasterCoprocessorEnvironment) {
      // if running on HMaster
      MasterCoprocessorEnvironment mEnv = (MasterCoprocessorEnvironment) env;
      zk = mEnv.getMasterServices().getZooKeeper();      
    } else if (env instanceof RegionServerCoprocessorEnvironment) {      
      RegionServerCoprocessorEnvironment rsEnv = (RegionServerCoprocessorEnvironment) env;
      zk = rsEnv.getRegionServerServices().getZooKeeper();      
    } else if (env instanceof RegionCoprocessorEnvironment) {
      // if running at region
      regionEnv = (RegionCoprocessorEnvironment) env;
      zk = regionEnv.getRegionServerServices().getZooKeeper();
    }

    // set the user provider
    this.userProvider = UserProvider.instantiate(env.getConfiguration());

    // If zk is null or IOException while obtaining auth manager,
    // throw RuntimeException so that the coprocessor is unloaded.
    if (zk != null) {
      try {
        this.authManager = TableAuthManager.get(zk, env.getConfiguration());
      } catch (IOException ioe) {
        throw new RuntimeException("Error obtaining TableAuthManager", ioe);
      }
    } else {
      throw new RuntimeException("Error obtaining TableAuthManager, zk found null.");
    }
  }

  public void stop(CoprocessorEnvironment env) {

  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> c,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    requirePermission("createTable", Permission.Action.CREATE);
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> c,
      final HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    if (!AccessControlLists.isAclTable(desc)) {
      final Configuration conf = c.getEnvironment().getConfiguration();
      final String owner = (desc.getOwnerString() != null) ? desc.getOwnerString() : 
        getActiveUser().getShortName();
      User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          UserPermission userperm = new UserPermission(Bytes.toBytes(owner), desc.getName(), null,
              Action.values());
          AccessControlLists.addUserPermission(conf, userperm);
          return null;
        }
      });
    }
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, byte[] tableName)
      throws IOException {
   requirePermission("deleteTable", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c,
      final byte[] tableName) throws IOException {
    final Configuration conf = c.getEnvironment().getConfiguration();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        AccessControlLists.removeTablePermissions(conf, tableName);
        return null;
      }
    });
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> c, byte[] tableName,
      HTableDescriptor htd) throws IOException {
    requirePermission("modifyTable", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, final HTableDescriptor htd) throws IOException {
    final Configuration conf = c.getEnvironment().getConfiguration();
    final String owner = (htd.getOwnerString() != null) ? htd.getOwnerString() : 
      getActiveUser().getShortName();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        UserPermission userperm = new UserPermission(Bytes.toBytes(owner), htd.getName(), null,
            Action.values());
        AccessControlLists.addUserPermission(conf, userperm);
        return null;
      }
    });
  }

  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> c, byte[] tableName,
      HColumnDescriptor column) throws IOException {
    requirePermission("addColumn", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, HColumnDescriptor column) throws IOException {}

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> c, byte[] tableName,
      HColumnDescriptor descriptor) throws IOException {
    requirePermission("modifyColumn", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName, HColumnDescriptor descriptor) throws IOException {}

  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c, byte[] tableName,
      byte[] col) throws IOException {
    requirePermission("deleteColumn", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      final byte[] tableName, final byte[] col) throws IOException {
    final Configuration conf = c.getEnvironment().getConfiguration();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        AccessControlLists.removeTablePermissions(conf, tableName, col);
        return null;
      }
    });
    this.authManager.getZKPermissionWatcher().deleteTableACLNode(tableName);
  }

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, byte[] tableName)
      throws IOException {
    requirePermission("enableTable", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName) throws IOException {}

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, byte[] tableName)
      throws IOException {
    if (Bytes.equals(tableName, AccessControlLists.ACL_GLOBAL_NAME)) {
      throw new AccessDeniedException("Not allowed to disable "
          + AccessControlLists.ACL_TABLE_NAME_STR + " table.");
    }
    requirePermission("disableTable", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> c,
      byte[] tableName) throws IOException {}

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo region,
      ServerName srcServer, ServerName destServer) throws IOException {
    requirePermission("move", region.getTableName(), null, null, Action.ADMIN);
  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo region, ServerName srcServer, ServerName destServer)
    throws IOException {}

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo)
      throws IOException {
    requirePermission("assign", regionInfo.getTableName(), null, null, Action.ADMIN);
  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo regionInfo) throws IOException {}

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo,
      boolean force) throws IOException {
    requirePermission("unassign", regionInfo.getTableName(), null, null, Action.ADMIN);
  }

  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo regionInfo, boolean force) throws IOException {}

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission("balance", Permission.Action.ADMIN);
  }
  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {}

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c,
      boolean newValue) throws IOException {
    requirePermission("balanceSwitch", Permission.Action.ADMIN);
    return newValue;
  }
  @Override
  public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c,
      boolean oldValue, boolean newValue) throws IOException {}

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission("shutdown", Permission.Action.ADMIN);
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission("stopMaster", Permission.Action.ADMIN);
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    // initialize the ACL storage table
    AccessControlLists.init(ctx.getEnvironment().getMasterServices());
  }

  @Override
  public void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
    requirePermission("snapshot", Permission.Action.ADMIN);
  }

  @Override
  public void postSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
    requirePermission("cloneSnapshot", Permission.Action.ADMIN);
  }

  @Override
  public void postCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
    requirePermission("restoreSnapshot", Permission.Action.ADMIN);
  }

  @Override
  public void postRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
    requirePermission("deleteSnapshot", Permission.Action.ADMIN);
  }

  @Override
  public void postDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
  }

  /* ---- RegionObserver implementation ---- */

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    RegionCoprocessorEnvironment env = e.getEnvironment();
    final HRegion region = env.getRegion();
    if (region == null) {
      LOG.error("NULL region from RegionCoprocessorEnvironment in preOpen()");
      return;
    } else {
      HRegionInfo regionInfo = region.getRegionInfo();
      if (isSpecialTable(regionInfo)) {
        isSystemOrSuperUser(regionEnv.getConfiguration());
      } else {
        requirePermission("open", Action.ADMIN);
      }
    }
  }

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    RegionCoprocessorEnvironment env = c.getEnvironment();
    final HRegion region = env.getRegion();
    if (region == null) {
      LOG.error("NULL region from RegionCoprocessorEnvironment in postOpen()");
      return;
    }
    if (AccessControlLists.isAclRegion(region)) {
      aclRegion = true;
      try {
        initialize(env);
      } catch (IOException ex) {
        // if we can't obtain permissions, it's better to fail
        // than perform checks incorrectly
        throw new RuntimeException("Failed to initialize permissions cache", ex);
      }
    }
  }

  @Override
  public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    requirePermission("flush", getTableName(e.getEnvironment()), null, null, Action.ADMIN,
        Action.CREATE);
  }

  @Override
  public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    requirePermission("split", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
      final Store store, final InternalScanner scanner) throws IOException {
    requirePermission("compact", getTableName(e.getEnvironment()), null, null, Action.ADMIN,
        Action.CREATE);
    return scanner;
  }

  @Override
  public void preCompactSelection(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Store store, final List<StoreFile> candidates) throws IOException {
    requirePermission("compactSelection", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
  }

  @Override
  public void preGetClosestRowBefore(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final Result result)
      throws IOException {
    requirePermission("getClosestRowBefore", TablePermission.Action.READ, c.getEnvironment(),
        (family != null ? Lists.newArrayList(family) : null));
  }

  @Override
  public void preGet(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Get get, final List<KeyValue> result) throws IOException {
    /*
     if column family level checks fail, check for a qualifier level permission
     in one of the families.  If it is present, then continue with the AccessControlFilter.
      */
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User requestUser = getActiveUser();
    AuthResult authResult = permissionGranted("get", requestUser,
        TablePermission.Action.READ, e, get.getFamilyMap());
    if (!authResult.isAllowed()) {
      if (hasFamilyQualifierPermission(requestUser,
          TablePermission.Action.READ, e, get.getFamilyMap())) {
        byte[] table = getTableName(e);
        AccessControlFilter filter = new AccessControlFilter(authManager,
            requestUser, table);

        // wrap any existing filter
        if (get.getFilter() != null) {
          FilterList wrapper = new FilterList(FilterList.Operator.MUST_PASS_ALL,
              Lists.newArrayList(filter, get.getFilter()));
          get.setFilter(wrapper);
        } else {
          get.setFilter(filter);
        }
        logResult(AuthResult.allow("get", "Access allowed with filter", requestUser,
            TablePermission.Action.READ, authResult.table));
      } else {
        logResult(authResult);
        throw new AccessDeniedException("Insufficient permissions (table=" +
          e.getRegion().getTableDesc().getNameAsString() + ", action=READ)");
      }
    } else {
      // log auth success
      logResult(authResult);
    }
  }

  @Override
  public boolean preExists(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Get get, final boolean exists) throws IOException {
    requirePermission("exists", TablePermission.Action.READ, c.getEnvironment(),
        get.familySet());
    return exists;
  }

  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit, final boolean writeToWAL)
      throws IOException {
    requirePermission("put", TablePermission.Action.WRITE, c.getEnvironment(),
        put.getFamilyMap());
  }

  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit, final boolean writeToWAL) {
    if (aclRegion) {
      updateACL(c.getEnvironment(), put.getFamilyMap());
    }
  }

  @Override
  public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Delete delete, final WALEdit edit, final boolean writeToWAL)
      throws IOException {
    requirePermission("delete", TablePermission.Action.WRITE, c.getEnvironment(),
        delete.getFamilyMap());
  }

  @Override
  public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Delete delete, final WALEdit edit, final boolean writeToWAL)
      throws IOException {
    if (aclRegion) {
      updateACL(c.getEnvironment(), delete.getFamilyMap());
    }
  }

  @Override
  public boolean preCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp,
      final WritableByteArrayComparable comparator, final Put put,
      final boolean result) throws IOException {
    Collection<byte[]> familyMap = Arrays.asList(new byte[][]{family});
    requirePermission("checkAndPut", TablePermission.Action.READ, c.getEnvironment(), familyMap);
    requirePermission("checkAndPut", TablePermission.Action.WRITE, c.getEnvironment(), familyMap);
    return result;
  }

  @Override
  public boolean preCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp,
      final WritableByteArrayComparable comparator, final Delete delete,
      final boolean result) throws IOException {
    Collection<byte[]> familyMap = Arrays.asList(new byte[][]{family});
    requirePermission("checkAndDelete", TablePermission.Action.READ, c.getEnvironment(), familyMap);
    requirePermission("checkAndDelete", TablePermission.Action.WRITE, c.getEnvironment(), familyMap);
    return result;
  }

  @Override
  public long preIncrementColumnValue(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL)
      throws IOException {
    requirePermission("incrementColumnValue", TablePermission.Action.WRITE, c.getEnvironment(),
        Arrays.asList(new byte[][]{family}));
    return -1;
  }

  @Override
  public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
      throws IOException {
    requirePermission("append", TablePermission.Action.WRITE, c.getEnvironment(), append.getFamilyMap());
    return null;
  }

  @Override
  public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment)
      throws IOException {
    requirePermission("increment", TablePermission.Action.WRITE, c.getEnvironment(),
        increment.getFamilyMap().keySet());
    return null;
  }

  @Override
  public RegionScanner preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s) throws IOException {
    /*
     if column family level checks fail, check for a qualifier level permission
     in one of the families.  If it is present, then continue with the AccessControlFilter.
      */
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    AuthResult authResult = permissionGranted("scannerOpen", user, TablePermission.Action.READ, e,
        scan.getFamilyMap());
    if (!authResult.isAllowed()) {
      if (hasFamilyQualifierPermission(user, TablePermission.Action.READ, e,
          scan.getFamilyMap())) {
        byte[] table = getTableName(e);
        AccessControlFilter filter = new AccessControlFilter(authManager,
            user, table);

        // wrap any existing filter
        if (scan.hasFilter()) {
          FilterList wrapper = new FilterList(FilterList.Operator.MUST_PASS_ALL,
              Lists.newArrayList(filter, scan.getFilter()));
          scan.setFilter(wrapper);
        } else {
          scan.setFilter(filter);
        }
        logResult(AuthResult.allow("scannerOpen", "Access allowed with filter", user,
            TablePermission.Action.READ, authResult.table));
      } else {
        // no table/family level perms and no qualifier level perms, reject
        logResult(authResult);
        throw new AccessDeniedException("Insufficient permissions for user '"+
            (user != null ? user.getShortName() : "null")+"' "+
            "for scanner open on table " + Bytes.toString(getTableName(e)));
      }
    } else {
      // log success
      logResult(authResult);
    }
    return s;
  }

  @Override
  public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s) throws IOException {
    User user = getActiveUser();
    if (user != null && user.getShortName() != null) {      // store reference to scanner owner for later checks
      scannerOwners.put(s, user.getShortName());
    }
    return s;
  }

  @Override
  public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s, final List<Result> result,
      final int limit, final boolean hasNext) throws IOException {
    requireScannerOwner(s);
    return hasNext;
  }

  @Override
  public void preScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s) throws IOException {
    requireScannerOwner(s);
  }

  @Override
  public void postScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s) throws IOException {
    // clean up any associated owner mapping
    scannerOwners.remove(s);
  }

  /**
   * Verify, when servicing an RPC, that the caller is the scanner owner.
   * If so, we assume that access control is correctly enforced based on
   * the checks performed in preScannerOpen()
   */
  private void requireScannerOwner(InternalScanner s)
      throws AccessDeniedException {
    if (RequestContext.isInRequestContext()) {
      String requestUserName = RequestContext.getRequestUserName();
      String owner = scannerOwners.get(s);
      if (owner != null && !owner.equals(requestUserName)) {
        throw new AccessDeniedException("User '"+ requestUserName +"' is not the scanner owner!");
      }
    }
  }

  /**
   * Verifies user has WRITE privileges on
   * the Column Families involved in the bulkLoadHFile
   * request. Specific Column Write privileges are presently
   * ignored.
   */
  @Override
  public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
      List<Pair<byte[], String>> familyPaths) throws IOException {
    List<byte[]> cfs = new LinkedList<byte[]>();
    for(Pair<byte[],String> el : familyPaths) {
      cfs.add(el.getFirst());
    }
    requirePermission("bulkLoadHFile", Permission.Action.CREATE, ctx.getEnvironment(), cfs);
  }

  private AuthResult hasSomeAccess(RegionCoprocessorEnvironment e, String request, Action action) throws IOException {
    User requestUser = getActiveUser();
    final byte[] tableName = e.getRegion().getTableDesc().getName();
    AuthResult authResult = permissionGranted(request, requestUser,
        action, e, Collections.EMPTY_MAP);
    if (!authResult.isAllowed()) {
      final Configuration conf = e.getConfiguration();
      // hasSomeAccess is called from bulkload pre hooks
      List<UserPermission> perms =
        User.runAsLoginUser(new PrivilegedExceptionAction<List<UserPermission>>() {
          @Override
          public List<UserPermission> run() throws Exception {
            return AccessControlLists.getUserPermissions(conf, tableName);
          }
        });
      for (UserPermission userPerm: perms) {
        for (Action userAction: userPerm.getActions()) {
          if (userAction.equals(action)) {
            return AuthResult.allow(request, "Access allowed", requestUser,
              action, tableName);
          }
        }
      }
    }
    return authResult;
  }

  /**
   * Authorization check for
   * SecureBulkLoadProtocol.prepareBulkLoad()
   * @param e
   * @throws IOException
   */
  public void prePrepareBulkLoad(RegionCoprocessorEnvironment e) throws IOException {
    AuthResult authResult = hasSomeAccess(e, "prepareBulkLoad", Action.WRITE);
    logResult(authResult);
    if (!authResult.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions (table=" +
        e.getRegion().getTableDesc().getNameAsString() + ", action=WRITE)");
    }
  }

  /**
   * Authorization security check for
   * SecureBulkLoadProtocol.cleanupBulkLoad()
   * @param e
   * @throws IOException
   */
  //TODO this should end up as a coprocessor hook
  public void preCleanupBulkLoad(RegionCoprocessorEnvironment e) throws IOException {
    AuthResult authResult = hasSomeAccess(e, "cleanupBulkLoad", Action.WRITE);
    logResult(authResult);
    if (!authResult.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions (table=" +
        e.getRegion().getTableDesc().getNameAsString() + ", action=WRITE)");
    }
  }

  /* ---- AccessControllerProtocol implementation ---- */
  /*
   * These methods are only allowed to be called against the _acl_ region(s).
   * This will be restricted by both client side and endpoint implementations.
   */
  @Override
  public void grant(final UserPermission perm) throws IOException {
    // verify it's only running at .acl.
    if (aclRegion) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received request to grant access permission " + perm.toString());
      }

      requirePermission("grant", perm.getTable(), perm.getFamily(), perm.getQualifier(), Action.ADMIN);

      User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          AccessControlLists.addUserPermission(regionEnv.getConfiguration(), perm);
          return null;
        }
      });

      if (AUDITLOG.isTraceEnabled()) {
        // audit log should store permission changes in addition to auth results
        AUDITLOG.trace("Granted permission " + perm.toString());
      }
    } else {
      throw new CoprocessorException(AccessController.class, "This method "
          + "can only execute at " + Bytes.toString(AccessControlLists.ACL_TABLE_NAME) + " table.");
    }
  }

  @Override
  @Deprecated
  public void grant(byte[] user, TablePermission permission)
      throws IOException {
    grant(new UserPermission(user, permission.getTable(),
            permission.getFamily(), permission.getQualifier(),
            permission.getActions()));
  }

  @Override
  public void revoke(final UserPermission perm) throws IOException {
    // only allowed to be called on _acl_ region
    if (aclRegion) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received request to revoke access permission " + perm.toString());
      }

      requirePermission("revoke", perm.getTable(), perm.getFamily(),
                        perm.getQualifier(), Action.ADMIN);

      User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          AccessControlLists.removeUserPermission(regionEnv.getConfiguration(), perm);
          return null;
        }
      });

      if (AUDITLOG.isTraceEnabled()) {
        // audit log should record all permission changes
        AUDITLOG.trace("Revoked permission " + perm.toString());
      }
    } else {
      throw new CoprocessorException(AccessController.class, "This method "
          + "can only execute at " + Bytes.toString(AccessControlLists.ACL_TABLE_NAME) + " table.");
    }
  }

  @Override
  @Deprecated
  public void revoke(byte[] user, TablePermission permission)
      throws IOException {
    revoke(new UserPermission(user, permission.getTable(),
            permission.getFamily(), permission.getQualifier(),
            permission.getActions()));
  }

  @Override
  public List<UserPermission> getUserPermissions(final byte[] tableName) throws IOException {
    // only allowed to be called on _acl_ region
    if (aclRegion) {
      requirePermission("userPermissions", tableName, null, null, Action.ADMIN);
      return User.runAsLoginUser(new PrivilegedExceptionAction<List<UserPermission>>() {
        @Override
        public List<UserPermission> run() throws Exception {
          return AccessControlLists.getUserPermissions(regionEnv.getConfiguration(), tableName);
        }
      });
    } else {
      throw new CoprocessorException(AccessController.class, "This method "
          + "can only execute at " + Bytes.toString(AccessControlLists.ACL_TABLE_NAME) + " table.");
    }
  }

  @Override
  public void checkPermissions(Permission[] permissions) throws IOException {
    byte[] tableName = regionEnv.getRegion().getTableDesc().getName();
    for (Permission permission : permissions) {
      if (permission instanceof TablePermission) {
        TablePermission tperm = (TablePermission) permission;
        for (Permission.Action action : permission.getActions()) {
          if (!Arrays.equals(tperm.getTable(), tableName)) {
            throw new CoprocessorException(AccessController.class, String.format("This method "
                + "can only execute at the table specified in TablePermission. " +
                "Table of the region:%s , requested table:%s", Bytes.toString(tableName),
                Bytes.toString(tperm.getTable())));
          }

          HashMap<byte[], Set<byte[]>> familyMap = Maps.newHashMapWithExpectedSize(1);
          if (tperm.getFamily() != null) {
            if (tperm.getQualifier() != null) {
              familyMap.put(tperm.getFamily(), Sets.newHashSet(tperm.getQualifier()));
            } else {
              familyMap.put(tperm.getFamily(), null);
            }
          }

          requirePermission("checkPermissions", action, regionEnv, familyMap);
        }

      } else {
        for (Permission.Action action : permission.getActions()) {
          requirePermission("checkPermissions", action);
        }
      }
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return PROTOCOL_VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    if (AccessControllerProtocol.class.getName().equals(protocol)) {
      return new ProtocolSignature(PROTOCOL_VERSION, null);
    }
    throw new HBaseRPC.UnknownProtocolException(
        "Unexpected protocol requested: "+protocol);
  }

  private byte[] getTableName(RegionCoprocessorEnvironment e) {
    HRegion region = e.getRegion();
    byte[] tableName = null;

    if (region != null) {
      HRegionInfo regionInfo = region.getRegionInfo();
      if (regionInfo != null) {
        tableName = regionInfo.getTableName();
      }
    }
    return tableName;
  }


  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested)
      throws IOException {
    requirePermission("close", Permission.Action.ADMIN);
  }

  @Override
  public void preLockRow(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] regionName,
      byte[] row) throws IOException {
    requirePermission("lockRow", getTableName(ctx.getEnvironment()), null, null,
      Permission.Action.WRITE, Permission.Action.CREATE);
  }

  @Override
  public void preUnlockRow(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] regionName,
      long lockId) throws IOException {
    requirePermission("unlockRow", getTableName(ctx.getEnvironment()), null, null,
      Permission.Action.WRITE, Permission.Action.CREATE);
  }

  private void isSystemOrSuperUser(Configuration conf) throws IOException {
    User user = userProvider.getCurrent();
    if (user == null) {
      throw new IOException("Unable to obtain the current user, "
          + "authorization checks for internal operations will not work correctly!");
    }

    String currentUser = user.getShortName();
    List<String> superusers = Lists.asList(currentUser,
      conf.getStrings(AccessControlLists.SUPERUSER_CONF_KEY, new String[0]));

    User activeUser = getActiveUser();
    if (!(superusers.contains(activeUser.getShortName()))) {
      throw new AccessDeniedException("User '" + (user != null ? user.getShortName() : "null")
          + "is not system or super user.");
    }
  }

  private boolean isSpecialTable(HRegionInfo regionInfo) {
    byte[] tableName = regionInfo.getTableName();
    return tableName.equals(AccessControlLists.ACL_TABLE_NAME)
      || tableName.equals(Bytes.toBytes("-ROOT-"))
      || tableName.equals(Bytes.toBytes(".META."));
  }

  @Override
  public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env)
      throws IOException {
    requirePermission("stop", Permission.Action.ADMIN);
  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<String> tableNamesList, List<HTableDescriptor> descriptors) throws IOException {
    // If the list is empty, this is a request for all table descriptors and requires GLOBAL
    // ADMIN privs.
    if (tableNamesList == null || tableNamesList.isEmpty()) {
      requirePermission("getTableDescriptors", Permission.Action.ADMIN);
    }
    // Otherwise, if the requestor has ADMIN or CREATE privs for all listed tables, the
    // request can be granted.
    else {
      MasterServices masterServices = ctx.getEnvironment().getMasterServices();
      for (String tableName: tableNamesList) {
        // Do not deny if the table does not exist
        byte[] nameAsBytes = Bytes.toBytes(tableName);
        try {
          masterServices.checkTableModifiable(nameAsBytes);
        } catch (TableNotFoundException ex) {
          // Skip checks for a table that does not exist
          continue;
        } catch (TableNotDisabledException ex) {
          // We don't care about this
        }
        requirePermission("getTableDescriptors", nameAsBytes, null, null,
          Permission.Action.ADMIN, Permission.Action.CREATE);
      }
    }
  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors) throws IOException {
  }
}
