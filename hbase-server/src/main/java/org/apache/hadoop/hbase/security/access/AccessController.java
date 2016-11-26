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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseMasterAndRegionObserver;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * Provides basic authorization checks for data access and administrative
 * operations.
 *
 * <p>
 * {@code AccessController} performs authorization checks for HBase operations
 * based on:
 * </p>
 * <ul>
 *   <li>the identity of the user performing the operation</li>
 *   <li>the scope over which the operation is performed, in increasing
 *   specificity: global, table, column family, or qualifier</li>
 *   <li>the type of action being performed (as mapped to
 *   {@link Permission.Action} values)</li>
 * </ul>
 * <p>
 * If the authorization check fails, an {@link AccessDeniedException}
 * will be thrown for the operation.
 * </p>
 *
 * <p>
 * To perform authorization checks, {@code AccessController} relies on the
 * RpcServerEngine being loaded to provide
 * the user identities for remote requests.
 * </p>
 *
 * <p>
 * The access control lists used for authorization can be manipulated via the
 * exposed {@link AccessControlService} Interface implementation, and the associated
 * {@code grant}, {@code revoke}, and {@code user_permission} HBase shell
 * commands.
 * </p>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class AccessController extends BaseMasterAndRegionObserver
    implements RegionServerObserver,
      AccessControlService.Interface, CoprocessorService, EndpointObserver, BulkLoadObserver {

  private static final Log LOG = LogFactory.getLog(AccessController.class);

  private static final Log AUDITLOG =
    LogFactory.getLog("SecurityLogger."+AccessController.class.getName());
  private static final String CHECK_COVERING_PERM = "check_covering_perm";
  private static final String TAG_CHECK_PASSED = "tag_check_passed";
  private static final byte[] TRUE = Bytes.toBytes(true);

  TableAuthManager authManager = null;

  /** flags if we are running on a region of the _acl_ table */
  boolean aclRegion = false;

  /** defined only for Endpoint implementation, so it can have way to
   access region services */
  private RegionCoprocessorEnvironment regionEnv;

  /** Mapping of scanner instances to the user who created them */
  private Map<InternalScanner,String> scannerOwners =
      new MapMaker().weakKeys().makeMap();

  private Map<TableName, List<UserPermission>> tableAcls;

  /** Provider for mapping principal names to Users */
  private UserProvider userProvider;

  /** if we are active, usually true, only not true if "hbase.security.authorization"
   has been set to false in site configuration */
  boolean authorizationEnabled;

  /** if we are able to support cell ACLs */
  boolean cellFeaturesEnabled;

  /** if we should check EXEC permissions */
  boolean shouldCheckExecPermission;

  /** if we should terminate access checks early as soon as table or CF grants
    allow access; pre-0.98 compatible behavior */
  boolean compatibleEarlyTermination;

  /** if we have been successfully initialized */
  private volatile boolean initialized = false;

  /** if the ACL table is available, only relevant in the master */
  private volatile boolean aclTabAvailable = false;

  public static boolean isAuthorizationSupported(Configuration conf) {
    return conf.getBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, true);
  }

  public static boolean isCellAuthorizationSupported(Configuration conf) {
    return isAuthorizationSupported(conf) &&
        (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS);
  }

  public Region getRegion() {
    return regionEnv != null ? regionEnv.getRegion() : null;
  }

  public TableAuthManager getAuthManager() {
    return authManager;
  }

  void initialize(RegionCoprocessorEnvironment e) throws IOException {
    final Region region = e.getRegion();
    Configuration conf = e.getConfiguration();
    Map<byte[], ListMultimap<String,TablePermission>> tables =
        AccessControlLists.loadAll(region);
    // For each table, write out the table's permissions to the respective
    // znode for that table.
    for (Map.Entry<byte[], ListMultimap<String,TablePermission>> t:
      tables.entrySet()) {
      byte[] entry = t.getKey();
      ListMultimap<String,TablePermission> perms = t.getValue();
      byte[] serialized = AccessControlLists.writePermissionsAsBytes(perms, conf);
      this.authManager.getZKPermissionWatcher().writeToZookeeper(entry, serialized);
    }
    initialized = true;
  }

  /**
   * Writes all table ACLs for the tables in the given Map up into ZooKeeper
   * znodes.  This is called to synchronize ACL changes following {@code _acl_}
   * table updates.
   */
  void updateACL(RegionCoprocessorEnvironment e,
      final Map<byte[], List<Cell>> familyMap) {
    Set<byte[]> entries =
        new TreeSet<byte[]>(Bytes.BYTES_RAWCOMPARATOR);
    for (Map.Entry<byte[], List<Cell>> f : familyMap.entrySet()) {
      List<Cell> cells = f.getValue();
      for (Cell cell: cells) {
        if (CellUtil.matchingFamily(cell, AccessControlLists.ACL_LIST_FAMILY)) {
          entries.add(CellUtil.cloneRow(cell));
        }
      }
    }
    ZKPermissionWatcher zkw = this.authManager.getZKPermissionWatcher();
    Configuration conf = regionEnv.getConfiguration();
    for (byte[] entry: entries) {
      try {
        try (Table t = regionEnv.getTable(AccessControlLists.ACL_TABLE_NAME)) {
          ListMultimap<String,TablePermission> perms =
              AccessControlLists.getPermissions(conf, entry, t);
          byte[] serialized = AccessControlLists.writePermissionsAsBytes(perms, conf);
          zkw.writeToZookeeper(entry, serialized);
        }
      } catch (IOException ex) {
        LOG.error("Failed updating permissions mirror for '" + Bytes.toString(entry) + "'",
            ex);
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
   * @return an authorization result
   */
  AuthResult permissionGranted(String request, User user, Action permRequest,
      RegionCoprocessorEnvironment e,
      Map<byte [], ? extends Collection<?>> families) {
    HRegionInfo hri = e.getRegion().getRegionInfo();
    TableName tableName = hri.getTable();

    // 1. All users need read access to hbase:meta table.
    // this is a very common operation, so deal with it quickly.
    if (hri.isMetaRegion()) {
      if (permRequest == Action.READ) {
        return AuthResult.allow(request, "All users allowed", user,
          permRequest, tableName, families);
      }
    }

    if (user == null) {
      return AuthResult.deny(request, "No user associated with request!", null,
        permRequest, tableName, families);
    }

    // 2. check for the table-level, if successful we can short-circuit
    if (authManager.authorize(user, tableName, (byte[])null, permRequest)) {
      return AuthResult.allow(request, "Table permission granted", user,
        permRequest, tableName, families);
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
                    permRequest, tableName, makeFamilyMap(family.getKey(), qualifier));
              }
            }
          } else if (family.getValue() instanceof List) { // List<KeyValue>
            List<KeyValue> kvList = (List<KeyValue>)family.getValue();
            for (KeyValue kv : kvList) {
              if (!authManager.authorize(user, tableName, family.getKey(),
                CellUtil.cloneQualifier(kv), permRequest)) {
                return AuthResult.deny(request, "Failed qualifier check", user, permRequest,
                  tableName, makeFamilyMap(family.getKey(), CellUtil.cloneQualifier(kv)));
              }
            }
          }
        } else {
          // no qualifiers and family-level check already failed
          return AuthResult.deny(request, "Failed family check", user, permRequest,
              tableName, makeFamilyMap(family.getKey(), null));
        }
      }

      // all family checks passed
      return AuthResult.allow(request, "All family checks passed", user, permRequest,
          tableName, families);
    }

    // 4. no families to check and table level access failed
    return AuthResult.deny(request, "No families to check and table permission failed",
        user, permRequest, tableName, families);
  }

  /**
   * Check the current user for authorization to perform a specific action
   * against the given set of row data.
   * @param opType the operation type
   * @param user the user
   * @param e the coprocessor environment
   * @param families the map of column families to qualifiers present in
   * the request
   * @param actions the desired actions
   * @return an authorization result
   */
  AuthResult permissionGranted(OpType opType, User user, RegionCoprocessorEnvironment e,
      Map<byte [], ? extends Collection<?>> families, Action... actions) {
    AuthResult result = null;
    for (Action action: actions) {
      result = permissionGranted(opType.toString(), user, action, e, families);
      if (!result.isAllowed()) {
        return result;
      }
    }
    return result;
  }

  private void logResult(AuthResult result) {
    if (AUDITLOG.isTraceEnabled()) {
      InetAddress remoteAddr = RpcServer.getRemoteAddress();
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
  private User getActiveUser(ObserverContext ctx) throws IOException {
    User user = ctx.getCaller();
    if (user == null) {
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
  private void requirePermission(User user, String request, TableName tableName, byte[] family,
      byte[] qualifier, Action... permissions) throws IOException {
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
    if (authorizationEnabled && !result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has any of the given permissions for the
   * given table, column family and column qualifier.
   * @param tableName Table requested
   * @param family Column family param
   * @param qualifier Column qualifier param
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if user has no authorization
   */
  private void requireTablePermission(User user, String request, TableName tableName, byte[] family,
      byte[] qualifier, Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorize(user, tableName, null, null, permission)) {
        result = AuthResult.allow(request, "Table permission granted", user,
            permission, tableName, null, null);
        result.getParams().setFamily(family).setQualifier(qualifier);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
            permission, tableName, family, qualifier);
        result.getParams().setFamily(family).setQualifier(qualifier);
      }
    }
    logResult(result);
    if (authorizationEnabled && !result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has any of the given permissions to access the table.
   *
   * @param tableName Table requested
   * @param permissions Actions being requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if user has no authorization
   */
  private void requireAccess(User user, String request, TableName tableName,
      Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.hasAccess(user, tableName, permission)) {
        result = AuthResult.allow(request, "Table permission granted", user,
                                  permission, tableName, null, null);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
                                 permission, tableName, null, null);
      }
    }
    logResult(result);
    if (authorizationEnabled && !result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has global privileges for the given action.
   * @param perm The action being requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if authorization is denied
   */
  private void requirePermission(User user, String request, Action perm) throws IOException {
    requireGlobalPermission(user, request, perm, null, null);
  }

  /**
   * Checks that the user has the given global permission. The generated
   * audit log message will contain context information for the operation
   * being authorized, based on the given parameters.
   * @param perm Action being requested
   * @param tableName Affected table name.
   * @param familyMap Affected column families.
   */
  private void requireGlobalPermission(User user, String request, Action perm, TableName tableName,
      Map<byte[], ? extends Collection<byte[]>> familyMap) throws IOException {
    AuthResult result = null;
    if (authManager.authorize(user, perm)) {
      result = AuthResult.allow(request, "Global check allowed", user, perm, tableName, familyMap);
      result.getParams().setTableName(tableName).setFamilies(familyMap);
      logResult(result);
    } else {
      result = AuthResult.deny(request, "Global check failed", user, perm, tableName, familyMap);
      result.getParams().setTableName(tableName).setFamilies(familyMap);
      logResult(result);
      if (authorizationEnabled) {
        throw new AccessDeniedException("Insufficient permissions for user '" +
          (user != null ? user.getShortName() : "null") +"' (global, action=" +
          perm.toString() + ")");
      }
    }
  }

  /**
   * Checks that the user has the given global permission. The generated
   * audit log message will contain context information for the operation
   * being authorized, based on the given parameters.
   * @param perm Action being requested
   * @param namespace
   */
  private void requireGlobalPermission(User user, String request, Action perm,
                                       String namespace) throws IOException {
    AuthResult authResult = null;
    if (authManager.authorize(user, perm)) {
      authResult = AuthResult.allow(request, "Global check allowed", user, perm, null);
      authResult.getParams().setNamespace(namespace);
      logResult(authResult);
    } else {
      authResult = AuthResult.deny(request, "Global check failed", user, perm, null);
      authResult.getParams().setNamespace(namespace);
      logResult(authResult);
      if (authorizationEnabled) {
        throw new AccessDeniedException("Insufficient permissions for user '" +
          (user != null ? user.getShortName() : "null") +"' (global, action=" +
          perm.toString() + ")");
      }
    }
  }

  /**
   * Checks that the user has the given global or namespace permission.
   * @param namespace
   * @param permissions Actions being requested
   */
  public void requireNamespacePermission(User user, String request, String namespace,
      Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorize(user, namespace, permission)) {
        result = AuthResult.allow(request, "Namespace permission granted",
            user, permission, namespace);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
            permission, namespace);
      }
    }
    logResult(result);
    if (authorizationEnabled && !result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions "
          + result.toContextString());
    }
  }

  /**
   * Checks that the user has the given global or namespace permission.
   * @param namespace
   * @param permissions Actions being requested
   */
  public void requireNamespacePermission(User user, String request, String namespace,
      TableName tableName, Map<byte[], ? extends Collection<byte[]>> familyMap,
      Action... permissions)
      throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorize(user, namespace, permission)) {
        result = AuthResult.allow(request, "Namespace permission granted",
            user, permission, namespace);
        result.getParams().setTableName(tableName).setFamilies(familyMap);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
            permission, namespace);
        result.getParams().setTableName(tableName).setFamilies(familyMap);
      }
    }
    logResult(result);
    if (authorizationEnabled && !result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions "
          + result.toContextString());
    }
  }

  /**
   * Returns <code>true</code> if the current user is allowed the given action
   * over at least one of the column qualifiers in the given column families.
   */
  private boolean hasFamilyQualifierPermission(User user,
      Action perm,
      RegionCoprocessorEnvironment env,
      Map<byte[], ? extends Collection<byte[]>> familyMap)
    throws IOException {
    HRegionInfo hri = env.getRegion().getRegionInfo();
    TableName tableName = hri.getTable();

    if (user == null) {
      return false;
    }

    if (familyMap != null && familyMap.size() > 0) {
      // at least one family must be allowed
      for (Map.Entry<byte[], ? extends Collection<byte[]>> family :
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

  private enum OpType {
    GET("get"),
    EXISTS("exists"),
    SCAN("scan"),
    PUT("put"),
    DELETE("delete"),
    CHECK_AND_PUT("checkAndPut"),
    CHECK_AND_DELETE("checkAndDelete"),
    INCREMENT_COLUMN_VALUE("incrementColumnValue"),
    APPEND("append"),
    INCREMENT("increment");

    private String type;

    private OpType(String type) {
      this.type = type;
    }

    @Override
    public String toString() {
      return type;
    }
  }

  /**
   * Determine if cell ACLs covered by the operation grant access. This is expensive.
   * @return false if cell ACLs failed to grant access, true otherwise
   * @throws IOException
   */
  private boolean checkCoveringPermission(User user, OpType request, RegionCoprocessorEnvironment e,
      byte[] row, Map<byte[], ? extends Collection<?>> familyMap, long opTs, Action... actions)
      throws IOException {
    if (!cellFeaturesEnabled) {
      return false;
    }
    long cellGrants = 0;
    long latestCellTs = 0;
    Get get = new Get(row);
    // Only in case of Put/Delete op, consider TS within cell (if set for individual cells).
    // When every cell, within a Mutation, can be linked with diff TS we can not rely on only one
    // version. We have to get every cell version and check its TS against the TS asked for in
    // Mutation and skip those Cells which is outside this Mutation TS.In case of Put, we have to
    // consider only one such passing cell. In case of Delete we have to consider all the cell
    // versions under this passing version. When Delete Mutation contains columns which are a
    // version delete just consider only one version for those column cells.
    boolean considerCellTs  = (request == OpType.PUT || request == OpType.DELETE);
    if (considerCellTs) {
      get.setMaxVersions();
    } else {
      get.setMaxVersions(1);
    }
    boolean diffCellTsFromOpTs = false;
    for (Map.Entry<byte[], ? extends Collection<?>> entry: familyMap.entrySet()) {
      byte[] col = entry.getKey();
      // TODO: HBASE-7114 could possibly unify the collection type in family
      // maps so we would not need to do this
      if (entry.getValue() instanceof Set) {
        Set<byte[]> set = (Set<byte[]>)entry.getValue();
        if (set == null || set.isEmpty()) {
          get.addFamily(col);
        } else {
          for (byte[] qual: set) {
            get.addColumn(col, qual);
          }
        }
      } else if (entry.getValue() instanceof List) {
        List<Cell> list = (List<Cell>)entry.getValue();
        if (list == null || list.isEmpty()) {
          get.addFamily(col);
        } else {
          // In case of family delete, a Cell will be added into the list with Qualifier as null.
          for (Cell cell : list) {
            if (cell.getQualifierLength() == 0
                && (cell.getTypeByte() == Type.DeleteFamily.getCode()
                || cell.getTypeByte() == Type.DeleteFamilyVersion.getCode())) {
              get.addFamily(col);
            } else {
              get.addColumn(col, CellUtil.cloneQualifier(cell));
            }
            if (considerCellTs) {
              long cellTs = cell.getTimestamp();
              latestCellTs = Math.max(latestCellTs, cellTs);
              diffCellTsFromOpTs = diffCellTsFromOpTs || (opTs != cellTs);
            }
          }
        }
      } else if (entry.getValue() == null) {
        get.addFamily(col);
      } else {
        throw new RuntimeException("Unhandled collection type " +
          entry.getValue().getClass().getName());
      }
    }
    // We want to avoid looking into the future. So, if the cells of the
    // operation specify a timestamp, or the operation itself specifies a
    // timestamp, then we use the maximum ts found. Otherwise, we bound
    // the Get to the current server time. We add 1 to the timerange since
    // the upper bound of a timerange is exclusive yet we need to examine
    // any cells found there inclusively.
    long latestTs = Math.max(opTs, latestCellTs);
    if (latestTs == 0 || latestTs == HConstants.LATEST_TIMESTAMP) {
      latestTs = EnvironmentEdgeManager.currentTime();
    }
    get.setTimeRange(0, latestTs + 1);
    // In case of Put operation we set to read all versions. This was done to consider the case
    // where columns are added with TS other than the Mutation TS. But normally this wont be the
    // case with Put. There no need to get all versions but get latest version only.
    if (!diffCellTsFromOpTs && request == OpType.PUT) {
      get.setMaxVersions(1);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Scanning for cells with " + get);
    }
    // This Map is identical to familyMap. The key is a BR rather than byte[].
    // It will be easy to do gets over this new Map as we can create get keys over the Cell cf by
    // new SimpleByteRange(cell.familyArray, cell.familyOffset, cell.familyLen)
    Map<ByteRange, List<Cell>> familyMap1 = new HashMap<ByteRange, List<Cell>>();
    for (Entry<byte[], ? extends Collection<?>> entry : familyMap.entrySet()) {
      if (entry.getValue() instanceof List) {
        familyMap1.put(new SimpleMutableByteRange(entry.getKey()), (List<Cell>) entry.getValue());
      }
    }
    RegionScanner scanner = getRegion(e).getScanner(new Scan(get));
    List<Cell> cells = Lists.newArrayList();
    Cell prevCell = null;
    ByteRange curFam = new SimpleMutableByteRange();
    boolean curColAllVersions = (request == OpType.DELETE);
    long curColCheckTs = opTs;
    boolean foundColumn = false;
    try {
      boolean more = false;
      ScannerContext scannerContext = ScannerContext.newBuilder().setBatchLimit(1).build();

      do {
        cells.clear();
        // scan with limit as 1 to hold down memory use on wide rows
        more = scanner.next(cells, scannerContext);
        for (Cell cell: cells) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Found cell " + cell);
          }
          boolean colChange = prevCell == null || !CellUtil.matchingColumn(prevCell, cell);
          if (colChange) foundColumn = false;
          prevCell = cell;
          if (!curColAllVersions && foundColumn) {
            continue;
          }
          if (colChange && considerCellTs) {
            curFam.set(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            List<Cell> cols = familyMap1.get(curFam);
            for (Cell col : cols) {
              // null/empty qualifier is used to denote a Family delete. The TS and delete type
              // associated with this is applicable for all columns within the family. That is
              // why the below (col.getQualifierLength() == 0) check.
              if ((col.getQualifierLength() == 0 && request == OpType.DELETE)
                  || CellUtil.matchingQualifier(cell, col)) {
                byte type = col.getTypeByte();
                if (considerCellTs) {
                  curColCheckTs = col.getTimestamp();
                }
                // For a Delete op we pass allVersions as true. When a Delete Mutation contains
                // a version delete for a column no need to check all the covering cells within
                // that column. Check all versions when Type is DeleteColumn or DeleteFamily
                // One version delete types are Delete/DeleteFamilyVersion
                curColAllVersions = (KeyValue.Type.DeleteColumn.getCode() == type)
                    || (KeyValue.Type.DeleteFamily.getCode() == type);
                break;
              }
            }
          }
          if (cell.getTimestamp() > curColCheckTs) {
            // Just ignore this cell. This is not a covering cell.
            continue;
          }
          foundColumn = true;
          for (Action action: actions) {
            // Are there permissions for this user for the cell?
            if (!authManager.authorize(user, getTableName(e), cell, action)) {
              // We can stop if the cell ACL denies access
              return false;
            }
          }
          cellGrants++;
        }
      } while (more);
    } catch (AccessDeniedException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error("Exception while getting cells to calculate covering permission", ex);
    } finally {
      scanner.close();
    }
    // We should not authorize unless we have found one or more cell ACLs that
    // grant access. This code is used to check for additional permissions
    // after no table or CF grants are found.
    return cellGrants > 0;
  }

  private static void addCellPermissions(final byte[] perms, Map<byte[], List<Cell>> familyMap) {
    // Iterate over the entries in the familyMap, replacing the cells therein
    // with new cells including the ACL data
    for (Map.Entry<byte[], List<Cell>> e: familyMap.entrySet()) {
      List<Cell> newCells = Lists.newArrayList();
      for (Cell cell: e.getValue()) {
        // Prepend the supplied perms in a new ACL tag to an update list of tags for the cell
        List<Tag> tags = new ArrayList<Tag>();
        tags.add(new ArrayBackedTag(AccessControlLists.ACL_TAG_TYPE, perms));
        Iterator<Tag> tagIterator = CellUtil.tagsIterator(cell);
        while (tagIterator.hasNext()) {
          tags.add(tagIterator.next());
        }
        newCells.add(CellUtil.createCell(cell, tags));
      }
      // This is supposed to be safe, won't CME
      e.setValue(newCells);
    }
  }

  // Checks whether incoming cells contain any tag with type as ACL_TAG_TYPE. This tag
  // type is reserved and should not be explicitly set by user.
  private void checkForReservedTagPresence(User user, Mutation m) throws IOException {
    // No need to check if we're not going to throw
    if (!authorizationEnabled) {
      m.setAttribute(TAG_CHECK_PASSED, TRUE);
      return;
    }
    // Superusers are allowed to store cells unconditionally.
    if (Superusers.isSuperUser(user)) {
      m.setAttribute(TAG_CHECK_PASSED, TRUE);
      return;
    }
    // We already checked (prePut vs preBatchMutation)
    if (m.getAttribute(TAG_CHECK_PASSED) != null) {
      return;
    }
    for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
      Iterator<Tag> tagsItr = CellUtil.tagsIterator(cellScanner.current());
      while (tagsItr.hasNext()) {
        if (tagsItr.next().getType() == AccessControlLists.ACL_TAG_TYPE) {
          throw new AccessDeniedException("Mutation contains cell with reserved type tag");
        }
      }
    }
    m.setAttribute(TAG_CHECK_PASSED, TRUE);
  }

  /* ---- MasterObserver implementation ---- */
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    CompoundConfiguration conf = new CompoundConfiguration();
    conf.add(env.getConfiguration());

    authorizationEnabled = isAuthorizationSupported(conf);
    if (!authorizationEnabled) {
      LOG.warn("The AccessController has been loaded with authorization checks disabled.");
    }

    shouldCheckExecPermission = conf.getBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY,
      AccessControlConstants.DEFAULT_EXEC_PERMISSION_CHECKS);

    cellFeaturesEnabled = (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS);
    if (!cellFeaturesEnabled) {
      LOG.info("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS
          + " is required to persist cell ACLs. Consider setting " + HFile.FORMAT_VERSION_KEY
          + " accordingly.");
    }

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
      conf.addStringMap(regionEnv.getRegion().getTableDesc().getConfiguration());
      zk = regionEnv.getRegionServerServices().getZooKeeper();
      compatibleEarlyTermination = conf.getBoolean(AccessControlConstants.CF_ATTRIBUTE_EARLY_OUT,
        AccessControlConstants.DEFAULT_ATTRIBUTE_EARLY_OUT);
    }

    // set the user-provider.
    this.userProvider = UserProvider.instantiate(env.getConfiguration());

    // If zk is null or IOException while obtaining auth manager,
    // throw RuntimeException so that the coprocessor is unloaded.
    if (zk != null) {
      try {
        this.authManager = TableAuthManager.getOrCreate(zk, env.getConfiguration());
      } catch (IOException ioe) {
        throw new RuntimeException("Error obtaining TableAuthManager", ioe);
      }
    } else {
      throw new RuntimeException("Error obtaining TableAuthManager, zk found null.");
    }

    tableAcls = new MapMaker().weakValues().makeMap();
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (this.authManager != null) {
      TableAuthManager.release(authManager);
    }
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> c,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    Set<byte[]> families = desc.getFamiliesKeys();
    Map<byte[], Set<byte[]>> familyMap = new TreeMap<byte[], Set<byte[]>>(Bytes.BYTES_COMPARATOR);
    for (byte[] family: families) {
      familyMap.put(family, null);
    }
    requireNamespacePermission(getActiveUser(c), "createTable",
        desc.getTableName().getNamespaceAsString(), desc.getTableName(), familyMap, Action.CREATE);
  }

  @Override
  public void postCompletedCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> c,
      final HTableDescriptor desc,
      final HRegionInfo[] regions) throws IOException {
    // When AC is used, it should be configured as the 1st CP.
    // In Master, the table operations like create, are handled by a Thread pool but the max size
    // for this pool is 1. So if multiple CPs create tables on startup, these creations will happen
    // sequentially only.
    // Related code in HMaster#startServiceThreads
    // {code}
    //   // We depend on there being only one instance of this executor running
    //   // at a time. To do concurrency, would need fencing of enable/disable of
    //   // tables.
    //   this.service.startExecutorService(ExecutorType.MASTER_TABLE_OPERATIONS, 1);
    // {code}
    // In future if we change this pool to have more threads, then there is a chance for thread,
    // creating acl table, getting delayed and by that time another table creation got over and
    // this hook is getting called. In such a case, we will need a wait logic here which will
    // wait till the acl table is created.
    if (AccessControlLists.isAclTable(desc)) {
      this.aclTabAvailable = true;
    } else if (!(TableName.NAMESPACE_TABLE_NAME.equals(desc.getTableName()))) {
      if (!aclTabAvailable) {
        LOG.warn("Not adding owner permission for table " + desc.getTableName() + ". "
            + AccessControlLists.ACL_TABLE_NAME + " is not yet created. "
            + getClass().getSimpleName() + " should be configured as the first Coprocessor");
      } else {
        String owner = desc.getOwnerString();
        // default the table owner to current user, if not specified.
        if (owner == null)
          owner = getActiveUser(c).getShortName();
        final UserPermission userperm = new UserPermission(Bytes.toBytes(owner),
            desc.getTableName(), null, Action.values());
        // switch to the real hbase master user for doing the RPC on the ACL table
        User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            AccessControlLists.addUserPermission(c.getEnvironment().getConfiguration(),
                userperm, c.getEnvironment().getTable(AccessControlLists.ACL_TABLE_NAME));
            return null;
          }
        });
      }
    }
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
      throws IOException {
    requirePermission(getActiveUser(c), "deleteTable", tableName, null, null,
        Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c,
      final TableName tableName) throws IOException {
    final Configuration conf = c.getEnvironment().getConfiguration();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        AccessControlLists.removeTablePermissions(conf, tableName,
            c.getEnvironment().getTable(AccessControlLists.ACL_TABLE_NAME));
        return null;
      }
    });
    this.authManager.getZKPermissionWatcher().deleteTableACLNode(tableName);
  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> c,
      final TableName tableName) throws IOException {
    requirePermission(getActiveUser(c), "truncateTable", tableName, null, null,
        Action.ADMIN, Action.CREATE);

    final Configuration conf = c.getEnvironment().getConfiguration();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        List<UserPermission> acls = AccessControlLists.getUserTablePermissions(conf, tableName);
        if (acls != null) {
          tableAcls.put(tableName, acls);
        }
        return null;
      }
    });
  }

  @Override
  public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {
    final Configuration conf = ctx.getEnvironment().getConfiguration();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        List<UserPermission> perms = tableAcls.get(tableName);
        if (perms != null) {
          for (UserPermission perm : perms) {
            AccessControlLists.addUserPermission(conf, perm,
                ctx.getEnvironment().getTable(AccessControlLists.ACL_TABLE_NAME));
          }
        }
        tableAcls.remove(tableName);
        return null;
      }
    });
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName,
      HTableDescriptor htd) throws IOException {
    requirePermission(getActiveUser(c), "modifyTable", tableName, null, null,
        Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, final HTableDescriptor htd) throws IOException {
    final Configuration conf = c.getEnvironment().getConfiguration();
    // default the table owner to current user, if not specified.
    final String owner = (htd.getOwnerString() != null) ? htd.getOwnerString() :
      getActiveUser(c).getShortName();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        UserPermission userperm = new UserPermission(Bytes.toBytes(owner),
            htd.getTableName(), null, Action.values());
        AccessControlLists.addUserPermission(conf, userperm,
            c.getEnvironment().getTable(AccessControlLists.ACL_TABLE_NAME));
        return null;
      }
    });
  }

  @Override
  public void preAddColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 TableName tableName, HColumnDescriptor columnFamily)
      throws IOException {
    requireTablePermission(getActiveUser(ctx), "addColumn", tableName, columnFamily.getName(), null,
        Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preModifyColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    TableName tableName, HColumnDescriptor columnFamily)
      throws IOException {
    requirePermission(getActiveUser(ctx), "modifyColumn", tableName, columnFamily.getName(), null,
        Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preDeleteColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    TableName tableName, byte[] columnFamily) throws IOException {
    requirePermission(getActiveUser(ctx), "deleteColumn", tableName, columnFamily, null,
        Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postDeleteColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final byte[] columnFamily) throws IOException {
    final Configuration conf = ctx.getEnvironment().getConfiguration();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        AccessControlLists.removeTablePermissions(conf, tableName, columnFamily,
            ctx.getEnvironment().getTable(AccessControlLists.ACL_TABLE_NAME));
        return null;
      }
    });
  }

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
      throws IOException {
    requirePermission(getActiveUser(c), "enableTable", tableName, null, null,
        Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
      throws IOException {
    if (Bytes.equals(tableName.getName(), AccessControlLists.ACL_GLOBAL_NAME)) {
      // We have to unconditionally disallow disable of the ACL table when we are installed,
      // even if not enforcing authorizations. We are still allowing grants and revocations,
      // checking permissions and logging audit messages, etc. If the ACL table is not
      // available we will fail random actions all over the place.
      throw new AccessDeniedException("Not allowed to disable "
          + AccessControlLists.ACL_TABLE_NAME + " table with AccessController installed");
    }
    requirePermission(getActiveUser(c), "disableTable", tableName, null, null,
        Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preAbortProcedure(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      final ProcedureExecutor<MasterProcedureEnv> procEnv,
      final long procId) throws IOException {
    if (!procEnv.isProcedureOwner(procId, getActiveUser(ctx))) {
      // If the user is not the procedure owner, then we should further probe whether
      // he can abort the procedure.
      requirePermission(getActiveUser(ctx), "abortProcedure", Action.ADMIN);
    }
  }

  @Override
  public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    // There is nothing to do at this time after the procedure abort request was sent.
  }

  @Override
  public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    // We are delegating the authorization check to postListProcedures as we don't have
    // any concrete set of procedures to work with
  }

  @Override
  public void postListProcedures(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ProcedureInfo> procInfoList) throws IOException {
    if (procInfoList.isEmpty()) {
      return;
    }

    // Retains only those which passes authorization checks, as the checks weren't done as part
    // of preListProcedures.
    Iterator<ProcedureInfo> itr = procInfoList.iterator();
    User user = getActiveUser(ctx);
    while (itr.hasNext()) {
      ProcedureInfo procInfo = itr.next();
      try {
        if (!ProcedureInfo.isProcedureOwner(procInfo, user)) {
          // If the user is not the procedure owner, then we should further probe whether
          // he can see the procedure.
          requirePermission(user, "listProcedures", Action.ADMIN);
        }
      } catch (AccessDeniedException e) {
        itr.remove();
      }
    }
  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo region,
      ServerName srcServer, ServerName destServer) throws IOException {
    requirePermission(getActiveUser(c), "move", region.getTable(), null, null, Action.ADMIN);
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo)
      throws IOException {
    requirePermission(getActiveUser(c), "assign", regionInfo.getTable(), null, null, Action.ADMIN);
  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo,
      boolean force) throws IOException {
    requirePermission(getActiveUser(c), "unassign", regionInfo.getTable(), null, null, Action.ADMIN);
  }

  @Override
  public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo regionInfo) throws IOException {
    requirePermission(getActiveUser(c), "regionOffline", regionInfo.getTable(), null, null,
        Action.ADMIN);
  }

  @Override
  public boolean preSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {
    requirePermission(getActiveUser(ctx), "setSplitOrMergeEnabled", Action.ADMIN);
    return false;
  }

  @Override
  public void postSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {
  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission(getActiveUser(c), "balance", Action.ADMIN);
  }

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c,
      boolean newValue) throws IOException {
    requirePermission(getActiveUser(c), "balanceSwitch", Action.ADMIN);
    return newValue;
  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission(getActiveUser(c), "shutdown", Action.ADMIN);
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission(getActiveUser(c), "stopMaster", Action.ADMIN);
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    if (!MetaTableAccessor.tableExists(ctx.getEnvironment().getMasterServices()
      .getConnection(), AccessControlLists.ACL_TABLE_NAME)) {
      // initialize the ACL storage table
      AccessControlLists.createACLTable(ctx.getEnvironment().getMasterServices());
    } else {
      aclTabAvailable = true;
    }
  }

  @Override
  public void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
    requirePermission(getActiveUser(ctx), "snapshot " + snapshot.getName(), hTableDescriptor.getTableName(), null, null,
      Permission.Action.ADMIN);
  }

  @Override
  public void preListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
    User user = getActiveUser(ctx);
    if (SnapshotDescriptionUtils.isSnapshotOwner(snapshot, user)) {
      // list it, if user is the owner of snapshot
      AuthResult result = AuthResult.allow("listSnapshot " + snapshot.getName(),
          "Snapshot owner check allowed", user, null, null, null);
      logResult(result);
    } else {
      requirePermission(user, "listSnapshot " + snapshot.getName(), Action.ADMIN);
    }
  }

  @Override
  public void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
    User user = getActiveUser(ctx);
    if (SnapshotDescriptionUtils.isSnapshotOwner(snapshot, user)
        && hTableDescriptor.getNameAsString().equals(snapshot.getTable())) {
      // Snapshot owner is allowed to create a table with the same name as the snapshot he took
      AuthResult result = AuthResult.allow("cloneSnapshot " + snapshot.getName(),
        "Snapshot owner check allowed", user, null, hTableDescriptor.getTableName(), null);
      logResult(result);
    } else {
      requirePermission(user, "cloneSnapshot " + snapshot.getName(), Action.ADMIN);
    }
  }

  @Override
  public void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
    User user = getActiveUser(ctx);
    if (SnapshotDescriptionUtils.isSnapshotOwner(snapshot, user)) {
      requirePermission(user, "restoreSnapshot " + snapshot.getName(), hTableDescriptor.getTableName(), null, null,
        Permission.Action.ADMIN);
    } else {
      requirePermission(user, "restoreSnapshot " + snapshot.getName(), Action.ADMIN);
    }
  }

  @Override
  public void preDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
    User user = getActiveUser(ctx);
    if (SnapshotDescriptionUtils.isSnapshotOwner(snapshot, user)) {
      // Snapshot owner is allowed to delete the snapshot
      AuthResult result = AuthResult.allow("deleteSnapshot " + snapshot.getName(),
          "Snapshot owner check allowed", user, null, null, null);
      logResult(result);
    } else {
      requirePermission(user, "deleteSnapshot " + snapshot.getName(), Action.ADMIN);
    }
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
    requireGlobalPermission(getActiveUser(ctx), "createNamespace", Action.ADMIN, ns.getName());
  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
      throws IOException {
    requireGlobalPermission(getActiveUser(ctx), "deleteNamespace", Action.ADMIN, namespace);
  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace) throws IOException {
    final Configuration conf = ctx.getEnvironment().getConfiguration();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        AccessControlLists.removeNamespacePermissions(conf, namespace,
            ctx.getEnvironment().getTable(AccessControlLists.ACL_TABLE_NAME));
        return null;
      }
    });
    this.authManager.getZKPermissionWatcher().deleteNamespaceACLNode(namespace);
    LOG.info(namespace + " entry deleted in " + AccessControlLists.ACL_TABLE_NAME + " table.");
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
    // We require only global permission so that
    // a user with NS admin cannot altering namespace configurations. i.e. namespace quota
    requireGlobalPermission(getActiveUser(ctx), "modifyNamespace", Action.ADMIN, ns.getName());
  }

  @Override
  public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
      throws IOException {
    requireNamespacePermission(getActiveUser(ctx), "getNamespaceDescriptor", namespace, Action.ADMIN);
  }

  @Override
  public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<NamespaceDescriptor> descriptors) throws IOException {
    // Retains only those which passes authorization checks, as the checks weren't done as part
    // of preGetTableDescriptors.
    Iterator<NamespaceDescriptor> itr = descriptors.iterator();
    User user = getActiveUser(ctx);
    while (itr.hasNext()) {
      NamespaceDescriptor desc = itr.next();
      try {
        requireNamespacePermission(user, "listNamespaces", desc.getName(), Action.ADMIN);
      } catch (AccessDeniedException e) {
        itr.remove();
      }
    }
  }

  @Override
  public void preTableFlush(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {
    requirePermission(getActiveUser(ctx), "flushTable", tableName, null, null,
        Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preSplitRegion(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final byte[] splitRow) throws IOException {
    requirePermission(getActiveUser(ctx), "split", tableName, null, null, Action.ADMIN);
  }

  /* ---- RegionObserver implementation ---- */

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c)
      throws IOException {
    RegionCoprocessorEnvironment env = c.getEnvironment();
    final Region region = env.getRegion();
    if (region == null) {
      LOG.error("NULL region from RegionCoprocessorEnvironment in preOpen()");
    } else {
      HRegionInfo regionInfo = region.getRegionInfo();
      if (regionInfo.getTable().isSystemTable()) {
        checkSystemOrSuperUser(getActiveUser(c));
      } else {
        requirePermission(getActiveUser(c), "preOpen", Action.ADMIN);
      }
    }
  }

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    RegionCoprocessorEnvironment env = c.getEnvironment();
    final Region region = env.getRegion();
    if (region == null) {
      LOG.error("NULL region from RegionCoprocessorEnvironment in postOpen()");
      return;
    }
    if (AccessControlLists.isAclRegion(region)) {
      aclRegion = true;
      // When this region is under recovering state, initialize will be handled by postLogReplay
      if (!region.isRecovering()) {
        try {
          initialize(env);
        } catch (IOException ex) {
          // if we can't obtain permissions, it's better to fail
          // than perform checks incorrectly
          throw new RuntimeException("Failed to initialize permissions cache", ex);
        }
      }
    } else {
      initialized = true;
    }
  }

  @Override
  public void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> c) {
    if (aclRegion) {
      try {
        initialize(c.getEnvironment());
      } catch (IOException ex) {
        // if we can't obtain permissions, it's better to fail
        // than perform checks incorrectly
        throw new RuntimeException("Failed to initialize permissions cache", ex);
      }
    }
  }

  @Override
  public void preFlush(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
    requirePermission(getActiveUser(c), "flush", getTableName(c.getEnvironment()), null, null,
        Action.ADMIN, Action.CREATE);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c,
      final Store store, final InternalScanner scanner, final ScanType scanType)
          throws IOException {
    requirePermission(getActiveUser(c), "compact", getTableName(c.getEnvironment()), null, null,
        Action.ADMIN, Action.CREATE);
    return scanner;
  }

  private void internalPreRead(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Query query, OpType opType) throws IOException {
    Filter filter = query.getFilter();
    // Don't wrap an AccessControlFilter
    if (filter != null && filter instanceof AccessControlFilter) {
      return;
    }
    User user = getActiveUser(c);
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<byte[]>> families = null;
    switch (opType) {
    case GET:
    case EXISTS:
      families = ((Get)query).getFamilyMap();
      break;
    case SCAN:
      families = ((Scan)query).getFamilyMap();
      break;
    default:
      throw new RuntimeException("Unhandled operation " + opType);
    }
    AuthResult authResult = permissionGranted(opType, user, env, families, Action.READ);
    Region region = getRegion(env);
    TableName table = getTableName(region);
    Map<ByteRange, Integer> cfVsMaxVersions = Maps.newHashMap();
    for (HColumnDescriptor hcd : region.getTableDesc().getFamilies()) {
      cfVsMaxVersions.put(new SimpleMutableByteRange(hcd.getName()), hcd.getMaxVersions());
    }
    if (!authResult.isAllowed()) {
      if (!cellFeaturesEnabled || compatibleEarlyTermination) {
        // Old behavior: Scan with only qualifier checks if we have partial
        // permission. Backwards compatible behavior is to throw an
        // AccessDeniedException immediately if there are no grants for table
        // or CF or CF+qual. Only proceed with an injected filter if there are
        // grants for qualifiers. Otherwise we will fall through below and log
        // the result and throw an ADE. We may end up checking qualifier
        // grants three times (permissionGranted above, here, and in the
        // filter) but that's the price of backwards compatibility.
        if (hasFamilyQualifierPermission(user, Action.READ, env, families)) {
          authResult.setAllowed(true);
          authResult.setReason("Access allowed with filter");
          // Only wrap the filter if we are enforcing authorizations
          if (authorizationEnabled) {
            Filter ourFilter = new AccessControlFilter(authManager, user, table,
              AccessControlFilter.Strategy.CHECK_TABLE_AND_CF_ONLY,
              cfVsMaxVersions);
            // wrap any existing filter
            if (filter != null) {
              ourFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                Lists.newArrayList(ourFilter, filter));
            }
            switch (opType) {
              case GET:
              case EXISTS:
                ((Get)query).setFilter(ourFilter);
                break;
              case SCAN:
                ((Scan)query).setFilter(ourFilter);
                break;
              default:
                throw new RuntimeException("Unhandled operation " + opType);
            }
          }
        }
      } else {
        // New behavior: Any access we might be granted is more fine-grained
        // than whole table or CF. Simply inject a filter and return what is
        // allowed. We will not throw an AccessDeniedException. This is a
        // behavioral change since 0.96.
        authResult.setAllowed(true);
        authResult.setReason("Access allowed with filter");
        // Only wrap the filter if we are enforcing authorizations
        if (authorizationEnabled) {
          Filter ourFilter = new AccessControlFilter(authManager, user, table,
            AccessControlFilter.Strategy.CHECK_CELL_DEFAULT, cfVsMaxVersions);
          // wrap any existing filter
          if (filter != null) {
            ourFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL,
              Lists.newArrayList(ourFilter, filter));
          }
          switch (opType) {
            case GET:
            case EXISTS:
              ((Get)query).setFilter(ourFilter);
              break;
            case SCAN:
              ((Scan)query).setFilter(ourFilter);
              break;
            default:
              throw new RuntimeException("Unhandled operation " + opType);
          }
        }
      }
    }

    logResult(authResult);
    if (authorizationEnabled && !authResult.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions for user '"
          + (user != null ? user.getShortName() : "null")
          + "' (table=" + table + ", action=READ)");
    }
  }

  @Override
  public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Get get, final List<Cell> result) throws IOException {
    internalPreRead(c, get, OpType.GET);
  }

  @Override
  public boolean preExists(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Get get, final boolean exists) throws IOException {
    internalPreRead(c, get, OpType.EXISTS);
    return exists;
  }

  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit, final Durability durability)
      throws IOException {
    User user = getActiveUser(c);
    checkForReservedTagPresence(user, put);

    // Require WRITE permission to the table, CF, or top visible value, if any.
    // NOTE: We don't need to check the permissions for any earlier Puts
    // because we treat the ACLs in each Put as timestamped like any other
    // HBase value. A new ACL in a new Put applies to that Put. It doesn't
    // change the ACL of any previous Put. This allows simple evolution of
    // security policy over time without requiring expensive updates.
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<Cell>> families = put.getFamilyCellMap();
    AuthResult authResult = permissionGranted(OpType.PUT, user, env, families, Action.WRITE);
    logResult(authResult);
    if (!authResult.isAllowed()) {
      if (cellFeaturesEnabled && !compatibleEarlyTermination) {
        put.setAttribute(CHECK_COVERING_PERM, TRUE);
      } else if (authorizationEnabled) {
        throw new AccessDeniedException("Insufficient permissions " + authResult.toContextString());
      }
    }

    // Add cell ACLs from the operation to the cells themselves
    byte[] bytes = put.getAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL);
    if (bytes != null) {
      if (cellFeaturesEnabled) {
        addCellPermissions(bytes, put.getFamilyCellMap());
      } else {
        throw new DoNotRetryIOException("Cell ACLs cannot be persisted");
      }
    }
  }

  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit, final Durability durability) {
    if (aclRegion) {
      updateACL(c.getEnvironment(), put.getFamilyCellMap());
    }
  }

  @Override
  public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Delete delete, final WALEdit edit, final Durability durability)
      throws IOException {
    // An ACL on a delete is useless, we shouldn't allow it
    if (delete.getAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL) != null) {
      throw new DoNotRetryIOException("ACL on delete has no effect: " + delete.toString());
    }
    // Require WRITE permissions on all cells covered by the delete. Unlike
    // for Puts we need to check all visible prior versions, because a major
    // compaction could remove them. If the user doesn't have permission to
    // overwrite any of the visible versions ('visible' defined as not covered
    // by a tombstone already) then we have to disallow this operation.
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<Cell>> families = delete.getFamilyCellMap();
    User user = getActiveUser(c);
    AuthResult authResult = permissionGranted(OpType.DELETE, user, env, families, Action.WRITE);
    logResult(authResult);
    if (!authResult.isAllowed()) {
      if (cellFeaturesEnabled && !compatibleEarlyTermination) {
        delete.setAttribute(CHECK_COVERING_PERM, TRUE);
      } else if (authorizationEnabled) {
        throw new AccessDeniedException("Insufficient permissions " +
          authResult.toContextString());
      }
    }
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (cellFeaturesEnabled && !compatibleEarlyTermination) {
      TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
      User user = getActiveUser(c);
      for (int i = 0; i < miniBatchOp.size(); i++) {
        Mutation m = miniBatchOp.getOperation(i);
        if (m.getAttribute(CHECK_COVERING_PERM) != null) {
          // We have a failure with table, cf and q perm checks and now giving a chance for cell
          // perm check
          OpType opType;
          if (m instanceof Put) {
            checkForReservedTagPresence(user, m);
            opType = OpType.PUT;
          } else {
            opType = OpType.DELETE;
          }
          AuthResult authResult = null;
          if (checkCoveringPermission(user, opType, c.getEnvironment(), m.getRow(),
            m.getFamilyCellMap(), m.getTimeStamp(), Action.WRITE)) {
            authResult = AuthResult.allow(opType.toString(), "Covering cell set",
              user, Action.WRITE, table, m.getFamilyCellMap());
          } else {
            authResult = AuthResult.deny(opType.toString(), "Covering cell set",
              user, Action.WRITE, table, m.getFamilyCellMap());
          }
          logResult(authResult);
          if (authorizationEnabled && !authResult.isAllowed()) {
            throw new AccessDeniedException("Insufficient permissions "
              + authResult.toContextString());
          }
        }
      }
    }
  }

  @Override
  public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Delete delete, final WALEdit edit, final Durability durability)
      throws IOException {
    if (aclRegion) {
      updateACL(c.getEnvironment(), delete.getFamilyCellMap());
    }
  }

  @Override
  public boolean preCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp,
      final ByteArrayComparable comparator, final Put put,
      final boolean result) throws IOException {
    User user = getActiveUser(c);
    checkForReservedTagPresence(user, put);

    // Require READ and WRITE permissions on the table, CF, and KV to update
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<byte[]>> families = makeFamilyMap(family, qualifier);
    AuthResult authResult = permissionGranted(OpType.CHECK_AND_PUT, user, env, families,
      Action.READ, Action.WRITE);
    logResult(authResult);
    if (!authResult.isAllowed()) {
      if (cellFeaturesEnabled && !compatibleEarlyTermination) {
        put.setAttribute(CHECK_COVERING_PERM, TRUE);
      } else if (authorizationEnabled) {
        throw new AccessDeniedException("Insufficient permissions " +
          authResult.toContextString());
      }
    }

    byte[] bytes = put.getAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL);
    if (bytes != null) {
      if (cellFeaturesEnabled) {
        addCellPermissions(bytes, put.getFamilyCellMap());
      } else {
        throw new DoNotRetryIOException("Cell ACLs cannot be persisted");
      }
    }
    return result;
  }

  @Override
  public boolean preCheckAndPutAfterRowLock(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte[] row, final byte[] family, final byte[] qualifier,
      final CompareFilter.CompareOp compareOp, final ByteArrayComparable comparator, final Put put,
      final boolean result) throws IOException {
    if (put.getAttribute(CHECK_COVERING_PERM) != null) {
      // We had failure with table, cf and q perm checks and now giving a chance for cell
      // perm check
      TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
      Map<byte[], ? extends Collection<byte[]>> families = makeFamilyMap(family, qualifier);
      AuthResult authResult = null;
      User user = getActiveUser(c);
      if (checkCoveringPermission(user, OpType.CHECK_AND_PUT, c.getEnvironment(), row, families,
          HConstants.LATEST_TIMESTAMP, Action.READ)) {
        authResult = AuthResult.allow(OpType.CHECK_AND_PUT.toString(), "Covering cell set",
            user, Action.READ, table, families);
      } else {
        authResult = AuthResult.deny(OpType.CHECK_AND_PUT.toString(), "Covering cell set",
            user, Action.READ, table, families);
      }
      logResult(authResult);
      if (authorizationEnabled && !authResult.isAllowed()) {
        throw new AccessDeniedException("Insufficient permissions " + authResult.toContextString());
      }
    }
    return result;
  }

  @Override
  public boolean preCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp,
      final ByteArrayComparable comparator, final Delete delete,
      final boolean result) throws IOException {
    // An ACL on a delete is useless, we shouldn't allow it
    if (delete.getAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL) != null) {
      throw new DoNotRetryIOException("ACL on checkAndDelete has no effect: " +
          delete.toString());
    }
    // Require READ and WRITE permissions on the table, CF, and the KV covered
    // by the delete
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<byte[]>> families = makeFamilyMap(family, qualifier);
    User user = getActiveUser(c);
    AuthResult authResult = permissionGranted(OpType.CHECK_AND_DELETE, user, env, families,
        Action.READ, Action.WRITE);
    logResult(authResult);
    if (!authResult.isAllowed()) {
      if (cellFeaturesEnabled && !compatibleEarlyTermination) {
        delete.setAttribute(CHECK_COVERING_PERM, TRUE);
      } else if (authorizationEnabled) {
        throw new AccessDeniedException("Insufficient permissions " +
          authResult.toContextString());
      }
    }
    return result;
  }

  @Override
  public boolean preCheckAndDeleteAfterRowLock(
      final ObserverContext<RegionCoprocessorEnvironment> c, final byte[] row, final byte[] family,
      final byte[] qualifier, final CompareFilter.CompareOp compareOp,
      final ByteArrayComparable comparator, final Delete delete, final boolean result)
      throws IOException {
    if (delete.getAttribute(CHECK_COVERING_PERM) != null) {
      // We had failure with table, cf and q perm checks and now giving a chance for cell
      // perm check
      TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
      Map<byte[], ? extends Collection<byte[]>> families = makeFamilyMap(family, qualifier);
      AuthResult authResult = null;
      User user = getActiveUser(c);
      if (checkCoveringPermission(user, OpType.CHECK_AND_DELETE, c.getEnvironment(), row, families,
          HConstants.LATEST_TIMESTAMP, Action.READ)) {
        authResult = AuthResult.allow(OpType.CHECK_AND_DELETE.toString(), "Covering cell set",
            user, Action.READ, table, families);
      } else {
        authResult = AuthResult.deny(OpType.CHECK_AND_DELETE.toString(), "Covering cell set",
            user, Action.READ, table, families);
      }
      logResult(authResult);
      if (authorizationEnabled && !authResult.isAllowed()) {
        throw new AccessDeniedException("Insufficient permissions " + authResult.toContextString());
      }
    }
    return result;
  }

  @Override
  public long preIncrementColumnValue(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL)
      throws IOException {
    // Require WRITE permission to the table, CF, and the KV to be replaced by the
    // incremented value
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<byte[]>> families = makeFamilyMap(family, qualifier);
    User user = getActiveUser(c);
    AuthResult authResult = permissionGranted(OpType.INCREMENT_COLUMN_VALUE, user, env, families,
        Action.WRITE);
    if (!authResult.isAllowed() && cellFeaturesEnabled && !compatibleEarlyTermination) {
      authResult.setAllowed(checkCoveringPermission(user, OpType.INCREMENT_COLUMN_VALUE, env, row,
        families, HConstants.LATEST_TIMESTAMP, Action.WRITE));
      authResult.setReason("Covering cell set");
    }
    logResult(authResult);
    if (authorizationEnabled && !authResult.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + authResult.toContextString());
    }
    return -1;
  }

  @Override
  public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
      throws IOException {
    User user = getActiveUser(c);
    checkForReservedTagPresence(user, append);

    // Require WRITE permission to the table, CF, and the KV to be appended
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<Cell>> families = append.getFamilyCellMap();
    AuthResult authResult = permissionGranted(OpType.APPEND, user, env, families, Action.WRITE);
    logResult(authResult);
    if (!authResult.isAllowed()) {
      if (cellFeaturesEnabled && !compatibleEarlyTermination) {
        append.setAttribute(CHECK_COVERING_PERM, TRUE);
      } else if (authorizationEnabled)  {
        throw new AccessDeniedException("Insufficient permissions " +
          authResult.toContextString());
      }
    }

    byte[] bytes = append.getAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL);
    if (bytes != null) {
      if (cellFeaturesEnabled) {
        addCellPermissions(bytes, append.getFamilyCellMap());
      } else {
        throw new DoNotRetryIOException("Cell ACLs cannot be persisted");
      }
    }

    return null;
  }

  @Override
  public Result preAppendAfterRowLock(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Append append) throws IOException {
    if (append.getAttribute(CHECK_COVERING_PERM) != null) {
      // We had failure with table, cf and q perm checks and now giving a chance for cell
      // perm check
      TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
      AuthResult authResult = null;
      User user = getActiveUser(c);
      if (checkCoveringPermission(user, OpType.APPEND, c.getEnvironment(), append.getRow(),
          append.getFamilyCellMap(), HConstants.LATEST_TIMESTAMP, Action.WRITE)) {
        authResult = AuthResult.allow(OpType.APPEND.toString(), "Covering cell set",
            user, Action.WRITE, table, append.getFamilyCellMap());
      } else {
        authResult = AuthResult.deny(OpType.APPEND.toString(), "Covering cell set",
            user, Action.WRITE, table, append.getFamilyCellMap());
      }
      logResult(authResult);
      if (authorizationEnabled && !authResult.isAllowed()) {
        throw new AccessDeniedException("Insufficient permissions " +
          authResult.toContextString());
      }
    }
    return null;
  }

  @Override
  public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment)
      throws IOException {
    User user = getActiveUser(c);
    checkForReservedTagPresence(user, increment);

    // Require WRITE permission to the table, CF, and the KV to be replaced by
    // the incremented value
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<Cell>> families = increment.getFamilyCellMap();
    AuthResult authResult = permissionGranted(OpType.INCREMENT, user, env, families,
      Action.WRITE);
    logResult(authResult);
    if (!authResult.isAllowed()) {
      if (cellFeaturesEnabled && !compatibleEarlyTermination) {
        increment.setAttribute(CHECK_COVERING_PERM, TRUE);
      } else if (authorizationEnabled) {
        throw new AccessDeniedException("Insufficient permissions " +
          authResult.toContextString());
      }
    }

    byte[] bytes = increment.getAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL);
    if (bytes != null) {
      if (cellFeaturesEnabled) {
        addCellPermissions(bytes, increment.getFamilyCellMap());
      } else {
        throw new DoNotRetryIOException("Cell ACLs cannot be persisted");
      }
    }

    return null;
  }

  @Override
  public Result preIncrementAfterRowLock(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment) throws IOException {
    if (increment.getAttribute(CHECK_COVERING_PERM) != null) {
      // We had failure with table, cf and q perm checks and now giving a chance for cell
      // perm check
      TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
      AuthResult authResult = null;
      User user = getActiveUser(c);
      if (checkCoveringPermission(user, OpType.INCREMENT, c.getEnvironment(), increment.getRow(),
          increment.getFamilyCellMap(), increment.getTimeRange().getMax(), Action.WRITE)) {
        authResult = AuthResult.allow(OpType.INCREMENT.toString(), "Covering cell set",
            user, Action.WRITE, table, increment.getFamilyCellMap());
      } else {
        authResult = AuthResult.deny(OpType.INCREMENT.toString(), "Covering cell set",
            user, Action.WRITE, table, increment.getFamilyCellMap());
      }
      logResult(authResult);
      if (authorizationEnabled && !authResult.isAllowed()) {
        throw new AccessDeniedException("Insufficient permissions " +
          authResult.toContextString());
      }
    }
    return null;
  }

  @Override
  public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx,
      MutationType opType, Mutation mutation, Cell oldCell, Cell newCell) throws IOException {
    // If the HFile version is insufficient to persist tags, we won't have any
    // work to do here
    if (!cellFeaturesEnabled) {
      return newCell;
    }

    // Collect any ACLs from the old cell
    List<Tag> tags = Lists.newArrayList();
    List<Tag> aclTags = Lists.newArrayList();
    ListMultimap<String,Permission> perms = ArrayListMultimap.create();
    if (oldCell != null) {
      Iterator<Tag> tagIterator = CellUtil.tagsIterator(oldCell);
      while (tagIterator.hasNext()) {
        Tag tag = tagIterator.next();
        if (tag.getType() != AccessControlLists.ACL_TAG_TYPE) {
          // Not an ACL tag, just carry it through
          if (LOG.isTraceEnabled()) {
            LOG.trace("Carrying forward tag from " + oldCell + ": type " + tag.getType()
                + " length " + tag.getValueLength());
          }
          tags.add(tag);
        } else {
          aclTags.add(tag);
        }
      }
    }

    // Do we have an ACL on the operation?
    byte[] aclBytes = mutation.getACL();
    if (aclBytes != null) {
      // Yes, use it
      tags.add(new ArrayBackedTag(AccessControlLists.ACL_TAG_TYPE, aclBytes));
    } else {
      // No, use what we carried forward
      if (perms != null) {
        // TODO: If we collected ACLs from more than one tag we may have a
        // List<Permission> of size > 1, this can be collapsed into a single
        // Permission
        if (LOG.isTraceEnabled()) {
          LOG.trace("Carrying forward ACLs from " + oldCell + ": " + perms);
        }
        tags.addAll(aclTags);
      }
    }

    // If we have no tags to add, just return
    if (tags.isEmpty()) {
      return newCell;
    }

    Cell rewriteCell = CellUtil.createCell(newCell, tags);
    return rewriteCell;
  }

  @Override
  public RegionScanner preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s) throws IOException {
    internalPreRead(c, scan, OpType.SCAN);
    return s;
  }

  @Override
  public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final RegionScanner s) throws IOException {
    User user = getActiveUser(c);
    if (user != null && user.getShortName() != null) {
      // store reference to scanner owner for later checks
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

  @Override
  public boolean postScannerFilterRow(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final Cell curRowCell, final boolean hasMore) throws IOException {
    // Impl in BaseRegionObserver might do unnecessary copy for Off heap backed Cells.
    return hasMore;
  }

  /**
   * Verify, when servicing an RPC, that the caller is the scanner owner.
   * If so, we assume that access control is correctly enforced based on
   * the checks performed in preScannerOpen()
   */
  private void requireScannerOwner(InternalScanner s) throws AccessDeniedException {
    if (!RpcServer.isInRpcCallContext())
      return;
    String requestUserName = RpcServer.getRequestUserName();
    String owner = scannerOwners.get(s);
    if (authorizationEnabled && owner != null && !owner.equals(requestUserName)) {
      throw new AccessDeniedException("User '"+ requestUserName +"' is not the scanner owner!");
    }
  }

  /**
   * Verifies user has CREATE privileges on
   * the Column Families involved in the bulkLoadHFile
   * request. Specific Column Write privileges are presently
   * ignored.
   */
  @Override
  public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
      List<Pair<byte[], String>> familyPaths) throws IOException {
    User user = getActiveUser(ctx);
    for(Pair<byte[],String> el : familyPaths) {
      requirePermission(user, "preBulkLoadHFile",
          ctx.getEnvironment().getRegion().getTableDesc().getTableName(),
          el.getFirst(),
          null,
          Action.CREATE);
    }
  }

  /**
   * Authorization check for
   * SecureBulkLoadProtocol.prepareBulkLoad()
   * @param ctx the context
   * @param request the request
   * @throws IOException
   */
  @Override
  public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx,
      PrepareBulkLoadRequest request) throws IOException {
    requireAccess(getActiveUser(ctx), "prePrepareBulkLoad",
        ctx.getEnvironment().getRegion().getTableDesc().getTableName(), Action.CREATE);
  }

  /**
   * Authorization security check for
   * SecureBulkLoadProtocol.cleanupBulkLoad()
   * @param ctx the context
   * @param request the request
   * @throws IOException
   */
  @Override
  public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx,
      CleanupBulkLoadRequest request) throws IOException {
    requireAccess(getActiveUser(ctx), "preCleanupBulkLoad",
        ctx.getEnvironment().getRegion().getTableDesc().getTableName(), Action.CREATE);
  }

  /* ---- EndpointObserver implementation ---- */

  @Override
  public Message preEndpointInvocation(ObserverContext<RegionCoprocessorEnvironment> ctx,
      Service service, String methodName, Message request) throws IOException {
    // Don't intercept calls to our own AccessControlService, we check for
    // appropriate permissions in the service handlers
    if (shouldCheckExecPermission && !(service instanceof AccessControlService)) {
      requirePermission(getActiveUser(ctx),
          "invoke(" + service.getDescriptorForType().getName() + "." + methodName + ")",
          getTableName(ctx.getEnvironment()), null, null,
          Action.EXEC);
    }
    return request;
  }

  @Override
  public void postEndpointInvocation(ObserverContext<RegionCoprocessorEnvironment> ctx,
      Service service, String methodName, Message request, Message.Builder responseBuilder)
      throws IOException { }

  /* ---- Protobuf AccessControlService implementation ---- */

  @Override
  public void grant(RpcController controller,
                    AccessControlProtos.GrantRequest request,
                    RpcCallback<AccessControlProtos.GrantResponse> done) {
    final UserPermission perm = AccessControlUtil.toUserPermission(request.getUserPermission());
    AccessControlProtos.GrantResponse response = null;
    try {
      // verify it's only running at .acl.
      if (aclRegion) {
        if (!initialized) {
          throw new CoprocessorException("AccessController not yet initialized");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received request to grant access permission " + perm.toString());
        }
        User caller = RpcServer.getRequestUser();

        switch(request.getUserPermission().getPermission().getType()) {
          case Global :
          case Table :
            requirePermission(caller, "grant", perm.getTableName(),
                perm.getFamily(), perm.getQualifier(), Action.ADMIN);
            break;
          case Namespace :
            requireNamespacePermission(caller, "grant", perm.getNamespace(), Action.ADMIN);
           break;
        }

        User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            AccessControlLists.addUserPermission(regionEnv.getConfiguration(), perm,
                regionEnv.getTable(AccessControlLists.ACL_TABLE_NAME));
            return null;
          }
        });

        if (AUDITLOG.isTraceEnabled()) {
          // audit log should store permission changes in addition to auth results
          AUDITLOG.trace("Granted permission " + perm.toString());
        }
      } else {
        throw new CoprocessorException(AccessController.class, "This method "
            + "can only execute at " + AccessControlLists.ACL_TABLE_NAME + " table.");
      }
      response = AccessControlProtos.GrantResponse.getDefaultInstance();
    } catch (IOException ioe) {
      // pass exception back up
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  @Override
  public void revoke(RpcController controller,
                     AccessControlProtos.RevokeRequest request,
                     RpcCallback<AccessControlProtos.RevokeResponse> done) {
    final UserPermission perm = AccessControlUtil.toUserPermission(request.getUserPermission());
    AccessControlProtos.RevokeResponse response = null;
    try {
      // only allowed to be called on _acl_ region
      if (aclRegion) {
        if (!initialized) {
          throw new CoprocessorException("AccessController not yet initialized");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received request to revoke access permission " + perm.toString());
        }
        User caller = RpcServer.getRequestUser();

        switch(request.getUserPermission().getPermission().getType()) {
          case Global :
          case Table :
            requirePermission(caller, "revoke", perm.getTableName(), perm.getFamily(),
              perm.getQualifier(), Action.ADMIN);
            break;
          case Namespace :
            requireNamespacePermission(caller, "revoke", perm.getNamespace(), Action.ADMIN);
            break;
        }

        User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            AccessControlLists.removeUserPermission(regionEnv.getConfiguration(), perm,
                regionEnv.getTable(AccessControlLists.ACL_TABLE_NAME));
            return null;
          }
        });

        if (AUDITLOG.isTraceEnabled()) {
          // audit log should record all permission changes
          AUDITLOG.trace("Revoked permission " + perm.toString());
        }
      } else {
        throw new CoprocessorException(AccessController.class, "This method "
            + "can only execute at " + AccessControlLists.ACL_TABLE_NAME + " table.");
      }
      response = AccessControlProtos.RevokeResponse.getDefaultInstance();
    } catch (IOException ioe) {
      // pass exception back up
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  @Override
  public void getUserPermissions(RpcController controller,
                                 AccessControlProtos.GetUserPermissionsRequest request,
                                 RpcCallback<AccessControlProtos.GetUserPermissionsResponse> done) {
    AccessControlProtos.GetUserPermissionsResponse response = null;
    try {
      // only allowed to be called on _acl_ region
      if (aclRegion) {
        if (!initialized) {
          throw new CoprocessorException("AccessController not yet initialized");
        }
        User caller = RpcServer.getRequestUser();

        List<UserPermission> perms = null;
        if (request.getType() == AccessControlProtos.Permission.Type.Table) {
          final TableName table = request.hasTableName() ?
            ProtobufUtil.toTableName(request.getTableName()) : null;
          requirePermission(caller, "userPermissions", table, null, null, Action.ADMIN);
          perms = User.runAsLoginUser(new PrivilegedExceptionAction<List<UserPermission>>() {
            @Override
            public List<UserPermission> run() throws Exception {
              return AccessControlLists.getUserTablePermissions(regionEnv.getConfiguration(), table);
            }
          });
        } else if (request.getType() == AccessControlProtos.Permission.Type.Namespace) {
          final String namespace = request.getNamespaceName().toStringUtf8();
          requireNamespacePermission(caller, "userPermissions", namespace, Action.ADMIN);
          perms = User.runAsLoginUser(new PrivilegedExceptionAction<List<UserPermission>>() {
            @Override
            public List<UserPermission> run() throws Exception {
              return AccessControlLists.getUserNamespacePermissions(regionEnv.getConfiguration(),
                namespace);
            }
          });
        } else {
          requirePermission(caller, "userPermissions", Action.ADMIN);
          perms = User.runAsLoginUser(new PrivilegedExceptionAction<List<UserPermission>>() {
            @Override
            public List<UserPermission> run() throws Exception {
              return AccessControlLists.getUserPermissions(regionEnv.getConfiguration(), null);
            }
          });
          // Adding superusers explicitly to the result set as AccessControlLists do not store them.
          // Also using acl as table name to be inline  with the results of global admin and will
          // help in avoiding any leakage of information about being superusers.
          for (String user: Superusers.getSuperUsers()) {
            perms.add(new UserPermission(user.getBytes(), AccessControlLists.ACL_TABLE_NAME, null,
                Action.values()));
          }
        }
        response = AccessControlUtil.buildGetUserPermissionsResponse(perms);
      } else {
        throw new CoprocessorException(AccessController.class, "This method "
            + "can only execute at " + AccessControlLists.ACL_TABLE_NAME + " table.");
      }
    } catch (IOException ioe) {
      // pass exception back up
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  @Override
  public void checkPermissions(RpcController controller,
                               AccessControlProtos.CheckPermissionsRequest request,
                               RpcCallback<AccessControlProtos.CheckPermissionsResponse> done) {
    Permission[] permissions = new Permission[request.getPermissionCount()];
    for (int i=0; i < request.getPermissionCount(); i++) {
      permissions[i] = AccessControlUtil.toPermission(request.getPermission(i));
    }
    AccessControlProtos.CheckPermissionsResponse response = null;
    try {
      User user = RpcServer.getRequestUser();
      TableName tableName = regionEnv.getRegion().getTableDesc().getTableName();
      for (Permission permission : permissions) {
        if (permission instanceof TablePermission) {
          // Check table permissions

          TablePermission tperm = (TablePermission) permission;
          for (Action action : permission.getActions()) {
            if (!tperm.getTableName().equals(tableName)) {
              throw new CoprocessorException(AccessController.class, String.format("This method "
                  + "can only execute at the table specified in TablePermission. " +
                  "Table of the region:%s , requested table:%s", tableName,
                  tperm.getTableName()));
            }

            Map<byte[], Set<byte[]>> familyMap =
                new TreeMap<byte[], Set<byte[]>>(Bytes.BYTES_COMPARATOR);
            if (tperm.getFamily() != null) {
              if (tperm.getQualifier() != null) {
                Set<byte[]> qualifiers = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
                qualifiers.add(tperm.getQualifier());
                familyMap.put(tperm.getFamily(), qualifiers);
              } else {
                familyMap.put(tperm.getFamily(), null);
              }
            }

            AuthResult result = permissionGranted("checkPermissions", user, action, regionEnv,
              familyMap);
            logResult(result);
            if (!result.isAllowed()) {
              // Even if passive we need to throw an exception here, we support checking
              // effective permissions, so throw unconditionally
              throw new AccessDeniedException("Insufficient permissions (table=" + tableName +
                (familyMap.size() > 0 ? ", family: " + result.toFamilyString() : "") +
                ", action=" + action.toString() + ")");
            }
          }

        } else {
          // Check global permissions

          for (Action action : permission.getActions()) {
            AuthResult result;
            if (authManager.authorize(user, action)) {
              result = AuthResult.allow("checkPermissions", "Global action allowed", user,
                action, null, null);
            } else {
              result = AuthResult.deny("checkPermissions", "Global action denied", user, action,
                null, null);
            }
            logResult(result);
            if (!result.isAllowed()) {
              // Even if passive we need to throw an exception here, we support checking
              // effective permissions, so throw unconditionally
              throw new AccessDeniedException("Insufficient permissions (action=" +
                action.toString() + ")");
            }
          }
        }
      }
      response = AccessControlProtos.CheckPermissionsResponse.getDefaultInstance();
    } catch (IOException ioe) {
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  @Override
  public Service getService() {
    return AccessControlProtos.AccessControlService.newReflectiveService(this);
  }

  private Region getRegion(RegionCoprocessorEnvironment e) {
    return e.getRegion();
  }

  private TableName getTableName(RegionCoprocessorEnvironment e) {
    Region region = e.getRegion();
    if (region != null) {
      return getTableName(region);
    }
    return null;
  }

  private TableName getTableName(Region region) {
    HRegionInfo regionInfo = region.getRegionInfo();
    if (regionInfo != null) {
      return regionInfo.getTable();
    }
    return null;
  }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested)
      throws IOException {
    requirePermission(getActiveUser(c), "preClose", Action.ADMIN);
  }

  private void checkSystemOrSuperUser(User activeUser) throws IOException {
    // No need to check if we're not going to throw
    if (!authorizationEnabled) {
      return;
    }
    if (!Superusers.isSuperUser(activeUser)) {
      throw new AccessDeniedException("User '" + (activeUser != null ?
        activeUser.getShortName() : "null") + "' is not system or super user.");
    }
  }

  @Override
  public void preStopRegionServer(
      ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException {
    requirePermission(getActiveUser(ctx), "preStopRegionServer", Action.ADMIN);
  }

  private Map<byte[], ? extends Collection<byte[]>> makeFamilyMap(byte[] family,
      byte[] qualifier) {
    if (family == null) {
      return null;
    }

    Map<byte[], Collection<byte[]>> familyMap = new TreeMap<byte[], Collection<byte[]>>(Bytes.BYTES_COMPARATOR);
    familyMap.put(family, qualifier != null ? ImmutableSet.of(qualifier) : null);
    return familyMap;
  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
       List<TableName> tableNamesList, List<HTableDescriptor> descriptors,
       String regex) throws IOException {
    // We are delegating the authorization check to postGetTableDescriptors as we don't have
    // any concrete set of table names when a regex is present or the full list is requested.
    if (regex == null && tableNamesList != null && !tableNamesList.isEmpty()) {
      // Otherwise, if the requestor has ADMIN or CREATE privs for all listed tables, the
      // request can be granted.
      MasterServices masterServices = ctx.getEnvironment().getMasterServices();
      for (TableName tableName: tableNamesList) {
        // Skip checks for a table that does not exist
        if (!masterServices.getTableStateManager().isTablePresent(tableName))
          continue;
        requirePermission(getActiveUser(ctx), "getTableDescriptors", tableName, null, null,
            Action.ADMIN, Action.CREATE);
      }
    }
  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<HTableDescriptor> descriptors,
      String regex) throws IOException {
    // Skipping as checks in this case are already done by preGetTableDescriptors.
    if (regex == null && tableNamesList != null && !tableNamesList.isEmpty()) {
      return;
    }

    // Retains only those which passes authorization checks, as the checks weren't done as part
    // of preGetTableDescriptors.
    Iterator<HTableDescriptor> itr = descriptors.iterator();
    while (itr.hasNext()) {
      HTableDescriptor htd = itr.next();
      try {
        requirePermission(getActiveUser(ctx), "getTableDescriptors", htd.getTableName(), null, null,
            Action.ADMIN, Action.CREATE);
      } catch (AccessDeniedException e) {
        itr.remove();
      }
    }
  }

  @Override
  public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors, String regex) throws IOException {
    // Retains only those which passes authorization checks.
    Iterator<HTableDescriptor> itr = descriptors.iterator();
    while (itr.hasNext()) {
      HTableDescriptor htd = itr.next();
      try {
        requireAccess(getActiveUser(ctx), "getTableNames", htd.getTableName(), Action.values());
      } catch (AccessDeniedException e) {
        itr.remove();
      }
    }
  }

  @Override
  public void preDispatchMerge(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionA, HRegionInfo regionB) throws IOException {
    requirePermission(getActiveUser(ctx), "mergeRegions", regionA.getTable(), null, null,
      Action.ADMIN);
  }

  @Override
  public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA,
      Region regionB) throws IOException {
    requirePermission(getActiveUser(ctx), "mergeRegions", regionA.getTableDesc().getTableName(),
        null, null, Action.ADMIN);
  }

  @Override
  public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c, Region regionA,
      Region regionB, Region mergedRegion) throws IOException { }

  @Override
  public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      Region regionA, Region regionB, List<Mutation> metaEntries) throws IOException { }

  @Override
  public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      Region regionA, Region regionB, Region mergedRegion) throws IOException { }

  @Override
  public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      Region regionA, Region regionB) throws IOException { }

  @Override
  public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      Region regionA, Region regionB) throws IOException { }

  @Override
  public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException {
    requirePermission(getActiveUser(ctx), "preRollLogWriterRequest", Permission.Action.ADMIN);
  }

  @Override
  public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException { }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final Quotas quotas) throws IOException {
    requirePermission(getActiveUser(ctx), "setUserQuota", Action.ADMIN);
  }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final TableName tableName, final Quotas quotas) throws IOException {
    requirePermission(getActiveUser(ctx), "setUserTableQuota", tableName, null, null, Action.ADMIN);
  }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final String namespace, final Quotas quotas) throws IOException {
    requirePermission(getActiveUser(ctx), "setUserNamespaceQuota", Action.ADMIN);
  }

  @Override
  public void preSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final Quotas quotas) throws IOException {
    requirePermission(getActiveUser(ctx), "setTableQuota", tableName, null, null, Action.ADMIN);
  }

  @Override
  public void preSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final Quotas quotas) throws IOException {
    requirePermission(getActiveUser(ctx), "setNamespaceQuota", Action.ADMIN);
  }

  @Override
  public ReplicationEndpoint postCreateReplicationEndPoint(
      ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
    return endpoint;
  }

  @Override
  public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      List<WALEntry> entries, CellScanner cells) throws IOException {
    requirePermission(getActiveUser(ctx), "replicateLogEntries", Action.WRITE);
  }

  @Override
  public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
      List<WALEntry> entries, CellScanner cells) throws IOException {
  }

  @Override
  public void preMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             Set<HostAndPort> servers, String targetGroup) throws IOException {
    requirePermission(getActiveUser(ctx), "moveServers", Action.ADMIN);
  }

  @Override
  public void preMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
                            Set<TableName> tables, String targetGroup) throws IOException {
    requirePermission(getActiveUser(ctx), "moveTables", Action.ADMIN);
  }

  @Override
  public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                            String name) throws IOException {
    requirePermission(getActiveUser(ctx), "addRSGroup", Action.ADMIN);
  }

  @Override
  public void preRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               String name) throws IOException {
    requirePermission(getActiveUser(ctx), "removeRSGroup", Action.ADMIN);
  }

  @Override
  public void preBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                String groupName) throws IOException {
    requirePermission(getActiveUser(ctx), "balanceRSGroup", Action.ADMIN);
  }
}
