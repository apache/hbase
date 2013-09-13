/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
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
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;

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
 * RpcServer being loaded to provide
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
public class AccessController extends BaseRegionObserver
    implements MasterObserver, RegionServerObserver,
      AccessControlService.Interface, CoprocessorService {

  public static final Log LOG = LogFactory.getLog(AccessController.class);

  private static final Log AUDITLOG =
    LogFactory.getLog("SecurityLogger."+AccessController.class.getName());

  TableAuthManager authManager = null;

  // flags if we are running on a region of the _acl_ table
  boolean aclRegion = false;

  // defined only for Endpoint implementation, so it can have way to
  // access region services.
  private RegionCoprocessorEnvironment regionEnv;

  /** Mapping of scanner instances to the user who created them */
  private Map<InternalScanner,String> scannerOwners =
      new MapMaker().weakKeys().makeMap();

  void initialize(RegionCoprocessorEnvironment e) throws IOException {
    final HRegion region = e.getRegion();

    Map<byte[], ListMultimap<String,TablePermission>> tables =
        AccessControlLists.loadAll(region);
    // For each table, write out the table's permissions to the respective
    // znode for that table.
    for (Map.Entry<byte[], ListMultimap<String,TablePermission>> t:
      tables.entrySet()) {
      byte[] entry = t.getKey();
      ListMultimap<String,TablePermission> perms = t.getValue();
      byte[] serialized = AccessControlLists.writePermissionsAsBytes(perms, e.getConfiguration());
      this.authManager.getZKPermissionWatcher().writeToZookeeper(entry, serialized);
    }
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
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        if (Bytes.equals(kv.getBuffer(), kv.getFamilyOffset(),
            kv.getFamilyLength(), AccessControlLists.ACL_LIST_FAMILY, 0,
            AccessControlLists.ACL_LIST_FAMILY.length)) {
          entries.add(kv.getRow());
        }
      }
    }
    ZKPermissionWatcher zkw = this.authManager.getZKPermissionWatcher();
    Configuration conf = regionEnv.getConfiguration();
    for (byte[] entry: entries) {
      try {
        ListMultimap<String,TablePermission> perms =
          AccessControlLists.getPermissions(conf, entry);
        byte[] serialized = AccessControlLists.writePermissionsAsBytes(perms, conf);
        zkw.writeToZookeeper(entry, serialized);
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
   * @return
   */
  AuthResult permissionGranted(String request, User user, Permission.Action permRequest,
      RegionCoprocessorEnvironment e,
      Map<byte [], ? extends Collection<?>> families) {
    HRegionInfo hri = e.getRegion().getRegionInfo();
    TableName tableName = hri.getTableName();

    // 1. All users need read access to hbase:meta table.
    // this is a very common operation, so deal with it quickly.
    if (hri.isMetaRegion()) {
      if (permRequest == Permission.Action.READ) {
        return AuthResult.allow(request, "All users allowed", user,
          permRequest, tableName, families);
      }
    }

    if (user == null) {
      return AuthResult.deny(request, "No user associated with request!", null,
        permRequest, tableName, families);
    }

    // Users with CREATE/ADMIN rights need to modify hbase:meta and _acl_ table
    // e.g. When a new table is created a new entry in hbase:meta is added,
    // so the user need to be allowed to write on it.
    // e.g. When a table is removed an entry is removed from hbase:meta and _acl_
    // and the user need to be allowed to write on both tables.
    if (permRequest == Permission.Action.WRITE &&
       (hri.isMetaRegion() ||
        Bytes.equals(tableName.getName(), AccessControlLists.ACL_GLOBAL_NAME)) &&
       (authManager.authorize(user, Permission.Action.CREATE) ||
        authManager.authorize(user, Permission.Action.ADMIN)))
    {
       return AuthResult.allow(request, "Table permission granted", user,
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
                      kv.getQualifier(), permRequest)) {
                return AuthResult.deny(request, "Failed qualifier check", user,
                    permRequest, tableName, makeFamilyMap(family.getKey(), kv.getQualifier()));
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

  private void logResult(AuthResult result) {
    if (AUDITLOG.isTraceEnabled()) {
      RequestContext ctx = RequestContext.get();
      InetAddress remoteAddr = null;
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
      user = User.getCurrent();
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
  private void requirePermission(String request, TableName tableName, byte[] family, byte[] qualifier,
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
   * Authorizes that the current user has any of the given permissions for the
   * given table, column family and column qualifier.
   * @param namespace
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if user has no authorization
   */
  private void requirePermission(String request, String namespace,
      Action... permissions) throws IOException {
    User user = getActiveUser();
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorize(user, namespace, permission)) {
        result = AuthResult.allow(request, "Table permission granted", user,
                                  permission, namespace);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
                                 permission, namespace);
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
    requireGlobalPermission(request, perm, null, null);
  }

  /**
   * Authorizes that the current user has permission to perform the given
   * action on the set of table column families.
   * @param perm Action that is required
   * @param env The current coprocessor environment
   * @param families The map of column families-qualifiers.
   * @throws AccessDeniedException if the authorization check failed
   */
  private void requirePermission(String request, Permission.Action perm,
        RegionCoprocessorEnvironment env,
        Map<byte[], ? extends Collection<?>> families)
      throws IOException {
    User user = getActiveUser();
    AuthResult result = permissionGranted(request, user, perm, env, families);
    logResult(result);

    if (!result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions (table=" +
        env.getRegion().getTableDesc().getTableName()+
        ((families != null && families.size() > 0) ? ", family: " +
        result.toFamilyString() : "") + ", action=" +
        perm.toString() + ")");
    }
  }

  /**
   * Checks that the user has the given global permission. The generated
   * audit log message will contain context information for the operation
   * being authorized, based on the given parameters.
   * @param perm Action being requested
   * @param tableName Affected table name.
   * @param familyMap Affected column families.
   */
  private void requireGlobalPermission(String request, Permission.Action perm, TableName tableName,
      Map<byte[], ? extends Collection<byte[]>> familyMap) throws IOException {
    User user = getActiveUser();
    if (authManager.authorize(user, perm)) {
      logResult(AuthResult.allow(request, "Global check allowed", user, perm, tableName, familyMap));
    } else {
      logResult(AuthResult.deny(request, "Global check failed", user, perm, tableName, familyMap));
      throw new AccessDeniedException("Insufficient permissions for user '" +
          (user != null ? user.getShortName() : "null") +"' (global, action=" +
          perm.toString() + ")");
    }
  }

  /**
   * Checks that the user has the given global permission. The generated
   * audit log message will contain context information for the operation
   * being authorized, based on the given parameters.
   * @param perm Action being requested
   * @param namespace
   */
  private void requireGlobalPermission(String request, Permission.Action perm,
                                       String namespace) throws IOException {
    User user = getActiveUser();
    if (authManager.authorize(user, perm)) {
      logResult(AuthResult.allow(request, "Global check allowed", user, perm, namespace));
    } else {
      logResult(AuthResult.deny(request, "Global check failed", user, perm, namespace));
      throw new AccessDeniedException("Insufficient permissions for user '" +
          (user != null ? user.getShortName() : "null") +"' (global, action=" +
          perm.toString() + ")");
    }
  }

  /**
   * Returns <code>true</code> if the current user is allowed the given action
   * over at least one of the column qualifiers in the given column families.
   */
  private boolean hasFamilyQualifierPermission(User user,
      Permission.Action perm,
      RegionCoprocessorEnvironment env,
      Map<byte[], ? extends Set<byte[]>> familyMap)
    throws IOException {
    HRegionInfo hri = env.getRegion().getRegionInfo();
    TableName tableName = hri.getTableName();

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
    Set<byte[]> families = desc.getFamiliesKeys();
    Map<byte[], Set<byte[]>> familyMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (byte[] family: families) {
      familyMap.put(family, null);
    }
    requireGlobalPermission("createTable", Permission.Action.CREATE, desc.getTableName(), familyMap);
  }

  @Override
  public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {}

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> c,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    if (!AccessControlLists.isAclTable(desc)) {
      String owner = desc.getOwnerString();
      // default the table owner to current user, if not specified.
      if (owner == null) owner = getActiveUser().getShortName();
      UserPermission userperm = new UserPermission(Bytes.toBytes(owner), desc.getTableName(), null,
          Action.values());
      AccessControlLists.addUserPermission(c.getEnvironment().getConfiguration(), userperm);
    }
  }

  @Override
  public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {}

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
      throws IOException {
    requirePermission("deleteTable", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {}
  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {
    AccessControlLists.removeTablePermissions(c.getEnvironment().getConfiguration(), tableName);
  }
  @Override
  public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {}

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName,
      HTableDescriptor htd) throws IOException {
    requirePermission("modifyTable", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, HTableDescriptor htd) throws IOException {}

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, HTableDescriptor htd) throws IOException {
    String owner = htd.getOwnerString();
    // default the table owner to current user, if not specified.
    if (owner == null) owner = getActiveUser().getShortName();
    UserPermission userperm = new UserPermission(Bytes.toBytes(owner), htd.getTableName(), null,
        Action.values());
    AccessControlLists.addUserPermission(c.getEnvironment().getConfiguration(), userperm);
  }

  @Override
  public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, HTableDescriptor htd) throws IOException {}


  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName,
      HColumnDescriptor column) throws IOException {
    requirePermission("addColumn", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, HColumnDescriptor column) throws IOException {}
  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, HColumnDescriptor column) throws IOException {}
  @Override
  public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, HColumnDescriptor column) throws IOException {}

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName,
      HColumnDescriptor descriptor) throws IOException {
    requirePermission("modifyColumn", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, HColumnDescriptor descriptor) throws IOException {}
  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, HColumnDescriptor descriptor) throws IOException {}
  @Override
  public void postModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, HColumnDescriptor descriptor) throws IOException {}


  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName,
      byte[] col) throws IOException {
    requirePermission("deleteColumn", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, byte[] col) throws IOException {}
  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, byte[] col) throws IOException {
    AccessControlLists.removeTablePermissions(c.getEnvironment().getConfiguration(),
                                              tableName, col);
  }
  @Override
  public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, byte[] col) throws IOException {}

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
      throws IOException {
    requirePermission("enableTable", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {}
  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {}
  @Override
  public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {}

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
      throws IOException {
    if (Bytes.equals(tableName.getName(), AccessControlLists.ACL_GLOBAL_NAME)) {
      throw new AccessDeniedException("Not allowed to disable "
          + AccessControlLists.ACL_TABLE_NAME + " table.");
    }
    requirePermission("disableTable", tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {}
  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {}
  @Override
  public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {}

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
  public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo regionInfo) throws IOException {
    requirePermission("regionOffline", regionInfo.getTableName(), null, null, Action.ADMIN);
  }

  @Override
  public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c,
      HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission("balance", Permission.Action.ADMIN);
  }
  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> c, List<RegionPlan> plans)
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
  public void preMasterInitialization(
      ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
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
    requirePermission("clone", Permission.Action.ADMIN);
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
    requirePermission("restore", Permission.Action.ADMIN);
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

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
    requireGlobalPermission("createNamespace", Action.ADMIN, ns.getName());
  }

  @Override
  public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
      throws IOException {
    requireGlobalPermission("deleteNamespace", Action.ADMIN, namespace);
  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  String namespace) throws IOException {
    AccessControlLists.removeNamespacePermissions(ctx.getEnvironment().getConfiguration(),
        namespace);
    LOG.info(namespace + "entry deleted in "+AccessControlLists.ACL_TABLE_NAME+" table.");
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
    requireGlobalPermission("modifyNamespace", Action.ADMIN, ns.getName());
  }

  @Override
  public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  NamespaceDescriptor ns) throws IOException {
  }

  /* ---- RegionObserver implementation ---- */

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e)
      throws IOException {
    RegionCoprocessorEnvironment env = e.getEnvironment();
    final HRegion region = env.getRegion();
    if (region == null) {
      LOG.error("NULL region from RegionCoprocessorEnvironment in preOpen()");
    } else {
      HRegionInfo regionInfo = region.getRegionInfo();
      if (isSpecialTable(regionInfo)) {
        isSystemOrSuperUser(regionEnv.getConfiguration());
      } else {
        requirePermission("preOpen", Action.ADMIN);
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
    requirePermission("flush", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
  }

  @Override
  public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    requirePermission("split", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
  }

  @Override
  public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e,
      byte[] splitRow) throws IOException {
    requirePermission("split", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
      final Store store, final InternalScanner scanner, final ScanType scanType)
          throws IOException {
    requirePermission("compact", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
    return scanner;
  }

  @Override
  public void preCompactSelection(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Store store, final List<StoreFile> candidates) throws IOException {
    requirePermission("compact", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
  }

  @Override
  public void preGetClosestRowBefore(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final Result result)
      throws IOException {
    assert family != null;
    //noinspection PrimitiveArrayArgumentToVariableArgMethod
    requirePermission("getClosestRowBefore", Permission.Action.READ, c.getEnvironment(),
        makeFamilyMap(family, null));
  }

  @Override
  public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Get get, final List<Cell> result) throws IOException {
    /*
     if column family level checks fail, check for a qualifier level permission
     in one of the families.  If it is present, then continue with the AccessControlFilter.
      */
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User requestUser = getActiveUser();
    AuthResult authResult = permissionGranted("get", requestUser,
        Permission.Action.READ, e, get.getFamilyMap());
    if (!authResult.isAllowed()) {
      if (hasFamilyQualifierPermission(requestUser,
          Permission.Action.READ, e, get.getFamilyMap())) {
        TableName table = getTableName(e);
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
            Permission.Action.READ, authResult.getTableName(), get.getFamilyMap()));
      } else {
        logResult(authResult);
        throw new AccessDeniedException("Insufficient permissions (table=" +
          e.getRegion().getTableDesc().getTableName() + ", action=READ)");
      }
    } else {
      // log auth success
      logResult(authResult);
    }
  }

  @Override
  public boolean preExists(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Get get, final boolean exists) throws IOException {
    requirePermission("exists", Permission.Action.READ, c.getEnvironment(),
        get.getFamilyMap());
    return exists;
  }

  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Put put, final WALEdit edit, final Durability durability)
      throws IOException {
    requirePermission("put", Permission.Action.WRITE, c.getEnvironment(),
        put.getFamilyCellMap());
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
    requirePermission("delete", Permission.Action.WRITE, c.getEnvironment(),
        delete.getFamilyCellMap());
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
    Map<byte[], ? extends Collection<byte[]>> familyMap = makeFamilyMap(family, qualifier);
    requirePermission("checkAndPut", Permission.Action.READ, c.getEnvironment(), familyMap);
    requirePermission("checkAndPut", Permission.Action.WRITE, c.getEnvironment(), familyMap);
    return result;
  }

  @Override
  public boolean preCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp,
      final ByteArrayComparable comparator, final Delete delete,
      final boolean result) throws IOException {
    Map<byte[], ? extends Collection<byte[]>> familyMap = makeFamilyMap(family, qualifier);
    requirePermission("checkAndDelete", Permission.Action.READ, c.getEnvironment(), familyMap);
    requirePermission("checkAndDelete", Permission.Action.WRITE, c.getEnvironment(), familyMap);
    return result;
  }

  @Override
  public long preIncrementColumnValue(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL)
      throws IOException {
    Map<byte[], ? extends Collection<byte[]>> familyMap = makeFamilyMap(family, qualifier);
    requirePermission("incrementColumnValue", Permission.Action.WRITE, c.getEnvironment(), familyMap);
    return -1;
  }

  @Override
  public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
      throws IOException {
    requirePermission("append", Permission.Action.WRITE, c.getEnvironment(), append.getFamilyCellMap());
    return null;
  }

  @Override
  public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment)
      throws IOException {
    // Create a map of family to qualifiers.
    Map<byte[], Set<byte[]>> familyMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte [], List<Cell>> entry: increment.getFamilyCellMap().entrySet()) {
      Set<byte[]> qualifiers = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
      for (Cell cell: entry.getValue()) {
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        qualifiers.add(kv.getQualifier());
      }
      familyMap.put(entry.getKey(), qualifiers);
    }
    requirePermission("increment", Permission.Action.WRITE, c.getEnvironment(), familyMap);
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
    AuthResult authResult = permissionGranted("scannerOpen", user, Permission.Action.READ, e,
        scan.getFamilyMap());
    if (!authResult.isAllowed()) {
      if (hasFamilyQualifierPermission(user, Permission.Action.READ, e,
          scan.getFamilyMap())) {
        TableName table = getTableName(e);
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
            Permission.Action.READ, authResult.getTableName(), scan.getFamilyMap()));
      } else {
        // no table/family level perms and no qualifier level perms, reject
        logResult(authResult);
        throw new AccessDeniedException("Insufficient permissions for user '"+
            (user != null ? user.getShortName() : "null")+"' "+
            "for scanner open on table " + getTableName(e));
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
      requirePermission("preBulkLoadHFile",
          ctx.getEnvironment().getRegion().getTableDesc().getTableName(),
          el.getFirst(),
          null,
          Permission.Action.WRITE);
    }
  }

  private AuthResult hasSomeAccess(RegionCoprocessorEnvironment e, String method, Action action) throws IOException {
    User requestUser = getActiveUser();
    TableName tableName = e.getRegion().getTableDesc().getTableName();
    AuthResult authResult = permissionGranted(method, requestUser,
        action, e, Collections.EMPTY_MAP);
    if (!authResult.isAllowed()) {
      for(UserPermission userPerm:
          AccessControlLists.getUserTablePermissions(regionEnv.getConfiguration(), tableName)) {
        for(Permission.Action userAction: userPerm.getActions()) {
          if(userAction.equals(action)) {
            return AuthResult.allow(method, "Access allowed", requestUser,
                action, tableName, null, null);
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
  //TODO this should end up as a coprocessor hook
  public void prePrepareBulkLoad(RegionCoprocessorEnvironment e) throws IOException {
    AuthResult authResult = hasSomeAccess(e, "prePrepareBulkLoad", Action.WRITE);
    logResult(authResult);
    if (!authResult.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions (table=" +
        e.getRegion().getTableDesc().getTableName() + ", action=WRITE)");
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
    AuthResult authResult = hasSomeAccess(e, "preCleanupBulkLoad", Action.WRITE);
    logResult(authResult);
    if (!authResult.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions (table=" +
        e.getRegion().getTableDesc().getTableName() + ", action=WRITE)");
    }
  }

  /* ---- Protobuf AccessControlService implementation ---- */
  @Override
  public void grant(RpcController controller,
                    AccessControlProtos.GrantRequest request,
                    RpcCallback<AccessControlProtos.GrantResponse> done) {
    UserPermission perm = ProtobufUtil.toUserPermission(request.getUserPermission());
    AccessControlProtos.GrantResponse response = null;
    try {
      // verify it's only running at .acl.
      if (aclRegion) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received request to grant access permission " + perm.toString());
        }

        switch(request.getUserPermission().getPermission().getType()) {
          case Global :
          case Table :
            requirePermission("grant", perm.getTable(), perm.getFamily(),
                perm.getQualifier(), Action.ADMIN);
            break;
          case Namespace :
            requireGlobalPermission("grant", Action.ADMIN, perm.getNamespace());
        }

        AccessControlLists.addUserPermission(regionEnv.getConfiguration(), perm);
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
      ResponseConverter.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  @Override
  public void revoke(RpcController controller,
                     AccessControlProtos.RevokeRequest request,
                     RpcCallback<AccessControlProtos.RevokeResponse> done) {
    UserPermission perm = ProtobufUtil.toUserPermission(request.getUserPermission());
    AccessControlProtos.RevokeResponse response = null;
    try {
      // only allowed to be called on _acl_ region
      if (aclRegion) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received request to revoke access permission " + perm.toString());
        }

        switch(request.getUserPermission().getPermission().getType()) {
          case Global :
          case Table :
            requirePermission("revoke", perm.getTable(), perm.getFamily(),
                              perm.getQualifier(), Action.ADMIN);
            break;
          case Namespace :
            requireGlobalPermission("revoke", Action.ADMIN, perm.getNamespace());
        }

        AccessControlLists.removeUserPermission(regionEnv.getConfiguration(), perm);
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
      ResponseConverter.setControllerException(controller, ioe);
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
        List<UserPermission> perms = null;
        if(request.getType() == AccessControlProtos.Permission.Type.Table) {
          TableName table = null;
          if (request.hasTableName()) {
            table = ProtobufUtil.toTableName(request.getTableName());
          }
          requirePermission("userPermissions", table, null, null, Action.ADMIN);

          perms = AccessControlLists.getUserTablePermissions(
              regionEnv.getConfiguration(), table);
        } else if (request.getType() == AccessControlProtos.Permission.Type.Namespace) {
          perms = AccessControlLists.getUserNamespacePermissions(
              regionEnv.getConfiguration(), request.getNamespaceName().toStringUtf8());
        } else {
          perms = AccessControlLists.getUserPermissions(
              regionEnv.getConfiguration(), null);
        }
        response = ResponseConverter.buildGetUserPermissionsResponse(perms);
      } else {
        throw new CoprocessorException(AccessController.class, "This method "
            + "can only execute at " + AccessControlLists.ACL_TABLE_NAME + " table.");
      }
    } catch (IOException ioe) {
      // pass exception back up
      ResponseConverter.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  @Override
  public void checkPermissions(RpcController controller,
                               AccessControlProtos.CheckPermissionsRequest request,
                               RpcCallback<AccessControlProtos.CheckPermissionsResponse> done) {
    Permission[] permissions = new Permission[request.getPermissionCount()];
    for (int i=0; i < request.getPermissionCount(); i++) {
      permissions[i] = ProtobufUtil.toPermission(request.getPermission(i));
    }
    AccessControlProtos.CheckPermissionsResponse response = null;
    try {
      TableName tableName = regionEnv.getRegion().getTableDesc().getTableName();
      for (Permission permission : permissions) {
        if (permission instanceof TablePermission) {
          TablePermission tperm = (TablePermission) permission;
          for (Permission.Action action : permission.getActions()) {
            if (!tperm.getTable().equals(tableName)) {
              throw new CoprocessorException(AccessController.class, String.format("This method "
                  + "can only execute at the table specified in TablePermission. " +
                  "Table of the region:%s , requested table:%s", tableName,
                  tperm.getTable()));
            }

            Map<byte[], Set<byte[]>> familyMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            if (tperm.getFamily() != null) {
              if (tperm.getQualifier() != null) {
                Set<byte[]> qualifiers = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
                qualifiers.add(tperm.getQualifier());
                familyMap.put(tperm.getFamily(), qualifiers);
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
      response = AccessControlProtos.CheckPermissionsResponse.getDefaultInstance();
    } catch (IOException ioe) {
      ResponseConverter.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  @Override
  public Service getService() {
    return AccessControlProtos.AccessControlService.newReflectiveService(this);
  }

  private TableName getTableName(RegionCoprocessorEnvironment e) {
    HRegion region = e.getRegion();
    TableName tableName = null;

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
    requirePermission("preClose", Action.ADMIN);
  }

  private void isSystemOrSuperUser(Configuration conf) throws IOException {
    User user = User.getCurrent();
    if (user == null) {
      throw new IOException("Unable to obtain the current user, " +
        "authorization checks for internal operations will not work correctly!");
    }

    String currentUser = user.getShortName();
    List<String> superusers = Lists.asList(currentUser, conf.getStrings(
      AccessControlLists.SUPERUSER_CONF_KEY, new String[0]));

    User activeUser = getActiveUser();
    if (!(superusers.contains(activeUser.getShortName()))) {
      throw new AccessDeniedException("User '" + (user != null ? user.getShortName() : "null") +
        "is not system or super user.");
    }
  }

  private boolean isSpecialTable(HRegionInfo regionInfo) {
    TableName tableName = regionInfo.getTableName();
    return tableName.equals(AccessControlLists.ACL_TABLE_NAME)
        || tableName.equals(TableName.NAMESPACE_TABLE_NAME)
        || tableName.equals(TableName.META_TABLE_NAME);
  }

  @Override
  public void preStopRegionServer(
      ObserverContext<RegionServerCoprocessorEnvironment> env)
      throws IOException {
    requirePermission("preStopRegionServer", Permission.Action.ADMIN);
  }

  private Map<byte[], ? extends Collection<byte[]>> makeFamilyMap(byte[] family,
      byte[] qualifier) {
    if (family == null) {
      return null;
    }

    Map<byte[], Collection<byte[]>> familyMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    familyMap.put(family, qualifier != null ? ImmutableSet.of(qualifier) : null);
    return familyMap;
  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList,
      List<HTableDescriptor> descriptors) throws IOException {
    // If the list is empty, this is a request for all table descriptors and requires GLOBAL
    // ADMIN privs.
    if (tableNamesList == null || tableNamesList.isEmpty()) {
      requireGlobalPermission("getTableDescriptors", Permission.Action.ADMIN, null, null);
    }
    // Otherwise, if the requestor has ADMIN or CREATE privs for all listed tables, the
    // request can be granted.
    else {
      MasterServices masterServices = ctx.getEnvironment().getMasterServices();
      for (TableName tableName: tableNamesList) {
        // Do not deny if the table does not exist
        try {
          masterServices.checkTableModifiable(tableName);
        } catch (TableNotFoundException ex) {
          // Skip checks for a table that does not exist
          continue;
        } catch (TableNotDisabledException ex) {
          // We don't care about this
        }
        requirePermission("getTableDescriptors", tableName, null, null,
          Permission.Action.ADMIN, Permission.Action.CREATE);
      }
    }
  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors) throws IOException {
  }
}
