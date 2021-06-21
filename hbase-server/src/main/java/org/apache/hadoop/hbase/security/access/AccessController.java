/**
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

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.HasPermissionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.HasPermissionResponse;
import org.apache.hadoop.hbase.quotas.GlobalQuotaSettings;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.MapMaker;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

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
@CoreCoprocessor
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class AccessController implements MasterCoprocessor, RegionCoprocessor,
    RegionServerCoprocessor, AccessControlService.Interface,
    MasterObserver, RegionObserver, RegionServerObserver, EndpointObserver, BulkLoadObserver {
  // TODO: encapsulate observer functions into separate class/sub-class.

  private static final Logger LOG = LoggerFactory.getLogger(AccessController.class);

  private static final Logger AUDITLOG =
    LoggerFactory.getLogger("SecurityLogger."+AccessController.class.getName());
  private static final String CHECK_COVERING_PERM = "check_covering_perm";
  private static final String TAG_CHECK_PASSED = "tag_check_passed";
  private static final byte[] TRUE = Bytes.toBytes(true);

  private AccessChecker accessChecker;
  private ZKPermissionWatcher zkPermissionWatcher;

  /** flags if we are running on a region of the _acl_ table */
  private boolean aclRegion = false;

  /** defined only for Endpoint implementation, so it can have way to
   access region services */
  private RegionCoprocessorEnvironment regionEnv;

  /** Mapping of scanner instances to the user who created them */
  private Map<InternalScanner,String> scannerOwners =
      new MapMaker().weakKeys().makeMap();

  private Map<TableName, List<UserPermission>> tableAcls;

  /** Provider for mapping principal names to Users */
  private UserProvider userProvider;

  /** if we are active, usually false, only true if "hbase.security.authorization"
   has been set to true in site configuration */
  private boolean authorizationEnabled;

  /** if we are able to support cell ACLs */
  private boolean cellFeaturesEnabled;

  /** if we should check EXEC permissions */
  private boolean shouldCheckExecPermission;

  /** if we should terminate access checks early as soon as table or CF grants
    allow access; pre-0.98 compatible behavior */
  private boolean compatibleEarlyTermination;

  /** if we have been successfully initialized */
  private volatile boolean initialized = false;

  /** if the ACL table is available, only relevant in the master */
  private volatile boolean aclTabAvailable = false;

  public static boolean isCellAuthorizationSupported(Configuration conf) {
    return AccessChecker.isAuthorizationSupported(conf) &&
        (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS);
  }

  public Region getRegion() {
    return regionEnv != null ? regionEnv.getRegion() : null;
  }

  public AuthManager getAuthManager() {
    return accessChecker.getAuthManager();
  }

  private void initialize(RegionCoprocessorEnvironment e) throws IOException {
    final Region region = e.getRegion();
    Configuration conf = e.getConfiguration();
    Map<byte[], ListMultimap<String, UserPermission>> tables = PermissionStorage.loadAll(region);
    // For each table, write out the table's permissions to the respective
    // znode for that table.
    for (Map.Entry<byte[], ListMultimap<String, UserPermission>> t:
      tables.entrySet()) {
      byte[] entry = t.getKey();
      ListMultimap<String, UserPermission> perms = t.getValue();
      byte[] serialized = PermissionStorage.writePermissionsAsBytes(perms, conf);
      zkPermissionWatcher.writeToZookeeper(entry, serialized);
    }
    initialized = true;
  }

  /**
   * Writes all table ACLs for the tables in the given Map up into ZooKeeper
   * znodes.  This is called to synchronize ACL changes following {@code _acl_}
   * table updates.
   */
  private void updateACL(RegionCoprocessorEnvironment e,
      final Map<byte[], List<Cell>> familyMap) {
    Set<byte[]> entries = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
    for (Map.Entry<byte[], List<Cell>> f : familyMap.entrySet()) {
      List<Cell> cells = f.getValue();
      for (Cell cell: cells) {
        if (CellUtil.matchingFamily(cell, PermissionStorage.ACL_LIST_FAMILY)) {
          entries.add(CellUtil.cloneRow(cell));
        }
      }
    }
    Configuration conf = regionEnv.getConfiguration();
    byte [] currentEntry = null;
    // TODO: Here we are already on the ACL region. (And it is single
    // region) We can even just get the region from the env and do get
    // directly. The short circuit connection would avoid the RPC overhead
    // so no socket communication, req write/read ..  But we have the PB
    // to and fro conversion overhead. get req is converted to PB req
    // and results are converted to PB results 1st and then to POJOs
    // again. We could have avoided such at least in ACL table context..
    try (Table t = e.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
      for (byte[] entry : entries) {
        currentEntry = entry;
        ListMultimap<String, UserPermission> perms =
            PermissionStorage.getPermissions(conf, entry, t, null, null, null, false);
        byte[] serialized = PermissionStorage.writePermissionsAsBytes(perms, conf);
        zkPermissionWatcher.writeToZookeeper(entry, serialized);
      }
    } catch(IOException ex) {
          LOG.error("Failed updating permissions mirror for '" +
                  (currentEntry == null? "null": Bytes.toString(currentEntry)) + "'", ex);
    }
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
  private AuthResult permissionGranted(OpType opType, User user, RegionCoprocessorEnvironment e,
      Map<byte [], ? extends Collection<?>> families, Action... actions) {
    AuthResult result = null;
    for (Action action: actions) {
      result = accessChecker.permissionGranted(opType.toString(), user, action,
        e.getRegion().getRegionInfo().getTable(), families);
      if (!result.isAllowed()) {
        return result;
      }
    }
    return result;
  }

  public void requireAccess(ObserverContext<?> ctx, String request, TableName tableName,
      Action... permissions) throws IOException {
    accessChecker.requireAccess(getActiveUser(ctx), request, tableName, permissions);
  }

  public void requirePermission(ObserverContext<?> ctx, String request,
      Action perm) throws IOException {
    accessChecker.requirePermission(getActiveUser(ctx), request, null, perm);
  }

  public void requireGlobalPermission(ObserverContext<?> ctx, String request,
      Action perm, TableName tableName,
      Map<byte[], ? extends Collection<byte[]>> familyMap) throws IOException {
    accessChecker.requireGlobalPermission(getActiveUser(ctx), request, perm, tableName, familyMap,
      null);
  }

  public void requireGlobalPermission(ObserverContext<?> ctx, String request,
      Action perm, String namespace) throws IOException {
    accessChecker.requireGlobalPermission(getActiveUser(ctx),
        request, perm, namespace);
  }

  public void requireNamespacePermission(ObserverContext<?> ctx, String request, String namespace,
      Action... permissions) throws IOException {
    accessChecker.requireNamespacePermission(getActiveUser(ctx),
        request, namespace, null, permissions);
  }

  public void requireNamespacePermission(ObserverContext<?> ctx, String request, String namespace,
      TableName tableName, Map<byte[], ? extends Collection<byte[]>> familyMap,
      Action... permissions) throws IOException {
    accessChecker.requireNamespacePermission(getActiveUser(ctx),
        request, namespace, tableName, familyMap,
        permissions);
  }

  public void requirePermission(ObserverContext<?> ctx, String request, TableName tableName,
      byte[] family, byte[] qualifier, Action... permissions) throws IOException {
    accessChecker.requirePermission(getActiveUser(ctx), request,
        tableName, family, qualifier, null, permissions);
  }

  public void requireTablePermission(ObserverContext<?> ctx, String request,
      TableName tableName,byte[] family, byte[] qualifier,
      Action... permissions) throws IOException {
    accessChecker.requireTablePermission(getActiveUser(ctx),
        request, tableName, family, qualifier, permissions);
  }

  public void checkLockPermissions(ObserverContext<?> ctx, String namespace,
      TableName tableName, RegionInfo[] regionInfos, String reason)
      throws IOException {
    accessChecker.checkLockPermissions(getActiveUser(ctx),
        namespace, tableName, regionInfos, reason);
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
    RegionInfo hri = env.getRegion().getRegionInfo();
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
            if (getAuthManager().authorizeUserTable(user, tableName,
                  family.getKey(), qualifier, perm)) {
              return true;
            }
          }
        } else {
          if (getAuthManager().authorizeUserFamily(user, tableName, family.getKey(), perm)) {
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
    Map<ByteRange, List<Cell>> familyMap1 = new HashMap<>();
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
            if (!getAuthManager().authorizeCell(user, getTableName(e), cell, action)) {
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
        List<Tag> tags = new ArrayList<>();
        tags.add(new ArrayBackedTag(PermissionStorage.ACL_TAG_TYPE, perms));
        Iterator<Tag> tagIterator = PrivateCellUtil.tagsIterator(cell);
        while (tagIterator.hasNext()) {
          tags.add(tagIterator.next());
        }
        newCells.add(PrivateCellUtil.createCell(cell, tags));
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
      Iterator<Tag> tagsItr = PrivateCellUtil.tagsIterator(cellScanner.current());
      while (tagsItr.hasNext()) {
        if (tagsItr.next().getType() == PermissionStorage.ACL_TAG_TYPE) {
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

    authorizationEnabled = AccessChecker.isAuthorizationSupported(conf);
    if (!authorizationEnabled) {
      LOG.warn("AccessController has been loaded with authorization checks DISABLED!");
    }

    shouldCheckExecPermission = conf.getBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY,
      AccessControlConstants.DEFAULT_EXEC_PERMISSION_CHECKS);

    cellFeaturesEnabled = (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS);
    if (!cellFeaturesEnabled) {
      LOG.info("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS
          + " is required to persist cell ACLs. Consider setting " + HFile.FORMAT_VERSION_KEY
          + " accordingly.");
    }

    if (env instanceof MasterCoprocessorEnvironment) {
      // if running on HMaster
      MasterCoprocessorEnvironment mEnv = (MasterCoprocessorEnvironment) env;
      if (mEnv instanceof HasMasterServices) {
        MasterServices masterServices = ((HasMasterServices) mEnv).getMasterServices();
        zkPermissionWatcher = masterServices.getZKPermissionWatcher();
        accessChecker = masterServices.getAccessChecker();
      }
    } else if (env instanceof RegionServerCoprocessorEnvironment) {
      RegionServerCoprocessorEnvironment rsEnv = (RegionServerCoprocessorEnvironment) env;
      if (rsEnv instanceof HasRegionServerServices) {
        RegionServerServices rsServices =
            ((HasRegionServerServices) rsEnv).getRegionServerServices();
        zkPermissionWatcher = rsServices.getZKPermissionWatcher();
        accessChecker = rsServices.getAccessChecker();
      }
    } else if (env instanceof RegionCoprocessorEnvironment) {
      // if running at region
      regionEnv = (RegionCoprocessorEnvironment) env;
      conf.addBytesMap(regionEnv.getRegion().getTableDescriptor().getValues());
      compatibleEarlyTermination = conf.getBoolean(AccessControlConstants.CF_ATTRIBUTE_EARLY_OUT,
        AccessControlConstants.DEFAULT_ATTRIBUTE_EARLY_OUT);
      if (regionEnv instanceof HasRegionServerServices) {
        RegionServerServices rsServices =
            ((HasRegionServerServices) regionEnv).getRegionServerServices();
        zkPermissionWatcher = rsServices.getZKPermissionWatcher();
        accessChecker = rsServices.getAccessChecker();
      }
    }

    if (zkPermissionWatcher == null) {
      throw new NullPointerException("ZKPermissionWatcher is null");
    } else if (accessChecker == null) {
      throw new NullPointerException("AccessChecker is null");
    }
    // set the user-provider.
    this.userProvider = UserProvider.instantiate(env.getConfiguration());
    tableAcls = new MapMaker().weakValues().makeMap();
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
  }

  /*********************************** Observer/Service Getters ***********************************/
  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<EndpointObserver> getEndpointObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<BulkLoadObserver> getBulkLoadObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<RegionServerObserver> getRegionServerObserver() {
    return Optional.of(this);
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(
        AccessControlProtos.AccessControlService.newReflectiveService(this));
  }

  /*********************************** Observer implementations ***********************************/

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> c,
      TableDescriptor desc, RegionInfo[] regions) throws IOException {
    Set<byte[]> families = desc.getColumnFamilyNames();
    Map<byte[], Set<byte[]>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] family: families) {
      familyMap.put(family, null);
    }
    requireNamespacePermission(c, "createTable",
        desc.getTableName().getNamespaceAsString(), desc.getTableName(), familyMap, Action.ADMIN,
        Action.CREATE);
  }

  @Override
  public void postCompletedCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> c,
      final TableDescriptor desc,
      final RegionInfo[] regions) throws IOException {
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
    if (PermissionStorage.isAclTable(desc)) {
      this.aclTabAvailable = true;
    } else if (!(TableName.NAMESPACE_TABLE_NAME.equals(desc.getTableName()))) {
      if (!aclTabAvailable) {
        LOG.warn("Not adding owner permission for table " + desc.getTableName() + ". "
            + PermissionStorage.ACL_TABLE_NAME + " is not yet created. "
            + getClass().getSimpleName() + " should be configured as the first Coprocessor");
      } else {
        String owner = desc.getOwnerString();
        // default the table owner to current user, if not specified.
        if (owner == null)
          owner = getActiveUser(c).getShortName();
        final UserPermission userPermission = new UserPermission(owner,
            Permission.newBuilder(desc.getTableName()).withActions(Action.values()).build());
        // switch to the real hbase master user for doing the RPC on the ACL table
        User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try (Table table =
                c.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
              PermissionStorage.addUserPermission(c.getEnvironment().getConfiguration(),
                userPermission, table);
            }
            return null;
          }
        });
      }
    }
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
      throws IOException {
    requirePermission(c, "deleteTable",
        tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c,
      final TableName tableName) throws IOException {
    final Configuration conf = c.getEnvironment().getConfiguration();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Table table =
            c.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
          PermissionStorage.removeTablePermissions(conf, tableName, table);
        }
        return null;
      }
    });
    zkPermissionWatcher.deleteTableACLNode(tableName);
  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> c,
      final TableName tableName) throws IOException {
    requirePermission(c, "truncateTable",
        tableName, null, null, Action.ADMIN, Action.CREATE);

    final Configuration conf = c.getEnvironment().getConfiguration();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        List<UserPermission> acls =
            PermissionStorage.getUserTablePermissions(conf, tableName, null, null, null, false);
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
            try (Table table =
                ctx.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
              PermissionStorage.addUserPermission(conf, perm, table);
            }
          }
        }
        tableAcls.remove(tableName);
        return null;
      }
    });
  }

  @Override
  public TableDescriptor preModifyTable(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, TableDescriptor currentDesc, TableDescriptor newDesc)
      throws IOException {
    // TODO: potentially check if this is a add/modify/delete column operation
    requirePermission(c, "modifyTable", tableName, null, null, Action.ADMIN, Action.CREATE);
    return newDesc;
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName, final TableDescriptor htd) throws IOException {
    final Configuration conf = c.getEnvironment().getConfiguration();
    // default the table owner to current user, if not specified.
    final String owner = (htd.getOwnerString() != null) ? htd.getOwnerString() :
      getActiveUser(c).getShortName();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        UserPermission userperm = new UserPermission(owner,
            Permission.newBuilder(htd.getTableName()).withActions(Action.values()).build());
        try (Table table =
            c.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
          PermissionStorage.addUserPermission(conf, userperm, table);
        }
        return null;
      }
    });
  }

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
      throws IOException {
    requirePermission(c, "enableTable",
        tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName)
      throws IOException {
    if (Bytes.equals(tableName.getName(), PermissionStorage.ACL_GLOBAL_NAME)) {
      // We have to unconditionally disallow disable of the ACL table when we are installed,
      // even if not enforcing authorizations. We are still allowing grants and revocations,
      // checking permissions and logging audit messages, etc. If the ACL table is not
      // available we will fail random actions all over the place.
      throw new AccessDeniedException("Not allowed to disable " + PermissionStorage.ACL_TABLE_NAME
          + " table with AccessController installed");
    }
    requirePermission(c, "disableTable",
        tableName, null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx,
      final long procId) throws IOException {
    requirePermission(ctx, "abortProcedure", Action.ADMIN);
  }

  @Override
  public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    // There is nothing to do at this time after the procedure abort request was sent.
  }

  @Override
  public void preGetProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    requirePermission(ctx, "getProcedure", Action.ADMIN);
  }

  @Override
  public void preGetLocks(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    User user = getActiveUser(ctx);
    accessChecker.requirePermission(user, "getLocks", null, Action.ADMIN);
  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo region,
      ServerName srcServer, ServerName destServer) throws IOException {
    requirePermission(c, "move",
        region.getTable(), null, null, Action.ADMIN);
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo regionInfo)
      throws IOException {
    requirePermission(c, "assign",
        regionInfo.getTable(), null, null, Action.ADMIN);
  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo regionInfo)
      throws IOException {
    requirePermission(c, "unassign",
        regionInfo.getTable(), null, null, Action.ADMIN);
  }

  @Override
  public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c,
      RegionInfo regionInfo) throws IOException {
    requirePermission(c, "regionOffline",
        regionInfo.getTable(), null, null, Action.ADMIN);
  }

  @Override
  public void preSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {
    requirePermission(ctx, "setSplitOrMergeEnabled",
        Action.ADMIN);
  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission(c, "balance", Action.ADMIN);
  }

  @Override
  public void preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c,
      boolean newValue) throws IOException {
    requirePermission(c, "balanceSwitch", Action.ADMIN);
  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission(c, "shutdown", Action.ADMIN);
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    requirePermission(c, "stopMaster", Action.ADMIN);
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    try (Admin admin = ctx.getEnvironment().getConnection().getAdmin()) {
      if (!admin.tableExists(PermissionStorage.ACL_TABLE_NAME)) {
        createACLTable(admin);
      } else {
        this.aclTabAvailable = true;
      }
    }
  }
  /**
   * Create the ACL table
   * @throws IOException
   */
  private static void createACLTable(Admin admin) throws IOException {
    /** Table descriptor for ACL table */
    ColumnFamilyDescriptor cfd =
        ColumnFamilyDescriptorBuilder.newBuilder(PermissionStorage.ACL_LIST_FAMILY).
        setMaxVersions(1).
        setInMemory(true).
        setBlockCacheEnabled(true).
        setBlocksize(8 * 1024).
        setBloomFilterType(BloomType.NONE).
        setScope(HConstants.REPLICATION_SCOPE_LOCAL).build();
    TableDescriptor td =
        TableDescriptorBuilder.newBuilder(PermissionStorage.ACL_TABLE_NAME).
          setColumnFamily(cfd).build();
    admin.createTable(td);
  }

  @Override
  public void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
      throws IOException {
    // Move this ACL check to SnapshotManager#checkPermissions as part of AC deprecation.
    requirePermission(ctx, "snapshot " + snapshot.getName(),
        hTableDescriptor.getTableName(), null, null, Permission.Action.ADMIN);
  }

  @Override
  public void preListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
    User user = getActiveUser(ctx);
    if (SnapshotDescriptionUtils.isSnapshotOwner(snapshot, user)) {
      // list it, if user is the owner of snapshot
      AuthResult result = AuthResult.allow("listSnapshot " + snapshot.getName(),
          "Snapshot owner check allowed", user, null, null, null);
      AccessChecker.logResult(result);
    } else {
      accessChecker.requirePermission(user, "listSnapshot " + snapshot.getName(), null,
        Action.ADMIN);
    }
  }

  @Override
  public void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
      throws IOException {
    User user = getActiveUser(ctx);
    if (SnapshotDescriptionUtils.isSnapshotOwner(snapshot, user)
        && hTableDescriptor.getTableName().getNameAsString().equals(snapshot.getTable())) {
      // Snapshot owner is allowed to create a table with the same name as the snapshot he took
      AuthResult result = AuthResult.allow("cloneSnapshot " + snapshot.getName(),
        "Snapshot owner check allowed", user, null, hTableDescriptor.getTableName(), null);
      AccessChecker.logResult(result);
    } else {
      accessChecker.requirePermission(user, "cloneSnapshot " + snapshot.getName(), null,
        Action.ADMIN);
    }
  }

  @Override
  public void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
      throws IOException {
    User user = getActiveUser(ctx);
    if (SnapshotDescriptionUtils.isSnapshotOwner(snapshot, user)) {
      accessChecker.requirePermission(user, "restoreSnapshot " + snapshot.getName(),
        hTableDescriptor.getTableName(), null, null, null, Permission.Action.ADMIN);
    } else {
      accessChecker.requirePermission(user, "restoreSnapshot " + snapshot.getName(), null,
        Action.ADMIN);
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
      AccessChecker.logResult(result);
    } else {
      accessChecker.requirePermission(user, "deleteSnapshot " + snapshot.getName(), null,
        Action.ADMIN);
    }
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
    requireGlobalPermission(ctx, "createNamespace",
        Action.ADMIN, ns.getName());
  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
      throws IOException {
    requireGlobalPermission(ctx, "deleteNamespace",
        Action.ADMIN, namespace);
  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace) throws IOException {
    final Configuration conf = ctx.getEnvironment().getConfiguration();
    User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Table table =
            ctx.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
          PermissionStorage.removeNamespacePermissions(conf, namespace, table);
        }
        return null;
      }
    });
    zkPermissionWatcher.deleteNamespaceACLNode(namespace);
    LOG.info(namespace + " entry deleted in " + PermissionStorage.ACL_TABLE_NAME + " table.");
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
    // We require only global permission so that
    // a user with NS admin cannot altering namespace configurations. i.e. namespace quota
    requireGlobalPermission(ctx, "modifyNamespace",
        Action.ADMIN, ns.getName());
  }

  @Override
  public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
      throws IOException {
    requireNamespacePermission(ctx, "getNamespaceDescriptor",
        namespace, Action.ADMIN);
  }

  @Override
  public void postListNamespaces(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<String> namespaces) throws IOException {
    /* always allow namespace listing */
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
        accessChecker.requireNamespacePermission(user, "listNamespaces", desc.getName(), null,
          Action.ADMIN);
      } catch (AccessDeniedException e) {
        itr.remove();
      }
    }
  }

  @Override
  public void preTableFlush(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {
    // Move this ACL check to MasterFlushTableProcedureManager#checkPermissions as part of AC
    // deprecation.
    requirePermission(ctx, "flushTable", tableName,
        null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public void preSplitRegion(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final byte[] splitRow) throws IOException {
    requirePermission(ctx, "split", tableName,
        null, null, Action.ADMIN);
  }

  @Override
  public void preClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    requirePermission(ctx, "clearDeadServers", Action.ADMIN);
  }

  @Override
  public void preDecommissionRegionServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ServerName> servers, boolean offload) throws IOException {
    requirePermission(ctx, "decommissionRegionServers", Action.ADMIN);
  }

  @Override
  public void preListDecommissionedRegionServers(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    requirePermission(ctx, "listDecommissionedRegionServers",
        Action.ADMIN);
  }

  @Override
  public void preRecommissionRegionServer(ObserverContext<MasterCoprocessorEnvironment> ctx,
      ServerName server, List<byte[]> encodedRegionNames) throws IOException {
    requirePermission(ctx, "recommissionRegionServers", Action.ADMIN);
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
      RegionInfo regionInfo = region.getRegionInfo();
      if (regionInfo.getTable().isSystemTable()) {
        checkSystemOrSuperUser(getActiveUser(c));
      } else {
        requirePermission(c, "preOpen", Action.ADMIN);
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
    if (PermissionStorage.isAclRegion(region)) {
      aclRegion = true;
      try {
        initialize(env);
      } catch (IOException ex) {
        // if we can't obtain permissions, it's better to fail
        // than perform checks incorrectly
        throw new RuntimeException("Failed to initialize permissions cache", ex);
      }
    } else {
      initialized = true;
    }
  }

  @Override
  public void preFlush(ObserverContext<RegionCoprocessorEnvironment> c,
      FlushLifeCycleTracker tracker) throws IOException {
    requirePermission(c, "flush", getTableName(c.getEnvironment()),
        null, null, Action.ADMIN, Action.CREATE);
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
      CompactionRequest request) throws IOException {
    requirePermission(c, "compact", getTableName(c.getEnvironment()),
        null, null, Action.ADMIN, Action.CREATE);
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
    for (ColumnFamilyDescriptor hcd : region.getTableDescriptor().getColumnFamilies()) {
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
            Filter ourFilter = new AccessControlFilter(getAuthManager(), user, table,
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
          Filter ourFilter = new AccessControlFilter(getAuthManager(), user, table,
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

    AccessChecker.logResult(authResult);
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
    AuthResult authResult = permissionGranted(OpType.PUT,
        user, env, families, Action.WRITE);
    AccessChecker.logResult(authResult);
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
    AuthResult authResult = permissionGranted(OpType.DELETE,
        user, env, families, Action.WRITE);
    AccessChecker.logResult(authResult);
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
          long timestamp;
          if (m instanceof Put) {
            checkForReservedTagPresence(user, m);
            opType = OpType.PUT;
            timestamp = m.getTimestamp();
          } else if (m instanceof Delete) {
            opType = OpType.DELETE;
            timestamp = m.getTimestamp();
          } else if (m instanceof Increment) {
            opType = OpType.INCREMENT;
            timestamp = ((Increment) m).getTimeRange().getMax();
          } else if (m instanceof Append) {
            opType = OpType.APPEND;
            timestamp = ((Append) m).getTimeRange().getMax();
          } else {
            // If the operation type is not Put/Delete/Increment/Append, do nothing
            continue;
          }
          AuthResult authResult = null;
          if (checkCoveringPermission(user, opType, c.getEnvironment(), m.getRow(),
            m.getFamilyCellMap(), timestamp, Action.WRITE)) {
            authResult = AuthResult.allow(opType.toString(), "Covering cell set",
              user, Action.WRITE, table, m.getFamilyCellMap());
          } else {
            authResult = AuthResult.deny(opType.toString(), "Covering cell set",
              user, Action.WRITE, table, m.getFamilyCellMap());
          }
          AccessChecker.logResult(authResult);
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
      final CompareOperator op,
      final ByteArrayComparable comparator, final Put put,
      final boolean result) throws IOException {
    User user = getActiveUser(c);
    checkForReservedTagPresence(user, put);

    // Require READ and WRITE permissions on the table, CF, and KV to update
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<byte[]>> families = makeFamilyMap(family, qualifier);
    AuthResult authResult = permissionGranted(OpType.CHECK_AND_PUT,
        user, env, families, Action.READ, Action.WRITE);
    AccessChecker.logResult(authResult);
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
      final CompareOperator opp, final ByteArrayComparable comparator, final Put put,
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
        authResult = AuthResult.allow(OpType.CHECK_AND_PUT.toString(),
            "Covering cell set", user, Action.READ, table, families);
      } else {
        authResult = AuthResult.deny(OpType.CHECK_AND_PUT.toString(),
            "Covering cell set", user, Action.READ, table, families);
      }
      AccessChecker.logResult(authResult);
      if (authorizationEnabled && !authResult.isAllowed()) {
        throw new AccessDeniedException("Insufficient permissions " + authResult.toContextString());
      }
    }
    return result;
  }

  @Override
  public boolean preCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOperator op,
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
    AuthResult authResult = permissionGranted(
        OpType.CHECK_AND_DELETE, user, env, families, Action.READ, Action.WRITE);
    AccessChecker.logResult(authResult);
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
      final ObserverContext<RegionCoprocessorEnvironment> c, final byte[] row,
      final byte[] family, final byte[] qualifier, final CompareOperator op,
      final ByteArrayComparable comparator, final Delete delete, final boolean result)
      throws IOException {
    if (delete.getAttribute(CHECK_COVERING_PERM) != null) {
      // We had failure with table, cf and q perm checks and now giving a chance for cell
      // perm check
      TableName table = c.getEnvironment().getRegion().getRegionInfo().getTable();
      Map<byte[], ? extends Collection<byte[]>> families = makeFamilyMap(family, qualifier);
      AuthResult authResult = null;
      User user = getActiveUser(c);
      if (checkCoveringPermission(user, OpType.CHECK_AND_DELETE, c.getEnvironment(),
          row, families, HConstants.LATEST_TIMESTAMP, Action.READ)) {
        authResult = AuthResult.allow(OpType.CHECK_AND_DELETE.toString(),
            "Covering cell set", user, Action.READ, table, families);
      } else {
        authResult = AuthResult.deny(OpType.CHECK_AND_DELETE.toString(),
            "Covering cell set", user, Action.READ, table, families);
      }
      AccessChecker.logResult(authResult);
      if (authorizationEnabled && !authResult.isAllowed()) {
        throw new AccessDeniedException("Insufficient permissions " + authResult.toContextString());
      }
    }
    return result;
  }

  @Override
  public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
      throws IOException {
    User user = getActiveUser(c);
    checkForReservedTagPresence(user, append);

    // Require WRITE permission to the table, CF, and the KV to be appended
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<Cell>> families = append.getFamilyCellMap();
    AuthResult authResult = permissionGranted(OpType.APPEND, user,
        env, families, Action.WRITE);
    AccessChecker.logResult(authResult);
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
  public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment)
      throws IOException {
    User user = getActiveUser(c);
    checkForReservedTagPresence(user, increment);

    // Require WRITE permission to the table, CF, and the KV to be replaced by
    // the incremented value
    RegionCoprocessorEnvironment env = c.getEnvironment();
    Map<byte[],? extends Collection<Cell>> families = increment.getFamilyCellMap();
    AuthResult authResult = permissionGranted(OpType.INCREMENT,
        user, env, families, Action.WRITE);
    AccessChecker.logResult(authResult);
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
  public List<Pair<Cell, Cell>> postIncrementBeforeWAL(
      ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation mutation,
      List<Pair<Cell, Cell>> cellPairs) throws IOException {
    // If the HFile version is insufficient to persist tags, we won't have any
    // work to do here
    if (!cellFeaturesEnabled || mutation.getACL() == null) {
      return cellPairs;
    }
    return cellPairs.stream().map(pair -> new Pair<>(pair.getFirst(),
        createNewCellWithTags(mutation, pair.getFirst(), pair.getSecond())))
        .collect(Collectors.toList());
  }

  @Override
  public List<Pair<Cell, Cell>> postAppendBeforeWAL(
      ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation mutation,
      List<Pair<Cell, Cell>> cellPairs) throws IOException {
    // If the HFile version is insufficient to persist tags, we won't have any
    // work to do here
    if (!cellFeaturesEnabled || mutation.getACL() == null) {
      return cellPairs;
    }
    return cellPairs.stream().map(pair -> new Pair<>(pair.getFirst(),
        createNewCellWithTags(mutation, pair.getFirst(), pair.getSecond())))
        .collect(Collectors.toList());
  }

  private Cell createNewCellWithTags(Mutation mutation, Cell oldCell, Cell newCell) {
    // As Increment and Append operations have already copied the tags of oldCell to the newCell,
    // there is no need to rewrite them again. Just extract non-acl tags of newCell if we need to
    // add a new acl tag for the cell. Actually, oldCell is useless here.
    List<Tag> tags = Lists.newArrayList();
    if (newCell != null) {
      Iterator<Tag> tagIterator = PrivateCellUtil.tagsIterator(newCell);
      while (tagIterator.hasNext()) {
        Tag tag = tagIterator.next();
        if (tag.getType() != PermissionStorage.ACL_TAG_TYPE) {
          // Not an ACL tag, just carry it through
          if (LOG.isTraceEnabled()) {
            LOG.trace("Carrying forward tag from " + newCell + ": type " + tag.getType()
                + " length " + tag.getValueLength());
          }
          tags.add(tag);
        }
      }
    }

    // We have checked the ACL tag of mutation is not null.
    // So that the tags could not be empty.
    tags.add(new ArrayBackedTag(PermissionStorage.ACL_TAG_TYPE, mutation.getACL()));
    return PrivateCellUtil.createCell(newCell, tags);
  }

  @Override
  public void preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan)
      throws IOException {
    internalPreRead(c, scan, OpType.SCAN);
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
  @Deprecated // Removed in later versions by HBASE-25277
  public boolean postScannerFilterRow(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final Cell curRowCell, final boolean hasMore) throws IOException {
    // 'default' in RegionObserver might do unnecessary copy for Off heap backed Cells.
    return hasMore;
  }

  /**
   * Verify, when servicing an RPC, that the caller is the scanner owner.
   * If so, we assume that access control is correctly enforced based on
   * the checks performed in preScannerOpen()
   */
  private void requireScannerOwner(InternalScanner s) throws AccessDeniedException {
    if (!RpcServer.isInRpcCallContext()) {
      return;
    }
    String requestUserName = RpcServer.getRequestUserName().orElse(null);
    String owner = scannerOwners.get(s);
    if (authorizationEnabled && owner != null && !owner.equals(requestUserName)) {
      throw new AccessDeniedException("User '"+ requestUserName +"' is not the scanner owner!");
    }
  }

  /**
   * Verifies user has CREATE or ADMIN privileges on
   * the Column Families involved in the bulkLoadHFile
   * request. Specific Column Write privileges are presently
   * ignored.
   */
  @Override
  public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
      List<Pair<byte[], String>> familyPaths) throws IOException {
    User user = getActiveUser(ctx);
    for(Pair<byte[],String> el : familyPaths) {
      accessChecker.requirePermission(user, "preBulkLoadHFile",
        ctx.getEnvironment().getRegion().getTableDescriptor().getTableName(), el.getFirst(), null,
        null, Action.ADMIN, Action.CREATE);
    }
  }

  /**
   * Authorization check for
   * SecureBulkLoadProtocol.prepareBulkLoad()
   * @param ctx the context
   * @throws IOException
   */
  @Override
  public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx)
  throws IOException {
    requireAccess(ctx, "prePrepareBulkLoad",
        ctx.getEnvironment().getRegion().getTableDescriptor().getTableName(), Action.ADMIN,
        Action.CREATE);
  }

  /**
   * Authorization security check for
   * SecureBulkLoadProtocol.cleanupBulkLoad()
   * @param ctx the context
   * @throws IOException
   */
  @Override
  public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx)
  throws IOException {
    requireAccess(ctx, "preCleanupBulkLoad",
        ctx.getEnvironment().getRegion().getTableDescriptor().getTableName(), Action.ADMIN,
        Action.CREATE);
  }

  /* ---- EndpointObserver implementation ---- */

  @Override
  public Message preEndpointInvocation(ObserverContext<RegionCoprocessorEnvironment> ctx,
      Service service, String methodName, Message request) throws IOException {
    // Don't intercept calls to our own AccessControlService, we check for
    // appropriate permissions in the service handlers
    if (shouldCheckExecPermission && !(service instanceof AccessControlService)) {
      requirePermission(ctx,
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

  /**
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link Admin#grant(UserPermission, boolean)} instead.
   * @see Admin#grant(UserPermission, boolean)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21739">HBASE-21739</a>
   */
  @Deprecated
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
        User caller = RpcServer.getRequestUser().orElse(null);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received request from {} to grant access permission {}",
            caller.getName(), perm.toString());
        }
        preGrantOrRevoke(caller, "grant", perm);

        // regionEnv is set at #start. Hopefully not null at this point.
        regionEnv.getConnection().getAdmin().grant(
          new UserPermission(perm.getUser(), perm.getPermission()),
          request.getMergeExistingPermissions());
        if (AUDITLOG.isTraceEnabled()) {
          // audit log should store permission changes in addition to auth results
          AUDITLOG.trace("Granted permission " + perm.toString());
        }
      } else {
        throw new CoprocessorException(AccessController.class, "This method "
            + "can only execute at " + PermissionStorage.ACL_TABLE_NAME + " table.");
      }
      response = AccessControlProtos.GrantResponse.getDefaultInstance();
    } catch (IOException ioe) {
      // pass exception back up
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  /**
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use {@link Admin#revoke(UserPermission)}
   *   instead.
   * @see Admin#revoke(UserPermission)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21739">HBASE-21739</a>
   */
  @Deprecated
  @Override
  public void revoke(RpcController controller, AccessControlProtos.RevokeRequest request,
      RpcCallback<AccessControlProtos.RevokeResponse> done) {
    final UserPermission perm = AccessControlUtil.toUserPermission(request.getUserPermission());
    AccessControlProtos.RevokeResponse response = null;
    try {
      // only allowed to be called on _acl_ region
      if (aclRegion) {
        if (!initialized) {
          throw new CoprocessorException("AccessController not yet initialized");
        }
        User caller = RpcServer.getRequestUser().orElse(null);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received request from {} to revoke access permission {}",
            caller.getShortName(), perm.toString());
        }
        preGrantOrRevoke(caller, "revoke", perm);
        // regionEnv is set at #start. Hopefully not null here.
        regionEnv.getConnection().getAdmin()
            .revoke(new UserPermission(perm.getUser(), perm.getPermission()));
        if (AUDITLOG.isTraceEnabled()) {
          // audit log should record all permission changes
          AUDITLOG.trace("Revoked permission " + perm.toString());
        }
      } else {
        throw new CoprocessorException(AccessController.class, "This method "
            + "can only execute at " + PermissionStorage.ACL_TABLE_NAME + " table.");
      }
      response = AccessControlProtos.RevokeResponse.getDefaultInstance();
    } catch (IOException ioe) {
      // pass exception back up
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  /**
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link Admin#getUserPermissions(GetUserPermissionsRequest)} instead.
   * @see Admin#getUserPermissions(GetUserPermissionsRequest)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21911">HBASE-21911</a>
   */
  @Deprecated
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
        User caller = RpcServer.getRequestUser().orElse(null);
        final String userName = request.hasUserName() ? request.getUserName().toStringUtf8() : null;
        final String namespace =
            request.hasNamespaceName() ? request.getNamespaceName().toStringUtf8() : null;
        final TableName table =
            request.hasTableName() ? ProtobufUtil.toTableName(request.getTableName()) : null;
        final byte[] cf =
            request.hasColumnFamily() ? request.getColumnFamily().toByteArray() : null;
        final byte[] cq =
            request.hasColumnQualifier() ? request.getColumnQualifier().toByteArray() : null;
        preGetUserPermissions(caller, userName, namespace, table, cf, cq);
        GetUserPermissionsRequest getUserPermissionsRequest = null;
        if (request.getType() == AccessControlProtos.Permission.Type.Table) {
          getUserPermissionsRequest = GetUserPermissionsRequest.newBuilder(table).withFamily(cf)
              .withQualifier(cq).withUserName(userName).build();
        } else if (request.getType() == AccessControlProtos.Permission.Type.Namespace) {
          getUserPermissionsRequest =
              GetUserPermissionsRequest.newBuilder(namespace).withUserName(userName).build();
        } else {
          getUserPermissionsRequest =
              GetUserPermissionsRequest.newBuilder().withUserName(userName).build();
        }
        List<UserPermission> perms =
            regionEnv.getConnection().getAdmin().getUserPermissions(getUserPermissionsRequest);
        response = AccessControlUtil.buildGetUserPermissionsResponse(perms);
      } else {
        throw new CoprocessorException(AccessController.class, "This method "
            + "can only execute at " + PermissionStorage.ACL_TABLE_NAME + " table.");
      }
    } catch (IOException ioe) {
      // pass exception back up
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  /**
   * @deprecated since 2.2.0 and will be removed 4.0.0. Use {@link Admin#hasUserPermissions(List)}
   *   instead.
   * @see Admin#hasUserPermissions(List)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-22117">HBASE-22117</a>
   */
  @Deprecated
  @Override
  public void checkPermissions(RpcController controller,
      AccessControlProtos.CheckPermissionsRequest request,
      RpcCallback<AccessControlProtos.CheckPermissionsResponse> done) {
    AccessControlProtos.CheckPermissionsResponse response = null;
    try {
      User user = RpcServer.getRequestUser().orElse(null);
      TableName tableName = regionEnv.getRegion().getTableDescriptor().getTableName();
      List<Permission> permissions = new ArrayList<>();
      for (int i = 0; i < request.getPermissionCount(); i++) {
        Permission permission = AccessControlUtil.toPermission(request.getPermission(i));
        permissions.add(permission);
        if (permission instanceof TablePermission) {
          TablePermission tperm = (TablePermission) permission;
          if (!tperm.getTableName().equals(tableName)) {
            throw new CoprocessorException(AccessController.class,
                String.format(
                  "This method can only execute at the table specified in "
                      + "TablePermission. Table of the region:%s , requested table:%s",
                  tableName, tperm.getTableName()));
          }
        }
      }
      for (Permission permission : permissions) {
        boolean hasPermission =
            accessChecker.hasUserPermission(user, "checkPermissions", permission);
        if (!hasPermission) {
          throw new AccessDeniedException("Insufficient permissions " + permission.toString());
        }
      }
      response = AccessControlProtos.CheckPermissionsResponse.getDefaultInstance();
    } catch (IOException ioe) {
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    done.run(response);
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
    RegionInfo regionInfo = region.getRegionInfo();
    if (regionInfo != null) {
      return regionInfo.getTable();
    }
    return null;
  }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested)
      throws IOException {
    requirePermission(c, "preClose", Action.ADMIN);
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
    requirePermission(ctx, "preStopRegionServer", Action.ADMIN);
  }

  private Map<byte[], ? extends Collection<byte[]>> makeFamilyMap(byte[] family,
      byte[] qualifier) {
    if (family == null) {
      return null;
    }

    Map<byte[], Collection<byte[]>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    familyMap.put(family, qualifier != null ? ImmutableSet.of(qualifier) : null);
    return familyMap;
  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
       List<TableName> tableNamesList, List<TableDescriptor> descriptors,
       String regex) throws IOException {
    // We are delegating the authorization check to postGetTableDescriptors as we don't have
    // any concrete set of table names when a regex is present or the full list is requested.
    if (regex == null && tableNamesList != null && !tableNamesList.isEmpty()) {
      // Otherwise, if the requestor has ADMIN or CREATE privs for all listed tables, the
      // request can be granted.
      TableName [] sns = null;
      try (Admin admin = ctx.getEnvironment().getConnection().getAdmin()) {
        sns = admin.listTableNames();
        if (sns == null) return;
        for (TableName tableName: tableNamesList) {
          // Skip checks for a table that does not exist
          if (!admin.tableExists(tableName)) continue;
          requirePermission(ctx, "getTableDescriptors", tableName, null, null,
            Action.ADMIN, Action.CREATE);
        }
      }
    }
  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<TableDescriptor> descriptors,
      String regex) throws IOException {
    // Skipping as checks in this case are already done by preGetTableDescriptors.
    if (regex == null && tableNamesList != null && !tableNamesList.isEmpty()) {
      return;
    }

    // Retains only those which passes authorization checks, as the checks weren't done as part
    // of preGetTableDescriptors.
    Iterator<TableDescriptor> itr = descriptors.iterator();
    while (itr.hasNext()) {
      TableDescriptor htd = itr.next();
      try {
        requirePermission(ctx, "getTableDescriptors", htd.getTableName(), null, null,
            Action.ADMIN, Action.CREATE);
      } catch (AccessDeniedException e) {
        itr.remove();
      }
    }
  }

  @Override
  public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableDescriptor> descriptors, String regex) throws IOException {
    // Retains only those which passes authorization checks.
    Iterator<TableDescriptor> itr = descriptors.iterator();
    while (itr.hasNext()) {
      TableDescriptor htd = itr.next();
      try {
        requireAccess(ctx, "getTableNames", htd.getTableName(), Action.values());
      } catch (AccessDeniedException e) {
        itr.remove();
      }
    }
  }

  @Override
  public void preMergeRegions(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                              final RegionInfo[] regionsToMerge) throws IOException {
    requirePermission(ctx, "mergeRegions", regionsToMerge[0].getTable(), null, null,
      Action.ADMIN);
  }

  @Override
  public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException {
    requirePermission(ctx, "preRollLogWriterRequest", Permission.Action.ADMIN);
  }

  @Override
  public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException { }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final GlobalQuotaSettings quotas) throws IOException {
    requirePermission(ctx, "setUserQuota", Action.ADMIN);
  }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final TableName tableName, final GlobalQuotaSettings quotas)
          throws IOException {
    requirePermission(ctx, "setUserTableQuota", tableName, null, null, Action.ADMIN);
  }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final String namespace, final GlobalQuotaSettings quotas)
          throws IOException {
    requirePermission(ctx, "setUserNamespaceQuota", Action.ADMIN);
  }

  @Override
  public void preSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final GlobalQuotaSettings quotas) throws IOException {
    requirePermission(ctx, "setTableQuota", tableName, null, null, Action.ADMIN);
  }

  @Override
  public void preSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final GlobalQuotaSettings quotas) throws IOException {
    requirePermission(ctx, "setNamespaceQuota", Action.ADMIN);
  }

  @Override
  public void preSetRegionServerQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String regionServer, GlobalQuotaSettings quotas) throws IOException {
    requirePermission(ctx, "setRegionServerQuota", Action.ADMIN);
  }

  @Override
  public ReplicationEndpoint postCreateReplicationEndPoint(
      ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
    return endpoint;
  }

  @Override
  public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException {
    requirePermission(ctx, "replicateLogEntries", Action.WRITE);
  }
  
  @Override
  public void  preClearCompactionQueues(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
          throws IOException {
    requirePermission(ctx, "preClearCompactionQueues", Permission.Action.ADMIN);
  }

  @Override
  public void preAddReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId, ReplicationPeerConfig peerConfig) throws IOException {
    requirePermission(ctx, "addReplicationPeer", Action.ADMIN);
  }

  @Override
  public void preRemoveReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {
    requirePermission(ctx, "removeReplicationPeer", Action.ADMIN);
  }

  @Override
  public void preEnableReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {
    requirePermission(ctx, "enableReplicationPeer", Action.ADMIN);
  }

  @Override
  public void preDisableReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {
    requirePermission(ctx, "disableReplicationPeer", Action.ADMIN);
  }

  @Override
  public void preGetReplicationPeerConfig(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {
    requirePermission(ctx, "getReplicationPeerConfig", Action.ADMIN);
  }

  @Override
  public void preUpdateReplicationPeerConfig(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId,
      ReplicationPeerConfig peerConfig) throws IOException {
    requirePermission(ctx, "updateReplicationPeerConfig", Action.ADMIN);
  }

  @Override
  public void preListReplicationPeers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String regex) throws IOException {
    requirePermission(ctx, "listReplicationPeers", Action.ADMIN);
  }

  @Override
  public void preRequestLock(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace,
      TableName tableName, RegionInfo[] regionInfos, String description) throws IOException {
    // There are operations in the CREATE and ADMIN domain which may require lock, READ
    // or WRITE. So for any lock request, we check for these two perms irrespective of lock type.
    String reason = String.format("Description=%s", description);
    checkLockPermissions(ctx, namespace, tableName, regionInfos, reason);
  }

  @Override
  public void preLockHeartbeat(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, String description) throws IOException {
    checkLockPermissions(ctx, null, tableName, null, description);
  }

  @Override
  public void preExecuteProcedures(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
      throws IOException {
    checkSystemOrSuperUser(getActiveUser(ctx));
  }

  @Override
  public void preSwitchRpcThrottle(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean enable) throws IOException {
    requirePermission(ctx, "switchRpcThrottle", Action.ADMIN);
  }

  @Override
  public void preIsRpcThrottleEnabled(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    requirePermission(ctx, "isRpcThrottleEnabled", Action.ADMIN);
  }

  @Override
  public void preSwitchExceedThrottleQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean enable) throws IOException {
    requirePermission(ctx, "switchExceedThrottleQuota", Action.ADMIN);
  }

  /**
   * Returns the active user to which authorization checks should be applied.
   * If we are in the context of an RPC call, the remote user is used,
   * otherwise the currently logged in user is used.
   */
  private User getActiveUser(ObserverContext<?> ctx) throws IOException {
    // for non-rpc handling, fallback to system user
    Optional<User> optionalUser = ctx.getCaller();
    if (optionalUser.isPresent()) {
      return optionalUser.get();
    }
    return userProvider.getCurrent();
  }

  /**
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link Admin#hasUserPermissions(String, List)} instead.
   * @see Admin#hasUserPermissions(String, List)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-22117">HBASE-22117</a>
   */
  @Deprecated
  @Override
  public void hasPermission(RpcController controller, HasPermissionRequest request,
      RpcCallback<HasPermissionResponse> done) {
    // Converts proto to a TablePermission object.
    TablePermission tPerm = AccessControlUtil.toTablePermission(request.getTablePermission());
    // Check input user name
    if (!request.hasUserName()) {
      throw new IllegalStateException("Input username cannot be empty");
    }
    final String inputUserName = request.getUserName().toStringUtf8();
    AccessControlProtos.HasPermissionResponse response = null;
    try {
      User caller = RpcServer.getRequestUser().orElse(null);
      List<Permission> permissions = Lists.newArrayList(tPerm);
      preHasUserPermissions(caller, inputUserName, permissions);
      boolean hasPermission = regionEnv.getConnection().getAdmin()
          .hasUserPermissions(inputUserName, permissions).get(0);
      response = ResponseConverter.buildHasPermissionResponse(hasPermission);
    } catch (IOException ioe) {
      ResponseConverter.setControllerException(controller, ioe);
    }
    done.run(response);
  }

  @Override
  public void preGrant(ObserverContext<MasterCoprocessorEnvironment> ctx,
      UserPermission userPermission, boolean mergeExistingPermissions) throws IOException {
    preGrantOrRevoke(getActiveUser(ctx), "grant", userPermission);
  }

  @Override
  public void preRevoke(ObserverContext<MasterCoprocessorEnvironment> ctx,
      UserPermission userPermission) throws IOException {
    preGrantOrRevoke(getActiveUser(ctx), "revoke", userPermission);
  }

  private void preGrantOrRevoke(User caller, String request, UserPermission userPermission)
      throws IOException {
    switch (userPermission.getPermission().scope) {
      case GLOBAL:
        accessChecker.requireGlobalPermission(caller, request, Action.ADMIN, "");
        break;
      case NAMESPACE:
        NamespacePermission namespacePerm = (NamespacePermission) userPermission.getPermission();
        accessChecker.requireNamespacePermission(caller, request, namespacePerm.getNamespace(),
          null, Action.ADMIN);
        break;
      case TABLE:
        TablePermission tablePerm = (TablePermission) userPermission.getPermission();
        accessChecker.requirePermission(caller, request, tablePerm.getTableName(),
          tablePerm.getFamily(), tablePerm.getQualifier(), null, Action.ADMIN);
        break;
      default:
    }
    if (!Superusers.isSuperUser(caller)) {
      accessChecker.performOnSuperuser(request, caller, userPermission.getUser());
    }
  }

  @Override
  public void preGetUserPermissions(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String userName, String namespace, TableName tableName, byte[] family, byte[] qualifier)
      throws IOException {
    preGetUserPermissions(getActiveUser(ctx), userName, namespace, tableName, family, qualifier);
  }

  private void preGetUserPermissions(User caller, String userName, String namespace,
      TableName tableName, byte[] family, byte[] qualifier) throws IOException {
    if (tableName != null) {
      accessChecker.requirePermission(caller, "getUserPermissions", tableName, family, qualifier,
        userName, Action.ADMIN);
    } else if (namespace != null) {
      accessChecker.requireNamespacePermission(caller, "getUserPermissions", namespace, userName,
        Action.ADMIN);
    } else {
      accessChecker.requirePermission(caller, "getUserPermissions", userName, Action.ADMIN);
    }
  }

  @Override
  public void preHasUserPermissions(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String userName, List<Permission> permissions) throws IOException {
    preHasUserPermissions(getActiveUser(ctx), userName, permissions);
  }

  private void preHasUserPermissions(User caller, String userName, List<Permission> permissions)
      throws IOException {
    String request = "hasUserPermissions";
    for (Permission permission : permissions) {
      if (!caller.getShortName().equals(userName)) {
        // User should have admin privilege if checking permission for other users
        if (permission instanceof TablePermission) {
          TablePermission tPerm = (TablePermission) permission;
          accessChecker.requirePermission(caller, request, tPerm.getTableName(), tPerm.getFamily(),
            tPerm.getQualifier(), userName, Action.ADMIN);
        } else if (permission instanceof NamespacePermission) {
          NamespacePermission nsPerm = (NamespacePermission) permission;
          accessChecker.requireNamespacePermission(caller, request, nsPerm.getNamespace(), userName,
            Action.ADMIN);
        } else {
          accessChecker.requirePermission(caller, request, userName, Action.ADMIN);
        }
      } else {
        // User don't need ADMIN privilege for self check.
        // Setting action as null in AuthResult to display empty action in audit log
        AuthResult result;
        if (permission instanceof TablePermission) {
          TablePermission tPerm = (TablePermission) permission;
          result = AuthResult.allow(request, "Self user validation allowed", caller, null,
            tPerm.getTableName(), tPerm.getFamily(), tPerm.getQualifier());
        } else if (permission instanceof NamespacePermission) {
          NamespacePermission nsPerm = (NamespacePermission) permission;
          result = AuthResult.allow(request, "Self user validation allowed", caller, null,
            nsPerm.getNamespace());
        } else {
          result = AuthResult.allow(request, "Self user validation allowed", caller, null, null,
            null, null);
        }
        AccessChecker.logResult(result);
      }
    }
  }
}
