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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclHelper.PathHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Set HDFS ACLs to hFiles to make HBase granted users have permission to scan snapshot
 * <p>
 * To use this feature, please mask sure HDFS config:
 * <ul>
 * <li>dfs.permissions.enabled = true</li>
 * <li>fs.permissions.umask-mode = 027 (or smaller umask than 027)</li>
 * </ul>
 * </p>
 * <p>
 * The implementation of this feature is as followings:
 * <ul>
 * <li>For common directories such as 'data' and 'archive', set other permission to '--x' to make
 * everyone have the permission to access the directory.</li>
 * <li>For namespace or table directories such as 'data/ns/table', 'archive/ns/table' and
 * '.hbase-snapshot/snapshotName', set user 'r-x' access acl and 'r-x' default acl when following
 * operations happen:
 * <ul>
 * <li>grant user with global, namespace or table permission;</li>
 * <li>revoke user from global, namespace or table;</li>
 * <li>snapshot table;</li>
 * <li>truncate table;</li>
 * </ul>
 * </li>
 * <li>Note: Because snapshots are at table level, so this feature just considers users with global,
 * namespace or table permissions, ignores users with table CF or cell permissions.</li>
 * </ul>
 * </p>
 */
@CoreCoprocessor
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class SnapshotScannerHDFSAclController implements MasterCoprocessor, MasterObserver {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotScannerHDFSAclController.class);

  private SnapshotScannerHDFSAclHelper hdfsAclHelper = null;
  private PathHelper pathHelper = null;
  private FileSystem fs = null;
  private volatile boolean initialized = false;
  /** Provider for mapping principal names to Users */
  private UserProvider userProvider;

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> c)
      throws IOException {
    if (c.getEnvironment().getConfiguration()
        .getBoolean(SnapshotScannerHDFSAclHelper.USER_SCAN_SNAPSHOT_ENABLE, false)) {
      MasterCoprocessorEnvironment mEnv = c.getEnvironment();
      if (!(mEnv instanceof HasMasterServices)) {
        throw new IOException("Does not implement HMasterServices");
      }
      MasterServices masterServices = ((HasMasterServices) mEnv).getMasterServices();
      hdfsAclHelper = new SnapshotScannerHDFSAclHelper(masterServices.getConfiguration(),
          masterServices.getConnection());
      pathHelper = hdfsAclHelper.getPathHelper();
      fs = pathHelper.getFileSystem();
      hdfsAclHelper.setCommonDirectoryPermission();
      initialized = true;
      userProvider = UserProvider.instantiate(c.getEnvironment().getConfiguration());
    } else {
      LOG.warn("Try to initialize the coprocessor SnapshotScannerHDFSAclController but failure "
          + "because the config " + SnapshotScannerHDFSAclHelper.USER_SCAN_SNAPSHOT_ENABLE
          + " is false.");
    }
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
    if (checkInitialized()) {
      try (Admin admin = c.getEnvironment().getConnection().getAdmin()) {
        if (admin.tableExists(PermissionStorage.ACL_TABLE_NAME)) {
          // Check if hbase acl table has 'm' CF, if not, add 'm' CF
          TableDescriptor tableDescriptor = admin.getDescriptor(PermissionStorage.ACL_TABLE_NAME);
          boolean containHdfsAclFamily =
              Arrays.stream(tableDescriptor.getColumnFamilies()).anyMatch(family -> Bytes
                  .equals(family.getName(), SnapshotScannerHDFSAclStorage.HDFS_ACL_FAMILY));
          if (!containHdfsAclFamily) {
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDescriptor)
                .setColumnFamily(ColumnFamilyDescriptorBuilder
                    .newBuilder(SnapshotScannerHDFSAclStorage.HDFS_ACL_FAMILY).build());
            admin.modifyTable(builder.build());
          }
        } else {
          throw new TableNotFoundException("Table " + PermissionStorage.ACL_TABLE_NAME
              + " is not created yet. Please check if " + getClass().getName()
              + " is configured after " + AccessController.class.getName());
        }
      }
    }
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c) {
    if (checkInitialized()) {
      hdfsAclHelper.close();
    }
  }

  @Override
  public void postCompletedCreateTableAction(ObserverContext<MasterCoprocessorEnvironment> c,
      TableDescriptor desc, RegionInfo[] regions) throws IOException {
    if (!desc.getTableName().isSystemTable() && checkInitialized()) {
      TableName tableName = desc.getTableName();
      List<Path> paths = hdfsAclHelper.getTableRootPaths(tableName, false);
      for (Path path : paths) {
        if (!fs.exists(path)) {
          fs.mkdirs(path);
        }
      }
      // Add table owner HDFS acls
      String owner =
          desc.getOwnerString() == null ? getActiveUser(c).getShortName() : desc.getOwnerString();
      hdfsAclHelper.addTableAcl(tableName, owner);
      try (Table aclTable =
          c.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        SnapshotScannerHDFSAclStorage.addUserTableHdfsAcl(aclTable, owner, tableName);
      }
    }
  }

  @Override
  public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> c,
      NamespaceDescriptor ns) throws IOException {
    if (checkInitialized()) {
      List<Path> paths = hdfsAclHelper.getNamespaceRootPaths(ns.getName());
      for (Path path : paths) {
        if (!fs.exists(path)) {
          fs.mkdirs(path);
        }
      }
    }
  }

  @Override
  public void postCompletedSnapshotAction(ObserverContext<MasterCoprocessorEnvironment> c,
      SnapshotDescription snapshot, TableDescriptor tableDescriptor) throws IOException {
    if (!tableDescriptor.getTableName().isSystemTable() && checkInitialized()) {
      hdfsAclHelper.snapshotAcl(snapshot);
    }
  }

  @Override
  public void postCompletedTruncateTableAction(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {
    if (!tableName.isSystemTable() && checkInitialized()) {
      hdfsAclHelper.resetTableAcl(tableName);
    }
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    if (!tableName.isSystemTable() && checkInitialized()) {
      /*
       * remove table user access HDFS acl from namespace directory if the user has no permissions
       * of global, ns of the table or other tables of the ns, eg: Bob has 'ns1:t1' read permission,
       * when delete 'ns1:t1', if Bob has global read permission, '@ns1' read permission or
       * 'ns1:other_tables' read permission, then skip remove Bob access acl in ns1Dirs, otherwise,
       * remove Bob access acl.
       */
      Set<String> removeUsers = new HashSet<>();
      try (Table aclTable =
          ctx.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        List<String> users = SnapshotScannerHDFSAclStorage.getTableUsers(aclTable, tableName);
        SnapshotScannerHDFSAclStorage.deleteTableHdfsAcl(aclTable, tableName);
        byte[] namespace = tableName.getNamespace();
        for (String user : users) {
          List<byte[]> userEntries = SnapshotScannerHDFSAclStorage.getUserEntries(aclTable, user);
          boolean remove = true;
          for (byte[] entry : userEntries) {
            if (PermissionStorage.isGlobalEntry(entry)) {
              remove = false;
              break;
            } else if (PermissionStorage.isNamespaceEntry(entry)
                && Bytes.equals(PermissionStorage.fromNamespaceEntry(entry), namespace)) {
              remove = false;
              break;
            } else if (Bytes.equals(TableName.valueOf(entry).getNamespace(), namespace)) {
              remove = false;
              break;
            }
          }
          if (remove) {
            removeUsers.add(user);
          }
        }
      }
      if (removeUsers.size() > 0) {
        hdfsAclHelper.removeNamespaceAcl(tableName, removeUsers);
      }
    }
  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {
    if (checkInitialized()) {
      try (Table aclTable =
          ctx.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        SnapshotScannerHDFSAclStorage.deleteNamespaceHdfsAcl(aclTable, namespace);
      }
      /**
       * Delete namespace tmp directory because it's created by this coprocessor when namespace is
       * created to make namespace default acl can be inherited by tables. The namespace data
       * directory is deleted by DeleteNamespaceProcedure, the namespace archive directory is
       * deleted by HFileCleaner.
       */
      Path tmpNsDir = pathHelper.getTmpNsDir(namespace);
      if (fs.exists(tmpNsDir)) {
        if (fs.listStatus(tmpNsDir).length == 0) {
          fs.delete(tmpNsDir, false);
        } else {
          LOG.error("The tmp directory {} of namespace {} is not empty after delete namespace",
            tmpNsDir, namespace);
        }
      }
    }
  }

  @Override
  public void postGrant(ObserverContext<MasterCoprocessorEnvironment> c,
      UserPermission userPermission, boolean mergeExistingPermissions) throws IOException {
    if (!checkInitialized()) {
      return;
    }
    try (Table aclTable =
        c.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
      Configuration conf = c.getEnvironment().getConfiguration();
      String userName = userPermission.getUser();
      switch (userPermission.getAccessScope()) {
        case GLOBAL:
          UserPermission perm = getUserGlobalPermission(conf, userName);
          if (perm != null && containReadPermission(perm)) {
            if (!isHdfsAclSet(aclTable, userName)) {
              Pair<Set<String>, Set<TableName>> namespaceAndTable =
                      SnapshotScannerHDFSAclStorage.getUserNamespaceAndTable(aclTable, userName);
              Set<String> skipNamespaces = namespaceAndTable.getFirst();
              Set<TableName> skipTables = namespaceAndTable.getSecond().stream()
                      .filter(t -> !skipNamespaces.contains(t.getNamespaceAsString()))
                      .collect(Collectors.toSet());
              hdfsAclHelper.grantAcl(userPermission, skipNamespaces, skipTables);
              SnapshotScannerHDFSAclStorage.addUserGlobalHdfsAcl(aclTable, userName);
            }
          } else {
            // The merged user permission doesn't contain READ, so remove user global HDFS acls if
            // it's set
            removeUserGlobalHdfsAcl(aclTable, userName, userPermission);
          }
          break;
        case NAMESPACE:
          String namespace = ((NamespacePermission) userPermission.getPermission()).getNamespace();
          UserPermission nsPerm = getUserNamespacePermission(conf, userName, namespace);
          if (nsPerm != null && containReadPermission(nsPerm)) {
            if (!isHdfsAclSet(aclTable, userName, namespace)) {
              Set<TableName> skipTables = SnapshotScannerHDFSAclStorage
                      .getUserNamespaceAndTable(aclTable, userName).getSecond();
              hdfsAclHelper.grantAcl(userPermission, new HashSet<>(0), skipTables);
            }
            SnapshotScannerHDFSAclStorage.addUserNamespaceHdfsAcl(aclTable, userName, namespace);
          } else {
            // The merged user permission doesn't contain READ, so remove user namespace HDFS acls
            // if it's set
            removeUserNamespaceHdfsAcl(aclTable, userName, namespace, userPermission);
          }
          break;
        case TABLE:
          TableName tableName = ((TablePermission) userPermission.getPermission()).getTableName();
          UserPermission tPerm = getUserTablePermission(conf, userName, tableName);
          if (tPerm != null) {
            TablePermission tablePermission = (TablePermission) tPerm.getPermission();
            if (tablePermission.hasFamily() || tablePermission.hasQualifier()) {
              break;
            }
          }
          if (tPerm != null && containReadPermission(tPerm)) {
            if (!isHdfsAclSet(aclTable, userName, tableName)) {
              hdfsAclHelper.grantAcl(userPermission, new HashSet<>(0), new HashSet<>(0));
            }
            SnapshotScannerHDFSAclStorage.addUserTableHdfsAcl(aclTable, userName, tableName);
          } else {
            // The merged user permission doesn't contain READ, so remove user table HDFS acls if
            // it's set
            removeUserTableHdfsAcl(aclTable, userName, tableName, userPermission);
          }
          break;
        default:
          throw new IllegalArgumentException(
              "Illegal user permission scope " + userPermission.getAccessScope());
      }
    }
  }

  @Override
  public void postRevoke(ObserverContext<MasterCoprocessorEnvironment> c,
      UserPermission userPermission) throws IOException {
    if (checkInitialized()) {
      try (Table aclTable =
          c.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        String userName = userPermission.getUser();
        Configuration conf = c.getEnvironment().getConfiguration();
        switch (userPermission.getAccessScope()) {
          case GLOBAL:
            UserPermission userGlobalPerm = getUserGlobalPermission(conf, userName);
            if (userGlobalPerm == null || !containReadPermission(userGlobalPerm)) {
              removeUserGlobalHdfsAcl(aclTable, userName, userPermission);
            }
            break;
          case NAMESPACE:
            NamespacePermission nsPerm = (NamespacePermission) userPermission.getPermission();
            UserPermission userNsPerm =
                getUserNamespacePermission(conf, userName, nsPerm.getNamespace());
            if (userNsPerm == null || !containReadPermission(userNsPerm)) {
              removeUserNamespaceHdfsAcl(aclTable, userName, nsPerm.getNamespace(), userPermission);
            }
            break;
          case TABLE:
            TablePermission tPerm = (TablePermission) userPermission.getPermission();
            UserPermission userTablePerm =
                getUserTablePermission(conf, userName, tPerm.getTableName());
            if (userTablePerm == null || !containReadPermission(userTablePerm)) {
              removeUserTableHdfsAcl(aclTable, userName, tPerm.getTableName(), userPermission);
            }
            break;
          default:
            throw new IllegalArgumentException(
                "Illegal user permission scope " + userPermission.getAccessScope());
        }
      }
    }
  }

  private void removeUserGlobalHdfsAcl(Table aclTable, String userName,
      UserPermission userPermission) throws IOException {
    if (SnapshotScannerHDFSAclStorage.hasUserGlobalHdfsAcl(aclTable, userName)) {
      // remove user global acls but reserve ns and table acls
      Pair<Set<String>, Set<TableName>> namespaceAndTable =
          SnapshotScannerHDFSAclStorage.getUserNamespaceAndTable(aclTable, userName);
      Set<String> skipNamespaces = namespaceAndTable.getFirst();
      Set<TableName> skipTables = namespaceAndTable.getSecond().stream()
          .filter(t -> !skipNamespaces.contains(t.getNamespaceAsString()))
          .collect(Collectors.toSet());
      hdfsAclHelper.revokeAcl(userPermission, skipNamespaces, skipTables);
      SnapshotScannerHDFSAclStorage.deleteUserGlobalHdfsAcl(aclTable, userName);
    }
  }

  private void removeUserNamespaceHdfsAcl(Table aclTable, String userName, String namespace,
      UserPermission userPermission) throws IOException {
    // remove user ns acls but reserve table acls
    if (SnapshotScannerHDFSAclStorage.hasUserNamespaceHdfsAcl(aclTable, userName, namespace)) {
      if (!SnapshotScannerHDFSAclStorage.hasUserGlobalHdfsAcl(aclTable, userName)) {
        Set<TableName> skipTables =
            SnapshotScannerHDFSAclStorage.getUserNamespaceAndTable(aclTable, userName).getSecond();
        hdfsAclHelper.revokeAcl(userPermission, new HashSet<>(), skipTables);
      }
      SnapshotScannerHDFSAclStorage.deleteUserNamespaceHdfsAcl(aclTable, userName, namespace);
    }
  }

  private void removeUserTableHdfsAcl(Table aclTable, String userName, TableName tableName,
      UserPermission userPermission) throws IOException {
    if (SnapshotScannerHDFSAclStorage.hasUserTableHdfsAcl(aclTable, userName, tableName)) {
      if (!SnapshotScannerHDFSAclStorage.hasUserGlobalHdfsAcl(aclTable, userName)
          && !SnapshotScannerHDFSAclStorage.hasUserNamespaceHdfsAcl(aclTable, userName,
            tableName.getNamespaceAsString())) {
        // remove table acls
        hdfsAclHelper.revokeAcl(userPermission, new HashSet<>(0), new HashSet<>(0));
      }
      SnapshotScannerHDFSAclStorage.deleteUserTableHdfsAcl(aclTable, userName, tableName);
    }
  }

  private boolean containReadPermission(UserPermission userPermission) {
    if (userPermission != null) {
      return Arrays.stream(userPermission.getPermission().getActions())
          .anyMatch(action -> action == Action.READ);
    }
    return false;
  }

  private UserPermission getUserGlobalPermission(Configuration conf, String userName)
      throws IOException {
    List<UserPermission> permissions = PermissionStorage.getUserPermissions(conf,
      PermissionStorage.ACL_GLOBAL_NAME, null, null, userName, true);
    if (permissions != null && permissions.size() > 0) {
      return permissions.get(0);
    }
    return null;
  }

  private UserPermission getUserNamespacePermission(Configuration conf, String userName,
      String namespace) throws IOException {
    List<UserPermission> permissions =
        PermissionStorage.getUserNamespacePermissions(conf, namespace, userName, true);
    if (permissions != null && permissions.size() > 0) {
      return permissions.get(0);
    }
    return null;
  }

  private UserPermission getUserTablePermission(Configuration conf, String userName,
      TableName tableName) throws IOException {
    List<UserPermission> permissions =
        PermissionStorage.getUserTablePermissions(conf, tableName, null, null, userName, true);
    if (permissions != null && permissions.size() > 0) {
      return permissions.get(0);
    }
    return null;
  }

  private boolean isHdfsAclSet(Table aclTable, String userName) throws IOException {
    return isHdfsAclSet(aclTable, userName, null, null);
  }

  private boolean isHdfsAclSet(Table aclTable, String userName, String namespace)
      throws IOException {
    return isHdfsAclSet(aclTable, userName, namespace, null);
  }

  private boolean isHdfsAclSet(Table aclTable, String userName, TableName tableName)
      throws IOException {
    return isHdfsAclSet(aclTable, userName, null, tableName);
  }

  /**
   * Check if user global/namespace/table HDFS acls is already set to hfile
   */
  private boolean isHdfsAclSet(Table aclTable, String userName, String namespace,
      TableName tableName) throws IOException {
    boolean isSet = SnapshotScannerHDFSAclStorage.hasUserGlobalHdfsAcl(aclTable, userName);
    if (namespace != null) {
      isSet = isSet
          || SnapshotScannerHDFSAclStorage.hasUserNamespaceHdfsAcl(aclTable, userName, namespace);
    }
    if (tableName != null) {
      isSet = isSet
          || SnapshotScannerHDFSAclStorage.hasUserNamespaceHdfsAcl(aclTable, userName,
            tableName.getNamespaceAsString())
          || SnapshotScannerHDFSAclStorage.hasUserTableHdfsAcl(aclTable, userName, tableName);
    }
    return isSet;
  }

  private boolean checkInitialized() {
    if (initialized) {
      return true;
    } else {
      return false;
    }
  }

  private User getActiveUser(ObserverContext<?> ctx) throws IOException {
    // for non-rpc handling, fallback to system user
    Optional<User> optionalUser = ctx.getCaller();
    if (optionalUser.isPresent()) {
      return optionalUser.get();
    }
    return userProvider.getCurrent();
  }

  static final class SnapshotScannerHDFSAclStorage {
    /**
     * Add a new CF in HBase acl table to record if the HBase read permission is synchronized to
     * related hfiles. The record has two usages: 1. check if we need to remove HDFS acls for a
     * grant without READ permission(eg: grant user table read permission and then grant user table
     * write permission without merging the existing permissions, in this case, need to remove HDFS
     * acls); 2. skip some HDFS acl sync because it may be already set(eg: grant user table read
     * permission and then grant user ns read permission; grant user table read permission and then
     * grant user table write permission with merging the existing permissions).
     */
    static final byte[] HDFS_ACL_FAMILY = Bytes.toBytes("m");
    // The value 'R' has no specific meaning, if cell value is not null, it means that the user HDFS
    // acls is set to hfiles.
    private static final byte[] HDFS_ACL_VALUE = Bytes.toBytes("R");

    static void addUserGlobalHdfsAcl(Table aclTable, String user) throws IOException {
      addUserEntry(aclTable, user, PermissionStorage.ACL_GLOBAL_NAME);
    }

    static void addUserNamespaceHdfsAcl(Table aclTable, String user, String namespace)
        throws IOException {
      addUserEntry(aclTable, user, Bytes.toBytes(PermissionStorage.toNamespaceEntry(namespace)));
    }

    static void addUserTableHdfsAcl(Table aclTable, String user, TableName tableName)
        throws IOException {
      addUserEntry(aclTable, user, tableName.getName());
    }

    private static void addUserEntry(Table t, String user, byte[] entry) throws IOException {
      Put p = new Put(entry);
      p.addColumn(HDFS_ACL_FAMILY, Bytes.toBytes(user), HDFS_ACL_VALUE);
      t.put(p);
    }

    static void deleteUserGlobalHdfsAcl(Table aclTable, String user) throws IOException {
      deleteUserEntry(aclTable, user, PermissionStorage.ACL_GLOBAL_NAME);
    }

    static void deleteUserNamespaceHdfsAcl(Table aclTable, String user, String namespace)
        throws IOException {
      deleteUserEntry(aclTable, user, Bytes.toBytes(PermissionStorage.toNamespaceEntry(namespace)));
    }

    static void deleteUserTableHdfsAcl(Table aclTable, String user, TableName tableName)
        throws IOException {
      deleteUserEntry(aclTable, user, tableName.getName());
    }

    private static void deleteUserEntry(Table aclTable, String user, byte[] entry)
        throws IOException {
      Delete delete = new Delete(entry);
      delete.addColumns(HDFS_ACL_FAMILY, Bytes.toBytes(user));
      aclTable.delete(delete);
    }

    static void deleteNamespaceHdfsAcl(Table aclTable, String namespace) throws IOException {
      deleteEntry(aclTable, Bytes.toBytes(PermissionStorage.toNamespaceEntry(namespace)));
    }

    static void deleteTableHdfsAcl(Table aclTable, TableName tableName) throws IOException {
      deleteEntry(aclTable, tableName.getName());
    }

    private static void deleteEntry(Table aclTable, byte[] entry) throws IOException {
      Delete delete = new Delete(entry);
      delete.addFamily(HDFS_ACL_FAMILY);
      aclTable.delete(delete);
    }

    static List<String> getTableUsers(Table aclTable, TableName tableName) throws IOException {
      return getEntryUsers(aclTable, tableName.getName());
    }

    private static List<String> getEntryUsers(Table aclTable, byte[] entry) throws IOException {
      List<String> users = new ArrayList<>();
      Get get = new Get(entry);
      get.addFamily(HDFS_ACL_FAMILY);
      Result result = aclTable.get(get);
      List<Cell> cells = result.listCells();
      if (cells != null) {
        for (Cell cell : cells) {
          if (cell != null) {
            users.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
          }
        }
      }
      return users;
    }

    static Pair<Set<String>, Set<TableName>> getUserNamespaceAndTable(Table aclTable,
        String userName) throws IOException {
      Set<String> namespaces = new HashSet<>();
      Set<TableName> tables = new HashSet<>();
      List<byte[]> userEntries = SnapshotScannerHDFSAclStorage.getUserEntries(aclTable, userName);
      for (byte[] entry : userEntries) {
        if (PermissionStorage.isNamespaceEntry(entry)) {
          namespaces.add(Bytes.toString(PermissionStorage.fromNamespaceEntry(entry)));
        } else if (PermissionStorage.isTableEntry(entry)) {
          tables.add(TableName.valueOf(entry));
        }
      }
      return new Pair<>(namespaces, tables);
    }

    static List<byte[]> getUserEntries(Table aclTable, String userName) throws IOException {
      Scan scan = new Scan();
      scan.addColumn(HDFS_ACL_FAMILY, Bytes.toBytes(userName));
      ResultScanner scanner = aclTable.getScanner(scan);
      List<byte[]> entry = new ArrayList<>();
      for (Result result : scanner) {
        if (result != null && result.getRow() != null) {
          entry.add(result.getRow());
        }
      }
      return entry;
    }

    static boolean hasUserGlobalHdfsAcl(Table aclTable, String user) throws IOException {
      return hasUserEntry(aclTable, user, PermissionStorage.ACL_GLOBAL_NAME);
    }

    static boolean hasUserNamespaceHdfsAcl(Table aclTable, String user, String namespace)
        throws IOException {
      return hasUserEntry(aclTable, user,
        Bytes.toBytes(PermissionStorage.toNamespaceEntry(namespace)));
    }

    static boolean hasUserTableHdfsAcl(Table aclTable, String user, TableName tableName)
        throws IOException {
      return hasUserEntry(aclTable, user, tableName.getName());
    }

    private static boolean hasUserEntry(Table aclTable, String userName, byte[] entry)
        throws IOException {
      Get get = new Get(entry);
      get.addColumn(HDFS_ACL_FAMILY, Bytes.toBytes(userName));
      return aclTable.exists(get);
    }
  }
}
