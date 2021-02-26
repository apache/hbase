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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
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
import org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclHelper.PathHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * Set HDFS ACLs to hFiles to make HBase granted users have permission to scan snapshot
 * <p>
 * To use this feature, please mask sure HDFS config:
 * <ul>
 * <li>dfs.namenode.acls.enabled = true</li>
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
  private MasterServices masterServices = null;
  private volatile boolean initialized = false;
  private volatile boolean aclTableInitialized = false;
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
        .getBoolean(SnapshotScannerHDFSAclHelper.ACL_SYNC_TO_HDFS_ENABLE, false)) {
      MasterCoprocessorEnvironment mEnv = c.getEnvironment();
      if (!(mEnv instanceof HasMasterServices)) {
        throw new IOException("Does not implement HMasterServices");
      }
      masterServices = ((HasMasterServices) mEnv).getMasterServices();
      hdfsAclHelper = new SnapshotScannerHDFSAclHelper(masterServices.getConfiguration(),
          masterServices.getConnection());
      pathHelper = hdfsAclHelper.getPathHelper();
      hdfsAclHelper.setCommonDirectoryPermission();
      initialized = true;
      userProvider = UserProvider.instantiate(c.getEnvironment().getConfiguration());
    } else {
      LOG.warn("Try to initialize the coprocessor SnapshotScannerHDFSAclController but failure "
          + "because the config " + SnapshotScannerHDFSAclHelper.ACL_SYNC_TO_HDFS_ENABLE
          + " is false.");
    }
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
    if (!initialized) {
      return;
    }
    try (Admin admin = c.getEnvironment().getConnection().getAdmin()) {
      if (admin.tableExists(PermissionStorage.ACL_TABLE_NAME)) {
        // Check if acl table has 'm' CF, if not, add 'm' CF
        TableDescriptor tableDescriptor = admin.getDescriptor(PermissionStorage.ACL_TABLE_NAME);
        boolean containHdfsAclFamily = Arrays.stream(tableDescriptor.getColumnFamilies()).anyMatch(
          family -> Bytes.equals(family.getName(), SnapshotScannerHDFSAclStorage.HDFS_ACL_FAMILY));
        if (!containHdfsAclFamily) {
          TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDescriptor)
              .setColumnFamily(ColumnFamilyDescriptorBuilder
                  .newBuilder(SnapshotScannerHDFSAclStorage.HDFS_ACL_FAMILY).build());
          admin.modifyTable(builder.build());
        }
        aclTableInitialized = true;
      } else {
        throw new TableNotFoundException("Table " + PermissionStorage.ACL_TABLE_NAME
            + " is not created yet. Please check if " + getClass().getName()
            + " is configured after " + AccessController.class.getName());
      }
    }
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c) {
    if (initialized) {
      hdfsAclHelper.close();
    }
  }

  @Override
  public void postCompletedCreateTableAction(ObserverContext<MasterCoprocessorEnvironment> c,
      TableDescriptor desc, RegionInfo[] regions) throws IOException {
    if (needHandleTableHdfsAcl(desc, "createTable " + desc.getTableName())) {
      TableName tableName = desc.getTableName();
      // 1. Create table directories to make HDFS acls can be inherited
      hdfsAclHelper.createTableDirectories(tableName);
      // 2. Add table owner HDFS acls
      String owner =
          desc.getOwnerString() == null ? getActiveUser(c).getShortName() : desc.getOwnerString();
      hdfsAclHelper.addTableAcl(tableName, Sets.newHashSet(owner), "create");
      // 3. Record table owner permission is synced to HDFS in acl table
      SnapshotScannerHDFSAclStorage.addUserTableHdfsAcl(c.getEnvironment().getConnection(), owner,
        tableName);
    }
  }

  @Override
  public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> c,
      NamespaceDescriptor ns) throws IOException {
    if (checkInitialized("createNamespace " + ns.getName())) {
      // Create namespace directories to make HDFS acls can be inherited
      List<Path> paths = hdfsAclHelper.getNamespaceRootPaths(ns.getName());
      for (Path path : paths) {
        hdfsAclHelper.createDirIfNotExist(path);
      }
    }
  }

  @Override
  public void postCompletedSnapshotAction(ObserverContext<MasterCoprocessorEnvironment> c,
      SnapshotDescription snapshot, TableDescriptor tableDescriptor) throws IOException {
    if (needHandleTableHdfsAcl(tableDescriptor, "snapshot " + snapshot.getName())) {
      // Add HDFS acls of users with table read permission to snapshot files
      hdfsAclHelper.snapshotAcl(snapshot);
    }
  }

  @Override
  public void postCompletedTruncateTableAction(ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tableName) throws IOException {
    if (needHandleTableHdfsAcl(tableName, "truncateTable " + tableName)) {
      // 1. create tmp table directories
      hdfsAclHelper.createTableDirectories(tableName);
      // 2. Since the table directories is recreated, so add HDFS acls again
      Set<String> users = hdfsAclHelper.getUsersWithTableReadAction(tableName, false, false);
      hdfsAclHelper.addTableAcl(tableName, users, "truncate");
    }
  }

  @Override
  public void postCompletedDeleteTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    if (!tableName.isSystemTable() && checkInitialized("deleteTable " + tableName)) {
      /*
       * Remove table user access HDFS acl from namespace directory if the user has no permissions
       * of global, ns of the table or other tables of the ns, eg: Bob has 'ns1:t1' read permission,
       * when delete 'ns1:t1', if Bob has global read permission, '@ns1' read permission or
       * 'ns1:other_tables' read permission, then skip remove Bob access acl in ns1Dirs, otherwise,
       * remove Bob access acl.
       */
      try (Table aclTable =
          ctx.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        Set<String> users = SnapshotScannerHDFSAclStorage.getTableUsers(aclTable, tableName);
        if (users.size() > 0) {
          // 1. Remove table archive directory default ACLs
          hdfsAclHelper.removeTableDefaultAcl(tableName, users);
          // 2. Delete table owner permission is synced to HDFS in acl table
          SnapshotScannerHDFSAclStorage.deleteTableHdfsAcl(aclTable, tableName);
          // 3. Remove namespace access acls
          Set<String> removeUsers = filterUsersToRemoveNsAccessAcl(aclTable, tableName, users);
          if (removeUsers.size() > 0) {
            hdfsAclHelper.removeNamespaceAccessAcl(tableName, removeUsers, "delete");
          }
        }
      }
    }
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, TableDescriptor oldDescriptor, TableDescriptor currentDescriptor)
      throws IOException {
    try (Table aclTable =
        ctx.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
      if (needHandleTableHdfsAcl(currentDescriptor, "modifyTable " + tableName)
          && !hdfsAclHelper.isAclSyncToHdfsEnabled(oldDescriptor)) {
        // 1. Create table directories used for acl inherited
        hdfsAclHelper.createTableDirectories(tableName);
        // 2. Add table users HDFS acls
        Set<String> tableUsers = hdfsAclHelper.getUsersWithTableReadAction(tableName, false, false);
        Set<String> users =
            hdfsAclHelper.getUsersWithNamespaceReadAction(tableName.getNamespaceAsString(), true);
        users.addAll(tableUsers);
        hdfsAclHelper.addTableAcl(tableName, users, "modify");
        // 3. Record table user acls are synced to HDFS in acl table
        SnapshotScannerHDFSAclStorage.addUserTableHdfsAcl(ctx.getEnvironment().getConnection(),
          tableUsers, tableName);
      } else if (needHandleTableHdfsAcl(oldDescriptor, "modifyTable " + tableName)
          && !hdfsAclHelper.isAclSyncToHdfsEnabled(currentDescriptor)) {
        // 1. Remove empty table directories
        List<Path> tableRootPaths = hdfsAclHelper.getTableRootPaths(tableName, false);
        for (Path path : tableRootPaths) {
          hdfsAclHelper.deleteEmptyDir(path);
        }
        // 2. Remove all table HDFS acls
        Set<String> tableUsers = hdfsAclHelper.getUsersWithTableReadAction(tableName, false, false);
        Set<String> users = hdfsAclHelper
            .getUsersWithNamespaceReadAction(tableName.getNamespaceAsString(), true);
        users.addAll(tableUsers);
        hdfsAclHelper.removeTableAcl(tableName, users);
        // 3. Remove namespace access HDFS acls for users who only own permission for this table
        hdfsAclHelper.removeNamespaceAccessAcl(tableName,
          filterUsersToRemoveNsAccessAcl(aclTable, tableName, tableUsers), "modify");
        // 4. Record table user acl is not synced to HDFS
        SnapshotScannerHDFSAclStorage.deleteUserTableHdfsAcl(ctx.getEnvironment().getConnection(),
          tableUsers, tableName);
      }
    }
  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {
    if (checkInitialized("deleteNamespace " + namespace)) {
      try (Table aclTable =
          ctx.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        // 1. Delete namespace archive dir default ACLs
        Set<String> users = SnapshotScannerHDFSAclStorage.getEntryUsers(aclTable,
          PermissionStorage.toNamespaceEntry(Bytes.toBytes(namespace)));
        hdfsAclHelper.removeNamespaceDefaultAcl(namespace, users);
        // 2. Record namespace user acl is not synced to HDFS
        SnapshotScannerHDFSAclStorage.deleteNamespaceHdfsAcl(ctx.getEnvironment().getConnection(),
          namespace);
        // 3. Delete tmp namespace directory
        /**
         * Delete namespace tmp directory because it's created by this coprocessor when namespace is
         * created to make namespace default acl can be inherited by tables. The namespace data
         * directory is deleted by DeleteNamespaceProcedure, the namespace archive directory is
         * deleted by HFileCleaner.
         */
        hdfsAclHelper.deleteEmptyDir(pathHelper.getTmpNsDir(namespace));
      }
    }
  }

  @Override
  public void postGrant(ObserverContext<MasterCoprocessorEnvironment> c,
      UserPermission userPermission, boolean mergeExistingPermissions) throws IOException {
    if (!checkInitialized(
      "grant " + userPermission + ", merge existing permissions " + mergeExistingPermissions)) {
      return;
    }
    try (Table aclTable =
        c.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
      Configuration conf = c.getEnvironment().getConfiguration();
      String userName = userPermission.getUser();
      switch (userPermission.getAccessScope()) {
        case GLOBAL:
          UserPermission perm = getUserGlobalPermission(conf, userName);
          if (perm != null && hdfsAclHelper.containReadAction(perm)) {
            if (!isHdfsAclSet(aclTable, userName)) {
              // 1. Get namespaces and tables which global user acls are already synced
              Pair<Set<String>, Set<TableName>> skipNamespaceAndTables =
                  SnapshotScannerHDFSAclStorage.getUserNamespaceAndTable(aclTable, userName);
              Set<String> skipNamespaces = skipNamespaceAndTables.getFirst();
              Set<TableName> skipTables = skipNamespaceAndTables.getSecond().stream()
                  .filter(t -> !skipNamespaces.contains(t.getNamespaceAsString()))
                  .collect(Collectors.toSet());
              // 2. Add HDFS acl(skip namespaces and tables directories whose acl is set)
              hdfsAclHelper.grantAcl(userPermission, skipNamespaces, skipTables);
              // 3. Record global acl is sync to HDFS
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
          if (nsPerm != null && hdfsAclHelper.containReadAction(nsPerm)) {
            if (!isHdfsAclSet(aclTable, userName, namespace)) {
              // 1. Get tables which namespace user acls are already synced
              Set<TableName> skipTables = SnapshotScannerHDFSAclStorage
                  .getUserNamespaceAndTable(aclTable, userName).getSecond();
              // 2. Add HDFS acl(skip tables directories whose acl is set)
              hdfsAclHelper.grantAcl(userPermission, new HashSet<>(0), skipTables);
            }
            // 3. Record namespace acl is synced to HDFS
            SnapshotScannerHDFSAclStorage.addUserNamespaceHdfsAcl(aclTable, userName, namespace);
          } else {
            // The merged user permission doesn't contain READ, so remove user namespace HDFS acls
            // if it's set
            removeUserNamespaceHdfsAcl(aclTable, userName, namespace, userPermission);
          }
          break;
        case TABLE:
          TablePermission tablePerm = (TablePermission) userPermission.getPermission();
          if (needHandleTableHdfsAcl(tablePerm)) {
            TableName tableName = tablePerm.getTableName();
            UserPermission tPerm = getUserTablePermission(conf, userName, tableName);
            if (tPerm != null && hdfsAclHelper.containReadAction(tPerm)) {
              if (!isHdfsAclSet(aclTable, userName, tableName)) {
                // 1. create table dirs
                hdfsAclHelper.createTableDirectories(tableName);
                // 2. Add HDFS acl
                hdfsAclHelper.grantAcl(userPermission, new HashSet<>(0), new HashSet<>(0));
              }
              // 2. Record table acl is synced to HDFS
              SnapshotScannerHDFSAclStorage.addUserTableHdfsAcl(aclTable, userName, tableName);
            } else {
              // The merged user permission doesn't contain READ, so remove user table HDFS acls if
              // it's set
              removeUserTableHdfsAcl(aclTable, userName, tableName, userPermission);
            }
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
    if (checkInitialized("revoke " + userPermission)) {
      try (Table aclTable =
          c.getEnvironment().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        String userName = userPermission.getUser();
        Configuration conf = c.getEnvironment().getConfiguration();
        switch (userPermission.getAccessScope()) {
          case GLOBAL:
            UserPermission userGlobalPerm = getUserGlobalPermission(conf, userName);
            if (userGlobalPerm == null || !hdfsAclHelper.containReadAction(userGlobalPerm)) {
              removeUserGlobalHdfsAcl(aclTable, userName, userPermission);
            }
            break;
          case NAMESPACE:
            NamespacePermission nsPerm = (NamespacePermission) userPermission.getPermission();
            UserPermission userNsPerm =
                getUserNamespacePermission(conf, userName, nsPerm.getNamespace());
            if (userNsPerm == null || !hdfsAclHelper.containReadAction(userNsPerm)) {
              removeUserNamespaceHdfsAcl(aclTable, userName, nsPerm.getNamespace(), userPermission);
            }
            break;
          case TABLE:
            TablePermission tPerm = (TablePermission) userPermission.getPermission();
            if (needHandleTableHdfsAcl(tPerm)) {
              TableName tableName = tPerm.getTableName();
              UserPermission userTablePerm = getUserTablePermission(conf, userName, tableName);
              if (userTablePerm == null || !hdfsAclHelper.containReadAction(userTablePerm)) {
                removeUserTableHdfsAcl(aclTable, userName, tableName, userPermission);
              }
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
      // 1. Get namespaces and tables which global user acls are already synced
      Pair<Set<String>, Set<TableName>> namespaceAndTable =
          SnapshotScannerHDFSAclStorage.getUserNamespaceAndTable(aclTable, userName);
      Set<String> skipNamespaces = namespaceAndTable.getFirst();
      Set<TableName> skipTables = namespaceAndTable.getSecond().stream()
          .filter(t -> !skipNamespaces.contains(t.getNamespaceAsString()))
          .collect(Collectors.toSet());
      // 2. Remove user HDFS acls(skip namespaces and tables directories
      // whose acl must be reversed)
      hdfsAclHelper.revokeAcl(userPermission, skipNamespaces, skipTables);
      // 3. Remove global user acl is synced to HDFS in acl table
      SnapshotScannerHDFSAclStorage.deleteUserGlobalHdfsAcl(aclTable, userName);
    }
  }

  private void removeUserNamespaceHdfsAcl(Table aclTable, String userName, String namespace,
      UserPermission userPermission) throws IOException {
    if (SnapshotScannerHDFSAclStorage.hasUserNamespaceHdfsAcl(aclTable, userName, namespace)) {
      if (!SnapshotScannerHDFSAclStorage.hasUserGlobalHdfsAcl(aclTable, userName)) {
        // 1. Get tables whose namespace user acls are already synced
        Set<TableName> skipTables =
            SnapshotScannerHDFSAclStorage.getUserNamespaceAndTable(aclTable, userName).getSecond();
        // 2. Remove user HDFS acls(skip tables directories whose acl must be reversed)
        hdfsAclHelper.revokeAcl(userPermission, new HashSet<>(), skipTables);
      }
      // 3. Remove namespace user acl is synced to HDFS in acl table
      SnapshotScannerHDFSAclStorage.deleteUserNamespaceHdfsAcl(aclTable, userName, namespace);
    }
  }

  private void removeUserTableHdfsAcl(Table aclTable, String userName, TableName tableName,
      UserPermission userPermission) throws IOException {
    if (SnapshotScannerHDFSAclStorage.hasUserTableHdfsAcl(aclTable, userName, tableName)) {
      if (!SnapshotScannerHDFSAclStorage.hasUserGlobalHdfsAcl(aclTable, userName)
          && !SnapshotScannerHDFSAclStorage.hasUserNamespaceHdfsAcl(aclTable, userName,
            tableName.getNamespaceAsString())) {
        // 1. Remove table acls
        hdfsAclHelper.revokeAcl(userPermission, new HashSet<>(0), new HashSet<>(0));
      }
      // 2. Remove table user acl is synced to HDFS in acl table
      SnapshotScannerHDFSAclStorage.deleteUserTableHdfsAcl(aclTable, userName, tableName);
    }
  }

  private UserPermission getUserGlobalPermission(Configuration conf, String userName)
      throws IOException {
    List<UserPermission> permissions = PermissionStorage.getUserPermissions(conf,
      PermissionStorage.ACL_GLOBAL_NAME, null, null, userName, true);
    return permissions.size() > 0 ? permissions.get(0) : null;
  }

  private UserPermission getUserNamespacePermission(Configuration conf, String userName,
      String namespace) throws IOException {
    List<UserPermission> permissions =
        PermissionStorage.getUserNamespacePermissions(conf, namespace, userName, true);
    return permissions.size() > 0 ? permissions.get(0) : null;
  }

  private UserPermission getUserTablePermission(Configuration conf, String userName,
      TableName tableName) throws IOException {
    List<UserPermission> permissions = PermissionStorage
        .getUserTablePermissions(conf, tableName, null, null, userName, true).stream()
        .filter(userPermission -> hdfsAclHelper
            .isNotFamilyOrQualifierPermission((TablePermission) userPermission.getPermission()))
        .collect(Collectors.toList());
    return permissions.size() > 0 ? permissions.get(0) : null;
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
   * Check if user global/namespace/table HDFS acls is already set
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

  @InterfaceAudience.Private
  boolean checkInitialized(String operation) {
    if (initialized) {
      if (aclTableInitialized) {
        return true;
      } else {
        LOG.warn("Skip set HDFS acls because acl table is not initialized when " + operation);
      }
    }
    return false;
  }

  private boolean needHandleTableHdfsAcl(TablePermission tablePermission) throws IOException {
    return needHandleTableHdfsAcl(tablePermission.getTableName(), "")
        && hdfsAclHelper.isNotFamilyOrQualifierPermission(tablePermission);
  }

  private boolean needHandleTableHdfsAcl(TableName tableName, String operation) throws IOException {
    return !tableName.isSystemTable() && checkInitialized(operation) && hdfsAclHelper
        .isAclSyncToHdfsEnabled(masterServices.getTableDescriptors().get(tableName));
  }

  private boolean needHandleTableHdfsAcl(TableDescriptor tableDescriptor, String operation) {
    TableName tableName = tableDescriptor.getTableName();
    return !tableName.isSystemTable() && checkInitialized(operation)
        && hdfsAclHelper.isAclSyncToHdfsEnabled(tableDescriptor);
  }

  private User getActiveUser(ObserverContext<?> ctx) throws IOException {
    // for non-rpc handling, fallback to system user
    Optional<User> optionalUser = ctx.getCaller();
    if (optionalUser.isPresent()) {
      return optionalUser.get();
    }
    return userProvider.getCurrent();
  }

  /**
   * Remove table user access HDFS acl from namespace directory if the user has no permissions of
   * global, ns of the table or other tables of the ns, eg: Bob has 'ns1:t1' read permission, when
   * delete 'ns1:t1', if Bob has global read permission, '@ns1' read permission or
   * 'ns1:other_tables' read permission, then skip remove Bob access acl in ns1Dirs, otherwise,
   * remove Bob access acl.
   * @param aclTable acl table
   * @param tableName the name of the table
   * @param tablesUsers table users set
   * @return users whose access acl will be removed from the namespace of the table
   * @throws IOException if an error occurred
   */
  private Set<String> filterUsersToRemoveNsAccessAcl(Table aclTable, TableName tableName,
      Set<String> tablesUsers) throws IOException {
    Set<String> removeUsers = new HashSet<>();
    byte[] namespace = tableName.getNamespace();
    for (String user : tablesUsers) {
      List<byte[]> userEntries = SnapshotScannerHDFSAclStorage.getUserEntries(aclTable, user);
      boolean remove = true;
      for (byte[] entry : userEntries) {
        if (PermissionStorage.isGlobalEntry(entry)
            || (PermissionStorage.isNamespaceEntry(entry)
                && Bytes.equals(PermissionStorage.fromNamespaceEntry(entry), namespace))
            || (!Bytes.equals(tableName.getName(), entry)
                && Bytes.equals(TableName.valueOf(entry).getNamespace(), namespace))) {
          remove = false;
          break;
        }
      }
      if (remove) {
        removeUsers.add(user);
      }
    }
    return removeUsers;
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

    static void addUserTableHdfsAcl(Connection connection, Set<String> users, TableName tableName)
        throws IOException {
      try (Table aclTable = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
        for (String user : users) {
          addUserTableHdfsAcl(aclTable, user, tableName);
        }
      }
    }

    static void addUserTableHdfsAcl(Connection connection, String user, TableName tableName)
        throws IOException {
      try (Table aclTable = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
        addUserTableHdfsAcl(aclTable, user, tableName);
      }
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

    static void deleteUserTableHdfsAcl(Connection connection, Set<String> users,
        TableName tableName) throws IOException {
      try (Table aclTable = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
        for (String user : users) {
          deleteUserTableHdfsAcl(aclTable, user, tableName);
        }
      }
    }

    private static void deleteUserEntry(Table aclTable, String user, byte[] entry)
        throws IOException {
      Delete delete = new Delete(entry);
      delete.addColumns(HDFS_ACL_FAMILY, Bytes.toBytes(user));
      aclTable.delete(delete);
    }

    static void deleteNamespaceHdfsAcl(Connection connection, String namespace) throws IOException {
      try (Table aclTable = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
        deleteEntry(aclTable, Bytes.toBytes(PermissionStorage.toNamespaceEntry(namespace)));
      }
    }

    static void deleteTableHdfsAcl(Table aclTable, TableName tableName) throws IOException {
      deleteEntry(aclTable, tableName.getName());
    }

    private static void deleteEntry(Table aclTable, byte[] entry) throws IOException {
      Delete delete = new Delete(entry);
      delete.addFamily(HDFS_ACL_FAMILY);
      aclTable.delete(delete);
    }

    static Set<String> getTableUsers(Table aclTable, TableName tableName) throws IOException {
      return getEntryUsers(aclTable, tableName.getName());
    }

    private static Set<String> getEntryUsers(Table aclTable, byte[] entry) throws IOException {
      Set<String> users = new HashSet<>();
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
      List<byte[]> userEntries = getUserEntries(aclTable, userName);
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
