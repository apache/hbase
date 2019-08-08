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

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A helper to modify or remove HBase granted user default and access HDFS ACLs over hFiles.
 */
@InterfaceAudience.Private
public class SnapshotScannerHDFSAclHelper implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotScannerHDFSAclHelper.class);

  public static final String ACL_SYNC_TO_HDFS_ENABLE = "hbase.acl.sync.to.hdfs.enable";
  public static final String ACL_SYNC_TO_HDFS_THREAD_NUMBER =
      "hbase.acl.sync.to.hdfs.thread.number";
  // The tmp directory to restore snapshot, it can not be a sub directory of HBase root dir
  public static final String SNAPSHOT_RESTORE_TMP_DIR = "hbase.snapshot.restore.tmp.dir";
  public static final String SNAPSHOT_RESTORE_TMP_DIR_DEFAULT =
      "/hbase/.tmpdir-to-restore-snapshot";
  // The default permission of the common directories if the feature is enabled.
  public static final String COMMON_DIRECTORY_PERMISSION =
      "hbase.acl.sync.to.hdfs.common.directory.permission";
  // The secure HBase permission is 700, 751 means all others have execute access and the mask is
  // set to read-execute to make the extended access ACL entries can work. Be cautious to set
  // this value.
  public static final String COMMON_DIRECTORY_PERMISSION_DEFAULT = "751";
  // The default permission of the snapshot restore directories if the feature is enabled.
  public static final String SNAPSHOT_RESTORE_DIRECTORY_PERMISSION =
      "hbase.acl.sync.to.hdfs.restore.directory.permission";
  // 753 means all others have write-execute access.
  public static final String SNAPSHOT_RESTORE_DIRECTORY_PERMISSION_DEFAULT = "753";

  private Admin admin;
  private final Configuration conf;
  private FileSystem fs;
  private PathHelper pathHelper;
  private ExecutorService pool;

  public SnapshotScannerHDFSAclHelper(Configuration configuration, Connection connection)
      throws IOException {
    this.conf = configuration;
    this.pathHelper = new PathHelper(conf);
    this.fs = pathHelper.getFileSystem();
    this.pool = Executors.newFixedThreadPool(conf.getInt(ACL_SYNC_TO_HDFS_THREAD_NUMBER, 10),
      new ThreadFactoryBuilder().setNameFormat("hdfs-acl-thread-%d").setDaemon(true).build());
    this.admin = connection.getAdmin();
  }

  @Override
  public void close() {
    if (pool != null) {
      pool.shutdown();
    }
    try {
      admin.close();
    } catch (IOException e) {
      LOG.error("Close admin error", e);
    }
  }

  public void setCommonDirectoryPermission() throws IOException {
    // Set public directory permission to 751 to make all users have access permission.
    // And we also need the access permission of the parent of HBase root directory, but
    // it's not set here, because the owner of HBase root directory may don't own permission
    // to change it's parent permission to 751.
    // The {root/.tmp} and {root/.tmp/data} directories are created to make global user HDFS
    // ACLs can be inherited.
    List<Path> paths = Lists.newArrayList(pathHelper.getRootDir(), pathHelper.getMobDir(),
      pathHelper.getTmpDir(), pathHelper.getArchiveDir());
    paths.addAll(getGlobalRootPaths());
    for (Path path : paths) {
      createDirIfNotExist(path);
      fs.setPermission(path, new FsPermission(
          conf.get(COMMON_DIRECTORY_PERMISSION, COMMON_DIRECTORY_PERMISSION_DEFAULT)));
    }
    // create snapshot restore directory
    Path restoreDir =
        new Path(conf.get(SNAPSHOT_RESTORE_TMP_DIR, SNAPSHOT_RESTORE_TMP_DIR_DEFAULT));
    createDirIfNotExist(restoreDir);
    fs.setPermission(restoreDir, new FsPermission(conf.get(SNAPSHOT_RESTORE_DIRECTORY_PERMISSION,
      SNAPSHOT_RESTORE_DIRECTORY_PERMISSION_DEFAULT)));
  }

  /**
   * Set acl when grant user permission
   * @param userPermission the user and permission
   * @param skipNamespaces the namespace set to skip set acl because already set
   * @param skipTables the table set to skip set acl because already set
   * @return false if an error occurred, otherwise true
   */
  public boolean grantAcl(UserPermission userPermission, Set<String> skipNamespaces,
      Set<TableName> skipTables) {
    try {
      long start = System.currentTimeMillis();
      handleGrantOrRevokeAcl(userPermission, HDFSAclOperation.OperationType.MODIFY, skipNamespaces,
        skipTables);
      LOG.info("Set HDFS acl when grant {}, cost {} ms", userPermission,
        System.currentTimeMillis() - start);
      return true;
    } catch (Exception e) {
      LOG.error("Set HDFS acl error when grant: {}", userPermission, e);
      return false;
    }
  }

  /**
   * Remove acl when grant or revoke user permission
   * @param userPermission the user and permission
   * @param skipNamespaces the namespace set to skip remove acl
   * @param skipTables the table set to skip remove acl
   * @return false if an error occurred, otherwise true
   */
  public boolean revokeAcl(UserPermission userPermission, Set<String> skipNamespaces,
      Set<TableName> skipTables) {
    try {
      long start = System.currentTimeMillis();
      handleGrantOrRevokeAcl(userPermission, HDFSAclOperation.OperationType.REMOVE, skipNamespaces,
        skipTables);
      LOG.info("Set HDFS acl when revoke {}, cost {} ms", userPermission,
        System.currentTimeMillis() - start);
      return true;
    } catch (Exception e) {
      LOG.error("Set HDFS acl error when revoke: {}", userPermission, e);
      return false;
    }
  }

  /**
   * Set acl when take a snapshot
   * @param snapshot the snapshot desc
   * @return false if an error occurred, otherwise true
   */
  public boolean snapshotAcl(SnapshotDescription snapshot) {
    try {
      long start = System.currentTimeMillis();
      TableName tableName = snapshot.getTableName();
      // global user permission can be inherited from default acl automatically
      Set<String> userSet = getUsersWithTableReadAction(tableName, true, false);
      if (userSet.size() > 0) {
        Path path = pathHelper.getSnapshotDir(snapshot.getName());
        handleHDFSAcl(new HDFSAclOperation(fs, path, userSet, HDFSAclOperation.OperationType.MODIFY,
            true, HDFSAclOperation.AclType.DEFAULT_ADN_ACCESS)).get();
      }
      LOG.info("Set HDFS acl when snapshot {}, cost {} ms", snapshot.getName(),
        System.currentTimeMillis() - start);
      return true;
    } catch (Exception e) {
      LOG.error("Set HDFS acl error when snapshot {}", snapshot, e);
      return false;
    }
  }

  /**
   * Remove table access acl from namespace dir when delete table
   * @param tableName the table
   * @param removeUsers the users whose access acl will be removed
   * @return false if an error occurred, otherwise true
   */
  public boolean removeNamespaceAccessAcl(TableName tableName, Set<String> removeUsers,
      String operation) {
    try {
      long start = System.currentTimeMillis();
      if (removeUsers.size() > 0) {
        handleNamespaceAccessAcl(tableName.getNamespaceAsString(), removeUsers,
          HDFSAclOperation.OperationType.REMOVE);
      }
      LOG.info("Remove HDFS acl when {} table {}, cost {} ms", operation, tableName,
        System.currentTimeMillis() - start);
      return true;
    } catch (Exception e) {
      LOG.error("Remove HDFS acl error when {} table {}", operation, tableName, e);
      return false;
    }
  }

  /**
   * Remove default acl from namespace archive dir when delete namespace
   * @param namespace the namespace
   * @param removeUsers the users whose default acl will be removed
   * @return false if an error occurred, otherwise true
   */
  public boolean removeNamespaceDefaultAcl(String namespace, Set<String> removeUsers) {
    try {
      long start = System.currentTimeMillis();
      Path archiveNsDir = pathHelper.getArchiveNsDir(namespace);
      HDFSAclOperation operation = new HDFSAclOperation(fs, archiveNsDir, removeUsers,
          HDFSAclOperation.OperationType.REMOVE, false, HDFSAclOperation.AclType.DEFAULT);
      operation.handleAcl();
      LOG.info("Remove HDFS acl when delete namespace {}, cost {} ms", namespace,
        System.currentTimeMillis() - start);
      return true;
    } catch (Exception e) {
      LOG.error("Remove HDFS acl error when delete namespace {}", namespace, e);
      return false;
    }
  }

  /**
   * Remove default acl from table archive dir when delete table
   * @param tableName the table name
   * @param removeUsers the users whose default acl will be removed
   * @return false if an error occurred, otherwise true
   */
  public boolean removeTableDefaultAcl(TableName tableName, Set<String> removeUsers) {
    try {
      long start = System.currentTimeMillis();
      Path archiveTableDir = pathHelper.getArchiveTableDir(tableName);
      HDFSAclOperation operation = new HDFSAclOperation(fs, archiveTableDir, removeUsers,
          HDFSAclOperation.OperationType.REMOVE, false, HDFSAclOperation.AclType.DEFAULT);
      operation.handleAcl();
      LOG.info("Remove HDFS acl when delete table {}, cost {} ms", tableName,
        System.currentTimeMillis() - start);
      return true;
    } catch (Exception e) {
      LOG.error("Remove HDFS acl error when delete table {}", tableName, e);
      return false;
    }
  }

  /**
   * Add table user acls
   * @param tableName the table
   * @param users the table users with READ permission
   * @return false if an error occurred, otherwise true
   */
  public boolean addTableAcl(TableName tableName, Set<String> users, String operation) {
    try {
      long start = System.currentTimeMillis();
      if (users.size() > 0) {
        HDFSAclOperation.OperationType operationType = HDFSAclOperation.OperationType.MODIFY;
        handleNamespaceAccessAcl(tableName.getNamespaceAsString(), users, operationType);
        handleTableAcl(Sets.newHashSet(tableName), users, new HashSet<>(0), new HashSet<>(0),
          operationType);
      }
      LOG.info("Set HDFS acl when {} table {}, cost {} ms", operation, tableName,
        System.currentTimeMillis() - start);
      return true;
    } catch (Exception e) {
      LOG.error("Set HDFS acl error when {} table {}", operation, tableName, e);
      return false;
    }
  }

  /**
   * Remove table acls when modify table
   * @param tableName the table
   * @param users the table users with READ permission
   * @return false if an error occurred, otherwise true
   */
  public boolean removeTableAcl(TableName tableName, Set<String> users) {
    try {
      long start = System.currentTimeMillis();
      if (users.size() > 0) {
        handleTableAcl(Sets.newHashSet(tableName), users, new HashSet<>(0), new HashSet<>(0),
          HDFSAclOperation.OperationType.REMOVE);
      }
      LOG.info("Set HDFS acl when create or modify table {}, cost {} ms", tableName,
        System.currentTimeMillis() - start);
      return true;
    } catch (Exception e) {
      LOG.error("Set HDFS acl error when create or modify table {}", tableName, e);
      return false;
    }
  }

  private void handleGrantOrRevokeAcl(UserPermission userPermission,
      HDFSAclOperation.OperationType operationType, Set<String> skipNamespaces,
      Set<TableName> skipTables) throws ExecutionException, InterruptedException, IOException {
    Set<String> users = Sets.newHashSet(userPermission.getUser());
    switch (userPermission.getAccessScope()) {
      case GLOBAL:
        handleGlobalAcl(users, skipNamespaces, skipTables, operationType);
        break;
      case NAMESPACE:
        NamespacePermission namespacePermission =
            (NamespacePermission) userPermission.getPermission();
        handleNamespaceAcl(Sets.newHashSet(namespacePermission.getNamespace()), users,
          skipNamespaces, skipTables, operationType);
        break;
      case TABLE:
        TablePermission tablePermission = (TablePermission) userPermission.getPermission();
        handleNamespaceAccessAcl(tablePermission.getNamespace(), users, operationType);
        handleTableAcl(Sets.newHashSet(tablePermission.getTableName()), users, skipNamespaces,
          skipTables, operationType);
        break;
      default:
        throw new IllegalArgumentException(
            "Illegal user permission scope " + userPermission.getAccessScope());
    }
  }

  private void handleGlobalAcl(Set<String> users, Set<String> skipNamespaces,
      Set<TableName> skipTables, HDFSAclOperation.OperationType operationType)
      throws ExecutionException, InterruptedException, IOException {
    // handle global root directories HDFS acls
    List<HDFSAclOperation> hdfsAclOperations = getGlobalRootPaths().stream()
        .map(path -> new HDFSAclOperation(fs, path, users, operationType, false,
            HDFSAclOperation.AclType.DEFAULT_ADN_ACCESS))
        .collect(Collectors.toList());
    handleHDFSAclParallel(hdfsAclOperations).get();
    // handle namespace HDFS acls
    handleNamespaceAcl(Sets.newHashSet(admin.listNamespaces()), users, skipNamespaces, skipTables,
      operationType);
  }

  private void handleNamespaceAcl(Set<String> namespaces, Set<String> users,
      Set<String> skipNamespaces, Set<TableName> skipTables,
      HDFSAclOperation.OperationType operationType)
      throws ExecutionException, InterruptedException, IOException {
    namespaces.removeAll(skipNamespaces);
    namespaces.remove(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
    // handle namespace root directories HDFS acls
    List<HDFSAclOperation> hdfsAclOperations = new ArrayList<>();
    Set<String> skipTableNamespaces =
        skipTables.stream().map(TableName::getNamespaceAsString).collect(Collectors.toSet());
    for (String ns : namespaces) {
      /**
       * When op is REMOVE, remove the DEFAULT namespace ACL while keep the ACCESS for skipTables,
       * otherwise remove both the DEFAULT + ACCESS ACLs. When op is MODIFY, just operate the
       * DEFAULT + ACCESS ACLs.
       */
      HDFSAclOperation.OperationType op = operationType;
      HDFSAclOperation.AclType aclType = HDFSAclOperation.AclType.DEFAULT_ADN_ACCESS;
      if (operationType == HDFSAclOperation.OperationType.REMOVE
          && skipTableNamespaces.contains(ns)) {
        // remove namespace directories default HDFS acls for skip tables
        op = HDFSAclOperation.OperationType.REMOVE;
        aclType = HDFSAclOperation.AclType.DEFAULT;
      }
      for (Path path : getNamespaceRootPaths(ns)) {
        hdfsAclOperations.add(new HDFSAclOperation(fs, path, users, op, false, aclType));
      }
    }
    handleHDFSAclParallel(hdfsAclOperations).get();
    // handle table directories HDFS acls
    Set<TableName> tables = new HashSet<>();
    for (String namespace : namespaces) {
      tables.addAll(admin.listTableDescriptorsByNamespace(Bytes.toBytes(namespace)).stream()
          .filter(this::isAclSyncToHdfsEnabled).map(TableDescriptor::getTableName)
          .collect(Collectors.toSet()));
    }
    handleTableAcl(tables, users, skipNamespaces, skipTables, operationType);
  }

  private void handleTableAcl(Set<TableName> tableNames, Set<String> users,
      Set<String> skipNamespaces, Set<TableName> skipTables,
      HDFSAclOperation.OperationType operationType)
      throws ExecutionException, InterruptedException, IOException {
    Set<TableName> filterTableNames = new HashSet<>();
    for (TableName tableName : tableNames) {
      if (!skipTables.contains(tableName)
          && !skipNamespaces.contains(tableName.getNamespaceAsString())) {
        filterTableNames.add(tableName);
      }
    }
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    // handle table HDFS acls
    for (TableName tableName : filterTableNames) {
      List<HDFSAclOperation> hdfsAclOperations = getTableRootPaths(tableName, true).stream()
          .map(path -> new HDFSAclOperation(fs, path, users, operationType, true,
              HDFSAclOperation.AclType.DEFAULT_ADN_ACCESS))
          .collect(Collectors.toList());
      CompletableFuture<Void> future = handleHDFSAclSequential(hdfsAclOperations);
      futures.add(future);
    }
    CompletableFuture<Void> future =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    future.get();
  }

  private void handleNamespaceAccessAcl(String namespace, Set<String> users,
      HDFSAclOperation.OperationType operationType)
      throws ExecutionException, InterruptedException {
    // handle namespace access HDFS acls
    List<HDFSAclOperation> hdfsAclOperations =
        getNamespaceRootPaths(namespace).stream().map(path -> new HDFSAclOperation(fs, path, users,
            operationType, false, HDFSAclOperation.AclType.ACCESS)).collect(Collectors.toList());
    CompletableFuture<Void> future = handleHDFSAclParallel(hdfsAclOperations);
    future.get();
  }

  void createTableDirectories(TableName tableName) throws IOException {
    List<Path> paths = getTableRootPaths(tableName, false);
    for (Path path : paths) {
      createDirIfNotExist(path);
    }
  }

  /**
   * return paths that user will global permission will visit
   * @return the path list
   */
  List<Path> getGlobalRootPaths() {
    return Lists.newArrayList(pathHelper.getTmpDataDir(), pathHelper.getDataDir(),
      pathHelper.getMobDataDir(), pathHelper.getArchiveDataDir(), pathHelper.getSnapshotRootDir());
  }

  /**
   * return paths that user will namespace permission will visit
   * @param namespace the namespace
   * @return the path list
   */
  List<Path> getNamespaceRootPaths(String namespace) {
    return Lists.newArrayList(pathHelper.getTmpNsDir(namespace), pathHelper.getDataNsDir(namespace),
      pathHelper.getMobDataNsDir(namespace), pathHelper.getArchiveNsDir(namespace));
  }

  /**
   * return paths that user will table permission will visit
   * @param tableName the table
   * @param includeSnapshotPath true if return table snapshots paths, otherwise false
   * @return the path list
   * @throws IOException if an error occurred
   */
  List<Path> getTableRootPaths(TableName tableName, boolean includeSnapshotPath)
      throws IOException {
    List<Path> paths = Lists.newArrayList(pathHelper.getTmpTableDir(tableName),
      pathHelper.getDataTableDir(tableName), pathHelper.getMobTableDir(tableName),
      pathHelper.getArchiveTableDir(tableName));
    if (includeSnapshotPath) {
      paths.addAll(getTableSnapshotPaths(tableName));
    }
    return paths;
  }

  private List<Path> getTableSnapshotPaths(TableName tableName) throws IOException {
    return admin.listSnapshots().stream()
        .filter(snapDesc -> snapDesc.getTableName().equals(tableName))
        .map(snapshotDescription -> pathHelper.getSnapshotDir(snapshotDescription.getName()))
        .collect(Collectors.toList());
  }

  /**
   * Return users with global read permission
   * @return users with global read permission
   * @throws IOException if an error occurred
   */
  private Set<String> getUsersWithGlobalReadAction() throws IOException {
    return getUsersWithReadAction(PermissionStorage.getGlobalPermissions(conf));
  }

  /**
   * Return users with namespace read permission
   * @param namespace the namespace
   * @param includeGlobal true if include users with global read action
   * @return users with namespace read permission
   * @throws IOException if an error occurred
   */
  Set<String> getUsersWithNamespaceReadAction(String namespace, boolean includeGlobal)
      throws IOException {
    Set<String> users =
        getUsersWithReadAction(PermissionStorage.getNamespacePermissions(conf, namespace));
    if (includeGlobal) {
      users.addAll(getUsersWithGlobalReadAction());
    }
    return users;
  }

  /**
   * Return users with table read permission
   * @param tableName the table
   * @param includeNamespace true if include users with namespace read action
   * @param includeGlobal true if include users with global read action
   * @return users with table read permission
   * @throws IOException if an error occurred
   */
  Set<String> getUsersWithTableReadAction(TableName tableName, boolean includeNamespace,
      boolean includeGlobal) throws IOException {
    Set<String> users =
        getUsersWithReadAction(PermissionStorage.getTablePermissions(conf, tableName));
    if (includeNamespace) {
      users
          .addAll(getUsersWithNamespaceReadAction(tableName.getNamespaceAsString(), includeGlobal));
    }
    return users;
  }

  private Set<String>
      getUsersWithReadAction(ListMultimap<String, UserPermission> permissionMultimap) {
    return permissionMultimap.entries().stream()
        .filter(entry -> checkUserPermission(entry.getValue())).map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  private boolean checkUserPermission(UserPermission userPermission) {
    boolean result = containReadAction(userPermission);
    if (result && userPermission.getPermission() instanceof TablePermission) {
      result = isNotFamilyOrQualifierPermission((TablePermission) userPermission.getPermission());
    }
    return result;
  }

  boolean containReadAction(UserPermission userPermission) {
    return userPermission.getPermission().implies(Permission.Action.READ);
  }

  boolean isNotFamilyOrQualifierPermission(TablePermission tablePermission) {
    return !tablePermission.hasFamily() && !tablePermission.hasQualifier();
  }

  public static boolean isAclSyncToHdfsEnabled(Configuration conf) {
    String[] masterCoprocessors = conf.getStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
    Set<String> masterCoprocessorSet = new HashSet<>();
    if (masterCoprocessors != null) {
      Collections.addAll(masterCoprocessorSet, masterCoprocessors);
    }
    return conf.getBoolean(SnapshotScannerHDFSAclHelper.ACL_SYNC_TO_HDFS_ENABLE, false)
        && masterCoprocessorSet.contains(SnapshotScannerHDFSAclController.class.getName())
        && masterCoprocessorSet.contains(AccessController.class.getName());
  }

  boolean isAclSyncToHdfsEnabled(TableDescriptor tableDescriptor) {
    return tableDescriptor == null ? false
        : Boolean.valueOf(tableDescriptor.getValue(ACL_SYNC_TO_HDFS_ENABLE));
  }

  PathHelper getPathHelper() {
    return pathHelper;
  }

  private CompletableFuture<Void> handleHDFSAcl(HDFSAclOperation acl) {
    return CompletableFuture.supplyAsync(() -> {
      List<HDFSAclOperation> childAclOperations = new ArrayList<>();
      try {
        acl.handleAcl();
        childAclOperations = acl.getChildAclOperations();
      } catch (FileNotFoundException e) {
        // Skip handle acl if file not found
      } catch (IOException e) {
        LOG.error("Set HDFS acl error for path {}", acl.path, e);
      }
      return childAclOperations;
    }, pool).thenComposeAsync(this::handleHDFSAclParallel, pool);
  }

  private CompletableFuture<Void> handleHDFSAclSequential(List<HDFSAclOperation> operations) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        for (HDFSAclOperation hdfsAclOperation : operations) {
          handleHDFSAcl(hdfsAclOperation).get();
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Set HDFS acl error", e);
      }
      return null;
    }, pool);
  }

  private CompletableFuture<Void> handleHDFSAclParallel(List<HDFSAclOperation> operations) {
    List<CompletableFuture<Void>> futures =
        operations.stream().map(this::handleHDFSAcl).collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  private static AclEntry aclEntry(AclEntryScope scope, String name) {
    return new AclEntry.Builder().setScope(scope)
        .setType(AuthUtil.isGroupPrincipal(name) ? GROUP : USER).setName(name)
        .setPermission(READ_EXECUTE).build();
  }

  void createDirIfNotExist(Path path) throws IOException {
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  void deleteEmptyDir(Path path) throws IOException {
    if (fs.exists(path) && fs.listStatus(path).length == 0) {
      fs.delete(path, false);
    }
  }

  /**
   * Inner class used to describe modify or remove what type of acl entries(ACCESS, DEFAULT,
   * ACCESS_AND_DEFAULT) for files or directories(and child files).
   */
  private static class HDFSAclOperation {
    enum OperationType {
      MODIFY, REMOVE
    }

    enum AclType {
      ACCESS, DEFAULT, DEFAULT_ADN_ACCESS
    }

    private interface Operation {
      void apply(FileSystem fs, Path path, List<AclEntry> aclList) throws IOException;
    }

    private FileSystem fs;
    private Path path;
    private Operation operation;
    private boolean recursive;
    private AclType aclType;
    private List<AclEntry> defaultAndAccessAclEntries;
    private List<AclEntry> accessAclEntries;
    private List<AclEntry> defaultAclEntries;

    HDFSAclOperation(FileSystem fs, Path path, Set<String> users, OperationType operationType,
        boolean recursive, AclType aclType) {
      this.fs = fs;
      this.path = path;
      this.defaultAndAccessAclEntries = getAclEntries(AclType.DEFAULT_ADN_ACCESS, users);
      this.accessAclEntries = getAclEntries(AclType.ACCESS, users);
      this.defaultAclEntries = getAclEntries(AclType.DEFAULT, users);
      if (operationType == OperationType.MODIFY) {
        operation = FileSystem::modifyAclEntries;
      } else if (operationType == OperationType.REMOVE) {
        operation = FileSystem::removeAclEntries;
      } else {
        throw new IllegalArgumentException("Illegal HDFS acl operation type: " + operationType);
      }
      this.recursive = recursive;
      this.aclType = aclType;
    }

    HDFSAclOperation(Path path, HDFSAclOperation parent) {
      this.fs = parent.fs;
      this.path = path;
      this.defaultAndAccessAclEntries = parent.defaultAndAccessAclEntries;
      this.accessAclEntries = parent.accessAclEntries;
      this.defaultAclEntries = parent.defaultAclEntries;
      this.operation = parent.operation;
      this.recursive = parent.recursive;
      this.aclType = parent.aclType;
    }

    List<HDFSAclOperation> getChildAclOperations() throws IOException {
      List<HDFSAclOperation> hdfsAclOperations = new ArrayList<>();
      if (recursive && fs.isDirectory(path)) {
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
          hdfsAclOperations.add(new HDFSAclOperation(fileStatus.getPath(), this));
        }
      }
      return hdfsAclOperations;
    }

    void handleAcl() throws IOException {
      if (fs.exists(path)) {
        if (fs.isDirectory(path)) {
          switch (aclType) {
            case ACCESS:
              operation.apply(fs, path, accessAclEntries);
              break;
            case DEFAULT:
              operation.apply(fs, path, defaultAclEntries);
              break;
            case DEFAULT_ADN_ACCESS:
              operation.apply(fs, path, defaultAndAccessAclEntries);
              break;
            default:
              throw new IllegalArgumentException("Illegal HDFS acl type: " + aclType);
          }
        } else {
          operation.apply(fs, path, accessAclEntries);
        }
      }
    }

    private List<AclEntry> getAclEntries(AclType aclType, Set<String> users) {
      List<AclEntry> aclEntries = new ArrayList<>();
      switch (aclType) {
        case ACCESS:
          for (String user : users) {
            aclEntries.add(aclEntry(ACCESS, user));
          }
          break;
        case DEFAULT:
          for (String user : users) {
            aclEntries.add(aclEntry(DEFAULT, user));
          }
          break;
        case DEFAULT_ADN_ACCESS:
          for (String user : users) {
            aclEntries.add(aclEntry(ACCESS, user));
            aclEntries.add(aclEntry(DEFAULT, user));
          }
          break;
        default:
          throw new IllegalArgumentException("Illegal HDFS acl type: " + aclType);
      }
      return aclEntries;
    }
  }

  static final class PathHelper {
    Configuration conf;
    Path rootDir;
    Path tmpDataDir;
    Path dataDir;
    Path mobDataDir;
    Path archiveDataDir;
    Path snapshotDir;

    PathHelper(Configuration conf) {
      this.conf = conf;
      rootDir = new Path(conf.get(HConstants.HBASE_DIR));
      tmpDataDir = new Path(new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY),
          HConstants.BASE_NAMESPACE_DIR);
      dataDir = new Path(rootDir, HConstants.BASE_NAMESPACE_DIR);
      mobDataDir = new Path(MobUtils.getMobHome(rootDir), HConstants.BASE_NAMESPACE_DIR);
      archiveDataDir = new Path(new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY),
          HConstants.BASE_NAMESPACE_DIR);
      snapshotDir = new Path(rootDir, HConstants.SNAPSHOT_DIR_NAME);
    }

    Path getRootDir() {
      return rootDir;
    }

    Path getDataDir() {
      return dataDir;
    }

    Path getMobDir() {
      return mobDataDir.getParent();
    }

    Path getMobDataDir() {
      return mobDataDir;
    }

    Path getTmpDir() {
      return new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY);
    }

    Path getTmpDataDir() {
      return tmpDataDir;
    }

    Path getArchiveDir() {
      return new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    }

    Path getArchiveDataDir() {
      return archiveDataDir;
    }

    Path getDataNsDir(String namespace) {
      return new Path(dataDir, namespace);
    }

    Path getMobDataNsDir(String namespace) {
      return new Path(mobDataDir, namespace);
    }

    Path getDataTableDir(TableName tableName) {
      return new Path(getDataNsDir(tableName.getNamespaceAsString()),
          tableName.getQualifierAsString());
    }

    Path getMobTableDir(TableName tableName) {
      return new Path(getMobDataNsDir(tableName.getNamespaceAsString()),
          tableName.getQualifierAsString());
    }

    Path getArchiveNsDir(String namespace) {
      return new Path(archiveDataDir, namespace);
    }

    Path getArchiveTableDir(TableName tableName) {
      return new Path(getArchiveNsDir(tableName.getNamespaceAsString()),
          tableName.getQualifierAsString());
    }

    Path getTmpNsDir(String namespace) {
      return new Path(tmpDataDir, namespace);
    }

    Path getTmpTableDir(TableName tableName) {
      return new Path(getTmpNsDir(tableName.getNamespaceAsString()),
          tableName.getQualifierAsString());
    }

    Path getSnapshotRootDir() {
      return snapshotDir;
    }

    Path getSnapshotDir(String snapshot) {
      return new Path(snapshotDir, snapshot);
    }

    FileSystem getFileSystem() throws IOException {
      return rootDir.getFileSystem(conf);
    }
  }
}
