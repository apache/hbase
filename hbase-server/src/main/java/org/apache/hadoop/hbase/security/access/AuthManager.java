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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Performs authorization checks for a given user's assigned permissions.
 * <p>
 *   There're following scopes: <b>Global</b>, <b>Namespace</b>, <b>Table</b>, <b>Family</b>,
 *   <b>Qualifier</b>, <b>Cell</b>.
 *   Generally speaking, higher scopes can overrides lower scopes,
 *   except for Cell permission can be granted even a user has not permission on specified table,
 *   which means the user can get/scan only those granted cells parts.
 * </p>
 * e.g, if user A has global permission R(ead), he can
 * read table T without checking table scope permission, so authorization checks alway starts from
 * Global scope.
 * <p>
 *   For each scope, not only user but also groups he belongs to will be checked.
 * </p>
 */
@InterfaceAudience.Private
public final class AuthManager implements Closeable {

  /**
   * Cache of permissions, it is thread safe.
   * @param <T> T extends Permission
   */
  private static class PermissionCache<T extends Permission> {
    private final Object mutex = new Object();
    private Map<String, Set<T>> cache = new HashMap<>();

    void put(String name, T perm) {
      synchronized (mutex) {
        Set<T> perms = cache.getOrDefault(name, new HashSet<>());
        perms.add(perm);
        cache.put(name, perms);
      }
    }

    Set<T> get(String name) {
      synchronized (mutex) {
        return cache.get(name);
      }
    }

    void clear() {
      synchronized (mutex) {
        for (Map.Entry<String, Set<T>> entry : cache.entrySet()) {
          entry.getValue().clear();
        }
        cache.clear();
      }
    }
  }
  PermissionCache<NamespacePermission> NS_NO_PERMISSION = new PermissionCache<>();
  PermissionCache<TablePermission> TBL_NO_PERMISSION = new PermissionCache<>();

  /**
   * Cache for global permission.
   * Since every user/group can only have one global permission, no need to user PermissionCache.
   */
  private volatile Map<String, GlobalPermission> globalCache;
  /** Cache for namespace permission. */
  private ConcurrentHashMap<String, PermissionCache<NamespacePermission>> namespaceCache =
    new ConcurrentHashMap<>();
  /** Cache for table permission. */
  private ConcurrentHashMap<TableName, PermissionCache<TablePermission>> tableCache =
    new ConcurrentHashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(AuthManager.class);

  private Configuration conf;
  private ZKPermissionWatcher zkperms;
  private final AtomicLong mtime = new AtomicLong(0L);

  private AuthManager(ZKWatcher watcher, Configuration conf)
      throws IOException {
    this.conf = conf;
    // initialize global permissions based on configuration
    globalCache = initGlobal(conf);

    this.zkperms = new ZKPermissionWatcher(watcher, this, conf);
    try {
      this.zkperms.start();
    } catch (KeeperException ke) {
      LOG.error("ZooKeeper initialization failed", ke);
    }
  }

  @Override
  public void close() {
    this.zkperms.close();
  }

  /**
   * Initialize with global permission assignments
   * from the {@code hbase.superuser} configuration key.
   */
  private Map<String, GlobalPermission> initGlobal(Configuration conf) throws IOException {
    UserProvider userProvider = UserProvider.instantiate(conf);
    User user = userProvider.getCurrent();
    if (user == null) {
      throw new IOException("Unable to obtain the current user, " +
        "authorization checks for internal operations will not work correctly!");
    }
    String currentUser = user.getShortName();

    Map<String, GlobalPermission> global = new HashMap<>();
    // the system user is always included
    List<String> superusers = Lists.asList(currentUser, conf.getStrings(
        Superusers.SUPERUSER_CONF_KEY, new String[0]));
    for (String name : superusers) {
      GlobalPermission globalPermission = new GlobalPermission(Permission.Action.values());
      global.put(name, globalPermission);
    }
    return global;
  }

  public ZKPermissionWatcher getZKPermissionWatcher() {
    return this.zkperms;
  }

  /**
   * Update acl info for table.
   * @param table name of table
   * @param data updated acl data
   * @throws IOException exception when deserialize data
   */
  public void refreshTableCacheFromWritable(TableName table, byte[] data) throws IOException {
    if (data != null && data.length > 0) {
      try {
        ListMultimap<String, Permission> perms =
          AccessControlLists.readPermissions(data, conf);
        if (perms != null) {
          if (Bytes.equals(table.getName(), AccessControlLists.ACL_GLOBAL_NAME)) {
            updateGlobalCache(perms);
          } else {
            updateTableCache(table, perms);
          }
        }
      } catch (DeserializationException e) {
        throw new IOException(e);
      }
    } else {
      LOG.info("Skipping permission cache refresh because writable data is empty");
    }
  }

  /**
   * Update acl info for namespace.
   * @param namespace namespace
   * @param data updated acl data
   * @throws IOException exception when deserialize data
   */
  public void refreshNamespaceCacheFromWritable(String namespace, byte[] data) throws IOException {
    if (data != null && data.length > 0) {
      try {
        ListMultimap<String, Permission> perms =
          AccessControlLists.readPermissions(data, conf);
        if (perms != null) {
          updateNamespaceCache(namespace, perms);
        }
      } catch (DeserializationException e) {
        throw new IOException(e);
      }
    } else {
      LOG.debug("Skipping permission cache refresh because writable data is empty");
    }
  }

  /**
   * Updates the internal global permissions cache.
   * @param globalPerms new global permissions
   */
  private void updateGlobalCache(ListMultimap<String, Permission> globalPerms) {
    try {
      Map<String, GlobalPermission> global = initGlobal(conf);
      for (String name : globalPerms.keySet()) {
        for (Permission permission : globalPerms.get(name)) {
          global.put(name, (GlobalPermission) permission);
        }
      }
      globalCache = global;
      mtime.incrementAndGet();
    } catch (Exception e) {
      // Never happens
      LOG.error("Error occurred while updating the global cache", e);
    }
  }

  /**
   * Updates the internal table permissions cache for specified table.
   * @param table updated table name
   * @param tablePerms new table permissions
   */
  private void updateTableCache(TableName table, ListMultimap<String, Permission> tablePerms) {
    PermissionCache<TablePermission> cacheToUpdate =
      tableCache.getOrDefault(table, new PermissionCache<>());
    clearCache(cacheToUpdate);
    updateCache(tablePerms, cacheToUpdate);
    tableCache.put(table, cacheToUpdate);
    mtime.incrementAndGet();
  }

  /**
   * Updates the internal namespace permissions cache for specified namespace.
   * @param namespace updated namespace
   * @param nsPerms new namespace permissions
   */
  private void updateNamespaceCache(String namespace,
      ListMultimap<String, Permission> nsPerms) {
    PermissionCache<NamespacePermission> cacheToUpdate =
      namespaceCache.getOrDefault(namespace, new PermissionCache<>());
    clearCache(cacheToUpdate);
    updateCache(nsPerms, cacheToUpdate);
    namespaceCache.put(namespace, cacheToUpdate);
    mtime.incrementAndGet();
  }

  private void clearCache(PermissionCache cacheToUpdate) {
    cacheToUpdate.clear();
  }

  @SuppressWarnings("unchecked")
  private void updateCache(ListMultimap<String, ? extends Permission> newPermissions,
      PermissionCache cacheToUpdate) {
    for (String name : newPermissions.keySet()) {
      for (Permission permission : newPermissions.get(name)) {
        cacheToUpdate.put(name, permission);
      }
    }
  }

  /**
   * Check if user has given action privilige in global scope.
   * @param user user name
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserGlobal(User user, Permission.Action action) {
    if (user == null) {
      return false;
    }
    if (authorizeGlobal(globalCache.get(user.getShortName()), action)) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (authorizeGlobal(globalCache.get(AuthUtil.toGroupEntry(group)), action)) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeGlobal(GlobalPermission permissions, Permission.Action action) {
    return permissions != null && permissions.implies(action);
  }

  /**
   * Check if user has given action privilige in namespace scope.
   * @param user user name
   * @param namespace namespace
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserNamespace(User user, String namespace, Permission.Action action) {
    if (user == null) {
      return false;
    }
    if (authorizeUserGlobal(user, action)) {
      return true;
    }
    PermissionCache<NamespacePermission> nsPermissions = namespaceCache.getOrDefault(namespace,
      NS_NO_PERMISSION);
    if (authorizeNamespace(nsPermissions.get(user.getShortName()), namespace, action)) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (authorizeNamespace(nsPermissions.get(AuthUtil.toGroupEntry(group)), namespace, action)) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeNamespace(Set<NamespacePermission> permissions,
      String namespace, Permission.Action action) {
    if (permissions == null) {
      return false;
    }
    for (NamespacePermission permission : permissions) {
      if (permission.implies(namespace, action)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the user has access to the full table or at least a family/qualifier
   * for the specified action.
   * @param user user name
   * @param table table name
   * @param action action in one of [Read, Write, Create, Exec, Admin]
   * @return true if the user has access to the table, false otherwise
   */
  public boolean accessUserTable(User user, TableName table, Permission.Action action) {
    if (user == null) {
      return false;
    }
    if (table == null) {
      table = AccessControlLists.ACL_TABLE_NAME;
    }
    if (authorizeUserNamespace(user, table.getNamespaceAsString(), action)) {
      return true;
    }
    PermissionCache<TablePermission> tblPermissions = tableCache.getOrDefault(table,
      TBL_NO_PERMISSION);
    if (hasAccessTable(tblPermissions.get(user.getShortName()), action)) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (hasAccessTable(tblPermissions.get(AuthUtil.toGroupEntry(group)), action)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasAccessTable(Set<TablePermission> permissions, Permission.Action action) {
    if (permissions == null) {
      return false;
    }
    for (TablePermission permission : permissions) {
      if (permission.implies(action)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if user has given action privilige in table scope.
   * @param user user name
   * @param table table name
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserTable(User user, TableName table, Permission.Action action) {
    return authorizeUserTable(user, table, null, null, action);
  }

  /**
   * Check if user has given action privilige in table:family scope.
   * @param user user name
   * @param table table name
   * @param family family name
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserTable(User user, TableName table, byte[] family,
      Permission.Action action) {
    return authorizeUserTable(user, table, family, null, action);
  }

  /**
   * Check if user has given action privilige in table:family:qualifier scope.
   * @param user user name
   * @param table table name
   * @param family family name
   * @param qualifier qualifier name
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserTable(User user, TableName table, byte[] family,
      byte[] qualifier, Permission.Action action) {
    if (user == null) {
      return false;
    }
    if (table == null) {
      table = AccessControlLists.ACL_TABLE_NAME;
    }
    if (authorizeUserNamespace(user, table.getNamespaceAsString(), action)) {
      return true;
    }
    PermissionCache<TablePermission> tblPermissions = tableCache.getOrDefault(table,
      TBL_NO_PERMISSION);
    if (authorizeTable(tblPermissions.get(user.getShortName()), table, family, qualifier, action)) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (authorizeTable(tblPermissions.get(AuthUtil.toGroupEntry(group)),
          table, family, qualifier, action)) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeTable(Set<TablePermission> permissions,
      TableName table, byte[] family, byte[] qualifier, Permission.Action action) {
    if (permissions == null) {
      return false;
    }
    for (TablePermission permission : permissions) {
      if (permission.implies(table, family, qualifier, action)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if user has given action privilige in table:family scope.
   * This method is for backward compatibility.
   * @param user user name
   * @param table table name
   * @param family family names
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserFamily(User user, TableName table,
      byte[] family, Permission.Action action) {
    PermissionCache<TablePermission> tblPermissions = tableCache.getOrDefault(table,
      TBL_NO_PERMISSION);
    if (authorizeFamily(tblPermissions.get(user.getShortName()), table, family, action)) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (authorizeFamily(tblPermissions.get(AuthUtil.toGroupEntry(group)),
          table, family, action)) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeFamily(Set<TablePermission> permissions,
      TableName table, byte[] family, Permission.Action action) {
    if (permissions == null) {
      return false;
    }
    for (TablePermission permission : permissions) {
      if (permission.implies(table, family, action)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if user has given action privilige in cell scope.
   * @param user user name
   * @param table table name
   * @param cell cell to be checked
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeCell(User user, TableName table, Cell cell, Permission.Action action) {
    try {
      List<Permission> perms = AccessControlLists.getCellPermissionsForUser(user, cell);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Perms for user " + user.getShortName() + " in cell " + cell + ": " +
          (perms != null ? perms : ""));
      }
      if (perms != null) {
        for (Permission p: perms) {
          if (p.implies(action)) {
            return true;
          }
        }
      }
    } catch (IOException e) {
      // We failed to parse the KV tag
      LOG.error("Failed parse of ACL tag in cell " + cell);
      // Fall through to check with the table and CF perms we were able
      // to collect regardless
    }
    return false;
  }

  /**
   * Remove given namespace from AuthManager's namespace cache.
   * @param ns namespace
   */
  public void removeNamespace(byte[] ns) {
    namespaceCache.remove(Bytes.toString(ns));
  }

  /**
   * Remove given table from AuthManager's table cache.
   * @param table table name
   */
  public void removeTable(TableName table) {
    tableCache.remove(table);
  }

  /**
   * Last modification logical time
   * @return time
   */
  public long getMTime() {
    return mtime.get();
  }

  private static Map<ZKWatcher, AuthManager> managerMap = new HashMap<>();

  private static Map<AuthManager, Integer> refCount = new HashMap<>();

  /**
   * Returns a AuthManager from the cache. If not cached, constructs a new one.
   * Returned instance should be released back by calling {@link #release(AuthManager)}.
   * @param watcher zk watcher
   * @param conf configuration
   * @return an AuthManager
   * @throws IOException zookeeper initialization failed
   */
  public synchronized static AuthManager getOrCreate(
      ZKWatcher watcher, Configuration conf) throws IOException {
    AuthManager instance = managerMap.get(watcher);
    if (instance == null) {
      instance = new AuthManager(watcher, conf);
      managerMap.put(watcher, instance);
    }
    int ref = refCount.get(instance) == null ? 0 : refCount.get(instance);
    refCount.put(instance, ref + 1);
    return instance;
  }

  @VisibleForTesting
  public static int getTotalRefCount() {
    int total = 0;
    for (int count : refCount.values()) {
      total += count;
    }
    return total;
  }

  /**
   * Releases the resources for the given AuthManager if the reference count is down to 0.
   * @param instance AuthManager to be released
   */
  public synchronized static void release(AuthManager instance) {
    if (refCount.get(instance) == null || refCount.get(instance) < 1) {
      String msg = "Something wrong with the AuthManager reference counting: " + instance
          + " whose count is " + refCount.get(instance);
      LOG.error(HBaseMarkers.FATAL, msg);
      instance.close();
      managerMap.remove(instance.getZKPermissionWatcher().getWatcher());
      instance.getZKPermissionWatcher().getWatcher().abort(msg, null);
    } else {
      int ref = refCount.get(instance);
      --ref;
      refCount.put(instance, ref);
      if (ref == 0) {
        instance.close();
        managerMap.remove(instance.getZKPermissionWatcher().getWatcher());
        refCount.remove(instance);
      }
    }
  }
}
