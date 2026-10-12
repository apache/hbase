/*
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Performs authorization checks for a given user's assigned permissions.
 * <p>
 * There're following scopes: <b>Global</b>, <b>Namespace</b>, <b>Table</b>, <b>Family</b>,
 * <b>Qualifier</b>, <b>Cell</b>. Generally speaking, higher scopes can overrides lower scopes,
 * except for Cell permission can be granted even a user has not permission on specified table,
 * which means the user can get/scan only those granted cells parts.
 * </p>
 * e.g, if user A has global permission R(ead), he can read table T without checking table scope
 * permission, so authorization checks alway starts from Global scope.
 * <p>
 * For each scope, not only user but also groups he belongs to will be checked.
 * </p>
 */
@InterfaceAudience.Private
public final class AuthManager {

  /**
   * Cache of permissions, it is thread safe.
   * @param <T> T extends Permission
   */
  private static class PermissionCache<T extends Permission> {
    private final Object mutex = new Object();
    private Map<String, Set<T>> cache = new HashMap<>();

    void put(String name, T perm) {
      synchronized (mutex) {
        Set<T> perms = cache.getOrDefault(name, ConcurrentHashMap.newKeySet());
        perms.add(perm);
        cache.put(name, perms);
      }
    }

    Set<T> get(String name) {
      synchronized (mutex) {
        return cache.get(name);
      }
    }
  }

  /**
   * Replacement of a single ACL entry, or its deletion. The entry name encoding is the row key of
   * {@code hbase:acl}: table name bytes, {@code @} prefixed namespace name, or
   * {@link PermissionStorage#ACL_GLOBAL_NAME} for global scope.
   */
  public static final class EntryUpdate {
    private final byte[] entry;
    private final ListMultimap<String, UserPermission> perms;
    private final boolean deleted;

    private EntryUpdate(byte[] entry, ListMultimap<String, UserPermission> perms, boolean deleted) {
      this.entry = entry;
      this.perms = perms;
      this.deleted = deleted;
    }

    /** Replaces the whole permission set of the entry. */
    public static EntryUpdate replace(byte[] entry, ListMultimap<String, UserPermission> perms) {
      return new EntryUpdate(entry, perms, false);
    }

    /** Marks the entry as removed. */
    public static EntryUpdate deleted(byte[] entry) {
      return new EntryUpdate(entry, null, true);
    }

    public byte[] getEntry() {
      return entry;
    }

    public ListMultimap<String, UserPermission> getPermissions() {
      return perms;
    }

    public boolean isDeleted() {
      return deleted;
    }
  }

  PermissionCache<NamespacePermission> NS_NO_PERMISSION = new PermissionCache<>();
  PermissionCache<TablePermission> TBL_NO_PERMISSION = new PermissionCache<>();

  /**
   * Cache for global permission excluding superuser and supergroup. Since every user/group can only
   * have one global permission, no need to use PermissionCache.
   */
  private volatile Map<String, GlobalPermission> globalCache = new ConcurrentHashMap<>();
  /** Cache for namespace permission. */
  private volatile ConcurrentHashMap<String, PermissionCache<NamespacePermission>> namespaceCache =
    new ConcurrentHashMap<>();
  /** Cache for table permission. */
  private volatile ConcurrentHashMap<TableName, PermissionCache<TablePermission>> tableCache =
    new ConcurrentHashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(AuthManager.class);

  private Configuration conf;
  private final AtomicLong mtime = new AtomicLong(0L);

  AuthManager(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Updates the internal global permissions cache.
   * @param globalPerms new global permissions
   */
  private void updateGlobalCache(ListMultimap<String, Permission> globalPerms) {
    Map<String, GlobalPermission> newGlobalCache = new ConcurrentHashMap<>();
    for (String name : globalPerms.keySet()) {
      for (Permission permission : globalPerms.get(name)) {
        newGlobalCache.put(name, toGlobalPermission(permission));
      }
    }
    globalCache = newGlobalCache;
    mtime.incrementAndGet();
  }

  /**
   * Updates the internal table permissions cache for specified table.
   * @param table      updated table name
   * @param tablePerms new table permissions
   */
  private void updateTableCache(TableName table, ListMultimap<String, Permission> tablePerms) {
    PermissionCache<TablePermission> cacheToUpdate = new PermissionCache<>();
    updateCache(tablePerms, cacheToUpdate);
    tableCache.put(table, cacheToUpdate);
    mtime.incrementAndGet();
  }

  /**
   * Updates the internal namespace permissions cache for specified namespace.
   * @param namespace updated namespace
   * @param nsPerms   new namespace permissions
   */
  private void updateNamespaceCache(String namespace, ListMultimap<String, Permission> nsPerms) {
    PermissionCache<NamespacePermission> cacheToUpdate = new PermissionCache<>();
    updateCache(nsPerms, cacheToUpdate);
    namespaceCache.put(namespace, cacheToUpdate);
    mtime.incrementAndGet();
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
   * Applies incremental ACL entry updates, each update replaces the whole entry in the cache, or
   * removes the entry when it is marked as deleted.
   */
  public void applyUpdates(List<EntryUpdate> updates) {
    for (EntryUpdate update : updates) {
      applyEntry(update.getEntry(), update.isDeleted() ? null : update.getPermissions());
    }
  }

  /**
   * Replaces the whole cache with the given snapshot. Callers build the snapshot aside first, so
   * readers never observe a partially loaded cache.
   * @param snapshot entry name to its permissions, see {@link PermissionStorage#loadAll}
   */
  public void applyFullSnapshot(Map<byte[], ListMultimap<String, UserPermission>> snapshot) {
    Map<String, GlobalPermission> newGlobalCache = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, PermissionCache<NamespacePermission>> newNamespaceCache =
      new ConcurrentHashMap<>();
    ConcurrentHashMap<TableName, PermissionCache<TablePermission>> newTableCache =
      new ConcurrentHashMap<>();
    for (Map.Entry<byte[], ListMultimap<String, UserPermission>> e : snapshot.entrySet()) {
      byte[] entry = e.getKey();
      ListMultimap<String, Permission> perms = toPermissions(e.getValue());
      if (PermissionStorage.isGlobalEntry(entry)) {
        for (String name : perms.keySet()) {
          for (Permission permission : perms.get(name)) {
            newGlobalCache.put(name, toGlobalPermission(permission));
          }
        }
      } else if (PermissionStorage.isNamespaceEntry(entry)) {
        PermissionCache<NamespacePermission> cacheToUpdate = new PermissionCache<>();
        updateCache(perms, cacheToUpdate);
        newNamespaceCache.put(Bytes.toString(PermissionStorage.fromNamespaceEntry(entry)),
          cacheToUpdate);
      } else {
        PermissionCache<TablePermission> cacheToUpdate = new PermissionCache<>();
        updateCache(perms, cacheToUpdate);
        newTableCache.put(TableName.valueOf(entry), cacheToUpdate);
      }
    }
    globalCache = newGlobalCache;
    namespaceCache = newNamespaceCache;
    tableCache = newTableCache;
    mtime.incrementAndGet();
  }

  private void applyEntry(byte[] entry, ListMultimap<String, UserPermission> perms) {
    if (PermissionStorage.isGlobalEntry(entry)) {
      if (perms == null) {
        globalCache.clear();
        mtime.incrementAndGet();
      } else {
        updateGlobalCache(toPermissions(perms));
      }
    } else if (PermissionStorage.isNamespaceEntry(entry)) {
      if (perms == null) {
        removeNamespace(entry);
      } else {
        updateNamespaceCache(Bytes.toString(PermissionStorage.fromNamespaceEntry(entry)),
          toPermissions(perms));
      }
    } else {
      if (perms == null) {
        removeTable(TableName.valueOf(entry));
      } else {
        updateTableCache(TableName.valueOf(entry), toPermissions(perms));
      }
    }
  }

  private static ListMultimap<String, Permission>
    toPermissions(ListMultimap<String, UserPermission> userPerms) {
    ListMultimap<String, Permission> perms = ArrayListMultimap.create();
    for (Map.Entry<String, UserPermission> e : userPerms.entries()) {
      perms.put(e.getKey(), e.getValue().getPermission());
    }
    return perms;
  }

  private static GlobalPermission toGlobalPermission(Permission permission) {
    if (permission instanceof GlobalPermission) {
      return (GlobalPermission) permission;
    }
    return new GlobalPermission(permission.getActions());
  }

  /**
   * Check if user has given action privilige in global scope.
   * @param user   user name
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserGlobal(User user, Permission.Action action) {
    if (user == null) {
      return false;
    }
    if (Superusers.isSuperUser(user)) {
      return true;
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
   * @param user      user name
   * @param namespace namespace
   * @param action    one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserNamespace(User user, String namespace, Permission.Action action) {
    if (user == null) {
      return false;
    }
    if (authorizeUserGlobal(user, action)) {
      return true;
    }
    PermissionCache<NamespacePermission> nsPermissions =
      namespaceCache.getOrDefault(namespace, NS_NO_PERMISSION);
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

  private boolean authorizeNamespace(Set<NamespacePermission> permissions, String namespace,
    Permission.Action action) {
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
   * Hard coded rules for {@code hbase:meta} and {@code hbase:acl}, evaluated before the cached
   * grants so that bootstrap does not depend on the ACL cache content:
   * <ul>
   * <li>{@code hbase:meta}: READ is allowed for any user, other actions for superuser only</li>
   * <li>{@code hbase:acl}: superuser only</li>
   * </ul>
   * Global and namespace grants must not override these rules. Note that a {@code null} table name
   * carries the historical "global scope" semantics of this class and is not subject to these
   * rules.
   * @return the verdict of the fixed rules, or {@code null} if they do not apply
   */
  private static Boolean fixedSystemTableRule(User user, TableName table,
    Permission.Action action) {
    if (table == null) {
      return null;
    }
    if (TableName.META_TABLE_NAME.equals(table)) {
      return action == Permission.Action.READ ? Boolean.TRUE : Boolean.valueOf(isSuperUser(user));
    }
    if (PermissionStorage.ACL_TABLE_NAME.equals(table)) {
      return isSuperUser(user);
    }
    return null;
  }

  private static boolean isSuperUser(User user) {
    return user != null && Superusers.isSuperUser(user);
  }

  /**
   * Checks if the user has access to the full table or at least a family/qualifier for the
   * specified action.
   * @param user   user name
   * @param table  table name
   * @param action action in one of [Read, Write, Create, Exec, Admin]
   * @return true if the user has access to the table, false otherwise
   */
  public boolean accessUserTable(User user, TableName table, Permission.Action action) {
    Boolean fixed = fixedSystemTableRule(user, table, action);
    if (fixed != null) {
      return fixed;
    }
    if (user == null) {
      return false;
    }
    if (table == null) {
      table = PermissionStorage.ACL_TABLE_NAME;
    }
    if (authorizeUserNamespace(user, table.getNamespaceAsString(), action)) {
      return true;
    }
    PermissionCache<TablePermission> tblPermissions =
      tableCache.getOrDefault(table, TBL_NO_PERMISSION);
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
   * @param user   user name
   * @param table  table name
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserTable(User user, TableName table, Permission.Action action) {
    return authorizeUserTable(user, table, null, null, action);
  }

  /**
   * Check if user has given action privilige in table:family scope.
   * @param user   user name
   * @param table  table name
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
   * @param user      user name
   * @param table     table name
   * @param family    family name
   * @param qualifier qualifier name
   * @param action    one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserTable(User user, TableName table, byte[] family, byte[] qualifier,
    Permission.Action action) {
    Boolean fixed = fixedSystemTableRule(user, table, action);
    if (fixed != null) {
      return fixed;
    }
    if (user == null) {
      return false;
    }
    if (table == null) {
      table = PermissionStorage.ACL_TABLE_NAME;
    }
    if (authorizeUserNamespace(user, table.getNamespaceAsString(), action)) {
      return true;
    }
    PermissionCache<TablePermission> tblPermissions =
      tableCache.getOrDefault(table, TBL_NO_PERMISSION);
    if (authorizeTable(tblPermissions.get(user.getShortName()), table, family, qualifier, action)) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (
        authorizeTable(tblPermissions.get(AuthUtil.toGroupEntry(group)), table, family, qualifier,
          action)
      ) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeTable(Set<TablePermission> permissions, TableName table, byte[] family,
    byte[] qualifier, Permission.Action action) {
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
   * Check if user has given action privilige in table:family scope. This method is for backward
   * compatibility.
   * @param user   user name
   * @param table  table name
   * @param family family names
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeUserFamily(User user, TableName table, byte[] family,
    Permission.Action action) {
    Boolean fixed = fixedSystemTableRule(user, table, action);
    if (fixed != null) {
      return fixed;
    }
    PermissionCache<TablePermission> tblPermissions =
      tableCache.getOrDefault(table, TBL_NO_PERMISSION);
    if (authorizeFamily(tblPermissions.get(user.getShortName()), table, family, action)) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (
        authorizeFamily(tblPermissions.get(AuthUtil.toGroupEntry(group)), table, family, action)
      ) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeFamily(Set<TablePermission> permissions, TableName table, byte[] family,
    Permission.Action action) {
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
   * @param user   user name
   * @param table  table name
   * @param cell   cell to be checked
   * @param action one of action in [Read, Write, Create, Exec, Admin]
   * @return true if user has, false otherwise
   */
  public boolean authorizeCell(User user, TableName table, Cell cell, Permission.Action action) {
    // fixed rules for hbase:meta / hbase:acl take precedence over cell tags
    Boolean fixed = fixedSystemTableRule(user, table, action);
    if (fixed != null) {
      return fixed;
    }
    try {
      assert cell instanceof ExtendedCell;
      List<Permission> perms =
        PermissionStorage.getCellPermissionsForUser(user, (ExtendedCell) cell);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Perms for user {} in table {} in cell {}: {}", user.getShortName(), table, cell,
          (perms != null ? perms : ""));
      }
      if (perms != null) {
        for (Permission p : perms) {
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
   */
  public long getMTime() {
    return mtime.get();
  }
}
