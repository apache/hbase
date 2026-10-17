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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * The in-memory ACL snapshot: permission caches for the global, namespace and table scopes, fed
 * from {@code hbase:acl} either as a whole (activation, reload) or through entry level deltas (a
 * grant/revoke). One instance is owned per process side and driven by that side's ACL holder
 * ({@code ACLManager} on the master, {@code AclStore} on a region server); {@link AuthManager}
 * reads it through {@link AclView} to make authorization decisions.
 * <p>
 * Thread safety: applying updates is serialized by the owner, readers may run concurrently with
 * applies. Per scope caches are replaced wholesale (volatile references), and
 * {@link PermissionCache} guards its own map.
 * </p>
 */
@InterfaceAudience.Private
public class AclSnapshot implements AclView {

  /**
   * Cache of permissions for one scope, it is thread safe.
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
   * @param snapshot entry name to its permissions, see {@link PermissionStorage#loadAllSnapshot}
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
  }

  private void applyEntry(byte[] entry, ListMultimap<String, UserPermission> perms) {
    if (PermissionStorage.isGlobalEntry(entry)) {
      if (perms == null) {
        globalCache.clear();
      } else {
        updateGlobalCache(toPermissions(perms));
      }
    } else if (PermissionStorage.isNamespaceEntry(entry)) {
      String namespace = Bytes.toString(PermissionStorage.fromNamespaceEntry(entry));
      if (perms == null) {
        namespaceCache.remove(namespace);
      } else {
        updateNamespaceCache(namespace, toPermissions(perms));
      }
    } else {
      if (perms == null) {
        tableCache.remove(TableName.valueOf(entry));
      } else {
        updateTableCache(TableName.valueOf(entry), toPermissions(perms));
      }
    }
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

  @Override
  public GlobalPermission getGlobalPermission(String entryName) {
    return globalCache.get(entryName);
  }

  @Override
  public Set<NamespacePermission> getNamespacePermissions(String namespace, String entryName) {
    PermissionCache<NamespacePermission> cache = namespaceCache.get(namespace);
    Set<NamespacePermission> perms = cache != null ? cache.get(entryName) : null;
    return perms != null ? perms : Collections.emptySet();
  }

  @Override
  public Set<TablePermission> getTablePermissions(TableName table, String entryName) {
    PermissionCache<TablePermission> cache = tableCache.get(table);
    Set<TablePermission> perms = cache != null ? cache.get(entryName) : null;
    return perms != null ? perms : Collections.emptySet();
  }
}
