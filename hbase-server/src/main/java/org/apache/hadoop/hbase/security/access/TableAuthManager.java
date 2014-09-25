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
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

/**
 * Performs authorization checks for a given user's assigned permissions
 */
@InterfaceAudience.Private
public class TableAuthManager {
  private static class PermissionCache<T extends Permission> {
    /** Cache of user permissions */
    private ListMultimap<String,T> userCache = ArrayListMultimap.create();
    /** Cache of group permissions */
    private ListMultimap<String,T> groupCache = ArrayListMultimap.create();

    public List<T> getUser(String user) {
      return userCache.get(user);
    }

    public void putUser(String user, T perm) {
      userCache.put(user, perm);
    }

    public List<T> replaceUser(String user, Iterable<? extends T> perms) {
      return userCache.replaceValues(user, perms);
    }

    public List<T> getGroup(String group) {
      return groupCache.get(group);
    }

    public void putGroup(String group, T perm) {
      groupCache.put(group, perm);
    }

    public List<T> replaceGroup(String group, Iterable<? extends T> perms) {
      return groupCache.replaceValues(group, perms);
    }

    /**
     * Returns a combined map of user and group permissions, with group names prefixed by
     * {@link AccessControlLists#GROUP_PREFIX}.
     */
    public ListMultimap<String,T> getAllPermissions() {
      ListMultimap<String,T> tmp = ArrayListMultimap.create();
      tmp.putAll(userCache);
      for (String group : groupCache.keySet()) {
        tmp.putAll(AccessControlLists.GROUP_PREFIX + group, groupCache.get(group));
      }
      return tmp;
    }
  }

  private static Log LOG = LogFactory.getLog(TableAuthManager.class);

  private static TableAuthManager instance;

  /** Cache of global permissions */
  private volatile PermissionCache<Permission> globalCache;

  private ConcurrentSkipListMap<TableName, PermissionCache<TablePermission>> tableCache =
      new ConcurrentSkipListMap<TableName, PermissionCache<TablePermission>>();

  private ConcurrentSkipListMap<String, PermissionCache<TablePermission>> nsCache =
    new ConcurrentSkipListMap<String, PermissionCache<TablePermission>>();

  private Configuration conf;
  private ZKPermissionWatcher zkperms;
  private volatile long mtime;

  private TableAuthManager(ZooKeeperWatcher watcher, Configuration conf)
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

  /**
   * Returns a new {@code PermissionCache} initialized with permission assignments
   * from the {@code hbase.superuser} configuration key.
   */
  private PermissionCache<Permission> initGlobal(Configuration conf) throws IOException {
    UserProvider userProvider = UserProvider.instantiate(conf);
    User user = userProvider.getCurrent();
    if (user == null) {
      throw new IOException("Unable to obtain the current user, " +
          "authorization checks for internal operations will not work correctly!");
    }
    PermissionCache<Permission> newCache = new PermissionCache<Permission>();
    String currentUser = user.getShortName();

    // the system user is always included
    List<String> superusers = Lists.asList(currentUser, conf.getStrings(
        AccessControlLists.SUPERUSER_CONF_KEY, new String[0]));
    if (superusers != null) {
      for (String name : superusers) {
        if (AccessControlLists.isGroupPrincipal(name)) {
          newCache.putGroup(AccessControlLists.getGroupName(name),
              new Permission(Permission.Action.values()));
        } else {
          newCache.putUser(name, new Permission(Permission.Action.values()));
        }
      }
    }
    return newCache;
  }

  public ZKPermissionWatcher getZKPermissionWatcher() {
    return this.zkperms;
  }

  public void refreshTableCacheFromWritable(TableName table,
                                       byte[] data) throws IOException {
    if (data != null && data.length > 0) {
      ListMultimap<String,TablePermission> perms;
      try {
        perms = AccessControlLists.readPermissions(data, conf);
      } catch (DeserializationException e) {
        throw new IOException(e);
      }

      if (perms != null) {
        if (Bytes.equals(table.getName(), AccessControlLists.ACL_GLOBAL_NAME)) {
          updateGlobalCache(perms);
        } else {
          updateTableCache(table, perms);
        }
      }
    } else {
      LOG.debug("Skipping permission cache refresh because writable data is empty");
    }
  }

  public void refreshNamespaceCacheFromWritable(String namespace, byte[] data) throws IOException {
    if (data != null && data.length > 0) {
      ListMultimap<String,TablePermission> perms;
      try {
        perms = AccessControlLists.readPermissions(data, conf);
      } catch (DeserializationException e) {
        throw new IOException(e);
      }
      if (perms != null) {
        updateNsCache(namespace, perms);
      }
    } else {
      LOG.debug("Skipping permission cache refresh because writable data is empty");
    }
  }

  /**
   * Updates the internal global permissions cache
   *
   * @param userPerms
   */
  private void updateGlobalCache(ListMultimap<String,TablePermission> userPerms) {
    PermissionCache<Permission> newCache = null;
    try {
      newCache = initGlobal(conf);
      for (Map.Entry<String,TablePermission> entry : userPerms.entries()) {
        if (AccessControlLists.isGroupPrincipal(entry.getKey())) {
          newCache.putGroup(AccessControlLists.getGroupName(entry.getKey()),
              new Permission(entry.getValue().getActions()));
        } else {
          newCache.putUser(entry.getKey(), new Permission(entry.getValue().getActions()));
        }
      }
      globalCache = newCache;
      mtime++;
    } catch (IOException e) {
      // Never happens
      LOG.error("Error occured while updating the global cache", e);
    }
  }

  /**
   * Updates the internal permissions cache for a single table, splitting
   * the permissions listed into separate caches for users and groups to optimize
   * group lookups.
   * 
   * @param table
   * @param tablePerms
   */
  private void updateTableCache(TableName table,
                                ListMultimap<String,TablePermission> tablePerms) {
    PermissionCache<TablePermission> newTablePerms = new PermissionCache<TablePermission>();

    for (Map.Entry<String,TablePermission> entry : tablePerms.entries()) {
      if (AccessControlLists.isGroupPrincipal(entry.getKey())) {
        newTablePerms.putGroup(AccessControlLists.getGroupName(entry.getKey()), entry.getValue());
      } else {
        newTablePerms.putUser(entry.getKey(), entry.getValue());
      }
    }

    tableCache.put(table, newTablePerms);
    mtime++;
  }

  /**
   * Updates the internal permissions cache for a single table, splitting
   * the permissions listed into separate caches for users and groups to optimize
   * group lookups.
   *
   * @param namespace
   * @param tablePerms
   */
  private void updateNsCache(String namespace,
                             ListMultimap<String, TablePermission> tablePerms) {
    PermissionCache<TablePermission> newTablePerms = new PermissionCache<TablePermission>();

    for (Map.Entry<String, TablePermission> entry : tablePerms.entries()) {
      if (AccessControlLists.isGroupPrincipal(entry.getKey())) {
        newTablePerms.putGroup(AccessControlLists.getGroupName(entry.getKey()), entry.getValue());
      } else {
        newTablePerms.putUser(entry.getKey(), entry.getValue());
      }
    }

    nsCache.put(namespace, newTablePerms);
    mtime++;
  }

  private PermissionCache<TablePermission> getTablePermissions(TableName table) {
    if (!tableCache.containsKey(table)) {
      tableCache.putIfAbsent(table, new PermissionCache<TablePermission>());
    }
    return tableCache.get(table);
  }

  private PermissionCache<TablePermission> getNamespacePermissions(String namespace) {
    if (!nsCache.containsKey(namespace)) {
      nsCache.putIfAbsent(namespace, new PermissionCache<TablePermission>());
    }
    return nsCache.get(namespace);
  }

  /**
   * Authorizes a global permission
   * @param perms
   * @param action
   * @return true if authorized, false otherwise
   */
  private boolean authorize(List<Permission> perms, Permission.Action action) {
    if (perms != null) {
      for (Permission p : perms) {
        if (p.implies(action)) {
          return true;
        }
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("No permissions found");
    }

    return false;
  }

  /**
   * Authorize a global permission based on ACLs for the given user and the
   * user's groups.
   * @param user
   * @param action
   * @return true if known and authorized, false otherwise
   */
  public boolean authorize(User user, Permission.Action action) {
    if (user == null) {
      return false;
    }

    if (authorize(globalCache.getUser(user.getShortName()), action)) {
      return true;
    }

    String[] groups = user.getGroupNames();
    if (groups != null) {
      for (String group : groups) {
        if (authorize(globalCache.getGroup(group), action)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean authorize(List<TablePermission> perms,
                            TableName table, byte[] family,
                            Permission.Action action) {
    return authorize(perms, table, family, null, action);
  }

  private boolean authorize(List<TablePermission> perms,
                            TableName table, byte[] family,
                            byte[] qualifier, Permission.Action action) {
    if (perms != null) {
      for (TablePermission p : perms) {
        if (p.implies(table, family, qualifier, action)) {
          return true;
        }
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("No permissions found for table="+table);
    }
    return false;
  }

  /**
   * Authorize a user for a given KV. This is called from AccessControlFilter.
   */
  public boolean authorize(User user, TableName table, Cell cell, Permission.Action action) {
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

  public boolean authorize(User user, String namespace, Permission.Action action) {
    // Global authorizations supercede namespace level
    if (authorizeUser(user, action)) {
      return true;
    }
    // Check namespace permissions
    PermissionCache<TablePermission> tablePerms = nsCache.get(namespace);
    if (tablePerms != null) {
      List<TablePermission> userPerms = tablePerms.getUser(user.getShortName());
      if (authorize(userPerms, namespace, action)) {
        return true;
      }
      String[] groupNames = user.getGroupNames();
      if (groupNames != null) {
        for (String group : groupNames) {
          List<TablePermission> groupPerms = tablePerms.getGroup(group);
          if (authorize(groupPerms, namespace, action)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private boolean authorize(List<TablePermission> perms, String namespace,
                            Permission.Action action) {
    if (perms != null) {
      for (TablePermission p : perms) {
        if (p.implies(namespace, action)) {
          return true;
        }
      }
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("No permissions for authorize() check, table=" + namespace);
    }

    return false;
  }

  /**
   * Checks global authorization for a specific action for a user, based on the
   * stored user permissions.
   */
  public boolean authorizeUser(User user, Permission.Action action) {
    return authorize(globalCache.getUser(user.getShortName()), action);
  }

  /**
   * Checks authorization to a given table and column family for a user, based on the
   * stored user permissions.
   *
   * @param user
   * @param table
   * @param family
   * @param action
   * @return true if known and authorized, false otherwise
   */
  public boolean authorizeUser(User user, TableName table, byte[] family,
      Permission.Action action) {
    return authorizeUser(user, table, family, null, action);
  }

  public boolean authorizeUser(User user, TableName table, byte[] family,
      byte[] qualifier, Permission.Action action) {
    if (table == null) table = AccessControlLists.ACL_TABLE_NAME;
    // Global and namespace authorizations supercede table level
    if (authorize(user, table.getNamespaceAsString(), action)) {    
      return true;
    }
    // Check table permissions
    return authorize(getTablePermissions(table).getUser(user.getShortName()), table, family,
        qualifier, action);
  }

  /**
   * Checks global authorization for a given action for a group, based on the stored
   * permissions.
   */
  public boolean authorizeGroup(String groupName, Permission.Action action) {
    return authorize(globalCache.getGroup(groupName), action);
  }

  /**
   * Checks authorization to a given table and column family for a group, based
   * on the stored permissions. 
   * @param groupName
   * @param table
   * @param family
   * @param action
   * @return true if known and authorized, false otherwise
   */
  public boolean authorizeGroup(String groupName, TableName table, byte[] family,
      Permission.Action action) {
    // Global authorization supercedes table level
    if (authorizeGroup(groupName, action)) {
      return true;
    }
    if (table == null) table = AccessControlLists.ACL_TABLE_NAME;
    // Namespace authorization supercedes table level
    if (authorize(getNamespacePermissions(table.getNamespaceAsString()).getGroup(groupName),
        table, family, action)) {
      return true;
    }
    // Check table level
    return authorize(getTablePermissions(table).getGroup(groupName), table, family, action);
  }

  public boolean authorize(User user, TableName table, byte[] family,
      byte[] qualifier, Permission.Action action) {
    if (authorizeUser(user, table, family, qualifier, action)) {
      return true;
    }

    String[] groups = user.getGroupNames();
    if (groups != null) {
      for (String group : groups) {
        if (authorizeGroup(group, table, family, action)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean authorize(User user, TableName table, byte[] family,
      Permission.Action action) {
    return authorize(user, table, family, null, action);
  }

  /**
   * Returns true if the given user has a {@link TablePermission} matching up
   * to the column family portion of a permission.  Note that this permission
   * may be scoped to a given column qualifier and does not guarantee that
   * authorize() on the same column family would return true.
   */
  public boolean matchPermission(User user,
      TableName table, byte[] family, Permission.Action action) {
    PermissionCache<TablePermission> tablePerms = tableCache.get(table);
    if (tablePerms != null) {
      List<TablePermission> userPerms = tablePerms.getUser(user.getShortName());
      if (userPerms != null) {
        for (TablePermission p : userPerms) {
          if (p.matchesFamily(table, family, action)) {
            return true;
          }
        }
      }

      String[] groups = user.getGroupNames();
      if (groups != null) {
        for (String group : groups) {
          List<TablePermission> groupPerms = tablePerms.getGroup(group);
          if (groupPerms != null) {
            for (TablePermission p : groupPerms) {
              if (p.matchesFamily(table, family, action)) {
                return true;
              }
            }
          }
        }
      }
    }

    return false;
  }

  public boolean matchPermission(User user,
      TableName table, byte[] family, byte[] qualifier,
      Permission.Action action) {
    PermissionCache<TablePermission> tablePerms = tableCache.get(table);
    if (tablePerms != null) {
      List<TablePermission> userPerms = tablePerms.getUser(user.getShortName());
      if (userPerms != null) {
        for (TablePermission p : userPerms) {
          if (p.matchesFamilyQualifier(table, family, qualifier, action)) {
            return true;
          }
        }
      }

      String[] groups = user.getGroupNames();
      if (groups != null) {
        for (String group : groups) {
          List<TablePermission> groupPerms = tablePerms.getGroup(group);
          if (groupPerms != null) {
            for (TablePermission p : groupPerms) {
              if (p.matchesFamilyQualifier(table, family, qualifier, action)) {
                return true;
              }
            }
          }
        }
      }
    }
    return false;
  }

  public void removeNamespace(byte[] ns) {
    nsCache.remove(Bytes.toString(ns));
  }

  public void removeTable(TableName table) {
    tableCache.remove(table);
  }

  /**
   * Overwrites the existing permission set for a given user for a table, and
   * triggers an update for zookeeper synchronization.
   * @param username
   * @param table
   * @param perms
   */
  public void setTableUserPermissions(String username, TableName table,
      List<TablePermission> perms) {
    PermissionCache<TablePermission> tablePerms = getTablePermissions(table);
    tablePerms.replaceUser(username, perms);
    writeTableToZooKeeper(table, tablePerms);
  }

  /**
   * Overwrites the existing permission set for a group and triggers an update
   * for zookeeper synchronization.
   * @param group
   * @param table
   * @param perms
   */
  public void setTableGroupPermissions(String group, TableName table,
      List<TablePermission> perms) {
    PermissionCache<TablePermission> tablePerms = getTablePermissions(table);
    tablePerms.replaceGroup(group, perms);
    writeTableToZooKeeper(table, tablePerms);
  }

  /**
   * Overwrites the existing permission set for a given user for a table, and
   * triggers an update for zookeeper synchronization.
   * @param username
   * @param namespace
   * @param perms
   */
  public void setNamespaceUserPermissions(String username, String namespace,
      List<TablePermission> perms) {
    PermissionCache<TablePermission> tablePerms = getNamespacePermissions(namespace);
    tablePerms.replaceUser(username, perms);
    writeNamespaceToZooKeeper(namespace, tablePerms);
  }

  /**
   * Overwrites the existing permission set for a group and triggers an update
   * for zookeeper synchronization.
   * @param group
   * @param namespace
   * @param perms
   */
  public void setNamespaceGroupPermissions(String group, String namespace,
      List<TablePermission> perms) {
    PermissionCache<TablePermission> tablePerms = getNamespacePermissions(namespace);
    tablePerms.replaceGroup(group, perms);
    writeNamespaceToZooKeeper(namespace, tablePerms);
  }

  public void writeTableToZooKeeper(TableName table,
      PermissionCache<TablePermission> tablePerms) {
    byte[] serialized = new byte[0];
    if (tablePerms != null) {
      serialized = AccessControlLists.writePermissionsAsBytes(tablePerms.getAllPermissions(), conf);
    }
    zkperms.writeToZookeeper(table.getName(), serialized);
  }

  public void writeNamespaceToZooKeeper(String namespace,
      PermissionCache<TablePermission> tablePerms) {
    byte[] serialized = new byte[0];
    if (tablePerms != null) {
      serialized = AccessControlLists.writePermissionsAsBytes(tablePerms.getAllPermissions(), conf);
    }
    zkperms.writeToZookeeper(Bytes.toBytes(AccessControlLists.toNamespaceEntry(namespace)),
        serialized);
  }

  public long getMTime() {
    return mtime;
  }

  static Map<ZooKeeperWatcher,TableAuthManager> managerMap =
    new HashMap<ZooKeeperWatcher,TableAuthManager>();

  public synchronized static TableAuthManager get(
      ZooKeeperWatcher watcher, Configuration conf) throws IOException {
    instance = managerMap.get(watcher);
    if (instance == null) {
      instance = new TableAuthManager(watcher, conf);
      managerMap.put(watcher, instance);
    }
    return instance;
  }
}
