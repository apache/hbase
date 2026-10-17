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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Master side facade over the {@code hbase:acl} table: performs permission updates and queries and
 * announces every update to the local cache ({@link AclRefresher}) and to the push pipeline
 * ({@link ACLManager}). All permission writes go through this class, so the announcing cannot be
 * forgotten by a write path; tests or tools that write the table directly bypass the pipeline and
 * are picked up by the periodic reload only.
 * <p>
 * Updates are stamped with a monotonically increasing timestamp assigned by {@link ACLManager} and
 * written into the cells, so the cell timestamps carry the write order.
 * </p>
 */
@InterfaceAudience.Private
public class AclTableManager {

  private static final Logger LOG = LoggerFactory.getLogger(AclTableManager.class);

  private final Configuration conf;
  private final Connection connection;
  private final ACLManager aclManager;
  private final AclRefresher refresher;
  private final AccessChecker accessChecker;

  public AclTableManager(Configuration conf, Connection connection, ACLManager aclManager,
    AclRefresher refresher, AccessChecker accessChecker) {
    this.conf = conf;
    this.connection = connection;
    this.aclManager = aclManager;
    this.refresher = refresher;
    this.accessChecker = accessChecker;
  }

  /** Grants the given permission, optionally merging it into the existing actions. */
  public void grant(UserPermission userPermission, boolean mergeExistingPermissions)
    throws IOException {
    byte[] entry = PermissionStorage.userPermissionRowKey(userPermission.getPermission());
    long ts = aclManager.nextTimestamp();
    try (Table t = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
      PermissionStorage.addUserPermission(conf, userPermission, t, mergeExistingPermissions, ts);
      announce(t, entry, ts);
    }
  }

  /** Revokes the given permission. */
  public void revoke(UserPermission userPermission) throws IOException {
    byte[] entry = PermissionStorage.userPermissionRowKey(userPermission.getPermission());
    long ts = aclManager.nextTimestamp();
    try (Table t = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
      PermissionStorage.removeUserPermission(conf, userPermission, t, ts);
      announce(t, entry, ts);
    }
  }

  /** Drops all permissions of a removed table. */
  public void removeTablePermissions(TableName tableName) throws IOException {
    long ts = aclManager.nextTimestamp();
    try (Table t = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
      PermissionStorage.removeTablePermissions(conf, tableName, t, ts);
      announce(t, tableName.getName(), ts);
    }
  }

  /** Drops all permissions of a removed namespace. */
  public void removeNamespacePermissions(String namespace) throws IOException {
    byte[] entry = Bytes.toBytes(PermissionStorage.toNamespaceEntry(namespace));
    long ts = aclManager.nextTimestamp();
    try (Table t = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
      PermissionStorage.removeNamespacePermissions(conf, namespace, t, ts);
      announce(t, entry, ts);
    }
  }

  public List<UserPermission> getUserTablePermissions(TableName table, byte[] cf, byte[] cq,
    String userName, boolean filter) throws IOException {
    return PermissionStorage.getUserTablePermissions(conf, table, cf, cq, userName, filter);
  }

  public List<UserPermission> getUserNamespacePermissions(String namespace, String userName,
    boolean filter) throws IOException {
    return PermissionStorage.getUserNamespacePermissions(conf, namespace, userName, filter);
  }

  public List<UserPermission> getUserPermissions(String userName, boolean filter)
    throws IOException {
    return PermissionStorage.getUserPermissions(conf, null, null, null, userName, filter);
  }

  /**
   * Checks the given permissions of the given user against the authorization cache.
   * @param caller   the request user
   * @param userName the user to check, when different from the caller the caller needs the
   *                 permission to check another user
   */
  public List<Boolean> hasUserPermissions(User caller, String userName,
    List<Permission> permissions) throws IOException {
    if (!caller.getShortName().equals(userName)) {
      List<String> groups = AccessChecker.getUserGroups(userName);
      caller = new AccessChecker.InputUser(userName, groups.toArray(new String[groups.size()]));
    }
    List<Boolean> results = new ArrayList<>(permissions.size());
    if (accessChecker != null) {
      for (Permission permission : permissions) {
        results.add(accessChecker.hasUserPermission(caller, "hasUserPermissions", permission));
      }
    } else {
      for (int i = 0; i < permissions.size(); i++) {
        results.add(true);
      }
    }
    return results;
  }

  /**
   * Applies the write to the local cache right away and queues it for broadcast, without waiting
   * for the broadcast.
   */
  private void announce(Table t, byte[] entry, long ts) throws IOException {
    ListMultimap<String, UserPermission> perms =
      PermissionStorage.getPermissions(conf, entry, t, null, null, null, false);
    AuthManager.EntryUpdate update = perms.isEmpty()
      ? AuthManager.EntryUpdate.deleted(entry)
      : AuthManager.EntryUpdate.replace(entry, perms);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Announcing acl entry {} update at ts {}, deleted={}", Bytes.toStringBinary(entry),
        ts, update.isDeleted());
    }
    refresher.submitUpdates(ts, Collections.singletonList(update));
    aclManager.onEntryUpdate(ts, update);
  }
}
