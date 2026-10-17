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
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * The granted permissions are read from the process side ACL snapshot through {@link AclView}; this
 * class only makes decisions and never mutates the snapshot.
 */
@InterfaceAudience.Private
public final class AuthManager {

  private static final Logger LOG = LoggerFactory.getLogger(AuthManager.class);

  private final AclView acls;

  AuthManager(AclView acls) {
    this.acls = acls;
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
    if (authorizeGlobal(acls.getGlobalPermission(user.getShortName()), action)) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (authorizeGlobal(acls.getGlobalPermission(AuthUtil.toGroupEntry(group)), action)) {
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
    if (
      authorizeNamespace(acls.getNamespacePermissions(namespace, user.getShortName()), namespace,
        action)
    ) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (
        authorizeNamespace(acls.getNamespacePermissions(namespace, AuthUtil.toGroupEntry(group)),
          namespace, action)
      ) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeNamespace(Set<NamespacePermission> permissions, String namespace,
    Permission.Action action) {
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
    if (hasAccessTable(acls.getTablePermissions(table, user.getShortName()), action)) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (hasAccessTable(acls.getTablePermissions(table, AuthUtil.toGroupEntry(group)), action)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasAccessTable(Set<TablePermission> permissions, Permission.Action action) {
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
    if (
      authorizeTable(acls.getTablePermissions(table, user.getShortName()), table, family, qualifier,
        action)
    ) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (
        authorizeTable(acls.getTablePermissions(table, AuthUtil.toGroupEntry(group)), table, family,
          qualifier, action)
      ) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeTable(Set<TablePermission> permissions, TableName table, byte[] family,
    byte[] qualifier, Permission.Action action) {
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
    if (
      authorizeFamily(acls.getTablePermissions(table, user.getShortName()), table, family, action)
    ) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (
        authorizeFamily(acls.getTablePermissions(table, AuthUtil.toGroupEntry(group)), table,
          family, action)
      ) {
        return true;
      }
    }
    return false;
  }

  private boolean authorizeFamily(Set<TablePermission> permissions, TableName table, byte[] family,
    Permission.Action action) {
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
}
