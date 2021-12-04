/*
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

import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class AccessChecker {
  private static final Logger LOG = LoggerFactory.getLogger(AccessChecker.class);
  private static final Logger AUDITLOG =
      LoggerFactory.getLogger("SecurityLogger." + AccessChecker.class.getName());
  private final AuthManager authManager;

  /** Group service to retrieve the user group information */
  private static Groups groupService;

  public static boolean isAuthorizationSupported(Configuration conf) {
    return conf.getBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, false);
  }

  /**
   * Constructor with existing configuration
   *
   * @param conf Existing configuration to use
   */
  public AccessChecker(final Configuration conf) {
    this.authManager = new AuthManager(conf);
    initGroupService(conf);
  }

  public AuthManager getAuthManager() {
    return authManager;
  }

  /**
   * Authorizes that the current user has any of the given permissions to access the table.
   *
   * @param user Active user to which authorization checks should be applied
   * @param request Request type.
   * @param tableName   Table requested
   * @param permissions Actions being requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if user has no authorization
   */
  public void requireAccess(User user, String request, TableName tableName,
      Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.accessUserTable(user, tableName, permission)) {
        result = AuthResult.allow(request, "Table permission granted",
            user, permission, tableName, null, null);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions",
            user, permission, tableName, null, null);
      }
    }
    logResult(result);
    if (!result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has global privileges for the given action.
   * @param user Active user to which authorization checks should be applied
   * @param request Request type
   * @param filterUser User name to be filtered from permission as requested
   * @param perm The action being requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if authorization is denied
   */
  public void requirePermission(User user, String request, String filterUser, Action perm)
      throws IOException {
    requireGlobalPermission(user, request, perm, null, null, filterUser);
  }

  /**
   * Checks that the user has the given global permission. The generated
   * audit log message will contain context information for the operation
   * being authorized, based on the given parameters.
   *
   * @param user Active user to which authorization checks should be applied
   * @param request Request type
   * @param perm      Action being requested
   * @param tableName Affected table name.
   * @param familyMap Affected column families.
   * @param filterUser User name to be filtered from permission as requested
   */
  public void requireGlobalPermission(User user, String request,
      Action perm, TableName tableName,
      Map<byte[], ? extends Collection<byte[]>> familyMap, String filterUser) throws IOException {
    AuthResult result;
    if (authManager.authorizeUserGlobal(user, perm)) {
      result = AuthResult.allow(request, "Global check allowed", user, perm, tableName, familyMap);
    } else {
      result = AuthResult.deny(request, "Global check failed", user, perm, tableName, familyMap);
    }
    result.getParams().setTableName(tableName).setFamilies(familyMap);
    result.getParams().addExtraParam("filterUser", filterUser);
    logResult(result);
    if (!result.isAllowed()) {
      throw new AccessDeniedException(
          "Insufficient permissions for user '" + (user != null ? user.getShortName() : "null")
              + "' (global, action=" + perm.toString() + ")");
    }
  }

  /**
   * Checks that the user has the given global permission. The generated
   * audit log message will contain context information for the operation
   * being authorized, based on the given parameters.
   *
   * @param user Active user to which authorization checks should be applied
   * @param request Request type
   * @param perm      Action being requested
   * @param namespace The given namespace
   */
  public void requireGlobalPermission(User user, String request, Action perm,
      String namespace) throws IOException {
    AuthResult authResult;
    if (authManager.authorizeUserGlobal(user, perm)) {
      authResult = AuthResult.allow(request, "Global check allowed", user, perm, null);
      authResult.getParams().setNamespace(namespace);
      logResult(authResult);
    } else {
      authResult = AuthResult.deny(request, "Global check failed", user, perm, null);
      authResult.getParams().setNamespace(namespace);
      logResult(authResult);
      throw new AccessDeniedException(
          "Insufficient permissions for user '" + (user != null ? user.getShortName() : "null")
              + "' (global, action=" + perm.toString() + ")");
    }
  }

  /**
   * Checks that the user has the given global or namespace permission.
   * @param user Active user to which authorization checks should be applied
   * @param request Request type
   * @param namespace Name space as requested
   * @param filterUser User name to be filtered from permission as requested
   * @param permissions Actions being requested
   */
  public void requireNamespacePermission(User user, String request, String namespace,
      String filterUser, Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorizeUserNamespace(user, namespace, permission)) {
        result =
            AuthResult.allow(request, "Namespace permission granted", user, permission, namespace);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user, permission, namespace);
      }
    }
    result.getParams().addExtraParam("filterUser", filterUser);
    logResult(result);
    if (!result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Checks that the user has the given global or namespace permission.
   *
   * @param user Active user to which authorization checks should be applied
   * @param request Request type
   * @param namespace  The given namespace
   * @param tableName Table requested
   * @param familyMap    Column family map requested
   * @param permissions Actions being requested
   */
  public void requireNamespacePermission(User user, String request, String namespace,
      TableName tableName, Map<byte[], ? extends Collection<byte[]>> familyMap,
      Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorizeUserNamespace(user, namespace, permission)) {
        result =
            AuthResult.allow(request, "Namespace permission granted", user, permission, namespace);
        result.getParams().setTableName(tableName).setFamilies(familyMap);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user, permission, namespace);
        result.getParams().setTableName(tableName).setFamilies(familyMap);
      }
    }
    logResult(result);
    if (!result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has any of the given permissions for the
   * given table, column family and column qualifier.
   *
   * @param user Active user to which authorization checks should be applied
   * @param request Request type
   * @param tableName Table requested
   * @param family    Column family requested
   * @param qualifier Column qualifier requested
   * @param filterUser User name to be filtered from permission as requested
   * @param permissions Actions being requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if user has no authorization
   */
  public void requirePermission(User user, String request, TableName tableName, byte[] family,
      byte[] qualifier, String filterUser, Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorizeUserTable(user, tableName, family, qualifier, permission)) {
        result = AuthResult.allow(request, "Table permission granted",
            user, permission, tableName, family, qualifier);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions",
          user, permission, tableName, family, qualifier);
      }
    }
    result.getParams().addExtraParam("filterUser", filterUser);
    logResult(result);
    if (!result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has any of the given permissions for the
   * given table, column family and column qualifier.
   *
   * @param user Active user to which authorization checks should be applied
   * @param request Request type
   * @param tableName Table requested
   * @param family    Column family param
   * @param qualifier Column qualifier param
   * @throws IOException           if obtaining the current user fails
   * @throws AccessDeniedException if user has no authorization
   */
  public void requireTablePermission(User user, String request,
      TableName tableName,byte[] family, byte[] qualifier,
      Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorizeUserTable(user, tableName, permission)) {
        result = AuthResult.allow(request, "Table permission granted",
            user, permission, tableName, null, null);
        result.getParams().setFamily(family).setQualifier(qualifier);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions",
                user, permission, tableName, family, qualifier);
        result.getParams().setFamily(family).setQualifier(qualifier);
      }
    }
    logResult(result);
    if (!result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Check if caller is granting or revoking superusers's or supergroups's permissions.
   * @param request request name
   * @param caller caller
   * @param userToBeChecked target user or group
   * @throws IOException AccessDeniedException if target user is superuser
   */
  public void performOnSuperuser(String request, User caller, String userToBeChecked)
      throws IOException {
    List<String> userGroups = new ArrayList<>();
    userGroups.add(userToBeChecked);
    if (!AuthUtil.isGroupPrincipal(userToBeChecked)) {
      for (String group : getUserGroups(userToBeChecked)) {
        userGroups.add(AuthUtil.toGroupEntry(group));
      }
    }
    for (String name : userGroups) {
      if (Superusers.isSuperUser(name)) {
        AuthResult result = AuthResult.deny(
          request,
          "Granting or revoking superusers's or supergroups's permissions is not allowed",
          caller,
          Action.ADMIN,
          NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
        logResult(result);
        throw new AccessDeniedException(result.getReason());
      }
    }
  }

  public void checkLockPermissions(User user, String namespace,
      TableName tableName, RegionInfo[] regionInfos, String reason)
      throws IOException {
    if (namespace != null && !namespace.isEmpty()) {
      requireNamespacePermission(user, reason, namespace, null, Action.ADMIN, Action.CREATE);
    } else if (tableName != null || (regionInfos != null && regionInfos.length > 0)) {
      // So, either a table or regions op. If latter, check perms ons table.
      TableName tn = tableName != null? tableName: regionInfos[0].getTable();
      requireTablePermission(user, reason, tn, null, null,
          Action.ADMIN, Action.CREATE);
    } else {
      throw new DoNotRetryIOException("Invalid lock level when requesting permissions.");
    }
  }

  public static void logResult(AuthResult result) {
    if (AUDITLOG.isTraceEnabled()) {
      User user = result.getUser();
      UserGroupInformation ugi = user != null ? user.getUGI() : null;
      AUDITLOG.trace(
        "Access {} for user {}; reason: {}; remote address: {}; request: {}; context: {};" +
          "auth method: {}",
        (result.isAllowed() ? "allowed" : "denied"),
        (user != null ? user.getShortName() : "UNKNOWN"),
        result.getReason(), RpcServer.getRemoteAddress().map(InetAddress::toString).orElse(""),
        result.getRequest(), result.toContextString(),
        ugi != null ? ugi.getAuthenticationMethod() : "UNKNOWN");
    }
  }

  /*
   * Validate the hasPermission operation caller with the filter user. Self check doesn't require
   * any privilege but for others caller must have ADMIN privilege.
   */
  public User validateCallerWithFilterUser(User caller, TablePermission tPerm, String inputUserName)
      throws IOException {
    User filterUser = null;
    if (!caller.getShortName().equals(inputUserName)) {
      // User should have admin privilege if checking permission for other users
      requirePermission(caller, "hasPermission", tPerm.getTableName(), tPerm.getFamily(),
        tPerm.getQualifier(), inputUserName, Action.ADMIN);
      // Initialize user instance for the input user name
      List<String> groups = getUserGroups(inputUserName);
      filterUser = new InputUser(inputUserName, groups.toArray(new String[groups.size()]));
    } else {
      // User don't need ADMIN privilege for self check.
      // Setting action as null in AuthResult to display empty action in audit log
      AuthResult result = AuthResult.allow("hasPermission", "Self user validation allowed", caller,
        null, tPerm.getTableName(), tPerm.getFamily(), tPerm.getQualifier());
      logResult(result);
      filterUser = caller;
    }
    return filterUser;
  }

  /**
   * A temporary user class to instantiate User instance based on the name and groups.
   */
  public static class InputUser extends User {
    private String name;
    private String shortName = null;
    private String[] groups;

    public InputUser(String name, String[] groups) {
      this.name = name;
      this.groups = groups;
    }

    @Override
    public String getShortName() {
      if (this.shortName == null) {
        try {
          this.shortName = new HadoopKerberosName(this.name).getShortName();
        } catch (IOException ioe) {
          throw new IllegalArgumentException(
              "Illegal principal name " + this.name + ": " + ioe.toString(), ioe);
        }
      }
      return shortName;
    }

    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public String[] getGroupNames() {
      return this.groups;
    }

    @Override
    public <T> T runAs(PrivilegedAction<T> action) {
      throw new UnsupportedOperationException(
          "Method not supported, this class has limited implementation");
    }

    @Override
    public <T> T runAs(PrivilegedExceptionAction<T> action)
        throws IOException, InterruptedException {
      throw new UnsupportedOperationException(
          "Method not supported, this class has limited implementation");
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  /*
   * Initialize the group service.
   */
  private void initGroupService(Configuration conf) {
    if (groupService == null) {
      if (conf.getBoolean(User.TestingGroups.TEST_CONF, false)) {
        UserProvider.setGroups(new User.TestingGroups(UserProvider.getGroups()));
        groupService = UserProvider.getGroups();
      } else {
        groupService = Groups.getUserToGroupsMappingService(conf);
      }
    }
  }

  /**
   * Retrieve the groups of the given user.
   * @param user User name
   * @return Groups
   */
  public static List<String> getUserGroups(String user) {
    try {
      return groupService.getGroups(user);
    } catch (IOException e) {
      LOG.error("Error occurred while retrieving group for " + user, e);
      return new ArrayList<>();
    }
  }

  /**
   * Authorizes that if the current user has the given permissions.
   * @param user Active user to which authorization checks should be applied
   * @param request Request type
   * @param permission Actions being requested
   * @return True if the user has the specific permission
   */
  public boolean hasUserPermission(User user, String request, Permission permission) {
    if (permission instanceof TablePermission) {
      TablePermission tPerm = (TablePermission) permission;
      for (Permission.Action action : permission.getActions()) {
        AuthResult authResult = permissionGranted(request, user, action, tPerm.getTableName(),
          tPerm.getFamily(), tPerm.getQualifier());
        AccessChecker.logResult(authResult);
        if (!authResult.isAllowed()) {
          return false;
        }
      }
    } else if (permission instanceof NamespacePermission) {
      NamespacePermission nsPerm = (NamespacePermission) permission;
      AuthResult authResult;
      for (Action action : nsPerm.getActions()) {
        if (getAuthManager().authorizeUserNamespace(user, nsPerm.getNamespace(), action)) {
          authResult =
              AuthResult.allow(request, "Namespace action allowed", user, action, null, null);
        } else {
          authResult =
              AuthResult.deny(request, "Namespace action denied", user, action, null, null);
        }
        AccessChecker.logResult(authResult);
        if (!authResult.isAllowed()) {
          return false;
        }
      }
    } else {
      AuthResult authResult;
      for (Permission.Action action : permission.getActions()) {
        if (getAuthManager().authorizeUserGlobal(user, action)) {
          authResult = AuthResult.allow(request, "Global action allowed", user, action, null, null);
        } else {
          authResult = AuthResult.deny(request, "Global action denied", user, action, null, null);
        }
        AccessChecker.logResult(authResult);
        if (!authResult.isAllowed()) {
          return false;
        }
      }
    }
    return true;
  }

  private AuthResult permissionGranted(String request, User user, Action permRequest,
      TableName tableName, byte[] family, byte[] qualifier) {
    Map<byte[], ? extends Collection<byte[]>> map = makeFamilyMap(family, qualifier);
    return permissionGranted(request, user, permRequest, tableName, map);
  }

  /**
   * Check the current user for authorization to perform a specific action against the given set of
   * row data.
   * <p>
   * Note: Ordering of the authorization checks has been carefully optimized to short-circuit the
   * most common requests and minimize the amount of processing required.
   * </p>
   * @param request User request
   * @param user User name
   * @param permRequest the action being requested
   * @param tableName Table name
   * @param families the map of column families to qualifiers present in the request
   * @return an authorization result
   */
  public AuthResult permissionGranted(String request, User user, Action permRequest,
      TableName tableName, Map<byte[], ? extends Collection<?>> families) {
    // 1. All users need read access to hbase:meta table.
    // this is a very common operation, so deal with it quickly.
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      if (permRequest == Action.READ) {
        return AuthResult.allow(request, "All users allowed", user, permRequest, tableName,
          families);
      }
    }

    if (user == null) {
      return AuthResult.deny(request, "No user associated with request!", null, permRequest,
        tableName, families);
    }

    // 2. check for the table-level, if successful we can short-circuit
    if (getAuthManager().authorizeUserTable(user, tableName, permRequest)) {
      return AuthResult.allow(request, "Table permission granted", user, permRequest, tableName,
        families);
    }

    // 3. check permissions against the requested families
    if (families != null && families.size() > 0) {
      // all families must pass
      for (Map.Entry<byte[], ? extends Collection<?>> family : families.entrySet()) {
        // a) check for family level access
        if (getAuthManager().authorizeUserTable(user, tableName, family.getKey(), permRequest)) {
          continue; // family-level permission overrides per-qualifier
        }

        // b) qualifier level access can still succeed
        if ((family.getValue() != null) && (family.getValue().size() > 0)) {
          if (family.getValue() instanceof Set) {
            // for each qualifier of the family
            Set<byte[]> familySet = (Set<byte[]>) family.getValue();
            for (byte[] qualifier : familySet) {
              if (!getAuthManager().authorizeUserTable(user, tableName, family.getKey(), qualifier,
                permRequest)) {
                return AuthResult.deny(request, "Failed qualifier check", user, permRequest,
                  tableName, makeFamilyMap(family.getKey(), qualifier));
              }
            }
          } else if (family.getValue() instanceof List) { // List<Cell>
            List<Cell> cellList = (List<Cell>) family.getValue();
            for (Cell cell : cellList) {
              if (!getAuthManager().authorizeUserTable(user, tableName, family.getKey(),
                CellUtil.cloneQualifier(cell), permRequest)) {
                return AuthResult.deny(request, "Failed qualifier check", user, permRequest,
                  tableName, makeFamilyMap(family.getKey(), CellUtil.cloneQualifier(cell)));
              }
            }
          }
        } else {
          // no qualifiers and family-level check already failed
          return AuthResult.deny(request, "Failed family check", user, permRequest, tableName,
            makeFamilyMap(family.getKey(), null));
        }
      }

      // all family checks passed
      return AuthResult.allow(request, "All family checks passed", user, permRequest, tableName,
        families);
    }

    // 4. no families to check and table level access failed
    return AuthResult.deny(request, "No families to check and table permission failed", user,
      permRequest, tableName, families);
  }

  private Map<byte[], ? extends Collection<byte[]>> makeFamilyMap(byte[] family, byte[] qualifier) {
    if (family == null) {
      return null;
    }

    Map<byte[], Collection<byte[]>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    familyMap.put(family, qualifier != null ? ImmutableSet.of(qualifier) : null);
    return familyMap;
  }
}
