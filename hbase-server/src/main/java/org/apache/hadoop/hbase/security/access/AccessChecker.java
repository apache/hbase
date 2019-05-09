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
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public final class AccessChecker {
  private static final Logger AUDITLOG =
      LoggerFactory.getLogger("SecurityLogger." + AccessChecker.class.getName());
  private TableAuthManager authManager;
  /**
   * if we are active, usually false, only true if "hbase.security.authorization"
   * has been set to true in site configuration.see HBASE-19483.
   */
  private boolean authorizationEnabled;

  public static boolean isAuthorizationSupported(Configuration conf) {
    return conf.getBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, false);
  }

  /**
   * Constructor with existing configuration
   *
   * @param conf Existing configuration to use
   * @param zkw reference to the {@link ZooKeeperWatcher}
   */
  public AccessChecker(final Configuration conf, final ZooKeeperWatcher zkw)
      throws RuntimeException {
    // If zk is null or IOException while obtaining auth manager,
    // throw RuntimeException so that the coprocessor is unloaded.
    if (zkw != null) {
      try {
        this.authManager = TableAuthManager.getOrCreate(zkw, conf);
      } catch (IOException ioe) {
        throw new RuntimeException("Error obtaining AccessChecker", ioe);
      }
    } else {
      throw new NullPointerException("Error obtaining AccessChecker, zk found null.");
    }
    authorizationEnabled = isAuthorizationSupported(conf);
  }

  public TableAuthManager getAuthManager() {
    return authManager;
  }

  public void logResult(AuthResult result) {
    if (AUDITLOG.isTraceEnabled()) {
      InetAddress remoteAddr = RpcServer.getRemoteAddress();
      AUDITLOG.trace("Access " + (result.isAllowed() ? "allowed" : "denied") +
          " for user " + (result.getUser() != null ? result.getUser().getShortName() : "UNKNOWN") +
          "; reason: " + result.getReason() +
          "; remote address: " + (remoteAddr != null ? remoteAddr : "") +
          "; request: " + result.getRequest() +
          "; context: " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has any of the given permissions for the
   * given table, column family and column qualifier.
   * @param tableName Table requested
   * @param family Column family requested
   * @param qualifier Column qualifier requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if user has no authorization
   */
  public void requirePermission(User user, String request, TableName tableName, byte[] family,
      byte[] qualifier, Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorize(user, tableName, family, qualifier, permission)) {
        result = AuthResult.allow(request, "Table permission granted", user,
            permission, tableName, family, qualifier);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
            permission, tableName, family, qualifier);
      }
    }
    logResult(result);
    if (authorizationEnabled && !result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has any of the given permissions for the
   * given table, column family and column qualifier.
   * @param tableName Table requested
   * @param family Column family param
   * @param qualifier Column qualifier param
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if user has no authorization
   */
  public void requireTablePermission(User user, String request, TableName tableName, byte[] family,
      byte[] qualifier, Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorize(user, tableName, null, null, permission)) {
        result = AuthResult.allow(request, "Table permission granted", user,
            permission, tableName, null, null);
        result.getParams().setFamily(family).setQualifier(qualifier);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
            permission, tableName, family, qualifier);
        result.getParams().setFamily(family).setQualifier(qualifier);
      }
    }
    logResult(result);
    if (authorizationEnabled && !result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has any of the given permissions to access the table.
   *
   * @param tableName Table requested
   * @param permissions Actions being requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if user has no authorization
   */
  public void requireAccess(User user, String request, TableName tableName,
      Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.hasAccess(user, tableName, permission)) {
        result = AuthResult.allow(request, "Table permission granted", user,
            permission, tableName, null, null);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
            permission, tableName, null, null);
      }
    }
    logResult(result);
    if (authorizationEnabled && !result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions " + result.toContextString());
    }
  }

  /**
   * Authorizes that the current user has global privileges for the given action.
   * @param perm The action being requested
   * @throws IOException if obtaining the current user fails
   * @throws AccessDeniedException if authorization is denied
   */
  public void requirePermission(User user, String request, Action perm) throws IOException {
    requireGlobalPermission(user, request, perm, null, null);
  }

  /**
   * Checks that the user has the given global permission. The generated
   * audit log message will contain context information for the operation
   * being authorized, based on the given parameters.
   * @param perm Action being requested
   * @param tableName Affected table name.
   * @param familyMap Affected column families.
   */
  public void requireGlobalPermission(User user, String request, Action perm, TableName tableName,
      Map<byte[], ? extends Collection<byte[]>> familyMap) throws IOException {
    AuthResult result = null;
    if (authManager.authorize(user, perm)) {
      result = AuthResult.allow(request, "Global check allowed", user, perm, tableName, familyMap);
      result.getParams().setTableName(tableName).setFamilies(familyMap);
      logResult(result);
    } else {
      result = AuthResult.deny(request, "Global check failed", user, perm, tableName, familyMap);
      result.getParams().setTableName(tableName).setFamilies(familyMap);
      logResult(result);
      if (authorizationEnabled) {
        throw new AccessDeniedException("Insufficient permissions for user '" +
            (user != null ? user.getShortName() : "null") +"' (global, action=" +
            perm.toString() + ")");
      }
    }
  }

  /**
   * Checks that the user has the given global permission. The generated
   * audit log message will contain context information for the operation
   * being authorized, based on the given parameters.
   * @param perm Action being requested
   * @param namespace  The given namespace
   */
  public void requireGlobalPermission(User user, String request, Action perm,
      String namespace) throws IOException {
    AuthResult authResult = null;
    if (authManager.authorize(user, perm)) {
      authResult = AuthResult.allow(request, "Global check allowed", user, perm, null);
      authResult.getParams().setNamespace(namespace);
      logResult(authResult);
    } else {
      authResult = AuthResult.deny(request, "Global check failed", user, perm, null);
      authResult.getParams().setNamespace(namespace);
      logResult(authResult);
      if (authorizationEnabled) {
        throw new AccessDeniedException("Insufficient permissions for user '" +
            (user != null ? user.getShortName() : "null") +"' (global, action=" +
            perm.toString() + ")");
      }
    }
  }

  /**
   * Checks that the user has the given global or namespace permission.
   * @param namespace  The given namespace
   * @param permissions Actions being requested
   */
  public void requireNamespacePermission(User user, String request, String namespace,
      Action... permissions) throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorize(user, namespace, permission)) {
        result = AuthResult.allow(request, "Namespace permission granted",
            user, permission, namespace);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
            permission, namespace);
      }
    }
    logResult(result);
    if (authorizationEnabled && !result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions "
          + result.toContextString());
    }
  }

  /**
   * Checks that the user has the given global or namespace permission.
   * @param namespace   The given namespace
   * @param permissions Actions being requested
   */
  public void requireNamespacePermission(User user, String request, String namespace, TableName tableName,
      Map<byte[], ? extends Collection<byte[]>> familyMap, Action... permissions)
      throws IOException {
    AuthResult result = null;

    for (Action permission : permissions) {
      if (authManager.authorize(user, namespace, permission)) {
        result = AuthResult.allow(request, "Namespace permission granted",
            user, permission, namespace);
        result.getParams().setTableName(tableName).setFamilies(familyMap);
        break;
      } else {
        // rest of the world
        result = AuthResult.deny(request, "Insufficient permissions", user,
            permission, namespace);
        result.getParams().setTableName(tableName).setFamilies(familyMap);
      }
    }
    logResult(result);
    if (authorizationEnabled && !result.isAllowed()) {
      throw new AccessDeniedException("Insufficient permissions "
          + result.toContextString());
    }
  }
}