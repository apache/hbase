/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService.BlockingInterface;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility client for doing access control admin operations.
 */
@InterfaceAudience.Public
public class AccessControlClient {
  public static final TableName ACL_TABLE_NAME =
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "acl");

  /**
   * Return true if authorization is supported and enabled
   * @param connection The connection to use
   * @return true if authorization is supported and enabled, false otherwise
   * @throws IOException
   */
  public static boolean isAuthorizationEnabled(Connection connection) throws IOException {
    return connection.getAdmin().getSecurityCapabilities()
        .contains(SecurityCapability.AUTHORIZATION);
  }

  /**
   * Return true if cell authorization is supported and enabled
   * @param connection The connection to use
   * @return true if cell authorization is supported and enabled, false otherwise
   * @throws IOException
   */
  public static boolean isCellAuthorizationEnabled(Connection connection) throws IOException {
    return connection.getAdmin().getSecurityCapabilities()
        .contains(SecurityCapability.CELL_AUTHORIZATION);
  }

  private static BlockingInterface getAccessControlServiceStub(Table ht)
      throws IOException {
    CoprocessorRpcChannel service = ht.coprocessorService(HConstants.EMPTY_START_ROW);
    BlockingInterface protocol =
        AccessControlProtos.AccessControlService.newBlockingStub(service);
    return protocol;
  }

  /**
   * Grants permission on the specified table for the specified user
   * @param connection The Connection instance to use
   * @param tableName
   * @param userName
   * @param family
   * @param qual
   * @param mergeExistingPermissions If set to false, later granted permissions will override
   *          previous granted permissions. otherwise, it'll merge with previous granted
   *          permissions.
   * @param actions
   * @throws Throwable
   */
  private static void grant(Connection connection, final TableName tableName,
      final String userName, final byte[] family, final byte[] qual, boolean mergeExistingPermissions,
      final Permission.Action... actions) throws Throwable {
    // TODO: Priority is not used.
    try (Table table = connection.getTable(ACL_TABLE_NAME)) {
      AccessControlUtil.grant(null, getAccessControlServiceStub(table), userName, tableName,
        family, qual, mergeExistingPermissions, actions);
    }
  }

  /**
   * Grants permission on the specified table for the specified user.
   * If permissions for a specified user exists, later granted permissions will override previous granted permissions.
   * @param connection The Connection instance to use
   * @param tableName
   * @param userName
   * @param family
   * @param qual
   * @param actions
   * @throws Throwable
   */
  public static void grant(Connection connection, final TableName tableName, final String userName,
      final byte[] family, final byte[] qual, final Permission.Action... actions) throws Throwable {
    grant(connection, tableName, userName, family, qual, true, actions);
  }

  /**
   * Grants permission on the specified namespace for the specified user.
   * @param connection
   * @param namespace
   * @param userName
   * @param mergeExistingPermissions If set to false, later granted permissions will override
   *          previous granted permissions. otherwise, it'll merge with previous granted
   *          permissions.
   * @param actions
   * @throws Throwable
   */
  private static void grant(Connection connection, final String namespace, final String userName,
      boolean mergeExistingPermissions, final Permission.Action... actions) throws Throwable {
    // TODO: Pass an rpcController.
    try (Table table = connection.getTable(ACL_TABLE_NAME)) {
      AccessControlUtil.grant(null, getAccessControlServiceStub(table), userName, namespace,
        mergeExistingPermissions, actions);
    }
  }

  /**
   * Grants permission on the specified namespace for the specified user.
   * If permissions on the specified namespace exists, later granted permissions will override previous granted
   * permissions.
   * @param connection The Connection instance to use
   * @param namespace
   * @param userName
   * @param actions
   * @throws Throwable
   */
  public static void grant(Connection connection, final String namespace, final String userName,
      final Permission.Action... actions) throws Throwable {
    grant(connection, namespace, userName, true, actions);
  }

  /**
   * Grants permission on the specified namespace for the specified user.
   * @param connection
   * @param userName
   * @param mergeExistingPermissions If set to false, later granted permissions will override
   *          previous granted permissions. otherwise, it'll merge with previous granted
   *          permissions.
   * @param actions
   * @throws Throwable
   */
  private static void grant(Connection connection, final String userName,
      boolean mergeExistingPermissions, final Permission.Action... actions) throws Throwable {
    // TODO: Pass an rpcController
    try (Table table = connection.getTable(ACL_TABLE_NAME)) {
      AccessControlUtil.grant(null, getAccessControlServiceStub(table), userName,
              mergeExistingPermissions, actions);
    }
  }

  /**
   * Grant global permissions for the specified user.
   * If permissions for the specified user exists, later granted permissions will override previous granted
   * permissions.
   * @param connection
   * @param userName
   * @param actions
   * @throws Throwable
   */
  public static void grant(Connection connection, final String userName,
      final Permission.Action... actions) throws Throwable {
    grant(connection, userName, true, actions);
  }

  public static boolean isAccessControllerRunning(Connection connection)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    try (Admin admin = connection.getAdmin()) {
      return admin.isTableAvailable(ACL_TABLE_NAME);
    }
  }

  /**
   * Revokes the permission on the table
   * @param connection The Connection instance to use
   * @param tableName
   * @param username
   * @param family
   * @param qualifier
   * @param actions
   * @throws Throwable
   */
  public static void revoke(Connection connection, final TableName tableName,
      final String username, final byte[] family, final byte[] qualifier,
      final Permission.Action... actions) throws Throwable {
    /** TODO: Pass an rpcController
    HBaseRpcController controller
      = ((ClusterConnection) connection).getRpcControllerFactory().newController();
    controller.setPriority(tableName);
    */
    try (Table table = connection.getTable(ACL_TABLE_NAME)) {
      AccessControlUtil.revoke(null, getAccessControlServiceStub(table), username, tableName,
        family, qualifier, actions);
    }
  }

  /**
   * Revokes the permission on the table for the specified user.
   * @param connection The Connection instance to use
   * @param namespace
   * @param userName
   * @param actions
   * @throws Throwable
   */
  public static void revoke(Connection connection, final String namespace,
      final String userName, final Permission.Action... actions) throws Throwable {
    /** TODO: Pass an rpcController
    HBaseRpcController controller
      = ((ClusterConnection) connection).getRpcControllerFactory().newController();
      */
    try (Table table = connection.getTable(ACL_TABLE_NAME)) {
      AccessControlUtil.revoke(null, getAccessControlServiceStub(table), userName, namespace,
        actions);
    }
  }

  /**
   * Revoke global permissions for the specified user.
   * @param connection The Connection instance to use
   */
  public static void revoke(Connection connection, final String userName,
      final Permission.Action... actions) throws Throwable {
    /** TODO: Pass an rpc controller.
    HBaseRpcController controller
      = ((ClusterConnection) connection).getRpcControllerFactory().newController();
      */
    try (Table table = connection.getTable(ACL_TABLE_NAME)) {
      AccessControlUtil.revoke(null, getAccessControlServiceStub(table), userName, actions);
    }
  }

  /**
   * List all the userPermissions matching the given pattern. If pattern is null, the behavior is
   * dependent on whether user has global admin privileges or not. If yes, the global permissions
   * along with the list of superusers would be returned. Else, no rows get returned.
   * @param connection The Connection instance to use
   * @param tableRegex The regular expression string to match against
   * @return List of UserPermissions
   * @throws Throwable
   */
  public static List<UserPermission> getUserPermissions(Connection connection, String tableRegex)
      throws Throwable {
    return getUserPermissions(connection, tableRegex, HConstants.EMPTY_STRING);
  }

  /**
   * List all the userPermissions matching the given table pattern and user name.
   * @param connection Connection
   * @param tableRegex The regular expression string to match against
   * @param userName User name, if empty then all user permissions will be retrieved.
   * @return List of UserPermissions
   * @throws Throwable on failure
   */
  public static List<UserPermission> getUserPermissions(Connection connection, String tableRegex,
      String userName) throws Throwable {
    /**
     * TODO: Pass an rpcController HBaseRpcController controller = ((ClusterConnection)
     * connection).getRpcControllerFactory().newController();
     */
    List<UserPermission> permList = new ArrayList<>();
    try (Table table = connection.getTable(ACL_TABLE_NAME)) {
      try (Admin admin = connection.getAdmin()) {
        CoprocessorRpcChannel service = table.coprocessorService(HConstants.EMPTY_START_ROW);
        BlockingInterface protocol =
            AccessControlProtos.AccessControlService.newBlockingStub(service);
        HTableDescriptor[] htds = null;
        if (tableRegex == null || tableRegex.isEmpty()) {
          permList = AccessControlUtil.getUserPermissions(null, protocol, userName);
        } else if (tableRegex.charAt(0) == '@') { // Namespaces
          String namespaceRegex = tableRegex.substring(1);
          for (NamespaceDescriptor nsds : admin.listNamespaceDescriptors()) { // Read out all
                                                                              // namespaces
            String namespace = nsds.getName();
            if (namespace.matches(namespaceRegex)) { // Match the given namespace regex?
              permList.addAll(AccessControlUtil.getUserPermissions(null, protocol,
                Bytes.toBytes(namespace), userName));
            }
          }
        } else { // Tables
          htds = admin.listTables(Pattern.compile(tableRegex), true);
          for (HTableDescriptor htd : htds) {
            permList.addAll(AccessControlUtil.getUserPermissions(null, protocol, htd.getTableName(),
              null, null, userName));
          }
        }
      }
    }
    return permList;
  }

  /**
   * List all the userPermissions matching the given table pattern and column family.
   * @param connection Connection
   * @param tableRegex The regular expression string to match against. It shouldn't be null, empty
   *          or a namespace regular expression.
   * @param columnFamily Column family
   * @return List of UserPermissions
   * @throws Throwable on failure
   */
  public static List<UserPermission> getUserPermissions(Connection connection, String tableRegex,
      byte[] columnFamily) throws Throwable {
    return getUserPermissions(connection, tableRegex, columnFamily, null, HConstants.EMPTY_STRING);
  }

  /**
   * List all the userPermissions matching the given table pattern, column family and user name.
   * @param connection Connection
   * @param tableRegex The regular expression string to match against. It shouldn't be null, empty
   *          or a namespace regular expression.
   * @param columnFamily Column family
   * @param userName User name, if empty then all user permissions will be retrieved.
   * @return List of UserPermissions
   * @throws Throwable on failure
   */
  public static List<UserPermission> getUserPermissions(Connection connection, String tableRegex,
      byte[] columnFamily, String userName) throws Throwable {
    return getUserPermissions(connection, tableRegex, columnFamily, null, userName);
  }

  /**
   * List all the userPermissions matching the given table pattern, column family and column
   * qualifier.
   * @param connection Connection
   * @param tableRegex The regular expression string to match against. It shouldn't be null, empty
   *          or a namespace regular expression.
   * @param columnFamily Column family
   * @param columnQualifier Column qualifier
   * @return List of UserPermissions
   * @throws Throwable on failure
   */
  public static List<UserPermission> getUserPermissions(Connection connection, String tableRegex,
      byte[] columnFamily, byte[] columnQualifier) throws Throwable {
    return getUserPermissions(connection, tableRegex, columnFamily, columnQualifier,
      HConstants.EMPTY_STRING);
  }

  /**
   * List all the userPermissions matching the given table pattern, column family and column
   * qualifier.
   * @param connection Connection
   * @param tableRegex The regular expression string to match against. It shouldn't be null, empty
   *          or a namespace regular expression.
   * @param columnFamily Column family
   * @param columnQualifier Column qualifier
   * @param userName User name, if empty then all user permissions will be retrieved.
   * @return List of UserPermissions
   * @throws Throwable on failure
   */
  public static List<UserPermission> getUserPermissions(Connection connection, String tableRegex,
      byte[] columnFamily, byte[] columnQualifier, String userName) throws Throwable {
    if (tableRegex == null || tableRegex.isEmpty() || tableRegex.charAt(0) == '@') {
      throw new IllegalArgumentException("Table name can't be null or empty or a namespace.");
    }
    /**
     * TODO: Pass an rpcController HBaseRpcController controller = ((ClusterConnection)
     * connection).getRpcControllerFactory().newController();
     */
    List<UserPermission> permList = new ArrayList<UserPermission>();
    try (Table table = connection.getTable(ACL_TABLE_NAME)) {
      try (Admin admin = connection.getAdmin()) {
        CoprocessorRpcChannel service = table.coprocessorService(HConstants.EMPTY_START_ROW);
        BlockingInterface protocol =
            AccessControlProtos.AccessControlService.newBlockingStub(service);
        HTableDescriptor[] htds = admin.listTables(Pattern.compile(tableRegex), true);
        // Retrieve table permissions
        for (HTableDescriptor htd : htds) {
          permList.addAll(AccessControlUtil.getUserPermissions(null, protocol, htd.getTableName(),
            columnFamily, columnQualifier, userName));
        }
      }
    }
    return permList;
  }

  /**
   * Validates whether specified user has permission to perform actions on the mentioned table,
   * column family or column qualifier.
   * @param connection Connection
   * @param tableName Table name, it shouldn't be null or empty.
   * @param columnFamily The column family. Optional argument, can be empty. If empty then
   *          validation will happen at table level.
   * @param columnQualifier The column qualifier. Optional argument, can be empty. If empty then
   *          validation will happen at table and column family level. columnQualifier will not be
   *          considered if columnFamily is passed as null or empty.
   * @param userName User name, it shouldn't be null or empty.
   * @param actions Actions
   * @return true if access allowed to the specified user, otherwise false.
   * @throws Throwable on failure
   */
  public static boolean hasPermission(Connection connection, String tableName, String columnFamily,
      String columnQualifier, String userName, Permission.Action... actions) throws Throwable {
    return hasPermission(connection, tableName, Bytes.toBytes(columnFamily),
      Bytes.toBytes(columnQualifier), userName, actions);
  }

  /**
   * Validates whether specified user has permission to perform actions on the mentioned table,
   * column family or column qualifier.
   * @param connection Connection
   * @param tableName Table name, it shouldn't be null or empty.
   * @param columnFamily The column family. Optional argument, can be empty. If empty then
   *          validation will happen at table level.
   * @param columnQualifier The column qualifier. Optional argument, can be empty. If empty then
   *          validation will happen at table and column family level. columnQualifier will not be
   *          considered if columnFamily is passed as null or empty.
   * @param userName User name, it shouldn't be null or empty.
   * @param actions Actions
   * @return true if access allowed to the specified user, otherwise false.
   * @throws Throwable on failure
   */
  public static boolean hasPermission(Connection connection, String tableName, byte[] columnFamily,
      byte[] columnQualifier, String userName, Permission.Action... actions) throws Throwable {
    if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(userName)) {
      throw new IllegalArgumentException("Table and user name can't be null or empty.");
    }
    boolean hasPermission = false;
    /**
     * todo: pass an rpccontroller hbaserpccontroller controller = ((clusterconnection)
     * connection).getrpccontrollerfactory().newcontroller();
     */
    try (Table table = connection.getTable(ACL_TABLE_NAME)) {
      CoprocessorRpcChannel service = table.coprocessorService(HConstants.EMPTY_START_ROW);
      BlockingInterface protocol =
          AccessControlProtos.AccessControlService.newBlockingStub(service);
      // Check whether user has permission
      hasPermission = AccessControlUtil.hasPermission(null, protocol, TableName.valueOf(tableName),
        columnFamily, columnQualifier, userName, actions);
    }
    return hasPermission;
  }
}
