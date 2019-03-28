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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * @since 2.0.0
 */
@InterfaceAudience.Private
public class AccessControlUtil {
  private AccessControlUtil() {}

  /**
   * Create a request to grant user table permissions.
   *
   * @param username the short user name who to grant permissions
   * @param tableName optional table name the permissions apply
   * @param family optional column family
   * @param qualifier optional qualifier
   * @param actions the permissions to be granted
   * @return A {@link AccessControlProtos} GrantRequest
   */
  public static AccessControlProtos.GrantRequest buildGrantRequest(
      String username, TableName tableName, byte[] family, byte[] qualifier,
      boolean mergeExistingPermissions, AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.TablePermission.Builder permissionBuilder =
        AccessControlProtos.TablePermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    if (tableName == null) {
      throw new NullPointerException("TableName cannot be null");
    }
    permissionBuilder.setTableName(ProtobufUtil.toProtoTableName(tableName));

    if (family != null) {
      permissionBuilder.setFamily(ByteStringer.wrap(family));
    }
    if (qualifier != null) {
      permissionBuilder.setQualifier(ByteStringer.wrap(qualifier));
    }
    ret.setType(AccessControlProtos.Permission.Type.Table)
       .setTablePermission(permissionBuilder);
    return AccessControlProtos.GrantRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).setMergeExistingPermissions(mergeExistingPermissions).build();
  }

  /**
   * Create a request to grant user namespace permissions.
   *
   * @param username the short user name who to grant permissions
   * @param namespace optional table name the permissions apply
   * @param actions the permissions to be granted
   * @return A {@link AccessControlProtos} GrantRequest
   */
  public static AccessControlProtos.GrantRequest buildGrantRequest(
      String username, String namespace, boolean mergeExistingPermissions,
      AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.NamespacePermission.Builder permissionBuilder =
        AccessControlProtos.NamespacePermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    if (namespace != null) {
      permissionBuilder.setNamespaceName(ByteString.copyFromUtf8(namespace));
    }
    ret.setType(AccessControlProtos.Permission.Type.Namespace)
       .setNamespacePermission(permissionBuilder);
    return AccessControlProtos.GrantRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).setMergeExistingPermissions(mergeExistingPermissions).build();
  }

  /**
   * Create a request to revoke user global permissions.
   *
   * @param username the short user name whose permissions to be revoked
   * @param actions the permissions to be revoked
   * @return A {@link AccessControlProtos} RevokeRequest
   */
  public static AccessControlProtos.RevokeRequest buildRevokeRequest(
      String username, AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.GlobalPermission.Builder permissionBuilder =
        AccessControlProtos.GlobalPermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    ret.setType(AccessControlProtos.Permission.Type.Global)
       .setGlobalPermission(permissionBuilder);
    return AccessControlProtos.RevokeRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).build();
  }

  /**
   * Create a request to revoke user namespace permissions.
   *
   * @param username the short user name whose permissions to be revoked
   * @param namespace optional table name the permissions apply
   * @param actions the permissions to be revoked
   * @return A {@link AccessControlProtos} RevokeRequest
   */
  public static AccessControlProtos.RevokeRequest buildRevokeRequest(
      String username, String namespace,
      AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.NamespacePermission.Builder permissionBuilder =
        AccessControlProtos.NamespacePermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    if (namespace != null) {
      permissionBuilder.setNamespaceName(ByteString.copyFromUtf8(namespace));
    }
    ret.setType(AccessControlProtos.Permission.Type.Namespace)
       .setNamespacePermission(permissionBuilder);
    return AccessControlProtos.RevokeRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).build();
  }

  /**
   * Create a request to grant user global permissions.
   *
   * @param username the short user name who to grant permissions
   * @param actions the permissions to be granted
   * @return A {@link AccessControlProtos} GrantRequest
   */
  public static AccessControlProtos.GrantRequest buildGrantRequest(String username,
      boolean mergeExistingPermissions, AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.GlobalPermission.Builder permissionBuilder =
        AccessControlProtos.GlobalPermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    ret.setType(AccessControlProtos.Permission.Type.Global)
       .setGlobalPermission(permissionBuilder);
    return AccessControlProtos.GrantRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).setMergeExistingPermissions(mergeExistingPermissions).build();
  }

  public static AccessControlProtos.UsersAndPermissions toUsersAndPermissions(String user,
      Permission perms) {
    return AccessControlProtos.UsersAndPermissions.newBuilder()
        .addUserPermissions(AccessControlProtos.UsersAndPermissions.UserPermissions.newBuilder()
            .setUser(ByteString.copyFromUtf8(user))
            .addPermissions(toPermission(perms))
            .build())
        .build();
  }

  public static AccessControlProtos.UsersAndPermissions toUsersAndPermissions(
      ListMultimap<String, Permission> perms) {
    AccessControlProtos.UsersAndPermissions.Builder builder =
        AccessControlProtos.UsersAndPermissions.newBuilder();
    for (Map.Entry<String, Collection<Permission>> entry : perms.asMap().entrySet()) {
      AccessControlProtos.UsersAndPermissions.UserPermissions.Builder userPermBuilder =
          AccessControlProtos.UsersAndPermissions.UserPermissions.newBuilder();
      userPermBuilder.setUser(ByteString.copyFromUtf8(entry.getKey()));
      for (Permission perm: entry.getValue()) {
        userPermBuilder.addPermissions(toPermission(perm));
      }
      builder.addUserPermissions(userPermBuilder.build());
    }
    return builder.build();
  }

  public static ListMultimap<String, Permission> toUsersAndPermissions(
      AccessControlProtos.UsersAndPermissions proto) {
    ListMultimap<String, Permission> result = ArrayListMultimap.create();
    for (AccessControlProtos.UsersAndPermissions.UserPermissions userPerms:
      proto.getUserPermissionsList()) {
      String user = userPerms.getUser().toStringUtf8();
      for (AccessControlProtos.Permission perm: userPerms.getPermissionsList()) {
        result.put(user, toPermission(perm));
      }
    }
    return result;
  }

  /**
   * Converts a TablePermission proto to a client TablePermission object.
   * @param proto the protobuf TablePermission
   * @return the converted TablePermission
   */
  public static TablePermission toTablePermission(AccessControlProtos.TablePermission proto) {
    Permission.Action[] actions = toPermissionActions(proto.getActionList());
    TableName table = null;
    byte[] qualifier = null;
    byte[] family = null;
    if (!proto.hasTableName()) {
      throw new IllegalStateException("TableName cannot be empty");
    }
    table = ProtobufUtil.toTableName(proto.getTableName());
    if (proto.hasFamily()) {
      family = proto.getFamily().toByteArray();
    }
    if (proto.hasQualifier()) {
      qualifier = proto.getQualifier().toByteArray();
    }
    return new TablePermission(table, family, qualifier, actions);
  }

  /**
   * Converts a Permission proto to a client Permission object.
   * @param proto the protobuf Permission
   * @return the converted Permission
   */
  public static Permission toPermission(AccessControlProtos.Permission proto) {
    if (proto.getType() == AccessControlProtos.Permission.Type.Global) {
      AccessControlProtos.GlobalPermission perm = proto.getGlobalPermission();
      Permission.Action[] actions = toPermissionActions(perm.getActionList());
      return Permission.newBuilder().withActions(actions).build();
    }
    if (proto.getType() == AccessControlProtos.Permission.Type.Namespace) {
      AccessControlProtos.NamespacePermission perm = proto.getNamespacePermission();
      Permission.Action[] actions = toPermissionActions(perm.getActionList());
      if (!proto.hasNamespacePermission()) {
        throw new IllegalStateException("Namespace must not be empty in NamespacePermission");
      }
      return Permission.newBuilder(perm.getNamespaceName().toStringUtf8()).withActions(actions)
          .build();
    }
    if (proto.getType() == AccessControlProtos.Permission.Type.Table) {
      AccessControlProtos.TablePermission perm = proto.getTablePermission();
      Permission.Action[] actions = toPermissionActions(perm.getActionList());
      byte[] qualifier = null;
      byte[] family = null;
      TableName table = null;
      if (!perm.hasTableName()) {
        throw new IllegalStateException("TableName cannot be empty");
      }
      table = ProtobufUtil.toTableName(perm.getTableName());
      if (perm.hasFamily()) {
        family = perm.getFamily().toByteArray();
      }
      if (perm.hasQualifier()) {
        qualifier = perm.getQualifier().toByteArray();
      }
      return Permission.newBuilder(table).withFamily(family).withQualifier(qualifier)
          .withActions(actions).build();
    }
    throw new IllegalStateException("Unrecognize Perm Type: " + proto.getType());
  }

  /**
   * Convert a client Permission to a Permission proto
   *
   * @param perm the client Permission
   * @return the protobuf Permission
   */
  public static AccessControlProtos.Permission toPermission(Permission perm) {
    AccessControlProtos.Permission.Builder ret = AccessControlProtos.Permission.newBuilder();
    if (perm instanceof NamespacePermission) {
      NamespacePermission namespace = (NamespacePermission) perm;
      ret.setType(AccessControlProtos.Permission.Type.Namespace);
      AccessControlProtos.NamespacePermission.Builder builder =
        AccessControlProtos.NamespacePermission.newBuilder();
      builder.setNamespaceName(ByteString.copyFromUtf8(namespace.getNamespace()));
      Permission.Action[] actions = perm.getActions();
      if (actions != null) {
        for (Permission.Action a : actions) {
          builder.addAction(toPermissionAction(a));
        }
      }
      ret.setNamespacePermission(builder);
    } else if (perm instanceof TablePermission) {
      TablePermission table = (TablePermission) perm;
      ret.setType(AccessControlProtos.Permission.Type.Table);
      AccessControlProtos.TablePermission.Builder builder =
        AccessControlProtos.TablePermission.newBuilder();
      builder.setTableName(ProtobufUtil.toProtoTableName(table.getTableName()));
      if (table.hasFamily()) {
        builder.setFamily(ByteStringer.wrap(table.getFamily()));
      }
      if (table.hasQualifier()) {
        builder.setQualifier(ByteStringer.wrap(table.getQualifier()));
      }
      Permission.Action[] actions = perm.getActions();
      if (actions != null) {
        for (Permission.Action a : actions) {
          builder.addAction(toPermissionAction(a));
        }
      }
      ret.setTablePermission(builder);
    } else {
      // perm instanceof GlobalPermission
      ret.setType(AccessControlProtos.Permission.Type.Global);
      AccessControlProtos.GlobalPermission.Builder builder =
        AccessControlProtos.GlobalPermission.newBuilder();
      Permission.Action[] actions = perm.getActions();
      if (actions != null) {
        for (Permission.Action a: actions) {
          builder.addAction(toPermissionAction(a));
        }
      }
      ret.setGlobalPermission(builder);
    }
    return ret.build();
  }

  /**
   * Converts a list of Permission.Action proto to an array of client Permission.Action objects.
   *
   * @param protoActions the list of protobuf Actions
   * @return the converted array of Actions
   */
  public static Permission.Action[]
      toPermissionActions(List<AccessControlProtos.Permission.Action> protoActions) {
    Permission.Action[] actions = new Permission.Action[protoActions.size()];
    for (int i = 0; i < protoActions.size(); i++) {
      actions[i] = toPermissionAction(protoActions.get(i));
    }
    return actions;
  }

  /**
   * Converts a Permission.Action proto to a client Permission.Action object.
   *
   * @param action the protobuf Action
   * @return the converted Action
   */
  public static Permission.Action toPermissionAction(
      AccessControlProtos.Permission.Action action) {
    switch (action) {
    case READ:
      return Permission.Action.READ;
    case WRITE:
      return Permission.Action.WRITE;
    case EXEC:
      return Permission.Action.EXEC;
    case CREATE:
      return Permission.Action.CREATE;
    case ADMIN:
      return Permission.Action.ADMIN;
    }
    throw new IllegalArgumentException("Unknown action value "+action.name());
  }

  /**
   * Convert a client Permission.Action to a Permission.Action proto
   *
   * @param action the client Action
   * @return the protobuf Action
   */
  public static AccessControlProtos.Permission.Action toPermissionAction(
      Permission.Action action) {
    switch (action) {
    case READ:
      return AccessControlProtos.Permission.Action.READ;
    case WRITE:
      return AccessControlProtos.Permission.Action.WRITE;
    case EXEC:
      return AccessControlProtos.Permission.Action.EXEC;
    case CREATE:
      return AccessControlProtos.Permission.Action.CREATE;
    case ADMIN:
      return AccessControlProtos.Permission.Action.ADMIN;
    }
    throw new IllegalArgumentException("Unknown action value "+action.name());
  }

  /**
   * Convert a client user permission to a user permission proto
   *
   * @param perm the client UserPermission
   * @return the protobuf UserPermission
   */
  public static AccessControlProtos.UserPermission toUserPermission(UserPermission perm) {
    return AccessControlProtos.UserPermission.newBuilder()
        .setUser(ByteString.copyFromUtf8(perm.getUser()))
        .setPermission(toPermission(perm.getPermission()))
        .build();
  }

  /**
   * Converts the permissions list into a protocol buffer GetUserPermissionsResponse
   */
  public static GetUserPermissionsResponse buildGetUserPermissionsResponse(
      final List<UserPermission> permissions) {
    GetUserPermissionsResponse.Builder builder = GetUserPermissionsResponse.newBuilder();
    for (UserPermission perm : permissions) {
      builder.addUserPermission(toUserPermission(perm));
    }
    return builder.build();
  }

  /**
   * Converts a user permission proto to a client user permission object.
   *
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static UserPermission toUserPermission(AccessControlProtos.UserPermission proto) {
    return new UserPermission(proto.getUser().toStringUtf8(), toPermission(proto.getPermission()));
  }

  /**
   * Convert a ListMultimap&lt;String, TablePermission&gt; where key is username
   * to a protobuf UserPermission
   *
   * @param perm the list of user and table permissions
   * @return the protobuf UserTablePermissions
   */
  public static AccessControlProtos.UsersAndPermissions toUserTablePermissions(
      ListMultimap<String, UserPermission> perm) {
    AccessControlProtos.UsersAndPermissions.Builder builder =
        AccessControlProtos.UsersAndPermissions.newBuilder();
    for (Map.Entry<String, Collection<UserPermission>> entry : perm.asMap().entrySet()) {
      AccessControlProtos.UsersAndPermissions.UserPermissions.Builder userPermBuilder =
          AccessControlProtos.UsersAndPermissions.UserPermissions.newBuilder();
      userPermBuilder.setUser(ByteString.copyFromUtf8(entry.getKey()));
      for (UserPermission userPerm: entry.getValue()) {
        userPermBuilder.addPermissions(toPermission(userPerm.getPermission()));
      }
      builder.addUserPermissions(userPermBuilder.build());
    }
    return builder.build();
  }

  /**
   * A utility used to grant a user global permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to grant permissions
   * @param actions the permissions to be granted
   * @throws ServiceException
   * @deprecated Use {@link Admin#grant(UserPermission, boolean)} instead.
   */
  @Deprecated
  public static void grant(RpcController controller,
      AccessControlService.BlockingInterface protocol, String userShortName, boolean mergeExistingPermissions,
      Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(toPermissionAction(a));
    }
    AccessControlProtos.GrantRequest request = buildGrantRequest(userShortName, mergeExistingPermissions,
        permActions.toArray(new AccessControlProtos.Permission.Action[actions.length]));
    protocol.grant(controller, request);
  }

  /**
   * A utility used to grant a user table permissions. The permissions will
   * be for a table table/column family/qualifier.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to grant permissions
   * @param tableName optional table name
   * @param f optional column family
   * @param q optional qualifier
   * @param actions the permissions to be granted
   * @throws ServiceException
   * @deprecated Use {@link Admin#grant(UserPermission, boolean)} instead.
   */
  @Deprecated
  public static void grant(RpcController controller,
      AccessControlService.BlockingInterface protocol, String userShortName, TableName tableName,
      byte[] f, byte[] q, boolean mergeExistingPermissions, Permission.Action... actions)
      throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(toPermissionAction(a));
    }
    AccessControlProtos.GrantRequest request =
        buildGrantRequest(userShortName, tableName, f, q, mergeExistingPermissions,
          permActions.toArray(new AccessControlProtos.Permission.Action[actions.length]));
    protocol.grant(controller, request);
  }

  /**
   * A utility used to grant a user namespace permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @param namespace the short name of the user to grant permissions
   * @param actions the permissions to be granted
   * @throws ServiceException
   * @deprecated Use {@link Admin#grant(UserPermission, boolean)} instead.
   */
  @Deprecated
  public static void grant(RpcController controller,
      AccessControlService.BlockingInterface protocol, String userShortName, String namespace,
      boolean mergeExistingPermissions, Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(toPermissionAction(a));
    }
    AccessControlProtos.GrantRequest request = buildGrantRequest(userShortName, namespace, mergeExistingPermissions,
        permActions.toArray(new AccessControlProtos.Permission.Action[actions.length]));
    protocol.grant(controller, request);
  }

  /**
   * A utility used to revoke a user's global permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to revoke permissions
   * @param actions the permissions to be revoked
   * @throws ServiceException on failure
   * @deprecated Use {@link Admin#revoke(UserPermission)} instead.
   */
  @Deprecated
  public static void revoke(RpcController controller,
      AccessControlService.BlockingInterface protocol, String userShortName,
      Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(toPermissionAction(a));
    }
    AccessControlProtos.RevokeRequest request = buildRevokeRequest(userShortName,
        permActions.toArray(new AccessControlProtos.Permission.Action[actions.length]));
    protocol.revoke(controller, request);
  }

  /**
   * A utility used to revoke a user's table permissions. The permissions will
   * be for a table/column family/qualifier.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to revoke permissions
   * @param tableName optional table name
   * @param f optional column family
   * @param q optional qualifier
   * @param actions the permissions to be revoked
   * @throws ServiceException on failure
   * @deprecated Use {@link Admin#revoke(UserPermission)} instead.
   */
  @Deprecated
  public static void revoke(RpcController controller,
      AccessControlService.BlockingInterface protocol, String userShortName, TableName tableName,
      byte[] f, byte[] q, Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(toPermissionAction(a));
    }
    AccessControlProtos.RevokeRequest request = buildRevokeRequest(userShortName, tableName, f, q,
        permActions.toArray(new AccessControlProtos.Permission.Action[actions.length]));
    protocol.revoke(controller, request);
  }

  /**
   * A utility used to revoke a user's namespace permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to revoke permissions
   * @param namespace optional table name
   * @param actions the permissions to be revoked
   * @throws ServiceException on failure
   * @deprecated Use {@link Admin#revoke(UserPermission)} instead.
   */
  @Deprecated
  public static void revoke(RpcController controller,
      AccessControlService.BlockingInterface protocol, String userShortName, String namespace,
      Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(toPermissionAction(a));
    }
    AccessControlProtos.RevokeRequest request = buildRevokeRequest(userShortName, namespace,
        permActions.toArray(new AccessControlProtos.Permission.Action[actions.length]));
    protocol.revoke(controller, request);
  }

  /**
   * A utility used to get user's global permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @throws ServiceException on failure
   * @deprecated Use {@link Admin#getUserPermissions(GetUserPermissionsRequest)} instead.
   */
  @Deprecated
  public static List<UserPermission> getUserPermissions(RpcController controller,
      AccessControlService.BlockingInterface protocol) throws ServiceException {
    return getUserPermissions(controller, protocol, HConstants.EMPTY_STRING);
  }

  /**
   * A utility used to get user's global permissions based on the specified user name.
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @param userName User name, if empty then all user permissions will be retrieved.
   * @throws ServiceException
   * @deprecated Use {@link Admin#getUserPermissions(GetUserPermissionsRequest)} instead.
   */
  @Deprecated
  public static List<UserPermission> getUserPermissions(RpcController controller,
      AccessControlService.BlockingInterface protocol, String userName) throws ServiceException {
    AccessControlProtos.GetUserPermissionsRequest.Builder builder =
        AccessControlProtos.GetUserPermissionsRequest.newBuilder();
    builder.setType(AccessControlProtos.Permission.Type.Global);
    if (!StringUtils.isEmpty(userName)) {
      builder.setUserName(ByteString.copyFromUtf8(userName));
    }

    AccessControlProtos.GetUserPermissionsRequest request = builder.build();
    AccessControlProtos.GetUserPermissionsResponse response =
        protocol.getUserPermissions(controller, request);
    List<UserPermission> perms = new ArrayList<>(response.getUserPermissionCount());
    for (AccessControlProtos.UserPermission perm : response.getUserPermissionList()) {
      perms.add(toUserPermission(perm));
    }
    return perms;
  }

  /**
   * A utility used to get user table permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @param t optional table name
   * @throws ServiceException
   * @deprecated Use {@link Admin#getUserPermissions(GetUserPermissionsRequest)} instead.
   */
  @Deprecated
  public static List<UserPermission> getUserPermissions(RpcController controller,
      AccessControlService.BlockingInterface protocol,
      TableName t) throws ServiceException {
    return getUserPermissions(controller, protocol, t, null, null, HConstants.EMPTY_STRING);
  }

  /**
   * A utility used to get user table permissions based on the column family, column qualifier and
   * user name.
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @param t optional table name
   * @param columnFamily Column family
   * @param columnQualifier Column qualifier
   * @param userName User name, if empty then all user permissions will be retrieved.
   * @throws ServiceException
   * @deprecated Use {@link Admin#getUserPermissions(GetUserPermissionsRequest)} instead.
   */
  @Deprecated
  public static List<UserPermission> getUserPermissions(RpcController controller,
      AccessControlService.BlockingInterface protocol, TableName t, byte[] columnFamily,
      byte[] columnQualifier, String userName) throws ServiceException {
    AccessControlProtos.GetUserPermissionsRequest.Builder builder =
        AccessControlProtos.GetUserPermissionsRequest.newBuilder();
    if (t != null) {
      builder.setTableName(ProtobufUtil.toProtoTableName(t));
    }
    if (Bytes.len(columnFamily) > 0) {
      builder.setColumnFamily(ByteString.copyFrom(columnFamily));
    }
    if (Bytes.len(columnQualifier) > 0) {
      builder.setColumnQualifier(ByteString.copyFrom(columnQualifier));
    }
    if (!StringUtils.isEmpty(userName)) {
      builder.setUserName(ByteString.copyFromUtf8(userName));
    }

    builder.setType(AccessControlProtos.Permission.Type.Table);
    AccessControlProtos.GetUserPermissionsRequest request = builder.build();
    AccessControlProtos.GetUserPermissionsResponse response =
        protocol.getUserPermissions(controller, request);
    List<UserPermission> perms = new ArrayList<>(response.getUserPermissionCount());
    for (AccessControlProtos.UserPermission perm : response.getUserPermissionList()) {
      perms.add(toUserPermission(perm));
    }
    return perms;
  }

  /**
   * A utility used to get permissions for selected namespace.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @param namespace name of the namespace
   * @throws ServiceException
   * @deprecated Use {@link Admin#getUserPermissions(GetUserPermissionsRequest)} instead.
   */
  @Deprecated
  public static List<UserPermission> getUserPermissions(RpcController controller,
      AccessControlService.BlockingInterface protocol,
      byte[] namespace) throws ServiceException {
    return getUserPermissions(controller, protocol, namespace, HConstants.EMPTY_STRING);
  }

  /**
   * A utility used to get permissions for selected namespace based on the specified user name.
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @param namespace name of the namespace
   * @param userName User name, if empty then all user permissions will be retrieved.
   * @throws ServiceException
   * @deprecated Use {@link Admin#getUserPermissions(GetUserPermissionsRequest)} instead.
   */
  @Deprecated
  public static List<UserPermission> getUserPermissions(RpcController controller,
      AccessControlService.BlockingInterface protocol, byte[] namespace, String userName)
      throws ServiceException {
    AccessControlProtos.GetUserPermissionsRequest.Builder builder =
        AccessControlProtos.GetUserPermissionsRequest.newBuilder();
    if (namespace != null) {
      builder.setNamespaceName(ByteStringer.wrap(namespace));
    }
    if (!StringUtils.isEmpty(userName)) {
      builder.setUserName(ByteString.copyFromUtf8(userName));
    }
    builder.setType(AccessControlProtos.Permission.Type.Namespace);
    AccessControlProtos.GetUserPermissionsRequest request = builder.build();
    AccessControlProtos.GetUserPermissionsResponse response =
        protocol.getUserPermissions(controller, request);
    List<UserPermission> perms = new ArrayList<>(response.getUserPermissionCount());
    for (AccessControlProtos.UserPermission perm : response.getUserPermissionList()) {
      perms.add(toUserPermission(perm));
    }
    return perms;
  }

  /**
   * Validates whether specified user has permission to perform actions on the mentioned table,
   * column family or column qualifier.
   * @param controller RpcController
   * @param protocol the AccessControlService protocol proxy
   * @param tableName Table name, it shouldn't be null or empty.
   * @param columnFamily The column family. Optional argument, can be empty. If empty then
   *          validation will happen at table level.
   * @param columnQualifier The column qualifier. Optional argument, can be empty. If empty then
   *          validation will happen at table and column family level. columnQualifier will not be
   *          considered if columnFamily is passed as null or empty.
   * @param userName User name, it shouldn't be null or empty.
   * @param actions Actions
   * @return true if access allowed, otherwise false
   * @throws ServiceException
   * @deprecated Use {@link Admin#hasUserPermissions(String, List)} instead.
   */
  @Deprecated
  public static boolean hasPermission(RpcController controller,
      AccessControlService.BlockingInterface protocol, TableName tableName, byte[] columnFamily,
      byte[] columnQualifier, String userName, Permission.Action[] actions)
      throws ServiceException {
    AccessControlProtos.TablePermission.Builder tablePermissionBuilder =
        AccessControlProtos.TablePermission.newBuilder();
    tablePermissionBuilder
        .setTableName(org.apache.hadoop.hbase.protobuf.ProtobufUtil.toProtoTableName(tableName));
    if (Bytes.len(columnFamily) > 0) {
      tablePermissionBuilder.setFamily(ByteStringer.wrap(columnFamily));
    }
    if (Bytes.len(columnQualifier) > 0) {
      tablePermissionBuilder.setQualifier(ByteString.copyFrom(columnQualifier));
    }
    for (Permission.Action a : actions) {
      tablePermissionBuilder.addAction(toPermissionAction(a));
    }
    AccessControlProtos.HasPermissionRequest request = AccessControlProtos.HasPermissionRequest
        .newBuilder().setTablePermission(tablePermissionBuilder)
        .setUserName(ByteString.copyFromUtf8(userName)).build();
    AccessControlProtos.HasPermissionResponse response =
        protocol.hasPermission(controller, request);
    return response.getHasPermission();
  }

  /**
   * Convert a protobuf UserTablePermissions to a ListMultimap&lt;Username, UserPermission&gt
   * @param proto the proto UsersAndPermissions
   * @return a ListMultimap with user and its permissions
   */
  public static ListMultimap<String, UserPermission> toUserPermission(
      AccessControlProtos.UsersAndPermissions proto) {
    ListMultimap<String, UserPermission> userPermission = ArrayListMultimap.create();
    AccessControlProtos.UsersAndPermissions.UserPermissions userPerm;
    for (int i = 0; i < proto.getUserPermissionsCount(); i++) {
      userPerm = proto.getUserPermissions(i);
      String username = userPerm.getUser().toStringUtf8();
      for (int j = 0; j < userPerm.getPermissionsCount(); j++) {
        userPermission.put(username,
          new UserPermission(username, toPermission(userPerm.getPermissions(j))));
      }
    }
    return userPermission;
  }

  /**
   * Convert a protobuf UserTablePermissions to a ListMultimap&lt;Username, Permission&gt
   * @param proto the proto UsersAndPermissions
   * @return a ListMultimap with user and its permissions
   */
  public static ListMultimap<String, Permission> toPermission(
      AccessControlProtos.UsersAndPermissions proto) {
    ListMultimap<String, Permission> perms = ArrayListMultimap.create();
    AccessControlProtos.UsersAndPermissions.UserPermissions userPerm;
    for (int i = 0; i < proto.getUserPermissionsCount(); i++) {
      userPerm = proto.getUserPermissions(i);
      String username = userPerm.getUser().toStringUtf8();
      for (int j = 0; j < userPerm.getPermissionsCount(); j++) {
        perms.put(username, toPermission(userPerm.getPermissions(j)));
      }
    }
    return perms;
  }

  /**
   * Create a request to revoke user table permissions.
   *
   * @param username the short user name whose permissions to be revoked
   * @param tableName optional table name the permissions apply
   * @param family optional column family
   * @param qualifier optional qualifier
   * @param actions the permissions to be revoked
   * @return A {@link AccessControlProtos} RevokeRequest
   */
  public static AccessControlProtos.RevokeRequest buildRevokeRequest(
      String username, TableName tableName, byte[] family, byte[] qualifier,
      AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder ret =
        AccessControlProtos.Permission.newBuilder();
    AccessControlProtos.TablePermission.Builder permissionBuilder =
        AccessControlProtos.TablePermission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    if (tableName != null) {
      permissionBuilder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    if (family != null) {
      permissionBuilder.setFamily(ByteStringer.wrap(family));
    }
    if (qualifier != null) {
      permissionBuilder.setQualifier(ByteStringer.wrap(qualifier));
    }
    ret.setType(AccessControlProtos.Permission.Type.Table)
    .setTablePermission(permissionBuilder);
    return AccessControlProtos.RevokeRequest.newBuilder()
        .setUserPermission(
            AccessControlProtos.UserPermission.newBuilder()
            .setUser(ByteString.copyFromUtf8(username))
            .setPermission(ret)
            ).build();
  }
}
