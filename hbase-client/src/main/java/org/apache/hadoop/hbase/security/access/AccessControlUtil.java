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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.util.ByteStringer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@InterfaceAudience.Private
public class AccessControlUtil {
  private AccessControlUtil() {}

  /**
   * Create a request to grant user permissions.
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
      AccessControlProtos.Permission.Action... actions) {
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
      ).build();
  }

  /**
   * Create a request to grant user permissions.
   *
   * @param username the short user name who to grant permissions
   * @param namespace optional table name the permissions apply
   * @param actions the permissions to be granted
   * @return A {@link AccessControlProtos} GrantRequest
   */
  public static AccessControlProtos.GrantRequest buildGrantRequest(
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
    return AccessControlProtos.GrantRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).build();
  }

  /**
   * Create a request to revoke user permissions.
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
   * Create a request to revoke user permissions.
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
   * Create a request to grant user permissions.
   *
   * @param username the short user name who to grant permissions
   * @param actions the permissions to be granted
   * @return A {@link AccessControlProtos} GrantRequest
   */
  public static AccessControlProtos.GrantRequest buildGrantRequest(
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
    return AccessControlProtos.GrantRequest.newBuilder()
      .setUserPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(ret)
      ).build();
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
   * Converts a Permission proto to a client Permission object.
   *
   * @param proto the protobuf Permission
   * @return the converted Permission
   */
  public static Permission toPermission(AccessControlProtos.Permission proto) {
    if (proto.getType() != AccessControlProtos.Permission.Type.Global) {
      return toTablePermission(proto);
    } else {
      List<Permission.Action> actions = toPermissionActions(
          proto.getGlobalPermission().getActionList());
      return new Permission(actions.toArray(new Permission.Action[actions.size()]));
    }
  }

  /**
   * Converts a Permission proto to a client TablePermission object.
   *
   * @param proto the protobuf Permission
   * @return the converted TablePermission
   */
  public static TablePermission toTablePermission(AccessControlProtos.Permission proto) {
    if(proto.getType() == AccessControlProtos.Permission.Type.Global) {
      AccessControlProtos.GlobalPermission perm = proto.getGlobalPermission();
      List<Permission.Action> actions = toPermissionActions(perm.getActionList());

      return new TablePermission(null, null, null,
          actions.toArray(new Permission.Action[actions.size()]));
    }
    if(proto.getType() == AccessControlProtos.Permission.Type.Namespace) {
      AccessControlProtos.NamespacePermission perm = proto.getNamespacePermission();
      List<Permission.Action> actions = toPermissionActions(perm.getActionList());

      if(!proto.hasNamespacePermission()) {
        throw new IllegalStateException("Namespace must not be empty in NamespacePermission");
      }
      String namespace = perm.getNamespaceName().toStringUtf8();
      return new TablePermission(namespace, actions.toArray(new Permission.Action[actions.size()]));
    }
    if(proto.getType() == AccessControlProtos.Permission.Type.Table) {
      AccessControlProtos.TablePermission perm = proto.getTablePermission();
      List<Permission.Action> actions = toPermissionActions(perm.getActionList());

      byte[] qualifier = null;
      byte[] family = null;
      TableName table = null;

      if (!perm.hasTableName()) {
        throw new IllegalStateException("TableName cannot be empty");
      }
      table = ProtobufUtil.toTableName(perm.getTableName());

      if (perm.hasFamily()) family = perm.getFamily().toByteArray();
      if (perm.hasQualifier()) qualifier = perm.getQualifier().toByteArray();

      return new TablePermission(table, family, qualifier,
          actions.toArray(new Permission.Action[actions.size()]));
    }
    throw new IllegalStateException("Unrecognize Perm Type: "+proto.getType());
  }

  /**
   * Convert a client Permission to a Permission proto
   *
   * @param perm the client Permission
   * @return the protobuf Permission
   */
  public static AccessControlProtos.Permission toPermission(Permission perm) {
    AccessControlProtos.Permission.Builder ret = AccessControlProtos.Permission.newBuilder();
    if (perm instanceof TablePermission) {
      TablePermission tablePerm = (TablePermission)perm;
      if(tablePerm.hasNamespace()) {
        ret.setType(AccessControlProtos.Permission.Type.Namespace);

        AccessControlProtos.NamespacePermission.Builder builder =
            AccessControlProtos.NamespacePermission.newBuilder();
        builder.setNamespaceName(ByteString.copyFromUtf8(tablePerm.getNamespace()));
        Permission.Action[] actions = perm.getActions();
        if (actions != null) {
          for (Permission.Action a : actions) {
            builder.addAction(toPermissionAction(a));
          }
        }
        ret.setNamespacePermission(builder);
        return ret.build();
      } else if (tablePerm.hasTable()) {
        ret.setType(AccessControlProtos.Permission.Type.Table);

        AccessControlProtos.TablePermission.Builder builder =
            AccessControlProtos.TablePermission.newBuilder();
        builder.setTableName(ProtobufUtil.toProtoTableName(tablePerm.getTableName()));
        if (tablePerm.hasFamily()) {
          builder.setFamily(ByteStringer.wrap(tablePerm.getFamily()));
        }
        if (tablePerm.hasQualifier()) {
          builder.setQualifier(ByteStringer.wrap(tablePerm.getQualifier()));
        }
        Permission.Action actions[] = perm.getActions();
        if (actions != null) {
          for (Permission.Action a : actions) {
            builder.addAction(toPermissionAction(a));
          }
        }
        ret.setTablePermission(builder);
        return ret.build();
      }
    }

    ret.setType(AccessControlProtos.Permission.Type.Global);

    AccessControlProtos.GlobalPermission.Builder builder =
        AccessControlProtos.GlobalPermission.newBuilder();
    Permission.Action actions[] = perm.getActions();
    if (actions != null) {
      for (Permission.Action a: actions) {
        builder.addAction(toPermissionAction(a));
      }
    }
    ret.setGlobalPermission(builder);
    return ret.build();
  }

  /**
   * Converts a list of Permission.Action proto to a list of client Permission.Action objects.
   *
   * @param protoActions the list of protobuf Actions
   * @return the converted list of Actions
   */
  public static List<Permission.Action> toPermissionActions(
      List<AccessControlProtos.Permission.Action> protoActions) {
    List<Permission.Action> actions = new ArrayList<Permission.Action>(protoActions.size());
    for (AccessControlProtos.Permission.Action a : protoActions) {
      actions.add(toPermissionAction(a));
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
        .setUser(ByteStringer.wrap(perm.getUser()))
        .setPermission(toPermission(perm))
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
    return new UserPermission(proto.getUser().toByteArray(),
        toTablePermission(proto.getPermission()));
  }

  /**
   * Convert a ListMultimap&lt;String, TablePermission&gt; where key is username
   * to a protobuf UserPermission
   *
   * @param perm the list of user and table permissions
   * @return the protobuf UserTablePermissions
   */
  public static AccessControlProtos.UsersAndPermissions toUserTablePermissions(
      ListMultimap<String, TablePermission> perm) {
    AccessControlProtos.UsersAndPermissions.Builder builder =
        AccessControlProtos.UsersAndPermissions.newBuilder();
    for (Map.Entry<String, Collection<TablePermission>> entry : perm.asMap().entrySet()) {
      AccessControlProtos.UsersAndPermissions.UserPermissions.Builder userPermBuilder =
          AccessControlProtos.UsersAndPermissions.UserPermissions.newBuilder();
      userPermBuilder.setUser(ByteString.copyFromUtf8(entry.getKey()));
      for (TablePermission tablePerm: entry.getValue()) {
        userPermBuilder.addPermissions(toPermission(tablePerm));
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
   */
  public static void grant(RpcController controller,
      AccessControlService.BlockingInterface protocol, String userShortName,
      Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(toPermissionAction(a));
    }
    AccessControlProtos.GrantRequest request = buildGrantRequest(userShortName,
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
   */
  public static void grant(RpcController controller,
      AccessControlService.BlockingInterface protocol, String userShortName, TableName tableName,
      byte[] f, byte[] q, Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(toPermissionAction(a));
    }
    AccessControlProtos.GrantRequest request = buildGrantRequest(userShortName, tableName, f, q,
        permActions.toArray(new AccessControlProtos.Permission.Action[actions.length]));
    protocol.grant(controller, request);
  }

  /**
   * A utility used to grant a user namespace permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param namespace the short name of the user to grant permissions
   * @param actions the permissions to be granted
   * @throws ServiceException
   */
  public static void grant(RpcController controller,
      AccessControlService.BlockingInterface protocol, String userShortName, String namespace,
      Permission.Action... actions) throws ServiceException {
    List<AccessControlProtos.Permission.Action> permActions =
        Lists.newArrayListWithCapacity(actions.length);
    for (Permission.Action a : actions) {
      permActions.add(toPermissionAction(a));
    }
    AccessControlProtos.GrantRequest request = buildGrantRequest(userShortName, namespace,
        permActions.toArray(new AccessControlProtos.Permission.Action[actions.length]));
    protocol.grant(controller, request);
  }

  /**
   * A utility used to revoke a user's global permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to revoke permissions
   * @param actions the permissions to be revoked
   * @throws ServiceException
   */
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
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to revoke permissions
   * @param tableName optional table name
   * @param f optional column family
   * @param q optional qualifier
   * @param actions the permissions to be revoked
   * @throws ServiceException
   */
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
   * @param protocol the AccessControlService protocol proxy
   * @param userShortName the short name of the user to revoke permissions
   * @param namespace optional table name
   * @param actions the permissions to be revoked
   * @throws ServiceException
   */
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
   * @param protocol the AccessControlService protocol proxy
   * @throws ServiceException
   */
  public static List<UserPermission> getUserPermissions(RpcController controller,
      AccessControlService.BlockingInterface protocol) throws ServiceException {
    AccessControlProtos.GetUserPermissionsRequest.Builder builder =
        AccessControlProtos.GetUserPermissionsRequest.newBuilder();
    builder.setType(AccessControlProtos.Permission.Type.Global);
    AccessControlProtos.GetUserPermissionsRequest request = builder.build();
    AccessControlProtos.GetUserPermissionsResponse response =
        protocol.getUserPermissions(controller, request);
    List<UserPermission> perms = new ArrayList<UserPermission>(response.getUserPermissionCount());
    for (AccessControlProtos.UserPermission perm: response.getUserPermissionList()) {
      perms.add(toUserPermission(perm));
    }
    return perms;
  }

  /**
   * A utility used to get user table permissions.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param t optional table name
   * @throws ServiceException
   */
  public static List<UserPermission> getUserPermissions(RpcController controller,
      AccessControlService.BlockingInterface protocol,
      TableName t) throws ServiceException {
    AccessControlProtos.GetUserPermissionsRequest.Builder builder =
        AccessControlProtos.GetUserPermissionsRequest.newBuilder();
    if (t != null) {
      builder.setTableName(ProtobufUtil.toProtoTableName(t));
    }
    builder.setType(AccessControlProtos.Permission.Type.Table);
    AccessControlProtos.GetUserPermissionsRequest request = builder.build();
    AccessControlProtos.GetUserPermissionsResponse response =
        protocol.getUserPermissions(controller, request);
    List<UserPermission> perms = new ArrayList<UserPermission>(response.getUserPermissionCount());
    for (AccessControlProtos.UserPermission perm: response.getUserPermissionList()) {
      perms.add(toUserPermission(perm));
    }
    return perms;
  }

  /**
   * A utility used to get permissions for selected namespace.
   * <p>
   * It's also called by the shell, in case you want to find references.
   *
   * @param protocol the AccessControlService protocol proxy
   * @param namespace name of the namespace
   * @throws ServiceException
   */
  public static List<UserPermission> getUserPermissions(RpcController controller,
      AccessControlService.BlockingInterface protocol,
      byte[] namespace) throws ServiceException {
    AccessControlProtos.GetUserPermissionsRequest.Builder builder =
        AccessControlProtos.GetUserPermissionsRequest.newBuilder();
    if (namespace != null) {
      builder.setNamespaceName(ByteStringer.wrap(namespace));
    }
    builder.setType(AccessControlProtos.Permission.Type.Namespace);
    AccessControlProtos.GetUserPermissionsRequest request = builder.build();
    AccessControlProtos.GetUserPermissionsResponse response =
        protocol.getUserPermissions(controller, request);
    List<UserPermission> perms = new ArrayList<UserPermission>(response.getUserPermissionCount());
    for (AccessControlProtos.UserPermission perm: response.getUserPermissionList()) {
      perms.add(toUserPermission(perm));
    }
    return perms;
  }

  /**
   * Convert a protobuf UserTablePermissions to a
   * ListMultimap&lt;String, TablePermission&gt; where key is username.
   *
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static ListMultimap<String, TablePermission> toUserTablePermissions(
      AccessControlProtos.UsersAndPermissions proto) {
    ListMultimap<String, TablePermission> perms = ArrayListMultimap.create();
    AccessControlProtos.UsersAndPermissions.UserPermissions userPerm;
    for (int i = 0; i < proto.getUserPermissionsCount(); i++) {
      userPerm = proto.getUserPermissions(i);
      for (int j = 0; j < userPerm.getPermissionsCount(); j++) {
        TablePermission tablePerm = toTablePermission(userPerm.getPermissions(j));
        perms.put(userPerm.getUser().toStringUtf8(), tablePerm);
      }
    }
    return perms;
  }

  /**
   * Create a request to revoke user permissions.
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