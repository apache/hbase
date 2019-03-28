/*
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Type;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Convert protobuf objects in AccessControl.proto under hbase-protocol-shaded to user-oriented
 * objects and vice versa. <br>
 *
 * In HBASE-15638, we create a hbase-protocol-shaded module for upgrading protobuf version to 3.x,
 * but there are still some coprocessor endpoints(such as AccessControl, Authentication,
 * MulitRowMutation) which depend on hbase-protocol module for CPEP compatibility. In fact, we use
 * PB objects in AccessControl.proto under hbase-protocol for access control logic and use shaded
 * AccessControl.proto only for serializing/deserializing permissions of .snapshotinfo.
 */
@InterfaceAudience.Private
public class ShadedAccessControlUtil {

  /**
   * Convert a client user permission to a user permission shaded proto.
   */
  public static AccessControlProtos.Permission.Action toPermissionAction(Permission.Action action) {
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
    throw new IllegalArgumentException("Unknown action value " + action.name());
  }

  /**
   * Convert a Permission.Action shaded proto to a client Permission.Action object.
   */
  public static Permission.Action toPermissionAction(AccessControlProtos.Permission.Action action) {
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
    throw new IllegalArgumentException("Unknown action value " + action.name());
  }

  /**
   * Converts a list of Permission.Action shaded proto to an array of client Permission.Action
   * objects.
   * @param protoActions the list of shaded protobuf Actions
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

  public static org.apache.hadoop.hbase.TableName toTableName(HBaseProtos.TableName tableNamePB) {
    return org.apache.hadoop.hbase.TableName.valueOf(
      tableNamePB.getNamespace().asReadOnlyByteBuffer(),
      tableNamePB.getQualifier().asReadOnlyByteBuffer());
  }

  public static HBaseProtos.TableName toProtoTableName(TableName tableName) {
    return HBaseProtos.TableName.newBuilder()
        .setNamespace(ByteString.copyFrom(tableName.getNamespace()))
        .setQualifier(ByteString.copyFrom(tableName.getQualifier())).build();
  }

  /**
   * Converts a Permission shaded proto to a client TablePermission object.
   * @param proto the protobuf Permission
   * @return the converted TablePermission
   */
  public static Permission toPermission(AccessControlProtos.Permission proto) {

    if (proto.getType() == AccessControlProtos.Permission.Type.Global) {
      AccessControlProtos.GlobalPermission perm = proto.getGlobalPermission();
      Action[] actions = toPermissionActions(perm.getActionList());
      return Permission.newBuilder().withActions(actions).build();
    }
    if (proto.getType() == AccessControlProtos.Permission.Type.Namespace) {
      AccessControlProtos.NamespacePermission perm = proto.getNamespacePermission();
      Action[] actions = toPermissionActions(perm.getActionList());

      if (!proto.hasNamespacePermission()) {
        throw new IllegalStateException("Namespace must not be empty in NamespacePermission");
      }
      String ns = perm.getNamespaceName().toStringUtf8();
      return Permission.newBuilder(ns).withActions(actions).build();
    }
    if (proto.getType() == AccessControlProtos.Permission.Type.Table) {
      AccessControlProtos.TablePermission perm = proto.getTablePermission();
      Action[] actions = toPermissionActions(perm.getActionList());

      byte[] qualifier = null;
      byte[] family = null;

      if (!perm.hasTableName()) {
        throw new IllegalStateException("TableName cannot be empty");
      }
      TableName table = toTableName(perm.getTableName());

      if (perm.hasFamily()) family = perm.getFamily().toByteArray();
      if (perm.hasQualifier()) qualifier = perm.getQualifier().toByteArray();
      return Permission.newBuilder(table).withFamily(family).withQualifier(qualifier)
          .withActions(actions).build();
    }
    throw new IllegalStateException("Unrecognize Perm Type: " + proto.getType());
  }

  /**
   * Convert a client Permission to a Permission shaded proto
   * @param perm the client Permission
   * @return the protobuf Permission
   */
  public static AccessControlProtos.Permission toPermission(Permission perm) {
    AccessControlProtos.Permission.Builder ret = AccessControlProtos.Permission.newBuilder();
    if (perm instanceof NamespacePermission) {
      NamespacePermission nsPerm = (NamespacePermission) perm;
      ret.setType(AccessControlProtos.Permission.Type.Namespace);
      AccessControlProtos.NamespacePermission.Builder builder =
          AccessControlProtos.NamespacePermission.newBuilder();
      builder.setNamespaceName(org.apache.hbase.thirdparty.com.google.protobuf.ByteString
          .copyFromUtf8(nsPerm.getNamespace()));
      Permission.Action[] actions = perm.getActions();
      if (actions != null) {
        for (Permission.Action a : actions) {
          builder.addAction(toPermissionAction(a));
        }
      }
      ret.setNamespacePermission(builder);
    } else if (perm instanceof TablePermission) {
      TablePermission tablePerm = (TablePermission) perm;
      ret.setType(AccessControlProtos.Permission.Type.Table);
      AccessControlProtos.TablePermission.Builder builder =
          AccessControlProtos.TablePermission.newBuilder();
      builder.setTableName(toProtoTableName(tablePerm.getTableName()));
      if (tablePerm.hasFamily()) {
        builder.setFamily(ByteString.copyFrom(tablePerm.getFamily()));
      }
      if (tablePerm.hasQualifier()) {
        builder.setQualifier(ByteString.copyFrom(tablePerm.getQualifier()));
      }
      Permission.Action[] actions = perm.getActions();
      if (actions != null) {
        for (Permission.Action a : actions) {
          builder.addAction(toPermissionAction(a));
        }
      }
      ret.setTablePermission(builder);
    } else {
      // perm.getAccessScope() == Permission.Scope.GLOBAL
      ret.setType(AccessControlProtos.Permission.Type.Global);
      AccessControlProtos.GlobalPermission.Builder builder =
          AccessControlProtos.GlobalPermission.newBuilder();
      Permission.Action[] actions = perm.getActions();
      if (actions != null) {
        for (Permission.Action a : actions) {
          builder.addAction(toPermissionAction(a));
        }
      }
      ret.setGlobalPermission(builder);
    }
    return ret.build();
  }

  /**
   * Convert a shaded protobuf UserTablePermissions to a ListMultimap&lt;String, TablePermission&gt;
   * where key is username.
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static ListMultimap<String, Permission> toUserTablePermissions(
      AccessControlProtos.UsersAndPermissions proto) {
    ListMultimap<String, Permission> perms = ArrayListMultimap.create();
    AccessControlProtos.UsersAndPermissions.UserPermissions userPerm;
    for (int i = 0; i < proto.getUserPermissionsCount(); i++) {
      userPerm = proto.getUserPermissions(i);
      for (int j = 0; j < userPerm.getPermissionsCount(); j++) {
        Permission perm = toPermission(userPerm.getPermissions(j));
        perms.put(userPerm.getUser().toStringUtf8(), perm);
      }
    }
    return perms;
  }

  /**
   * Convert a ListMultimap&lt;String, TablePermission&gt; where key is username to a shaded
   * protobuf UserPermission
   * @param perm the list of user and table permissions
   * @return the protobuf UserTablePermissions
   */
  public static AccessControlProtos.UsersAndPermissions
      toUserTablePermissions(ListMultimap<String, UserPermission> perm) {
    AccessControlProtos.UsersAndPermissions.Builder builder =
        AccessControlProtos.UsersAndPermissions.newBuilder();
    for (Map.Entry<String, Collection<UserPermission>> entry : perm.asMap().entrySet()) {
      AccessControlProtos.UsersAndPermissions.UserPermissions.Builder userPermBuilder =
          AccessControlProtos.UsersAndPermissions.UserPermissions.newBuilder();
      userPermBuilder.setUser(ByteString.copyFromUtf8(entry.getKey()));
      for (UserPermission userPerm : entry.getValue()) {
        userPermBuilder.addPermissions(toPermission(userPerm.getPermission()));
      }
      builder.addUserPermissions(userPermBuilder.build());
    }
    return builder.build();
  }

  /**
   * Converts a user permission proto to a client user permission object.
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static UserPermission toUserPermission(AccessControlProtos.UserPermission proto) {
    return new UserPermission(proto.getUser().toStringUtf8(), toPermission(proto.getPermission()));
  }

  /**
   * Convert a client user permission to a user permission proto
   * @param perm the client UserPermission
   * @return the protobuf UserPermission
   */
  public static AccessControlProtos.UserPermission toUserPermission(UserPermission perm) {
    return AccessControlProtos.UserPermission.newBuilder()
        .setUser(ByteString.copyFromUtf8(perm.getUser()))
        .setPermission(toPermission(perm.getPermission())).build();
  }

  public static GrantRequest buildGrantRequest(UserPermission userPermission,
      boolean mergeExistingPermissions) {
    return GrantRequest.newBuilder().setUserPermission(toUserPermission(userPermission))
        .setMergeExistingPermissions(mergeExistingPermissions).build();
  }

  public static RevokeRequest buildRevokeRequest(UserPermission userPermission) {
    return RevokeRequest.newBuilder().setUserPermission(toUserPermission(userPermission)).build();
  }

  public static AccessControlProtos.GetUserPermissionsRequest
      buildGetUserPermissionsRequest(GetUserPermissionsRequest request) {
    AccessControlProtos.GetUserPermissionsRequest.Builder builder =
        AccessControlProtos.GetUserPermissionsRequest.newBuilder();
    if (request.getUserName() != null && !request.getUserName().isEmpty()) {
      builder.setUserName(ByteString.copyFromUtf8(request.getUserName()));
    }
    if (request.getNamespace() != null && !request.getNamespace().isEmpty()) {
      builder.setNamespaceName(ByteString.copyFromUtf8(request.getNamespace()));
      builder.setType(Type.Namespace);
    }
    if (request.getTableName() != null) {
      builder.setTableName(toProtoTableName(request.getTableName()));
      builder.setType(Type.Table);
    }
    if (!builder.hasType()) {
      builder.setType(Type.Global);
    }
    if (request.getFamily() != null && request.getFamily().length > 0) {
      builder.setColumnFamily(ByteString.copyFrom(request.getFamily()));
    }
    if (request.getQualifier() != null && request.getQualifier().length > 0) {
      builder.setColumnQualifier(ByteString.copyFrom(request.getQualifier()));
    }
    return builder.build();
  }

  public static GetUserPermissionsResponse
      buildGetUserPermissionsResponse(final List<UserPermission> permissions) {
    GetUserPermissionsResponse.Builder builder = GetUserPermissionsResponse.newBuilder();
    for (UserPermission perm : permissions) {
      builder.addUserPermission(toUserPermission(perm));
    }
    return builder.build();
  }

  public static HasUserPermissionsRequest buildHasUserPermissionsRequest(String userName,
      List<Permission> permissions) {
    HasUserPermissionsRequest.Builder builder = HasUserPermissionsRequest.newBuilder();
    if (userName != null && !userName.isEmpty()) {
      builder.setUserName(ByteString.copyFromUtf8(userName));
    }
    for (Permission permission : permissions) {
      builder.addPermission(toPermission(permission));
    }
    return builder.build();
  }
}
