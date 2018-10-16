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


import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
  public static
      org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Action
      toPermissionAction(Permission.Action action) {
    switch (action) {
    case READ:
      return org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Action.READ;
    case WRITE:
      return org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Action.WRITE;
    case EXEC:
      return org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Action.EXEC;
    case CREATE:
      return org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Action.CREATE;
    case ADMIN:
      return org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Action.ADMIN;
    }
    throw new IllegalArgumentException("Unknown action value " + action.name());
  }

  /**
   * Convert a Permission.Action shaded proto to a client Permission.Action object.
   */
  public static Permission.Action toPermissionAction(
      org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Action action) {
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
   * Converts a list of Permission.Action shaded proto to a list of client Permission.Action
   * objects.
   * @param protoActions the list of shaded protobuf Actions
   * @return the converted list of Actions
   */
  public static List<Permission.Action> toPermissionActions(
      List<org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Action> protoActions) {
    List<Permission.Action> actions = new ArrayList<>(protoActions.size());
    for (org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Action a : protoActions) {
      actions.add(toPermissionAction(a));
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
  public static TablePermission toTablePermission(AccessControlProtos.Permission proto) {

    if (proto.getType() == AccessControlProtos.Permission.Type.Global) {
      AccessControlProtos.GlobalPermission perm = proto.getGlobalPermission();
      List<Action> actions = toPermissionActions(perm.getActionList());

      return new TablePermission(null, null, null,
          actions.toArray(new Permission.Action[actions.size()]));
    }
    if (proto.getType() == AccessControlProtos.Permission.Type.Namespace) {
      AccessControlProtos.NamespacePermission perm = proto.getNamespacePermission();
      List<Permission.Action> actions = toPermissionActions(perm.getActionList());

      if (!proto.hasNamespacePermission()) {
        throw new IllegalStateException("Namespace must not be empty in NamespacePermission");
      }
      String namespace = perm.getNamespaceName().toStringUtf8();
      return new TablePermission(namespace, actions.toArray(new Permission.Action[actions.size()]));
    }
    if (proto.getType() == AccessControlProtos.Permission.Type.Table) {
      AccessControlProtos.TablePermission perm = proto.getTablePermission();
      List<Permission.Action> actions = toPermissionActions(perm.getActionList());

      byte[] qualifier = null;
      byte[] family = null;
      TableName table = null;

      if (!perm.hasTableName()) {
        throw new IllegalStateException("TableName cannot be empty");
      }
      table = toTableName(perm.getTableName());

      if (perm.hasFamily()) family = perm.getFamily().toByteArray();
      if (perm.hasQualifier()) qualifier = perm.getQualifier().toByteArray();

      return new TablePermission(table, family, qualifier,
          actions.toArray(new Permission.Action[actions.size()]));
    }
    throw new IllegalStateException("Unrecognize Perm Type: " + proto.getType());
  }

  /**
   * Convert a client Permission to a Permission shaded proto
   * @param perm the client Permission
   * @return the protobuf Permission
   */
  public static org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission
      toPermission(Permission perm) {
    org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Builder ret =
        org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission
            .newBuilder();
    if (perm instanceof TablePermission) {
      TablePermission tablePerm = (TablePermission) perm;
      if (tablePerm.hasNamespace()) {
        ret.setType(
          org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Type.Namespace);

        org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.NamespacePermission.Builder builder =
            org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.NamespacePermission
                .newBuilder();
        builder.setNamespaceName(org.apache.hbase.thirdparty.com.google.protobuf.ByteString
            .copyFromUtf8(tablePerm.getNamespace()));
        Permission.Action[] actions = perm.getActions();
        if (actions != null) {
          for (Permission.Action a : actions) {
            builder.addAction(toPermissionAction(a));
          }
        }
        ret.setNamespacePermission(builder);
        return ret.build();
      } else if (tablePerm.hasTable()) {
        ret.setType(
          org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Type.Table);

        org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.TablePermission.Builder builder =
            org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.TablePermission
                .newBuilder();
        builder.setTableName(toProtoTableName(tablePerm.getTableName()));
        if (tablePerm.hasFamily()) {
          builder.setFamily(ByteString.copyFrom(tablePerm.getFamily()));
        }
        if (tablePerm.hasQualifier()) {
          builder.setQualifier(ByteString.copyFrom(tablePerm.getQualifier()));
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

    ret.setType(
      org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Type.Global);

    org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GlobalPermission.Builder builder =
        org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GlobalPermission
            .newBuilder();
    Permission.Action actions[] = perm.getActions();
    if (actions != null) {
      for (Permission.Action a : actions) {
        builder.addAction(toPermissionAction(a));
      }
    }
    ret.setGlobalPermission(builder);
    return ret.build();
  }

  /**
   * Convert a shaded protobuf UserTablePermissions to a ListMultimap&lt;String, TablePermission&gt;
   * where key is username.
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static ListMultimap<String, TablePermission> toUserTablePermissions(
      org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.UsersAndPermissions proto) {
    ListMultimap<String, TablePermission> perms = ArrayListMultimap.create();
    org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.UsersAndPermissions.UserPermissions userPerm;
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
   * Convert a ListMultimap&lt;String, TablePermission&gt; where key is username to a shaded
   * protobuf UserPermission
   * @param perm the list of user and table permissions
   * @return the protobuf UserTablePermissions
   */
  public static
      org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.UsersAndPermissions
      toUserTablePermissions(ListMultimap<String, TablePermission> perm) {
    org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.UsersAndPermissions.Builder builder =
        org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.UsersAndPermissions
            .newBuilder();
    for (Map.Entry<String, Collection<TablePermission>> entry : perm.asMap().entrySet()) {
      org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.UsersAndPermissions.UserPermissions.Builder userPermBuilder =
          org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.UsersAndPermissions.UserPermissions
              .newBuilder();
      userPermBuilder.setUser(ByteString.copyFromUtf8(entry.getKey()));
      for (TablePermission tablePerm : entry.getValue()) {
        userPermBuilder.addPermissions(toPermission(tablePerm));
      }
      builder.addUserPermissions(userPermBuilder.build());
    }
    return builder.build();
  }

  /**
   * Converts a user permission proto to a client user permission object.
   *
   * @param proto the protobuf UserPermission
   * @return the converted UserPermission
   */
  public static UserPermission toUserPermission(org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.UserPermission proto) {
    return new UserPermission(proto.getUser().toByteArray(),
        toTablePermission(proto.getPermission()));
  }
}
