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
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService.BlockingInterface;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GrantResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.RevokeResponse;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;

/**
 * Utility client for doing access control admin operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AccessControlClient {
  public static final TableName ACL_TABLE_NAME =
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "acl");

  private static HTable getAclTable(Configuration conf) throws IOException {
    return new HTable(conf, ACL_TABLE_NAME);
  }

  private static BlockingInterface getAccessControlServiceStub(HTable ht)
      throws IOException {
    CoprocessorRpcChannel service = ht.coprocessorService(HConstants.EMPTY_START_ROW);
    BlockingInterface protocol =
        AccessControlProtos.AccessControlService.newBlockingStub(service);
    return protocol;
  }


  /**
   * Grants permission on the specified table for the specified user
   * @param conf
   * @param tableName
   * @param userName
   * @param family
   * @param qual
   * @param actions
   * @throws Throwable
   */
  public static void grant(Configuration conf, final TableName tableName,
      final String userName, final byte[] family, final byte[] qual,
      final Permission.Action... actions) throws Throwable {
    HTable ht = null;
    try {
      ht = getAclTable(conf);
      ProtobufUtil.grant(getAccessControlServiceStub(ht), userName, tableName, family, qual,
          actions);
    } finally {
      if (ht != null) {
        ht.close();
      }
    }
  }
  /**
   * Grants permission on the specified namespace for the specified user.
   * @param conf
   * @param namespace
   * @param userName
   * @param actions
   * @throws Throwable
   */
  public static void grant(Configuration conf, final String namespace,
      final String userName, final Permission.Action... actions) throws Throwable {
    HTable ht = null;
    try {
      ht = getAclTable(conf);
      ProtobufUtil.grant(getAccessControlServiceStub(ht), userName, namespace, actions);
    } finally {
      if (ht != null) {
        ht.close();
      }
    }
  }

  /**
   * Grant global permissions for the specified user.
   */
  public static void grant(Configuration conf, final String userName,
       final Permission.Action... actions) throws Throwable {
    HTable ht = null;
    try {
      ht = getAclTable(conf);
      ProtobufUtil.grant(getAccessControlServiceStub(ht), userName, actions);
    } finally {
      if (ht != null) {
        ht.close();
      }
    }
  }

  public static boolean isAccessControllerRunning(Configuration conf)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    HBaseAdmin ha = null;
    try {
      ha = new HBaseAdmin(conf);
      return ha.isTableAvailable(ACL_TABLE_NAME);
    } finally {
      if (ha != null) {
        ha.close();
      }
    }
  }
  /**
   * Revokes the permission on the table
   * @param conf
   * @param tableName
   * @param username
   * @param family
   * @param qualifier
   * @param actions
   * @throws Throwable
   */
  public static void revoke(Configuration conf, final TableName tableName,
      final String username, final byte[] family, final byte[] qualifier,
      final Permission.Action... actions) throws Throwable {
    HTable ht = null;
    try {
      ht = getAclTable(conf);
      ProtobufUtil.revoke(getAccessControlServiceStub(ht), username, tableName, family, qualifier,
          actions);
    } finally {
      if (ht != null) {
        ht.close();
      }
    }
  }

  /**
   * Revokes the permission on the table for the specified user.
   * @param conf
   * @param namespace
   * @param userName
   * @param actions
   * @throws Throwable
   */
  public static void revoke(Configuration conf, final String namespace,
      final String userName, final Permission.Action... actions) throws Throwable {
    HTable ht = null;
    try {
      ht = getAclTable(conf);
      ProtobufUtil.revoke(getAccessControlServiceStub(ht), userName, namespace, actions);
    } finally {
      if (ht != null) {
        ht.close();
      }
    }
  }

  /**
   * Revoke global permissions for the specified user.
   */
  public static void revoke(Configuration conf, final String userName,
      final Permission.Action... actions) throws Throwable {
    HTable ht = null;
    try {
      ht = getAclTable(conf);
      ProtobufUtil.revoke(getAccessControlServiceStub(ht), userName, actions);
    } finally {
      if (ht != null) {
        ht.close();
      }
    }
  }

  /**
   * List all the userPermissions matching the given pattern.
   * @param conf
   * @param tableRegex The regular expression string to match against
   * @return - returns an array of UserPermissions
   * @throws Throwable
   */
  public static List<UserPermission> getUserPermissions(Configuration conf, String tableRegex)
      throws Throwable {
    List<UserPermission> permList = new ArrayList<UserPermission>();
    HTable ht = null;
    HBaseAdmin ha = null;
    try {
      ha = new HBaseAdmin(conf);
      ht = new HTable(conf, ACL_TABLE_NAME);
      CoprocessorRpcChannel service = ht.coprocessorService(HConstants.EMPTY_START_ROW);
      BlockingInterface protocol =
          AccessControlProtos.AccessControlService.newBlockingStub(service);
      HTableDescriptor[] htds = null;

      if (tableRegex == null || tableRegex.isEmpty()) {
        permList = ProtobufUtil.getUserPermissions(protocol);
      } else if (tableRegex.charAt(0) == '@') {
        String namespace = tableRegex.substring(1);
        permList = ProtobufUtil.getUserPermissions(protocol, Bytes.toBytes(namespace));
      } else {
        htds = ha.listTables(Pattern.compile(tableRegex));
        for (HTableDescriptor hd : htds) {
          permList.addAll(ProtobufUtil.getUserPermissions(protocol, hd.getTableName()));
        }
      }
    } finally {
      if (ht != null) {
        ht.close();
      }
      if (ha != null) {
        ha.close();
      }
    }
    return permList;
  }

  /**
   * Grants permission on the specified table for the specified user
   * @param conf
   * @param tableName
   * @param userName
   * @param family
   * @param qual
   * @param actions
   * @return GrantResponse
   * @throws Throwable
   * @deprecated Use {@link #grant(Configuration, TableName, String, byte[], byte[], Permission.Action...)}  instead.
   */
  @Deprecated
  public static GrantResponse grant(Configuration conf, final TableName tableName,
                                    final String userName, final byte[] family, final byte[] qual,
                                    final AccessControlProtos.Permission.Action... actions) throws Throwable {
    HTable ht = null;
    try {
      TableName aclTableName =
          TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "acl");
      ht = new HTable(conf, aclTableName.getName());
      Batch.Call<AccessControlService, GrantResponse> callable =
          new Batch.Call<AccessControlService, GrantResponse>() {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<GrantResponse> rpcCallback =
                new BlockingRpcCallback<GrantResponse>();
            @Override
            public GrantResponse call(AccessControlService service) throws IOException {
              GrantRequest.Builder builder = GrantRequest.newBuilder();
              AccessControlProtos.Permission.Builder ret =
                  AccessControlProtos.Permission.newBuilder();
              AccessControlProtos.TablePermission.Builder permissionBuilder =
                  AccessControlProtos.TablePermission
                      .newBuilder();
              for (AccessControlProtos.Permission.Action a : actions) {
                permissionBuilder.addAction(a);
              }
              permissionBuilder.setTableName(ProtobufUtil.toProtoTableName(tableName));
              if (family != null) {
                permissionBuilder.setFamily(ByteStringer.wrap(family));
              }
              if (qual != null) {
                permissionBuilder.setQualifier(ByteStringer.wrap(qual));
              }
              ret.setType(AccessControlProtos.Permission.Type.Table).setTablePermission(
                  permissionBuilder);
              builder.setUserPermission(AccessControlProtos.UserPermission.newBuilder()
                  .setUser(ByteString.copyFromUtf8(userName)).setPermission(ret));
              service.grant(controller, builder.build(), rpcCallback);
              return rpcCallback.get();
            }
          };
      Map<byte[], GrantResponse> result = ht.coprocessorService(AccessControlService.class,
          HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, callable);
      return result.values().iterator().next(); // There will be exactly one
                                                // region for labels
                                                // table and so one entry in
                                                // result Map.
    } finally {
      if (ht != null) {
        ht.close();
      }
    }
  }

  /**
   * Revokes the permission on the table
   * @param conf
   * @param username
   * @param tableName
   * @param family
   * @param qualifier
   * @param actions
   * @return RevokeResponse
   * @throws Throwable
   * @deprecated Use {@link #revoke(Configuration, TableName, String, byte[], byte[], Permission.Action...)} instead
   */
  @Deprecated
  public static RevokeResponse revoke(Configuration conf, final String username,
                                      final TableName tableName, final byte[] family, final byte[] qualifier,
                                      final AccessControlProtos.Permission.Action... actions) throws Throwable {
    HTable ht = null;
    try {
      TableName aclTableName = TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR,
          "acl");
      ht = new HTable(conf, aclTableName.getName());
      Batch.Call<AccessControlService, AccessControlProtos.RevokeResponse> callable =
          new Batch.Call<AccessControlService, AccessControlProtos.RevokeResponse>() {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<AccessControlProtos.RevokeResponse> rpcCallback =
                new BlockingRpcCallback<AccessControlProtos.RevokeResponse>();
            @Override
            public RevokeResponse call(AccessControlService service) throws IOException {
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
              ret.setType(AccessControlProtos.Permission.Type.Table).setTablePermission(
                  permissionBuilder);
              RevokeRequest builder = AccessControlProtos.RevokeRequest
                  .newBuilder()
                  .setUserPermission(
                      AccessControlProtos.UserPermission.newBuilder()
                          .setUser(ByteString.copyFromUtf8(username)).setPermission(ret)).build();
              service.revoke(controller, builder, rpcCallback);
              return rpcCallback.get();
            }
          };
      Map<byte[], RevokeResponse> result = ht.coprocessorService(AccessControlService.class,
          HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, callable);
      return result.values().iterator().next();
    } finally {
      if (ht != null) {
        ht.close();
      }
    }
  }
}
