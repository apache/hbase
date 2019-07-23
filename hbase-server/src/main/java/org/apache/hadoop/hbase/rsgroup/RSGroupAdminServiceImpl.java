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
package org.apache.hadoop.hbase.rsgroup;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.AddRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.AddRSGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.ListRSGroupInfosRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.ListRSGroupInfosResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServersAndTablesRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServersAndTablesResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServersResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveTablesRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveTablesResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveServersResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.Permission.Action;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * Implementation of RSGroupAdminService defined in RSGroupAdmin.proto. This class calls
 * {@link RSGroupAdminServer} for actual work, converts result to protocol buffer response, handles
 * exceptions if any occurred and then calls the {@code RpcCallback} with the response.
 */
class RSGroupAdminServiceImpl extends RSGroupAdminProtos.RSGroupAdminService {

  private MasterServices master;

  private RSGroupAdminServer groupAdminServer;

  private AccessChecker accessChecker;

  /** Provider for mapping principal names to Users */
  private UserProvider userProvider;

  RSGroupAdminServiceImpl() {
  }

  void initialize(MasterServices master, RSGroupAdminServer groupAdminServer,
      AccessChecker accessChecker, UserProvider userProvider) {
    this.master = master;
    this.groupAdminServer = groupAdminServer;
    this.accessChecker = accessChecker;
    this.userProvider = userProvider;
  }

  @VisibleForTesting
  void checkPermission(String request) throws IOException {
    accessChecker.requirePermission(getActiveUser(), request, null, Action.ADMIN);
  }

  /**
   * Returns the active user to which authorization checks should be applied. If we are in the
   * context of an RPC call, the remote user is used, otherwise the currently logged in user is
   * used.
   */
  private User getActiveUser() throws IOException {
    // for non-rpc handling, fallback to system user
    Optional<User> optionalUser = RpcServer.getRequestUser();
    if (optionalUser.isPresent()) {
      return optionalUser.get();
    }
    return userProvider.getCurrent();
  }

  @Override
  public void getRSGroupInfo(RpcController controller, GetRSGroupInfoRequest request,
      RpcCallback<GetRSGroupInfoResponse> done) {
    GetRSGroupInfoResponse.Builder builder = GetRSGroupInfoResponse.newBuilder();
    String groupName = request.getRSGroupName();
    RSGroupAdminEndpoint.LOG.info(
      master.getClientIdAuditPrefix() + " initiates rsgroup info retrieval, group=" + groupName);
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preGetRSGroupInfo(groupName);
      }
      checkPermission("getRSGroupInfo");
      RSGroupInfo rsGroupInfo = groupAdminServer.getRSGroupInfo(groupName);
      if (rsGroupInfo != null) {
        builder.setRSGroupInfo(ProtobufUtil.toProtoGroupInfo(rsGroupInfo));
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postGetRSGroupInfo(groupName);
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void getRSGroupInfoOfTable(RpcController controller, GetRSGroupInfoOfTableRequest request,
      RpcCallback<GetRSGroupInfoOfTableResponse> done) {
    GetRSGroupInfoOfTableResponse.Builder builder = GetRSGroupInfoOfTableResponse.newBuilder();
    TableName tableName = ProtobufUtil.toTableName(request.getTableName());
    RSGroupAdminEndpoint.LOG.info(
      master.getClientIdAuditPrefix() + " initiates rsgroup info retrieval, table=" + tableName);
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preGetRSGroupInfoOfTable(tableName);
      }
      checkPermission("getRSGroupInfoOfTable");
      RSGroupInfo RSGroupInfo = groupAdminServer.getRSGroupInfoOfTable(tableName);
      if (RSGroupInfo != null) {
        builder.setRSGroupInfo(ProtobufUtil.toProtoGroupInfo(RSGroupInfo));
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postGetRSGroupInfoOfTable(tableName);
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void moveServers(RpcController controller, MoveServersRequest request,
      RpcCallback<MoveServersResponse> done) {
    MoveServersResponse.Builder builder = MoveServersResponse.newBuilder();
    Set<Address> hostPorts = Sets.newHashSet();
    for (HBaseProtos.ServerName el : request.getServersList()) {
      hostPorts.add(Address.fromParts(el.getHostName(), el.getPort()));
    }
    RSGroupAdminEndpoint.LOG.info(master.getClientIdAuditPrefix() + " move servers " + hostPorts +
        " to rsgroup " + request.getTargetGroup());
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveServers(hostPorts, request.getTargetGroup());
      }
      checkPermission("moveServers");
      groupAdminServer.moveServers(hostPorts, request.getTargetGroup());
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveServers(hostPorts, request.getTargetGroup());
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void moveTables(RpcController controller, MoveTablesRequest request,
      RpcCallback<MoveTablesResponse> done) {
    MoveTablesResponse.Builder builder = MoveTablesResponse.newBuilder();
    Set<TableName> tables = new HashSet<>(request.getTableNameList().size());
    for (HBaseProtos.TableName tableName : request.getTableNameList()) {
      tables.add(ProtobufUtil.toTableName(tableName));
    }
    RSGroupAdminEndpoint.LOG.info(master.getClientIdAuditPrefix() + " move tables " + tables +
        " to rsgroup " + request.getTargetGroup());
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveTables(tables, request.getTargetGroup());
      }
      checkPermission("moveTables");
      groupAdminServer.moveTables(tables, request.getTargetGroup());
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveTables(tables, request.getTargetGroup());
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void addRSGroup(RpcController controller, AddRSGroupRequest request,
      RpcCallback<AddRSGroupResponse> done) {
    AddRSGroupResponse.Builder builder = AddRSGroupResponse.newBuilder();
    RSGroupAdminEndpoint.LOG
        .info(master.getClientIdAuditPrefix() + " add rsgroup " + request.getRSGroupName());
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preAddRSGroup(request.getRSGroupName());
      }
      checkPermission("addRSGroup");
      groupAdminServer.addRSGroup(request.getRSGroupName());
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postAddRSGroup(request.getRSGroupName());
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void removeRSGroup(RpcController controller, RemoveRSGroupRequest request,
      RpcCallback<RemoveRSGroupResponse> done) {
    RemoveRSGroupResponse.Builder builder = RemoveRSGroupResponse.newBuilder();
    RSGroupAdminEndpoint.LOG
        .info(master.getClientIdAuditPrefix() + " remove rsgroup " + request.getRSGroupName());
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preRemoveRSGroup(request.getRSGroupName());
      }
      checkPermission("removeRSGroup");
      groupAdminServer.removeRSGroup(request.getRSGroupName());
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postRemoveRSGroup(request.getRSGroupName());
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void balanceRSGroup(RpcController controller, BalanceRSGroupRequest request,
      RpcCallback<BalanceRSGroupResponse> done) {
    BalanceRSGroupResponse.Builder builder = BalanceRSGroupResponse.newBuilder();
    RSGroupAdminEndpoint.LOG.info(
      master.getClientIdAuditPrefix() + " balance rsgroup, group=" + request.getRSGroupName());
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preBalanceRSGroup(request.getRSGroupName());
      }
      checkPermission("balanceRSGroup");
      boolean balancerRan = groupAdminServer.balanceRSGroup(request.getRSGroupName());
      builder.setBalanceRan(balancerRan);
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postBalanceRSGroup(request.getRSGroupName(), balancerRan);
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
      builder.setBalanceRan(false);
    }
    done.run(builder.build());
  }

  @Override
  public void listRSGroupInfos(RpcController controller, ListRSGroupInfosRequest request,
      RpcCallback<ListRSGroupInfosResponse> done) {
    ListRSGroupInfosResponse.Builder builder = ListRSGroupInfosResponse.newBuilder();
    RSGroupAdminEndpoint.LOG.info(master.getClientIdAuditPrefix() + " list rsgroup");
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preListRSGroups();
      }
      checkPermission("listRSGroup");
      for (RSGroupInfo RSGroupInfo : groupAdminServer.listRSGroups()) {
        builder.addRSGroupInfo(ProtobufUtil.toProtoGroupInfo(RSGroupInfo));
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postListRSGroups();
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void getRSGroupInfoOfServer(RpcController controller,
      GetRSGroupInfoOfServerRequest request, RpcCallback<GetRSGroupInfoOfServerResponse> done) {
    GetRSGroupInfoOfServerResponse.Builder builder = GetRSGroupInfoOfServerResponse.newBuilder();
    Address hp =
        Address.fromParts(request.getServer().getHostName(), request.getServer().getPort());
    RSGroupAdminEndpoint.LOG
        .info(master.getClientIdAuditPrefix() + " initiates rsgroup info retrieval, server=" + hp);
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preGetRSGroupInfoOfServer(hp);
      }
      checkPermission("getRSGroupInfoOfServer");
      RSGroupInfo info = groupAdminServer.getRSGroupOfServer(hp);
      if (info != null) {
        builder.setRSGroupInfo(ProtobufUtil.toProtoGroupInfo(info));
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postGetRSGroupInfoOfServer(hp);
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void moveServersAndTables(RpcController controller, MoveServersAndTablesRequest request,
      RpcCallback<MoveServersAndTablesResponse> done) {
    MoveServersAndTablesResponse.Builder builder = MoveServersAndTablesResponse.newBuilder();
    Set<Address> hostPorts = Sets.newHashSet();
    for (HBaseProtos.ServerName el : request.getServersList()) {
      hostPorts.add(Address.fromParts(el.getHostName(), el.getPort()));
    }
    Set<TableName> tables = new HashSet<>(request.getTableNameList().size());
    for (HBaseProtos.TableName tableName : request.getTableNameList()) {
      tables.add(ProtobufUtil.toTableName(tableName));
    }
    RSGroupAdminEndpoint.LOG.info(master.getClientIdAuditPrefix() + " move servers " + hostPorts +
        " and tables " + tables + " to rsgroup" + request.getTargetGroup());
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveServersAndTables(hostPorts, tables,
          request.getTargetGroup());
      }
      checkPermission("moveServersAndTables");
      groupAdminServer.moveServersAndTables(hostPorts, tables, request.getTargetGroup());
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveServersAndTables(hostPorts, tables,
          request.getTargetGroup());
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void removeServers(RpcController controller, RemoveServersRequest request,
      RpcCallback<RemoveServersResponse> done) {
    RemoveServersResponse.Builder builder = RemoveServersResponse.newBuilder();
    Set<Address> servers = Sets.newHashSet();
    for (HBaseProtos.ServerName el : request.getServersList()) {
      servers.add(Address.fromParts(el.getHostName(), el.getPort()));
    }
    RSGroupAdminEndpoint.LOG.info(
      master.getClientIdAuditPrefix() + " remove decommissioned servers from rsgroup: " + servers);
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preRemoveServers(servers);
      }
      checkPermission("removeServers");
      groupAdminServer.removeServers(servers);
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postRemoveServers(servers);
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(builder.build());
  }
}