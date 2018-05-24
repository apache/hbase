/**
 * Copyright The Apache Software Foundation
 *
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

import com.google.common.collect.Sets;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin.MasterSwitchType;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
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
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RSGroupAdminService;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveServersResponse;
import org.apache.hadoop.hbase.protobuf.generated.TableProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.TableAuthManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

public class RSGroupAdminEndpoint extends RSGroupAdminService
    implements CoprocessorService, Coprocessor, MasterObserver {

  private static final Log LOG = LogFactory.getLog(RSGroupAdminEndpoint.class);

  private MasterServices master = null;

  private static RSGroupInfoManagerImpl groupInfoManager;
  private RSGroupAdminServer groupAdminServer;
  private AccessChecker accessChecker;

  /** Provider for mapping principal names to Users */
  private UserProvider userProvider;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    MasterCoprocessorEnvironment menv = (MasterCoprocessorEnvironment)env;
    master = menv.getMasterServices();
    setGroupInfoManager(new RSGroupInfoManagerImpl(master));
    groupAdminServer = new RSGroupAdminServer(master, groupInfoManager);
    Class<?> clazz =
        master.getConfiguration().getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, null);
    if (!RSGroupableBalancer.class.isAssignableFrom(clazz)) {
      throw new IOException("Configured balancer is not a GroupableBalancer");
    }
    ZooKeeperWatcher zk = menv.getMasterServices().getZooKeeper();
    accessChecker = new AccessChecker(env.getConfiguration(), zk);

    // set the user-provider.
    this.userProvider = UserProvider.instantiate(env.getConfiguration());
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (accessChecker.getAuthManager() != null) {
      TableAuthManager.release(accessChecker.getAuthManager());
    }
  }

  @Override
  public Service getService() {
    return this;
  }

  private static void setStaticGroupInfoManager(RSGroupInfoManagerImpl groupInfoManager) {
    RSGroupAdminEndpoint.groupInfoManager = groupInfoManager;
  }

  private void setGroupInfoManager(RSGroupInfoManagerImpl groupInfoManager) throws IOException {
    if (groupInfoManager == null) {
      groupInfoManager = new RSGroupInfoManagerImpl(master);
      groupInfoManager.init();
    } else if (!groupInfoManager.isInit()) {
      groupInfoManager.init();
    }
    setStaticGroupInfoManager(groupInfoManager);
  }

  public RSGroupInfoManager getGroupInfoManager() {
    return groupInfoManager;
  }

  @Override
  public void getRSGroupInfo(RpcController controller,
                           GetRSGroupInfoRequest request,
                           RpcCallback<GetRSGroupInfoResponse> done) {
    GetRSGroupInfoResponse response = null;
    try {
      GetRSGroupInfoResponse.Builder builder =
          GetRSGroupInfoResponse.newBuilder();
      RSGroupInfo RSGroupInfo = groupAdminServer.getRSGroupInfo(request.getRSGroupName());
      checkPermission("getRSGroupInfo");
      if(RSGroupInfo != null) {
        builder.setRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo));
      }
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void getRSGroupInfoOfTable(RpcController controller,
                                  GetRSGroupInfoOfTableRequest request,
                                  RpcCallback<GetRSGroupInfoOfTableResponse> done) {
    GetRSGroupInfoOfTableResponse response = null;
    try {
      GetRSGroupInfoOfTableResponse.Builder builder =
          GetRSGroupInfoOfTableResponse.newBuilder();
      TableName tableName = ProtobufUtil.toTableName(request.getTableName());
      checkPermission("getRSGroupInfoOfTable");
      RSGroupInfo info = groupAdminServer.getRSGroupInfoOfTable(tableName);
      if (info == null) {
        response = builder.build();
      } else {
        response = builder.setRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(info)).build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void moveServers(RpcController controller,
                          MoveServersRequest request,
                          RpcCallback<MoveServersResponse> done) {
    RSGroupAdminProtos.MoveServersResponse response = null;
    try {
      RSGroupAdminProtos.MoveServersResponse.Builder builder =
          RSGroupAdminProtos.MoveServersResponse.newBuilder();
      Set<Address> servers = Sets.newHashSet();
      for(HBaseProtos.ServerName el: request.getServersList()) {
        servers.add(Address.fromParts(el.getHostName(), el.getPort()));
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveServers(servers, request.getTargetGroup());
      }
      checkPermission("moveServers");
      groupAdminServer.moveServers(servers, request.getTargetGroup());
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveServers(servers, request.getTargetGroup());
      }
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void moveTables(RpcController controller,
                         MoveTablesRequest request,
                         RpcCallback<MoveTablesResponse> done) {
    MoveTablesResponse response = null;
    try {
      MoveTablesResponse.Builder builder =
          MoveTablesResponse.newBuilder();
      Set<TableName> tables = new HashSet<TableName>(request.getTableNameList().size());
      for(TableProtos.TableName tableName: request.getTableNameList()) {
        tables.add(ProtobufUtil.toTableName(tableName));
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveTables(tables, request.getTargetGroup());
      }
      checkPermission("moveTables");
      groupAdminServer.moveTables(tables, request.getTargetGroup());
      response = builder.build();
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveTables(tables, request.getTargetGroup());
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void moveServersAndTables(RpcController controller, MoveServersAndTablesRequest request,
      RpcCallback<MoveServersAndTablesResponse> done) {
    MoveServersAndTablesResponse.Builder builder = MoveServersAndTablesResponse.newBuilder();
    try {
      Set<Address> servers = Sets.newHashSet();
      for (HBaseProtos.ServerName el : request.getServersList()) {
        servers.add(Address.fromParts(el.getHostName(), el.getPort()));
      }
      Set<TableName> tables = new HashSet<>(request.getTableNameList().size());
      for (TableProtos.TableName tableName : request.getTableNameList()) {
        tables.add(ProtobufUtil.toTableName(tableName));
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveServersAndTables(servers, tables,
            request.getTargetGroup());
      }
      checkPermission("moveServersAndTables");
      groupAdminServer.moveServersAndTables(servers, tables, request.getTargetGroup());
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveServersAndTables(servers, tables,
            request.getTargetGroup());
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void addRSGroup(RpcController controller,
                       AddRSGroupRequest request,
                       RpcCallback<AddRSGroupResponse> done) {
    AddRSGroupResponse response = null;
    try {
      AddRSGroupResponse.Builder builder =
          AddRSGroupResponse.newBuilder();
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preAddRSGroup(request.getRSGroupName());
      }
      checkPermission("addRSGroup");
      groupAdminServer.addRSGroup(request.getRSGroupName());
      response = builder.build();
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postAddRSGroup(request.getRSGroupName());
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void removeRSGroup(RpcController controller,
                          RemoveRSGroupRequest request,
                          RpcCallback<RemoveRSGroupResponse> done) {
    RemoveRSGroupResponse response = null;
    try {
      RemoveRSGroupResponse.Builder builder =
          RemoveRSGroupResponse.newBuilder();
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preRemoveRSGroup(request.getRSGroupName());
      }
      checkPermission("removeRSGroup");
      groupAdminServer.removeRSGroup(request.getRSGroupName());
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postRemoveRSGroup(request.getRSGroupName());
      }
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void balanceRSGroup(RpcController controller,
                           BalanceRSGroupRequest request,
                           RpcCallback<BalanceRSGroupResponse> done) {
    BalanceRSGroupResponse.Builder builder = BalanceRSGroupResponse.newBuilder();
    try {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preBalanceRSGroup(request.getRSGroupName());
      }
      checkPermission("balanceRSGroup");
      boolean balancerRan = groupAdminServer.balanceRSGroup(request.getRSGroupName());
      builder.setBalanceRan(balancerRan);
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postBalanceRSGroup(request.getRSGroupName(),
            balancerRan);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
      builder.setBalanceRan(false);
    }
    done.run(builder.build());
  }

  @Override
  public void listRSGroupInfos(RpcController controller,
                             ListRSGroupInfosRequest request,
                             RpcCallback<ListRSGroupInfosResponse> done) {
    ListRSGroupInfosResponse response = null;
    try {
      ListRSGroupInfosResponse.Builder builder =
          ListRSGroupInfosResponse.newBuilder();
      checkPermission("listRSGroupInfos");
      for(RSGroupInfo RSGroupInfo : groupAdminServer.listRSGroups()) {
        builder.addRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo));
      }
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void getRSGroupInfoOfServer(RpcController controller,
                                   GetRSGroupInfoOfServerRequest request,
                                   RpcCallback<GetRSGroupInfoOfServerResponse> done) {
    GetRSGroupInfoOfServerResponse.Builder builder = GetRSGroupInfoOfServerResponse.newBuilder();
    try {
      Address server =
          Address.fromParts(request.getServer().getHostName(), request.getServer().getPort());
      checkPermission("getRSGroupInfoOfServer");
      RSGroupInfo RSGroupInfo = groupAdminServer.getRSGroupOfServer(server);
      if (RSGroupInfo != null) {
        builder.setRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo));
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void removeServers(RpcController controller,
      RemoveServersRequest request,
      RpcCallback<RemoveServersResponse> done) {
    RemoveServersResponse.Builder builder =
        RemoveServersResponse.newBuilder();
    try {
      Set<Address> servers = Sets.newHashSet();
      for (HBaseProtos.ServerName el : request.getServersList()) {
        servers.add(Address.fromParts(el.getHostName(), el.getPort()));
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preRemoveServers(servers);
      }
      checkPermission("removeServers");
      groupAdminServer.removeServers(servers);
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postRemoveServers(servers);
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  void assignTableToGroup(HTableDescriptor desc) throws IOException {
    String groupName;
    try {
      groupName =
        master.getNamespaceDescriptor(desc.getTableName().getNamespaceAsString())
                .getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP);
      if (groupName == null) {
        groupName = RSGroupInfo.DEFAULT_GROUP;
      }
    } catch (MasterNotRunningException | PleaseHoldException e) {
      LOG.info("Master has not initialized yet; temporarily using default RSGroup '" +
        RSGroupInfo.DEFAULT_GROUP + "' for deploy of system table");
      groupName = RSGroupInfo.DEFAULT_GROUP;
    }
    RSGroupInfo rsGroupInfo = groupAdminServer.getRSGroupInfo(groupName);
    if (rsGroupInfo == null) {
      throw new ConstraintException("Default RSGroup (" + groupName + ") for this table's "
          + "namespace does not exist.");
    }
    if (!rsGroupInfo.containsTable(desc.getTableName())) {
      groupAdminServer.moveTables(Sets.newHashSet(desc.getTableName()), groupName);
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // MasterObserver overrides
  /////////////////////////////////////////////////////////////////////////////

  // Assign table to default RSGroup.
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    groupAdminServer.prepareRSGroupForTable(desc);
  }

  // Remove table from its RSGroup.
  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName) throws IOException {
    groupAdminServer.cleanupRSGroupForTable(tableName);
  }

  //unused cp hooks

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              HTableDescriptor desc,
                              HRegionInfo[] regions) throws IOException {
    assignTableToGroup(desc);
  }

  @Override
  public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    HTableDescriptor desc,
                                    HRegionInfo[] regions) throws IOException {

  }

  @Override
  public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     HTableDescriptor desc,
                                     HRegionInfo[] regions) throws IOException {

  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             TableName tableName) throws IOException {

  }

  @Override
  public void preDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    TableName tableName) throws IOException {

  }

  @Override
  public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     TableName tableName) throws IOException {

  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName) throws IOException {

  }

  @Override
  public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName) throws IOException {

  }

  @Override
  public void preTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName) throws IOException {

  }

  @Override
  public void postTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                       TableName tableName) throws IOException {

  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
                             HTableDescriptor htd) throws IOException {

  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName, HTableDescriptor htd) throws IOException {

  }

  @Override
  public void preModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    TableName tableName, HTableDescriptor htd) throws IOException {

  }

  @Override
  public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     TableName tableName, HTableDescriptor htd) throws IOException {

  }

  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                           TableName tableName, HColumnDescriptor column) throws IOException {

  }

  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
                            HColumnDescriptor column) throws IOException {

  }

  @Override
  public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  TableName tableName, HColumnDescriptor column)
      throws IOException {

  }

  @Override
  public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                   TableName tableName, HColumnDescriptor column)
      throws IOException {

  }

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName, HColumnDescriptor descriptor)
      throws IOException {

  }

  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName, HColumnDescriptor descriptor)
      throws IOException {

  }

  @Override
  public void preModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     TableName tableName, HColumnDescriptor descriptor)
      throws IOException {

  }

  @Override
  public void postModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName, HColumnDescriptor descriptor)
      throws IOException {

  }

  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] c) throws IOException {

  }

  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] c) throws IOException {

  }

  @Override
  public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] c) throws IOException {

  }

  @Override
  public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName, byte[] c) throws IOException {

  }

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             TableName tableName) throws IOException {

  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName) throws IOException {

  }

  @Override
  public void preEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    TableName tableName) throws IOException {

  }

  @Override
  public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     TableName tableName) throws IOException {

  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName) throws IOException {

  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName) throws IOException {

  }

  @Override
  public void preDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     TableName tableName) throws IOException {

  }

  @Override
  public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName) throws IOException {

  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
                      ServerName srcServer, ServerName destServer) throws IOException {

  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
                       ServerName srcServer, ServerName destServer) throws IOException {

  }

  @Override
  public void preAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                ProcedureExecutor<MasterProcedureEnv> procEnv,
                                long procId) throws IOException {

  }

  @Override
  public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {

  }

  @Override
  public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx) throws
      IOException {

  }

  @Override
  public void postListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 List<ProcedureInfo> procInfoList) throws IOException {

  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
                        HRegionInfo regionInfo) throws IOException {

  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo) throws IOException {

  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo, boolean force) throws IOException {

  }

  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
                           HRegionInfo regionInfo, boolean force) throws IOException {

  }

  @Override
  public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               HRegionInfo regionInfo) throws IOException {

  }

  @Override
  public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                HRegionInfo regionInfo) throws IOException {

  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {

  }

  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx,
                          List<RegionPlan> plans) throws IOException {

  }

  @Override
  public boolean preSetSplitOrMergeEnabled(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                           boolean newValue, MasterSwitchType switchType) throws
      IOException {
    return false;
  }

  @Override
  public void postSetSplitOrMergeEnabled(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                         boolean newValue, MasterSwitchType switchType) throws
      IOException {

  }

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  boolean newValue) throws IOException {
    return newValue;
  }

  @Override
  public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx, boolean
      oldValue, boolean newValue) throws IOException {

  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {

  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {

  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {

  }

  @Override
  public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {

  }

  @Override
  public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                          SnapshotDescription snapshot,
                          HTableDescriptor hTableDescriptor) throws IOException {


  }

  @Override
  public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription
      snapshot, HTableDescriptor hTableDescriptor) throws IOException {

  }

  @Override
  public void preListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              SnapshotDescription snapshot) throws IOException {

  }

  @Override
  public void postListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               SnapshotDescription snapshot) throws IOException {

  }

  @Override
  public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               SnapshotDescription snapshot,
                               HTableDescriptor hTableDescriptor) throws IOException {
    assignTableToGroup(hTableDescriptor);
  }

  @Override
  public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                SnapshotDescription snapshot,
                                HTableDescriptor hTableDescriptor) throws IOException {

  }

  @Override
  public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 SnapshotDescription snapshot,
                                 HTableDescriptor hTableDescriptor) throws IOException {

  }

  @Override
  public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  SnapshotDescription snapshot,
                                  HTableDescriptor hTableDescriptor) throws IOException {

  }

  @Override
  public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                SnapshotDescription snapshot) throws IOException {

  }

  @Override
  public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 SnapshotDescription snapshot) throws IOException {

  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     List<TableName> tableNamesList,
                                     List<HTableDescriptor> descriptors) throws IOException {

  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      List<HTableDescriptor> descriptors) throws IOException {

  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     List<TableName> tableNamesList,
                                     List<HTableDescriptor> descriptors,
                                     String regex) throws IOException {

  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      List<TableName> tableNamesList,
                                      List<HTableDescriptor> descriptors,
                                      String regex) throws IOException {

  }

  @Override
  public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               List<HTableDescriptor> descriptors,
                               String regex) throws IOException {

  }

  @Override
  public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                List<HTableDescriptor> descriptors,
                                String regex) throws IOException {

  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 NamespaceDescriptor ns) throws IOException {
    String group = ns.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP);
    if(group != null && groupAdminServer.getRSGroupInfo(group) == null) {
      throw new ConstraintException("Region server group "+group+" does not exit");
    }
  }

  @Override
  public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  NamespaceDescriptor ns) throws IOException {

  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 String namespace) throws IOException {

  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  String namespace) throws IOException {

  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 NamespaceDescriptor ns) throws IOException {
    preCreateNamespace(ctx, ns);
  }

  @Override
  public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  NamespaceDescriptor ns) throws IOException {

  }

  @Override
  public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                        String namespace) throws IOException {

  }

  @Override
  public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                         NamespaceDescriptor ns) throws IOException {

  }

  @Override
  public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                          List<NamespaceDescriptor> descriptors)
      throws IOException {

  }

  @Override
  public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                           List<NamespaceDescriptor> descriptors)
      throws IOException {

  }

  @Override
  public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             TableName tableName) throws IOException {

  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
                              Quotas quotas) throws IOException {

  }

  @Override
  public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               String userName, Quotas quotas) throws IOException {

  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
                              TableName tableName, Quotas quotas) throws IOException {

  }

  @Override
  public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      userName, TableName tableName, Quotas quotas) throws IOException {

  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
                              String namespace, Quotas quotas) throws IOException {

  }

  @Override
  public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
                               String namespace, Quotas quotas) throws IOException {

  }

  @Override
  public void preSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName, Quotas quotas) throws IOException {

  }

  @Override
  public void postSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName, Quotas quotas) throws IOException {

  }

  @Override
  public void preSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                   String namespace, Quotas quotas) throws IOException {

  }

  @Override
  public void postSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    String namespace, Quotas quotas) throws IOException {

  }

  @Override
  public void preDispatchMerge(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               HRegionInfo regionA, HRegionInfo regionB) throws IOException {

  }

  @Override
  public void postDispatchMerge(ObserverContext<MasterCoprocessorEnvironment> c,
                                HRegionInfo regionA, HRegionInfo regionB) throws IOException {

  }

  @Override
  public void preGetClusterStatus(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void postGetClusterStatus(ObserverContext<MasterCoprocessorEnvironment> ctx,
      ClusterStatus status) throws IOException {
  }

  @Override
  public void preClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {

  }

  @Override
  public void postClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ServerName> servers, List<ServerName> notClearedServers)
      throws IOException {
    Set<Address> clearedServer = Sets.newHashSet();
    for (ServerName server: servers) {
      if (!notClearedServers.contains(server)) {
        clearedServer.add(server.getAddress());
      }
    }
    groupAdminServer.removeServers(clearedServer);
  }

  @Override
  public void preMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             Set<Address> servers, String targetGroup) throws IOException {

  }

  @Override
  public void postMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              Set<Address> servers, String targetGroup) throws IOException {

  }

  @Override
  public void preMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName>
      tables, String targetGroup) throws IOException {

  }

  @Override
  public void postMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             Set<TableName> tables, String targetGroup) throws IOException {

  }

  @Override
  public void preMoveServersAndTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
  }

  @Override
  public void postMoveServersAndTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
  }

  @Override
  public void preRemoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers) throws IOException {
  }

  @Override
  public void postRemoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers) throws IOException {
  }

  @Override
  public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                            String name) throws IOException {

  }

  @Override
  public void postAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             String name) throws IOException {

  }

  @Override
  public void preRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               String name) throws IOException {

  }

  @Override
  public void postRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                String name) throws IOException {

  }

  @Override
  public void preBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                String groupName) throws IOException {

  }

  @Override
  public void postBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 String groupName, boolean balancerRan) throws IOException {

  }

  public void checkPermission(String request) throws IOException {
    accessChecker.requirePermission(getActiveUser(), request, Permission.Action.ADMIN);
  }

  /**
   * Returns the active user to which authorization checks should be applied.
   * If we are in the context of an RPC call, the remote user is used,
   * otherwise the currently logged in user is used.
   */
  private User getActiveUser() throws IOException {
    User user = RpcServer.getRequestUser();
    if (user == null) {
      // for non-rpc handling, fallback to system user
      user = userProvider.getCurrent();
    }
    return user;
  }
}
