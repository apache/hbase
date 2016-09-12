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
import com.google.common.net.HostAndPort;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
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
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServersResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveTablesRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveTablesResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RSGroupAdminService;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupResponse;


public class RSGroupAdminEndpoint extends RSGroupAdminService
    implements CoprocessorService, Coprocessor, MasterObserver {

  private static final Log LOG = LogFactory.getLog(RSGroupAdminEndpoint.class);
  private MasterServices master = null;

  private static RSGroupInfoManagerImpl groupInfoManager;
  private RSGroupAdminServer groupAdminServer;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    MasterCoprocessorEnvironment menv = (MasterCoprocessorEnvironment)env;
    master = menv.getMasterServices();
    setGroupInfoManager(new RSGroupInfoManagerImpl(master));
    groupAdminServer = new RSGroupAdminServer(master, groupInfoManager);
    Class clazz =
        master.getConfiguration().getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, null);
    if (!RSGroupableBalancer.class.isAssignableFrom(clazz)) {
      throw new IOException("Configured balancer is not a GroupableBalancer");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
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
      if(RSGroupInfo != null) {
        builder.setRSGroupInfo(ProtobufUtil.toProtoGroupInfo(RSGroupInfo));
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
      RSGroupInfo RSGroupInfo = groupAdminServer.getRSGroupInfoOfTable(tableName);
      if (RSGroupInfo == null) {
        response = builder.build();
      } else {
        response = builder.setRSGroupInfo(ProtobufUtil.toProtoGroupInfo(RSGroupInfo)).build();
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
      Set<HostAndPort> hostPorts = Sets.newHashSet();
      for(HBaseProtos.ServerName el: request.getServersList()) {
        hostPorts.add(HostAndPort.fromParts(el.getHostName(), el.getPort()));
      }
      groupAdminServer.moveServers(hostPorts, request.getTargetGroup());
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
      for(HBaseProtos.TableName tableName: request.getTableNameList()) {
        tables.add(ProtobufUtil.toTableName(tableName));
      }
      groupAdminServer.moveTables(tables, request.getTargetGroup());
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void addRSGroup(RpcController controller,
                       AddRSGroupRequest request,
                       RpcCallback<AddRSGroupResponse> done) {
    AddRSGroupResponse response = null;
    try {
      AddRSGroupResponse.Builder builder =
          AddRSGroupResponse.newBuilder();
      groupAdminServer.addRSGroup(request.getRSGroupName());
      response = builder.build();
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
      groupAdminServer.removeRSGroup(request.getRSGroupName());
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
      builder.setBalanceRan(groupAdminServer.balanceRSGroup(request.getRSGroupName()));
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
      for(RSGroupInfo RSGroupInfo : groupAdminServer.listRSGroups()) {
        builder.addRSGroupInfo(ProtobufUtil.toProtoGroupInfo(RSGroupInfo));
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
      HostAndPort hp =
          HostAndPort.fromParts(request.getServer().getHostName(), request.getServer().getPort());
      RSGroupInfo RSGroupInfo = groupAdminServer.getRSGroupOfServer(hp);
      if (RSGroupInfo != null) {
        builder.setRSGroupInfo(ProtobufUtil.toProtoGroupInfo(RSGroupInfo));
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(builder.build());
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    groupAdminServer.prepareRSGroupForTable(desc);
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName) throws IOException {
    groupAdminServer.cleanupRSGroupForTable(tableName);
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 NamespaceDescriptor ns) throws IOException {
    String group = ns.getConfigurationValue(RSGroupInfo.NAMESPACEDESC_PROP_GROUP);
    if(group != null && groupAdminServer.getRSGroupInfo(group) == null) {
      throw new ConstraintException("Region server group "+group+" does not exit");
    }
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 NamespaceDescriptor ns) throws IOException {
    preCreateNamespace(ctx, ns);
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              HTableDescriptor desc,
                              HRegionInfo[] regions) throws IOException {
  }

  @Deprecated
  @Override
  public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    HTableDescriptor desc,
                                    HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HTableDescriptor desc,
      final HRegionInfo[] regions) throws IOException {
  }

  @Deprecated
  @Override
  public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     HTableDescriptor desc,
                                     HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void postCompletedCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HTableDescriptor desc,
      final HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             TableName tableName) throws IOException {
  }

  @Deprecated
  @Override
  public void preDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    TableName tableName) throws IOException {
  }

  @Override
  public void preDeleteTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {
  }

  @Deprecated
  @Override
  public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     TableName tableName) throws IOException {
  }

  @Override
  public void postCompletedDeleteTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {
  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName) throws IOException {
  }

  @Override
  public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName) throws IOException {
  }

  @Deprecated
  @Override
  public void preTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName) throws IOException {
  }

  @Override
  public void preTruncateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {
  }

  @Deprecated
  @Override
  public void postTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                       TableName tableName) throws IOException {
  }

  @Override
  public void postCompletedTruncateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             TableName tableName,
                             HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName,
                              HTableDescriptor htd) throws IOException {
  }

  @Deprecated
  @Override
  public void preModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    TableName tableName,
                                    HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preModifyTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HTableDescriptor htd) throws IOException {
  }

  @Deprecated
  @Override
  public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     TableName tableName,
                                     HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postCompletedModifyTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                           TableName tableName,
                           HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preAddColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 TableName tableName,
                                 HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                            TableName tableName,
                            HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postAddColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  TableName tableName,
                                  HColumnDescriptor columnFamily) throws IOException {
  }

  @Deprecated
  @Override
  public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  TableName tableName,
                                  HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preAddColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {
  }

  @Deprecated
  @Override
  public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                   TableName tableName,
                                   HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postCompletedAddColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName,
                              HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preModifyColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    TableName tableName,
                                    HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName,
                               HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postModifyColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, HColumnDescriptor columnFamily) throws IOException {

  }

  @Deprecated
  @Override
  public void preModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, HColumnDescriptor columnFamily) throws IOException {

  }

  @Override
  public void preModifyColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily)
      throws IOException {

  }

  @Deprecated
  @Override
  public void postModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName, HColumnDescriptor columnFamily) throws
      IOException {

  }

  @Override
  public void postCompletedModifyColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final HColumnDescriptor columnFamily)
      throws IOException {

  }

  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void preDeleteColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void postDeleteColumnFamily(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] columnFamily) throws IOException {

  }

  @Deprecated
  @Override
  public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void preDeleteColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final byte[] columnFamily) throws
      IOException {

  }

  @Deprecated
  @Override
  public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void postCompletedDeleteColumnFamilyAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final byte[] columnFamily) throws
      IOException {

  }

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Deprecated
  @Override
  public void preEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void preEnableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {

  }

  @Deprecated
  @Override
  public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void postCompletedEnableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {

  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Deprecated
  @Override
  public void preDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void preDisableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {

  }

  @Deprecated
  @Override
  public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName) throws IOException {

  }

  @Override
  public void postCompletedDisableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {

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
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo) throws IOException {

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
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo, boolean force) throws IOException {

  }

  @Override
  public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo) throws IOException {

  }

  @Override
  public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo) throws IOException {

  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {

  }

  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan>
      plans) throws IOException {

  }

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx, boolean
      newValue) throws IOException {
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
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws
      IOException {

  }

  @Override
  public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> ctx) throws
      IOException {

  }

  @Override
  public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription
      snapshot, HTableDescriptor hTableDescriptor) throws IOException {

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
                               SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {

  }

  @Override
  public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {

  }

  @Override
  public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {

  }

  @Override
  public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  SnapshotDescription snapshot, HTableDescriptor
      hTableDescriptor) throws IOException {

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
                                     List<TableName> tableNamesList, List<HTableDescriptor>
      descriptors, String regex) throws IOException {

  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      List<TableName> tableNamesList, List<HTableDescriptor>
      descriptors, String regex) throws IOException {

  }

  @Override
  public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               List<HTableDescriptor> descriptors, String regex) throws
      IOException {

  }

  @Override
  public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                List<HTableDescriptor> descriptors, String regex) throws
      IOException {

  }

  @Override
  public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  NamespaceDescriptor ns) throws IOException {

  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      namespace) throws IOException {

  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      namespace) throws IOException {

  }

  @Override
  public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  NamespaceDescriptor ns) throws IOException {

  }

  @Override
  public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      namespace) throws IOException {

  }

  @Override
  public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                         NamespaceDescriptor ns) throws IOException {

  }

  @Override
  public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                          List<NamespaceDescriptor> descriptors) throws
      IOException {

  }

  @Override
  public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                           List<NamespaceDescriptor> descriptors) throws
      IOException {

  }

  @Override
  public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public boolean preSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {
    return false;
  }

  @Override
  public void postSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {
  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
                              Quotas quotas) throws IOException {

  }

  @Override
  public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      userName, Quotas quotas) throws IOException {

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
  public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      userName, String namespace, Quotas quotas) throws IOException {

  }

  @Override
  public void preSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, Quotas quotas) throws IOException {

  }

  @Override
  public void postSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, Quotas quotas) throws IOException {

  }

  @Override
  public void preSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      namespace, Quotas quotas) throws IOException {

  }

  @Override
  public void postSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      namespace, Quotas quotas) throws IOException {
  }

  @Override
  public void preDispatchMerge(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionA, HRegionInfo regionB) throws IOException {
  }

  @Override
  public void postDispatchMerge(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo
      regionA, HRegionInfo regionB) throws IOException {
  }

  @Override
  public void preMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<HostAndPort>
      servers, String targetGroup) throws IOException {
  }

  @Override
  public void postMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<HostAndPort>
      servers, String targetGroup) throws IOException {
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
  public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void postAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void preRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void postRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void preBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName)
      throws IOException {
  }

  @Override
  public void postBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 String groupName, boolean balancerRan) throws IOException {
  }

  @Override
  public void preAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx,
      ProcedureExecutor<MasterProcedureEnv> procEnv, long procId) throws IOException {
  }

  @Override
  public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void postListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ProcedureInfo> procInfoList) throws IOException {
  }
}
