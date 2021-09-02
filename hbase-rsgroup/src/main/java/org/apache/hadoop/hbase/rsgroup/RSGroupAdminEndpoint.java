/*
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
import com.google.protobuf.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
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
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RSGroupAdminService;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveServersResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RenameRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RenameRSGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.UpdateRSGroupConfigRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.UpdateRSGroupConfigResponse;
import org.apache.hadoop.hbase.protobuf.generated.TableProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Encapsulate MasterObserver functions into separate subclass.
@CoreCoprocessor
@InterfaceAudience.Private
public class RSGroupAdminEndpoint implements MasterCoprocessor, MasterObserver {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupAdminEndpoint.class);

  private MasterServices master = null;
  // Only instance of RSGroupInfoManager. RSGroup aware load balancers ask for this instance on
  // their setup.
  private RSGroupInfoManager groupInfoManager;
  private RSGroupAdminServer groupAdminServer;
  private final RSGroupAdminService groupAdminService = new RSGroupAdminServiceImpl();
  private AccessChecker accessChecker;

  /** Provider for mapping principal names to Users */
  private UserProvider userProvider;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (!(env instanceof HasMasterServices)) {
      throw new IOException("Does not implement HMasterServices");
    }

    master = ((HasMasterServices)env).getMasterServices();
    groupInfoManager = RSGroupInfoManagerImpl.getInstance(master);
    groupAdminServer = new RSGroupAdminServer(master, groupInfoManager);
    Class<?> clazz =
        master.getConfiguration().getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, null);
    if (!RSGroupableBalancer.class.isAssignableFrom(clazz)) {
      throw new IOException("Configured balancer does not support RegionServer groups.");
    }
    accessChecker = ((HasMasterServices) env).getMasterServices().getAccessChecker();

    // set the user-provider.
    this.userProvider = UserProvider.instantiate(env.getConfiguration());
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(groupAdminService);
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  RSGroupInfoManager getGroupInfoManager() {
    return groupInfoManager;
  }

  RSGroupAdminServer getGroupAdminServer() {
    return groupAdminServer;
  }
  /**
   * Implementation of RSGroupAdminService defined in RSGroupAdmin.proto.
   * This class calls {@link RSGroupAdminServer} for actual work, converts result to protocol
   * buffer response, handles exceptions if any occurred and then calls the {@code RpcCallback} with
   * the response.
   */
  private class RSGroupAdminServiceImpl extends RSGroupAdminProtos.RSGroupAdminService {
    @Override
    public void getRSGroupInfo(RpcController controller,
        GetRSGroupInfoRequest request, RpcCallback<GetRSGroupInfoResponse> done) {
      GetRSGroupInfoResponse.Builder builder = GetRSGroupInfoResponse.newBuilder();
      String groupName = request.getRSGroupName();
      LOG.info(master.getClientIdAuditPrefix() + " initiates rsgroup info retrieval, group="
              + groupName);
      try {
        checkPermission("getRSGroupInfo");
        RSGroupInfo rsGroupInfo = groupAdminServer.getRSGroupInfo(groupName);
        if (rsGroupInfo != null) {
          builder.setRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(rsGroupInfo));
        }
      } catch (IOException e) {
        CoprocessorRpcUtils.setControllerException(controller, e);
      }
      done.run(builder.build());
    }

    @Override
    public void getRSGroupInfoOfTable(RpcController controller,
        GetRSGroupInfoOfTableRequest request, RpcCallback<GetRSGroupInfoOfTableResponse> done) {
      GetRSGroupInfoOfTableResponse.Builder builder = GetRSGroupInfoOfTableResponse.newBuilder();
      TableName tableName = ProtobufUtil.toTableName(request.getTableName());
      LOG.info(master.getClientIdAuditPrefix() + " initiates rsgroup info retrieval, table="
          + tableName);
      try {
        checkPermission("getRSGroupInfoOfTable");
        RSGroupInfo RSGroupInfo = groupAdminServer.getRSGroupInfoOfTable(tableName);
        if (RSGroupInfo != null) {
          builder.setRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo));
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
      LOG.info(master.getClientIdAuditPrefix() + " move servers " + hostPorts +" to rsgroup "
          + request.getTargetGroup());
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
      for (TableProtos.TableName tableName : request.getTableNameList()) {
        tables.add(ProtobufUtil.toTableName(tableName));
      }
      LOG.info(master.getClientIdAuditPrefix() + " move tables " + tables +" to rsgroup "
          + request.getTargetGroup());
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
      LOG.info(master.getClientIdAuditPrefix() + " add rsgroup " + request.getRSGroupName());
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
    public void removeRSGroup(RpcController controller,
        RemoveRSGroupRequest request, RpcCallback<RemoveRSGroupResponse> done) {
      RemoveRSGroupResponse.Builder builder =
          RemoveRSGroupResponse.newBuilder();
      LOG.info(master.getClientIdAuditPrefix() + " remove rsgroup " + request.getRSGroupName());
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
    public void balanceRSGroup(RpcController controller,
        BalanceRSGroupRequest request, RpcCallback<BalanceRSGroupResponse> done) {
      BalanceRSGroupResponse.Builder builder = BalanceRSGroupResponse.newBuilder();
      LOG.info(master.getClientIdAuditPrefix() + " balance rsgroup, group="
          + request.getRSGroupName());
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
        CoprocessorRpcUtils.setControllerException(controller, e);
        builder.setBalanceRan(false);
      }
      done.run(builder.build());
    }

    @Override
    public void listRSGroupInfos(RpcController controller,
        ListRSGroupInfosRequest request, RpcCallback<ListRSGroupInfosResponse> done) {
      ListRSGroupInfosResponse.Builder builder = ListRSGroupInfosResponse.newBuilder();
      LOG.info(master.getClientIdAuditPrefix() + " list rsgroup");
      try {
        checkPermission("listRSGroup");
        for (RSGroupInfo RSGroupInfo : groupAdminServer.listRSGroups()) {
          builder.addRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(RSGroupInfo));
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
      Address hp = Address.fromParts(request.getServer().getHostName(),
          request.getServer().getPort());
      LOG.info(master.getClientIdAuditPrefix() + " initiates rsgroup info retrieval, server="
          + hp);
      try {
        checkPermission("getRSGroupInfoOfServer");
        RSGroupInfo info = groupAdminServer.getRSGroupOfServer(hp);
        if (info != null) {
          builder.setRSGroupInfo(RSGroupProtobufUtil.toProtoGroupInfo(info));
        }
      } catch (IOException e) {
        CoprocessorRpcUtils.setControllerException(controller, e);
      }
      done.run(builder.build());
    }

    @Override
    public void moveServersAndTables(RpcController controller,
        MoveServersAndTablesRequest request, RpcCallback<MoveServersAndTablesResponse> done) {
      MoveServersAndTablesResponse.Builder builder = MoveServersAndTablesResponse.newBuilder();
      Set<Address> hostPorts = Sets.newHashSet();
      for (HBaseProtos.ServerName el : request.getServersList()) {
        hostPorts.add(Address.fromParts(el.getHostName(), el.getPort()));
      }
      Set<TableName> tables = new HashSet<>(request.getTableNameList().size());
      for (TableProtos.TableName tableName : request.getTableNameList()) {
        tables.add(ProtobufUtil.toTableName(tableName));
      }
      LOG.info(master.getClientIdAuditPrefix() + " move servers " + hostPorts
          + " and tables " + tables + " to rsgroup" + request.getTargetGroup());
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
    public void removeServers(RpcController controller,
        RemoveServersRequest request,
        RpcCallback<RemoveServersResponse> done) {
      RemoveServersResponse.Builder builder =
          RemoveServersResponse.newBuilder();
      Set<Address> servers = Sets.newHashSet();
      for (HBaseProtos.ServerName el : request.getServersList()) {
        servers.add(Address.fromParts(el.getHostName(), el.getPort()));
      }
      LOG.info(master.getClientIdAuditPrefix()
          + " remove decommissioned servers from rsgroup: " + servers);
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

    @Override
    public void renameRSGroup(RpcController controller,
                              RenameRSGroupRequest request,
                              RpcCallback<RenameRSGroupResponse> done) {
      String oldRSGroup = request.getOldRsgroupName();
      String newRSGroup = request.getNewRsgroupName();
      LOG.info("{} rename rsgroup from {} to {}",
        master.getClientIdAuditPrefix(), oldRSGroup, newRSGroup);

      RenameRSGroupResponse.Builder builder = RenameRSGroupResponse.newBuilder();
      try {
        if (master.getMasterCoprocessorHost() != null) {
          master.getMasterCoprocessorHost().preRenameRSGroup(oldRSGroup, newRSGroup);
        }
        checkPermission("renameRSGroup");
        groupAdminServer.renameRSGroup(oldRSGroup, newRSGroup);
        if (master.getMasterCoprocessorHost() != null) {
          master.getMasterCoprocessorHost().postRenameRSGroup(oldRSGroup, newRSGroup);
        }
      } catch (IOException e) {
        CoprocessorRpcUtils.setControllerException(controller, e);
      }
      done.run(builder.build());
    }

    @Override
    public void updateRSGroupConfig(RpcController controller, UpdateRSGroupConfigRequest request,
                                    RpcCallback<UpdateRSGroupConfigResponse> done) {
      UpdateRSGroupConfigResponse.Builder builder = UpdateRSGroupConfigResponse.newBuilder();
      String groupName = request.getGroupName();
      Map<String, String> configuration = Maps.newHashMap();
      request.getConfigurationList().forEach(p -> configuration.put(p.getName(), p.getValue()));
      LOG.info("{} update rsgroup {} configuration {}", master.getClientIdAuditPrefix(), groupName,
          configuration);
      try {
        if (master.getMasterCoprocessorHost() != null) {
          master.getMasterCoprocessorHost().preUpdateRSGroupConfig(groupName, configuration);
        }
        groupAdminServer.updateRSGroupConfig(groupName, configuration);
        if (master.getMasterCoprocessorHost() != null) {
          master.getMasterCoprocessorHost().postUpdateRSGroupConfig(groupName, configuration);
        }
      } catch (IOException e) {
        CoprocessorRpcUtils.setControllerException(controller, e);
      }
      done.run(builder.build());
    }
  }

  boolean rsgroupHasServersOnline(TableDescriptor desc) throws IOException {
    String groupName;
    try {
      groupName =
        master.getClusterSchema().getNamespace(desc.getTableName().getNamespaceAsString())
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
      throw new ConstraintException(
          "Default RSGroup (" + groupName + ") for this table's " + "namespace does not exist.");
    }

    for (ServerName onlineServer : master.getServerManager().createDestinationServersList()) {
      if (rsGroupInfo.getServers().contains(onlineServer.getAddress())) {
        return true;
      }
    }
    return false;
  }

  void assignTableToGroup(TableDescriptor desc) throws IOException {
    RSGroupInfo rsGroupInfo = determineRSGroupInfoForTable(desc);
    if (rsGroupInfo == null) {
      throw new ConstraintException("Default RSGroup for this table " + desc.getTableName()
        + " does not exist.");
    }
    if (!rsGroupInfo.containsTable(desc.getTableName())) {
      LOG.debug("Pre-moving table " + desc.getTableName() + " to RSGroup " + rsGroupInfo.getName());
      groupAdminServer.moveTables(Sets.newHashSet(desc.getTableName()), rsGroupInfo.getName());
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // MasterObserver overrides
  /////////////////////////////////////////////////////////////////////////////

  @Override
  public void preCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableDescriptor desc,
      final RegionInfo[] regions) throws IOException {
    if (desc.getTableName().isSystemTable()) {
      return;
    }
    moveTableToValidRSGroup(desc);
  }

  @Override
  public void preModifyTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
    throws IOException {
    // If table's rsgroup is changed, it must be valid
    if (!currentDescriptor.getRegionServerGroup().equals(newDescriptor.getRegionServerGroup())) {
      RSGroupInfo rsGroupInfo = determineRSGroupInfoForTable(newDescriptor);
      validateRSGroup(newDescriptor, rsGroupInfo);
    }
  }

  @Override
  public void postCompletedModifyTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, TableDescriptor oldDescriptor, TableDescriptor currentDescriptor)
    throws IOException {
    // If table's rsgroup is changed, move table into the rsgroup.
    if (!oldDescriptor.getRegionServerGroup().equals(currentDescriptor.getRegionServerGroup())) {
      RSGroupInfo rsGroupInfo = determineRSGroupInfoForTable(currentDescriptor);
      moveTableToRSGroup(currentDescriptor, rsGroupInfo);
    }
  }

  // Determine and validate rs group then move table to this valid rs group.
  private void moveTableToValidRSGroup(TableDescriptor desc) throws IOException {
    RSGroupInfo rsGroupInfo = determineRSGroupInfoForTable(desc);
    validateRSGroup(desc, rsGroupInfo);
    moveTableToRSGroup(desc, rsGroupInfo);
  }

  private void validateRSGroup(TableDescriptor desc, RSGroupInfo rsGroupInfo) throws IOException {
    if (rsGroupInfo == null) {
      throw new ConstraintException(
        "Default RSGroup for this table " + desc.getTableName() + " does not exist.");
    }
    if (!RSGroupUtil.rsGroupHasOnlineServer(master, rsGroupInfo)) {
      throw new HBaseIOException(
        "No online servers in the rsgroup " + rsGroupInfo.getName() + " which table " + desc
          .getTableName().getNameAsString() + " belongs to");
    }
  }

  private void moveTableToRSGroup(final TableDescriptor desc, RSGroupInfo rsGroupInfo)
    throws IOException {
    // In case of modify table, when rs group is not changed, move is not required.
    if (!rsGroupInfo.containsTable(desc.getTableName())) {
      synchronized (groupInfoManager) {
        groupInfoManager
          .moveTables(Collections.singleton(desc.getTableName()), rsGroupInfo.getName());
      }
    }
  }

  private RSGroupInfo determineRSGroupInfoForTable(final TableDescriptor desc) throws IOException {
    Optional<String> optGroupNameOfTable = desc.getRegionServerGroup();
    if (optGroupNameOfTable.isPresent()) {
      final RSGroupInfo rsGroup = groupInfoManager.getRSGroup(optGroupNameOfTable.get());
      if (rsGroup == null) {
        // When rs group is set in table descriptor then it must exist
        throw new ConstraintException(
            "Region server group " + optGroupNameOfTable.get() + " does not exist.");
      } else {
        return rsGroup;
      }
    }
    return groupInfoManager.determineRSGroupInfoForTable(desc.getTableName());
  }
  // Remove table from its RSGroup.
  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName) throws IOException {
    try {
      RSGroupInfo group = groupAdminServer.getRSGroupInfoOfTable(tableName);
      if (group != null) {
        LOG.debug(String.format("Removing deleted table '%s' from rsgroup '%s'", tableName,
            group.getName()));
        groupAdminServer.moveTables(Sets.newHashSet(tableName), null);
      }
    } catch (IOException ex) {
      LOG.debug("Failed to perform RSGroup information cleanup for table: " + tableName, ex);
    }
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 NamespaceDescriptor ns) throws IOException {
    String group = ns.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP);
    if(group != null && groupAdminServer.getRSGroupInfo(group) == null) {
      throw new ConstraintException("Region server group " + group + " does not exist.");
    }
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
    preCreateNamespace(ctx, ns);
  }

  @Override
  public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, TableDescriptor desc) throws IOException {
    assignTableToGroup(desc);
  }

  @Override
  public void postClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ServerName> servers, List<ServerName> notClearedServers)
      throws IOException {
    Set<Address> clearedServer = servers.stream().
        filter(server -> !notClearedServers.contains(server)).
        map(ServerName::getAddress).
        collect(Collectors.toSet());
    if(!clearedServer.isEmpty()) {
      groupAdminServer.removeServers(clearedServer);
    }
  }

  public void checkPermission(String request) throws IOException {
    accessChecker.requirePermission(getActiveUser(), request, null, Action.ADMIN);
  }

  /**
   * Returns the active user to which authorization checks should be applied.
   * If we are in the context of an RPC call, the remote user is used,
   * otherwise the currently logged in user is used.
   */
  private User getActiveUser() throws IOException {
    // for non-rpc handling, fallback to system user
    Optional<User> optionalUser = RpcServer.getRequestUser();
    if (optionalUser.isPresent()) {
      return optionalUser.get();
    }
    return userProvider.getCurrent();
  }
}
