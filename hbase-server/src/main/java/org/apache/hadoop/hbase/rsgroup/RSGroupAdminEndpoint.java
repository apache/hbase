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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;

// TODO: Encapsulate MasterObserver functions into separate subclass.
@CoreCoprocessor
@InterfaceAudience.Private
public class RSGroupAdminEndpoint implements MasterCoprocessor, MasterObserver {
  // Only instance of RSGroupInfoManager. RSGroup aware load balancers ask for this instance on
  // their setup.
  private MasterServices master;
  private RSGroupAdminServiceImpl groupAdminService = new RSGroupAdminServiceImpl();
  private AccessChecker accessChecker;
  private UserProvider userProvider;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (!(env instanceof HasMasterServices)) {
      throw new IOException("Does not implement HMasterServices");
    }

    master = ((HasMasterServices) env).getMasterServices();
    Class<?> clazz =
      master.getConfiguration().getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, null);
    if (!RSGroupableBalancer.class.isAssignableFrom(clazz)) {
      throw new IOException("Configured balancer does not support RegionServer groups.");
    }
    accessChecker = master.getAccessChecker();
    userProvider = UserProvider.instantiate(env.getConfiguration());
    groupAdminService.initialize(master);
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
  public void stop(CoprocessorEnvironment env) {
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(groupAdminService);
  }

  RSGroupInfoManager getGroupInfoManager() {
    return master.getRSRSGroupInfoManager();
  }

  /////////////////////////////////////////////////////////////////////////////
  // MasterObserver overrides
  /////////////////////////////////////////////////////////////////////////////

  @Override
  public void postClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
    List<ServerName> servers, List<ServerName> notClearedServers) throws IOException {
    Set<Address> clearedServer =
      servers.stream().filter(server -> !notClearedServers.contains(server))
        .map(ServerName::getAddress).collect(Collectors.toSet());
    if (!clearedServer.isEmpty()) {
      master.getRSRSGroupInfoManager().removeServers(clearedServer);
    }
  }

  private RSGroupInfo checkGroupExists(Optional<String> optGroupName, Supplier<String> forWhom)
    throws IOException {
    if (optGroupName.isPresent()) {
      String groupName = optGroupName.get();
      RSGroupInfo group = master.getRSRSGroupInfoManager().getRSGroup(groupName);
      if (group == null) {
        throw new ConstraintException(
          "Region server group " + groupName + " for " + forWhom.get() + " does not exit");
      }
      return group;
    }
    return null;
  }

  private Optional<String> getNamespaceGroup(NamespaceDescriptor namespaceDesc) {
    return Optional
      .ofNullable(namespaceDesc.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP));
  }

  // Do not allow creating new tables/namespaces which has an empty rs group, expect the default rs
  // group. Notice that we do not check for online servers, as this is not stable because region
  // servers can die at any time.
  private void checkGroupNotEmpty(RSGroupInfo rsGroupInfo, Supplier<String> forWhom)
    throws ConstraintException {
    if (rsGroupInfo == null || rsGroupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
      // we do not have a rs group config or we explicitly set the rs group to default, then no need
      // to check.
      return;
    }
    if (rsGroupInfo.getServers().isEmpty()) {
      throw new ConstraintException(
        "No servers in the rsgroup " + rsGroupInfo.getName() + " for " + forWhom.get());
    }
  }

  @Override
  public void preCreateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableDescriptor desc, RegionInfo[] regions) throws IOException {
    if (desc.getTableName().isSystemTable()) {
      // do not check for system tables as we may block the bootstrap.
      return;
    }
    Supplier<String> forWhom = () -> "table " + desc.getTableName();
    RSGroupInfo rsGroupInfo = checkGroupExists(desc.getRegionServerGroup(), forWhom);
    if (rsGroupInfo == null) {
      // we do not set rs group info on table, check if we have one on namespace
      String namespace = desc.getTableName().getNamespaceAsString();
      NamespaceDescriptor nd = master.getClusterSchema().getNamespace(namespace);
      forWhom = () -> "table " + desc.getTableName() + "(inherit from namespace)";
      rsGroupInfo = checkGroupExists(getNamespaceGroup(nd), forWhom);
    }
    checkGroupNotEmpty(rsGroupInfo, forWhom);
  }

  @Override
  public TableDescriptor preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
    throws IOException {
    if (!currentDescriptor.getRegionServerGroup().equals(newDescriptor.getRegionServerGroup())) {
      Supplier<String> forWhom = () -> "table " + newDescriptor.getTableName();
      RSGroupInfo rsGroupInfo = checkGroupExists(newDescriptor.getRegionServerGroup(), forWhom);
      checkGroupNotEmpty(rsGroupInfo, forWhom);
    }
    return MasterObserver.super.preModifyTable(ctx, tableName, currentDescriptor, newDescriptor);
  }

  private void checkNamespaceGroup(NamespaceDescriptor nd) throws IOException {
    Supplier<String> forWhom = () -> "namespace " + nd.getName();
    RSGroupInfo rsGroupInfo = checkGroupExists(getNamespaceGroup(nd), forWhom);
    checkGroupNotEmpty(rsGroupInfo, forWhom);
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    NamespaceDescriptor ns) throws IOException {
    checkNamespaceGroup(ns);
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    NamespaceDescriptor currentNsDescriptor, NamespaceDescriptor newNsDescriptor)
    throws IOException {
    if (!Objects.equals(
      currentNsDescriptor.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP),
      newNsDescriptor.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP))) {
      checkNamespaceGroup(newNsDescriptor);
    }
  }

  @Override
  public void preMoveServersAndTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "moveServersAndTables",
        null, Permission.Action.ADMIN);
    try (Admin admin = ctx.getEnvironment().getConnection().getAdmin()) {
      for (TableName tableName : tables) {
        // Skip checks for a table that does not exist
        if (!admin.tableExists(tableName)) {
          throw new TableNotFoundException(tableName);
        }
      }
    }
  }

  @Override
  public void preMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers, String targetGroup) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "moveServers",
        null, Permission.Action.ADMIN);
  }

  @Override
  public void preMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<TableName> tables, String targetGroup) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "moveTables",
        null, Permission.Action.ADMIN);
    try (Admin admin = ctx.getEnvironment().getConnection().getAdmin()) {
      for (TableName tableName : tables) {
        // Skip checks for a table that does not exist
        if (!admin.tableExists(tableName)) {
          throw new TableNotFoundException(tableName);
        }
      }
    }
  }

  @Override
  public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String name) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "addRSGroup",
        null, Permission.Action.ADMIN);
  }

  @Override
  public void preRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String name) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "removeRSGroup",
        null, Permission.Action.ADMIN);
  }

  @Override
  public void preBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String groupName) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "balanceRSGroup",
        null, Permission.Action.ADMIN);
  }

  @Override
  public void preRemoveServers(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "removeServers",
        null, Permission.Action.ADMIN);
  }

  @Override
  public void preGetRSGroupInfo(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String groupName) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "getRSGroupInfo",
        null, Permission.Action.ADMIN);
  }

  @Override
  public void preGetRSGroupInfoOfTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "getRSGroupInfoOfTable",
        null, Permission.Action.ADMIN);
    //todo: should add check for table existence
  }

  @Override
  public void preListRSGroups(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    accessChecker.requirePermission(getActiveUser(), "listRSGroups",
        null, Permission.Action.ADMIN);
  }

  @Override
  public void preGetRSGroupInfoOfServer(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Address server) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "getRSGroupInfoOfServer",
        null, Permission.Action.ADMIN);
  }

  @Override
  public void preSetRSGroupForTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<TableName> tables, String groupName) throws IOException {
    accessChecker.requirePermission(getActiveUser(), "setRSGroupForTables",
        null, Permission.Action.ADMIN);
    try (Admin admin = ctx.getEnvironment().getConnection().getAdmin()) {
      for (TableName tableName : tables) {
        // Skip checks for a table that does not exist
        if (!admin.tableExists(tableName)) {
          throw new TableNotFoundException(tableName);
        }
      }
    }
  }
}
