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

import com.google.protobuf.Service;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
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
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

// TODO: Encapsulate MasterObserver functions into separate subclass.
@CoreCoprocessor
@InterfaceAudience.Private
public class RSGroupAdminEndpoint implements MasterCoprocessor, MasterObserver {
  static final Logger LOG = LoggerFactory.getLogger(RSGroupAdminEndpoint.class);

  private MasterServices master;
  // Only instance of RSGroupInfoManager. RSGroup aware load balancers ask for this instance on
  // their setup.
  private RSGroupInfoManager groupInfoManager;
  private RSGroupAdminServer groupAdminServer;
  private RSGroupAdminServiceImpl groupAdminService = new RSGroupAdminServiceImpl();

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (!(env instanceof HasMasterServices)) {
      throw new IOException("Does not implement HMasterServices");
    }

    master = ((HasMasterServices) env).getMasterServices();
    groupInfoManager = RSGroupInfoManagerImpl.getInstance(master);
    groupAdminServer = new RSGroupAdminServer(master, groupInfoManager);
    Class<?> clazz =
        master.getConfiguration().getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, null);
    if (!RSGroupableBalancer.class.isAssignableFrom(clazz)) {
      throw new IOException("Configured balancer does not support RegionServer groups.");
    }
    AccessChecker accessChecker = ((HasMasterServices) env).getMasterServices().getAccessChecker();

    // set the user-provider.
    UserProvider userProvider = UserProvider.instantiate(env.getConfiguration());
    groupAdminService.initialize(master, groupAdminServer, accessChecker, userProvider);
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

  @VisibleForTesting
  RSGroupAdminServiceImpl getGroupAdminService() {
    return groupAdminService;
  }

  private void assignTableToGroup(TableDescriptor desc) throws IOException {
    String groupName =
        master.getClusterSchema().getNamespace(desc.getTableName().getNamespaceAsString())
            .getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP);
    if (groupName == null) {
      groupName = RSGroupInfo.DEFAULT_GROUP;
    }
    RSGroupInfo rsGroupInfo = groupAdminServer.getRSGroupInfo(groupName);
    if (rsGroupInfo == null) {
      throw new ConstraintException(
          "Default RSGroup (" + groupName + ") for this table's namespace does not exist.");
    }
    if (!rsGroupInfo.containsTable(desc.getTableName())) {
      LOG.debug("Pre-moving table " + desc.getTableName() + " to RSGroup " + groupName);
      groupAdminServer.moveTables(Sets.newHashSet(desc.getTableName()), groupName);
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // MasterObserver overrides
  /////////////////////////////////////////////////////////////////////////////

  private boolean rsgroupHasServersOnline(TableDescriptor desc) throws IOException {
    String groupName;
    try {
      groupName = master.getClusterSchema().getNamespace(desc.getTableName().getNamespaceAsString())
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

  @Override
  public void preCreateTableAction(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableDescriptor desc, final RegionInfo[] regions) throws IOException {
    if (!desc.getTableName().isSystemTable() && !rsgroupHasServersOnline(desc)) {
      throw new HBaseIOException("No online servers in the rsgroup, which table " +
          desc.getTableName().getNameAsString() + " belongs to");
    }
  }

  // Assign table to default RSGroup.
  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableDescriptor desc, RegionInfo[] regions) throws IOException {
    assignTableToGroup(desc);
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
    if (group != null && groupAdminServer.getRSGroupInfo(group) == null) {
      throw new ConstraintException("Region server group " + group + " does not exit");
    }
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor currentNsDesc, NamespaceDescriptor newNsDesc) throws IOException {
    preCreateNamespace(ctx, newNsDesc);
  }

  @Override
  public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, TableDescriptor desc) throws IOException {
    assignTableToGroup(desc);
  }

  @Override
  public void postClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ServerName> servers, List<ServerName> notClearedServers) throws IOException {
    Set<Address> clearedServer =
        servers.stream().filter(server -> !notClearedServers.contains(server))
            .map(ServerName::getAddress).collect(Collectors.toSet());
    if (!clearedServer.isEmpty()) {
      groupAdminServer.removeServers(clearedServer);
    }
  }
}
