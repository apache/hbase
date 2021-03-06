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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to support Region Server Grouping (HBase-6721).
 */
@InterfaceAudience.Private
public class RSGroupAdminServer implements RSGroupAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupAdminServer.class);
  public static final String KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE = "should keep at least " +
          "one server in 'default' RSGroup.";

  private MasterServices master;
  private final RSGroupInfoManager rsGroupInfoManager;

  public RSGroupAdminServer(MasterServices master, RSGroupInfoManager rsGroupInfoManager) {
    this.master = master;
    this.rsGroupInfoManager = rsGroupInfoManager;
  }

  @Override
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    return rsGroupInfoManager.getRSGroup(groupName);
  }

  @Override
  public RSGroupInfo getRSGroupInfoOfTable(TableName tableName) throws IOException {
    // We are reading across two Maps in the below with out synchronizing across
    // them; should be safe most of the time.
    String groupName = rsGroupInfoManager.getRSGroupOfTable(tableName);
    return groupName == null? null: rsGroupInfoManager.getRSGroup(groupName);
  }

  private void checkOnlineServersOnly(Set<Address> servers) throws ConstraintException {
    // This uglyness is because we only have Address, not ServerName.
    // Online servers are keyed by ServerName.
    Set<Address> onlineServers = new HashSet<>();
    for(ServerName server: master.getServerManager().getOnlineServers().keySet()) {
      onlineServers.add(server.getAddress());
    }
    for (Address address: servers) {
      if (!onlineServers.contains(address)) {
        throw new ConstraintException(
            "Server " + address + " is not an online server in 'default' RSGroup.");
      }
    }
  }

  /**
   * Check passed name. Fail if nulls or if corresponding RSGroupInfo not found.
   * @return The RSGroupInfo named <code>name</code>
   */
  private RSGroupInfo getAndCheckRSGroupInfo(String name) throws IOException {
    if (StringUtils.isEmpty(name)) {
      throw new ConstraintException("RSGroup cannot be null.");
    }
    RSGroupInfo rsGroupInfo = getRSGroupInfo(name);
    if (rsGroupInfo == null) {
      throw new ConstraintException("RSGroup does not exist: " + name);
    }
    return rsGroupInfo;
  }

  /**
   * @return List of Regions associated with this <code>server</code>.
   */
  private List<RegionInfo> getRegions(final Address server) {
    LinkedList<RegionInfo> regions = new LinkedList<>();
    for (Map.Entry<RegionInfo, ServerName> el :
        master.getAssignmentManager().getRegionStates().getRegionAssignments().entrySet()) {
      if (el.getValue() == null) {
        continue;
      }

      if (el.getValue().getAddress().equals(server)) {
        addRegion(regions, el.getKey());
      }
    }
    for (RegionStateNode state : master.getAssignmentManager().getRegionsInTransition()) {
      if (state.getRegionLocation() != null &&
          state.getRegionLocation().getAddress().equals(server)) {
        addRegion(regions, state.getRegionInfo());
      }
    }
    return regions;
  }

  private void addRegion(final LinkedList<RegionInfo> regions, RegionInfo hri) {
    // If meta, move it last otherwise other unassigns fail because meta is not
    // online for them to update state in. This is dodgy. Needs to be made more
    // robust. See TODO below.
    if (hri.isMetaRegion()) {
      regions.addLast(hri);
    } else {
      regions.addFirst(hri);
    }
  }

  /**
   * Check servers and tables.
   *
   * @param servers servers to move
   * @param tables tables to move
   * @param targetGroupName target group name
   * @throws IOException if nulls or if servers and tables not belong to the same group
   */
  private void checkServersAndTables(Set<Address> servers, Set<TableName> tables,
                                     String targetGroupName) throws IOException {
    // Presume first server's source group. Later ensure all servers are from this group.
    Address firstServer = servers.iterator().next();
    RSGroupInfo tmpSrcGrp = rsGroupInfoManager.getRSGroupOfServer(firstServer);
    if (tmpSrcGrp == null) {
      // Be careful. This exception message is tested for in TestRSGroupsAdmin2...
      throw new ConstraintException("Server " + firstServer
        + " is either offline or it does not exist.");
    }
    RSGroupInfo srcGrp = new RSGroupInfo(tmpSrcGrp);

    // Only move online servers
    checkOnlineServersOnly(servers);

    // Ensure all servers are of same rsgroup.
    for (Address server: servers) {
      String tmpGroup = rsGroupInfoManager.getRSGroupOfServer(server).getName();
      if (!tmpGroup.equals(srcGrp.getName())) {
        throw new ConstraintException("Move server request should only come from one source " +
                "RSGroup. Expecting only " + srcGrp.getName() + " but contains " + tmpGroup);
      }
    }

    // Ensure all tables and servers are of same rsgroup.
    for (TableName table : tables) {
      String tmpGroup = rsGroupInfoManager.getRSGroupOfTable(table);
      if (!tmpGroup.equals(srcGrp.getName())) {
        throw new ConstraintException("Move table request should only come from one source " +
                "RSGroup. Expecting only " + srcGrp.getName() + " but contains " + tmpGroup);
      }
    }

    if (srcGrp.getServers().size() <= servers.size() && srcGrp.getTables().size() > tables.size()) {
      throw new ConstraintException("Cannot leave a RSGroup " + srcGrp.getName() +
              " that contains tables without servers to host them.");
    }
  }

  /**
   * Move every region from servers which are currently located on these servers,
   * but should not be located there.
   *
   * @param movedServers  the servers that are moved to new group
   * @param movedTables the tables that are moved to new group
   * @param srcGrpServers all servers in the source group, excluding the movedServers
   * @param targetGroupName the target group
   * @param sourceGroupName the source group
   * @throws IOException if any error while moving regions
   */
  private void moveServerRegionsFromGroup(Set<Address> movedServers, Set<TableName> movedTables,
    Set<Address> srcGrpServers, String targetGroupName,
    String sourceGroupName) throws IOException {
    // Get server names corresponding to given Addresses
    List<ServerName> movedServerNames = new ArrayList<>(movedServers.size());
    List<ServerName> srcGrpServerNames = new ArrayList<>(srcGrpServers.size());
    for (ServerName serverName : master.getServerManager().getOnlineServers().keySet()) {
      // In case region move failed in previous attempt, regionsOwners and newRegionsOwners
      // can have the same servers. So for all servers below both conditions to be checked
      if (srcGrpServers.contains(serverName.getAddress())) {
        srcGrpServerNames.add(serverName);
      }
      if (movedServers.contains(serverName.getAddress())) {
        movedServerNames.add(serverName);
      }
    }
    // Set true to indicate at least one region movement failed
    boolean errorInRegionMove;
    List<Pair<RegionInfo, Future<byte[]>>> assignmentFutures = new ArrayList<>();
    int retry = 0;
    do {
      errorInRegionMove = false;
      for (ServerName server : movedServerNames) {
        List<RegionInfo> regionsOnServer = getRegions(server.getAddress());
        for (RegionInfo region : regionsOnServer) {
          if (!movedTables.contains(region.getTable()) && !srcGrpServers
            .contains(getRegionAddress(region))) {
            LOG.info("Moving server region {}, which do not belong to RSGroup {}",
              region.getShortNameToLog(), targetGroupName);
            // Move region back to source RSGroup servers
            ServerName dest =
              this.master.getLoadBalancer().randomAssignment(region, srcGrpServerNames);
            if (dest == null) {
              errorInRegionMove = true;
              continue;
            }
            RegionPlan rp = new RegionPlan(region, server, dest);
            try {
              Future<byte[]> future = this.master.getAssignmentManager().moveAsync(rp);
              assignmentFutures.add(Pair.newPair(region, future));
            } catch (Exception ioe) {
              errorInRegionMove = true;
              LOG.error("Move region {} failed, will retry, current retry time is {}",
                region.getShortNameToLog(), retry, ioe);
            }
          }
        }
      }
      boolean allRegionsMoved =
        waitForRegionMovement(assignmentFutures, sourceGroupName, retry);
      if (allRegionsMoved && !errorInRegionMove) {
        LOG.info("All regions from {} are moved back to {}", movedServerNames, sourceGroupName);
        return;
      } else {
        retry++;
        try {
          rsGroupInfoManager.wait(1000);
        } catch (InterruptedException e) {
          LOG.warn("Sleep interrupted", e);
          Thread.currentThread().interrupt();
        }
      }
    } while (retry <= 50);
  }

  private Address getRegionAddress(RegionInfo hri) {
    ServerName sn = master.getAssignmentManager().getRegionStates().getRegionServerOfRegion(hri);
    return sn.getAddress();
  }

  /**
   * Wait for all the region move to complete. Keep waiting for other region movement
   * completion even if some region movement fails.
   */
  private boolean waitForRegionMovement(List<Pair<RegionInfo, Future<byte[]>>> regionMoveFutures,
    String groupName, int retryCount) {
    LOG.info("Moving {} region(s) to group {}, current retry={}", regionMoveFutures.size(),
      groupName, retryCount);
    boolean allRegionsMoved = true;
    for (Pair<RegionInfo, Future<byte[]>> pair : regionMoveFutures) {
      try {
        pair.getSecond().get();
        if (master.getAssignmentManager().getRegionStates().
          getRegionState(pair.getFirst()).isFailedOpen()) {
          allRegionsMoved = false;
        }
      } catch (InterruptedException e) {
        LOG.warn("Sleep interrupted", e);
        // Dont return form there lets wait for other regions to complete movement.
        allRegionsMoved = false;
      } catch (Exception e) {
        allRegionsMoved = false;
        LOG.error("Move region {} to group {} failed, will retry on next attempt",
          pair.getFirst().getShortNameToLog(), groupName, e);
      }
    }
    return allRegionsMoved;
  }

  /**
   * Moves regions of tables which are not on target group servers.
   *
   * @param tables    the tables that will move to new group
   * @param targetGrp the target group
   * @throws IOException if moving the region fails
   */
  private void moveTableRegionsToGroup(Set<TableName> tables, RSGroupInfo targetGrp)
    throws IOException {
    List<ServerName> targetGrpSevers = new ArrayList<>(targetGrp.getServers().size());
    for (ServerName serverName : master.getServerManager().getOnlineServers().keySet()) {
      if (targetGrp.getServers().contains(serverName.getAddress())) {
        targetGrpSevers.add(serverName);
      }
    }
    //Set true to indicate at least one region movement failed
    boolean errorInRegionMove;
    int retry = 0;
    List<Pair<RegionInfo, Future<byte[]>>> assignmentFutures = new ArrayList<>();
    do {
      errorInRegionMove = false;
      for (TableName table : tables) {
        if (master.getTableStateManager().isTableState(table, TableState.State.DISABLED,
          TableState.State.DISABLING)) {
          LOG.debug("Skipping move regions because the table {} is disabled", table);
          continue;
        }
        LOG.info("Moving region(s) for table {} to RSGroup {}", table, targetGrp.getName());
        for (RegionInfo region : master.getAssignmentManager().getRegionStates()
          .getRegionsOfTable(table)) {
          ServerName sn =
            master.getAssignmentManager().getRegionStates().getRegionServerOfRegion(region);
          if (!targetGrp.containsServer(sn.getAddress())) {
            LOG.info("Moving region {} to RSGroup {}", region.getShortNameToLog(),
              targetGrp.getName());
            ServerName dest =
              this.master.getLoadBalancer().randomAssignment(region, targetGrpSevers);
            if (dest == null) {
              errorInRegionMove = true;
              continue;
            }
            RegionPlan rp = new RegionPlan(region, sn, dest);
            try {
              Future<byte[]> future = this.master.getAssignmentManager().moveAsync(rp);
              assignmentFutures.add(Pair.newPair(region, future));
            } catch (Exception ioe) {
              errorInRegionMove = true;
              LOG.error("Move region {} to group failed, will retry, current retry time is {}",
                region.getShortNameToLog(), retry, ioe);
            }

          }
        }
      }
      boolean allRegionsMoved =
        waitForRegionMovement(assignmentFutures, targetGrp.getName(), retry);
      if (allRegionsMoved && !errorInRegionMove) {
        LOG.info("All regions from table(s) {} moved to target group {}.", tables,
          targetGrp.getName());
        return;
      } else {
        retry++;
        try {
          rsGroupInfoManager.wait(1000);
        } catch (InterruptedException e) {
          LOG.warn("Sleep interrupted", e);
          Thread.currentThread().interrupt();
        }
      }
    } while (retry <= 50);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
      justification="Ignoring complaint because don't know what it is complaining about")
  @Override
  public void moveServers(Set<Address> servers, String targetGroupName) throws IOException {
    if (servers == null) {
      throw new ConstraintException("The list of servers to move cannot be null.");
    }
    if (servers.isEmpty()) {
      // For some reason this difference between null servers and isEmpty is important distinction.
      // TODO. Why? Stuff breaks if I equate them.
      return;
    }
    //check target group
    getAndCheckRSGroupInfo(targetGroupName);

    // Hold a lock on the manager instance while moving servers to prevent
    // another writer changing our state while we are working.
    synchronized (rsGroupInfoManager) {
      // Presume first server's source group. Later ensure all servers are from this group.
      Address firstServer = servers.iterator().next();
      RSGroupInfo srcGrp = rsGroupInfoManager.getRSGroupOfServer(firstServer);
      if (srcGrp == null) {
        // Be careful. This exception message is tested for in TestRSGroupsAdmin2...
        throw new ConstraintException("Server " + firstServer
          + " is either offline or it does not exist.");
      }
      // Only move online servers (when moving from 'default') or servers from other
      // groups. This prevents bogus servers from entering groups
      if (RSGroupInfo.DEFAULT_GROUP.equals(srcGrp.getName())) {
        if (srcGrp.getServers().size() <= servers.size()) {
          throw new ConstraintException(KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE);
        }
        checkOnlineServersOnly(servers);
      }
      // Ensure all servers are of same rsgroup.
      for (Address server: servers) {
        String tmpGroup = rsGroupInfoManager.getRSGroupOfServer(server).getName();
        if (!tmpGroup.equals(srcGrp.getName())) {
          throw new ConstraintException("Move server request should only come from one source " +
              "RSGroup. Expecting only " + srcGrp.getName() + " but contains " + tmpGroup);
        }
      }
      if (srcGrp.getServers().size() <= servers.size() && srcGrp.getTables().size() > 0) {
        throw new ConstraintException("Cannot leave a RSGroup " + srcGrp.getName() +
            " that contains tables without servers to host them.");
      }

      // MovedServers may be < passed in 'servers'.
      Set<Address> movedServers = rsGroupInfoManager.moveServers(servers, srcGrp.getName(),
          targetGroupName);
      moveServerRegionsFromGroup(movedServers, Collections.emptySet(),
        rsGroupInfoManager.getRSGroup(srcGrp.getName()).getServers(),
        targetGroupName, srcGrp.getName());
      LOG.info("Move servers done: {} => {}", srcGrp.getName(), targetGroupName);
    }
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    if (tables == null) {
      throw new ConstraintException("The list of tables cannot be null.");
    }
    if (tables.size() < 1) {
      LOG.debug("moveTables() passed an empty set. Ignoring.");
      return;
    }

    // Hold a lock on the manager instance while moving servers to prevent
    // another writer changing our state while we are working.
    synchronized (rsGroupInfoManager) {
      if(targetGroup != null) {
        RSGroupInfo destGroup = rsGroupInfoManager.getRSGroup(targetGroup);
        if(destGroup == null) {
          throw new ConstraintException("Target " + targetGroup + " RSGroup does not exist.");
        }
        if(destGroup.getServers().size() < 1) {
          throw new ConstraintException("Target RSGroup must have at least one server.");
        }
      }
      rsGroupInfoManager.moveTables(tables, targetGroup);

      // targetGroup is null when a table is being deleted. In this case no further
      // action is required.
      if (targetGroup != null) {
        modifyOrMoveTables(tables, rsGroupInfoManager.getRSGroup(targetGroup));
      }
    }
  }

  @Override
  public void addRSGroup(String name) throws IOException {
    rsGroupInfoManager.addRSGroup(new RSGroupInfo(name));
  }

  @Override
  public void removeRSGroup(String name) throws IOException {
    // Hold a lock on the manager instance while moving servers to prevent
    // another writer changing our state while we are working.
    synchronized (rsGroupInfoManager) {
      RSGroupInfo rsGroupInfo = rsGroupInfoManager.getRSGroup(name);
      if (rsGroupInfo == null) {
        throw new ConstraintException("RSGroup " + name + " does not exist");
      }
      int tableCount = rsGroupInfo.getTables().size();
      if (tableCount > 0) {
        throw new ConstraintException("RSGroup " + name + " has " + tableCount +
            " tables; you must remove these tables from the rsgroup before " +
            "the rsgroup can be removed.");
      }
      int serverCount = rsGroupInfo.getServers().size();
      if (serverCount > 0) {
        throw new ConstraintException("RSGroup " + name + " has " + serverCount +
            " servers; you must remove these servers from the RSGroup before" +
            "the RSGroup can be removed.");
      }
      for (NamespaceDescriptor ns : master.getClusterSchema().getNamespaces()) {
        String nsGroup = ns.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP);
        if (nsGroup != null && nsGroup.equals(name)) {
          throw new ConstraintException(
            "RSGroup " + name + " is referenced by namespace: " + ns.getName());
        }
      }
      rsGroupInfoManager.removeRSGroup(name);
    }
  }

  @Override
  public boolean balanceRSGroup(String groupName) throws IOException {
    ServerManager serverManager = master.getServerManager();
    LoadBalancer balancer = master.getLoadBalancer();

    synchronized (balancer) {
      // If balance not true, don't run balancer.
      if (!((HMaster) master).isBalancerOn()) {
        return false;
      }

      if (getRSGroupInfo(groupName) == null) {
        throw new ConstraintException("RSGroup does not exist: "+groupName);
      }
      // Only allow one balance run at at time.
      Map<String, RegionState> groupRIT = rsGroupGetRegionsInTransition(groupName);
      if (groupRIT.size() > 0) {
        LOG.debug("Not running balancer because {} region(s) in transition: {}", groupRIT.size(),
            StringUtils.abbreviate(
              master.getAssignmentManager().getRegionStates().getRegionsInTransition().toString(),
              256));
        return false;
      }
      if (serverManager.areDeadServersInProgress()) {
        LOG.debug("Not running balancer because processing dead regionserver(s): {}",
            serverManager.getDeadServers());
        return false;
      }

      //We balance per group instead of per table
      Map<TableName, Map<ServerName, List<RegionInfo>>> assignmentsByTable =
          getRSGroupAssignmentsByTable(master.getTableStateManager(), groupName);
      List<RegionPlan> plans = balancer.balanceCluster(assignmentsByTable);
      boolean balancerRan = !plans.isEmpty();
      if (balancerRan) {
        LOG.info("RSGroup balance {} starting with plan count: {}", groupName, plans.size());
        master.executeRegionPlansWithThrottling(plans);
        LOG.info("RSGroup balance " + groupName + " completed");
      }
      return balancerRan;
    }
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    return rsGroupInfoManager.listRSGroups();
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address hostPort) throws IOException {
    return rsGroupInfoManager.getRSGroupOfServer(hostPort);
  }

  @Override
  public void moveServersAndTables(Set<Address> servers, Set<TableName> tables, String targetGroup)
      throws IOException {
    if (servers == null || servers.isEmpty()) {
      throw new ConstraintException("The list of servers to move cannot be null or empty.");
    }
    if (tables == null || tables.isEmpty()) {
      throw new ConstraintException("The list of tables to move cannot be null or empty.");
    }

    //check target group
    getAndCheckRSGroupInfo(targetGroup);

    // Hold a lock on the manager instance while moving servers and tables to prevent
    // another writer changing our state while we are working.
    synchronized (rsGroupInfoManager) {
      //check servers and tables status
      checkServersAndTables(servers, tables, targetGroup);

      //Move servers and tables to a new group.
      String srcGroup = getRSGroupOfServer(servers.iterator().next()).getName();
      rsGroupInfoManager.moveServersAndTables(servers, tables, srcGroup, targetGroup);

      //move regions on these servers which do not belong to group tables
      moveServerRegionsFromGroup(servers, tables,
        rsGroupInfoManager.getRSGroup(srcGroup).getServers(),
        targetGroup, srcGroup);
      //move regions of these tables which are not on group servers
      modifyOrMoveTables(tables, rsGroupInfoManager.getRSGroup(targetGroup));
    }
    LOG.info("Move servers and tables done. Severs: {}, Tables: {} => {}", servers, tables,
        targetGroup);
  }

  @Override
  public void removeServers(Set<Address> servers) throws IOException {
    {
      if (servers == null || servers.isEmpty()) {
        throw new ConstraintException("The set of servers to remove cannot be null or empty.");
      }
      // Hold a lock on the manager instance while moving servers to prevent
      // another writer changing our state while we are working.
      synchronized (rsGroupInfoManager) {
        //check the set of servers
        checkForDeadOrOnlineServers(servers);
        rsGroupInfoManager.removeServers(servers);
        LOG.info("Remove decommissioned servers {} from RSGroup done", servers);
      }
    }
  }

  @Override
  public void renameRSGroup(String oldName, String newName) throws IOException {
    synchronized (rsGroupInfoManager) {
      rsGroupInfoManager.renameRSGroup(oldName, newName);
      Set<TableDescriptor> updateTables = master.getTableDescriptors().getAll().values().stream()
        .filter(t -> oldName.equals(t.getRegionServerGroup().orElse(null)))
        .collect(Collectors.toSet());
      // Update rs group info into table descriptors
      modifyTablesAndWaitForCompletion(updateTables, newName);
    }
  }

  @Override
  public void updateRSGroupConfig(String groupName, Map<String, String> configuration)
      throws IOException {
    synchronized (rsGroupInfoManager) {
      rsGroupInfoManager.updateRSGroupConfig(groupName, configuration);
    }
  }

  private Map<String, RegionState> rsGroupGetRegionsInTransition(String groupName)
      throws IOException {
    Map<String, RegionState> rit = Maps.newTreeMap();
    AssignmentManager am = master.getAssignmentManager();
    for(TableName tableName : getRSGroupInfo(groupName).getTables()) {
      for(RegionInfo regionInfo: am.getRegionStates().getRegionsOfTable(tableName)) {
        RegionState state = am.getRegionStates().getRegionTransitionState(regionInfo);
        if(state != null) {
          rit.put(regionInfo.getEncodedName(), state);
        }
      }
    }
    return rit;
  }

  /**
   * This is an EXPENSIVE clone. Cloning though is the safest thing to do. Can't let out original
   * since it can change and at least the load balancer wants to iterate this exported list. Load
   * balancer should iterate over this list because cloned list will ignore disabled table and split
   * parent region cases. This method is invoked by {@link #balanceRSGroup}
   * @return A clone of current assignments for this group.
   */
  Map<TableName, Map<ServerName, List<RegionInfo>>> getRSGroupAssignmentsByTable(
      TableStateManager tableStateManager, String groupName) throws IOException {
    Map<TableName, Map<ServerName, List<RegionInfo>>> result = Maps.newHashMap();
    RSGroupInfo rsGroupInfo = getRSGroupInfo(groupName);
    Map<TableName, Map<ServerName, List<RegionInfo>>> assignments = Maps.newHashMap();
    for (Map.Entry<RegionInfo, ServerName> entry : master.getAssignmentManager().getRegionStates()
        .getRegionAssignments().entrySet()) {
      TableName currTable = entry.getKey().getTable();
      ServerName currServer = entry.getValue();
      RegionInfo currRegion = entry.getKey();
      if (rsGroupInfo.getTables().contains(currTable)) {
        if (tableStateManager.isTableState(currTable, TableState.State.DISABLED,
          TableState.State.DISABLING)) {
          continue;
        }
        if (currRegion.isSplitParent()) {
          continue;
        }
        assignments.putIfAbsent(currTable, new HashMap<>());
        assignments.get(currTable).putIfAbsent(currServer, new ArrayList<>());
        assignments.get(currTable).get(currServer).add(currRegion);
      }
    }

    Map<ServerName, List<RegionInfo>> serverMap = Maps.newHashMap();
    for(ServerName serverName: master.getServerManager().getOnlineServers().keySet()) {
      if(rsGroupInfo.getServers().contains(serverName.getAddress())) {
        serverMap.put(serverName, Collections.emptyList());
      }
    }

    // add all tables that are members of the group
    for(TableName tableName : rsGroupInfo.getTables()) {
      if(assignments.containsKey(tableName)) {
        result.put(tableName, new HashMap<>());
        result.get(tableName).putAll(serverMap);
        result.get(tableName).putAll(assignments.get(tableName));
        LOG.debug("Adding assignments for {}: {}", tableName, assignments.get(tableName));
      }
    }

    return result;
  }

  /**
   * Check if the set of servers are belong to dead servers list or online servers list.
   * @param servers servers to remove
   */
  private void checkForDeadOrOnlineServers(Set<Address> servers) throws ConstraintException {
    // This uglyness is because we only have Address, not ServerName.
    Set<Address> onlineServers = new HashSet<>();
    List<ServerName> drainingServers = master.getServerManager().getDrainingServersList();
    for (ServerName server : master.getServerManager().getOnlineServers().keySet()) {
      // Only online but not decommissioned servers are really online
      if (!drainingServers.contains(server)) {
        onlineServers.add(server.getAddress());
      }
    }

    Set<Address> deadServers = new HashSet<>();
    for(ServerName server: master.getServerManager().getDeadServers().copyServerNames()) {
      deadServers.add(server.getAddress());
    }

    for (Address address: servers) {
      if (onlineServers.contains(address)) {
        throw new ConstraintException(
            "Server " + address + " is an online server, not allowed to remove.");
      }
      if (deadServers.contains(address)) {
        throw new ConstraintException(
            "Server " + address + " is on the dead servers list,"
                + " Maybe it will come back again, not allowed to remove.");
      }
    }
  }

  // Modify table or move table's regions
  void modifyOrMoveTables(Set<TableName> tables, RSGroupInfo targetGroup) throws IOException {
    Set<TableName> tablesToBeMoved = new HashSet<>(tables.size());
    Set<TableDescriptor> tablesToBeModified = new HashSet<>(tables.size());
    // Segregate tables into to be modified or to be moved category
    for (TableName tableName : tables) {
      TableDescriptor descriptor = master.getTableDescriptors().get(tableName);
      if (descriptor == null) {
        LOG.error(
          "TableDescriptor of table {} not found. Skipping the region movement of this table.");
        continue;
      }
      if (descriptor.getRegionServerGroup().isPresent()) {
        tablesToBeModified.add(descriptor);
      } else {
        tablesToBeMoved.add(tableName);
      }
    }
    List<Long> procedureIds = null;
    if (!tablesToBeModified.isEmpty()) {
      procedureIds = modifyTables(tablesToBeModified, targetGroup.getName());
    }
    if (!tablesToBeMoved.isEmpty()) {
      moveTableRegionsToGroup(tablesToBeMoved, targetGroup);
    }
    // By this time moveTableRegionsToGroup is finished, lets wait for modifyTables completion
    if (procedureIds != null) {
      waitForProcedureCompletion(procedureIds);
    }
  }

  private void modifyTablesAndWaitForCompletion(Set<TableDescriptor> tableDescriptors,
    String targetGroup) throws IOException {
    final List<Long> procIds = modifyTables(tableDescriptors, targetGroup);
    waitForProcedureCompletion(procIds);
  }

  // Modify table internally moves the regions as well. So separate region movement is not needed
  private List<Long> modifyTables(Set<TableDescriptor> tableDescriptors, String targetGroup)
    throws IOException {
    List<Long> procIds = new ArrayList<>(tableDescriptors.size());
    for (TableDescriptor oldTd : tableDescriptors) {
      TableDescriptor newTd =
        TableDescriptorBuilder.newBuilder(oldTd).setRegionServerGroup(targetGroup).build();
      procIds.add(master
        .modifyTable(oldTd.getTableName(), newTd, HConstants.NO_NONCE, HConstants.NO_NONCE));
    }
    return procIds;
  }

  private void waitForProcedureCompletion(List<Long> procIds) throws IOException {
    for (long procId : procIds) {
      Procedure<?> proc = master.getMasterProcedureExecutor().getProcedure(procId);
      if (proc == null) {
        continue;
      }
      ProcedureSyncWait
        .waitForProcedureToCompleteIOE(master.getMasterProcedureExecutor(), proc, Long.MAX_VALUE);
    }
  }
}
