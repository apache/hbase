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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * Service to support Region Server Grouping (HBase-6721).
 */
@InterfaceAudience.Private
public class RSGroupAdminServer implements RSGroupAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupAdminServer.class);

  private MasterServices master;
  private final RSGroupInfoManager rsGroupInfoManager;

  public RSGroupAdminServer(MasterServices master, RSGroupInfoManager rsGroupInfoManager)
      throws IOException {
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
  private RSGroupInfo getAndCheckRSGroupInfo(String name)
  throws IOException {
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
      if (state.getRegionLocation().getAddress().equals(server)) {
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
      // Be careful. This exception message is tested for in TestRSGroupsBase...
      throw new ConstraintException("Source RSGroup for server " + firstServer
              + " does not exist.");
    }
    RSGroupInfo srcGrp = new RSGroupInfo(tmpSrcGrp);
    if (srcGrp.getName().equals(targetGroupName)) {
      throw new ConstraintException("Target RSGroup " + targetGroupName +
              " is same as source " + srcGrp.getName() + " RSGroup.");
    }
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
   * Moves every region from servers which are currently located on these servers,
   * but should not be located there.
   * @param servers the servers that will move to new group
   * @param tables these tables will be kept on the servers, others will be moved
   * @param targetGroupName the target group name
   * @throws IOException if moving the server and tables fail
   */
  private void moveRegionsFromServers(Set<Address> servers, Set<TableName> tables,
      String targetGroupName) throws IOException {
    boolean foundRegionsToMove;
    RSGroupInfo targetGrp = getRSGroupInfo(targetGroupName);
    Set<Address> allSevers = new HashSet<>(servers);
    do {
      foundRegionsToMove = false;
      for (Iterator<Address> iter = allSevers.iterator(); iter.hasNext();) {
        Address rs = iter.next();
        // Get regions that are associated with this server and filter regions by tables.
        List<RegionInfo> regions = new ArrayList<>();
        for (RegionInfo region : getRegions(rs)) {
          if (!tables.contains(region.getTable())) {
            regions.add(region);
          }
        }

        LOG.info("Moving " + regions.size() + " region(s) from " + rs +
            " for server move to " + targetGroupName);
        if (!regions.isEmpty()) {
          for (RegionInfo region: regions) {
            // Regions might get assigned from tables of target group so we need to filter
            if (!targetGrp.containsTable(region.getTable())) {
              this.master.getAssignmentManager().move(region);
              if (master.getAssignmentManager().getRegionStates().
                  getRegionState(region).isFailedOpen()) {
                continue;
              }
              foundRegionsToMove = true;
            }
          }
        }
        if (!foundRegionsToMove) {
          iter.remove();
        }
      }
      try {
        rsGroupInfoManager.wait(1000);
      } catch (InterruptedException e) {
        LOG.warn("Sleep interrupted", e);
        Thread.currentThread().interrupt();
      }
    } while (foundRegionsToMove);
  }

  /**
   * Moves every region of tables which should be kept on the servers,
   * but currently they are located on other servers.
   * @param servers the regions of these servers will be kept on the servers, others will be moved
   * @param tables the tables that will move to new group
   * @param targetGroupName the target group name
   * @throws IOException if moving the region fails
   */
  private void moveRegionsToServers(Set<Address> servers, Set<TableName> tables,
      String targetGroupName) throws IOException {
    for (TableName table: tables) {
      LOG.info("Moving region(s) from " + table + " for table move to " + targetGroupName);
      for (RegionInfo region : master.getAssignmentManager().getRegionStates()
          .getRegionsOfTable(table)) {
        ServerName sn = master.getAssignmentManager().getRegionStates()
            .getRegionServerOfRegion(region);
        if (!servers.contains(sn.getAddress())) {
          master.getAssignmentManager().move(region);
        }
      }
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
      justification="Ignoring complaint because don't know what it is complaining about")
  @Override
  public void moveServers(Set<Address> servers, String targetGroupName)
  throws IOException {
    if (servers == null) {
      throw new ConstraintException("The list of servers to move cannot be null.");
    }
    if (servers.isEmpty()) {
      // For some reason this difference between null servers and isEmpty is important distinction.
      // TODO. Why? Stuff breaks if I equate them.
      return;
    }
    RSGroupInfo targetGrp = getAndCheckRSGroupInfo(targetGroupName);

    // Hold a lock on the manager instance while moving servers to prevent
    // another writer changing our state while we are working.
    synchronized (rsGroupInfoManager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveServers(servers, targetGroupName);
      }
      // Presume first server's source group. Later ensure all servers are from this group.
      Address firstServer = servers.iterator().next();
      RSGroupInfo srcGrp = rsGroupInfoManager.getRSGroupOfServer(firstServer);
      if (srcGrp == null) {
        // Be careful. This exception message is tested for in TestRSGroupsBase...
        throw new ConstraintException("Source RSGroup for server " + firstServer
            + " does not exist.");
      }
      if (srcGrp.getName().equals(targetGroupName)) {
        throw new ConstraintException("Target RSGroup " + targetGroupName +
            " is same as source " + srcGrp + " RSGroup.");
      }
      // Only move online servers (when moving from 'default') or servers from other
      // groups. This prevents bogus servers from entering groups
      if (RSGroupInfo.DEFAULT_GROUP.equals(srcGrp.getName())) {
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
      List<Address> editableMovedServers = Lists.newArrayList(movedServers);
      boolean foundRegionsToMove;
      do {
        foundRegionsToMove = false;
        for (Iterator<Address> iter = editableMovedServers.iterator(); iter.hasNext();) {
          Address rs = iter.next();
          // Get regions that are associated with this server.
          List<RegionInfo> regions = getRegions(rs);

          LOG.info("Moving " + regions.size() + " region(s) from " + rs +
              " for server move to " + targetGroupName);

          for (RegionInfo region: regions) {
            // Regions might get assigned from tables of target group so we need to filter
            if (targetGrp.containsTable(region.getTable())) {
              continue;
            }
            LOG.info("Moving region " + region.getShortNameToLog());
            this.master.getAssignmentManager().move(region);
            if (master.getAssignmentManager().getRegionStates().
                getRegionState(region).isFailedOpen()) {
              // If region is in FAILED_OPEN state, it won't recover, not without
              // operator intervention... in hbase-2.0.0 at least. Continue rather
              // than mark region as 'foundRegionsToMove'.
              continue;
            }
            foundRegionsToMove = true;
          }
          if (!foundRegionsToMove) {
            iter.remove();
          }
        }
        try {
          rsGroupInfoManager.wait(1000);
        } catch (InterruptedException e) {
          LOG.warn("Sleep interrupted", e);
          Thread.currentThread().interrupt();
        }
      } while (foundRegionsToMove);

      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveServers(servers, targetGroupName);
      }
      LOG.info("Move server done: " + srcGrp.getName() + "=>" + targetGroupName);
    }
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    if (tables == null) {
      throw new ConstraintException("The list of servers cannot be null.");
    }
    if (tables.size() < 1) {
      LOG.debug("moveTables() passed an empty set. Ignoring.");
      return;
    }

    // Hold a lock on the manager instance while moving servers to prevent
    // another writer changing our state while we are working.
    synchronized (rsGroupInfoManager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveTables(tables, targetGroup);
      }
      if(targetGroup != null) {
        RSGroupInfo destGroup = rsGroupInfoManager.getRSGroup(targetGroup);
        if(destGroup == null) {
          throw new ConstraintException("Target " + targetGroup + " RSGroup does not exist.");
        }
        if(destGroup.getServers().size() < 1) {
          throw new ConstraintException("Target RSGroup must have at least one server.");
        }
      }

      for (TableName table : tables) {
        String srcGroup = rsGroupInfoManager.getRSGroupOfTable(table);
        if(srcGroup != null && srcGroup.equals(targetGroup)) {
          throw new ConstraintException(
              "Source RSGroup " + srcGroup + " is same as target " + targetGroup +
              " RSGroup for table " + table);
        }
        LOG.info("Moving table " + table.getNameAsString() + " to RSGroup " + targetGroup);
      }
      rsGroupInfoManager.moveTables(tables, targetGroup);

      // targetGroup is null when a table is being deleted. In this case no further
      // action is required.
      if (targetGroup != null) {
        for (TableName table: tables) {
          if (master.getAssignmentManager().isTableDisabled(table)) {
            LOG.debug("Skipping move regions because the table" + table + " is disabled.");
            continue;
          }
          for (RegionInfo region :
              master.getAssignmentManager().getRegionStates().getRegionsOfTable(table)) {
            LOG.info("Moving region " + region.getShortNameToLog() +
                " to RSGroup " + targetGroup);
            master.getAssignmentManager().move(region);
          }
        }
      }

      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveTables(tables, targetGroup);
      }
    }
  }

  @Override
  public void addRSGroup(String name) throws IOException {
    if (master.getMasterCoprocessorHost() != null) {
      master.getMasterCoprocessorHost().preAddRSGroup(name);
    }
    rsGroupInfoManager.addRSGroup(new RSGroupInfo(name));
    if (master.getMasterCoprocessorHost() != null) {
      master.getMasterCoprocessorHost().postAddRSGroup(name);
    }
  }

  @Override
  public void removeRSGroup(String name) throws IOException {
    // Hold a lock on the manager instance while moving servers to prevent
    // another writer changing our state while we are working.
    synchronized (rsGroupInfoManager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preRemoveRSGroup(name);
      }
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
      for (NamespaceDescriptor ns: master.getClusterSchema().getNamespaces()) {
        String nsGroup = ns.getConfigurationValue(rsGroupInfo.NAMESPACE_DESC_PROP_GROUP);
        if (nsGroup != null &&  nsGroup.equals(name)) {
          throw new ConstraintException("RSGroup " + name + " is referenced by namespace: " +
              ns.getName());
        }
      }
      rsGroupInfoManager.removeRSGroup(name);
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postRemoveRSGroup(name);
      }
    }
  }

  @Override
  public boolean balanceRSGroup(String groupName) throws IOException {
    ServerManager serverManager = master.getServerManager();
    AssignmentManager assignmentManager = master.getAssignmentManager();
    LoadBalancer balancer = master.getLoadBalancer();

    synchronized (balancer) {
      // If balance not true, don't run balancer.
      if (!((HMaster) master).isBalancerOn()) {
        return false;
      }

      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preBalanceRSGroup(groupName);
      }
      if (getRSGroupInfo(groupName) == null) {
        throw new ConstraintException("RSGroup does not exist: "+groupName);
      }
      // Only allow one balance run at at time.
      Map<String, RegionState> groupRIT = rsGroupGetRegionsInTransition(groupName);
      if (groupRIT.size() > 0) {
        LOG.debug("Not running balancer because " + groupRIT.size() + " region(s) in transition: " +
          StringUtils.abbreviate(
              master.getAssignmentManager().getRegionStates().getRegionsInTransition().toString(),
              256));
        return false;
      }
      if (serverManager.areDeadServersInProgress()) {
        LOG.debug("Not running balancer because processing dead regionserver(s): " +
            serverManager.getDeadServers());
        return false;
      }

      //We balance per group instead of per table
      List<RegionPlan> plans = new ArrayList<>();
      for(Map.Entry<TableName, Map<ServerName, List<RegionInfo>>> tableMap:
          getRSGroupAssignmentsByTable(groupName).entrySet()) {
        LOG.info("Creating partial plan for table " + tableMap.getKey() + ": "
            + tableMap.getValue());
        List<RegionPlan> partialPlans = balancer.balanceCluster(tableMap.getValue());
        LOG.info("Partial plan for table " + tableMap.getKey() + ": " + partialPlans);
        if (partialPlans != null) {
          plans.addAll(partialPlans);
        }
      }
      long startTime = System.currentTimeMillis();
      boolean balancerRan = !plans.isEmpty();
      if (balancerRan) {
        LOG.info("RSGroup balance " + groupName + " starting with plan count: " + plans.size());
        for (RegionPlan plan: plans) {
          LOG.info("balance " + plan);
          assignmentManager.moveAsync(plan);
        }
        LOG.info("RSGroup balance " + groupName + " completed after " +
            (System.currentTimeMillis()-startTime) + " seconds");
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postBalanceRSGroup(groupName, balancerRan);
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
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveServersAndTables(servers, tables, targetGroup);
      }
      //check servers and tables status
      checkServersAndTables(servers, tables, targetGroup);

      //Move servers and tables to a new group.
      String srcGroup = getRSGroupOfServer(servers.iterator().next()).getName();
      rsGroupInfoManager.moveServersAndTables(servers, tables, srcGroup, targetGroup);

      //move regions which should not belong to these tables
      moveRegionsFromServers(servers, tables, targetGroup);
      //move regions which should belong to these servers
      moveRegionsToServers(servers, tables, targetGroup);

      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveServersAndTables(servers, tables, targetGroup);
      }
    }
    LOG.info("Move servers and tables done. Severs :"
            + servers + " , Tables : " + tables + " => " +  targetGroup);
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
        if (master.getMasterCoprocessorHost() != null) {
          master.getMasterCoprocessorHost().preRemoveServers(servers);
        }
        //check the set of servers
        checkForDeadOrOnlineServers(servers);
        rsGroupInfoManager.removeServers(servers);
        if (master.getMasterCoprocessorHost() != null) {
          master.getMasterCoprocessorHost().postRemoveServers(servers);
        }
        LOG.info("Remove decommissioned servers " + servers + " from rsgroup done.");
      }
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

  private Map<TableName, Map<ServerName, List<RegionInfo>>>
      getRSGroupAssignmentsByTable(String groupName) throws IOException {
    Map<TableName, Map<ServerName, List<RegionInfo>>> result = Maps.newHashMap();
    RSGroupInfo rsGroupInfo = getRSGroupInfo(groupName);
    Map<TableName, Map<ServerName, List<RegionInfo>>> assignments = Maps.newHashMap();
    for(Map.Entry<RegionInfo, ServerName> entry:
        master.getAssignmentManager().getRegionStates().getRegionAssignments().entrySet()) {
      TableName currTable = entry.getKey().getTable();
      ServerName currServer = entry.getValue();
      RegionInfo currRegion = entry.getKey();
      if (rsGroupInfo.getTables().contains(currTable)) {
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
        LOG.debug("Adding assignments for " + tableName + ": " + assignments.get(tableName));
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
}
