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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * Service to support Region Server Grouping (HBase-6721).
 */
@InterfaceAudience.Private
public class RSGroupAdminServer implements RSGroupAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupAdminServer.class);
  static final String KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE = "should keep at least " +
          "one server in 'default' RSGroup.";

  private MasterServices master;
  final RSGroupInfoManager rsGroupInfoManager;

  /** Define the config key of retries threshold when movements failed */
  //made package private for testing
  static final String FAILED_MOVE_MAX_RETRY = "hbase.rsgroup.move.max.retry";

  /** Define the default number of retries */
  //made package private for testing
  static final int DEFAULT_MAX_RETRY_VALUE = 50;

  private int moveMaxRetry;

  public RSGroupAdminServer(MasterServices master, RSGroupInfoManager rsGroupInfoManager) {
    this.master = master;
    this.rsGroupInfoManager = rsGroupInfoManager;
    this.moveMaxRetry = master.getConfiguration().getInt(FAILED_MOVE_MAX_RETRY,
        DEFAULT_MAX_RETRY_VALUE);
  }

  @Override
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    return rsGroupInfoManager.getRSGroup(groupName);
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
   * Move every region from servers which are currently located on these servers, but should not be
   * located there.
   * @param servers the servers that will move to new group
   * @param targetGroupName the target group name
   * @throws IOException if moving the server and tables fail
   */
  private void moveServerRegionsFromGroup(Set<Address> servers, String targetGroupName)
    throws IOException {
    moveRegionsBetweenGroups(servers, targetGroupName, rs -> getRegions(rs), info -> {
      try {
        String groupName = RSGroupUtil.getRSGroupInfo(master, rsGroupInfoManager, info.getTable())
          .map(RSGroupInfo::getName).orElse(RSGroupInfo.DEFAULT_GROUP);
        return groupName.equals(targetGroupName);
      } catch (IOException e) {
        LOG.warn("Failed to test group for region {} and target group {}", info, targetGroupName);
        return false;
      }
    }, rs -> rs.getHostname());
  }

  private <T> void moveRegionsBetweenGroups(Set<T> regionsOwners, String targetGroupName,
      Function<T, List<RegionInfo>> getRegionsInfo, Function<RegionInfo, Boolean> validation,
      Function<T, String> getOwnerName) throws IOException {
    boolean hasRegionsToMove;
    int retry = 0;
    Set<T> allOwners = new HashSet<>(regionsOwners);
    Set<String> failedRegions = new HashSet<>();
    IOException toThrow = null;
    do {
      hasRegionsToMove = false;
      for (Iterator<T> iter = allOwners.iterator(); iter.hasNext(); ) {
        T owner = iter.next();
        // Get regions that are associated with this server and filter regions by group tables.
        for (RegionInfo region : getRegionsInfo.apply(owner)) {
          if (!validation.apply(region)) {
            LOG.info("Moving region {}, which do not belong to RSGroup {}",
                region.getShortNameToLog(), targetGroupName);
            try {
              this.master.getAssignmentManager().move(region);
              failedRegions.remove(region.getRegionNameAsString());
            } catch (IOException ioe) {
              LOG.debug("Move region {} from group failed, will retry, current retry time is {}",
                  region.getShortNameToLog(), retry, ioe);
              toThrow = ioe;
              failedRegions.add(region.getRegionNameAsString());
            }
            if (master.getAssignmentManager().getRegionStates().
                getRegionState(region).isFailedOpen()) {
              continue;
            }
            hasRegionsToMove = true;
          }
        }

        if (!hasRegionsToMove) {
          LOG.info("No more regions to move from {} to RSGroup", getOwnerName.apply(owner));
          iter.remove();
        }
      }

      retry++;
      try {
        rsGroupInfoManager.wait(1000);
      } catch (InterruptedException e) {
        LOG.warn("Sleep interrupted", e);
        Thread.currentThread().interrupt();
      }
    } while (hasRegionsToMove && retry <= moveMaxRetry);

    //has up to max retry time or there are no more regions to move
    if (hasRegionsToMove) {
      // print failed moved regions, for later process conveniently
      String msg = String
          .format("move regions for group %s failed, failed regions: %s", targetGroupName,
              failedRegions);
      LOG.error(msg);
      throw new DoNotRetryIOException(
          msg + ", just record the last failed region's cause, more details in server log",
          toThrow);
    }
  }

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
        // Be careful. This exception message is tested for in TestRSGroupsBase...
        throw new ConstraintException("Source RSGroup for server " + firstServer
            + " does not exist.");
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
      if (srcGrp.getServers().size() <= servers.size()) {
        // check if there are still tables reference this group
        for (TableDescriptor td : master.getTableDescriptors().getAll().values()) {
          Optional<String> optGroupName = td.getRegionServerGroup();
          if (optGroupName.isPresent() && optGroupName.get().equals(srcGrp.getName())) {
            throw new ConstraintException(
                "Cannot leave a RSGroup " + srcGrp.getName() + " that contains tables('" +
                    td.getTableName() + "' at least) without servers to host them.");
          }
        }
      }

      // MovedServers may be < passed in 'servers'.
      Set<Address> movedServers = rsGroupInfoManager.moveServers(servers, srcGrp.getName(),
          targetGroupName);
      moveServerRegionsFromGroup(movedServers, targetGroupName);
      LOG.info("Move servers done: {} => {}", srcGrp.getName(), targetGroupName);
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
      int serverCount = rsGroupInfo.getServers().size();
      if (serverCount > 0) {
        throw new ConstraintException("RSGroup " + name + " has " + serverCount +
          " servers; you must remove these servers from the RSGroup before" +
          " the RSGroup can be removed.");
      }
      for (TableDescriptor td : master.getTableDescriptors().getAll().values()) {
        if (td.getRegionServerGroup().map(name::equals).orElse(false)) {
          throw new ConstraintException("RSGroup " + name + " is already referenced by " +
            td.getTableName() + "; you must remove all the tables from the rsgroup before " +
            "the rsgroup can be removed.");
        }
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
        throw new ConstraintException("RSGroup does not exist: " + groupName);
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

      // We balance per group instead of per table
      List<RegionPlan> plans = new ArrayList<>();
      Map<TableName, Map<ServerName, List<RegionInfo>>> assignmentsByTable =
        getRSGroupAssignmentsByTable(groupName);
      for (Map.Entry<TableName, Map<ServerName, List<RegionInfo>>> tableMap : assignmentsByTable
        .entrySet()) {
        LOG.info("Creating partial plan for table {} : {}", tableMap.getKey(), tableMap.getValue());
        List<RegionPlan> partialPlans = balancer.balanceCluster(tableMap.getValue());
        LOG.info("Partial plan for table {} : {}", tableMap.getKey(), partialPlans);
        if (partialPlans != null) {
          plans.addAll(partialPlans);
        }
      }
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
  public void removeServers(Set<Address> servers) throws IOException {
    if (servers == null || servers.isEmpty()) {
      throw new ConstraintException("The set of servers to remove cannot be null or empty.");
    }
    // Hold a lock on the manager instance while moving servers to prevent
    // another writer changing our state while we are working.
    synchronized (rsGroupInfoManager) {
      // check the set of servers
      checkForDeadOrOnlineServers(servers);
      rsGroupInfoManager.removeServers(servers);
      LOG.info("Remove decommissioned servers {} from RSGroup done", servers);
    }
  }

  private boolean isTableInGroup(TableName tableName, String groupName,
    Set<TableName> tablesInGroupCache) throws IOException {
    if (tablesInGroupCache.contains(tableName)) {
      return true;
    }
    if (RSGroupUtil.getRSGroupInfo(master, rsGroupInfoManager, tableName).map(RSGroupInfo::getName)
      .orElse(RSGroupInfo.DEFAULT_GROUP).equals(groupName)) {
      tablesInGroupCache.add(tableName);
      return true;
    }
    return false;
  }

  private Map<String, RegionState> rsGroupGetRegionsInTransition(String groupName)
    throws IOException {
    Map<String, RegionState> rit = Maps.newTreeMap();
    Set<TableName> tablesInGroupCache = new HashSet<>();
    for (RegionStateNode regionNode : master.getAssignmentManager().getRegionsInTransition()) {
      TableName tn = regionNode.getTable();
      if (isTableInGroup(tn, groupName, tablesInGroupCache)) {
        rit.put(regionNode.getRegionInfo().getEncodedName(), regionNode.toRegionState());
      }
    }
    return rit;
  }

  private Map<TableName, Map<ServerName, List<RegionInfo>>>
    getRSGroupAssignmentsByTable(String groupName) throws IOException {
    Map<TableName, Map<ServerName, List<RegionInfo>>> result = Maps.newHashMap();
    Set<TableName> tablesInGroupCache = new HashSet<>();
    for (Map.Entry<RegionInfo, ServerName> entry : master.getAssignmentManager().getRegionStates()
      .getRegionAssignments().entrySet()) {
      RegionInfo region = entry.getKey();
      TableName tn = region.getTable();
      ServerName server = entry.getValue();
      if (isTableInGroup(tn, groupName, tablesInGroupCache)) {
        result.computeIfAbsent(tn, k -> new HashMap<>())
          .computeIfAbsent(server, k -> new ArrayList<>()).add(region);
      }
    }
    RSGroupInfo rsGroupInfo = getRSGroupInfo(groupName);
    for (ServerName serverName : master.getServerManager().getOnlineServers().keySet()) {
      if (rsGroupInfo.containsServer(serverName.getAddress())) {
        for (Map<ServerName, List<RegionInfo>> map : result.values()) {
          map.computeIfAbsent(serverName, k -> Collections.emptyList());
        }
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
