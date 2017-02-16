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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.net.Address;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Service to support Region Server Grouping (HBase-6721).
 */
@InterfaceAudience.Private
public class RSGroupAdminServer implements RSGroupAdmin {
  private static final Log LOG = LogFactory.getLog(RSGroupAdminServer.class);

  private MasterServices master;
  private final RSGroupInfoManager rsGroupInfoManager;

  public RSGroupAdminServer(MasterServices master,
                            RSGroupInfoManager RSGroupInfoManager) throws IOException {
    this.master = master;
    this.rsGroupInfoManager = RSGroupInfoManager;
  }

  @Override
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    return getRSGroupInfoManager().getRSGroup(groupName);
  }

  @Override
  public RSGroupInfo getRSGroupInfoOfTable(TableName tableName) throws IOException {
    String groupName = getRSGroupInfoManager().getRSGroupOfTable(tableName);
    return groupName == null? null: getRSGroupInfoManager().getRSGroup(groupName);
  }

  private void checkOnlineServersOnly(Set<Address> servers) throws ConstraintException {
    Set<Address> onlineServers = new HashSet<Address>();
    for(ServerName server: master.getServerManager().getOnlineServers().keySet()) {
      onlineServers.add(server.getAddress());
    }
    for (Address el: servers) {
      if (!onlineServers.contains(el)) {
        throw new ConstraintException(
            "Server " + el + " is not an online server in 'default' RSGroup.");
      }
    }
  }

  /**
   * Check passed name. Fail if nulls or if corresponding RSGroupInfo not found.
   * @return The RSGroupInfo named <code>name</code>
   * @throws IOException
   */
  private RSGroupInfo getAndCheckRSGroupInfo(String name)
  throws IOException {
    if (StringUtils.isEmpty(name)) {
      throw new ConstraintException("RSGroup cannot be null.");
    }
    RSGroupInfo rsgi = getRSGroupInfo(name);
    if (rsgi == null) {
      throw new ConstraintException("RSGroup does not exist: " + name);
    }
    return rsgi;
  }

  /**
   * @return List of Regions associated with this <code>server</code>.
   */
  private List<HRegionInfo> getRegions(final Address server) {
    LinkedList<HRegionInfo> regions = new LinkedList<HRegionInfo>();
    for (Map.Entry<HRegionInfo, ServerName> el :
        master.getAssignmentManager().getRegionStates().getRegionAssignments().entrySet()) {
      if (el.getValue().getAddress().equals(server)) {
        addRegion(regions, el.getKey());
      }
    }
    for (RegionState state:
        this.master.getAssignmentManager().getRegionStates().getRegionsInTransition()) {
      if (state.getServerName().getAddress().equals(server)) {
        addRegion(regions, state.getRegion());
      }
    }
    return regions;
  }

  private void addRegion(final LinkedList<HRegionInfo> regions, HRegionInfo hri) {
    // If meta, move it last otherwise other unassigns fail because meta is not
    // online for them to update state in. This is dodgy. Needs to be made more
    // robust. See TODO below.
    if (hri.isMetaRegion()) regions.addLast(hri);
    else regions.addFirst(hri);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
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
    RSGroupInfoManager manager = getRSGroupInfoManager();
    // Lock the manager during the below manipulations.
    synchronized (manager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveServers(servers, targetGroupName);
      }
      // Presume first server is the source group. Later we check all servers are from
      // this same group.
      Address firstServer = servers.iterator().next();
      RSGroupInfo srcGrp = manager.getRSGroupOfServer(firstServer);
      if (srcGrp == null) {
        // Be careful. This message is tested for in TestRSGroupsBase...
        throw new ConstraintException("Source RSGroup for server " + firstServer + " does not exist.");
      }
      if (srcGrp.getName().equals(targetGroupName)) {
        throw new ConstraintException( "Target RSGroup " + targetGroupName +
            " is same as source " + srcGrp + " RSGroup.");
      }
      // Only move online servers (when from 'default') or servers from other groups.
      // This prevents bogus servers from entering groups
      if (RSGroupInfo.DEFAULT_GROUP.equals(srcGrp.getName())) {
        checkOnlineServersOnly(servers);
      }
      // Check all servers are of same rsgroup.
      for (Address server: servers) {
        String tmpGroup = manager.getRSGroupOfServer(server).getName();
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
      Set<Address> movedServers = manager.moveServers(servers, srcGrp.getName(), targetGroupName);
      // Appy makes note that if we were passed in a List of servers,
      // we'd save having to do stuff like the below.
      List<Address> editableMovedServers = Lists.newArrayList(movedServers);
      boolean foundRegionsToUnassign;
      do {
        foundRegionsToUnassign = false;
        for (Iterator<Address> iter = editableMovedServers.iterator(); iter.hasNext();) {
          Address rs = iter.next();
          // Get regions that are associated with this server.
          List<HRegionInfo> regions = getRegions(rs);

          // Unassign regions for a server
          // TODO: This is problematic especially if hbase:meta is in the mix.
          // We need to update state in hbase:meta and if unassigned we hang
          // around in here. There is a silly sort on linked list done above
          // in getRegions putting hbase:meta last which helps but probably holes.
          LOG.info("Unassigning " + regions.size() +
              " region(s) from " + rs + " for server move to " + targetGroupName);
          if (!regions.isEmpty()) {
            // TODO bulk unassign or throttled unassign?
            for (HRegionInfo region: regions) {
              // Regions might get assigned from tables of target group so we need to filter
              if (!targetGrp.containsTable(region.getTable())) {
                this.master.getAssignmentManager().unassign(region);
                if (master.getAssignmentManager().getRegionStates().
                    getRegionState(region).isFailedOpen()) {
                  // If region is in FAILED_OPEN state, it won't recover, not without
                  // operator intervention... in hbase-2.0.0 at least. Continue rather
                  // than mark region as 'foundRegionsToUnassign'.
                  continue;
                }
                foundRegionsToUnassign = true;
              }
            }
          }
          if (!foundRegionsToUnassign) {
            iter.remove();
          }
        }
        try {
          manager.wait(1000);
        } catch (InterruptedException e) {
          LOG.warn("Sleep interrupted", e);
          Thread.currentThread().interrupt();
        }
      } while (foundRegionsToUnassign);
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveServers(servers, targetGroupName);
      }
      LOG.info("Move server done: " + srcGrp.getName() + "=>" + targetGroupName);
    }
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    if (tables == null) {
      throw new ConstraintException(
          "The list of servers cannot be null.");
    }
    if (tables.size() < 1) {
      LOG.debug("moveTables() passed an empty set. Ignoring.");
      return;
    }
    RSGroupInfoManager manager = getRSGroupInfoManager();
    // Lock the manager during below machinations.
    synchronized (manager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveTables(tables, targetGroup);
      }

      if(targetGroup != null) {
        RSGroupInfo destGroup = manager.getRSGroup(targetGroup);
        if(destGroup == null) {
          throw new ConstraintException("Target " + targetGroup + " RSGroup does not exist.");
        }
        if(destGroup.getServers().size() < 1) {
          throw new ConstraintException("Target RSGroup must have at least one server.");
        }
      }

      for (TableName table : tables) {
        String srcGroup = manager.getRSGroupOfTable(table);
        if(srcGroup != null && srcGroup.equals(targetGroup)) {
          throw new ConstraintException(
              "Source RSGroup " + srcGroup + " is same as target " + targetGroup +
              " RSGroup for table " + table);
        }
      }
      manager.moveTables(tables, targetGroup);
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveTables(tables, targetGroup);
      }
    }
    for (TableName table: tables) {
      LockManager.MasterLock lock = master.getLockManager().createMasterLock(table,
          LockProcedure.LockType.EXCLUSIVE, this.getClass().getName() + ": RSGroup: table move");
      try {
        try {
          lock.acquire();
        } catch (InterruptedException e) {
          throw new IOException("Interrupted when waiting for table lock", e);
        }
        for (HRegionInfo region :
            master.getAssignmentManager().getRegionStates().getRegionsOfTable(table)) {
          master.getAssignmentManager().unassign(region);
        }
      } finally {
        lock.release();
      }
    }
  }

  @Override
  public void addRSGroup(String name) throws IOException {
    if (master.getMasterCoprocessorHost() != null) {
      master.getMasterCoprocessorHost().preAddRSGroup(name);
    }
    getRSGroupInfoManager().addRSGroup(new RSGroupInfo(name));
    if (master.getMasterCoprocessorHost() != null) {
      master.getMasterCoprocessorHost().postAddRSGroup(name);
    }
  }

  @Override
  public void removeRSGroup(String name) throws IOException {
    RSGroupInfoManager manager = getRSGroupInfoManager();
    // Hold lock across coprocessor calls.
    synchronized (manager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preRemoveRSGroup(name);
      }
      RSGroupInfo RSGroupInfo = manager.getRSGroup(name);
      if (RSGroupInfo == null) {
        throw new ConstraintException("RSGroup " + name + " does not exist");
      }
      int tableCount = RSGroupInfo.getTables().size();
      if (tableCount > 0) {
        throw new ConstraintException("RSGroup " + name + " has " + tableCount +
            " tables; you must remove these tables from the rsgroup before " +
            "the rsgroup can be removed.");
      }
      int serverCount = RSGroupInfo.getServers().size();
      if (serverCount > 0) {
        throw new ConstraintException("RSGroup " + name + " has " + serverCount +
            " servers; you must remove these servers from the RSGroup before" +
            "the RSGroup can be removed.");
      }
      for (NamespaceDescriptor ns: master.getClusterSchema().getNamespaces()) {
        String nsGroup = ns.getConfigurationValue(RSGroupInfo.NAMESPACEDESC_PROP_GROUP);
        if (nsGroup != null &&  nsGroup.equals(name)) {
          throw new ConstraintException("RSGroup " + name + " is referenced by namespace: " +
              ns.getName());
        }
      }
      manager.removeRSGroup(name);
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

    boolean balancerRan;
    synchronized (balancer) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preBalanceRSGroup(groupName);
      }
      if (getRSGroupInfo(groupName) == null) {
        throw new ConstraintException("RSGroup does not exist: "+groupName);
      }
      // Only allow one balance run at at time.
      Map<String, RegionState> groupRIT = rsGroupGetRegionsInTransition(groupName);
      if (groupRIT.size() > 0) {
        LOG.debug("Not running balancer because " +
          groupRIT.size() +
          " region(s) in transition: " +
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
      List<RegionPlan> plans = new ArrayList<RegionPlan>();
      for(Map.Entry<TableName, Map<ServerName, List<HRegionInfo>>> tableMap:
          getRSGroupAssignmentsByTable(groupName).entrySet()) {
        LOG.info("Creating partial plan for table "+tableMap.getKey()+": "+tableMap.getValue());
        List<RegionPlan> partialPlans = balancer.balanceCluster(tableMap.getValue());
        LOG.info("Partial plan for table "+tableMap.getKey()+": "+partialPlans);
        if (partialPlans != null) {
          plans.addAll(partialPlans);
        }
      }
      long startTime = System.currentTimeMillis();
      balancerRan = plans != null;
      if (plans != null && !plans.isEmpty()) {
        LOG.info("RSGroup balance "+groupName+" starting with plan count: "+plans.size());
        for (RegionPlan plan: plans) {
          LOG.info("balance " + plan);
          assignmentManager.balance(plan);
        }
        LOG.info("RSGroup balance "+groupName+" completed after "+
            (System.currentTimeMillis()-startTime)+" seconds");
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postBalanceRSGroup(groupName, balancerRan);
      }
    }
    return balancerRan;
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    return getRSGroupInfoManager().listRSGroups();
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address hostPort) throws IOException {
    return getRSGroupInfoManager().getRSGroupOfServer(hostPort);
  }

  private RSGroupInfoManager getRSGroupInfoManager() throws IOException {
    return rsGroupInfoManager;
  }

  private Map<String, RegionState> rsGroupGetRegionsInTransition(String groupName)
      throws IOException {
    Map<String, RegionState> rit = Maps.newTreeMap();
    AssignmentManager am = master.getAssignmentManager();
    RSGroupInfo RSGroupInfo = getRSGroupInfo(groupName);
    for(TableName tableName : RSGroupInfo.getTables()) {
      for(HRegionInfo regionInfo: am.getRegionStates().getRegionsOfTable(tableName)) {
        RegionState state =
            master.getAssignmentManager().getRegionStates().getRegionTransitionState(regionInfo);
        if(state != null) {
          rit.put(regionInfo.getEncodedName(), state);
        }
      }
    }
    return rit;
  }

  private Map<TableName, Map<ServerName, List<HRegionInfo>>>
      getRSGroupAssignmentsByTable(String groupName) throws IOException {
    Map<TableName, Map<ServerName, List<HRegionInfo>>> result = Maps.newHashMap();
    RSGroupInfo RSGroupInfo = getRSGroupInfo(groupName);
    Map<TableName, Map<ServerName, List<HRegionInfo>>> assignments = Maps.newHashMap();
    for(Map.Entry<HRegionInfo, ServerName> entry:
        master.getAssignmentManager().getRegionStates().getRegionAssignments().entrySet()) {
      TableName currTable = entry.getKey().getTable();
      ServerName currServer = entry.getValue();
      HRegionInfo currRegion = entry.getKey();
      if(RSGroupInfo.getTables().contains(currTable)) {
        if(!assignments.containsKey(entry.getKey().getTable())) {
          assignments.put(currTable, new HashMap<ServerName, List<HRegionInfo>>());
        }
        if(!assignments.get(currTable).containsKey(currServer)) {
          assignments.get(currTable).put(currServer, new ArrayList<HRegionInfo>());
        }
        assignments.get(currTable).get(currServer).add(currRegion);
      }
    }

    Map<ServerName, List<HRegionInfo>> serverMap = Maps.newHashMap();
    for(ServerName serverName: master.getServerManager().getOnlineServers().keySet()) {
      if(RSGroupInfo.getServers().contains(serverName.getAddress())) {
        serverMap.put(serverName, Collections.EMPTY_LIST);
      }
    }

    //add all tables that are members of the group
    for(TableName tableName : RSGroupInfo.getTables()) {
      if(assignments.containsKey(tableName)) {
        result.put(tableName, new HashMap<ServerName, List<HRegionInfo>>());
        result.get(tableName).putAll(serverMap);
        result.get(tableName).putAll(assignments.get(tableName));
        LOG.debug("Adding assignments for "+tableName+": "+assignments.get(tableName));
      }
    }

    return result;
  }

  public void prepareRSGroupForTable(HTableDescriptor desc) throws IOException {
    String groupName =
        master.getClusterSchema().getNamespace(desc.getTableName().getNamespaceAsString())
                .getConfigurationValue(RSGroupInfo.NAMESPACEDESC_PROP_GROUP);
    if (groupName == null) {
      groupName = RSGroupInfo.DEFAULT_GROUP;
    }
    RSGroupInfo RSGroupInfo = getRSGroupInfo(groupName);
    if (RSGroupInfo == null) {
      throw new ConstraintException("RSGroup " + groupName + " does not exist.");
    }
    if (!RSGroupInfo.containsTable(desc.getTableName())) {
      LOG.debug("Pre-moving table " + desc.getTableName() + " to RSGroup " + groupName);
      moveTables(Sets.newHashSet(desc.getTableName()), groupName);
    }
  }

  public void cleanupRSGroupForTable(TableName tableName) throws IOException {
    try {
      RSGroupInfo group = getRSGroupInfoOfTable(tableName);
      if (group != null) {
        LOG.debug("Removing deleted table from table rsgroup " + group.getName());
        moveTables(Sets.newHashSet(tableName), null);
      }
    } catch (ConstraintException ex) {
      LOG.debug("Failed to perform RSGroup information cleanup for table: " + tableName, ex);
    } catch (IOException ex) {
      LOG.debug("Failed to perform RSGroup information cleanup for table: " + tableName, ex);
    }
  }
}
