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

import static org.apache.hadoop.hbase.rsgroup.Utility.getOnlineServers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

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
import org.apache.hadoop.hbase.util.Addressing;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

/**
 * Service to support Region Server Grouping (HBase-6721)
 */
@InterfaceAudience.Private
public class RSGroupAdminServer extends RSGroupAdmin {
  private static final Log LOG = LogFactory.getLog(RSGroupAdminServer.class);

  private MasterServices master;
  // List of servers that are being moved from one group to another
  // Key=host:port,Value=targetGroup
  private NavigableMap<HostAndPort,String> serversInTransition =
      new ConcurrentSkipListMap<HostAndPort, String>(new Addressing.HostAndPortComparable());
  private RSGroupInfoManager rsgroupInfoManager;

  public RSGroupAdminServer(MasterServices master,
                            RSGroupInfoManager RSGroupInfoManager) throws IOException {
    this.master = master;
    this.rsgroupInfoManager = RSGroupInfoManager;
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

  @Override
  public void moveServers(Set<HostAndPort> servers, String targetGroupName)
      throws IOException {
    if (servers == null) {
      throw new ConstraintException("The list of servers to move cannot be null.");
    }
    if (servers.isEmpty()) {
      return;
    }
    if (StringUtils.isEmpty(targetGroupName)) {
      throw new ConstraintException("The target rsgroup cannot be null.");
    }
    RSGroupInfo targetGrp = getRSGroupInfo(targetGroupName);
    if (targetGrp == null) {
      throw new ConstraintException("RSGroup " + targetGroupName + " does not exist.");
    }
    RSGroupInfoManager manager = getRSGroupInfoManager();
    synchronized (manager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveServers(servers, targetGroupName);
      }
      HostAndPort firstServer = servers.iterator().next();
      // We only allow a move from a single source group so this should be ok
      RSGroupInfo srcGrp = manager.getRSGroupOfServer(firstServer);
      if (srcGrp == null) {
        throw new ConstraintException("Server " + firstServer + " does not have a rsgroup.");
      }
      if (srcGrp.getName().equals(targetGroupName)) {
        throw new ConstraintException( "Target rsgroup " + targetGroupName +
            " is same as source " + srcGrp + " rsgroup.");
      }
      // Only move online servers (from default) or servers from other groups.
      // This prevents bogus servers from entering groups
      if (RSGroupInfo.DEFAULT_GROUP.equals(srcGrp.getName())) {
        Set<HostAndPort> onlineServers = getOnlineServers(this.master);
        for (HostAndPort el: servers) {
          if (!onlineServers.contains(el)) {
            throw new ConstraintException(
                "Server " + el + " is not an online server in 'default' rsgroup.");
          }
        }
      }

      if(srcGrp.getServers().size() <= servers.size() &&
          srcGrp.getTables().size() > 0) {
        throw new ConstraintException("Cannot leave a rsgroup " + srcGrp.getName() +
            " that contains tables without servers to host them.");
      }

      String sourceGroupName =
          manager.getRSGroupOfServer(srcGrp.getServers().iterator().next()).getName();
      if (getRSGroupInfo(targetGroupName) == null) {
        throw new ConstraintException("Target " + targetGroupName + " rsgroup does not exist.");
      }

      for (HostAndPort server: servers) {
        if (serversInTransition.containsKey(server)) {
          throw new ConstraintException(
              "Server list contains a server " + server + " that is already being moved.");
        }
        String tmpGroup = manager.getRSGroupOfServer(server).getName();
        if (sourceGroupName != null && !tmpGroup.equals(sourceGroupName)) {
          throw new ConstraintException(
              "Move server request should only come from one source rsgroup. "+
              "Expecting only " + sourceGroupName + " but contains " + tmpGroup);
        }
      }

      if (sourceGroupName.equals(targetGroupName)) {
        throw new ConstraintException(
            "Target rsgroup " + sourceGroupName + " is same as source rsgroup.");
      }
      try {
        //update the servers as in transition
        for (HostAndPort server : servers) {
          serversInTransition.put(server, targetGroupName);
        }

        Set<HostAndPort> movedServers =
            manager.moveServers(servers, sourceGroupName, targetGroupName);
        boolean found;
        do {
          found = false;
          for (Iterator<HostAndPort> iter = movedServers.iterator();
               iter.hasNext(); ) {
            HostAndPort rs = iter.next();
            //get online regions
            List<HRegionInfo> regions = new LinkedList<HRegionInfo>();
            for (Map.Entry<HRegionInfo, ServerName> el :
                master.getAssignmentManager().getRegionStates().getRegionAssignments().entrySet()) {
              if (el.getValue().getHostPort().equals(rs)) {
                regions.add(el.getKey());
              }
            }
            for (RegionState state :
                master.getAssignmentManager().getRegionStates().getRegionsInTransition()) {
              if (state.getServerName().getHostPort().equals(rs)) {
                regions.add(state.getRegion());
              }
            }

            //unassign regions for a server
            LOG.info("Unassigning " + regions.size() +
                " regions from server " + rs + " for move to " + targetGroupName);
            if (regions.size() > 0) {
              //TODO bulk unassign or throttled unassign?
              for (HRegionInfo region : regions) {
                //regions might get assigned from tables of target group
                //so we need to filter
                if (!targetGrp.containsTable(region.getTable())) {
                  master.getAssignmentManager().unassign(region);
                  if (master.getAssignmentManager().getRegionStates().
                      getRegionState(region).isFailedOpen()) {
                    // If region is in FAILED_OPEN state, it won't recover, not without
                    // operator intervention... in hbase-2.0.0 at least. Continue rather
                    // than mark region as 'found'.
                    continue;
                  }
                  found = true;
                }
              }
            }
            if (!found) {
              iter.remove();
            }
          }
          try {
            manager.wait(1000);
          } catch (InterruptedException e) {
            LOG.warn("Sleep interrupted", e);
            Thread.currentThread().interrupt();
          }
        } while (found);
      } finally {
        //remove from transition
        for (HostAndPort server : servers) {
          serversInTransition.remove(server);
        }
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveServers(servers, targetGroupName);
      }
      LOG.info("Move server done: "+sourceGroupName+"->"+targetGroupName);
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
    synchronized (manager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveTables(tables, targetGroup);
      }

      if(targetGroup != null) {
        RSGroupInfo destGroup = manager.getRSGroup(targetGroup);
        if(destGroup == null) {
          throw new ConstraintException("Target " + targetGroup + " rsgroup does not exist.");
        }
        if(destGroup.getServers().size() < 1) {
          throw new ConstraintException("Target rsgroup must have at least one server.");
        }
      }

      for (TableName table : tables) {
        String srcGroup = manager.getRSGroupOfTable(table);
        if(srcGroup != null && srcGroup.equals(targetGroup)) {
          throw new ConstraintException(
              "Source rsgroup " + srcGroup + " is same as target " + targetGroup +
              " rsgroup for table " + table);
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
    synchronized (manager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preRemoveRSGroup(name);
      }
      RSGroupInfo RSGroupInfo = getRSGroupInfoManager().getRSGroup(name);
      if(RSGroupInfo == null) {
        throw new ConstraintException("RSGroup " + name + " does not exist");
      }
      int tableCount = RSGroupInfo.getTables().size();
      if (tableCount > 0) {
        throw new ConstraintException("RSGroup " + name + " has " + tableCount +
            " tables; you must remove these tables from the rsgroup before " +
            "the rsgroup can be removed.");
      }
      int serverCount = RSGroupInfo.getServers().size();
      if(serverCount > 0) {
        throw new ConstraintException("RSGroup " + name + " has " + serverCount +
            " servers; you must remove these servers from the rsgroup before" +
            "the rsgroup can be removed.");
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
  public RSGroupInfo getRSGroupOfServer(HostAndPort hostPort) throws IOException {
    return getRSGroupInfoManager().getRSGroupOfServer(hostPort);
  }

  @InterfaceAudience.Private
  public RSGroupInfoManager getRSGroupInfoManager() throws IOException {
    return rsgroupInfoManager;
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
      if(RSGroupInfo.getServers().contains(serverName.getHostPort())) {
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
      LOG.debug("Pre-moving table " + desc.getTableName() + " to rsgroup " + groupName);
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
      LOG.debug("Failed to perform rsgroup information cleanup for table: " + tableName, ex);
    } catch (IOException ex) {
      LOG.debug("Failed to perform rsgroup information cleanup for table: " + tableName, ex);
    }
  }

  @Override
  public void close() throws IOException {
  }
}
