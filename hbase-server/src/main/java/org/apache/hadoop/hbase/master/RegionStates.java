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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Preconditions;

/**
 * Region state accountant. It holds the states of all regions in the memory.
 * In normal scenario, it should match the meta table and the true region states.
 *
 * This map is used by AssignmentManager to track region states.
 */
@InterfaceAudience.Private
public class RegionStates {
  private static final Log LOG = LogFactory.getLog(RegionStates.class);

  /**
   * Regions currently in transition.
   */
  final HashMap<String, RegionState> regionsInTransition;

  /**
   * Region encoded name to state map.
   * All the regions should be in this map.
   */
  private final HashMap<String, RegionState> regionStates;

  /**
   * Server to regions assignment map.
   * Contains the set of regions currently assigned to a given server.
   */
  private final Map<ServerName, Set<HRegionInfo>> serverHoldings;

  /**
   * Region to server assignment map.
   * Contains the server a given region is currently assigned to.
   */
  private final TreeMap<HRegionInfo, ServerName> regionAssignments;

  /**
   * Encoded region name to server assignment map for re-assignment
   * purpose. Contains the server a given region is last known assigned
   * to, which has not completed log splitting, so not assignable.
   * If a region is currently assigned, this server info in this
   * map should be the same as that in regionAssignments.
   * However the info in regionAssignments is cleared when the region
   * is offline while the info in lastAssignments is cleared when
   * the region is closed or the server is dead and processed.
   */
  private final HashMap<String, ServerName> lastAssignments;

  /**
   * Map a host port pair string to the latest start code
   * of a region server which is known to be dead. It is dead
   * to us, but server manager may not know it yet.
   */
  private final HashMap<String, Long> deadServers;

  /**
   * Map a dead servers to the time when log split is done.
   * Since log splitting is not ordered, we have to remember
   * all processed instances. The map is cleaned up based
   * on a configured time. By default, we assume a dead
   * server should be done with log splitting in two hours.
   */
  private final HashMap<ServerName, Long> processedServers;
  private long lastProcessedServerCleanTime;

  private final RegionStateStore regionStateStore;
  private final ServerManager serverManager;
  private final Server server;

  // The maximum time to keep a log split info in region states map
  static final String LOG_SPLIT_TIME = "hbase.master.maximum.logsplit.keeptime";
  static final long DEFAULT_LOG_SPLIT_TIME = 7200000L; // 2 hours

  RegionStates(final Server master,
      final ServerManager serverManager, final RegionStateStore regionStateStore) {
    regionStates = new HashMap<String, RegionState>();
    regionsInTransition = new HashMap<String, RegionState>();
    serverHoldings = new HashMap<ServerName, Set<HRegionInfo>>();
    regionAssignments = new TreeMap<HRegionInfo, ServerName>();
    lastAssignments = new HashMap<String, ServerName>();
    processedServers = new HashMap<ServerName, Long>();
    deadServers = new HashMap<String, Long>();
    this.regionStateStore = regionStateStore;
    this.serverManager = serverManager;
    this.server = master;
  }

  /**
   * @return an unmodifiable the region assignment map
   */
  public synchronized Map<HRegionInfo, ServerName> getRegionAssignments() {
    return Collections.unmodifiableMap(regionAssignments);
  }

  public synchronized ServerName getRegionServerOfRegion(HRegionInfo hri) {
    return regionAssignments.get(hri);
  }

  /**
   * Get regions in transition and their states
   */
  @SuppressWarnings("unchecked")
  public synchronized Map<String, RegionState> getRegionsInTransition() {
    return (Map<String, RegionState>)regionsInTransition.clone();
  }

  /**
   * @return True if specified region in transition.
   */
  public synchronized boolean isRegionInTransition(final HRegionInfo hri) {
    return regionsInTransition.containsKey(hri.getEncodedName());
  }

  /**
   * @return True if specified region in transition.
   */
  public synchronized boolean isRegionInTransition(final String encodedName) {
    return regionsInTransition.containsKey(encodedName);
  }

  /**
   * @return True if any region in transition.
   */
  public synchronized boolean isRegionsInTransition() {
    return !regionsInTransition.isEmpty();
  }

  /**
   * @return True if specified region assigned, and not in transition.
   */
  public synchronized boolean isRegionOnline(final HRegionInfo hri) {
    return !isRegionInTransition(hri) && regionAssignments.containsKey(hri);
  }

  /**
   * @return True if specified region offline/closed, but not in transition.
   * If the region is not in the map, it is offline to us too.
   */
  public synchronized boolean isRegionOffline(final HRegionInfo hri) {
    return getRegionState(hri) == null || (!isRegionInTransition(hri)
      && isRegionInState(hri, State.OFFLINE, State.CLOSED));
  }

  /**
   * @return True if specified region is in one of the specified states.
   */
  public boolean isRegionInState(
      final HRegionInfo hri, final State... states) {
    return isRegionInState(hri.getEncodedName(), states);
  }

  /**
   * @return True if specified region is in one of the specified states.
   */
  public boolean isRegionInState(
      final String encodedName, final State... states) {
    RegionState regionState = getRegionState(encodedName);
    return isOneOfStates(regionState, states);
  }

  /**
   * Wait for the state map to be updated by assignment manager.
   */
  public synchronized void waitForUpdate(
      final long timeout) throws InterruptedException {
    this.wait(timeout);
  }

  /**
   * Get region transition state
   */
  public RegionState getRegionTransitionState(final HRegionInfo hri) {
    return getRegionTransitionState(hri.getEncodedName());
  }

  /**
   * Get region transition state
   */
  public synchronized RegionState
      getRegionTransitionState(final String encodedName) {
    return regionsInTransition.get(encodedName);
  }

  /**
   * Add a list of regions to RegionStates. If a region is split
   * and offline, its state will be SPLIT. Otherwise, its state will
   * be OFFLINE. Region already in RegionStates will be skipped.
   */
  public void createRegionStates(
      final List<HRegionInfo> hris) {
    for (HRegionInfo hri: hris) {
      createRegionState(hri);
    }
  }

  /**
   * Add a region to RegionStates. If the region is split
   * and offline, its state will be SPLIT. Otherwise, its state will
   * be OFFLINE. If it is already in RegionStates, this call has
   * no effect, and the original state is returned.
   */
  public RegionState createRegionState(final HRegionInfo hri) {
    return createRegionState(hri, null, null);
  }

  /**
   * Add a region to RegionStates with the specified state.
   * If the region is already in RegionStates, this call has
   * no effect, and the original state is returned.
   */
  public synchronized RegionState createRegionState(
      final HRegionInfo hri, State newState, ServerName serverName) {
    if (newState == null || (newState == State.OPEN && serverName == null)) {
      newState =  State.OFFLINE;
    }
    if (hri.isOffline() && hri.isSplit()) {
      newState = State.SPLIT;
      serverName = null;
    }
    String encodedName = hri.getEncodedName();
    RegionState regionState = regionStates.get(encodedName);
    if (regionState != null) {
      LOG.warn("Tried to create a state for a region already in RegionStates, "
        + "used existing: " + regionState + ", ignored new: " + newState);
    } else {
      regionState = new RegionState(hri, newState, serverName);
      regionStates.put(encodedName, regionState);
      if (newState == State.OPEN) {
        regionAssignments.put(hri, serverName);
        lastAssignments.put(encodedName, serverName);
        Set<HRegionInfo> regions = serverHoldings.get(serverName);
        if (regions == null) {
          regions = new HashSet<HRegionInfo>();
          serverHoldings.put(serverName, regions);
        }
        regions.add(hri);
      } else if (!regionState.isUnassignable()) {
        regionsInTransition.put(encodedName, regionState);
      }
    }
    return regionState;
  }

  /**
   * Update a region state. It will be put in transition if not already there.
   */
  public RegionState updateRegionState(
      final HRegionInfo hri, final State state) {
    RegionState regionState = getRegionState(hri.getEncodedName());
    return updateRegionState(hri, state,
      regionState == null ? null : regionState.getServerName());
  }

  /**
   * Update a region state. It will be put in transition if not already there.
   *
   * If we can't find the region info based on the region name in
   * the transition, log a warning and return null.
   */
  public RegionState updateRegionState(
      final RegionTransition transition, final State state) {
    byte [] regionName = transition.getRegionName();
    HRegionInfo regionInfo = getRegionInfo(regionName);
    if (regionInfo == null) {
      String prettyRegionName = HRegionInfo.prettyPrint(
        HRegionInfo.encodeRegionName(regionName));
      LOG.warn("Failed to find region " + prettyRegionName
        + " in updating its state to " + state
        + " based on region transition " + transition);
      return null;
    }
    return updateRegionState(regionInfo, state,
      transition.getServerName());
  }

  /**
   * Update a region state. It will be put in transition if not already there.
   */
  public RegionState updateRegionState(
      final HRegionInfo hri, final State state, final ServerName serverName) {
    return updateRegionState(hri, state, serverName, HConstants.NO_SEQNUM);
  }

  public void regionOnline(
      final HRegionInfo hri, final ServerName serverName) {
    regionOnline(hri, serverName, HConstants.NO_SEQNUM);
  }

  /**
   * A region is online, won't be in transition any more.
   * We can't confirm it is really online on specified region server
   * because it hasn't been put in region server's online region list yet.
   */
  public void regionOnline(final HRegionInfo hri,
      final ServerName serverName, long openSeqNum) {
    if (!serverManager.isServerOnline(serverName)) {
      // This is possible if the region server dies before master gets a
      // chance to handle ZK event in time. At this time, if the dead server
      // is already processed by SSH, we should ignore this event.
      // If not processed yet, ignore and let SSH deal with it.
      LOG.warn("Ignored, " + hri.getEncodedName()
        + " was opened on a dead server: " + serverName);
      return;
    }
    updateRegionState(hri, State.OPEN, serverName, openSeqNum);

    synchronized (this) {
      regionsInTransition.remove(hri.getEncodedName());
      ServerName oldServerName = regionAssignments.put(hri, serverName);
      if (!serverName.equals(oldServerName)) {
        LOG.info("Onlined " + hri.getShortNameToLog() + " on " + serverName);
        Set<HRegionInfo> regions = serverHoldings.get(serverName);
        if (regions == null) {
          regions = new HashSet<HRegionInfo>();
          serverHoldings.put(serverName, regions);
        }
        regions.add(hri);
        if (oldServerName != null) {
          LOG.info("Offlined " + hri.getShortNameToLog() + " from " + oldServerName);
          Set<HRegionInfo> oldRegions = serverHoldings.get(oldServerName);
          oldRegions.remove(hri);
          if (oldRegions.isEmpty()) {
            serverHoldings.remove(oldServerName);
          }
        }
      }
    }
  }

  /**
   * A dead server's hlogs have been split so that all the regions
   * used to be open on it can be safely assigned now. Mark them assignable.
   */
  public synchronized void logSplit(final ServerName serverName) {
    for (Iterator<Map.Entry<String, ServerName>> it
        = lastAssignments.entrySet().iterator(); it.hasNext();) {
      Map.Entry<String, ServerName> e = it.next();
      if (e.getValue().equals(serverName)) {
        it.remove();
      }
    }
    long now = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding to processed servers " + serverName);
    }
    processedServers.put(serverName, Long.valueOf(now));
    Configuration conf = server.getConfiguration();
    long obsoleteTime = conf.getLong(LOG_SPLIT_TIME, DEFAULT_LOG_SPLIT_TIME);
    // Doesn't have to be very accurate about the clean up time
    if (now > lastProcessedServerCleanTime + obsoleteTime) {
      lastProcessedServerCleanTime = now;
      long cutoff = now - obsoleteTime;
      for (Iterator<Map.Entry<ServerName, Long>> it
          = processedServers.entrySet().iterator(); it.hasNext();) {
        Map.Entry<ServerName, Long> e = it.next();
        if (e.getValue().longValue() < cutoff) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Removed from processed servers " + e.getKey());
          }
          it.remove();
        }
      }
    }
  }

  /**
   * Log split is done for a given region, so it is assignable now.
   */
  public void logSplit(final HRegionInfo region) {
    clearLastAssignment(region);
  }

  public synchronized void clearLastAssignment(final HRegionInfo region) {
    lastAssignments.remove(region.getEncodedName());
  }

  /**
   * A region is offline, won't be in transition any more.
   */
  public void regionOffline(final HRegionInfo hri) {
    regionOffline(hri, null);
  }

  /**
   * A region is offline, won't be in transition any more. Its state
   * should be the specified expected state, which can only be
   * Split/Merged/Offline/null(=Offline)/SplittingNew/MergingNew.
   */
  public void regionOffline(
      final HRegionInfo hri, final State expectedState) {
    Preconditions.checkArgument(expectedState == null
      || RegionState.isUnassignable(expectedState),
        "Offlined region should not be " + expectedState);
    if (isRegionInState(hri, State.SPLITTING_NEW, State.MERGING_NEW)) {
      // Remove it from all region maps
      deleteRegion(hri);
      return;
    }
    State newState =
      expectedState == null ? State.OFFLINE : expectedState;
    updateRegionState(hri, newState);

    synchronized (this) {
      regionsInTransition.remove(hri.getEncodedName());
      ServerName oldServerName = regionAssignments.remove(hri);
      if (oldServerName != null && serverHoldings.containsKey(oldServerName)) {
        LOG.info("Offlined " + hri.getShortNameToLog() + " from " + oldServerName);
        Set<HRegionInfo> oldRegions = serverHoldings.get(oldServerName);
        oldRegions.remove(hri);
        if (oldRegions.isEmpty()) {
          serverHoldings.remove(oldServerName);
        }
      }
    }
  }

  /**
   * A server is offline, all regions on it are dead.
   */
  public synchronized List<HRegionInfo> serverOffline(
      final ZooKeeperWatcher watcher, final ServerName sn) {
    // Offline all regions on this server not already in transition.
    List<HRegionInfo> rits = new ArrayList<HRegionInfo>();
    Set<HRegionInfo> assignedRegions = serverHoldings.get(sn);
    if (assignedRegions == null) {
      assignedRegions = new HashSet<HRegionInfo>();
    }

    // Offline regions outside the loop to avoid ConcurrentModificationException
    Set<HRegionInfo> regionsToOffline = new HashSet<HRegionInfo>();
    for (HRegionInfo region : assignedRegions) {
      // Offline open regions, no need to offline if SPLIT/MERGED/OFFLINE
      if (isRegionOnline(region)) {
        regionsToOffline.add(region);
      } else {
        if (isRegionInState(region, State.SPLITTING, State.MERGING)) {
          LOG.debug("Offline splitting/merging region " + getRegionState(region));
          try {
            // Delete the ZNode if exists
            ZKAssign.deleteNodeFailSilent(watcher, region);
            regionsToOffline.add(region);
          } catch (KeeperException ke) {
            server.abort("Unexpected ZK exception deleting node " + region, ke);
          }
        }
      }
    }

    for (HRegionInfo hri : regionsToOffline) {
      regionOffline(hri);
    }

    for (RegionState state : regionsInTransition.values()) {
      HRegionInfo hri = state.getRegion();
      if (assignedRegions.contains(hri)) {
        // Region is open on this region server, but in transition.
        // This region must be moving away from this server, or splitting/merging.
        // SSH will handle it, either skip assigning, or re-assign.
        LOG.info("Transitioning " + state + " will be handled by SSH for " + sn);
      } else if (sn.equals(state.getServerName())) {
        // Region is in transition on this region server, and this
        // region is not open on this server. So the region must be
        // moving to this server from another one (i.e. opening or
        // pending open on this server, was open on another one.
        // Offline state is also kind of pending open if the region is in
        // transition. The region could be in failed_close state too if we have
        // tried several times to open it while this region server is not reachable)
        if (state.isPendingOpenOrOpening() || state.isFailedClose() || state.isOffline()) {
          LOG.info("Found region in " + state + " to be reassigned by SSH for " + sn);
          rits.add(hri);
        } else {
          LOG.warn("THIS SHOULD NOT HAPPEN: unexpected " + state);
        }
      }
    }

    this.notifyAll();
    return rits;
  }

  /**
   * Gets the online regions of the specified table.
   * This method looks at the in-memory state.  It does not go to <code>hbase:meta</code>.
   * Only returns <em>online</em> regions.  If a region on this table has been
   * closed during a disable, etc., it will be included in the returned list.
   * So, the returned list may not necessarily be ALL regions in this table, its
   * all the ONLINE regions in the table.
   * @param tableName
   * @return Online regions from <code>tableName</code>
   */
  public synchronized List<HRegionInfo> getRegionsOfTable(TableName tableName) {
    List<HRegionInfo> tableRegions = new ArrayList<HRegionInfo>();
    // boundary needs to have table's name but regionID 0 so that it is sorted
    // before all table's regions.
    HRegionInfo boundary = new HRegionInfo(tableName, null, null, false, 0L);
    for (HRegionInfo hri: regionAssignments.tailMap(boundary).keySet()) {
      if(!hri.getTable().equals(tableName)) break;
      tableRegions.add(hri);
    }
    return tableRegions;
  }


  /**
   * Wait on region to clear regions-in-transition.
   * <p>
   * If the region isn't in transition, returns immediately.  Otherwise, method
   * blocks until the region is out of transition.
   */
  public synchronized void waitOnRegionToClearRegionsInTransition(
      final HRegionInfo hri) throws InterruptedException {
    if (!isRegionInTransition(hri)) return;

    while(!server.isStopped() && isRegionInTransition(hri)) {
      RegionState rs = getRegionState(hri);
      LOG.info("Waiting on " + rs + " to clear regions-in-transition");
      waitForUpdate(100);
    }

    if (server.isStopped()) {
      LOG.info("Giving up wait on region in " +
        "transition because stoppable.isStopped is set");
    }
  }

  /**
   * A table is deleted. Remove its regions from all internal maps.
   * We loop through all regions assuming we don't delete tables too much.
   */
  public void tableDeleted(final TableName tableName) {
    Set<HRegionInfo> regionsToDelete = new HashSet<HRegionInfo>();
    synchronized (this) {
      for (RegionState state: regionStates.values()) {
        HRegionInfo region = state.getRegion();
        if (region.getTable().equals(tableName)) {
          regionsToDelete.add(region);
        }
      }
    }
    for (HRegionInfo region: regionsToDelete) {
      deleteRegion(region);
    }
  }

  /**
   * Checking if a region was assigned to a server which is not online now.
   * If so, we should hold re-assign this region till SSH has split its hlogs.
   * Once logs are split, the last assignment of this region will be reset,
   * which means a null last assignment server is ok for re-assigning.
   *
   * A region server could be dead but we don't know it yet. We may
   * think it's online falsely. Therefore if a server is online, we still
   * need to confirm it reachable and having the expected start code.
   */
  synchronized boolean wasRegionOnDeadServer(final String encodedName) {
    ServerName server = lastAssignments.get(encodedName);
    return isServerDeadAndNotProcessed(server);
  }

  synchronized boolean isServerDeadAndNotProcessed(ServerName server) {
    if (server == null) return false;
    if (serverManager.isServerOnline(server)) {
      String hostAndPort = server.getHostAndPort();
      long startCode = server.getStartcode();
      Long deadCode = deadServers.get(hostAndPort);
      if (deadCode == null || startCode > deadCode.longValue()) {
        if (serverManager.isServerReachable(server)) {
          return false;
        }
        // The size of deadServers won't grow unbounded.
        deadServers.put(hostAndPort, Long.valueOf(startCode));
      }
      // Watch out! If the server is not dead, the region could
      // remain unassigned. That's why ServerManager#isServerReachable
      // should use some retry.
      //
      // We cache this info since it is very unlikely for that
      // instance to come back up later on. We don't want to expire
      // the server since we prefer to let it die naturally.
      LOG.warn("Couldn't reach online server " + server);
    }
    // Now, we know it's dead. Check if it's processed
    return !processedServers.containsKey(server);
  }

 /**
   * Get the last region server a region was on for purpose of re-assignment,
   * i.e. should the re-assignment be held back till log split is done?
   */
  synchronized ServerName getLastRegionServerOfRegion(final String encodedName) {
    return lastAssignments.get(encodedName);
  }

  synchronized void setLastRegionServerOfRegions(
      final ServerName serverName, final List<HRegionInfo> regionInfos) {
    for (HRegionInfo hri: regionInfos) {
      setLastRegionServerOfRegion(serverName, hri.getEncodedName());
    }
  }

  synchronized void setLastRegionServerOfRegion(
      final ServerName serverName, final String encodedName) {
    lastAssignments.put(encodedName, serverName);
  }

  synchronized void closeAllUserRegions(Set<TableName> excludedTables) {
    Set<HRegionInfo> toBeClosed = new HashSet<HRegionInfo>(regionStates.size());
    for(RegionState state: regionStates.values()) {
      HRegionInfo hri = state.getRegion();
      TableName tableName = hri.getTable();
      if (!hri.isSplit() && !hri.isMetaRegion()
          && !excludedTables.contains(tableName)) {
        toBeClosed.add(hri);
      }
    }
    for (HRegionInfo hri: toBeClosed) {
      updateRegionState(hri, State.CLOSED);
    }
  }

  /**
   * Compute the average load across all region servers.
   * Currently, this uses a very naive computation - just uses the number of
   * regions being served, ignoring stats about number of requests.
   * @return the average load
   */
  protected synchronized double getAverageLoad() {
    int numServers = 0, totalLoad = 0;
    for (Map.Entry<ServerName, Set<HRegionInfo>> e: serverHoldings.entrySet()) {
      Set<HRegionInfo> regions = e.getValue();
      ServerName serverName = e.getKey();
      int regionCount = regions.size();
      if (serverManager.isServerOnline(serverName)) {
        totalLoad += regionCount;
        numServers++;
      }
    }
    return numServers == 0 ? 0.0 :
      (double)totalLoad / (double)numServers;
  }

  /**
   * This is an EXPENSIVE clone.  Cloning though is the safest thing to do.
   * Can't let out original since it can change and at least the load balancer
   * wants to iterate this exported list.  We need to synchronize on regions
   * since all access to this.servers is under a lock on this.regions.
   *
   * @return A clone of current assignments by table.
   */
  protected Map<TableName, Map<ServerName, List<HRegionInfo>>>
      getAssignmentsByTable() {
    Map<TableName, Map<ServerName, List<HRegionInfo>>> result =
      new HashMap<TableName, Map<ServerName,List<HRegionInfo>>>();
    synchronized (this) {
      if (!server.getConfiguration().getBoolean("hbase.master.loadbalance.bytable", false)) {
        Map<ServerName, List<HRegionInfo>> svrToRegions =
          new HashMap<ServerName, List<HRegionInfo>>(serverHoldings.size());
        for (Map.Entry<ServerName, Set<HRegionInfo>> e: serverHoldings.entrySet()) {
          svrToRegions.put(e.getKey(), new ArrayList<HRegionInfo>(e.getValue()));
        }
        result.put(TableName.valueOf("ensemble"), svrToRegions);
      } else {
        for (Map.Entry<ServerName, Set<HRegionInfo>> e: serverHoldings.entrySet()) {
          for (HRegionInfo hri: e.getValue()) {
            if (hri.isMetaRegion()) continue;
            TableName tablename = hri.getTable();
            Map<ServerName, List<HRegionInfo>> svrToRegions = result.get(tablename);
            if (svrToRegions == null) {
              svrToRegions = new HashMap<ServerName, List<HRegionInfo>>(serverHoldings.size());
              result.put(tablename, svrToRegions);
            }
            List<HRegionInfo> regions = svrToRegions.get(e.getKey());
            if (regions == null) {
              regions = new ArrayList<HRegionInfo>();
              svrToRegions.put(e.getKey(), regions);
            }
            regions.add(hri);
          }
        }
      }
    }

    Map<ServerName, ServerLoad>
      onlineSvrs = serverManager.getOnlineServers();
    // Take care of servers w/o assignments.
    for (Map<ServerName, List<HRegionInfo>> map: result.values()) {
      for (ServerName svr: onlineSvrs.keySet()) {
        if (!map.containsKey(svr)) {
          map.put(svr, new ArrayList<HRegionInfo>());
        }
      }
    }
    return result;
  }

  protected RegionState getRegionState(final HRegionInfo hri) {
    return getRegionState(hri.getEncodedName());
  }

  protected synchronized RegionState getRegionState(final String encodedName) {
    return regionStates.get(encodedName);
  }

  /**
   * Get the HRegionInfo from cache, if not there, from the hbase:meta table
   * @param  regionName
   * @return HRegionInfo for the region
   */
  protected HRegionInfo getRegionInfo(final byte [] regionName) {
    String encodedName = HRegionInfo.encodeRegionName(regionName);
    RegionState regionState = getRegionState(encodedName);
    if (regionState != null) {
      return regionState.getRegion();
    }

    try {
      Pair<HRegionInfo, ServerName> p =
        MetaReader.getRegion(server.getCatalogTracker(), regionName);
      HRegionInfo hri = p == null ? null : p.getFirst();
      if (hri != null) {
        createRegionState(hri);
      }
      return hri;
    } catch (IOException e) {
      server.abort("Aborting because error occoured while reading "
        + Bytes.toStringBinary(regionName) + " from hbase:meta", e);
      return null;
    }
  }

  static boolean isOneOfStates(RegionState regionState, State... states) {
    State s = regionState != null ? regionState.getState() : null;
    for (State state: states) {
      if (s == state) return true;
    }
    return false;
  }

  /**
   * Update a region state. It will be put in transition if not already there.
   */
  private RegionState updateRegionState(final HRegionInfo hri,
      final State state, final ServerName serverName, long openSeqNum) {
    if (state == State.FAILED_CLOSE || state == State.FAILED_OPEN) {
      LOG.warn("Failed to open/close " + hri.getShortNameToLog()
        + " on " + serverName + ", set to " + state);
    }

    String encodedName = hri.getEncodedName();
    RegionState regionState = new RegionState(
      hri, state, System.currentTimeMillis(), serverName);
    RegionState oldState = getRegionState(encodedName);
    if (!regionState.equals(oldState)) {
      LOG.info("Transition " + oldState + " to " + regionState);
      // Persist region state before updating in-memory info, if needed
      regionStateStore.updateRegionState(openSeqNum, regionState, oldState);
    }

    synchronized (this) {
      regionsInTransition.put(encodedName, regionState);
      regionStates.put(encodedName, regionState);

      // For these states, region should be properly closed.
      // There should be no log splitting issue.
      if ((state == State.CLOSED || state == State.MERGED
          || state == State.SPLIT) && lastAssignments.containsKey(encodedName)) {
        ServerName last = lastAssignments.get(encodedName);
        if (last.equals(serverName)) {
          lastAssignments.remove(encodedName);
        } else {
          LOG.warn(encodedName + " moved to " + state + " on "
            + serverName + ", expected " + last);
        }
      }

      // Once a region is opened, record its last assignment right away.
      if (serverName != null && state == State.OPEN) {
        ServerName last = lastAssignments.get(encodedName);
        if (!serverName.equals(last)) {
          lastAssignments.put(encodedName, serverName);
          if (last != null && isServerDeadAndNotProcessed(last)) {
            LOG.warn(encodedName + " moved to " + serverName
              + ", while it's previous host " + last
              + " is dead but not processed yet");
          }
        }
      }

      // notify the change
      this.notifyAll();
    }
    return regionState;
  }

  /**
   * Remove a region from all state maps.
   */
  private synchronized void deleteRegion(final HRegionInfo hri) {
    String encodedName = hri.getEncodedName();
    regionsInTransition.remove(encodedName);
    regionStates.remove(encodedName);
    lastAssignments.remove(encodedName);
    ServerName sn = regionAssignments.remove(hri);
    if (sn != null) {
      Set<HRegionInfo> regions = serverHoldings.get(sn);
      regions.remove(hri);
    }
  }
}
