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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
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
  private final Map<String, RegionState> regionStates;

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

  private final ServerManager serverManager;
  private final Server server;

  RegionStates(final Server master, final ServerManager serverManager) {
    regionStates = new HashMap<String, RegionState>();
    regionsInTransition = new HashMap<String, RegionState>();
    serverHoldings = new HashMap<ServerName, Set<HRegionInfo>>();
    regionAssignments = new TreeMap<HRegionInfo, ServerName>();
    this.serverManager = serverManager;
    this.server = master;
  }

  /**
   * @return an unmodifiable the region assignment map
   */
  @SuppressWarnings("unchecked")
  public synchronized Map<HRegionInfo, ServerName> getRegionAssignments() {
    return (Map<HRegionInfo, ServerName>)regionAssignments.clone();
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
  public synchronized boolean isRegionInTransition(final String regionName) {
    return regionsInTransition.containsKey(regionName);
  }

  /**
   * @return True if any region in transition.
   */
  public synchronized boolean isRegionsInTransition() {
    return !regionsInTransition.isEmpty();
  }

  /**
   * @return True if specified region assigned.
   */
  public synchronized boolean isRegionAssigned(final HRegionInfo hri) {
    return regionAssignments.containsKey(hri);
  }

  /**
   * @return True if specified region is in specified state
   */
  public synchronized boolean isRegionInState(
      final HRegionInfo hri, final State state) {
    RegionState regionState = getRegionState(hri);
    State s = regionState != null ? regionState.getState() : null;
    return s == state;
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
  public synchronized RegionState
      getRegionTransitionState(final HRegionInfo hri) {
    return regionsInTransition.get(hri.getEncodedName());
  }

  /**
   * Get region transition state
   */
  public synchronized RegionState
      getRegionTransitionState(final String regionName) {
    return regionsInTransition.get(regionName);
  }

  /**
   * Add a list of regions to RegionStates. If a region is split
   * and offline, its state will be SPLIT. Otherwise, its state will
   * be OFFLINE. Region already in RegionStates will be skipped.
   */
  public synchronized void createRegionStates(
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
  public synchronized RegionState createRegionState(final HRegionInfo hri) {
    State newState = (hri.isOffline() && hri.isSplit()) ? State.SPLIT : State.OFFLINE;
    String regionName = hri.getEncodedName();
    RegionState regionState = regionStates.get(regionName);
    if (regionState != null) {
      LOG.warn("Tried to create a state for a region already in RegionStates, "
        + "used existing: " + regionState + ", ignored new: " + newState);
    } else {
      regionState = new RegionState(hri, newState);
      regionStates.put(regionName, regionState);
    }
    return regionState;
  }

  /**
   * Update a region state. It will be put in transition if not already there.
   */
  public synchronized RegionState updateRegionState(
      final HRegionInfo hri, final State state) {
    RegionState regionState = regionStates.get(hri.getEncodedName());
    ServerName serverName = (regionState == null || state == State.CLOSED
      || state == State.OFFLINE) ? null : regionState.getServerName();
    return updateRegionState(hri, state, serverName);
  }

  /**
   * Update a region state. It will be put in transition if not already there.
   *
   * If we can't find the region info based on the region name in
   * the transition, log a warning and return null.
   */
  public synchronized RegionState updateRegionState(
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
  public synchronized RegionState updateRegionState(
      final HRegionInfo hri, final State state, final ServerName serverName) {
    ServerName newServerName = serverName;
    if (serverName != null &&
        (state == State.CLOSED || state == State.OFFLINE)) {
      LOG.warn("Closed " + hri.getShortNameToLog() + " still on "
        + serverName + "? Ignored, reset it to null");
      newServerName = null;
    }

    if (state == State.FAILED_CLOSE || state == State.FAILED_OPEN) {
      LOG.warn("Failed to transition " + hri.getShortNameToLog()
        + " on " + serverName + ", set to " + state);
    }

    String regionName = hri.getEncodedName();
    RegionState regionState = new RegionState(
      hri, state, System.currentTimeMillis(), newServerName);
    RegionState oldState = regionStates.put(regionName, regionState);
    if (oldState == null || oldState.getState() != regionState.getState()) {
      LOG.info("Transitioned " + oldState + " to " + regionState);
    }
    if (newServerName != null || (
        state != State.PENDING_CLOSE && state != State.CLOSING)) {
      regionsInTransition.put(regionName, regionState);
    }

    // notify the change
    this.notifyAll();
    return regionState;
  }

  /**
   * A region is online, won't be in transition any more.
   * We can't confirm it is really online on specified region server
   * because it hasn't been put in region server's online region list yet.
   */
  public synchronized void regionOnline(
      final HRegionInfo hri, final ServerName serverName) {
    String regionName = hri.getEncodedName();
    RegionState oldState = regionStates.get(regionName);
    if (oldState == null) {
      LOG.warn("Online region not in RegionStates: " + hri.getShortNameToLog());
    } else {
      State state = oldState.getState();
      ServerName sn = oldState.getServerName();
      if (state != State.OPEN || sn == null || !sn.equals(serverName)) {
        LOG.debug("Online " + hri.getShortNameToLog() + " with current state=" + state +
          ", expected state=OPEN" + ", assigned to server: " + sn + " expected " + serverName);
      }
    }
    updateRegionState(hri, State.OPEN, serverName);
    regionsInTransition.remove(regionName);

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
        serverHoldings.get(oldServerName).remove(hri);
      }
    }
  }

  /**
   * A region is offline, won't be in transition any more.
   */
  public void regionOffline(final HRegionInfo hri) {
    regionOffline(hri, null);
  }

  /**
   * A region is offline, won't be in transition any more.
   * Its state should be the specified expected state, which
   * can be Split/Merged/Offline/null(=Offline) only.
   */
  public synchronized void regionOffline(
      final HRegionInfo hri, final State expectedState) {
    Preconditions.checkArgument(expectedState == null
      || expectedState == State.OFFLINE || expectedState == State.SPLIT
      || expectedState == State.MERGED, "Offlined region should be in state"
        + " OFFLINE/SPLIT/MERGED instead of " + expectedState);
    String regionName = hri.getEncodedName();
    RegionState oldState = regionStates.get(regionName);
    if (oldState == null) {
      LOG.warn("Offline region not in RegionStates: " + hri.getShortNameToLog());
    } else if (LOG.isDebugEnabled()) {
      State state = oldState.getState();
      ServerName sn = oldState.getServerName();
      if (state != State.OFFLINE
          && state != State.SPLITTING && state != State.MERGING) {
        LOG.debug("Offline " + hri.getShortNameToLog() + " with current state="
          + state + ", expected state=OFFLINE/SPLITTING/MERGING");
      }
      if (sn != null && state == State.OFFLINE) {
        LOG.debug("Offline " + hri.getShortNameToLog()
          + " with current state=OFFLINE, assigned to server: "
          + sn + ", expected null");
      }
    }
    State newState = expectedState;
    if (newState == null) newState = State.OFFLINE;
    updateRegionState(hri, newState);
    regionsInTransition.remove(regionName);

    ServerName oldServerName = regionAssignments.remove(hri);
    if (oldServerName != null) {
      LOG.info("Offlined " + hri.getShortNameToLog() + " from " + oldServerName);
      serverHoldings.get(oldServerName).remove(hri);
    }
  }

  /**
   * A server is offline, all regions on it are dead.
   */
  public synchronized List<HRegionInfo> serverOffline(
      final ZooKeeperWatcher watcher, final ServerName sn) {
    // Clean up this server from map of servers to regions, and remove all regions
    // of this server from online map of regions.
    List<HRegionInfo> rits = new ArrayList<HRegionInfo>();
    Set<HRegionInfo> assignedRegions = serverHoldings.remove(sn);
    if (assignedRegions == null) {
      assignedRegions = new HashSet<HRegionInfo>();
    }

    for (HRegionInfo region : assignedRegions) {
      regionAssignments.remove(region);
    }

    Set<HRegionInfo> regionsToOffline = new HashSet<HRegionInfo>();
    for (RegionState state : regionsInTransition.values()) {
      HRegionInfo hri = state.getRegion();
      if (assignedRegions.contains(hri)) {
        // Region is open on this region server, but in transition.
        // This region must be moving away from this server, or splitting/merging.
        // SSH will handle it, either skip assigning, or re-assign.
        LOG.info("Transitioning " + state + " will be handled by SSH for " + sn);
        if (state.isSplitting() || state.isMerging()) {
          LOG.info("Offline splitting/merging region " + state);
          try {
            // Delete the ZNode if exists
            ZKAssign.deleteNodeFailSilent(watcher, hri);
            // Offline regions outside the loop to avoid ConcurrentModificationException
            regionsToOffline.add(hri);
          } catch (KeeperException ke) {
            server.abort("Unexpected ZK exception deleting node " + hri, ke);
          }
        }
      } else if (sn.equals(state.getServerName())) {
        // Region is in transition on this region server, and this
        // region is not open on this server. So the region must be
        // moving to this server from another one (i.e. opening or
        // pending open on this server, was open on another one
        if (state.isPendingOpen() || state.isOpening()) {
          LOG.info("Found opening region " + state + " to be reassigned by SSH for " + sn);
          rits.add(hri);
        } else {
          LOG.warn("THIS SHOULD NOT HAPPEN: unexpected state "
            + state + " of region in transition on server " + sn);
        }
      }
    }
    for (HRegionInfo hri : regionsToOffline) {
      regionOffline(hri);
    }

    assignedRegions.clear();
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
   * Waits until the specified region has completed assignment.
   * <p>
   * If the region is already assigned, returns immediately.  Otherwise, method
   * blocks until the region is assigned.
   */
  public synchronized void waitForAssignment(
      final HRegionInfo hri) throws InterruptedException {
    if (!isRegionAssigned(hri)) return;

    while(!server.isStopped() && !isRegionAssigned(hri)) {
      RegionState rs = getRegionState(hri);
      LOG.info("Waiting on " + rs + " to be assigned");
      waitForUpdate(100);
    }

    if (server.isStopped()) {
      LOG.info("Giving up wait on region " +
        "assignment because stoppable.isStopped is set");
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
      if (regionCount > 0 || serverManager.isServerOnline(serverName)) {
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

  protected synchronized RegionState getRegionState(final HRegionInfo hri) {
    return regionStates.get(hri.getEncodedName());
  }

  protected synchronized RegionState getRegionState(final String regionName) {
    return regionStates.get(regionName);
  }

  /**
   * Get the HRegionInfo from cache, if not there, from the hbase:meta table
   * @param  regionName
   * @return HRegionInfo for the region
   */
  protected HRegionInfo getRegionInfo(final byte [] regionName) {
    String encodedName = HRegionInfo.encodeRegionName(regionName);
    RegionState regionState = regionStates.get(encodedName);
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
}
