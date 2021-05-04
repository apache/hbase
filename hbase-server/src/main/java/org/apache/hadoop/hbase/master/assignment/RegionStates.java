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
package org.apache.hadoop.hbase.master.assignment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RegionStates contains a set of Maps that describes the in-memory state of the AM, with
 * the regions available in the system, the region in transition, the offline regions and
 * the servers holding regions.
 */
@InterfaceAudience.Private
public class RegionStates {
  private static final Logger LOG = LoggerFactory.getLogger(RegionStates.class);

  // This comparator sorts the RegionStates by time stamp then Region name.
  // Comparing by timestamp alone can lead us to discard different RegionStates that happen
  // to share a timestamp.
  private static class RegionStateStampComparator implements Comparator<RegionState> {
    @Override
    public int compare(final RegionState l, final RegionState r) {
      int stampCmp = Long.compare(l.getStamp(), r.getStamp());
      return stampCmp != 0 ? stampCmp : RegionInfo.COMPARATOR.compare(l.getRegion(), r.getRegion());
    }
  }

  public final static RegionStateStampComparator REGION_STATE_STAMP_COMPARATOR =
      new RegionStateStampComparator();

  // TODO: Replace the ConcurrentSkipListMaps
  /**
   * RegionName -- i.e. RegionInfo.getRegionName() -- as bytes to {@link RegionStateNode}
   */
  private final ConcurrentSkipListMap<byte[], RegionStateNode> regionsMap =
      new ConcurrentSkipListMap<byte[], RegionStateNode>(Bytes.BYTES_COMPARATOR);

  private final ConcurrentSkipListMap<RegionInfo, RegionStateNode> regionInTransition =
    new ConcurrentSkipListMap<RegionInfo, RegionStateNode>(RegionInfo.COMPARATOR);

  /**
   * Regions marked as offline on a read of hbase:meta. Unused or at least, once
   * offlined, regions have no means of coming on line again. TODO.
   */
  private final ConcurrentSkipListMap<RegionInfo, RegionStateNode> regionOffline =
    new ConcurrentSkipListMap<RegionInfo, RegionStateNode>();

  private final ConcurrentSkipListMap<byte[], RegionFailedOpen> regionFailedOpen =
    new ConcurrentSkipListMap<byte[], RegionFailedOpen>(Bytes.BYTES_COMPARATOR);

  private final ConcurrentHashMap<ServerName, ServerStateNode> serverMap =
      new ConcurrentHashMap<ServerName, ServerStateNode>();

  public RegionStates() { }

  /**
   * Called on stop of AssignmentManager.
   */
  public void clear() {
    regionsMap.clear();
    regionInTransition.clear();
    regionOffline.clear();
    serverMap.clear();
  }

  public boolean isRegionInRegionStates(final RegionInfo hri) {
    return (regionsMap.containsKey(hri.getRegionName()) || regionInTransition.containsKey(hri)
        || regionOffline.containsKey(hri));
  }

  // ==========================================================================
  //  RegionStateNode helpers
  // ==========================================================================
  RegionStateNode createRegionStateNode(RegionInfo regionInfo) {
    RegionStateNode newNode = new RegionStateNode(regionInfo, regionInTransition);
    RegionStateNode oldNode = regionsMap.putIfAbsent(regionInfo.getRegionName(), newNode);
    return oldNode != null ? oldNode : newNode;
  }

  public RegionStateNode getOrCreateRegionStateNode(RegionInfo regionInfo) {
    RegionStateNode node = getRegionStateNodeFromName(regionInfo.getRegionName());
    return node != null ? node : createRegionStateNode(regionInfo);
  }

  RegionStateNode getRegionStateNodeFromName(byte[] regionName) {
    return regionsMap.get(regionName);
  }

  public RegionStateNode getRegionStateNode(RegionInfo regionInfo) {
    return getRegionStateNodeFromName(regionInfo.getRegionName());
  }

  public void deleteRegion(final RegionInfo regionInfo) {
    regionsMap.remove(regionInfo.getRegionName());
    // See HBASE-20860
    // After master restarts, merged regions' RIT state may not be cleaned,
    // making sure they are cleaned here
    if (regionInTransition.containsKey(regionInfo)) {
      regionInTransition.remove(regionInfo);
    }
    // Remove from the offline regions map too if there.
    if (this.regionOffline.containsKey(regionInfo)) {
      if (LOG.isTraceEnabled()) LOG.trace("Removing from regionOffline Map: " + regionInfo);
      this.regionOffline.remove(regionInfo);
    }
  }

  public void deleteRegions(final List<RegionInfo> regionInfos) {
    regionInfos.forEach(this::deleteRegion);
  }

  List<RegionStateNode> getTableRegionStateNodes(final TableName tableName) {
    final ArrayList<RegionStateNode> regions = new ArrayList<RegionStateNode>();
    for (RegionStateNode node: regionsMap.tailMap(tableName.getName()).values()) {
      if (!node.getTable().equals(tableName)) break;
      regions.add(node);
    }
    return regions;
  }

  ArrayList<RegionState> getTableRegionStates(final TableName tableName) {
    final ArrayList<RegionState> regions = new ArrayList<RegionState>();
    for (RegionStateNode node: regionsMap.tailMap(tableName.getName()).values()) {
      if (!node.getTable().equals(tableName)) break;
      regions.add(node.toRegionState());
    }
    return regions;
  }

  ArrayList<RegionInfo> getTableRegionsInfo(final TableName tableName) {
    final ArrayList<RegionInfo> regions = new ArrayList<RegionInfo>();
    for (RegionStateNode node: regionsMap.tailMap(tableName.getName()).values()) {
      if (!node.getTable().equals(tableName)) break;
      regions.add(node.getRegionInfo());
    }
    return regions;
  }

  /** @return A view of region state nodes for all the regions. */
  public Collection<RegionStateNode> getRegionStateNodes() {
    return Collections.unmodifiableCollection(regionsMap.values());
  }

  /** @return A snapshot of region state nodes for all the regions. */
  public ArrayList<RegionState> getRegionStates() {
    final ArrayList<RegionState> regions = new ArrayList<>(regionsMap.size());
    for (RegionStateNode node: regionsMap.values()) {
      regions.add(node.toRegionState());
    }
    return regions;
  }

  // ==========================================================================
  //  RegionState helpers
  // ==========================================================================
  public RegionState getRegionState(final RegionInfo regionInfo) {
    RegionStateNode regionStateNode = getRegionStateNode(regionInfo);
    return regionStateNode == null ? null : regionStateNode.toRegionState();
  }

  public RegionState getRegionState(final String encodedRegionName) {
    // TODO: Need a map <encodedName, ...> but it is just dispatch merge...
    for (RegionStateNode node: regionsMap.values()) {
      if (node.getRegionInfo().getEncodedName().equals(encodedRegionName)) {
        return node.toRegionState();
      }
    }
    return null;
  }

  // ============================================================================================
  //  TODO: helpers
  // ============================================================================================
  public boolean hasTableRegionStates(final TableName tableName) {
    // TODO
    return !getTableRegionStates(tableName).isEmpty();
  }

  /**
   * @return Return online regions of table; does not include OFFLINE or SPLITTING regions.
   */
  public List<RegionInfo> getRegionsOfTable(TableName table) {
    return getRegionsOfTable(table,
      regionNode -> !regionNode.isInState(State.OFFLINE, State.SPLIT) &&
        !regionNode.getRegionInfo().isSplitParent());
  }

  private HRegionLocation createRegionForReopen(RegionStateNode node) {
    node.lock();
    try {
      if (!include(node, false)) {
        return null;
      }
      if (node.isInState(State.OPEN)) {
        return new HRegionLocation(node.getRegionInfo(), node.getRegionLocation(),
          node.getOpenSeqNum());
      } else if (node.isInState(State.OPENING)) {
        return new HRegionLocation(node.getRegionInfo(), node.getRegionLocation(), -1);
      } else {
        return null;
      }
    } finally {
      node.unlock();
    }
  }

  /**
   * Get the regions to be reopened when modifying a table.
   * <p/>
   * Notice that the {@code openSeqNum} in the returned HRegionLocation is also used to indicate the
   * state of this region, positive means the region is in {@link State#OPEN}, -1 means
   * {@link State#OPENING}. And for regions in other states we do not need reopen them.
   */
  public List<HRegionLocation> getRegionsOfTableForReopen(TableName tableName) {
    return getTableRegionStateNodes(tableName).stream().map(this::createRegionForReopen)
      .filter(r -> r != null).collect(Collectors.toList());
  }

  /**
   * Check whether the region has been reopened. The meaning of the {@link HRegionLocation} is the
   * same with {@link #getRegionsOfTableForReopen(TableName)}.
   * <p/>
   * For a region which is in {@link State#OPEN} before, if the region state is changed or the open
   * seq num is changed, we can confirm that it has been reopened.
   * <p/>
   * For a region which is in {@link State#OPENING} before, usually it will be in {@link State#OPEN}
   * now and we will schedule a MRP to reopen it. But there are several exceptions:
   * <ul>
   * <li>The region is in state other than {@link State#OPEN} or {@link State#OPENING}.</li>
   * <li>The location of the region has been changed</li>
   * </ul>
   * Of course the region could still be in {@link State#OPENING} state and still on the same
   * server, then here we will still return a {@link HRegionLocation} for it, just like
   * {@link #getRegionsOfTableForReopen(TableName)}.
   * @param oldLoc the previous state/location of this region
   * @return null if the region has been reopened, otherwise a new {@link HRegionLocation} which
   *         means we still need to reopen the region.
   * @see #getRegionsOfTableForReopen(TableName)
   */
  public HRegionLocation checkReopened(HRegionLocation oldLoc) {
    RegionStateNode node = getRegionStateNode(oldLoc.getRegion());
    // HBASE-20921
    // if the oldLoc's state node does not exist, that means the region is
    // merged or split, no need to check it
    if (node == null) {
      return null;
    }
    node.lock();
    try {
      if (oldLoc.getSeqNum() >= 0) {
        // in OPEN state before
        if (node.isInState(State.OPEN)) {
          if (node.getOpenSeqNum() > oldLoc.getSeqNum()) {
            // normal case, the region has been reopened
            return null;
          } else {
            // the open seq num does not change, need to reopen again
            return new HRegionLocation(node.getRegionInfo(), node.getRegionLocation(),
              node.getOpenSeqNum());
          }
        } else {
          // the state has been changed so we can make sure that the region has been reopened(not
          // finished maybe, but not a problem).
          return null;
        }
      } else {
        // in OPENING state before
        if (!node.isInState(State.OPEN, State.OPENING)) {
          // not in OPEN or OPENING state, then we can make sure that the region has been
          // reopened(not finished maybe, but not a problem)
          return null;
        } else {
          if (!node.getRegionLocation().equals(oldLoc.getServerName())) {
            // the region has been moved, so we can make sure that the region has been reopened.
            return null;
          }
          // normal case, we are still in OPENING state, or the reopen has been opened and the state
          // is changed to OPEN.
          long openSeqNum = node.isInState(State.OPEN) ? node.getOpenSeqNum() : -1;
          return new HRegionLocation(node.getRegionInfo(), node.getRegionLocation(), openSeqNum);
        }
      }
    } finally {
      node.unlock();
    }
  }

  /**
   * Get the regions for enabling a table.
   * <p/>
   * Here we want the EnableTableProcedure to be more robust and can be used to fix some nasty
   * states, so the checks in this method will be a bit strange. In general, a region can only be
   * offline when it is split, for merging we will just delete the parent regions, but with HBCK we
   * may force update the state of a region to fix some nasty bugs, so in this method we will try to
   * bring the offline regions back if it is not split. That's why we only check for split state
   * here.
   */
  public List<RegionInfo> getRegionsOfTableForEnabling(TableName table) {
    return getRegionsOfTable(table,
      regionNode -> !regionNode.isInState(State.SPLIT) && !regionNode.getRegionInfo().isSplit());
  }

  /**
   * Get the regions for deleting a table.
   * <p/>
   * Here we need to return all the regions irrespective of the states in order to archive them
   * all. This is because if we don't archive OFFLINE/SPLIT regions and if a snapshot or a cloned
   * table references to the regions, we will lose the data of the regions.
   */
  public List<RegionInfo> getRegionsOfTableForDeleting(TableName table) {
    return getTableRegionStateNodes(table).stream().map(RegionStateNode::getRegionInfo)
      .collect(Collectors.toList());
  }

  /**
   * @return Return the regions of the table and filter them.
   */
  private List<RegionInfo> getRegionsOfTable(TableName table, Predicate<RegionStateNode> filter) {
    return getTableRegionStateNodes(table).stream().filter(filter).map(n -> n.getRegionInfo())
      .collect(Collectors.toList());
  }

  /**
   * Utility. Whether to include region in list of regions. Default is to
   * weed out split and offline regions.
   * @return True if we should include the <code>node</code> (do not include
   * if split or offline unless <code>offline</code> is set to true.
   */
  private boolean include(final RegionStateNode node, final boolean offline) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("WORKING ON " + node + " " + node.getRegionInfo());
    }
    final RegionInfo hri = node.getRegionInfo();
    if (node.isInState(State.SPLIT) || hri.isSplit()) {
      return false;
    }
    if ((node.isInState(State.OFFLINE) || hri.isOffline()) && !offline) {
      return false;
    }
    return (!hri.isOffline() && !hri.isSplit()) ||
        ((hri.isOffline() || hri.isSplit()) && offline);
  }

  // ============================================================================================
  // Split helpers
  // These methods will only be called in ServerCrashProcedure, and at the end of SCP we will remove
  // the ServerStateNode by calling removeServer.
  // ============================================================================================

  private void setServerState(ServerName serverName, ServerState state) {
    ServerStateNode serverNode = getOrCreateServer(serverName);
    synchronized (serverNode) {
      serverNode.setState(state);
    }
  }

  /**
   * Call this when we start meta log splitting a crashed Server.
   * @see #metaLogSplit(ServerName)
   */
  public void metaLogSplitting(ServerName serverName) {
    setServerState(serverName, ServerState.SPLITTING_META);
  }

  /**
   * Called after we've split the meta logs on a crashed Server.
   * @see #metaLogSplitting(ServerName)
   */
  public void metaLogSplit(ServerName serverName) {
    setServerState(serverName, ServerState.SPLITTING_META_DONE);
  }

  /**
   * Call this when we start log splitting for a crashed Server.
   * @see #logSplit(ServerName)
   */
  public void logSplitting(final ServerName serverName) {
    setServerState(serverName, ServerState.SPLITTING);
  }

  /**
   * Called after we've split all logs on a crashed Server.
   * @see #logSplitting(ServerName)
   */
  public void logSplit(final ServerName serverName) {
    setServerState(serverName, ServerState.OFFLINE);
  }

  public void updateRegionState(RegionInfo regionInfo, State state) {
    RegionStateNode regionNode = getOrCreateRegionStateNode(regionInfo);
    regionNode.lock();
    try {
      regionNode.setState(state);
    } finally {
      regionNode.unlock();
    }
  }

  // ============================================================================================
  //  TODO:
  // ============================================================================================
  public List<RegionInfo> getAssignedRegions() {
    final List<RegionInfo> result = new ArrayList<RegionInfo>();
    for (RegionStateNode node: regionsMap.values()) {
      if (!node.isInTransition()) {
        result.add(node.getRegionInfo());
      }
    }
    return result;
  }

  public boolean isRegionInState(RegionInfo regionInfo, State... state) {
    RegionStateNode regionNode = getRegionStateNode(regionInfo);
    if (regionNode != null) {
      regionNode.lock();
      try {
        return regionNode.isInState(state);
      } finally {
        regionNode.unlock();
      }
    }
    return false;
  }

  public boolean isRegionOnline(final RegionInfo regionInfo) {
    return isRegionInState(regionInfo, State.OPEN);
  }

  /**
   * @return True if region is offline (In OFFLINE or CLOSED state).
   */
  public boolean isRegionOffline(final RegionInfo regionInfo) {
    return isRegionInState(regionInfo, State.OFFLINE, State.CLOSED);
  }

  public Map<ServerName, List<RegionInfo>> getSnapShotOfAssignment(
      final Collection<RegionInfo> regions) {
    final Map<ServerName, List<RegionInfo>> result = new HashMap<ServerName, List<RegionInfo>>();
    if (regions != null) {
      for (RegionInfo hri : regions) {
        final RegionStateNode node = getRegionStateNode(hri);
        if (node == null) {
          continue;
        }
        createSnapshot(node, result);
      }
    } else {
      for (RegionStateNode node : regionsMap.values()) {
        if (node == null) {
          continue;
        }
        createSnapshot(node, result);
      }
    }
    return result;
  }

  private void createSnapshot(RegionStateNode node, Map<ServerName, List<RegionInfo>> result) {
    final ServerName serverName = node.getRegionLocation();
    if (serverName == null) {
      return;
    }

    List<RegionInfo> serverRegions = result.get(serverName);
    if (serverRegions == null) {
      serverRegions = new ArrayList<RegionInfo>();
      result.put(serverName, serverRegions);
    }
    serverRegions.add(node.getRegionInfo());
  }

  public Map<RegionInfo, ServerName> getRegionAssignments() {
    final HashMap<RegionInfo, ServerName> assignments = new HashMap<RegionInfo, ServerName>();
    for (RegionStateNode node: regionsMap.values()) {
      assignments.put(node.getRegionInfo(), node.getRegionLocation());
    }
    return assignments;
  }

  public Map<RegionState.State, List<RegionInfo>> getRegionByStateOfTable(TableName tableName) {
    final State[] states = State.values();
    final Map<RegionState.State, List<RegionInfo>> tableRegions =
        new HashMap<State, List<RegionInfo>>(states.length);
    for (int i = 0; i < states.length; ++i) {
      tableRegions.put(states[i], new ArrayList<RegionInfo>());
    }

    for (RegionStateNode node: regionsMap.values()) {
      if (node.getTable().equals(tableName)) {
        tableRegions.get(node.getState()).add(node.getRegionInfo());
      }
    }
    return tableRegions;
  }

  public ServerName getRegionServerOfRegion(RegionInfo regionInfo) {
    RegionStateNode regionNode = getRegionStateNode(regionInfo);
    if (regionNode != null) {
      regionNode.lock();
      try {
        ServerName server = regionNode.getRegionLocation();
        return server != null ? server : regionNode.getLastHost();
      } finally {
        regionNode.unlock();
      }
    }
    return null;
  }

  /**
   * This is an EXPENSIVE clone.  Cloning though is the safest thing to do.
   * Can't let out original since it can change and at least the load balancer
   * wants to iterate this exported list.  We need to synchronize on regions
   * since all access to this.servers is under a lock on this.regions.
   *
   * @return A clone of current open or opening assignments.
   */
  public Map<TableName, Map<ServerName, List<RegionInfo>>> getAssignmentsForBalancer(
      TableStateManager tableStateManager, List<ServerName> onlineServers) {
    final Map<TableName, Map<ServerName, List<RegionInfo>>> result = new HashMap<>();
    for (RegionStateNode node : regionsMap.values()) {
      // DisableTableProcedure first sets the table state to DISABLED and then force unassigns
      // the regions in a loop. The balancer should ignore all regions for tables in DISABLED
      // state because even if still currently open we expect them to be offlined very soon.
      if (isTableDisabled(tableStateManager, node.getTable())) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Ignoring {} because table is disabled", node);
        }
        continue;
      }
      // When balancing, we are only interested in OPEN or OPENING regions. These can be
      // expected to remain online until the next balancer iteration or unless the balancer
      // decides to move it. Regions in other states are not eligible for balancing, because
      // they are closing, splitting, merging, or otherwise already in transition.
      if (!node.isInState(State.OPEN, State.OPENING)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Ignoring {} because region is not OPEN or OPENING", node);
        }
        continue;
      }
      Map<ServerName, List<RegionInfo>> tableResult =
          result.computeIfAbsent(node.getTable(), t -> new HashMap<>());
      final ServerName serverName = node.getRegionLocation();
      // A region in ONLINE or OPENING state should have a location.
      if (serverName == null) {
        LOG.warn("Skipping, no server for {}", node);
        continue;
      }
      List<RegionInfo> serverResult =
          tableResult.computeIfAbsent(serverName, s -> new ArrayList<>());
      serverResult.add(node.getRegionInfo());
    }
    // Add online servers with no assignment for the table.
    for (Map<ServerName, List<RegionInfo>> table : result.values()) {
      for (ServerName serverName : onlineServers) {
        table.computeIfAbsent(serverName, key -> new ArrayList<>());
      }
    }
    return result;
  }

  private boolean isTableDisabled(final TableStateManager tableStateManager,
      final TableName tableName) {
    return tableStateManager.isTableState(tableName, TableState.State.DISABLED,
      TableState.State.DISABLING);
  }

  // ==========================================================================
  //  Region in transition helpers
  // ==========================================================================
  public boolean hasRegionsInTransition() {
    return !regionInTransition.isEmpty();
  }

  public boolean isRegionInTransition(final RegionInfo regionInfo) {
    final RegionStateNode node = regionInTransition.get(regionInfo);
    return node != null ? node.isInTransition() : false;
  }

  public RegionState getRegionTransitionState(RegionInfo hri) {
    RegionStateNode node = regionInTransition.get(hri);
    if (node == null) {
      return null;
    }

    node.lock();
    try {
      return node.isInTransition() ? node.toRegionState() : null;
    } finally {
      node.unlock();
    }
  }

  public List<RegionStateNode> getRegionsInTransition() {
    return new ArrayList<RegionStateNode>(regionInTransition.values());
  }

  /**
   * Get the number of regions in transition.
   */
  public int getRegionsInTransitionCount() {
    return regionInTransition.size();
  }

  public List<RegionState> getRegionsStateInTransition() {
    final List<RegionState> rit = new ArrayList<RegionState>(regionInTransition.size());
    for (RegionStateNode node: regionInTransition.values()) {
      rit.add(node.toRegionState());
    }
    return rit;
  }

  public SortedSet<RegionState> getRegionsInTransitionOrderedByTimestamp() {
    final SortedSet<RegionState> rit = new TreeSet<RegionState>(REGION_STATE_STAMP_COMPARATOR);
    for (RegionStateNode node: regionInTransition.values()) {
      rit.add(node.toRegionState());
    }
    return rit;
  }

  // ==========================================================================
  //  Region offline helpers
  // ==========================================================================
  // TODO: Populated when we read meta but regions never make it out of here.
  public void addToOfflineRegions(final RegionStateNode regionNode) {
    LOG.info("Added to offline, CURRENTLY NEVER CLEARED!!! " + regionNode);
    regionOffline.put(regionNode.getRegionInfo(), regionNode);
  }

  // TODO: Unused.
  public void removeFromOfflineRegions(final RegionInfo regionInfo) {
    regionOffline.remove(regionInfo);
  }

  // ==========================================================================
  //  Region FAIL_OPEN helpers
  // ==========================================================================
  public static final class RegionFailedOpen {
    private final RegionStateNode regionNode;

    private volatile Exception exception = null;
    private AtomicInteger retries = new AtomicInteger();

    public RegionFailedOpen(final RegionStateNode regionNode) {
      this.regionNode = regionNode;
    }

    public RegionStateNode getRegionStateNode() {
      return regionNode;
    }

    public RegionInfo getRegionInfo() {
      return regionNode.getRegionInfo();
    }

    public int incrementAndGetRetries() {
      return this.retries.incrementAndGet();
    }

    public int getRetries() {
      return retries.get();
    }

    public void setException(final Exception exception) {
      this.exception = exception;
    }

    public Exception getException() {
      return this.exception;
    }
  }

  public RegionFailedOpen addToFailedOpen(final RegionStateNode regionNode) {
    final byte[] key = regionNode.getRegionInfo().getRegionName();
    RegionFailedOpen node = regionFailedOpen.get(key);
    if (node == null) {
      RegionFailedOpen newNode = new RegionFailedOpen(regionNode);
      RegionFailedOpen oldNode = regionFailedOpen.putIfAbsent(key, newNode);
      node = oldNode != null ? oldNode : newNode;
    }
    return node;
  }

  public RegionFailedOpen getFailedOpen(final RegionInfo regionInfo) {
    return regionFailedOpen.get(regionInfo.getRegionName());
  }

  public void removeFromFailedOpen(final RegionInfo regionInfo) {
    regionFailedOpen.remove(regionInfo.getRegionName());
  }

  public List<RegionState> getRegionFailedOpen() {
    if (regionFailedOpen.isEmpty()) return Collections.emptyList();

    ArrayList<RegionState> regions = new ArrayList<RegionState>(regionFailedOpen.size());
    for (RegionFailedOpen r: regionFailedOpen.values()) {
      regions.add(r.getRegionStateNode().toRegionState());
    }
    return regions;
  }

  // ==========================================================================
  //  Servers
  // ==========================================================================

  /**
   * Be judicious calling this method. Do it on server register ONLY otherwise
   * you could mess up online server accounting. TOOD: Review usage and convert
   * to {@link #getServerNode(ServerName)} where we can.
   */
  public ServerStateNode getOrCreateServer(final ServerName serverName) {
    ServerStateNode node = serverMap.get(serverName);
    if (node == null) {
      node = new ServerStateNode(serverName);
      ServerStateNode oldNode = serverMap.putIfAbsent(serverName, node);
      node = oldNode != null ? oldNode : node;
    }
    return node;
  }

  /**
   * Called by SCP at end of successful processing.
   */
  public void removeServer(final ServerName serverName) {
    serverMap.remove(serverName);
  }

  /**
   * @return Pertinent ServerStateNode or NULL if none found (Do not make modifications).
   */
  public ServerStateNode getServerNode(final ServerName serverName) {
    return serverMap.get(serverName);
  }

  public double getAverageLoad() {
    int numServers = 0;
    int totalLoad = 0;
    for (ServerStateNode node: serverMap.values()) {
      totalLoad += node.getRegionCount();
      numServers++;
    }
    return numServers == 0 ? 0.0: (double)totalLoad / (double)numServers;
  }

  public ServerStateNode addRegionToServer(final RegionStateNode regionNode) {
    ServerStateNode serverNode = getOrCreateServer(regionNode.getRegionLocation());
    serverNode.addRegion(regionNode);
    return serverNode;
  }

  public ServerStateNode removeRegionFromServer(final ServerName serverName,
      final RegionStateNode regionNode) {
    ServerStateNode serverNode = getOrCreateServer(serverName);
    serverNode.removeRegion(regionNode);
    return serverNode;
  }

  // ==========================================================================
  //  ToString helpers
  // ==========================================================================
  public static String regionNamesToString(final Collection<byte[]> regions) {
    final StringBuilder sb = new StringBuilder();
    final Iterator<byte[]> it = regions.iterator();
    sb.append("[");
    if (it.hasNext()) {
      sb.append(Bytes.toStringBinary(it.next()));
      while (it.hasNext()) {
        sb.append(", ");
        sb.append(Bytes.toStringBinary(it.next()));
      }
    }
    sb.append("]");
    return sb.toString();
  }
}
