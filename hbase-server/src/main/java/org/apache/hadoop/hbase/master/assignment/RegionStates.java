/**
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

package org.apache.hadoop.hbase.master.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * RegionStates contains a set of Maps that describes the in-memory state of the AM, with
 * the regions available in the system, the region in transition, the offline regions and
 * the servers holding regions.
 */
@InterfaceAudience.Private
public class RegionStates {
  private static final Logger LOG = LoggerFactory.getLogger(RegionStates.class);

  protected static final State[] STATES_EXPECTED_ON_OPEN = new State[] {
    State.OPEN, // State may already be OPEN if we died after receiving the OPEN from regionserver
                // but before complete finish of AssignProcedure. HBASE-20100.
    State.OFFLINE, State.CLOSED,      // disable/offline
    State.SPLITTING, State.SPLIT,     // ServerCrashProcedure
    State.OPENING, State.FAILED_OPEN, // already in-progress (retrying)
  };

  protected static final State[] STATES_EXPECTED_ON_CLOSE = new State[] {
    State.SPLITTING, State.SPLIT, State.MERGING, // ServerCrashProcedure
    State.OPEN,                   // enabled/open
    State.CLOSING                 // already in-progress (retrying)
  };

  private static class AssignmentProcedureEvent extends ProcedureEvent<RegionInfo> {
    public AssignmentProcedureEvent(final RegionInfo regionInfo) {
      super(regionInfo);
    }
  }

  private static class ServerReportEvent extends ProcedureEvent<ServerName> {
    public ServerReportEvent(final ServerName serverName) {
      super(serverName);
    }
  }

  /**
   * Current Region State.
   * In-memory only. Not persisted.
   */
  // Mutable/Immutable? Changes have to be synchronized or not?
  // Data members are volatile which seems to say multi-threaded access is fine.
  // In the below we do check and set but the check state could change before
  // we do the set because no synchronization....which seems dodgy. Clear up
  // understanding here... how many threads accessing? Do locks make it so one
  // thread at a time working on a single Region's RegionStateNode? Lets presume
  // so for now. Odd is that elsewhere in this RegionStates, we synchronize on
  // the RegionStateNode instance. TODO.
  public static class RegionStateNode implements Comparable<RegionStateNode> {
    private final RegionInfo regionInfo;
    private final ProcedureEvent<?> event;

    private volatile RegionTransitionProcedure procedure = null;
    private volatile ServerName regionLocation = null;
    private volatile ServerName lastHost = null;
    /**
     * A Region-in-Transition (RIT) moves through states.
     * See {@link State} for complete list. A Region that
     * is opened moves from OFFLINE => OPENING => OPENED.
     */
    private volatile State state = State.OFFLINE;

    /**
     * Updated whenever a call to {@link #setRegionLocation(ServerName)}
     * or {@link #setState(State, State...)}.
     */
    private volatile long lastUpdate = 0;

    private volatile long openSeqNum = HConstants.NO_SEQNUM;

    public RegionStateNode(final RegionInfo regionInfo) {
      this.regionInfo = regionInfo;
      this.event = new AssignmentProcedureEvent(regionInfo);
    }

    /**
     * @param update new region state this node should be assigned.
     * @param expected current state should be in this given list of expected states
     * @return true, if current state is in expected list; otherwise false.
     */
    public boolean setState(final State update, final State... expected) {
      if (!isInState(expected)) {
        return false;
      }
      this.state = update;
      this.lastUpdate = EnvironmentEdgeManager.currentTime();
      return true;
    }

    /**
     * Put region into OFFLINE mode (set state and clear location).
     * @return Last recorded server deploy
     */
    public ServerName offline() {
      setState(State.OFFLINE);
      return setRegionLocation(null);
    }

    /**
     * Set new {@link State} but only if currently in <code>expected</code> State
     * (if not, throw {@link UnexpectedStateException}.
     */
    public void transitionState(final State update, final State... expected)
    throws UnexpectedStateException {
      if (!setState(update, expected)) {
        throw new UnexpectedStateException("Expected " + Arrays.toString(expected) +
          " so could move to " + update + " but current state=" + getState());
      }
    }

    public boolean isInState(final State... expected) {
      if (expected != null && expected.length > 0) {
        boolean expectedState = false;
        for (int i = 0; i < expected.length; ++i) {
          expectedState |= (getState() == expected[i]);
        }
        return expectedState;
      }
      return true;
    }

    public boolean isStuck() {
      return isInState(State.FAILED_OPEN) && getProcedure() != null;
    }

    public boolean isInTransition() {
      return getProcedure() != null;
    }

    public long getLastUpdate() {
      return procedure != null ? procedure.getLastUpdate() : lastUpdate;
    }

    public void setLastHost(final ServerName serverName) {
      this.lastHost = serverName;
    }

    public void setOpenSeqNum(final long seqId) {
      this.openSeqNum = seqId;
    }

    public ServerName setRegionLocation(final ServerName serverName) {
      ServerName lastRegionLocation = this.regionLocation;
      if (LOG.isTraceEnabled() && serverName == null) {
        LOG.trace("Tracking when we are set to null " + this, new Throwable("TRACE"));
      }
      this.regionLocation = serverName;
      this.lastUpdate = EnvironmentEdgeManager.currentTime();
      return lastRegionLocation;
    }

    public boolean setProcedure(final RegionTransitionProcedure proc) {
      if (this.procedure != null && this.procedure != proc) {
        return false;
      }
      this.procedure = proc;
      return true;
    }

    public boolean unsetProcedure(final RegionTransitionProcedure proc) {
      if (this.procedure != null && this.procedure != proc) {
        return false;
      }
      this.procedure = null;
      return true;
    }

    public RegionTransitionProcedure getProcedure() {
      return procedure;
    }

    public ProcedureEvent<?> getProcedureEvent() {
      return event;
    }

    public RegionInfo getRegionInfo() {
      return regionInfo;
    }

    public TableName getTable() {
      return getRegionInfo().getTable();
    }

    public boolean isSystemTable() {
      return getTable().isSystemTable();
    }

    public ServerName getLastHost() {
      return lastHost;
    }

    public ServerName getRegionLocation() {
      return regionLocation;
    }

    public State getState() {
      return state;
    }

    public long getOpenSeqNum() {
      return openSeqNum;
    }

    public int getFormatVersion() {
      // we don't have any format for now
      // it should probably be in regionInfo.getFormatVersion()
      return 0;
    }

    public RegionState toRegionState() {
      return new RegionState(getRegionInfo(), getState(), getLastUpdate(), getRegionLocation());
    }

    @Override
    public int compareTo(final RegionStateNode other) {
      // NOTE: RegionInfo sort by table first, so we are relying on that.
      // we have a TestRegionState#testOrderedByTable() that check for that.
      return RegionInfo.COMPARATOR.compare(getRegionInfo(), other.getRegionInfo());
    }

    @Override
    public int hashCode() {
      return getRegionInfo().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
      if (this == other) return true;
      if (!(other instanceof RegionStateNode)) return false;
      return compareTo((RegionStateNode)other) == 0;
    }

    @Override
    public String toString() {
      return toDescriptiveString();
    }

    public String toShortString() {
      // rit= is the current Region-In-Transition State -- see State enum.
      return String.format("rit=%s, location=%s", getState(), getRegionLocation());
    }

    public String toDescriptiveString() {
      return String.format("%s, table=%s, region=%s",
        toShortString(), getTable(), getRegionInfo().getEncodedName());
    }
  }

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

  /**
   * Server State.
   */
  public enum ServerState {
    /**
     * Initial state. Available.
     */
    ONLINE,

    /**
     * Server expired/crashed. Currently undergoing WAL splitting.
     */
    SPLITTING,

    /**
     * WAL splitting done.
     */
    OFFLINE
  }

  /**
   * State of Server; list of hosted regions, etc.
   */
  public static class ServerStateNode implements Comparable<ServerStateNode> {
    private final ServerReportEvent reportEvent;

    private final Set<RegionStateNode> regions;
    private final ServerName serverName;

    private volatile ServerState state = ServerState.ONLINE;
    private volatile int versionNumber = 0;

    public ServerStateNode(final ServerName serverName) {
      this.serverName = serverName;
      this.regions = ConcurrentHashMap.newKeySet();
      this.reportEvent = new ServerReportEvent(serverName);
    }

    public ServerName getServerName() {
      return serverName;
    }

    public ServerState getState() {
      return state;
    }

    public int getVersionNumber() {
      return versionNumber;
    }

    public ProcedureEvent<?> getReportEvent() {
      return reportEvent;
    }

    public boolean isOffline() {
      return this.state.equals(ServerState.OFFLINE);
    }

    public boolean isInState(final ServerState... expected) {
      boolean expectedState = false;
      if (expected != null) {
        for (int i = 0; i < expected.length; ++i) {
          expectedState |= (state == expected[i]);
        }
      }
      return expectedState;
    }

    public void setState(final ServerState state) {
      this.state = state;
    }

    public void setVersionNumber(final int versionNumber) {
      this.versionNumber = versionNumber;
    }

    public Set<RegionStateNode> getRegions() {
      return regions;
    }

    public int getRegionCount() {
      return regions.size();
    }

    public ArrayList<RegionInfo> getRegionInfoList() {
      ArrayList<RegionInfo> hris = new ArrayList<RegionInfo>(regions.size());
      for (RegionStateNode region: regions) {
        hris.add(region.getRegionInfo());
      }
      return hris;
    }

    public void addRegion(final RegionStateNode regionNode) {
      this.regions.add(regionNode);
    }

    public void removeRegion(final RegionStateNode regionNode) {
      this.regions.remove(regionNode);
    }

    @Override
    public int compareTo(final ServerStateNode other) {
      return getServerName().compareTo(other.getServerName());
    }

    @Override
    public int hashCode() {
      return getServerName().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
      if (this == other) return true;
      if (!(other instanceof ServerStateNode)) return false;
      return compareTo((ServerStateNode)other) == 0;
    }

    @Override
    public String toString() {
      return String.format("ServerStateNode(%s)", getServerName());
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

  public void clear() {
    regionsMap.clear();
    regionInTransition.clear();
    regionOffline.clear();
    serverMap.clear();
  }

  @VisibleForTesting
  public boolean isRegionInRegionStates(final RegionInfo hri) {
    return (regionsMap.containsKey(hri.getRegionName()) || regionInTransition.containsKey(hri)
        || regionOffline.containsKey(hri));
  }

  // ==========================================================================
  //  RegionStateNode helpers
  // ==========================================================================
  protected RegionStateNode createRegionStateNode(final RegionInfo regionInfo) {
    RegionStateNode newNode = new RegionStateNode(regionInfo);
    RegionStateNode oldNode = regionsMap.putIfAbsent(regionInfo.getRegionName(), newNode);
    return oldNode != null ? oldNode : newNode;
  }

  protected RegionStateNode getOrCreateRegionStateNode(final RegionInfo regionInfo) {
    RegionStateNode node = regionsMap.get(regionInfo.getRegionName());
    return node != null ? node : createRegionStateNode(regionInfo);
  }

  RegionStateNode getRegionStateNodeFromName(final byte[] regionName) {
    return regionsMap.get(regionName);
  }

  protected RegionStateNode getRegionStateNode(final RegionInfo regionInfo) {
    return getRegionStateNodeFromName(regionInfo.getRegionName());
  }

  public void deleteRegion(final RegionInfo regionInfo) {
    regionsMap.remove(regionInfo.getRegionName());
    // Remove from the offline regions map too if there.
    if (this.regionOffline.containsKey(regionInfo)) {
      if (LOG.isTraceEnabled()) LOG.trace("Removing from regionOffline Map: " + regionInfo);
      this.regionOffline.remove(regionInfo);
    }
  }

  ArrayList<RegionStateNode> getTableRegionStateNodes(final TableName tableName) {
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

  Collection<RegionStateNode> getRegionStateNodes() {
    return regionsMap.values();
  }

  public ArrayList<RegionState> getRegionStates() {
    final ArrayList<RegionState> regions = new ArrayList<RegionState>(regionsMap.size());
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
  public List<RegionInfo> getRegionsOfTable(final TableName table) {
    return getRegionsOfTable(table, false);
  }

  /**
   * @return Return the regions of the table; does not include OFFLINE unless you set
   * <code>offline</code> to true. Does not include regions that are in the
   * {@link State#SPLIT} state.
   */
  public List<RegionInfo> getRegionsOfTable(final TableName table, final boolean offline) {
    final ArrayList<RegionStateNode> nodes = getTableRegionStateNodes(table);
    final ArrayList<RegionInfo> hris = new ArrayList<RegionInfo>(nodes.size());
    for (RegionStateNode node: nodes) {
      if (include(node, offline)) hris.add(node.getRegionInfo());
    }
    return hris;
  }

  /**
   * Utility. Whether to include region in list of regions. Default is to
   * weed out split and offline regions.
   * @return True if we should include the <code>node</code> (do not include
   * if split or offline unless <code>offline</code> is set to true.
   */
  boolean include(final RegionStateNode node, final boolean offline) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("WORKING ON " + node + " " + node.getRegionInfo());
    }
    if (node.isInState(State.SPLIT)) return false;
    if (node.isInState(State.OFFLINE) && !offline) return false;
    final RegionInfo hri = node.getRegionInfo();
    return (!hri.isOffline() && !hri.isSplit()) ||
        ((hri.isOffline() || hri.isSplit()) && offline);
  }

  /**
   * Returns the set of regions hosted by the specified server
   * @param serverName the server we are interested in
   * @return set of RegionInfo hosted by the specified server
   */
  public List<RegionInfo> getServerRegionInfoSet(final ServerName serverName) {
    final ServerStateNode serverInfo = getServerNode(serverName);
    if (serverInfo == null) return Collections.emptyList();

    synchronized (serverInfo) {
      return serverInfo.getRegionInfoList();
    }
  }

  // ============================================================================================
  //  TODO: split helpers
  // ============================================================================================

  /**
   * Call this when we start log splitting a crashed Server.
   * @see #logSplit(ServerName)
   */
  public void logSplitting(final ServerName serverName) {
    final ServerStateNode serverNode = getOrCreateServer(serverName);
    synchronized (serverNode) {
      serverNode.setState(ServerState.SPLITTING);
    }
  }

  /**
   * Called after we've split all logs on a crashed Server.
   * @see #logSplitting(ServerName)
   */
  public void logSplit(final ServerName serverName) {
    final ServerStateNode serverNode = getOrCreateServer(serverName);
    synchronized (serverNode) {
      serverNode.setState(ServerState.OFFLINE);
    }
  }

  public void logSplit(final RegionInfo regionInfo) {
    final RegionStateNode regionNode = getRegionStateNode(regionInfo);
    synchronized (regionNode) {
      regionNode.setState(State.SPLIT);
    }
  }

  @VisibleForTesting
  public void updateRegionState(final RegionInfo regionInfo, final State state) {
    final RegionStateNode regionNode = getOrCreateRegionStateNode(regionInfo);
    synchronized (regionNode) {
      regionNode.setState(state);
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

  public boolean isRegionInState(final RegionInfo regionInfo, final State... state) {
    final RegionStateNode region = getRegionStateNode(regionInfo);
    if (region != null) {
      synchronized (region) {
        return region.isInState(state);
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
    for (RegionInfo hri: regions) {
      final RegionStateNode node = getRegionStateNode(hri);
      if (node == null) continue;

      // TODO: State.OPEN
      final ServerName serverName = node.getRegionLocation();
      if (serverName == null) continue;

      List<RegionInfo> serverRegions = result.get(serverName);
      if (serverRegions == null) {
        serverRegions = new ArrayList<RegionInfo>();
        result.put(serverName, serverRegions);
      }

      serverRegions.add(node.getRegionInfo());
    }
    return result;
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

  public ServerName getRegionServerOfRegion(final RegionInfo regionInfo) {
    final RegionStateNode region = getRegionStateNode(regionInfo);
    if (region != null) {
      synchronized (region) {
        ServerName server = region.getRegionLocation();
        return server != null ? server : region.getLastHost();
      }
    }
    return null;
  }

  /**
   * This is an EXPENSIVE clone.  Cloning though is the safest thing to do.
   * Can't let out original since it can change and at least the load balancer
   * wants to iterate this exported list.  We need to synchronize on regions
   * since all access to this.servers is under a lock on this.regions.
   * @param forceByCluster a flag to force to aggregate the server-load to the cluster level
   * @return A clone of current assignments by table.
   */
  public Map<TableName, Map<ServerName, List<RegionInfo>>> getAssignmentsByTable(
      final boolean forceByCluster) {
    if (!forceByCluster) return getAssignmentsByTable();

    final HashMap<ServerName, List<RegionInfo>> ensemble =
      new HashMap<ServerName, List<RegionInfo>>(serverMap.size());
    for (ServerStateNode serverNode: serverMap.values()) {
      ensemble.put(serverNode.getServerName(), serverNode.getRegionInfoList());
    }

    // TODO: can we use Collections.singletonMap(HConstants.ENSEMBLE_TABLE_NAME, ensemble)?
    final Map<TableName, Map<ServerName, List<RegionInfo>>> result =
      new HashMap<TableName, Map<ServerName, List<RegionInfo>>>(1);
    result.put(HConstants.ENSEMBLE_TABLE_NAME, ensemble);
    return result;
  }

  public Map<TableName, Map<ServerName, List<RegionInfo>>> getAssignmentsByTable() {
    final Map<TableName, Map<ServerName, List<RegionInfo>>> result = new HashMap<>();
    for (RegionStateNode node: regionsMap.values()) {
      Map<ServerName, List<RegionInfo>> tableResult = result.get(node.getTable());
      if (tableResult == null) {
        tableResult = new HashMap<ServerName, List<RegionInfo>>();
        result.put(node.getTable(), tableResult);
      }

      final ServerName serverName = node.getRegionLocation();
      if (serverName == null) {
        LOG.info("Skipping, no server for " + node);
        continue;
      }
      List<RegionInfo> serverResult = tableResult.get(serverName);
      if (serverResult == null) {
        serverResult = new ArrayList<RegionInfo>();
        tableResult.put(serverName, serverResult);
      }

      serverResult.add(node.getRegionInfo());
    }
    // Add online servers with no assignment for the table.
    for (Map<ServerName, List<RegionInfo>> table: result.values()) {
        for (ServerName svr : serverMap.keySet()) {
          if (!table.containsKey(svr)) {
            table.put(svr, new ArrayList<RegionInfo>());
          }
        }
    }
    return result;
  }

  // ==========================================================================
  //  Region in transition helpers
  // ==========================================================================
  protected boolean addRegionInTransition(final RegionStateNode regionNode,
      final RegionTransitionProcedure procedure) {
    if (procedure != null && !regionNode.setProcedure(procedure)) return false;

    regionInTransition.put(regionNode.getRegionInfo(), regionNode);
    return true;
  }

  protected void removeRegionInTransition(final RegionStateNode regionNode,
      final RegionTransitionProcedure procedure) {
    regionInTransition.remove(regionNode.getRegionInfo());
    regionNode.unsetProcedure(procedure);
  }

  public boolean hasRegionsInTransition() {
    return !regionInTransition.isEmpty();
  }

  public boolean isRegionInTransition(final RegionInfo regionInfo) {
    final RegionStateNode node = regionInTransition.get(regionInfo);
    return node != null ? node.isInTransition() : false;
  }

  /**
   * @return If a procedure-in-transition for <code>hri</code>, return it else null.
   */
  public RegionTransitionProcedure getRegionTransitionProcedure(final RegionInfo hri) {
    RegionStateNode node = regionInTransition.get(hri);
    if (node == null) return null;
    return node.getProcedure();
  }

  public RegionState getRegionTransitionState(final RegionInfo hri) {
    RegionStateNode node = regionInTransition.get(hri);
    if (node == null) return null;

    synchronized (node) {
      return node.isInTransition() ? node.toRegionState() : null;
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

  public void removeServer(final ServerName serverName) {
    serverMap.remove(serverName);
  }

  protected ServerStateNode getServerNode(final ServerName serverName) {
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
