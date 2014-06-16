/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableServers;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.executor.RegionTransitionEventData;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.LegacyRootZNodeUpdater;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * Class to manage assigning regions to servers, state of root and meta, etc.
 */
public class RegionManager {
  protected static final Log LOG = LogFactory.getLog(RegionManager.class);

  private final AtomicReference<HServerInfo> rootRegionLocation =
    new AtomicReference<HServerInfo>(null);

  private final RootScanner rootScannerThread;
  final MetaScanner metaScannerThread;

  /** Set by root scanner to indicate the number of meta regions */
  private final AtomicInteger numberOfMetaRegions = new AtomicInteger();

  /** These are the online meta regions */
  private final NavigableMap<byte [], MetaRegion> onlineMetaRegions =
    new ConcurrentSkipListMap<byte [], MetaRegion>(Bytes.BYTES_COMPARATOR);

  private final NavigableMap<byte [], MetaRegion> metaRegionLocationsBeforeScan =
      new TreeMap<byte [], MetaRegion>(Bytes.BYTES_COMPARATOR);

  private static final byte[] OVERLOADED = Bytes.toBytes("Overloaded");

  private static final byte [] META_REGION_PREFIX = Bytes.toBytes(".META.,");

  private final AssignmentManager assignmentManager;

  private int metaRegionsCount = 0;
  private int notMetaRegionsCount = 0;

  /**
   * Map key -> tableName, value -> ThrottledRegionReopener
   * An entry is created in the map before an alter operation is performed on the
   * table. It is cleared when all the regions have reopened.
   */
  private final Map<String, ThrottledRegionReopener> tablesReopeningRegions =
      new ConcurrentHashMap<String, ThrottledRegionReopener>();
  /**
   * Map of region name to RegionState for regions that are in transition such as
   *
   * unassigned -> pendingOpen -> open
   * closing -> pendingClose -> closed; if (closed && !offline) -> unassigned
   *
   * At the end of a transition, removeRegion is used to remove the region from
   * the map (since it is no longer in transition)
   *
   * Note: Needs to be SortedMap so we can specify a comparator
   *
   * @see RegionState inner-class below
   */
   final SortedMap<String, RegionState> regionsInTransition =
    Collections.synchronizedSortedMap(new TreeMap<String, RegionState>());

   /** Serves as a cache for locating where a particular region is open.
    * Currently being used to detect legitmate duplicate assignments from
    * spurious ones, that may seem to occur if a ZK notification is received
    * twice.
    *
    * maps regionName --> serverName
    *
    * Note: This is a temporary hack. Should be safe to remove once we get
    * rid of duplicate notifications from ZK.
    */
   final ConcurrentMap<String, String> regionLocationHintToDetectDupAssignment =
       new ConcurrentHashMap<String, String>();

   // regions in transition are also recorded in ZK using the zk wrapper
   final ZooKeeperWrapper zkWrapper;

  // How many regions to assign a server at a time.
  private final int maxAssignInOneGo;

  final HMaster master;
  private final LoadBalancer loadBalancer;

  /** Set of regions to split. */
  private final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>
    regionsToSplit = Collections.synchronizedSortedMap(
        new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
        (Bytes.BYTES_COMPARATOR));
  /** Set of regions to compact. */
  private final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>
    regionsToCompact = Collections.synchronizedSortedMap(
        new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
        (Bytes.BYTES_COMPARATOR));
  /** Set of column families to compact within a region.
  This map is a double SortedMap, first indexed on regionName and then indexed
  on column family name. This is done to facilitate the fact that we might want
  to perform a certain action on only a column family within a region.
  */
  private final SortedMap<byte[],
          SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>>
    cfsToCompact = Collections.synchronizedSortedMap(
        new TreeMap<byte[],SortedMap<byte[],Pair<HRegionInfo,HServerAddress>>>
        (Bytes.BYTES_COMPARATOR));
  /** Set of column families to major compact within a region.
  This map is a double SortedMap, first indexed on regionName and then indexed
  on column family name. This is done to facilitate the fact that we might want
  to perform a certain action on only a column family within a region.
  */
  private final SortedMap<byte[],
          SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>>
    cfsToMajorCompact = Collections.synchronizedSortedMap(
        new TreeMap<byte[],SortedMap<byte[],Pair<HRegionInfo,HServerAddress>>>
        (Bytes.BYTES_COMPARATOR));
  /** Set of regions to major compact. */
  private final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>
    regionsToMajorCompact = Collections.synchronizedSortedMap(
        new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
        (Bytes.BYTES_COMPARATOR));
  /** Set of regions to flush. */
  private final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>
    regionsToFlush = Collections.synchronizedSortedMap(
        new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
        (Bytes.BYTES_COMPARATOR));
  private final int zooKeeperNumRetries;
  private final int zooKeeperPause;

  /**
   * Set of region servers which send heart beat in the first period of time
   * during the master boots. Hold the best locality regions for these
   * region servers.
   */
  private Set<String> quickStartRegionServerSet = new HashSet<String>();

  private boolean stoppedScanners = false;

  private LegacyRootZNodeUpdater legacyRootZNodeUpdater;


  RegionManager(HMaster master) throws IOException {
    Configuration conf = master.getConfiguration();

    this.master = master;
    this.zkWrapper =
        ZooKeeperWrapper.getInstance(conf, master.getZKWrapperName());
    this.maxAssignInOneGo = conf.getInt("hbase.regions.percheckin", 10);

    if (master.shouldAssignRegionsWithFavoredNodes()) {
      this.loadBalancer = new AssignmentLoadBalancer(conf);
    } else {
      this.loadBalancer = new DefaultLoadBalancer();
    }
    this.assignmentManager = new AssignmentManager(master);

    // The root region
    rootScannerThread = new RootScanner(master);

    // Scans the meta table
    metaScannerThread = new MetaScanner(master);

    zooKeeperNumRetries = conf.getInt(HConstants.ZOOKEEPER_RETRIES,
        HConstants.DEFAULT_ZOOKEEPER_RETRIES);
    zooKeeperPause = conf.getInt(HConstants.ZOOKEEPER_PAUSE,
        HConstants.DEFAULT_ZOOKEEPER_PAUSE);

    legacyRootZNodeUpdater = new LegacyRootZNodeUpdater(zkWrapper, master,
        rootRegionLocation);
  }

  public LoadBalancer getLoadBalancer() {
    return this.loadBalancer;
  }

  void start() {
    assignmentManager.start();
    Threads.setDaemonThreadRunning(rootScannerThread,
      "RegionManager.rootScanner");
    Threads.setDaemonThreadRunning(metaScannerThread,
      "RegionManager.metaScanner");
    Threads.setDaemonThreadRunning(legacyRootZNodeUpdater, null);
  }

  public AssignmentManager getAssignmentManager() {
    return this.assignmentManager;
  }

  void unsetRootRegion() {
    synchronized (regionsInTransition) {
      synchronized (rootRegionLocation) {
        rootRegionLocation.set(null);
        rootRegionLocation.notifyAll();
      }
      regionsInTransition.remove(
          HRegionInfo.ROOT_REGIONINFO.getRegionNameAsString());
      LOG.info("-ROOT- region unset (but not set to be reassigned)");
    }
  }

  void reassignRootRegion() {
    unsetRootRegion();
    if (!master.isClusterShutdownRequested()) {
      synchronized (regionsInTransition) {
        // ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN has the same regionName
        String regionName = HRegionInfo.ROOT_REGIONINFO.getRegionNameAsString();
        byte[] data = null;
        try {
          data = Writables.getBytes(new RegionTransitionEventData(HBaseEventType.M2ZK_REGION_OFFLINE, HMaster.MASTER));
        } catch (IOException e) {
          LOG.error("Error creating event data for " + HBaseEventType.M2ZK_REGION_OFFLINE, e);
        }
        // ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN has the same regionName
        zkWrapper.createOrUpdateUnassignedRegion(
            HRegionInfo.ROOT_REGIONINFO.getEncodedName(), data);
        LOG.debug("Created UNASSIGNED zNode " + regionName + " in state " + HBaseEventType.M2ZK_REGION_OFFLINE);
        RegionState s;
        if (HTableDescriptor.isMetaregionSeqidRecordEnabled(master.getConfiguration())) {
          s = new RegionState(HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN,
                  RegionState.State.UNASSIGNED);
        } else {
          s = new RegionState(HRegionInfo.ROOT_REGIONINFO, RegionState.State.UNASSIGNED);
        }
        regionsInTransition.put(regionName, s);
        LOG.info("ROOT inserted into regionsInTransition");
      }
    }
  }

  /**
   * Assigns regions to region servers attempting to balance the load across all
   * region servers. Note that no synchronization is necessary as the caller
   * (ServerManager.processMsgs) already owns the monitor for the RegionManager.
   *
   * @param info
   * @param mostLoadedRegions
   * @param returnMsgs
   */
  void assignRegions(HServerInfo info, HRegionInfo[] mostLoadedRegions,
      ArrayList<HMsg> returnMsgs) {
    if (this.master.getIsSplitLogAfterStartupDone() == false) {
      // wait for log splitting at startup to complete. The regions will
      // be assigned when the region server reports next
      return;
    }

    if (this.master.shouldAssignRegionsWithFavoredNodes()) {
      // assign regions with favored nodes
      assignRegionsWithFavoredNodes(info, mostLoadedRegions, returnMsgs);
    } else {
      // assign regions without favored nodes
      assignRegionsWithoutFavoredNodes(info, mostLoadedRegions, returnMsgs);
    }
  }

  private void assignRegionsWithFavoredNodes(HServerInfo regionServer,
      HRegionInfo[] mostLoadedRegions, ArrayList<HMsg> returnMsgs) {
    // get the regions that are waiting for assignment for that region server
    Set<RegionState> regionsToAssign = regionsAwaitingAssignment(regionServer);

    if (regionsToAssign.isEmpty() &&
        master.getRegionServerOperationQueue().isEmpty() &&
        !master.isLoadBalancerDisabled()) {
        // There are no regions waiting to be assigned.
        // load balance as before
        this.loadBalancer.loadBalancing(regionServer, mostLoadedRegions, returnMsgs);
    } else {
      assignRegionsToOneServer(regionsToAssign, regionServer, returnMsgs);
    }
  }

  /**
   * @return true if there is a single regionserver online.
   */
  private boolean isSingleRegionServer() {
    return master.numServers() == 1;
  }

  private void assignRegionsWithoutFavoredNodes(HServerInfo info,
      HRegionInfo[] mostLoadedRegions, ArrayList<HMsg> returnMsgs) {
    // the region may assigned to this region server
    Set<RegionState> regionsToAssign = null;

    HServerLoad thisServersLoad = info.getLoad();
    boolean isSingleServer = isSingleRegionServer();
    boolean holdRegionForBestRegionServer = false;
    boolean assignmentByLocality = false;

    // only check assignmentByLocality when the
    // PreferredRegionToRegionServerMapping is not null;
    if (this.master.getPreferredRegionToRegionServerMapping() != null) {
      long masterRunningTime = System.currentTimeMillis()
              - this.master.getMasterStartupTime();
      holdRegionForBestRegionServer =
        masterRunningTime < this.master.getHoldRegionForBestLocalityPeriod();
      assignmentByLocality =
        masterRunningTime < this.master.getApplyPreferredAssignmentPeriod();

      // once it has passed the ApplyPreferredAssignmentPeriod, clear up
      // the quickStartRegionServerSet and PreferredRegionToRegionServerMapping
      // and it won't check the assignmentByLocality anymore.
      if (!assignmentByLocality) {
        quickStartRegionServerSet = null;
        this.master.clearPreferredRegionToRegionServerMapping();
      }
    }

    if (assignmentByLocality) {
      // have to add . at the end of host name
      String hostName = info.getHostname();
      quickStartRegionServerSet.add(hostName);
    }

    // this variable keeps track of the code path to go through; if true, than
    // the server we are examining was registered as restarting and thus we
    // should assign all the regions to it directly; else, we should go through
    // the normal code path
    MutableBoolean preferredAssignment = new MutableBoolean(false);

    // get the region set to be assigned to this region server
    regionsToAssign = regionsAwaitingAssignment(info.getServerAddress(),
        isSingleServer, preferredAssignment, assignmentByLocality,
        holdRegionForBestRegionServer,
        quickStartRegionServerSet);

    if (regionsToAssign.isEmpty()) {
      // There are no regions waiting to be assigned.
      if (!assignmentByLocality
          && master.getRegionServerOperationQueue().isEmpty()
          && !master.isLoadBalancerDisabled()) {
        // load balance as before
        this.loadBalancer.loadBalancing(info, mostLoadedRegions, returnMsgs);
      }
    } else {
      // if there's only one server or assign the region by locality,
      // just give the regions to this server
      if (isSingleServer || assignmentByLocality
          || preferredAssignment.booleanValue()) {
        assignRegionsToOneServer(regionsToAssign, info, returnMsgs);
      } else {
        // otherwise, give this server a few regions taking into account the
        // load of all the other servers
        assignRegionsToMultipleServers(thisServersLoad, regionsToAssign, info,
            returnMsgs);
      }
    }
  }

  /*
   * Make region assignments taking into account multiple servers' loads.
   *
   * Note that no synchronization is needed while we iterate over
   * regionsInTransition because this method is only called by assignRegions
   * whose caller owns the monitor for RegionManager
   *
   * TODO: This code is unintelligible. REWRITE. Add TESTS! St.Ack 09/30/2009
   * @param thisServersLoad
   * @param regionsToAssign
   * @param info
   * @param returnMsgs
   */
  private void assignRegionsToMultipleServers(final HServerLoad thisServersLoad,
      final Set<RegionState> regionsToAssign, final HServerInfo info,
      final ArrayList<HMsg> returnMsgs) {
    boolean isMetaAssign = false;
    for (RegionState s : regionsToAssign) {
      if (s.getRegionInfo().isMetaRegion())
        isMetaAssign = true;
    }
    int nRegionsToAssign = regionsToAssign.size();
    int otherServersRegionsCount =
      regionsToGiveOtherServers(nRegionsToAssign, thisServersLoad);
    nRegionsToAssign -= otherServersRegionsCount;
    if (nRegionsToAssign > 0 || isMetaAssign) {
      LOG.debug("Assigning for " + info + ": total nregions to assign="
          + nRegionsToAssign + ", regions to give other servers than this="
          + otherServersRegionsCount + ", isMetaAssign=" + isMetaAssign);

      // See how many we can assign before this server becomes more heavily
      // loaded than the next most heavily loaded server.
      HServerLoad heavierLoad = new HServerLoad();
      int nservers = computeNextHeaviestLoad(thisServersLoad, heavierLoad);
      int nregions = 0;
      // Advance past any less-loaded servers
      for (HServerLoad load = new HServerLoad(thisServersLoad);
      load.compareTo(heavierLoad) <= 0 && nregions < nRegionsToAssign;
      load.setNumberOfRegions(load.getNumberOfRegions() + 1), nregions++) {
        // continue;
      }
      if (nregions < nRegionsToAssign) {
        // There are some more heavily loaded servers
        // but we can't assign all the regions to this server.
        if (nservers > 0) {
          // There are other servers that can share the load.
          // Split regions that need assignment across the servers.
          nregions = (int) Math.ceil((1.0 * nRegionsToAssign)/(1.0 * nservers));
        } else {
          // No other servers with same load.
          // Split regions over all available servers
          nregions = (int) Math.ceil((1.0 * nRegionsToAssign)/
              (1.0 * master.getServerManager().numServers()));
        }
      } else {
        // Assign all regions to this server
        nregions = nRegionsToAssign;
      }
      LOG.debug("Assigning " + info + " " + nregions + " regions");
      assignRegions(regionsToAssign, nregions, info, returnMsgs);
    }
  }

  /*
   * Assign <code>nregions</code> regions.
   * @param regionsToAssign
   * @param nregions
   * @param info
   * @param returnMsgs
   */
  private void assignRegions(final Set<RegionState> regionsToAssign,
      final int nregions, final HServerInfo info,
      final ArrayList<HMsg> returnMsgs) {
    int count = nregions;
    if (count > this.maxAssignInOneGo) {
      count = this.maxAssignInOneGo;
    }
    for (RegionState s : regionsToAssign) {
      doRegionAssignment(s, info, returnMsgs);
      if (--count <= 0) {
        break;
      }
    }
  }

  /*
   * Assign all to the only server. An unlikely case but still possible.
   * Note that no synchronization is needed on regionsInTransition while
   * iterating on it because the only caller is assignRegions whose caller owns
   * the monitor for RegionManager
   *
   * @param regionsToAssign
   * @param serverName
   * @param returnMsgs
   */
  private void assignRegionsToOneServer(final Set<RegionState> regionsToAssign,
      final HServerInfo info, final ArrayList<HMsg> returnMsgs) {
    for (RegionState s : regionsToAssign) {
      doRegionAssignment(s, info, returnMsgs);
    }
  }

  /*
   * Do single region assignment.
   * @param rs
   * @param sinfo
   * @param returnMsgs
   */
  private void doRegionAssignment(final RegionState rs,
      final HServerInfo sinfo, final ArrayList<HMsg> returnMsgs) {
    String regionName = rs.getRegionInfo().getRegionNameAsString();
    LOG.info("Assigning region " + regionName + " to " + sinfo.getServerName());
    rs.setPendingOpenUnacked(sinfo.getServerName());
    synchronized (this.regionsInTransition) {
      byte[] data = null;
      try {
        data = Writables.getBytes(new RegionTransitionEventData(
            HBaseEventType.M2ZK_REGION_OFFLINE, HMaster.MASTER));
      } catch (IOException e) {
        LOG.error("Error creating event data for "
            + HBaseEventType.M2ZK_REGION_OFFLINE, e);
      }
      zkWrapper.createOrUpdateUnassignedRegion(rs.getRegionInfo()
          .getEncodedName(), data);
      LOG.debug("Created UNASSIGNED zNode " + regionName + " in state "
          + HBaseEventType.M2ZK_REGION_OFFLINE);
      this.regionsInTransition.put(regionName, rs);
    }

    if (assignmentManager.hasAssignmentFromPlan(rs.getRegionInfo())) {
      String favoredNodes = RegionPlacement.getFavoredNodes(
          assignmentManager.getAssignmentFromPlan(rs.regionInfo));
      returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_OPEN, rs.getRegionInfo(),
          favoredNodes.getBytes()));
    } else {
      returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_OPEN, rs.getRegionInfo()));
    }
    // Now that we have told the server to open the region. Clean up the assignment plan.
    assignmentManager.removeTransientAssignment(sinfo.getServerAddress(), rs.regionInfo);
  }

  /*
   * @param nRegionsToAssign
   * @param thisServersLoad
   * @return How many regions should go to servers other than this one; i.e.
   * more lightly loaded servers
   */
  private int regionsToGiveOtherServers(final int numUnassignedRegions,
      final HServerLoad thisServersLoad) {

    SortedMap<HServerLoad, Collection<String>> lightServers =
        master.getServerManager().getServersToLoad().getLightServers(thisServersLoad);
    // Examine the list of servers that are more lightly loaded than this one.
    // Pretend that we will assign regions to these more lightly loaded servers
    // until they reach load equal with ours. Then, see how many regions are
    // left unassigned. That is how many regions we should assign to this server
    int nRegions = 0;
    for (Map.Entry<HServerLoad, Collection<String>> e : lightServers.entrySet()) {
      HServerLoad lightLoad = new HServerLoad(e.getKey());
      do {
        lightLoad.setNumberOfRegions(lightLoad.getNumberOfRegions() + 1);
        nRegions += 1;
      } while (lightLoad.compareTo(thisServersLoad) <= 0
          && nRegions < numUnassignedRegions);
      nRegions *= e.getValue().size();
      if (nRegions >= numUnassignedRegions) {
        break;
      }
    }
    return nRegions;
  }

  /**
   * Get the set of regions that should be assignable in this pass.
   *
   * Note that no synchronization on regionsInTransition is needed because the
   * only caller (assignRegions, whose caller is ServerManager.processMsgs) owns
   * the monitor for RegionManager
   */
  private Set<RegionState> regionsAwaitingAssignment(HServerInfo server) {
    // set of regions we want to assign to this server
    Set<RegionState> regionsToAssign = new HashSet<RegionState>();
    boolean isSingleServer = isSingleRegionServer();
    HServerAddress addr = server.getServerAddress();
    boolean isMetaServer = isMetaServer(addr);
    RegionState rootState = null;
    boolean isPreferredAssignment = false;
    boolean reassigningMetas =
      (numberOfMetaRegions.get() > onlineMetaRegions.size());
    boolean isMetaOrRoot = isMetaServer || isRootServer(addr);

    // Assign ROOT region if ROOT region is offline.
    synchronized (this.regionsInTransition) {
      // ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN has the same regionName
      rootState = regionsInTransition.get(HRegionInfo.ROOT_REGIONINFO
          .getRegionNameAsString());
    }
    if (rootState != null && rootState.isUnassigned()) {
      // just make sure it isn't hosting META regions (unless
      // it's the only server left).
      if ((!isMetaServer || isSingleServer) && !master.isServerBlackListed(server.getHostnamePort())) {
        regionsToAssign.add(rootState);
        LOG.debug("Going to assign -ROOT- region to server " +
            server.getHostnamePort());
      }
      return regionsToAssign;
    }

    // Don't assign META to this server who has already hosted any ROOT or META
    if (isMetaOrRoot && reassigningMetas && !isSingleServer) {
      return regionsToAssign;
    }

    // Get the set of the regions which are preserved
    // for the current region server
    Set<HRegionInfo> preservedRegionsForCurrentRS =
      assignmentManager.getTransientAssignments(addr);

    synchronized (this.regionsInTransition) {
      int nonPreferredAssignment = 0;
      for (RegionState regionState : regionsInTransition.values()) {
        HRegionInfo regionInfo = regionState.getRegionInfo();
        if (regionInfo == null) continue;
        if (regionInfo.isRootRegion() && !regionState.isUnassigned()) {
          LOG.debug("The -ROOT- region"
              + " has been assigned and will be online soon. " +
                  "Do nothing for server " + server.getHostnamePort());
          break;
        }
        // Assign the META region here explicitly
        if (regionInfo.isMetaRegion()) {
          if (regionState.isUnassigned() && !master.isServerBlackListed(server.getHostnamePort())) {
            regionsToAssign.clear();
            regionsToAssign.add(regionState);
            LOG.debug("Going to assign META region: " +
                regionInfo.getRegionNameAsString() + " to server " +
                server.getHostnamePort());
          } else {
            LOG.debug("The .META. region " + regionInfo.getRegionNameAsString()
                + " has been assigned and will be online soon. " +
                "Do nothing for server " + server.getHostnamePort());
          }
          break;
        }

        // Can't assign user regions until all meta regions have been assigned,
        // the initial meta scan is done and there are enough online
        // region servers
        if (reassigningMetas || !master.getServerManager().hasEnoughRegionServers()) {
          LOG.debug("Cannot assign region " + regionInfo.getRegionNameAsString()
              + " because not all the META are online, "
              + "or the initial META scan is not completed, or there are no "
              + "enough online region servers");
          continue;
        }

        // Cannot assign region which is NOT in the unassigned state
        if (!regionState.isUnassigned()) {
          continue;
        }

        if (preservedRegionsForCurrentRS == null ||
            !preservedRegionsForCurrentRS.contains(regionInfo)) {
          if (assignmentManager.hasTransientAssignment(regionInfo) ||
              nonPreferredAssignment > this.maxAssignInOneGo ||
              master.isServerBlackListed(server.getHostnamePort())) {
            // Hold the region for its favored nodes and limit the number of
            // non preferred assignments for each region server.
            continue;
          }
          // This is a non preferred assignment.
          isPreferredAssignment = false;
          nonPreferredAssignment++;
        } else {
          isPreferredAssignment = true;
        }

        // Assign the current region to the region server.
        regionsToAssign.add(regionState);
        LOG.debug("Going to assign user region " +
            regionInfo.getRegionNameAsString() +
            " to server " + server.getHostnamePort() + " in a " +
            (isPreferredAssignment ? "": "non-") + "preferred way");

      }
    }
    return regionsToAssign;
  }

  /**
   * Get the set of regions that should be assignable in this pass.
   *
   * Note that no synchronization on regionsInTransition is needed because the
   * only caller (assignRegions, whose caller is ServerManager.processMsgs) owns
   * the monitor for RegionManager
   */
  private Set<RegionState> regionsAwaitingAssignment(HServerAddress addr,
      boolean isSingleServer, MutableBoolean isPreferredAssignment,
      boolean assignmentByLocality, boolean holdRegionForBestRegionserver,
      Set<String> quickStartRegionServerSet) {
    // set of regions we want to assign to this server
    Set<RegionState> regionsToAssign = new HashSet<RegionState>();

    Set<HRegionInfo> regions = assignmentManager.getTransientAssignments(addr);
    if (null != regions) {
      isPreferredAssignment.setValue(true);
      // One could use regionsInTransition.keySet().containsAll(regions) but
      // this provides more control and probably the same complexity. Also, this
      // gives direct logging of precise errors
      HRegionInfo[] regionInfo = regions.toArray(new HRegionInfo[regions.size()]);
      for (HRegionInfo ri : regionInfo) {
        RegionState state = regionsInTransition.get(ri.getRegionNameAsString());
        if (null != state && state.isUnassigned()) {
          regionsToAssign.add(state);
          assignmentManager.removeTransientAssignment(addr, ri);
        }
      }
      StringBuilder regionNames = new StringBuilder();
      regionNames.append("[ ");
      for (RegionState regionState : regionsToAssign) {
        regionNames.append(Bytes.toString(regionState.getRegionName()));
        regionNames.append(" , ");
      }
      regionNames.append(" ]");
      LOG.debug("Assigning regions to " + addr + " : " + regionNames);
      // return its initial regions ASAP
      return regionsToAssign;
    }

    boolean isMetaServer = isMetaServer(addr);
    boolean isRootServer = isRootServer(addr);
    boolean isMetaOrRoot = isMetaServer || isRootServer;
    // lookup hostname of addr if needed
    String hostName = null;
    RegionState rootState = null;
    int nonPreferredAssignmentCount = 0;
    // Handle if root is unassigned... only assign root if root is offline.
    synchronized (this.regionsInTransition) {
      // ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN has the same regionName
      rootState = regionsInTransition.get(HRegionInfo.ROOT_REGIONINFO
          .getRegionNameAsString());
    }
    if (rootState != null && rootState.isUnassigned()) {
      // make sure root isnt assigned here first.
      // if so return 'empty list'
      // by definition there is no way this could be a ROOT region (since it's
      // unassigned) so just make sure it isn't hosting META regions (unless
      // it's the only server left).
      if (!isMetaServer || isSingleServer) {
        regionsToAssign.add(rootState);
      }
      return regionsToAssign;
    }

    // Look over the set of regions that aren't currently assigned to
    // determine which we should assign to this server.
    boolean reassigningMetas = numberOfMetaRegions.get() != onlineMetaRegions
        .size();
    if (reassigningMetas && isMetaOrRoot && !isSingleServer) {
      return regionsToAssign; // dont assign anything to this server.
    }

    synchronized (this.regionsInTransition) {
      for (RegionState s : regionsInTransition.values()) {
        if (! s.isUnassigned()) {
          continue;
        }
        String regionName = s.getRegionInfo().getEncodedName();
        String tableName = s.getRegionInfo().getTableDesc().getNameAsString();
        String name = tableName + ":" + regionName;
        HRegionInfo i = s.getRegionInfo();
        if (i == null) {
          continue;
        }
        if (reassigningMetas && !i.isMetaRegion()) {
          // Can't assign user regions until all meta regions have been assigned
          // and are on-line
          continue;
        }
        if (!i.isMetaRegion()
            && !master.getServerManager().hasEnoughRegionServers()) {
          LOG.debug("user region " + i.getRegionNameAsString()
              + " is in transition but not enough servers yet");
          continue;
        }

        // if we are holding it, don't give it away to any other server
        if (assignmentManager.hasTransientAssignment(s.getRegionInfo())) {
          continue;
        }
        if (assignmentByLocality && !i.isRootRegion() && !i.isMetaRegion()) {
          Text preferredHostNameTxt =
            (Text)this.master.getPreferredRegionToRegionServerMapping().get(new Text(name));

          if (hostName == null) {
            hostName = addr.getHostname();
          }
          if (preferredHostNameTxt != null) {
            String preferredHost = preferredHostNameTxt.toString();
            if (hostName.startsWith(preferredHost)) {
              LOG.debug("Doing Preferred Region Assignment for : " + name +
                  " to the " + hostName);
              // add the region to its preferred region server.
              regionsToAssign.add(s);
              continue;
            } else if (holdRegionForBestRegionserver ||
                quickStartRegionServerSet.contains(preferredHost)) {
              continue;
            }
          }
        }
        // Only assign a configured number unassigned region at one time in the
        // non preferred assignment case.
        if ((nonPreferredAssignmentCount++) < this.maxAssignInOneGo) {
          regionsToAssign.add(s);
        }
      }
    }
    return regionsToAssign;
  }

  /*
   * Figure out the load that is next highest amongst all regionservers. Also,
   * return how many servers exist at that load.
   */
  private int computeNextHeaviestLoad(HServerLoad referenceLoad,
    HServerLoad heavierLoad) {

    SortedMap<HServerLoad, Collection<String>> heavyServers = master.getServerManager().
        getServersToLoad().getHeavyServers(referenceLoad);
    int nservers = 0;
    for (Map.Entry<HServerLoad, Collection<String>> e : heavyServers.entrySet()) {
      Collection<String> servers = e.getValue();
      nservers += servers.size();
      if (e.getKey().compareTo(referenceLoad) == 0) {
        // This is the load factor of the server we are considering
        nservers -= 1;
        continue;
      }

      // If we get here, we are at the first load entry that is a
      // heavier load than the server we are considering
      heavierLoad.setNumberOfRequests(e.getKey().getNumberOfRequests());
      heavierLoad.setNumberOfRegions(e.getKey().getNumberOfRegions());
      break;
    }
    return nservers;
  }

  /*
   * The server checking in right now is overloaded. We will tell it to close
   * some or all of its most loaded regions, allowing it to reduce its load.
   * The closed regions will then get picked up by other underloaded machines.
   *
   * Note that no synchronization is needed because the only caller
   * (assignRegions) whose caller owns the monitor for RegionManager
   */
  void unassignSomeRegions(final HServerInfo info,
      int numRegionsToClose, final HRegionInfo[] mostLoadedRegions,
      ArrayList<HMsg> returnMsgs) {
    LOG.debug("Unassigning " + numRegionsToClose + " regions from " +
      info.getServerName());
    int regionIdx = 0;
    int regionsClosed = 0;
    int skipped = 0;
    while (regionsClosed < numRegionsToClose &&
        regionIdx < mostLoadedRegions.length) {
      HRegionInfo currentRegion = mostLoadedRegions[regionIdx];
      regionIdx++;
      // skip the region if it's meta or root
      if (currentRegion.isRootRegion() || currentRegion.isMetaTable()) {
        continue;
      }
      final String regionName = currentRegion.getRegionNameAsString();
      if (regionIsInTransition(regionName)) {
        skipped++;
        continue;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to close region " + regionName);
      }
      // make a message to close the region
      returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_CLOSE, currentRegion,
        OVERLOADED));
      // mark the region as closing
      setClosing(info.getServerName(), currentRegion, false);
      // increment the count of regions we've marked
      regionsClosed++;
    }
    LOG.info("Skipped assigning " + skipped + " region(s) to " +
      info.getServerName() + " because already in transition");
  }

  /*
   * PathFilter that accepts hbase tables only.
   */
  static class TableDirFilter implements PathFilter {
    @Override
    public boolean accept(final Path path) {
      // skip the region servers' log dirs && version file
      // HBASE-1112 want to separate the log dirs from table's data dirs by a
      // special character.
      final String pathname = path.getName();
      return (!pathname.equals(HConstants.HREGION_LOGDIR_NAME)
              && !pathname.equals(HConstants.VERSION_FILE_NAME));
    }

  }

  /*
   * PathFilter that accepts all but compaction.dir names.
   */
  static class RegionDirFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return !path.getName().equals(HConstants.HREGION_COMPACTIONDIR_NAME);
    }
  }

  /**
   * @return Read-only map of online regions.
   */
  public Map<byte [], MetaRegion> getOnlineMetaRegions() {
    synchronized (onlineMetaRegions) {
      return Collections.unmodifiableMap(onlineMetaRegions);
    }
  }

  public boolean metaRegionsInTransition() {
    synchronized (onlineMetaRegions) {
      for (MetaRegion metaRegion : onlineMetaRegions.values()) {
        String regionName = Bytes.toString(metaRegion.getRegionName());
        if (regionIsInTransition(regionName)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Return a map of the regions in transition on a server.
   * Returned map entries are region name -> RegionState
   */
  Map<String, RegionState> getRegionsInTransitionOnServer(String serverName) {
    Map<String, RegionState> ret = new HashMap<String, RegionState>();
    synchronized (regionsInTransition) {
      for (Map.Entry<String, RegionState> entry : regionsInTransition.entrySet()) {
        RegionState rs = entry.getValue();
        if (serverName.equals(rs.getServerName())) {
          ret.put(entry.getKey(), rs);
        }
      }
    }
    return ret;
  }

  /**
   * Stop the root and meta scanners so that the region servers serving meta regions can shut down.
   * Not thread-safe, but if called twice from the same thread, scanners will only be stopped once.
   */
  public void stopScanners() {
    if (!stoppedScanners) {
      this.rootScannerThread.interruptAndStop();
      this.metaScannerThread.interruptAndStop();
      stoppedScanners = true;
    }
  }

  /**
   * Force the rootScannerThread, and the metaScannerThread to scan the
   * root/meta region at once.
   *
   * These threads are supposed to scan ROOT/META regions every so often
   * (typically 1 min). This function is used to force the scan to happen
   * immediately.
   */
  public void forceScans() {
    if (!stoppedScanners) {
      LOG.debug("Going to trigger a metaScan for -ROOT-");
      this.rootScannerThread.triggerNow();
      LOG.debug("Going to trigger a metaScan for .META.");
      this.metaScannerThread.triggerNow();
    }
  }

  /**
   * Terminate all threads but don't clean up any state.
   */
  public void joinThreads() {
    try {
      if (rootScannerThread.isAlive()) {
        rootScannerThread.join();       // Wait for the root scanner to finish.
      }
    } catch (InterruptedException iex) {
      LOG.warn("root scanner", iex);
    }
    try {
      if (metaScannerThread.isAlive()) {
        metaScannerThread.join();       // Wait for meta scanner to finish.
      }
    } catch (InterruptedException iex) {
      LOG.warn("meta scanner", iex);
    }
  }

  /**
   * Block until meta regions are online or we're shutting down.
   * @return true if we found meta regions, false if we're closing.
   */
  public boolean areAllMetaRegionsOnline() {
    synchronized (onlineMetaRegions) {
      return (rootRegionLocation.get() != null &&
          numberOfMetaRegions.get() <= onlineMetaRegions.size());
    }
  }

  /**
   * Search our map of online meta regions to find the first meta region that
   * should contain a pointer to <i>newRegion</i>.
   * @param newRegion
   * @return MetaRegion where the newRegion should live
   */
  public MetaRegion getFirstMetaRegionForRegion(HRegionInfo newRegion) {
    synchronized (onlineMetaRegions) {
      return getMetaRegionPointingTo(onlineMetaRegions, newRegion);
    }
  }

  static MetaRegion getMetaRegionPointingTo(NavigableMap<byte[], MetaRegion> metaRegions,
      HRegionInfo newRegion) {
    if (metaRegions.isEmpty()) {
      return null;
    } else if (metaRegions.size() == 1) {
      return metaRegions.get(metaRegions.firstKey());
    } else {
      if (metaRegions.containsKey(newRegion.getRegionName())) {
        return metaRegions.get(newRegion.getRegionName());
      }
      return metaRegions.get(metaRegions.headMap(newRegion.getRegionName()).lastKey());
    }
  }

  /**
   * Get a set of all the meta regions that contain info about a given table.
   * @param tableName Table you need to know all the meta regions for
   * @return set of MetaRegion objects that contain the table
   * @throws NotAllMetaRegionsOnlineException
   */
  public Set<MetaRegion> getMetaRegionsForTable(byte [] tableName)
  throws NotAllMetaRegionsOnlineException {
    byte [] firstMetaRegion = null;
    Set<MetaRegion> metaRegions = new HashSet<MetaRegion>();
    if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
      if (rootRegionLocation.get() == null) {
        throw new NotAllMetaRegionsOnlineException(
            Bytes.toString(HConstants.ROOT_TABLE_NAME));
      }
      metaRegions.add(new MetaRegion(rootRegionLocation.get().getServerAddress(),
          HTableDescriptor.isMetaregionSeqidRecordEnabled(master.getConfiguration()) ?
          HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN : HRegionInfo.ROOT_REGIONINFO));
    } else {
      if (!areAllMetaRegionsOnline()) {
        throw new NotAllMetaRegionsOnlineException();
      }
      synchronized (onlineMetaRegions) {
        if (onlineMetaRegions.size() == 1) {
          firstMetaRegion = onlineMetaRegions.firstKey();
        } else if (onlineMetaRegions.containsKey(tableName)) {
          firstMetaRegion = tableName;
        } else {
          firstMetaRegion = onlineMetaRegions.headMap(tableName).lastKey();
        }
        metaRegions.addAll(onlineMetaRegions.tailMap(firstMetaRegion).values());
      }
    }
    return metaRegions;
  }

  /**
   * Get metaregion that would host passed in row.
   * @param row Row need to know all the meta regions for
   * @return MetaRegion for passed row.
   * @throws NotAllMetaRegionsOnlineException
   */
  public MetaRegion getMetaRegionForRow(final byte [] row)
  throws NotAllMetaRegionsOnlineException {
    if (!areAllMetaRegionsOnline()) {
      throw new NotAllMetaRegionsOnlineException();
    }
    // Row might be in -ROOT- table.  If so, return -ROOT- region.
    int prefixlen = META_REGION_PREFIX.length;
    if (row.length > prefixlen &&
     Bytes.compareTo(META_REGION_PREFIX, 0, prefixlen, row, 0, prefixlen) == 0) {
      return new MetaRegion(this.master.getRegionManager().getRootRegionLocation(),
          HTableDescriptor.isMetaregionSeqidRecordEnabled(master.getConfiguration()) ?
            HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN : HRegionInfo.ROOT_REGIONINFO);
    }
    return this.onlineMetaRegions.floorEntry(row).getValue();
  }

  /**
   * Create a new HRegion, put a row for it into META (or ROOT), and mark the
   * new region unassigned so that it will get assigned to a region server.
   * @param newRegion HRegionInfo for the region to create
   * @param server server hosting the META (or ROOT) region where the new
   * region needs to be noted
   * @param metaRegionName name of the meta region where new region is to be
   * written
   * @throws IOException
   */
  public void createRegion(HRegionInfo newRegion, HRegionInterface server,
      byte [] metaRegionName)
  throws IOException {
    createRegion(newRegion, server, metaRegionName, null);
  }

  /**
   * Create a new HRegion, put a row for it into META (or ROOT), and mark the
   * new region unassigned so that it will get assigned to a region server.
   * @param newRegion HRegionInfo for the region to create
   * @param server server hosting the META (or ROOT) region where the new
   * region needs to be noted
   * @param metaRegionName name of the meta region where new region is to be
   * @param favoriteNodeList The list of favorite nodes for this new region.
   * written
   * @throws IOException
   */
  public void createRegion(HRegionInfo newRegion, HRegionInterface server,
      byte [] metaRegionName,  List<HServerAddress> favoriteNodeList)
  throws IOException {
    // 2. Create the HRegion
    HRegion region = HRegion.createHRegion(newRegion, this.master.getRootDir(),
      master.getConfiguration());

    // 3. Insert into meta
    HRegionInfo info = region.getRegionInfo();
    byte [] regionName = region.getRegionName();

    Put put = new Put(regionName);
    // 3.1 Put the region info into meta table.
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(info));

    // 3.2 Put the favorite nodes into meta.
    if (favoriteNodeList != null) {
      String favoredNodes = RegionPlacement.getFavoredNodes(favoriteNodeList);
      put.add(HConstants.CATALOG_FAMILY, HConstants.FAVOREDNODES_QUALIFIER,
          EnvironmentEdgeManager.currentTimeMillis(), favoredNodes.getBytes());
      LOG.info("Create the region " + info.getRegionNameAsString() +
          " with favored nodes " + favoredNodes);
    }
    server.put(metaRegionName, put);

    // 4. Close the new region to flush it to disk.  Close its log file too.
    region.close();
    region.getLog().closeAndDelete();

    // After all regions are created, the caller will schedule
    // the meta scanner to run immediately and assign out the
    // regions.
  }

  /**
   * Set a MetaRegion as online.
   * @param metaRegion
   */
  public void putMetaRegionOnline(MetaRegion metaRegion) {
    onlineMetaRegions.put(metaRegion.getStartKey(), metaRegion);
  }

  /**
   * Get a list of online MetaRegions
   * @return list of MetaRegion objects
   */
  public List<MetaRegion> getListOfOnlineMetaRegions() {
    List<MetaRegion> regions;
    synchronized(onlineMetaRegions) {
      regions = new ArrayList<MetaRegion>(onlineMetaRegions.values());
    }
    return regions;
  }

  /**
   * Count of online meta regions
   * @return count of online meta regions
   */
  public int numOnlineMetaRegions() {
    return onlineMetaRegions.size();
  }

  /**
   * Check if a meta region is online by its name
   * @param startKey name of the meta region to check
   * @return true if the region is online, false otherwise
   */
  public boolean isMetaRegionOnline(byte [] startKey) {
    return onlineMetaRegions.containsKey(startKey);
  }

  /**
   * Set an online MetaRegion offline - remove it from the map.
   * @param startKey Startkey to use finding region to remove.
   * @return the MetaRegion that was taken offline.
   */
  public MetaRegion offlineMetaRegionWithStartKey(byte [] startKey) {
    LOG.info("META region whose startkey is " + Bytes.toString(startKey) +
      " removed from onlineMetaRegions");
    return onlineMetaRegions.remove(startKey);
  }

  public boolean isRootServer(HServerAddress server) {
    return this.master.getRegionManager().getRootRegionLocation() != null &&
      server.equals(master.getRegionManager().getRootRegionLocation());
  }

  /**
   * Returns the list of byte[] start-keys for any .META. regions hosted
   * on the indicated server.
   *
   * @param server server address
   * @return list of meta region start-keys.
   */
  public List<byte[]> listMetaRegionsForServer(HServerAddress server) {
    List<byte[]> metas = new ArrayList<byte[]>();
    for ( MetaRegion region : onlineMetaRegions.values() ) {
      if (server.equals(region.getServer())) {
        metas.add(region.getStartKey());
      }
    }
    return metas;
  }

  /**
   * Does this server have any META regions open on it, or any meta
   * regions being assigned to it?
   *
   * @param server Server IP:port
   * @return true if server has meta region assigned
   */
  public boolean isMetaServer(HServerAddress server) {
    for ( MetaRegion region : onlineMetaRegions.values() ) {
      if (server.equals(region.getServer())) {
        return true;
      }
    }

    // This might be expensive, but we need to make sure we dont
    // get double assignment to the same regionserver.
    synchronized(regionsInTransition) {
      for (RegionState s : regionsInTransition.values()) {
        if (s.getRegionInfo().isMetaRegion()
            && !s.isUnassigned()
            && s.getServerName() != null
            && s.getServerName().equals(server.toString())) {
          // TODO this code appears to be entirely broken, since
          // server.toString() has no start code, but s.getServerName()
          // does!
          LOG.fatal("I DONT BELIEVE YOU WILL EVER SEE THIS!");
          // Has an outstanding meta region to be assigned.
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Is this server assigned to transition the ROOT table. HBASE-1928
   *
   * @param server Server
   * @return true if server is transitioning the ROOT table
   */
  public boolean isRootInTransitionOnThisServer(final String server) {
    synchronized (this.regionsInTransition) {
      for (RegionState s : regionsInTransition.values()) {
        if (s.getRegionInfo().isRootRegion()
            && !s.isUnassigned()
            && s.getServerName() != null
            && s.getServerName().equals(server)) {
          // Has an outstanding root region to be assigned.
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Is this server assigned to transition a META table. HBASE-1928
   *
   * @param server Server
   * @return if this server was transitioning a META table then a not null HRegionInfo pointing to it
   */
  public HRegionInfo getMetaServerRegionInfo(final String server) {
    synchronized (this.regionsInTransition) {
      for (RegionState s : regionsInTransition.values()) {
        if (s.getRegionInfo().isMetaRegion()
            && !s.isUnassigned()
            && s.getServerName() != null
            && s.getServerName().equals(server)) {
          // Has an outstanding meta region to be assigned.
          return s.getRegionInfo();
        }
      }
    }
    return null;
  }

  /**
   * Call to take this metaserver offline for immediate reassignment.  Used only
   * when we know a region has shut down cleanly.
   *
   * A meta server is a server that hosts either -ROOT- or any .META. regions.
   *
   * If you are considering a unclean shutdown potentially, use ProcessServerShutdown which
   * calls other methods to immediately unassign root/meta but delay the reassign until the
   * log has been split.
   *
   * @param server the server that went down
   * @return true if this was in fact a meta server, false if it did not carry meta regions.
   */
  public synchronized boolean offlineMetaServer(HServerAddress server) {
    boolean hasMeta = false;

    // check to see if ROOT and/or .META. are on this server, reassign them.
    // use master.getRootRegionLocation.
    if (master.getRegionManager().getRootRegionLocation() != null &&
        server.equals(master.getRegionManager().getRootRegionLocation())) {
      LOG.info("Offlined ROOT server: " + server);
      reassignRootRegion();
      hasMeta = true;
    }
    // AND
    for ( MetaRegion region : onlineMetaRegions.values() ) {
      if (server.equals(region.getServer())) {
        LOG.info("Offlining META region: " + region);
        offlineMetaRegionWithStartKey(region.getStartKey());
        // Set for reassignment.
        setUnassigned(region.getRegionInfo(), true);
        hasMeta = true;
      }
    }
    return hasMeta;
  }

  /**
   * Remove a region from the region state map.
   *
   * @param info
   */
  public void removeRegion(HRegionInfo info) {
    synchronized (this.regionsInTransition) {
      this.regionsInTransition.remove(info.getRegionNameAsString());
      zkWrapper.deleteUnassignedRegion(info.getEncodedName());
    }
  }

  /**
   * @param regionName
   * @return true if the named region is in a transition state
   */
  public boolean regionIsInTransition(String regionName) {
    synchronized (this.regionsInTransition) {
      return regionsInTransition.containsKey(regionName);
    }
  }

  /**
   * @param regionName
   * @return true if the region is unassigned, pendingOpen or open
   */
  public boolean regionIsOpening(String regionName) {
    synchronized (this.regionsInTransition) {
      RegionState state = regionsInTransition.get(regionName);
      if (state != null) {
        return state.isOpening();
      }
    }
    return false;
  }

  /**
   * Set a region to unassigned. Always writes the region's unassigned znode.
   * @param info Region to set unassigned
   * @param force if true mark region unassigned whatever its current state
   */
  void setUnassigned(HRegionInfo info, boolean force) {
    setUnassignedGeneral(true, info, force);
  }

  void setUnassignedGeneral(boolean writeToZK, HRegionInfo info, boolean force) {
    RegionState s = null;
    long t0, t1, t2, t3;
    t0 = System.currentTimeMillis();
    synchronized(this.regionsInTransition) {
      t1 = System.currentTimeMillis();
      s = regionsInTransition.get(info.getRegionNameAsString());
      if (s == null) {
        byte[] data = null;
        try {
          data = Writables.getBytes(new RegionTransitionEventData(HBaseEventType.M2ZK_REGION_OFFLINE, HMaster.MASTER));
        } catch (IOException e) {
          // TODO: Review what we should do here.  If Writables work this
          //       should never happen
          LOG.error("Error creating event data for " + HBaseEventType.M2ZK_REGION_OFFLINE, e);
        }
        if (writeToZK) {
          zkWrapper.createOrUpdateUnassignedRegion(info.getEncodedName(), data);
          LOG.debug("Created/updated UNASSIGNED zNode " + info.getRegionNameAsString() +
                    " in state " + HBaseEventType.M2ZK_REGION_OFFLINE);
        }
        s = new RegionState(info, RegionState.State.UNASSIGNED);
        regionsInTransition.put(info.getRegionNameAsString(), s);
      }

      t2 = System.currentTimeMillis();
      if (force || (!s.isPendingOpenAckedOrUnacked() && !s.isOpen())) {
        // Refresh assignment information when a region is marked unassigned so
        // that it opens on the preferred server.
        this.assignmentManager.executeAssignmentPlan(info);
        s.setUnassigned();
      }
      t3 = System.currentTimeMillis();
    }

    if (LOG.isDebugEnabled())
      LOG.debug("Took " + this.toString() + " "
          + (t3 - t0) + " ms. for RegionManager.setUnassigned "
          + (t1 - t0) + " ms. to get the lock. "
          + (t2 - t1) + " ms. to update regionsInTransition. "
          + (t3 - t2) + " ms. to executeAssignmentPlan. "
          );
  }

  /**
   * Check if a region is on the unassigned list
   * @param info HRegionInfo to check for
   * @return true if on the unassigned list, false if it isn't. Note that this
   * means a region could not be on the unassigned list AND not be assigned, if
   * it happens to be between states.
   */
  public boolean isUnassigned(HRegionInfo info) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(info.getRegionNameAsString());
      if (s != null) {
        return s.isUnassigned();
      }
    }
    return false;
  }

  /**
   * Check if a region has been assigned and we're waiting for a response from
   * the region server.
   *
   * @param regionName name of the region
   * @return true if open, false otherwise
   */
  public boolean isPendingOpenAckedOrUnacked(String regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        return s.isPendingOpenAckedOrUnacked();
      }
    }
    return false;
  }

  /**
   * Region has been assigned to a server and the server has told us it is open
   * @param regionName
   */
  public void setPendingOpenAcked(String regionName) {
    Preconditions.checkNotNull(regionName);
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        s.setPendingOpenAcked();
      } else {
        LOG.debug("regionsInTransition does not have an entry for " + regionName);
      }
    }
  }

  /**
   * Region has been assigned to a server and the server has told us it is open
   * @param regionName
   */
  public void setOpen(String regionName) {
    Preconditions.checkNotNull(regionName);
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        s.setOpen();
        this.master.getMetrics().incRegionsOpened();
        if (s.serverName != null) {
          this.regionLocationHintToDetectDupAssignment.put(regionName, s.serverName);
        }
      }
    }
  }

  /**
   * Check if the region was last opened at the particular server
   * @param regionName
   * @param serverName
   * @return true if regionName was last opened at serverName
   */
  public boolean lastOpenedAt(String regionName, String serverName) {
    String openAt = this.regionLocationHintToDetectDupAssignment.get(regionName);
    return openAt != null && openAt.equals(serverName);
  }

  /**
   * @param regionName
   * @return true if region is marked to be offlined.
   */
  public boolean isOfflined(String regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        return s.isOfflined();
      }
    }
    return false;
  }

  /**
   * Mark a region as closing
   * @param serverName
   * @param regionInfo
   * @param setOffline
   */
  public void setClosing(String serverName, final HRegionInfo regionInfo,
      final boolean setOffline) {
    synchronized (this.regionsInTransition) {
      RegionState s =
        this.regionsInTransition.get(regionInfo.getRegionNameAsString());
      if (s == null) {
        s = new RegionState(regionInfo, RegionState.State.CLOSING);
      }
      // If region was asked to open before getting here, we could be taking
      // the wrong server name
      if(s.isPendingOpenAckedOrUnacked()) {
        serverName = s.getServerName();
      }
      s.setClosing(serverName, setOffline);
      this.regionsInTransition.put(regionInfo.getRegionNameAsString(), s);
      if (!setOffline) {
        // Refresh assignment information when a region is closed and not
        // marked offline so that it opens on the preferred server.
        this.assignmentManager.executeAssignmentPlan(regionInfo);
      }
    }
  }

  /**
   * Remove the map of region names to region infos waiting to be offlined for a
   * given server
   *
   * @param serverName
   * @return set of infos to close
   */
  public Set<HRegionInfo> getMarkedToClose(String serverName) {
    Set<HRegionInfo> result = new HashSet<HRegionInfo>();
    synchronized (regionsInTransition) {
      for (RegionState s: regionsInTransition.values()) {
        if (s.isClosing() && !s.isPendingClose() && !s.isClosed() &&
            s.getServerName().compareTo(serverName) == 0) {
          result.add(s.getRegionInfo());
        }
      }
    }
    return result;
  }

  /**
   * Returns the set of Regions that have been asked to open on this server
   * but, it has not acked yet.
   *
   * @param serverName
   * @return set of infos that were requested to open
   */
  public Set<HRegionInfo> getRegionsInPendingOpenUnacked(String serverName) {
    Set<HRegionInfo> result = new HashSet<HRegionInfo>();
    synchronized (regionsInTransition) {
      for (RegionState s: regionsInTransition.values()) {
        if (s.isPendingOpenUnacked() && s.getServerName().compareTo(serverName) == 0) {
          result.add(s.getRegionInfo());
        }
      }
    }
    return result;
  }

  /**
   * Called when we have told a region server to close the region
   *
   * @param regionName
   */
  public void setPendingClose(String regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        s.setPendingClose();
      }
    }
  }

  /**
   * @param regionName
   */
  public void setClosed(String regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        s.setClosed();
      }
    }
  }

  /**
   * Get the root region location.
   * @return HServerAddress describing root region server.
   */
  public HServerAddress getRootRegionLocation() {
    return HServerInfo.getAddress(rootRegionLocation.get());
  }

  /** Returns root region location as a server info object (with a start code) */
  public HServerInfo getRootServerInfo() {
    return rootRegionLocation.get();
  }

  /**
   * Block until either the root region location is available or we're shutting
   * down.
   */
  public void waitForRootRegionLocation() {
    synchronized (rootRegionLocation) {
      while (!master.isStopped() && rootRegionLocation.get() == null) {
        // rootRegionLocation will be filled in when we get an 'open region'
        // regionServerReport message from the HRegionServer that has been
        // allocated the ROOT region below.
        try {
          // Cycle rather than hold here in case master is closed meantime.
          rootRegionLocation.wait(this.master.getThreadWakeFrequency());
        } catch (InterruptedException e) {
          LOG.error("Interrupted when waiting for root region location");
          continue;
        }
      }
    }
  }

  /**
   * Return the number of meta regions.
   * @return number of meta regions
   */
  public int numMetaRegions() {
    return numberOfMetaRegions.get();
  }

  /**
   * Bump the count of meta regions up one
   */
  public void incrementNumMetaRegions() {
    numberOfMetaRegions.incrementAndGet();
  }

  private long getPauseTime(int tries) {
    int attempt = tries;
    if (attempt >= HConstants.RETRY_BACKOFF.length) {
      attempt = HConstants.RETRY_BACKOFF.length - 1;
    }
    return this.zooKeeperPause * HConstants.RETRY_BACKOFF[attempt];
  }

  private void sleep(int attempt) {
    try {
      Thread.sleep(getPauseTime(attempt));
    } catch (InterruptedException e) {
      // continue
    }
  }

  private void writeRootRegionLocationToZooKeeper(HServerInfo hsi) {
    for (int attempt = 0; attempt < zooKeeperNumRetries; ++attempt) {
      if (master.getZooKeeperWrapper().writeRootRegionLocation(hsi)) {
        return;
      }

      sleep(attempt);
    }

    LOG.error("Failed to write root region location to ZooKeeper after " +
              zooKeeperNumRetries + " retries, shutting down the cluster");

    this.master.requestClusterShutdown();
  }

  /**
   * Set the root region location.
   * @param address Address of the region server where the root lives
   */
  public void setRootRegionLocation(HServerInfo hsi) {
    writeRootRegionLocationToZooKeeper(hsi);
    synchronized (rootRegionLocation) {
      // the root region has been assigned, remove it from transition in ZK
      // ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN has the same regionName
      zkWrapper.deleteUnassignedRegion(HRegionInfo.ROOT_REGIONINFO.getEncodedName());
      rootRegionLocation.set(new HServerInfo(hsi));
      rootRegionLocation.notifyAll();
    }
  }

  /**
   * Set the number of meta regions.
   * @param num Number of meta regions
   */
  public void setNumMetaRegions(int num) {
    numberOfMetaRegions.set(num);
  }

  public void addRegionsInfo(int r, boolean isMeta)
  {
    if(isMeta) {
      metaRegionsCount = r;
    }
    else {
      notMetaRegionsCount = r;
    }
  }

  public int getRegionsCount()
  {
    //meta + not-meta + root
    return metaRegionsCount + notMetaRegionsCount + 1;
  }

  /**
   * Starts an action that is specific to a column family.
   * @param regionName
   * @param columnFamily
   * @param info
   * @param server
   * @param op
   */
  public void startCFAction(byte[] regionName, byte[] columnFamily,
      HRegionInfo info, HServerAddress server, HConstants.Modify op) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding operation " + op + " for column family : "
          + new String(columnFamily) + " from tasklist");
    }
    switch (op) {
      case TABLE_COMPACT:
        startCFAction(regionName, columnFamily, info, server,
            this.cfsToCompact);
        break;
      case TABLE_MAJOR_COMPACT:
        startCFAction(regionName, columnFamily, info, server,
            this.cfsToMajorCompact);
        break;
      default:
        throw new IllegalArgumentException("illegal table action " + op);
    }
  }

  private void startCFAction(final byte[] regionName,
      final byte[] columnFamily,
      final HRegionInfo info,
      final HServerAddress server,
      final SortedMap<byte[], SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>> map) {
    synchronized (map) {
      SortedMap<byte[], Pair<HRegionInfo, HServerAddress>> cfMap =
        map.get(regionName);
      if (cfMap == null) {
        cfMap = Collections.synchronizedSortedMap(
            new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
            (Bytes.BYTES_COMPARATOR));
      }
      cfMap.put(columnFamily, new Pair<HRegionInfo,HServerAddress>(info, server));
      map.put(regionName, cfMap);
    }
  }

  /**
   * @param regionName
   * @param info
   * @param server
   * @param op
   */
  public void startAction(byte[] regionName, HRegionInfo info,
      HServerAddress server, HConstants.Modify op) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding operation " + op + " from tasklist");
    }
    switch (op) {
      case TABLE_SPLIT:
        startAction(regionName, info, server, this.regionsToSplit);
        break;
      case TABLE_COMPACT:
        startAction(regionName, info, server, this.regionsToCompact);
        break;
      case TABLE_MAJOR_COMPACT:
        startAction(regionName, info, server, this.regionsToMajorCompact);
        break;
      case TABLE_FLUSH:
        startAction(regionName, info, server, this.regionsToFlush);
        break;
      default:
        throw new IllegalArgumentException("illegal table action " + op);
    }
  }

  private void startAction(final byte[] regionName, final HRegionInfo info,
      final HServerAddress server,
      final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>> map) {
    map.put(regionName, new Pair<HRegionInfo,HServerAddress>(info, server));
  }

  /**
   * @param regionName
   */
  public void endActions(byte[] regionName) {
    regionsToSplit.remove(regionName);
    regionsToCompact.remove(regionName);
    cfsToCompact.remove(regionName);
    cfsToMajorCompact.remove(regionName);
  }

  /**
   * Send messages to the given region server asking it to split any
   * regions in 'regionsToSplit', etc.
   * @param serverInfo
   * @param returnMsgs
   */
  public void applyActions(HServerInfo serverInfo, ArrayList<HMsg> returnMsgs) {
    applyActions(serverInfo, returnMsgs, this.regionsToCompact,
        HMsg.Type.MSG_REGION_COMPACT);
    applyActions(serverInfo, returnMsgs, this.regionsToSplit,
      HMsg.Type.MSG_REGION_SPLIT);
    applyActions(serverInfo, returnMsgs, this.regionsToFlush,
        HMsg.Type.MSG_REGION_FLUSH);
    applyActions(serverInfo, returnMsgs, this.regionsToMajorCompact,
        HMsg.Type.MSG_REGION_MAJOR_COMPACT);

    // CF specific actions for a region.
    applyCFActions(serverInfo, returnMsgs, this.cfsToCompact,
        HMsg.Type.MSG_REGION_CF_COMPACT);
    applyCFActions(serverInfo, returnMsgs, this.cfsToMajorCompact,
        HMsg.Type.MSG_REGION_CF_MAJOR_COMPACT);
  }

  private void applyActions(final HServerInfo serverInfo,
      final ArrayList<HMsg> returnMsgs,
      final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>> map,
      final HMsg.Type msg) {
    HServerAddress addr = serverInfo.getServerAddress();
    synchronized (map) {
      Iterator<Pair<HRegionInfo, HServerAddress>> i = map.values().iterator();
      while (i.hasNext()) {
        Pair<HRegionInfo,HServerAddress> pair = i.next();
        if (addr.equals(pair.getSecond())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Sending " + msg + " " + pair.getFirst() + " to " + addr);
          }
          returnMsgs.add(new HMsg(msg, pair.getFirst()));
          i.remove();
        }
      }
    }
  }

  /**
   * Applies actions specific to a column family within a region.
   */
  private void applyCFActions(final HServerInfo serverInfo,
      final ArrayList<HMsg> returnMsgs,
      final SortedMap<byte[], SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>> map,
      final HMsg.Type msg) {
    HServerAddress addr = serverInfo.getServerAddress();
    synchronized (map) {
      Iterator <SortedMap<byte[], Pair <HRegionInfo, HServerAddress>>> it1 =
        map.values().iterator();
      while(it1.hasNext()) {
        SortedMap<byte[], Pair<HRegionInfo, HServerAddress>> cfMap = it1.next();
        Iterator<Map.Entry<byte[], Pair<HRegionInfo, HServerAddress>>> it2 =
          cfMap.entrySet().iterator();
        while (it2.hasNext()) {
          Entry<byte[], Pair<HRegionInfo, HServerAddress>> mapPairs =
              it2.next();
          Pair<HRegionInfo,HServerAddress> pair =
            (Pair<HRegionInfo,HServerAddress>)mapPairs.getValue();
          if (addr.equals(pair.getSecond())) {
            byte[] columnFamily = (byte[])mapPairs.getKey();
            if (LOG.isDebugEnabled()) {
              LOG.debug("Sending " + msg + " " + pair.getFirst() + " to " + addr
                  + " for column family : " + new String(columnFamily));
            }
            returnMsgs.add(new HMsg(msg, pair.getFirst(), columnFamily));
            it2.remove();
          }
        }
        if (cfMap.isEmpty()) {
          // If entire map is empty, remove it from the parent map.
          it1.remove();
        }
      }
    }
  }

  /**
   * Classes which implement LoadBalancer are used to balance regions across
   * servers. They operate by unassigning some regions from a server so that
   * those regions can be assigned to other servers.
   */
  abstract class LoadBalancer {

    // The maximum number of regions to close on one server during one iteration
    // of load balancing.
    // -1 or 0 to turn off
    // TODO: change default in HBASE-862, need a suggestion
    protected final int maxRegToClose;    // hbase.regions.close.max
    protected final float slop;           // hbase.regions.slop

    LoadBalancer() {
      Configuration conf = master.getConfiguration();
      float confSlop = conf.getFloat(HConstants.LOAD_BALANCER_SLOP_KEY, (float) 0.3);
      this.slop = confSlop <= 0 ? 1 : confSlop;
      this.maxRegToClose = conf.getInt("hbase.regions.close.max", -1);
    }

    /**
     * Balance regions across servers by unassigning some regions from the
     * specified server if they could be served elsewhere with better load
     * distribution.
     * @param info the server whose regions are being balanced
     * @param mostLoadedRegions the regions to consider for balancing
     * @param returnMsgs any regions to be unassigned will be added here
     */
    public abstract void loadBalancing(HServerInfo info,
        HRegionInfo[] mostLoadedRegions, ArrayList<HMsg> returnMsgs);
  }

  /**
   * Class to balance regions according to preferred assignments. Regions which
   * are not on their preferred host but could be will be unassigned from their
   * current host and assigned to their preferred host. This behavior will also
   * consider secondary and tertiary preferred hosts if the primary is dead.
   */
  class AssignmentLoadBalancer extends LoadBalancer {
    long timeDelta =
        HConstants.DEFAULT_HBASE_REGION_ASSIGNMENT_LOADBALANCER_WAITTIME_MS;
    Map<HServerAddress, Long> lastAssignedForTime;

    AssignmentLoadBalancer(Configuration conf) {
      super();
      timeDelta = conf.getLong(HConstants.HBASE_REGION_ASSIGNMENT_LOADBALANCER_WAITTIME_MS,
          HConstants.DEFAULT_HBASE_REGION_ASSIGNMENT_LOADBALANCER_WAITTIME_MS);
      lastAssignedForTime = new HashMap<HServerAddress, Long>();
    }

    /**
     * Unassign some regions if there is a server with a higher preference, or
     * if a server with equal preference has a lower load.
     * @param info the server from which to unassign regions
     * @param mostLoadedRegions the candidate regions for moving
     * @param returnMsgs region close messages to be passed to the server
     */
    @Override
    public void loadBalancing(HServerInfo info, HRegionInfo[] mostLoadedRegions,
        ArrayList<HMsg> returnMsgs) {

      if (master.isServerBlackListed(info.getHostnamePort())) {
        LOG.debug("Server " + info.getHostnamePort() + " is blacklisted. " +
            "Cannot load balance the regions for this server. Returning");
        return;
      }
      int regionsUnassigned = balanceToPrimary(info, mostLoadedRegions,
          returnMsgs);

      if (regionsUnassigned <= 0) {
        regionsUnassigned = balanceFromUnfavored(info, mostLoadedRegions,
            returnMsgs);
      }

      if (regionsUnassigned <= 0) {
        regionsUnassigned = balanceSecondaries(info, mostLoadedRegions,
            returnMsgs);
      }
    }

    /**
     * If the primary assignment of any region hosted by the server {@code info}
     * is not that server, but the primary assignment server is alive, move that
     * region to the primary assignment server.
     * @param info the server whose regions to balance
     * @param mostLoadedRegions the regions to balance
     * @param returnMsgs region close messages to be passed to the server
     * @return the number of regions that were unassigned
     */
    private int balanceToPrimary(HServerInfo info,
        HRegionInfo[] mostLoadedRegions, ArrayList<HMsg> returnMsgs) {
      int regionsUnassigned = 0;
      // If for any of these regions, this server is not the primary but the
      // primary is alive, unassign that region and let it move to the primary.
      for (HRegionInfo region : mostLoadedRegions) {
        List<HServerAddress> preferences =
            assignmentManager.getAssignmentFromPlan(region);
        if (preferences == null || preferences.isEmpty()) {
          // No prefered assignment, do nothing.
          continue;
        } else if (info.getServerAddress().equals(preferences.get(0))) {
          // This server is the primary, do nothing.
          continue;
        } else {
          if (getLoadIfAlive(preferences.get(0)) == null) {
            // Primary server is not alive, try next region.
            continue;
          }
          if (canOffloadTo(preferences.get(0)) == false) {
            // Primary server is not ready to accept a new region.
            continue;
          }

          // Primary server is alive, unassign this region.
          if (unassignRegion(info, region, returnMsgs)) {
            regionsUnassigned++;
            if (regionsUnassigned >= maxRegToClose && maxRegToClose > 0) {
              LOG.debug("Unassigned " + region.getRegionNameAsString()
                  + " from the server " + info.getHostnamePort()
                  + " because the primary server: " + preferences.get(0)
                  + " is a live");
              unassignedFor(preferences.get(0));
              return regionsUnassigned;
            }
          }
        }
      }
      return regionsUnassigned;
    }

    /**
     * If for any of the regions hosted by server {@code info}, that server is
     * not a favored node and one of the favored nodes for that region is alive,
     * move that region to a favored node.
     * @param info the server whose regions to balance
     * @param mostLoadedRegions the regions to balance
     * @param returnMsgs region close messages to be passed to the server
     * @return the number of regions that were unassigned
     */
    private int balanceFromUnfavored(HServerInfo info,
        HRegionInfo[] mostLoadedRegions, ArrayList<HMsg> returnMsgs) {
      int regionsUnassigned = 0;
      for (HRegionInfo region : mostLoadedRegions) {
        List<HServerAddress> preferences =
            assignmentManager.getAssignmentFromPlan(region);
        if (preferences == null || preferences.isEmpty()) {
          // No preferredAssignment, do nothing.
          continue;
        } else if (preferences.contains(info.getServerAddress())) {
          // This server already a favored node, do nothing.
          continue;
        }

        int leastLoad = Integer.MAX_VALUE;
        HServerAddress leastLoadedSecondary = null;

        // This server is not a favored node for the current region. Check if
        // one of the secondary servers is alive, and determine which of those
        // has the least load.
        for (int i = 1; i < preferences.size(); i++) {
          HServerLoad secondaryLoad = getLoadIfAlive(preferences.get(i));
          if (secondaryLoad != null &&
              canOffloadTo(preferences.get(i)) &&
              secondaryLoad.getNumberOfRegions() < leastLoad) {
            leastLoad = secondaryLoad.getNumberOfRegions();
            leastLoadedSecondary = preferences.get(i);
          }
        }

        if (leastLoadedSecondary != null) {
          // Move the region if the current server is not a preferred assignment
          // for that region.
          if (unassignRegion(info, region, returnMsgs)) {
            regionsUnassigned++;
            unassignedFor(leastLoadedSecondary);
            if (regionsUnassigned >= maxRegToClose && maxRegToClose > 0) {
              LOG.debug("Unassigned " + region.getRegionNameAsString()
                  + " from the unfavoraed server " + info.getHostnamePort()
                  + " because one least loaded secondary server: "
                  + leastLoadedSecondary + " is a live");
              return regionsUnassigned;
            }
          }
        }
      }
      return regionsUnassigned;
    }

    /**
     * For any of the regions hosted by server {@code info}, if that server is
     * currently hosted by an overloaded secondary node and another secondary
     * node is underloaded, move the region from the overloaded node to the
     * underloaded one.
     * @param info the server whose regions to balance
     * @param mostLoadedRegions the regions to balance
     * @param returnMsgs region close messages to be passed to the server
     * @return the number of regions that were unassigned
     */
    private int balanceSecondaries(HServerInfo info,
        HRegionInfo[] mostLoadedRegions, ArrayList<HMsg> returnMsgs) {
      int regionsUnassigned = 0;
      double avgLoad = master.getAverageLoad();
      int avgLoadMinusSlop = (int)Math.floor(avgLoad * (1 - this.slop)) - 1;
      int avgLoadPlusSlop = (int)Math.ceil(avgLoad * (1 + this.slop));
      int serverLoad = master.getServerManager().getServersToLoad().get(
          info.getServerName()).getNumberOfRegions();

      for (HRegionInfo region : mostLoadedRegions) {
        List<HServerAddress> preferences =
            assignmentManager.getAssignmentFromPlan(region);
        if (preferences == null || preferences.isEmpty()) {
          // No preferredAssignment, do nothing.
          continue;
        } else if (info.getServerAddress().equals(preferences.get(0))) {
          // This server is the primary, do nothing.
          continue;
        }

        // This server is not the primary for the current region. Check if
        // another favored node has lower load and move the region there if so.
        for (int i = 1; i < preferences.size(); i++) {
          if (preferences.get(i).equals(info.getServerAddress())) {
            // Same server as currently hosting region, try next one.
            continue;
          }
          HServerLoad otherLoad = getLoadIfAlive(preferences.get(i));
          if (otherLoad == null) {
            // Other server is not alive, try next one.
            continue;
          }

          if (!canOffloadTo(preferences.get(i))) {
            // Other server is not accepting new regions.
            continue;
          }

          // Only move the region if the other server is under-loaded and the
          // current server is overloaded.
          if (serverLoad - regionsUnassigned > avgLoadPlusSlop &&
              otherLoad.getNumberOfRegions() < avgLoadMinusSlop) {
            if (unassignRegion(info, region, returnMsgs)) {
              unassignedFor(preferences.get(i));
              // Need to override transient assignment that may have been added
              // for the region to its current server when unassigning.
              assignmentManager.removeTransientAssignment(
                  info.getServerAddress(), region);
              assignmentManager.addTransientAssignment(preferences.get(i),
                  region);
              regionsUnassigned++;
              if (regionsUnassigned >= maxRegToClose && maxRegToClose > 0) {
                LOG.debug("Unassigned " + region.getRegionNameAsString()
                    + " from the overloaded secondary server: "
                    + info.getHostnamePort()
                    + " because another low loaded secondary server: "
                    + preferences.get(i) + " is a live");
                return regionsUnassigned;
              }
            }
          }
        }
      }
      return regionsUnassigned;
    }

    private boolean canOffloadTo(HServerAddress hServerAddress) {

      // Server is black listed. Cannot move the regions to this server.
      if (ServerManager.isServerBlackListed(hServerAddress.getHostNameWithPort())) {
        LOG.info("Blacklisted Server. Cannot offload. Returning...");
        return false;
      }

      Map<HServerAddress, Long> map = lastAssignedForTime;
      Long ts = map.get(hServerAddress);
      long tscur = EnvironmentEdgeManager.currentTimeMillis();

      if (ts == null || (tscur - ts) > timeDelta) {
        return true;
      }
      return false;
    }

    private void unassignedFor(HServerAddress hServerAddress) {
      Map<HServerAddress, Long> map = this.lastAssignedForTime;
      long currentTime = EnvironmentEdgeManager.currentTimeMillis();
      map.put(hServerAddress, currentTime);
    }

    /**
     * Get the load for the region server at the address {@code server} unless
     * that server does not exist, is dead, or is shutting down. In cases where
     * the load cannot be retrieved, return null.
     * @param server the address of the server whose load to get
     * @return the load for the server or null
     */
    private HServerLoad getLoadIfAlive(HServerAddress server) {
      HServerInfo other =
          master.getServerManager().getHServerInfo(server);
      if (other == null ||
          master.getServerManager().isDeadProcessingPending(other.getServerName())) {
        return null;
      }
      return master.getServerManager().getServersToLoad()
          .get(other.getServerName());
    }

    /**
     * Unassign a certain region from a certain server, unless that region is
     * already in transition. A region close message will be added tot he list
     * of return messages.
     * @param info the server on which to close the region
     * @param region the region to be unassigned
     * @param returnMsgs a region close message will be added here
     * @return true if the region was unassigned
     */
    private boolean unassignRegion(HServerInfo info, HRegionInfo region,
        ArrayList<HMsg> returnMsgs) {
      if (region.isRootRegion() || region.isMetaTable()) {
        return false;
      }
      final String regionName = region.getRegionNameAsString();
      if (regionIsInTransition(regionName)) {
        // Region may have already been unassigned, abort this operation.
        return false;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("AssignmentLoadBalancer going to close region " + regionName);
      }
      // Make a message to close the region
      returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_CLOSE, region,
        OVERLOADED));
      // Mark the region as closing
      setClosing(info.getServerName(), region, false);
      setPendingClose(regionName);
      return true;
    }
  }

  /**
   * Class to balance region servers load.
   * It keeps Region Servers load in slop range by unassigning Regions
   * from most loaded servers.
   *
   * Equilibrium is reached when load of all serves are in slop range
   * [avgLoadMinusSlop, avgLoadPlusSlop], where
   *  avgLoadPlusSlop = Math.ceil(avgLoad * (1 + this.slop)), and
   *  avgLoadMinusSlop = Math.floor(avgLoad * (1 - this.slop)) - 1.
   */
  class DefaultLoadBalancer extends LoadBalancer {
    DefaultLoadBalancer() {
      super();
    }

    /**
     * Balance server load by unassigning some regions.
     *
     * @param info - server info
     * @param mostLoadedRegions - array of most loaded regions
     * @param returnMsgs - array of return massages
     */
    @Override
    public void loadBalancing(HServerInfo info, HRegionInfo[] mostLoadedRegions,
        ArrayList<HMsg> returnMsgs) {
      HServerLoad servLoad = info.getLoad();
      double avg = master.getAverageLoad();

      // nothing to balance if server load not more then average load
      if(servLoad.getLoad() <= Math.floor(avg) || avg <= 2.0) {
        return;
      }

      // check if current server is overloaded
      int numRegionsToClose = balanceFromOverloaded(info.getServerName(),
        servLoad, avg);

      // check if we can unload server by low loaded servers
      if(numRegionsToClose <= 0) {
        numRegionsToClose = balanceToLowloaded(info.getServerName(), servLoad,
            avg);
      }

      if(maxRegToClose > 0) {
        numRegionsToClose = Math.min(numRegionsToClose, maxRegToClose);
      }

      if(numRegionsToClose > 0) {
        unassignSomeRegions(info, numRegionsToClose, mostLoadedRegions,
            returnMsgs);
      }
    }

    /*
     * Check if server load is not overloaded (with load > avgLoadPlusSlop).
     * @return number of regions to unassign.
     */
    private int balanceFromOverloaded(final String serverName,
        HServerLoad srvLoad, double avgLoad) {
      int avgLoadPlusSlop = (int)Math.ceil(avgLoad * (1 + this.slop));
      int numSrvRegs = srvLoad.getNumberOfRegions();
      if (numSrvRegs > avgLoadPlusSlop) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Server " + serverName + " is carrying more than its fair " +
            "share of regions: " +
            "load=" + numSrvRegs + ", avg=" + avgLoad + ", slop=" + this.slop);
        }
        return numSrvRegs - (int)Math.ceil(avgLoad);
      }
      return 0;
    }

    /*
     * Check if server is most loaded and can be unloaded to
     * low loaded servers (with load < avgLoadMinusSlop).
     * @return number of regions to unassign.
     */
    private int balanceToLowloaded(String srvName, HServerLoad srvLoad,
        double avgLoad) {

      ServerLoadMap<HServerLoad> serverLoadMap = master.getServerManager().getServersToLoad();

      if (!serverLoadMap.isMostLoadedServer(srvName))
        return 0;

      // this server is most loaded, we will try to unload it by lowest
      // loaded servers
      int avgLoadMinusSlop = (int)Math.floor(avgLoad * (1 - this.slop)) - 1;
      HServerLoad lowestServerLoad = serverLoadMap.getLowestLoad();
      int lowestLoad = lowestServerLoad.getNumberOfRegions();

      if(lowestLoad >= avgLoadMinusSlop)
        return 0; // there is no low loaded servers

      int lowSrvCount = serverLoadMap.numServersByLoad(lowestServerLoad);
      int numSrvRegs = srvLoad.getNumberOfRegions();
      int numMoveToLowLoaded = (avgLoadMinusSlop - lowestLoad) * lowSrvCount;

      int numRegionsToClose = numSrvRegs - (int)Math.floor(avgLoad);
      numRegionsToClose = Math.min(numRegionsToClose, numMoveToLowLoaded);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Server(s) are carrying only " + lowestLoad + " regions. " +
          "Server " + srvName + " is most loaded (" + numSrvRegs +
          "). Shedding " + numRegionsToClose + " regions to pass to " +
          " least loaded (numMoveToLowLoaded=" + numMoveToLowLoaded +")");
      }
      return numRegionsToClose;
    }
  }

  /**
   * @return Snapshot of regionsintransition as a sorted Map.
   */
  NavigableMap<String, String> getRegionsInTransition() {
    NavigableMap<String, String> result = new TreeMap<String, String>();
    synchronized (this.regionsInTransition) {
      if (this.regionsInTransition.isEmpty()) return result;
      for (Map.Entry<String, RegionState> e: this.regionsInTransition.entrySet()) {
        result.put(e.getKey(), e.getValue().toString());
      }
    }
    return result;
  }

  /**
   * @param regionname Name to clear from regions in transistion.
   * @return True if we removed an element for the passed regionname.
   */
  boolean clearFromInTransition(final byte [] regionname) {
    boolean result = false;
    synchronized (this.regionsInTransition) {
      if (this.regionsInTransition.isEmpty()) return result;
      for (Map.Entry<String, RegionState> e: this.regionsInTransition.entrySet()) {
        if (Bytes.equals(regionname, e.getValue().getRegionName())) {
          this.regionsInTransition.remove(e.getKey());
          LOG.debug("Removed " + e.getKey() + ", " + e.getValue());
          result = true;
          break;
        }
      }
    }
    return result;
  }

  /*
   * State of a Region as it transitions from closed to open, etc.  See
   * note on regionsInTransition data member above for listing of state
   * transitions.
   */
  static class RegionState implements Comparable<RegionState> {
    private final HRegionInfo regionInfo;

    enum State {
      UNASSIGNED, // awaiting a server to be assigned
      PENDING_OPEN_UNACKED, // told a server to open, not sure if it got the message.
      PENDING_OPEN_ACKED, // told a server to open, it got the message, hasn't opened yet
      OPEN, // has been opened on RS, but not yet marked in META/ROOT
      CLOSING, // a msg has been enqueued to close ths region, but not delivered to RS yet
      PENDING_CLOSE, // msg has been delivered to RS to close this region
      CLOSED // region has been closed but not yet marked in meta

    }

    private State state;

    private boolean isOfflined;

    /* Set when region is assigned or closing */
    private String serverName = null;

    /* Constructor */
    RegionState(HRegionInfo info, State state) {
      this.regionInfo = info;
      this.state = state;
    }

    synchronized HRegionInfo getRegionInfo() {
      return this.regionInfo;
    }

    synchronized byte [] getRegionName() {
      return this.regionInfo.getRegionName();
    }

    /*
     * @return Server this region was assigned to
     */
    synchronized String getServerName() {
      return this.serverName;
    }

    /*
     * @return true if the region is being opened
     */
    synchronized boolean isOpening() {
      return state == State.UNASSIGNED ||
        state == State.PENDING_OPEN_UNACKED ||
        state == State.PENDING_OPEN_ACKED ||
        state == State.OPEN;
    }

    /*
     * @return true if region is unassigned
     */
    synchronized boolean isUnassigned() {
      return state == State.UNASSIGNED;
    }

    /*
     * Note: callers of this method (reassignRootRegion,
     * regionsAwaitingAssignment, setUnassigned) ensure that this method is not
     * called unless it is safe to do so.
     */
    synchronized void setUnassigned() {
      state = State.UNASSIGNED;
      this.serverName = null;
    }

    synchronized boolean isPendingOpenAcked() {
      return state == State.PENDING_OPEN_ACKED;
    }

    synchronized boolean isPendingOpenUnacked() {
      return state == State.PENDING_OPEN_UNACKED;
    }

    synchronized boolean isPendingOpenAckedOrUnacked() {
      return state == State.PENDING_OPEN_ACKED || state == State.PENDING_OPEN_UNACKED;
    }

    /*
     * @param serverName Server region was assigned to.
     */
    synchronized void setPendingOpenUnacked(final String serverName) {
      if (state != State.UNASSIGNED) {
        LOG.warn("Cannot assign a region that is not currently unassigned. " +
          "FIX!! State: " + toString());
      }
      state = State.PENDING_OPEN_UNACKED;
      this.serverName = serverName;
    }

    synchronized void setPendingOpenAcked() {
      // it is okay to setPendingOpenAcked if it is already acked.
      if (state != State.PENDING_OPEN_UNACKED && state != State.PENDING_OPEN_ACKED) {
        LOG.warn("Cannot assign a region that is not currently unacked. " +
          "FIX!! State: " + toString());
      }
      state = State.PENDING_OPEN_ACKED;
    }

    synchronized boolean isOpen() {
      return state == State.OPEN;
    }

    synchronized void setOpen() {
      if (state != State.PENDING_OPEN_ACKED && state != State.PENDING_OPEN_UNACKED) {
        LOG.warn("Cannot set a region as open if it has not been pending. " +
          "FIX!! State: " + toString());
      }
      state = State.OPEN;
    }

    synchronized boolean isClosing() {
      return state == State.CLOSING;
    }

    synchronized void setClosing(String serverName, boolean setOffline) {
      state = State.CLOSING;
      this.serverName = serverName;
      this.isOfflined = setOffline;
    }

    synchronized boolean isPendingClose() {
      return state == State.PENDING_CLOSE;
    }

    synchronized void setPendingClose() {
      if (state != State.CLOSING) {
        LOG.warn("Cannot set a region as pending close if it has not been " +
          "closing.  FIX!! State: " + toString());
      }
      state = State.PENDING_CLOSE;
    }

    synchronized boolean isClosed() {
      return state == State.CLOSED;
    }

    synchronized void setClosed() {
      if (state != State.PENDING_CLOSE &&
          state != State.PENDING_OPEN_UNACKED &&
          state != State.PENDING_OPEN_ACKED &&
          state != State.CLOSING) {
        throw new IllegalStateException(
            "Cannot set a region to be closed if it was not already marked as" +
            " pending close, pending open or closing. State: " + this);
      }
      state = State.CLOSED;
    }

    synchronized boolean isOfflined() {
      return (state == State.CLOSING ||
        state == State.PENDING_CLOSE) && isOfflined;
    }

    @Override
    public synchronized String toString() {
      return ("name=" + Bytes.toString(getRegionName()) +
          ", state=" + this.state);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return this.compareTo((RegionState) o) == 0;
    }

    @Override
    public int hashCode() {
      return Bytes.toString(getRegionName()).hashCode();
    }

    @Override
    public int compareTo(RegionState o) {
      if (o == null) {
        return 1;
      }
      return Bytes.compareTo(getRegionName(), o.getRegionName());
    }
  }



  /**
   * Method used to do housekeeping for holding regions for a RegionServer going
   * down for a restart
   *
   * @param regionServer
   *          the RegionServer going down for a restart
   * @param regions
   *          the HRegions it was previously serving
   */
  public void addRegionServerForRestart(final HServerInfo regionServer,
      Set<HRegionInfo> regions) {
    LOG.debug("Holding regions of restartng server: " +
        regionServer.getServerName());
    HServerAddress addr = regionServer.getServerAddress();

    if (master.isServerBlackListed(addr.getHostNameWithPort())) {
      LOG.warn("Not adding regions for restarting server " + addr.getHostAddressWithPort()
          + " as the server is blacklisted.");
      return;
    }
    for (HRegionInfo region : regions) {
      assignmentManager.addTransientAssignment(addr, region);
    }
  }

  /**
   * Get the current ThrottledRegionReopener for a given table if it already exists. Create one with
   * the specified wait interval and maxNumRegionsClosed if it doesn't exists yet.
   * @param tableName The table this re-opener will work on.
   * @param waitInterval The amount of time to spread the close of regions over.
   * @param maxNumRegionsClosed The maximum number of regions to have closed at a time.
   * @return
   */
  public ThrottledRegionReopener createThrottledReopener(String tableName, int waitInterval, int maxNumRegionsClosed) {
    if (!tablesReopeningRegions.containsKey(tableName)) {
      ThrottledRegionReopener throttledReopener = new ThrottledRegionReopener(
          tableName,
          this.master,
          this,
          waitInterval,
          maxNumRegionsClosed);
      tablesReopeningRegions.put(tableName, throttledReopener);
    }
    return tablesReopeningRegions.get(tableName);
  }

  /**
   * Return the throttler for this table
   * @param tableName
   * @return
   */
  public ThrottledRegionReopener getThrottledReopener(String tableName) {
    return tablesReopeningRegions.get(tableName);
  }

  /**
   * Delete the throttler when the operation is complete
   * @param tableName
   */
  public void deleteThrottledReopener(String tableName) {
    // if tablesReopeningRegions.contains do something
    if (tablesReopeningRegions.containsKey(tableName)) {
      tablesReopeningRegions.remove(tableName);

      //The table was locked by the master before the alter table command
      //was issued. Now that the reopen of the regions is done the table should
      //be unlocked.
      try {
        master.unlockTable(Bytes.toBytes(tableName));
      } catch (IllegalStateException ise) {
        LOG.warn("RegionManager tried to unlock " + tableName + " and it was not locked.");
      } catch (IOException e) {
        LOG.warn("An exception was encountered while RegionManager was trying to unlock " + tableName +
            ". " + e.getStackTrace());
      }
      LOG.debug("Removed throttler for " + tableName);
    } else {
      LOG.debug("Tried to delete a throttled reopener, but it does not exist.");
    }
  }

  /**
   * When the region is opened, check if it is reopening and notify the throttler
   * for further processing.
   * @param region
   */
  public void notifyRegionReopened(HRegionInfo region) {
    String tableName = region.getTableDesc().getNameAsString();
    if (tablesReopeningRegions.containsKey(tableName)) {
      tablesReopeningRegions.get(tableName).notifyRegionOpened(region);
    }
  }

  MetaScanner getMetaScanner() {
    return metaScannerThread;
  }

  /**
   * Composes a map of .META. region locations for both online .META. regions and regions that
   * we know are assigned to regionservers, but have not been scanned yet. This is used on master
   * startup to write pending region location changes from the ZK unassigned directory to .META.
   */
  NavigableMap<byte[], MetaRegion> getAllMetaRegionLocations() {
    NavigableMap<byte[], MetaRegion> m =
        new TreeMap<byte[], MetaRegion>(Bytes.BYTES_COMPARATOR);
    m.putAll(metaRegionLocationsBeforeScan);
    m.putAll(onlineMetaRegions);
    return m;
  }

  /**
   * Modifies region state in regionsInTransition based on the initial scan of the ZK unassigned
   * directory.
   * @param event event type written by the regionserver to the znode
   * @param regionInfo region info
   * @param serverName regionserver name
   */
  void setRegionStateOnRecovery(HBaseEventType event, HRegionInfo regionInfo, String serverName) {
    String regionName = regionInfo.getRegionNameAsString();
    String stateStr = null;
    if (event == HBaseEventType.RS2ZK_REGION_CLOSING ||
        event == HBaseEventType.RS2ZK_REGION_CLOSED) {
      synchronized (regionsInTransition) {
        RegionState s = regionsInTransition.get(regionName);
        if (s == null) {
          s = new RegionState(regionInfo, RegionState.State.PENDING_CLOSE);
          regionsInTransition.put(regionName, s);
        } else {
          s.setClosing(serverName, s.isOfflined());
          s.setPendingClose();
        }
        stateStr = s.toString();
      }
    }

    if (event == HBaseEventType.RS2ZK_REGION_OPENED ||
        event == HBaseEventType.RS2ZK_REGION_OPENING) {
      synchronized (regionsInTransition) {
        RegionState s = regionsInTransition.get(regionName);
        if (s == null) {
          s = new RegionState(regionInfo, RegionState.State.PENDING_OPEN_ACKED);
          regionsInTransition.put(regionName, s);
        } else {
          s.setUnassigned();
          s.setPendingOpenUnacked(serverName);
          s.setPendingOpenAcked();
        }
        stateStr = s.toString();
      }
    }

    if (stateStr != null) {
      LOG.info("Set state in regionsInTransition: " + stateStr);
    }
  }

  /** Recovers root region location from ZK. Should only be called on master startup. */
  void recoverRootRegionLocationFromZK() {
    HServerInfo rootLocationInZK = zkWrapper.readRootRegionServerInfo(
      TableServers.getProtocolVersionFromConf(this.master.getConfiguration()));
    if (rootLocationInZK != null) {
      synchronized (rootRegionLocation) {
        rootRegionLocation.set(rootLocationInZK);
        rootRegionLocation.notifyAll();
      }
    }
  }

}
