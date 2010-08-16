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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.catalog.RootLocationEditor;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.master.LoadBalancer.RegionPlan;
import org.apache.hadoop.hbase.master.handler.ClosedRegionHandler;
import org.apache.hadoop.hbase.master.handler.OpenedRegionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTableDisable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.NodeAndData;
import org.apache.hadoop.io.Writable;
import org.apache.zookeeper.KeeperException;

/**
 * Manages and performs region assignment.
 * <p>
 * Monitors ZooKeeper for events related to regions in transition.
 * <p>
 * Handles existing regions in transition during master failover.
 */
public class AssignmentManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(AssignmentManager.class);

  protected Server master;

  private ServerManager serverManager;

  private CatalogTracker catalogTracker;

  private TimeoutMonitor timeoutMonitor;

  /** Regions currently in transition. */
  private final Map<String,RegionState> regionsInTransition =
    new TreeMap<String,RegionState>();

  /** Plans for region movement. */
  // TODO: When do plans get cleaned out?  Ever?
  private final Map<String,RegionPlan> regionPlans =
    new TreeMap<String,RegionPlan>();

  /** Set of tables that have been disabled. */
  private final Set<String> disabledTables =
    Collections.synchronizedSet(new HashSet<String>());

  /**
   * Server to regions assignment map.
   * Contains the set of regions currently assigned to a given server.
   */
  private final SortedMap<HServerInfo,Set<HRegionInfo>> servers =
        new TreeMap<HServerInfo,Set<HRegionInfo>>();

  /**
   * Region to server assignment map.
   * Contains the server a given region is currently assigned to.
   * This object should be used for all synchronization around servers/regions.
   */
  private final SortedMap<HRegionInfo,HServerInfo> regions =
    new TreeMap<HRegionInfo,HServerInfo>();

  private final ReentrantLock assignLock = new ReentrantLock();

  /**
   * Constructs a new assignment manager.
   *
   * <p>This manager must be started with {@link #start()}.
   *
   * @param status master status
   * @param serverManager
   * @param catalogTracker
   */
  public AssignmentManager(Server master, ServerManager serverManager,
      CatalogTracker catalogTracker) {
    super(master.getZooKeeper());
    this.master = master;
    this.serverManager = serverManager;
    this.catalogTracker = catalogTracker;
    Configuration conf = master.getConfiguration();
    this.timeoutMonitor = new TimeoutMonitor(
        conf.getInt("hbase.master.assignment.timeoutmonitor.period", 30000),
        master,
        conf.getInt("hbase.master.assignment.timeoutmonitor.timeout", 15000));
    Threads.setDaemonThreadRunning(timeoutMonitor,
        master.getServerName() + ".timeoutMonitor");
  }

  /**
   * Cluster startup.  Reset all unassigned nodes and assign all user regions.
   * @throws IOException
   * @throws KeeperException
   */
  void processStartup() throws IOException, KeeperException {
    // Cleanup any existing ZK nodes and start watching
    ZKAssign.deleteAllNodes(watcher);
    ZKUtil.listChildrenAndWatchForNewChildren(watcher,
        watcher.assignmentZNode);
    // Assign all existing user regions out
    assignAllUserRegions();
  }

  /**
   * Handle failover.  Restore state from META and ZK.  Handle any regions in
   * transition.
   * @throws KeeperException
   * @throws IOException
   */
  void processFailover() throws KeeperException, IOException {
    // Concurrency note: In the below the accesses on regionsInTransition are
    // outside of a synchronization block where usually all accesses to RIT are
    // synchronized.  The presumption is that in this case it is safe since this
    // method is being played by a single thread on startup.

    // Scan META to build list of existing regions, servers, and assignment
    rebuildUserRegions();
    // Pickup any disabled tables
    rebuildDisabledTables();
    // Check existing regions in transition
    List<String> nodes = ZKUtil.listChildrenAndWatchForNewChildren(watcher,
        watcher.assignmentZNode);
    if(nodes.isEmpty()) {
      LOG.info("No regions in transition in ZK to process on failover");
      return;
    }
    LOG.info("Failed-over master needs to process " + nodes.size() +
        " regions in transition");
    for(String regionName : nodes) {
      RegionTransitionData data = ZKAssign.getData(watcher, regionName);
      HRegionInfo regionInfo =
        MetaReader.getRegion(catalogTracker, data.getRegionName()).getFirst();
      String encodedName = regionInfo.getEncodedName();
      switch(data.getEventType()) {
        case RS2ZK_REGION_CLOSING:
          // Just insert region into RIT.
          // If this never updates the timeout will trigger new assignment
          regionsInTransition.put(encodedName,
              new RegionState(regionInfo, RegionState.State.CLOSING,
                  data.getStamp()));
          break;

        case RS2ZK_REGION_CLOSED:
          // Region is closed, insert into RIT and handle it
          regionsInTransition.put(encodedName,
              new RegionState(regionInfo, RegionState.State.CLOSED,
                  data.getStamp()));
          new ClosedRegionHandler(master, this, data, regionInfo).execute();
          break;

        case RS2ZK_REGION_OPENING:
          // Just insert region into RIT
          // If this never updates the timeout will trigger new assignment
          regionsInTransition.put(encodedName,
              new RegionState(regionInfo, RegionState.State.OPENING,
                  data.getStamp()));
          break;

        case RS2ZK_REGION_OPENED:
          // Region is opened, insert into RIT and handle it
          regionsInTransition.put(encodedName,
              new RegionState(regionInfo, RegionState.State.OPENING,
                  data.getStamp()));
          new OpenedRegionHandler(master, this, data, regionInfo,
              serverManager.getServerInfo(data.getServerName())).execute();
          break;
      }
    }
  }

  /**
   * Handles various states an unassigned node can be in.
   * <p>
   * Method is called when a state change is suspected for an unassigned node.
   * <p>
   * This deals with skipped transitions (we got a CLOSED but didn't see CLOSING
   * yet).
   * @param data
   */
  private void handleRegion(RegionTransitionData data) {
    synchronized(regionsInTransition) {
      // Verify this is a known server
      if(!serverManager.isServerOnline(data.getServerName())) {
        LOG.warn("Attempted to handle region transition for server " +
            data.getServerName() + " but server is not online");
        return;
      }
      String encodedName = HRegionInfo.encodeRegionName(data.getRegionName());
      LOG.debug("Handling region transition for server " +
          data.getServerName() + " and region " + encodedName);
      RegionState regionState = regionsInTransition.get(encodedName);
      switch(data.getEventType()) {

        case RS2ZK_REGION_CLOSING:
          // Should see CLOSING after we have asked it to CLOSE or additional
          // times after already being in state of CLOSING
          if(regionState == null ||
              (!regionState.isPendingClose() && !regionState.isClosing())) {
            LOG.warn("Received CLOSING for region " + encodedName +
                " from server " + data.getServerName() + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_CLOSE or CLOSING states");
            return;
          }
          // Transition to CLOSING (or update stamp if already CLOSING)
          regionState.update(RegionState.State.CLOSING, data.getStamp());
          break;

        case RS2ZK_REGION_CLOSED:
          // Should see CLOSED after CLOSING but possible after PENDING_CLOSE
          if(regionState == null ||
              (!regionState.isPendingClose() && !regionState.isClosing())) {
            LOG.warn("Received CLOSED for region " + encodedName +
                " from server " + data.getServerName() + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_CLOSE or CLOSING states");
            return;
          }
          // Handle CLOSED by assigning elsewhere or stopping if a disable
          new ClosedRegionHandler(master, this, data, regionState.getRegion())
          .submit();
          break;

        case RS2ZK_REGION_OPENING:
          // Should see OPENING after we have asked it to OPEN or additional
          // times after already being in state of OPENING
          if(regionState == null ||
              (!regionState.isPendingOpen() && !regionState.isOpening())) {
            LOG.warn("Received OPENING for region " + encodedName +
                " from server " + data.getServerName() + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_OPEN or OPENING states");
            return;
          }
          // Transition to OPENING (or update stamp if already OPENING)
          regionState.update(RegionState.State.OPENING, data.getStamp());
          break;

        case RS2ZK_REGION_OPENED:
          // Should see OPENED after OPENING but possible after PENDING_OPEN
          if(regionState == null ||
              (!regionState.isPendingOpen() && !regionState.isOpening())) {
            LOG.warn("Received OPENED for region " + encodedName +
                " from server " + data.getServerName() + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_OPEN or OPENING states");
            return;
          }
          // If this is a catalog table, update catalog manager accordingly
          // Moving root and meta editing over to RS who does the opening
          LOG.debug("Processing OPENED for region " + regionState.getRegion() +
              " which isMeta[" + regionState.getRegion().isMetaRegion() + "] " +
              " isRoot[" + regionState.getRegion().isRootRegion() + "]");

          // Used to have updating of root/meta locations here but it's
          // automatic in CatalogTracker now

          // Handle OPENED by removing from transition and deleted zk node
          new OpenedRegionHandler(master, this, data, regionState.getRegion(),
              serverManager.getServerInfo(data.getServerName()))
          .submit();
          break;
      }
    }
  }

  // ZooKeeper events

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING or CLOSING of a region by
   * creating an unassigned node.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further events</li>
   *   <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public void nodeCreated(String path) {
    if(path.startsWith(watcher.assignmentZNode)) {
      synchronized(regionsInTransition) {
        try {
          RegionTransitionData data = ZKAssign.getData(watcher, path);
          if(data == null) {
            return;
          }
          handleRegion(data);
        } catch (KeeperException e) {
          master.abort("Unexpected ZK exception reading unassigned node data", e);
        }
      }
    }
  }

  /**
   * Existing unassigned node has had data changed.
   *
   * <p>This happens when an RS transitions from OFFLINE to OPENING, or between
   * OPENING/OPENED and CLOSING/CLOSED.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further events</li>
   *   <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public void nodeDataChanged(String path) {
    if(path.startsWith(watcher.assignmentZNode)) {
      synchronized(regionsInTransition) {
        try {
          RegionTransitionData data = ZKAssign.getData(watcher, path);
          if(data == null) {
            return;
          }
          handleRegion(data);
        } catch (KeeperException e) {
          master.abort("Unexpected ZK exception reading unassigned node data", e);
        }
      }
    }
  }

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING or CLOSING of a region by
   * creating an unassigned node.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further children changed events</li>
   *   <li>Watch all new children for changed events</li>
   *   <li>Read all children and handle them</li>
   * </ol>
   */
  @Override
  public void nodeChildrenChanged(String path) {
    if(path.equals(watcher.assignmentZNode)) {
      synchronized(regionsInTransition) {
        try {
          List<NodeAndData> newNodes = ZKUtil.watchAndGetNewChildren(watcher,
              watcher.assignmentZNode);
          for(NodeAndData newNode : newNodes) {
            LOG.debug("Handling new unassigned node: " + newNode);
            handleRegion(RegionTransitionData.fromBytes(newNode.getData()));
          }
        } catch(KeeperException e) {
          master.abort("Unexpected ZK exception reading unassigned children", e);
        }
      }
    }
  }

  /**
   * Marks the region as online.  Removes it from regions in transition and
   * updates the in-memory assignment information.
   * <p>
   * Used when a region has been successfully opened on a region server.
   * @param regionInfo
   * @param serverInfo
   */
  public void regionOnline(HRegionInfo regionInfo, HServerInfo serverInfo) {
    synchronized(regionsInTransition) {
      regionsInTransition.remove(regionInfo.getEncodedName());
      regionsInTransition.notifyAll();
    }
    synchronized(regions) {
      regions.put(regionInfo, serverInfo);
      Set<HRegionInfo> regionSet = servers.get(serverInfo);
      if(regionSet == null) {
        regionSet = new TreeSet<HRegionInfo>();
        servers.put(serverInfo, regionSet);
      }
      regionSet.add(regionInfo);
    }
  }

  /**
   * Marks the region as offline.  Removes it from regions in transition and
   * removes in-memory assignment information.
   * <p>
   * Used when a region has been closed and should remain closed.
   * @param regionInfo
   * @param serverInfo
   */
  public void regionOffline(HRegionInfo regionInfo) {
    synchronized(regionsInTransition) {
      regionsInTransition.remove(regionInfo.getEncodedName());
      regionsInTransition.notifyAll();
    }
    synchronized(regions) {
      HServerInfo serverInfo = regions.remove(regionInfo);
      Set<HRegionInfo> serverRegions = servers.get(serverInfo);
      serverRegions.remove(regionInfo);
    }
  }

  /**
   * Sets the region as offline by removing in-memory assignment information but
   * retaining transition information.
   * <p>
   * Used when a region has been closed but should be reassigned.
   * @param regionInfo
   */
  public void setOffline(HRegionInfo regionInfo) {
    synchronized(regions) {
      HServerInfo serverInfo = regions.remove(regionInfo);
      Set<HRegionInfo> serverRegions = servers.get(serverInfo);
      serverRegions.remove(regionInfo);
    }
  }

  // Assignment methods

  /**
   * Assigns the specified region.
   * <p>
   * If a RegionPlan is available with a valid destination then it will be used
   * to determine what server region is assigned to.  If no RegionPlan is
   * available, region will be assigned to a random available server.
   * <p>
   * Updates the RegionState and sends the OPEN RPC.
   * <p>
   * This will only succeed if the region is in transition and in a CLOSED or
   * OFFLINE state or not in transition (in-memory not zk), and of course, the
   * chosen server is up and running (It may have just crashed!).  If the
   * in-memory checks pass, the zk node is forced to OFFLINE before assigning.
   *
   * @param regionName server to be assigned
   */
  public void assign(HRegionInfo region) {
    LOG.debug("Starting assignment for region " + region);
    // Grab the state of this region and synchronize on it
    String encodedName = region.getEncodedName();
    RegionState state;
    // This assignLock is used bridging the two synchronization blocks.  Once
    // we've made it into the 'state' synchronization block, then we can let
    // go of this lock.  There must be a better construct that this -- St.Ack 20100811
    this.assignLock.lock();
    try {
      synchronized(regionsInTransition) {
        state = regionsInTransition.get(encodedName);
        if(state == null) {
          state = new RegionState(region, RegionState.State.OFFLINE);
          regionsInTransition.put(encodedName, state);
        }
      }
      synchronized(state) {
        this.assignLock.unlock();
        assign(state);
      }
    } finally {
      if (this.assignLock.isHeldByCurrentThread()) this.assignLock.unlock();
    }
  }

  /**
   * Caller must hold lock on the passed <code>state</code> object.
   * @param state 
   */
  private void assign(final RegionState state) {
    if(!state.isClosed() && !state.isOffline()) {
      LOG.info("Attempting to assign region but it is in transition and in " +
          "an unexpected state:" + state);
      return;
    } else {
      state.update(RegionState.State.OFFLINE);
    }
    try {
      if(!ZKAssign.createOrForceNodeOffline(master.getZooKeeper(),
          state.getRegion(), master.getServerName())) {
        LOG.warn("Attempted to create/force node into OFFLINE state before " +
            "completing assignment but failed to do so");
        return;
      }
    } catch (KeeperException e) {
      master.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      return;
    }
    // Pickup existing plan or make a new one
    String encodedName = state.getRegion().getEncodedName();
    RegionPlan plan;
    synchronized(regionPlans) {
      plan = regionPlans.get(encodedName);
      if(plan == null) {
        LOG.debug("No previous transition plan for " + encodedName +
            " so generating a random one from " + serverManager.numServers() +
            " ( " + serverManager.getOnlineServers().size() + ") available servers");
        plan = new RegionPlan(encodedName, null,
            LoadBalancer.randomAssignment(serverManager.getOnlineServersList()));
        regionPlans.put(encodedName, plan);
      }
    }
    try {
      // Send OPEN RPC. This can fail if the server on other end is is not up.
      serverManager.sendRegionOpen(plan.getDestination(), state.getRegion());
    } catch (Throwable t) {
      LOG.warn("Failed assignment of " + state.getRegion());
      // Clean out plan we failed execute and one that doesn't look like it'll
      // succeed anyways; we need a new plan!
      synchronized(regionPlans) {
        this.regionPlans.remove(encodedName);
      }
    }
    // Transition RegionState to PENDING_OPEN
    state.update(RegionState.State.PENDING_OPEN);
  }

  /**
   * Unassigns the specified region.
   * <p>
   * Updates the RegionState and sends the CLOSE RPC.
   * <p>
   * If a RegionPlan is already set, it will remain.  If this is being used
   * to disable a table, be sure to use {@link #disableTable(String)} to ensure
   * regions are not onlined after being closed.
   *
   * @param regionName server to be unassigned
   */
  public void unassign(HRegionInfo region) {
    LOG.debug("Starting unassignment of region " + region + " (offlining)");
    // Check if this region is currently assigned
    if (!regions.containsKey(region)) {
      LOG.debug("Attempted to unassign region " + region + " but it is not " +
          "currently assigned anywhere");
      return;
    }
    String regionName = region.getEncodedName();
    // Grab the state of this region and synchronize on it
    RegionState state;
    synchronized(regionsInTransition) {
      state = regionsInTransition.get(regionName);
      if(state == null) {
        state = new RegionState(region, RegionState.State.PENDING_CLOSE);
        regionsInTransition.put(regionName, state);
      } else {
        LOG.debug("Attempting to unassign region " + region + " but it is " +
            "already in transition (" + state.getState() + ")");
        return;
      }
    }
    // Send CLOSE RPC
    try {
      serverManager.sendRegionClose(regions.get(region), state.getRegion());
    } catch (NotServingRegionException e) {
      LOG.warn("Attempted to close region " + region + " but got an NSRE", e);
    }
  }

  /**
   * Waits until the specified region has completed assignment.
   * <p>
   * If the region is already assigned, returns immediately.  Otherwise, method
   * blocks until the region is assigned.
   * @param regionInfo region to wait on assignment for
   * @throws InterruptedException
   */
  public void waitForAssignment(HRegionInfo regionInfo)
  throws InterruptedException {
    synchronized(regions) {
      while(!regions.containsKey(regionInfo)) {
        regions.wait();
      }
    }
  }

  /**
   * Assigns the ROOT region.
   * <p>
   * Assumes that ROOT is currently closed and is not being actively served by
   * any RegionServer.
   * <p>
   * Forcibly unsets the current root region location in ZooKeeper and assigns
   * ROOT to a random RegionServer.
   * @throws KeeperException 
   */
  public void assignRoot() throws KeeperException {
    RootLocationEditor.deleteRootLocation(this.master.getZooKeeper());
    assign(HRegionInfo.ROOT_REGIONINFO);
  }

  /**
   * Assigns the META region.
   * <p>
   * Assumes that META is currently closed and is not being actively served by
   * any RegionServer.
   * <p>
   * Forcibly assigns META to a random RegionServer.
   */
  public void assignMeta() {
    // Force assignment to a random server
    assign(HRegionInfo.FIRST_META_REGIONINFO);
  }

  /**
   * Assigns all user regions, if any exist.  Used during cluster startup.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown and the cluster
   * should be shutdown.
   */
  public void assignAllUserRegions() throws IOException {
    // First experiment at synchronous assignment
    // Simpler because just wait for no regions in transition

    // Scan META for all user regions
    List<HRegionInfo> allRegions =
      MetaScanner.listAllRegions(master.getConfiguration());
    if (allRegions == null || allRegions.isEmpty()) {
      return;
    }

    // Get all available servers
    List<HServerInfo> servers = serverManager.getOnlineServersList();

    LOG.info("Assigning " + allRegions.size() + " across " + servers.size() +
        " servers");

    // Generate a cluster startup region placement plan
    Map<HServerInfo,List<HRegionInfo>> bulkPlan =
      LoadBalancer.bulkAssignment(allRegions, servers);

    // For each server, create OFFLINE nodes and send OPEN RPCs
    for(Map.Entry<HServerInfo,List<HRegionInfo>> entry : bulkPlan.entrySet()) {
      HServerInfo server = entry.getKey();
      List<HRegionInfo> regions = entry.getValue();
      LOG.debug("Assigning " + regions.size() + " regions to " + server);
      for(HRegionInfo region : regions) {
        LOG.debug("Assigning " + region + " to " + server);
        String regionName = region.getEncodedName();
        RegionPlan plan = new RegionPlan(regionName, null,server);
        regionPlans.put(regionName, plan);
        assign(region);
      }
    }

    // Wait for no regions to be in transition
    try {
      waitUntilNoRegionsInTransition();
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for regions to be assigned", e);
      throw new IOException(e);
    }

    LOG.info("\n\nAll user regions have been assigned");
  }

  private void rebuildUserRegions() throws IOException {
    Map<HRegionInfo,HServerAddress> allRegions =
      MetaReader.fullScan(catalogTracker);
    for(Map.Entry<HRegionInfo,HServerAddress> region : allRegions.entrySet()) {
      HServerAddress regionLocation = region.getValue();
      HRegionInfo regionInfo = region.getKey();
      if(regionLocation == null) {
        regions.put(regionInfo, null);
        continue;
      }
      HServerInfo serverInfo = serverManager.getHServerInfo(regionLocation);
      regions.put(regionInfo, serverInfo);
      Set<HRegionInfo> regionSet = servers.get(serverInfo);
      if(regionSet == null) {
        regionSet = new TreeSet<HRegionInfo>();
        servers.put(serverInfo, regionSet);
      }
      regionSet.add(regionInfo);
    }
  }

  /**
   * Blocks until there are no regions in transition.  It is possible that there
   * are regions in transition immediately after this returns but guarantees
   * that if it returns without an exception that there was a period of time
   * with no regions in transition from the point-of-view of the in-memory
   * state of the Master.
   * @throws InterruptedException
   */
  public void waitUntilNoRegionsInTransition() throws InterruptedException {
    synchronized(regionsInTransition) {
      while(regionsInTransition.size() > 0) {
        regionsInTransition.wait();
      }
    }
  }

  /**
   * @return A copy of the Map of regions currently in transition.
   */
  public NavigableMap<String, RegionState> getRegionsInTransition() {
    return new TreeMap<String, RegionState>(this.regionsInTransition);
  }

  /**
   * Checks if the specified table has been disabled by the user.
   * @param tableName
   * @return
   */
  public boolean isTableDisabled(String tableName) {
    synchronized(disabledTables) {
      return disabledTables.contains(tableName);
    }
  }

  /**
   * Checks if the table of the specified region has been disabled by the user.
   * @param regionName
   * @return
   */
  public boolean isTableOfRegionDisabled(byte [] regionName) {
    return isTableDisabled(Bytes.toString(
        HRegionInfo.getTableName(regionName)));
  }

  /**
   * Sets the specified table to be disabled.
   * @param tableName table to be disabled
   */
  public void disableTable(String tableName) {
    synchronized(disabledTables) {
      if(!isTableDisabled(tableName)) {
        disabledTables.add(tableName);
        try {
          ZKTableDisable.disableTable(master.getZooKeeper(), tableName);
        } catch (KeeperException e) {
          LOG.warn("ZK error setting table as disabled", e);
        }
      }
    }
  }

  /**
   * Unsets the specified table from being disabled.
   * <p>
   * This operation only acts on the in-memory
   * @param tableName table to be undisabled
   */
  public void undisableTable(String tableName) {
    synchronized(disabledTables) {
      if(isTableDisabled(tableName)) {
        disabledTables.remove(tableName);
        try {
          ZKTableDisable.undisableTable(master.getZooKeeper(), tableName);
        } catch (KeeperException e) {
          LOG.warn("ZK error setting table as disabled", e);
        }
      }
    }
  }

  /**
   * Rebuild the set of disabled tables from zookeeper.  Used during master
   * failover.
   */
  private void rebuildDisabledTables() {
    synchronized(disabledTables) {
      List<String> disabledTables;
      try {
        disabledTables = ZKTableDisable.getDisabledTables(master.getZooKeeper());
      } catch (KeeperException e) {
        LOG.warn("ZK error getting list of disabled tables", e);
        return;
      }
      if(!disabledTables.isEmpty()) {
        LOG.info("Rebuilt list of " + disabledTables.size() + " disabled " +
            "tables from zookeeper");
        disabledTables.addAll(disabledTables);
      }
    }
  }

  /**
   * Gets the online regions of the specified table.
   * @param tableName
   * @return
   */
  public List<HRegionInfo> getRegionsOfTable(byte[] tableName) {
    List<HRegionInfo> tableRegions = new ArrayList<HRegionInfo>();
    for(HRegionInfo regionInfo : regions.tailMap(new HRegionInfo(
        new HTableDescriptor(tableName), null, null)).keySet()) {
      if(Bytes.equals(regionInfo.getTableDesc().getName(), tableName)) {
        tableRegions.add(regionInfo);
      } else {
        break;
      }
    }
    return tableRegions;
  }

  /**
   * Unsets the specified table as disabled (enables it).
   */
  public class TimeoutMonitor extends Chore {

    private final int timeout;

    /**
     * Creates a periodic monitor to check for time outs on region transition
     * operations.  This will deal with retries if for some reason something
     * doesn't happen within the specified timeout.
     * @param period
   * @param stopper When {@link Stoppable#isStopped()} is true, this thread will
   * cleanup and exit cleanly.
     * @param timeout
     */
    public TimeoutMonitor(final int period, final Stoppable stopper,
        final int timeout) {
      super("AssignmentTimeoutMonitor", period, stopper);
      this.timeout = timeout;
    }

    @Override
    protected void chore() {
      synchronized(regionsInTransition) {
        // Iterate all regions in transition checking for time outs
        long now = System.currentTimeMillis();
        for(RegionState regionState : regionsInTransition.values()) {
          if(regionState.getStamp() + timeout <= now) {
            HRegionInfo regionInfo = regionState.getRegion();
            String regionName = regionInfo.getEncodedName();
            LOG.info("Region transition timed out for region " + regionName);
            // Expired!  Do a retry.
            switch(regionState.getState()) {
              case OFFLINE:
              case CLOSED:
                LOG.info("Region has been OFFLINE or CLOSED for too long, " +
                    "reassigning " + regionInfo.getRegionNameAsString());
                assign(regionState.getRegion());
                break;
              case PENDING_OPEN:
              case OPENING:
                LOG.info("Region has been PENDING_OPEN or OPENING for too " +
                    "long, reassigning " + regionInfo.getRegionNameAsString());
                assign(regionState.getRegion());
                break;
              case OPEN:
                LOG.warn("Long-running region in OPEN state?  This should " +
                    "not happen");
                break;
              case PENDING_CLOSE:
              case CLOSING:
                LOG.info("Region has been PENDING_CLOSE or CLOSING for too " +
                    "long, resending close rpc");
                unassign(regionInfo);
                break;
            }
          }
        }
      }
    }
  }

  public static class RegionState implements Writable {
    private HRegionInfo region;

    public enum State {
      OFFLINE,        // region is in an offline state
      PENDING_OPEN,   // sent rpc to server to open but has not begun
      OPENING,        // server has begun to open but not yet done
      OPEN,           // server opened region and updated meta
      PENDING_CLOSE,  // sent rpc to server to close but has not begun
      CLOSING,        // server has begun to close but not yet done
      CLOSED          // server closed region and updated meta
    }

    private State state;
    private long stamp;

    public RegionState() {}

    RegionState(HRegionInfo region, State state) {
      this(region, state, System.currentTimeMillis());
    }

    RegionState(HRegionInfo region, State state, long stamp) {
      this.region = region;
      this.state = state;
      this.stamp = stamp;
    }

    public void update(State state, long stamp) {
      this.state = state;
      this.stamp = stamp;
    }

    public void update(State state) {
      this.state = state;
      this.stamp = System.currentTimeMillis();
    }

    public State getState() {
      return state;
    }

    public long getStamp() {
      return stamp;
    }

    public HRegionInfo getRegion() {
      return region;
    }

    public boolean isClosing() {
      return state == State.CLOSING;
    }

    public boolean isClosed() {
      return state == State.CLOSED;
    }

    public boolean isPendingClose() {
      return state == State.PENDING_CLOSE;
    }

    public boolean isOpening() {
      return state == State.OPENING;
    }

    public boolean isOpened() {
      return state == State.OPEN;
    }

    public boolean isPendingOpen() {
      return state == State.PENDING_OPEN;
    }

    public boolean isOffline() {
      return state == State.OFFLINE;
    }

    @Override
    public String toString() {
      return "RegionState (" + region.getRegionNameAsString() + ") " + state +
             " at time " + stamp;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      region = new HRegionInfo();
      region.readFields(in);
      state = State.valueOf(in.readUTF());
      stamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      region.write(out);
      out.writeUTF(state.name());
      out.writeLong(stamp);
    }
  }
}
