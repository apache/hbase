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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.favored.FavoredNodesPromoter;
import org.apache.hadoop.hbase.master.AssignmentListener;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsAssignmentManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates.ServerState;
import org.apache.hadoop.hbase.master.assignment.RegionStates.ServerStateNode;
import org.apache.hadoop.hbase.master.balancer.FavoredStochasticBalancer;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureInMemoryChore;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.regionserver.SequenceId;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

/**
 * The AssignmentManager is the coordinator for region assign/unassign operations.
 * <ul>
 * <li>In-memory states of regions and servers are stored in {@link RegionStates}.</li>
 * <li>hbase:meta state updates are handled by {@link RegionStateStore}.</li>
 * </ul>
 * Regions are created by CreateTable, Split, Merge.
 * Regions are deleted by DeleteTable, Split, Merge.
 * Assigns are triggered by CreateTable, EnableTable, Split, Merge, ServerCrash.
 * Unassigns are triggered by DisableTable, Split, Merge
 */
@InterfaceAudience.Private
public class AssignmentManager implements ServerListener {
  private static final Logger LOG = LoggerFactory.getLogger(AssignmentManager.class);

  // TODO: AMv2
  //  - handle region migration from hbase1 to hbase2.
  //  - handle sys table assignment first (e.g. acl, namespace)
  //  - handle table priorities
  //  - If ServerBusyException trying to update hbase:meta, we abort the Master
  //   See updateRegionLocation in RegionStateStore.
  //
  // See also
  // https://docs.google.com/document/d/1eVKa7FHdeoJ1-9o8yZcOTAQbv0u0bblBlCCzVSIn69g/edit#heading=h.ystjyrkbtoq5
  // for other TODOs.

  public static final String BOOTSTRAP_THREAD_POOL_SIZE_CONF_KEY =
      "hbase.assignment.bootstrap.thread.pool.size";

  public static final String ASSIGN_DISPATCH_WAIT_MSEC_CONF_KEY =
      "hbase.assignment.dispatch.wait.msec";
  private static final int DEFAULT_ASSIGN_DISPATCH_WAIT_MSEC = 150;

  public static final String ASSIGN_DISPATCH_WAITQ_MAX_CONF_KEY =
      "hbase.assignment.dispatch.wait.queue.max.size";
  private static final int DEFAULT_ASSIGN_DISPATCH_WAITQ_MAX = 100;

  public static final String RIT_CHORE_INTERVAL_MSEC_CONF_KEY =
      "hbase.assignment.rit.chore.interval.msec";
  private static final int DEFAULT_RIT_CHORE_INTERVAL_MSEC = 60 * 1000;

  public static final String ASSIGN_MAX_ATTEMPTS =
      "hbase.assignment.maximum.attempts";
  private static final int DEFAULT_ASSIGN_MAX_ATTEMPTS = Integer.MAX_VALUE;

  /** Region in Transition metrics threshold time */
  public static final String METRICS_RIT_STUCK_WARNING_THRESHOLD =
      "hbase.metrics.rit.stuck.warning.threshold";
  private static final int DEFAULT_RIT_STUCK_WARNING_THRESHOLD = 60 * 1000;

  private final ProcedureEvent<?> metaAssignEvent = new ProcedureEvent<>("meta assign");
  private final ProcedureEvent<?> metaLoadEvent = new ProcedureEvent<>("meta load");

  /** Listeners that are called on assignment events. */
  private final CopyOnWriteArrayList<AssignmentListener> listeners =
      new CopyOnWriteArrayList<AssignmentListener>();

  private final MetricsAssignmentManager metrics;
  private final RegionInTransitionChore ritChore;
  private final MasterServices master;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final RegionStates regionStates = new RegionStates();
  private final RegionStateStore regionStateStore;

  private final Map<ServerName, Set<byte[]>> rsReports = new HashMap<>();

  private final boolean shouldAssignRegionsWithFavoredNodes;
  private final int assignDispatchWaitQueueMaxSize;
  private final int assignDispatchWaitMillis;
  private final int assignMaxAttempts;

  private final Object checkIfShouldMoveSystemRegionLock = new Object();

  private Thread assignThread;

  public AssignmentManager(final MasterServices master) {
    this(master, new RegionStateStore(master));
  }

  public AssignmentManager(final MasterServices master, final RegionStateStore stateStore) {
    this.master = master;
    this.regionStateStore = stateStore;
    this.metrics = new MetricsAssignmentManager();

    final Configuration conf = master.getConfiguration();

    // Only read favored nodes if using the favored nodes load balancer.
    this.shouldAssignRegionsWithFavoredNodes = FavoredStochasticBalancer.class.isAssignableFrom(
        conf.getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, Object.class));

    this.assignDispatchWaitMillis = conf.getInt(ASSIGN_DISPATCH_WAIT_MSEC_CONF_KEY,
        DEFAULT_ASSIGN_DISPATCH_WAIT_MSEC);
    this.assignDispatchWaitQueueMaxSize = conf.getInt(ASSIGN_DISPATCH_WAITQ_MAX_CONF_KEY,
        DEFAULT_ASSIGN_DISPATCH_WAITQ_MAX);

    this.assignMaxAttempts = Math.max(1, conf.getInt(ASSIGN_MAX_ATTEMPTS,
        DEFAULT_ASSIGN_MAX_ATTEMPTS));

    int ritChoreInterval = conf.getInt(RIT_CHORE_INTERVAL_MSEC_CONF_KEY,
        DEFAULT_RIT_CHORE_INTERVAL_MSEC);
    this.ritChore = new RegionInTransitionChore(ritChoreInterval);
  }

  public void start() throws IOException, KeeperException {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    LOG.trace("Starting assignment manager");

    // Register Server Listener
    master.getServerManager().registerListener(this);

    // Start the Assignment Thread
    startAssignmentThread();

    // load meta region state
    ZKWatcher zkw = master.getZooKeeper();
    // it could be null in some tests
    if (zkw != null) {
      RegionState regionState = MetaTableLocator.getMetaRegionState(zkw);
      RegionStateNode regionStateNode =
        regionStates.getOrCreateRegionStateNode(RegionInfoBuilder.FIRST_META_REGIONINFO);
      synchronized (regionStateNode) {
        regionStateNode.setRegionLocation(regionState.getServerName());
        regionStateNode.setState(regionState.getState());
        setMetaAssigned(regionState.getRegion(), regionState.getState() == State.OPEN);
      }
    }
  }

  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    LOG.info("Stopping assignment manager");

    // The AM is started before the procedure executor,
    // but the actual work will be loaded/submitted only once we have the executor
    final boolean hasProcExecutor = master.getMasterProcedureExecutor() != null;

    // Remove the RIT chore
    if (hasProcExecutor) {
      master.getMasterProcedureExecutor().removeChore(this.ritChore);
    }

    // Stop the Assignment Thread
    stopAssignmentThread();

    // Stop the RegionStateStore
    regionStates.clear();

    // Unregister Server Listener
    master.getServerManager().unregisterListener(this);

    // Update meta events (for testing)
    if (hasProcExecutor) {
      metaLoadEvent.suspend();
      for (RegionInfo hri: getMetaRegionSet()) {
        setMetaAssigned(hri, false);
      }
    }
  }

  public boolean isRunning() {
    return running.get();
  }

  public Configuration getConfiguration() {
    return master.getConfiguration();
  }

  public MetricsAssignmentManager getAssignmentManagerMetrics() {
    return metrics;
  }

  private LoadBalancer getBalancer() {
    return master.getLoadBalancer();
  }

  private MasterProcedureEnv getProcedureEnvironment() {
    return master.getMasterProcedureExecutor().getEnvironment();
  }

  private MasterProcedureScheduler getProcedureScheduler() {
    return getProcedureEnvironment().getProcedureScheduler();
  }

  int getAssignMaxAttempts() {
    return assignMaxAttempts;
  }

  /**
   * Add the listener to the notification list.
   * @param listener The AssignmentListener to register
   */
  public void registerListener(final AssignmentListener listener) {
    this.listeners.add(listener);
  }

  /**
   * Remove the listener from the notification list.
   * @param listener The AssignmentListener to unregister
   */
  public boolean unregisterListener(final AssignmentListener listener) {
    return this.listeners.remove(listener);
  }

  public RegionStates getRegionStates() {
    return regionStates;
  }

  public RegionStateStore getRegionStateStore() {
    return regionStateStore;
  }

  public List<ServerName> getFavoredNodes(final RegionInfo regionInfo) {
    return this.shouldAssignRegionsWithFavoredNodes?
        ((FavoredStochasticBalancer)getBalancer()).getFavoredNodes(regionInfo):
          ServerName.EMPTY_SERVER_LIST;
  }

  // ============================================================================================
  //  Table State Manager helpers
  // ============================================================================================
  TableStateManager getTableStateManager() {
    return master.getTableStateManager();
  }

  public boolean isTableEnabled(final TableName tableName) {
    return getTableStateManager().isTableState(tableName, TableState.State.ENABLED);
  }

  public boolean isTableDisabled(final TableName tableName) {
    return getTableStateManager().isTableState(tableName,
      TableState.State.DISABLED, TableState.State.DISABLING);
  }

  // ============================================================================================
  //  META Helpers
  // ============================================================================================
  private boolean isMetaRegion(final RegionInfo regionInfo) {
    return regionInfo.isMetaRegion();
  }

  public boolean isMetaRegion(final byte[] regionName) {
    return getMetaRegionFromName(regionName) != null;
  }

  public RegionInfo getMetaRegionFromName(final byte[] regionName) {
    for (RegionInfo hri: getMetaRegionSet()) {
      if (Bytes.equals(hri.getRegionName(), regionName)) {
        return hri;
      }
    }
    return null;
  }

  public boolean isCarryingMeta(final ServerName serverName) {
    // TODO: handle multiple meta
    return isCarryingRegion(serverName, RegionInfoBuilder.FIRST_META_REGIONINFO);
  }

  private boolean isCarryingRegion(final ServerName serverName, final RegionInfo regionInfo) {
    // TODO: check for state?
    final RegionStateNode node = regionStates.getRegionStateNode(regionInfo);
    return(node != null && serverName.equals(node.getRegionLocation()));
  }

  private RegionInfo getMetaForRegion(final RegionInfo regionInfo) {
    //if (regionInfo.isMetaRegion()) return regionInfo;
    // TODO: handle multiple meta. if the region provided is not meta lookup
    // which meta the region belongs to.
    return RegionInfoBuilder.FIRST_META_REGIONINFO;
  }

  // TODO: handle multiple meta.
  private static final Set<RegionInfo> META_REGION_SET =
      Collections.singleton(RegionInfoBuilder.FIRST_META_REGIONINFO);
  public Set<RegionInfo> getMetaRegionSet() {
    return META_REGION_SET;
  }

  // ============================================================================================
  //  META Event(s) helpers
  // ============================================================================================
  /**
   * Notice that, this only means the meta region is available on a RS, but the AM may still be
   * loading the region states from meta, so usually you need to check {@link #isMetaLoaded()} first
   * before checking this method, unless you can make sure that your piece of code can only be
   * executed after AM builds the region states.
   * @see #isMetaLoaded()
   */
  public boolean isMetaAssigned() {
    return metaAssignEvent.isReady();
  }

  public boolean isMetaRegionInTransition() {
    return !isMetaAssigned();
  }

  /**
   * Notice that this event does not mean the AM has already finished region state rebuilding. See
   * the comment of {@link #isMetaAssigned()} for more details.
   * @see #isMetaAssigned()
   */
  public boolean waitMetaAssigned(Procedure<?> proc, RegionInfo regionInfo) {
    return getMetaAssignEvent(getMetaForRegion(regionInfo)).suspendIfNotReady(proc);
  }

  private void setMetaAssigned(RegionInfo metaRegionInfo, boolean assigned) {
    assert isMetaRegion(metaRegionInfo) : "unexpected non-meta region " + metaRegionInfo;
    ProcedureEvent<?> metaAssignEvent = getMetaAssignEvent(metaRegionInfo);
    if (assigned) {
      metaAssignEvent.wake(getProcedureScheduler());
    } else {
      metaAssignEvent.suspend();
    }
  }

  private ProcedureEvent<?> getMetaAssignEvent(RegionInfo metaRegionInfo) {
    assert isMetaRegion(metaRegionInfo) : "unexpected non-meta region " + metaRegionInfo;
    // TODO: handle multiple meta.
    return metaAssignEvent;
  }

  /**
   * Wait until AM finishes the meta loading, i.e, the region states rebuilding.
   * @see #isMetaLoaded()
   * @see #waitMetaAssigned(Procedure, RegionInfo)
   */
  public boolean waitMetaLoaded(Procedure<?> proc) {
    return metaLoadEvent.suspendIfNotReady(proc);
  }

  @VisibleForTesting
  void wakeMetaLoadedEvent() {
    metaLoadEvent.wake(getProcedureScheduler());
    assert isMetaLoaded() : "expected meta to be loaded";
  }

  /**
   * Return whether AM finishes the meta loading, i.e, the region states rebuilding.
   * @see #isMetaAssigned()
   * @see #waitMetaLoaded(Procedure)
   */
  public boolean isMetaLoaded() {
    return metaLoadEvent.isReady();
  }

  /**
   * Start a new thread to check if there are region servers whose versions are higher than others.
   * If so, move all system table regions to RS with the highest version to keep compatibility.
   * The reason is, RS in new version may not be able to access RS in old version when there are
   * some incompatible changes.
   * <p>This method is called when a new RegionServer is added to cluster only.</p>
   */
  public void checkIfShouldMoveSystemRegionAsync() {
    // TODO: Fix this thread. If a server is killed and a new one started, this thread thinks that
    // it should 'move' the system tables from the old server to the new server but
    // ServerCrashProcedure is on it; and it will take care of the assign without dataloss.
    if (this.master.getServerManager().countOfRegionServers() <= 1) {
      return;
    }
    // This thread used to run whenever there was a change in the cluster. The ZooKeeper
    // childrenChanged notification came in before the nodeDeleted message and so this method
    // cold run before a ServerCrashProcedure could run. That meant that this thread could see
    // a Crashed Server before ServerCrashProcedure and it could find system regions on the
    // crashed server and go move them before ServerCrashProcedure had a chance; could be
    // dataloss too if WALs were not recovered.
    new Thread(() -> {
      try {
        synchronized (checkIfShouldMoveSystemRegionLock) {
          List<RegionPlan> plans = new ArrayList<>();
          // TODO: I don't think this code does a good job if all servers in cluster have same
          // version. It looks like it will schedule unnecessary moves.
          for (ServerName server : getExcludedServersForSystemTable()) {
            if (master.getServerManager().isServerDead(server)) {
              // TODO: See HBASE-18494 and HBASE-18495. Though getExcludedServersForSystemTable()
              // considers only online servers, the server could be queued for dead server
              // processing. As region assignments for crashed server is handled by
              // ServerCrashProcedure, do NOT handle them here. The goal is to handle this through
              // regular flow of LoadBalancer as a favored node and not to have this special
              // handling.
              continue;
            }
            List<RegionInfo> regionsShouldMove = getSystemTables(server);
            if (!regionsShouldMove.isEmpty()) {
              for (RegionInfo regionInfo : regionsShouldMove) {
                // null value for dest forces destination server to be selected by balancer
                RegionPlan plan = new RegionPlan(regionInfo, server, null);
                if (regionInfo.isMetaRegion()) {
                  // Must move meta region first.
                  LOG.info("Async MOVE of {} to newer Server={}",
                      regionInfo.getEncodedName(), server);
                  moveAsync(plan);
                } else {
                  plans.add(plan);
                }
              }
            }
            for (RegionPlan plan : plans) {
              LOG.info("Async MOVE of {} to newer Server={}",
                  plan.getRegionInfo().getEncodedName(), server);
              moveAsync(plan);
            }
          }
        }
      } catch (Throwable t) {
        LOG.error(t.toString(), t);
      }
    }).start();
  }

  private List<RegionInfo> getSystemTables(ServerName serverName) {
    Set<RegionStateNode> regions = this.getRegionStates().getServerNode(serverName).getRegions();
    if (regions == null) {
      return new ArrayList<>();
    }
    return regions.stream()
        .map(RegionStateNode::getRegionInfo)
        .filter(r -> r.getTable().isSystemTable())
        .collect(Collectors.toList());
  }

  public void assign(final RegionInfo regionInfo, ServerName sn) throws IOException {
    AssignProcedure proc = createAssignProcedure(regionInfo, sn);
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public void assign(final RegionInfo regionInfo) throws IOException {
    AssignProcedure proc = createAssignProcedure(regionInfo);
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public void unassign(final RegionInfo regionInfo) throws IOException {
    unassign(regionInfo, false);
  }

  public void unassign(final RegionInfo regionInfo, final boolean forceNewPlan)
  throws IOException {
    // TODO: rename this reassign
    RegionStateNode node = this.regionStates.getRegionStateNode(regionInfo);
    ServerName destinationServer = node.getRegionLocation();
    if (destinationServer == null) {
      throw new UnexpectedStateException("DestinationServer is null; Assigned? " + node.toString());
    }
    assert destinationServer != null; node.toString();
    UnassignProcedure proc = createUnassignProcedure(regionInfo, destinationServer, forceNewPlan);
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public void move(final RegionInfo regionInfo) throws IOException {
    RegionStateNode node = this.regionStates.getRegionStateNode(regionInfo);
    ServerName sourceServer = node.getRegionLocation();
    RegionPlan plan = new RegionPlan(regionInfo, sourceServer, null);
    MoveRegionProcedure proc = createMoveRegionProcedure(plan);
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public Future<byte[]> moveAsync(final RegionPlan regionPlan) throws HBaseIOException {
    MoveRegionProcedure proc = createMoveRegionProcedure(regionPlan);
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  /**
   * Create round-robin assigns. Use on table creation to distribute out regions across cluster.
   * @return AssignProcedures made out of the passed in <code>hris</code> and a call to the balancer
   *         to populate the assigns with targets chosen using round-robin (default balancer
   *         scheme). If at assign-time, the target chosen is no longer up, thats fine, the
   *         AssignProcedure will ask the balancer for a new target, and so on.
   */
  public AssignProcedure[] createRoundRobinAssignProcedures(List<RegionInfo> hris) {
    return createRoundRobinAssignProcedures(hris, null);
  }

  // ============================================================================================
  //  RegionTransition procedures helpers
  // ============================================================================================

  /**
   * Create round-robin assigns. Use on table creation to distribute out regions across cluster.
   * @return AssignProcedures made out of the passed in <code>hris</code> and a call
   * to the balancer to populate the assigns with targets chosen using round-robin (default
   * balancer scheme). If at assign-time, the target chosen is no longer up, thats fine,
   * the AssignProcedure will ask the balancer for a new target, and so on.
   */
  public AssignProcedure[] createRoundRobinAssignProcedures(final List<RegionInfo> hris,
      List<ServerName> serversToExclude) {
    if (hris.isEmpty()) {
      return null;
    }
    if (serversToExclude != null
        && this.master.getServerManager().getOnlineServersList().size() == 1) {
      LOG.debug("Only one region server found and hence going ahead with the assignment");
      serversToExclude = null;
    }
    try {
      // Ask the balancer to assign our regions. Pass the regions en masse. The balancer can do
      // a better job if it has all the assignments in the one lump.
      Map<ServerName, List<RegionInfo>> assignments = getBalancer().roundRobinAssignment(hris,
          this.master.getServerManager().createDestinationServersList(serversToExclude));
      // Return mid-method!
      return createAssignProcedures(assignments, hris.size());
    } catch (HBaseIOException hioe) {
      LOG.warn("Failed roundRobinAssignment", hioe);
    }
    // If an error above, fall-through to this simpler assign. Last resort.
    return createAssignProcedures(hris);
  }

  /**
   * Create an array of AssignProcedures w/o specifying a target server.
   * If no target server, at assign time, we will try to use the former location of the region
   * if one exists. This is how we 'retain' the old location across a server restart.
   * Used by {@link ServerCrashProcedure} assigning regions on a server that has crashed (SCP is
   * also used across a cluster-restart just-in-case to ensure we do cleanup of any old WALs or
   * server processes).
   */
  public AssignProcedure[] createAssignProcedures(final List<RegionInfo> hris) {
    if (hris.isEmpty()) {
      return null;
    }
    int index = 0;
    AssignProcedure [] procedures = new AssignProcedure[hris.size()];
    for (RegionInfo hri : hris) {
      // Sort the procedures so meta and system regions are first in the returned array.
      procedures[index++] = createAssignProcedure(hri);
    }
    if (procedures.length > 1) {
      // Sort the procedures so meta and system regions are first in the returned array.
      Arrays.sort(procedures, AssignProcedure.COMPARATOR);
    }
    return procedures;
  }

  // Make this static for the method below where we use it typing the AssignProcedure array we
  // return as result.
  private static final AssignProcedure [] ASSIGN_PROCEDURE_ARRAY_TYPE = new AssignProcedure[] {};

  /**
   * @param assignments Map of assignments from which we produce an array of AssignProcedures.
   * @param size Count of assignments to make (the caller may know the total count)
   * @return Assignments made from the passed in <code>assignments</code>
   */
  private AssignProcedure[] createAssignProcedures(Map<ServerName, List<RegionInfo>> assignments,
      int size) {
    List<AssignProcedure> procedures = new ArrayList<>(size > 0? size: 8/*Arbitrary*/);
    for (Map.Entry<ServerName, List<RegionInfo>> e: assignments.entrySet()) {
      for (RegionInfo ri: e.getValue()) {
        AssignProcedure ap = createAssignProcedure(ri, e.getKey());
        ap.setOwner(getProcedureEnvironment().getRequestUser().getShortName());
        procedures.add(ap);
      }
    }
    if (procedures.size() > 1) {
      // Sort the procedures so meta and system regions are first in the returned array.
      procedures.sort(AssignProcedure.COMPARATOR);
    }
    return procedures.toArray(ASSIGN_PROCEDURE_ARRAY_TYPE);
  }

  // Needed for the following method so it can type the created Array we retur n
  private static final UnassignProcedure [] UNASSIGN_PROCEDURE_ARRAY_TYPE =
      new UnassignProcedure[0];

  UnassignProcedure[] createUnassignProcedures(final Collection<RegionStateNode> nodes) {
    if (nodes.isEmpty()) return null;
    final List<UnassignProcedure> procs = new ArrayList<UnassignProcedure>(nodes.size());
    for (RegionStateNode node: nodes) {
      if (!this.regionStates.include(node, false)) continue;
      // Look for regions that are offline/closed; i.e. already unassigned.
      if (this.regionStates.isRegionOffline(node.getRegionInfo())) continue;
      assert node.getRegionLocation() != null: node.toString();
      procs.add(createUnassignProcedure(node.getRegionInfo(), node.getRegionLocation(), false));
    }
    return procs.toArray(UNASSIGN_PROCEDURE_ARRAY_TYPE);
  }

  /**
   * Called by things like DisableTableProcedure to get a list of UnassignProcedure
   * to unassign the regions of the table.
   */
  public AssignProcedure createAssignProcedure(final RegionInfo regionInfo) {
    return createAssignProcedure(regionInfo, null, false);
  }

  public AssignProcedure createAssignProcedure(final RegionInfo regionInfo, boolean override) {
    return createAssignProcedure(regionInfo, null, override);
  }

  public AssignProcedure createAssignProcedure(final RegionInfo regionInfo,
      ServerName targetServer) {
    return createAssignProcedure(regionInfo, targetServer, false);
  }

  public AssignProcedure createAssignProcedure(final RegionInfo regionInfo,
      final ServerName targetServer, boolean override) {
    AssignProcedure proc = new AssignProcedure(regionInfo, targetServer, override);
    proc.setOwner(getProcedureEnvironment().getRequestUser().getShortName());
    return proc;
  }

  public UnassignProcedure createUnassignProcedure(final RegionInfo regionInfo) {
    return createUnassignProcedure(regionInfo, null, false);
  }

  public UnassignProcedure createUnassignProcedure(final RegionInfo regionInfo,
      boolean override) {
    return createUnassignProcedure(regionInfo, null, override);
  }

  public UnassignProcedure[] createUnassignProcedures(final TableName tableName) {
    return createUnassignProcedures(regionStates.getTableRegionStateNodes(tableName));
  }

  UnassignProcedure createUnassignProcedure(final RegionInfo regionInfo,
      final ServerName destinationServer, final boolean force) {
    return createUnassignProcedure(regionInfo, destinationServer, force, false);
  }

  UnassignProcedure createUnassignProcedure(final RegionInfo regionInfo,
      final ServerName destinationServer, final boolean force,
      final boolean removeAfterUnassigning) {
    // If destinationServer is null, figure it.
    ServerName sn = destinationServer != null? destinationServer:
        getRegionStates().getRegionState(regionInfo).getServerName();
    assert sn != null;
    UnassignProcedure proc = new UnassignProcedure(regionInfo, sn, force, removeAfterUnassigning);
    proc.setOwner(getProcedureEnvironment().getRequestUser().getShortName());
    return proc;
  }

  private MoveRegionProcedure createMoveRegionProcedure(RegionPlan plan) throws HBaseIOException {
    if (plan.getRegionInfo().getTable().isSystemTable()) {
      List<ServerName> exclude = getExcludedServersForSystemTable();
      if (plan.getDestination() != null && exclude.contains(plan.getDestination())) {
        try {
          LOG.info("Can not move " + plan.getRegionInfo() + " to " + plan.getDestination() +
            " because the server is not with highest version");
          plan.setDestination(getBalancer().randomAssignment(plan.getRegionInfo(),
            this.master.getServerManager().createDestinationServersList(exclude)));
        } catch (HBaseIOException e) {
          LOG.warn(e.toString(), e);
        }
      }
    }
    return new MoveRegionProcedure(getProcedureEnvironment(), plan, true);
  }


  public SplitTableRegionProcedure createSplitProcedure(final RegionInfo regionToSplit,
      final byte[] splitKey) throws IOException {
    return new SplitTableRegionProcedure(getProcedureEnvironment(), regionToSplit, splitKey);
  }

  public MergeTableRegionsProcedure createMergeProcedure(RegionInfo ... ris) throws IOException {
    return new MergeTableRegionsProcedure(getProcedureEnvironment(), ris, false);
  }

  /**
   * Delete the region states. This is called by "DeleteTable"
   */
  public void deleteTable(final TableName tableName) throws IOException {
    final ArrayList<RegionInfo> regions = regionStates.getTableRegionsInfo(tableName);
    regionStateStore.deleteRegions(regions);
    for (int i = 0; i < regions.size(); ++i) {
      final RegionInfo regionInfo = regions.get(i);
      // we expect the region to be offline
      regionStates.removeFromOfflineRegions(regionInfo);
      regionStates.deleteRegion(regionInfo);
    }
  }

  // ============================================================================================
  //  RS Region Transition Report helpers
  // ============================================================================================
  // TODO: Move this code in MasterRpcServices and call on specific event?
  public ReportRegionStateTransitionResponse reportRegionStateTransition(
      final ReportRegionStateTransitionRequest req)
  throws PleaseHoldException {
    final ReportRegionStateTransitionResponse.Builder builder =
        ReportRegionStateTransitionResponse.newBuilder();
    final ServerName serverName = ProtobufUtil.toServerName(req.getServer());
    try {
      for (RegionStateTransition transition: req.getTransitionList()) {
        switch (transition.getTransitionCode()) {
          case OPENED:
          case FAILED_OPEN:
          case CLOSED:
            assert transition.getRegionInfoCount() == 1 : transition;
            final RegionInfo hri = ProtobufUtil.toRegionInfo(transition.getRegionInfo(0));
            updateRegionTransition(serverName, transition.getTransitionCode(), hri,
                transition.hasOpenSeqNum() ? transition.getOpenSeqNum() : HConstants.NO_SEQNUM);
            break;
          case READY_TO_SPLIT:
          case SPLIT:
          case SPLIT_REVERTED:
            assert transition.getRegionInfoCount() == 3 : transition;
            final RegionInfo parent = ProtobufUtil.toRegionInfo(transition.getRegionInfo(0));
            final RegionInfo splitA = ProtobufUtil.toRegionInfo(transition.getRegionInfo(1));
            final RegionInfo splitB = ProtobufUtil.toRegionInfo(transition.getRegionInfo(2));
            updateRegionSplitTransition(serverName, transition.getTransitionCode(),
              parent, splitA, splitB);
            break;
          case READY_TO_MERGE:
          case MERGED:
          case MERGE_REVERTED:
            assert transition.getRegionInfoCount() == 3 : transition;
            final RegionInfo merged = ProtobufUtil.toRegionInfo(transition.getRegionInfo(0));
            final RegionInfo mergeA = ProtobufUtil.toRegionInfo(transition.getRegionInfo(1));
            final RegionInfo mergeB = ProtobufUtil.toRegionInfo(transition.getRegionInfo(2));
            updateRegionMergeTransition(serverName, transition.getTransitionCode(),
              merged, mergeA, mergeB);
            break;
        }
      }
    } catch (PleaseHoldException e) {
      if (LOG.isTraceEnabled()) LOG.trace("Failed transition " + e.getMessage());
      throw e;
    } catch (UnsupportedOperationException|IOException e) {
      // TODO: at the moment we have a single error message and the RS will abort
      // if the master says that one of the region transitions failed.
      LOG.warn("Failed transition", e);
      builder.setErrorMessage("Failed transition " + e.getMessage());
    }
    return builder.build();
  }

  /**
   * Called when the RegionServer wants to report a Procedure transition.
   * Ends up calling {@link #reportTransition(RegionStateNode, ServerName, TransitionCode, long)}
   */
  private void updateRegionTransition(final ServerName serverName, final TransitionCode state,
      final RegionInfo regionInfo, final long seqId)
      throws PleaseHoldException, UnexpectedStateException {
    checkMetaLoaded(regionInfo);

    final RegionStateNode regionNode = regionStates.getRegionStateNode(regionInfo);
    if (regionNode == null) {
      // the table/region is gone. maybe a delete, split, merge
      throw new UnexpectedStateException(String.format(
        "Server %s was trying to transition region %s to %s. but the region was removed.",
        serverName, regionInfo, state));
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Update region transition serverName=%s region=%s regionState=%s",
        serverName, regionNode, state));
    }

    if (!reportTransition(regionNode, serverName, state, seqId)) {
      // Don't log WARN if shutting down cluster; during shutdown. Avoid the below messages:
      // 2018-08-13 10:45:10,551 WARN ...AssignmentManager: No matching procedure found for
      //   rit=OPEN, location=ve0538.halxg.cloudera.com,16020,1533493000958,
      //   table=IntegrationTestBigLinkedList, region=65ab289e2fc1530df65f6c3d7cde7aa5 transition
      //   to CLOSED
      // These happen because on cluster shutdown, we currently let the RegionServers close
      // regions. This is the only time that region close is not run by the Master (so cluster
      // goes down fast). Consider changing it so Master runs all shutdowns.
      if (this.master.getServerManager().isClusterShutdown() &&
          state.equals(TransitionCode.CLOSED)) {
        LOG.info("RegionServer {} {}", state, regionNode.getRegionInfo().getEncodedName());
      } else {
        LOG.warn("No matching procedure found for {} transition to {}", regionNode, state);
      }
    }
  }

  // FYI: regionNode is sometimes synchronized by the caller but not always.
  private boolean reportTransition(final RegionStateNode regionNode, ServerName serverName,
      final TransitionCode state, final long seqId)
      throws UnexpectedStateException {
    synchronized (regionNode) {
      final RegionTransitionProcedure proc = regionNode.getProcedure();
      if (proc == null) return false;

      // serverNode.getReportEvent().removeProcedure(proc);
      proc.reportTransition(master.getMasterProcedureExecutor().getEnvironment(),
        serverName, state, seqId);
    }
    return true;
  }

  private void updateRegionSplitTransition(final ServerName serverName, final TransitionCode state,
      final RegionInfo parent, final RegionInfo hriA, final RegionInfo hriB)
      throws IOException {
    checkMetaLoaded(parent);

    if (state != TransitionCode.READY_TO_SPLIT) {
      throw new UnexpectedStateException("unsupported split regionState=" + state +
        " for parent region " + parent +
        " maybe an old RS (< 2.0) had the operation in progress");
    }

    // sanity check on the request
    if (!Bytes.equals(hriA.getEndKey(), hriB.getStartKey())) {
      throw new UnsupportedOperationException(
        "unsupported split request with bad keys: parent=" + parent +
        " hriA=" + hriA + " hriB=" + hriB);
    }

    if (!master.isSplitOrMergeEnabled(MasterSwitchType.SPLIT)) {
      LOG.warn("Split switch is off! skip split of " + parent);
      throw new DoNotRetryIOException("Split region " + parent.getRegionNameAsString() +
          " failed due to split switch off");
    }

    // Submit the Split procedure
    final byte[] splitKey = hriB.getStartKey();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Split request from " + serverName +
          ", parent=" + parent + " splitKey=" + Bytes.toStringBinary(splitKey));
    }
    master.getMasterProcedureExecutor().submitProcedure(createSplitProcedure(parent, splitKey));

    // If the RS is < 2.0 throw an exception to abort the operation, we are handling the split
    if (master.getServerManager().getVersionNumber(serverName) < 0x0200000) {
      throw new UnsupportedOperationException(String.format(
        "Split handled by the master: parent=%s hriA=%s hriB=%s", parent.getShortNameToLog(), hriA, hriB));
    }
  }

  private void updateRegionMergeTransition(final ServerName serverName, final TransitionCode state,
      final RegionInfo merged, final RegionInfo hriA, final RegionInfo hriB) throws IOException {
    checkMetaLoaded(merged);

    if (state != TransitionCode.READY_TO_MERGE) {
      throw new UnexpectedStateException("Unsupported merge regionState=" + state +
        " for regionA=" + hriA + " regionB=" + hriB + " merged=" + merged +
        " maybe an old RS (< 2.0) had the operation in progress");
    }

    if (!master.isSplitOrMergeEnabled(MasterSwitchType.MERGE)) {
      LOG.warn("Merge switch is off! skip merge of regionA=" + hriA + " regionB=" + hriB);
      throw new DoNotRetryIOException("Merge of regionA=" + hriA + " regionB=" + hriB +
        " failed because merge switch is off");
    }

    // Submit the Merge procedure
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling merge request from RS=" + merged + ", merged=" + merged);
    }
    master.getMasterProcedureExecutor().submitProcedure(createMergeProcedure(hriA, hriB));

    // If the RS is < 2.0 throw an exception to abort the operation, we are handling the merge
    if (master.getServerManager().getVersionNumber(serverName) < 0x0200000) {
      throw new UnsupportedOperationException(String.format(
        "Merge not handled yet: regionState=%s merged=%s hriA=%s hriB=%s", state, merged, hriA,
          hriB));
    }
  }

  // ============================================================================================
  //  RS Status update (report online regions) helpers
  // ============================================================================================
  /**
   * the master will call this method when the RS send the regionServerReport().
   * the report will contains the "online regions".
   * this method will check the the online regions against the in-memory state of the AM,
   * if there is a mismatch we will try to fence out the RS with the assumption
   * that something went wrong on the RS side.
   */
  public void reportOnlineRegions(final ServerName serverName, final Set<byte[]> regionNames)
      throws YouAreDeadException {
    if (!isRunning()) return;
    if (LOG.isTraceEnabled()) {
      LOG.trace("ReportOnlineRegions " + serverName + " regionCount=" + regionNames.size() +
        ", metaLoaded=" + isMetaLoaded() + " " +
          regionNames.stream().map(element -> Bytes.toStringBinary(element)).
            collect(Collectors.toList()));
    }

    // Make sure there is a ServerStateNode for this server that just checked in.
    ServerStateNode serverNode = regionStates.getOrCreateServer(serverName);
    synchronized (serverNode) {
      if (!serverNode.isInState(ServerState.ONLINE)) {
        LOG.warn("Got a report from a server result in state " + serverNode.getState());
        return;
      }
    }

    // Track the regionserver reported online regions in memory.
    synchronized (rsReports) {
      rsReports.put(serverName, regionNames);
    }

    if (regionNames.isEmpty()) {
      // nothing to do if we don't have regions
      LOG.trace("no online region found on " + serverName);
    } else if (!isMetaLoaded()) {
      // if we are still on startup, discard the report unless is from someone holding meta
      checkOnlineRegionsReportForMeta(serverName, regionNames);
    } else {
      // The Heartbeat updates us of what regions are only. check and verify the state.
      checkOnlineRegionsReport(serverNode, regionNames);
    }

    // wake report event
    wakeServerReportEvent(serverNode);
  }

  void checkOnlineRegionsReportForMeta(final ServerName serverName, final Set<byte[]> regionNames) {
    try {
      for (byte[] regionName: regionNames) {
        final RegionInfo hri = getMetaRegionFromName(regionName);
        if (hri == null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Skip online report for region={} while meta is loading from server={}",
                Bytes.toStringBinary(regionName), serverName);
          }
          continue;
        }

        final RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(hri);
        LOG.info("META REPORTED: " + regionNode);
        if (!reportTransition(regionNode, serverName, TransitionCode.OPENED, 0)) {
          LOG.warn("META REPORTED but no procedure found (complete?); set location={}", serverName);
          regionNode.setRegionLocation(serverName);
        } else if (LOG.isTraceEnabled()) {
          LOG.trace("META REPORTED: " + regionNode);
        }
      }
    } catch (UnexpectedStateException e) {
      LOG.warn("KILLING " + serverName + ": " + e.getMessage());
      killRegionServer(serverName);
    }
  }

  void checkOnlineRegionsReport(final ServerStateNode serverNode, final Set<byte[]> regionNames) {
    final ServerName serverName = serverNode.getServerName();
    try {
      for (byte[] regionName: regionNames) {
        if (!isRunning()) return;
        final RegionStateNode regionNode = regionStates.getRegionStateNodeFromName(regionName);
        if (regionNode == null) {
          throw new UnexpectedStateException("Not online: " + Bytes.toStringBinary(regionName));
        }
        synchronized (regionNode) {
          if (regionNode.isInState(State.OPENING, State.OPEN)) {
            if (!regionNode.getRegionLocation().equals(serverName)) {
              throw new UnexpectedStateException(regionNode.toString() +
                " reported OPEN on server=" + serverName +
                " but state has otherwise.");
            } else if (regionNode.isInState(State.OPENING)) {
              try {
                if (!reportTransition(regionNode, serverNode.getServerName(),
                    TransitionCode.OPENED, 0)) {
                  LOG.warn(regionNode.toString() + " reported OPEN on server=" + serverName +
                    " but state has otherwise AND NO procedure is running");
                }
              } catch (UnexpectedStateException e) {
                LOG.warn("{} reported unexpteced OPEN: {} sever={}", regionNode.toString(),
                    e.getMessage(), serverName, e);
              }
            }
          } else if (!regionNode.isInState(State.CLOSING, State.SPLITTING)) {
            long diff = regionNode.getLastUpdate() - EnvironmentEdgeManager.currentTime();
            if (diff > 1000/*One Second... make configurable if an issue*/) {
              // So, we can get report that a region is CLOSED or SPLIT because a heartbeat
              // came in at about same time as a region transition. Make sure there is some
              // elapsed time between killing remote server.
              throw new UnexpectedStateException(regionNode.toString() +
                " reported an unexpected OPEN; time since last update=" + diff);
            }
          }
        }
      }
    } catch (UnexpectedStateException e) {
      //See HBASE-21421, we can count on reportRegionStateTransition calls
      //We only log a warming here. It could be a network lag.
      LOG.warn("Failed to checkOnlineRegionsReport, maybe due to network lag, "
          + "if this message continues, be careful of double assign. report from server={}",
          serverName, e);
    }
  }

  protected boolean waitServerReportEvent(ServerName serverName, Procedure<?> proc) {
    ServerStateNode ssn = this.regionStates.getServerNode(serverName);
    if (ssn == null) {
      LOG.warn("Why is ServerStateNode for {} empty at this point? Creating...", serverName);
      ssn = this.regionStates.getOrCreateServer(serverName);
    }
    return ssn.getReportEvent().suspendIfNotReady(proc);
  }

  protected void wakeServerReportEvent(final ServerStateNode serverNode) {
    serverNode.getReportEvent().wake(getProcedureScheduler());
  }

  // ============================================================================================
  //  RIT chore
  // ============================================================================================
  private static class RegionInTransitionChore extends ProcedureInMemoryChore<MasterProcedureEnv> {
    public RegionInTransitionChore(final int timeoutMsec) {
      super(timeoutMsec);
    }

    @Override
    protected void periodicExecute(final MasterProcedureEnv env) {
      final AssignmentManager am = env.getAssignmentManager();

      final RegionInTransitionStat ritStat = am.computeRegionInTransitionStat();
      if (ritStat.hasRegionsOverThreshold()) {
        for (RegionState hri: ritStat.getRegionOverThreshold()) {
          am.handleRegionOverStuckWarningThreshold(hri.getRegion());
        }
      }

      // update metrics
      am.updateRegionsInTransitionMetrics(ritStat);
    }
  }

  public RegionInTransitionStat computeRegionInTransitionStat() {
    final RegionInTransitionStat rit = new RegionInTransitionStat(getConfiguration());
    rit.update(this);
    return rit;
  }

  public static class RegionInTransitionStat {
    private final int ritThreshold;

    private HashMap<String, RegionState> ritsOverThreshold = null;
    private long statTimestamp;
    private long oldestRITTime = 0;
    private int totalRITsTwiceThreshold = 0;
    private int totalRITs = 0;

    @VisibleForTesting
    public RegionInTransitionStat(final Configuration conf) {
      this.ritThreshold =
        conf.getInt(METRICS_RIT_STUCK_WARNING_THRESHOLD, DEFAULT_RIT_STUCK_WARNING_THRESHOLD);
    }

    public int getRITThreshold() {
      return ritThreshold;
    }

    public long getTimestamp() {
      return statTimestamp;
    }

    public int getTotalRITs() {
      return totalRITs;
    }

    public long getOldestRITTime() {
      return oldestRITTime;
    }

    public int getTotalRITsOverThreshold() {
      Map<String, RegionState> m = this.ritsOverThreshold;
      return m != null ? m.size() : 0;
    }

    public boolean hasRegionsTwiceOverThreshold() {
      return totalRITsTwiceThreshold > 0;
    }

    public boolean hasRegionsOverThreshold() {
      Map<String, RegionState> m = this.ritsOverThreshold;
      return m != null && !m.isEmpty();
    }

    public Collection<RegionState> getRegionOverThreshold() {
      Map<String, RegionState> m = this.ritsOverThreshold;
      return m != null? m.values(): Collections.emptySet();
    }

    public boolean isRegionOverThreshold(final RegionInfo regionInfo) {
      Map<String, RegionState> m = this.ritsOverThreshold;
      return m != null && m.containsKey(regionInfo.getEncodedName());
    }

    public boolean isRegionTwiceOverThreshold(final RegionInfo regionInfo) {
      Map<String, RegionState> m = this.ritsOverThreshold;
      if (m == null) return false;
      final RegionState state = m.get(regionInfo.getEncodedName());
      if (state == null) return false;
      return (statTimestamp - state.getStamp()) > (ritThreshold * 2);
    }

    protected void update(final AssignmentManager am) {
      final RegionStates regionStates = am.getRegionStates();
      this.statTimestamp = EnvironmentEdgeManager.currentTime();
      update(regionStates.getRegionsStateInTransition(), statTimestamp);
      update(regionStates.getRegionFailedOpen(), statTimestamp);
    }

    private void update(final Collection<RegionState> regions, final long currentTime) {
      for (RegionState state: regions) {
        totalRITs++;
        final long ritTime = currentTime - state.getStamp();
        if (ritTime > ritThreshold) {
          if (ritsOverThreshold == null) {
            ritsOverThreshold = new HashMap<String, RegionState>();
          }
          ritsOverThreshold.put(state.getRegion().getEncodedName(), state);
          totalRITsTwiceThreshold += (ritTime > (ritThreshold * 2)) ? 1 : 0;
        }
        if (oldestRITTime < ritTime) {
          oldestRITTime = ritTime;
        }
      }
    }
  }

  private void updateRegionsInTransitionMetrics(final RegionInTransitionStat ritStat) {
    metrics.updateRITOldestAge(ritStat.getOldestRITTime());
    metrics.updateRITCount(ritStat.getTotalRITs());
    metrics.updateRITCountOverThreshold(ritStat.getTotalRITsOverThreshold());
  }

  private void handleRegionOverStuckWarningThreshold(final RegionInfo regionInfo) {
    final RegionStateNode regionNode = regionStates.getRegionStateNode(regionInfo);
    //if (regionNode.isStuck()) {
    LOG.warn("STUCK Region-In-Transition {}", regionNode);
  }

  // ============================================================================================
  //  TODO: Master load/bootstrap
  // ============================================================================================
  public void joinCluster() throws IOException {
    long startTime = System.nanoTime();
    LOG.debug("Joining cluster...");

    // Scan hbase:meta to build list of existing regions, servers, and assignment.
    // hbase:meta is online now or will be. Inside loadMeta, we keep trying. Can't make progress
    // w/o  meta.
    loadMeta();

    while (master.getServerManager().countOfRegionServers() < 1) {
      LOG.info("Waiting for RegionServers to join; current count={}",
        master.getServerManager().countOfRegionServers());
      Threads.sleep(250);
    }
    LOG.info("Number of RegionServers={}", master.getServerManager().countOfRegionServers());

    // Start the RIT chore
    master.getMasterProcedureExecutor().addChore(this.ritChore);

    long costMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
    LOG.info("Joined the cluster in {}", StringUtils.humanTimeDiff(costMs));
  }

  /**
   * Create assign procedure for offline regions.
   * Just follow the old processofflineServersWithOnlineRegions method. Since now we do not need to
   * deal with dead server any more, we only deal with the regions in OFFLINE state in this method.
   * And this is a bit strange, that for new regions, we will add it in CLOSED state instead of
   * OFFLINE state, and usually there will be a procedure to track them. The
   * processofflineServersWithOnlineRegions is a legacy from long ago, as things are going really
   * different now, maybe we do not need this method any more. Need to revisit later.
   */
  // Public so can be run by the Master as part of the startup. Needs hbase:meta to be online.
  // Needs to be done after the table state manager has been started.
  public void processOfflineRegions() {
    List<RegionInfo> offlineRegions = regionStates.getRegionStates().stream()
      .filter(RegionState::isOffline).filter(s -> isTableEnabled(s.getRegion().getTable()))
      .map(RegionState::getRegion).collect(Collectors.toList());
    if (!offlineRegions.isEmpty()) {
      master.getMasterProcedureExecutor().submitProcedures(
        master.getAssignmentManager().createRoundRobinAssignProcedures(offlineRegions));
    }
  }

  private void loadMeta() throws IOException {
    // TODO: use a thread pool
    regionStateStore.visitMeta(new RegionStateStore.RegionStateVisitor() {
      @Override
      public void visitRegionState(Result result, final RegionInfo regionInfo, final State state,
          final ServerName regionLocation, final ServerName lastHost, final long openSeqNum) {
        if (state == null && regionLocation == null && lastHost == null &&
            openSeqNum == SequenceId.NO_SEQUENCE_ID) {
          // This is a row with nothing in it.
          LOG.warn("Skipping empty row={}", result);
          return;
        }
        State localState = state;
        if (localState == null) {
          // No region state column data in hbase:meta table! Are I doing a rolling upgrade from
          // hbase1 to hbase2? Am I restoring a SNAPSHOT or otherwise adding a region to hbase:meta?
          // In any of these cases, state is empty. For now, presume OFFLINE but there are probably
          // cases where we need to probe more to be sure this correct; TODO informed by experience.
          LOG.info(regionInfo.getEncodedName() + " regionState=null; presuming " + State.OFFLINE);

          localState = State.OFFLINE;
        }
        final RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(regionInfo);
        synchronized (regionNode) {
          if (!regionNode.isInTransition()) {
            regionNode.setState(localState);
            regionNode.setLastHost(lastHost);
            regionNode.setRegionLocation(regionLocation);
            regionNode.setOpenSeqNum(openSeqNum);
            if (localState == State.OPEN) {
              assert regionLocation != null : "found null region location for " + regionNode;
              regionStates.getOrCreateServer(regionNode.getRegionLocation());
              regionStates.addRegionToServer(regionNode);
            } else if (localState == State.OFFLINE || regionInfo.isOffline()) {
              regionStates.addToOfflineRegions(regionNode);
            } else if (localState == State.CLOSED && getTableStateManager().
                isTableState(regionNode.getTable(), TableState.State.DISABLED,
                TableState.State.DISABLING)) {
              // The region is CLOSED and the table is DISABLED/ DISABLING, there is nothing to
              // schedule; the region is inert.
            } else {
              // This is region in CLOSING or OPENING state.
              // These regions should have a procedure in replay.
              // If they don't, then they will show as STUCK after a while because of the below
              // registration and will need intervention by operator to fix. Add them to a server
              // even if the server is not around so a SCP cleans them up.
              if (regionLocation != null) {
                regionStates.getOrCreateServer(regionLocation);
              }
              regionStates.addRegionInTransition(regionNode, null);
            }
          } else {
            LOG.info("RIT {}", regionNode);
          }
        }
      }
    });

    // every assignment is blocked until meta is loaded.
    wakeMetaLoadedEvent();
  }

  /**
   * Used to check if the meta loading is done.
   * <p/>
   * if not we throw PleaseHoldException since we are rebuilding the RegionStates
   * @param hri region to check if it is already rebuild
   * @throws PleaseHoldException if meta has not been loaded yet
   */
  private void checkMetaLoaded(RegionInfo hri) throws PleaseHoldException {
    if (!isRunning()) {
      throw new PleaseHoldException("AssignmentManager not running");
    }
    boolean meta = isMetaRegion(hri);
    boolean metaLoaded = isMetaLoaded();
    if (!meta && !metaLoaded) {
      throw new PleaseHoldException(
        "Master not fully online; hbase:meta=" + meta + ", metaLoaded=" + metaLoaded);
    }
  }

  // ============================================================================================
  //  TODO: Metrics
  // ============================================================================================
  public int getNumRegionsOpened() {
    // TODO: Used by TestRegionPlacement.java and assume monotonically increasing value
    return 0;
  }

  public long submitServerCrash(final ServerName serverName, final boolean shouldSplitWal) {
    boolean carryingMeta = isCarryingMeta(serverName);

    // Remove the in-memory rsReports result
    synchronized (rsReports) {
      rsReports.remove(serverName);
    }

    ProcedureExecutor<MasterProcedureEnv> procExec = this.master.getMasterProcedureExecutor();
    long pid = procExec.submitProcedure(new ServerCrashProcedure(procExec.getEnvironment(),
        serverName, shouldSplitWal, carryingMeta));
    LOG.debug("Added=" + serverName
        + " to dead servers, submitted shutdown handler to be executed meta=" + carryingMeta);
    return pid;
  }

  public void offlineRegion(final RegionInfo regionInfo) {
    // TODO used by MasterRpcServices ServerCrashProcedure
    final RegionStateNode node = regionStates.getRegionStateNode(regionInfo);
    if (node != null) node.offline();
  }

  public void onlineRegion(final RegionInfo regionInfo, final ServerName serverName) {
    // TODO used by TestSplitTransactionOnCluster.java
  }

  public Map<ServerName, List<RegionInfo>> getSnapShotOfAssignment(
      final Collection<RegionInfo> regions) {
    return regionStates.getSnapShotOfAssignment(regions);
  }

  // ============================================================================================
  //  TODO: UTILS/HELPERS?
  // ============================================================================================
  /**
   * Used by the client (via master) to identify if all regions have the schema updates
   *
   * @param tableName
   * @return Pair indicating the status of the alter command (pending/total)
   * @throws IOException
   */
  public Pair<Integer, Integer> getReopenStatus(TableName tableName) {
    if (isTableDisabled(tableName)) return new Pair<Integer, Integer>(0, 0);

    final List<RegionState> states = regionStates.getTableRegionStates(tableName);
    int ritCount = 0;
    for (RegionState regionState: states) {
      if (!regionState.isOpened() && !regionState.isSplit()) {
        ritCount++;
      }
    }
    return new Pair<Integer, Integer>(ritCount, states.size());
  }

  // ============================================================================================
  //  TODO: Region State In Transition
  // ============================================================================================
  protected boolean addRegionInTransition(final RegionStateNode regionNode,
      final RegionTransitionProcedure procedure) {
    return regionStates.addRegionInTransition(regionNode, procedure);
  }

  protected void removeRegionInTransition(final RegionStateNode regionNode,
      final RegionTransitionProcedure procedure) {
    regionStates.removeRegionInTransition(regionNode, procedure);
  }

  public boolean hasRegionsInTransition() {
    return regionStates.hasRegionsInTransition();
  }

  public List<RegionStateNode> getRegionsInTransition() {
    return regionStates.getRegionsInTransition();
  }

  public List<RegionInfo> getAssignedRegions() {
    return regionStates.getAssignedRegions();
  }

  public RegionInfo getRegionInfo(final byte[] regionName) {
    final RegionStateNode regionState = regionStates.getRegionStateNodeFromName(regionName);
    return regionState != null ? regionState.getRegionInfo() : null;
  }

  // ============================================================================================
  //  TODO: Region Status update
  // ============================================================================================
  private void sendRegionOpenedNotification(final RegionInfo regionInfo,
      final ServerName serverName) {
    getBalancer().regionOnline(regionInfo, serverName);
    if (!this.listeners.isEmpty()) {
      for (AssignmentListener listener : this.listeners) {
        listener.regionOpened(regionInfo, serverName);
      }
    }
  }

  private void sendRegionClosedNotification(final RegionInfo regionInfo) {
    getBalancer().regionOffline(regionInfo);
    if (!this.listeners.isEmpty()) {
      for (AssignmentListener listener : this.listeners) {
        listener.regionClosed(regionInfo);
      }
    }
  }

  public void markRegionAsOpening(final RegionStateNode regionNode) throws IOException {
    synchronized (regionNode) {
      regionNode.transitionState(State.OPENING, RegionStates.STATES_EXPECTED_ON_OPEN);
      regionStates.addRegionToServer(regionNode);
      regionStateStore.updateRegionLocation(regionNode);
    }

    // update the operation count metrics
    metrics.incrementOperationCounter();
  }

  public void undoRegionAsOpening(final RegionStateNode regionNode) {
    boolean opening = false;
    synchronized (regionNode) {
      if (regionNode.isInState(State.OPENING)) {
        opening = true;
        regionStates.removeRegionFromServer(regionNode.getRegionLocation(), regionNode);
      }
      // Should we update hbase:meta?
    }
    if (opening) {
      // TODO: Metrics. Do opposite of metrics.incrementOperationCounter();
    }
  }

  public void markRegionAsOpened(final RegionStateNode regionNode) throws IOException {
    final RegionInfo hri = regionNode.getRegionInfo();
    synchronized (regionNode) {
      regionNode.transitionState(State.OPEN, RegionStates.STATES_EXPECTED_ON_OPEN);
      if (isMetaRegion(hri)) {
        // Usually we'd set a table ENABLED at this stage but hbase:meta is ALWAYs enabled, it
        // can't be disabled -- so skip the RPC (besides... enabled is managed by TableStateManager
        // which is backed by hbase:meta... Avoid setting ENABLED to avoid having to update state
        // on table that contains state.
        setMetaAssigned(hri, true);
      }
      regionStates.addRegionToServer(regionNode);
      // TODO: OPENING Updates hbase:meta too... we need to do both here and there?
      // That is a lot of hbase:meta writing.
      regionStateStore.updateRegionLocation(regionNode);
      sendRegionOpenedNotification(hri, regionNode.getRegionLocation());
    }
  }

  public void markRegionAsClosing(final RegionStateNode regionNode) throws IOException {
    final RegionInfo hri = regionNode.getRegionInfo();
    synchronized (regionNode) {
      regionNode.transitionState(State.CLOSING, RegionStates.STATES_EXPECTED_ON_CLOSE);
      // Set meta has not initialized early. so people trying to create/edit tables will wait
      if (isMetaRegion(hri)) {
        setMetaAssigned(hri, false);
      }
      regionStates.addRegionToServer(regionNode);
      regionStateStore.updateRegionLocation(regionNode);
    }

    // update the operation count metrics
    metrics.incrementOperationCounter();
  }

  public void undoRegionAsClosing(final RegionStateNode regionNode) {
    // TODO: Metrics. Do opposite of metrics.incrementOperationCounter();
    // There is nothing to undo?
  }

  public void markRegionAsClosed(final RegionStateNode regionNode) throws IOException {
    final RegionInfo hri = regionNode.getRegionInfo();
    synchronized (regionNode) {
      regionNode.transitionState(State.CLOSED, RegionStates.STATES_EXPECTED_ON_CLOSE);
      regionStates.removeRegionFromServer(regionNode.getRegionLocation(), regionNode);
      regionNode.setLastHost(regionNode.getRegionLocation());
      regionNode.setRegionLocation(null);
      regionStateStore.updateRegionLocation(regionNode);
      sendRegionClosedNotification(hri);
    }
  }

  public void markRegionAsSplit(final RegionInfo parent, final ServerName serverName,
      final RegionInfo daughterA, final RegionInfo daughterB) throws IOException {
    // Update hbase:meta. Parent will be marked offline and split up in hbase:meta.
    // The parent stays in regionStates until cleared when removed by CatalogJanitor.
    // Update its state in regionStates to it shows as offline and split when read
    // later figuring what regions are in a table and what are not: see
    // regionStates#getRegionsOfTable
    final RegionStateNode node = regionStates.getOrCreateRegionStateNode(parent);
    node.setState(State.SPLIT);
    final RegionStateNode nodeA = regionStates.getOrCreateRegionStateNode(daughterA);
    nodeA.setState(State.SPLITTING_NEW);
    final RegionStateNode nodeB = regionStates.getOrCreateRegionStateNode(daughterB);
    nodeB.setState(State.SPLITTING_NEW);

    regionStateStore.splitRegion(parent, daughterA, daughterB, serverName);
    if (shouldAssignFavoredNodes(parent)) {
      List<ServerName> onlineServers = this.master.getServerManager().getOnlineServersList();
      ((FavoredNodesPromoter)getBalancer()).
          generateFavoredNodesForDaughter(onlineServers, parent, daughterA, daughterB);
    }
  }

  /**
   * When called here, the merge has happened. The merged regions have been
   * unassigned and the above markRegionClosed has been called on each so they have been
   * disassociated from a hosting Server. The merged region will be open after this call. The
   * merged regions are removed from hbase:meta below. Later they are deleted from the filesystem
   * by the catalog janitor running against hbase:meta. It notices when the merged region no
   * longer holds references to the old regions (References are deleted after a compaction
   * rewrites what the Reference points at but not until the archiver chore runs, are the
   * References removed).
   */
  public void markRegionAsMerged(final RegionInfo child, final ServerName serverName,
        RegionInfo [] mergeParents)
      throws IOException {
    final RegionStateNode node = regionStates.getOrCreateRegionStateNode(child);
    node.setState(State.MERGED);
    for (RegionInfo ri: mergeParents) {
      regionStates.deleteRegion(ri);

    }
    regionStateStore.mergeRegions(child, mergeParents, serverName);
    if (shouldAssignFavoredNodes(child)) {
      ((FavoredNodesPromoter)getBalancer()).
        generateFavoredNodesForMergedRegion(child, mergeParents);
    }
  }

  /*
   * Favored nodes should be applied only when FavoredNodes balancer is configured and the region
   * belongs to a non-system table.
   */
  private boolean shouldAssignFavoredNodes(RegionInfo region) {
    return this.shouldAssignRegionsWithFavoredNodes &&
        FavoredNodesManager.isFavoredNodeApplicable(region);
  }

  // ============================================================================================
  //  Assign Queue (Assign/Balance)
  // ============================================================================================
  private final ArrayList<RegionStateNode> pendingAssignQueue = new ArrayList<RegionStateNode>();
  private final ReentrantLock assignQueueLock = new ReentrantLock();
  private final Condition assignQueueFullCond = assignQueueLock.newCondition();

  /**
   * Add the assign operation to the assignment queue.
   * The pending assignment operation will be processed,
   * and each region will be assigned by a server using the balancer.
   */
  protected void queueAssign(final RegionStateNode regionNode) {
    regionNode.getProcedureEvent().suspend();

    // TODO: quick-start for meta and the other sys-tables?
    assignQueueLock.lock();
    try {
      pendingAssignQueue.add(regionNode);
      if (regionNode.isSystemTable() ||
          pendingAssignQueue.size() == 1 ||
          pendingAssignQueue.size() >= assignDispatchWaitQueueMaxSize) {
        assignQueueFullCond.signal();
      }
    } finally {
      assignQueueLock.unlock();
    }
  }

  private void startAssignmentThread() {
    // Get Server Thread name. Sometimes the Server is mocked so may not implement HasThread.
    // For example, in tests.
    String name = master instanceof HasThread? ((HasThread)master).getName():
        master.getServerName().toShortString();
    assignThread = new Thread(name) {
      @Override
      public void run() {
        while (isRunning()) {
          processAssignQueue();
        }
        pendingAssignQueue.clear();
      }
    };
    assignThread.setDaemon(true);
    assignThread.start();
  }

  private void stopAssignmentThread() {
    assignQueueSignal();
    try {
      while (assignThread.isAlive()) {
        assignQueueSignal();
        assignThread.join(250);
      }
    } catch (InterruptedException e) {
      LOG.warn("join interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  private void assignQueueSignal() {
    assignQueueLock.lock();
    try {
      assignQueueFullCond.signal();
    } finally {
      assignQueueLock.unlock();
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings("WA_AWAIT_NOT_IN_LOOP")
  private HashMap<RegionInfo, RegionStateNode> waitOnAssignQueue() {
    HashMap<RegionInfo, RegionStateNode> regions = null;

    assignQueueLock.lock();
    try {
      if (pendingAssignQueue.isEmpty() && isRunning()) {
        assignQueueFullCond.await();
      }

      if (!isRunning()) return null;
      assignQueueFullCond.await(assignDispatchWaitMillis, TimeUnit.MILLISECONDS);
      regions = new HashMap<RegionInfo, RegionStateNode>(pendingAssignQueue.size());
      for (RegionStateNode regionNode: pendingAssignQueue) {
        regions.put(regionNode.getRegionInfo(), regionNode);
      }
      pendingAssignQueue.clear();
    } catch (InterruptedException e) {
      LOG.warn("got interrupted ", e);
      Thread.currentThread().interrupt();
    } finally {
      assignQueueLock.unlock();
    }
    return regions;
  }

  private void processAssignQueue() {
    final HashMap<RegionInfo, RegionStateNode> regions = waitOnAssignQueue();
    if (regions == null || regions.size() == 0 || !isRunning()) {
      return;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("PROCESS ASSIGN QUEUE regionCount=" + regions.size());
    }

    // TODO: Optimize balancer. pass a RegionPlan?
    final HashMap<RegionInfo, ServerName> retainMap = new HashMap<>();
    final List<RegionInfo> userHRIs = new ArrayList<>(regions.size());
    // Regions for system tables requiring reassignment
    final List<RegionInfo> systemHRIs = new ArrayList<>();
    for (RegionStateNode regionStateNode: regions.values()) {
      boolean sysTable = regionStateNode.isSystemTable();
      final List<RegionInfo> hris = sysTable? systemHRIs: userHRIs;
      if (regionStateNode.getRegionLocation() != null) {
        retainMap.put(regionStateNode.getRegionInfo(), regionStateNode.getRegionLocation());
      } else {
        hris.add(regionStateNode.getRegionInfo());
      }
    }

    // TODO: connect with the listener to invalidate the cache

    // TODO use events
    List<ServerName> servers = master.getServerManager().createDestinationServersList();
    for (int i = 0; servers.size() < 1; ++i) {
      // Report every fourth time around this loop; try not to flood log.
      if (i % 4 == 0) {
        LOG.warn("No servers available; cannot place " + regions.size() + " unassigned regions.");
      }

      if (!isRunning()) {
        LOG.debug("Stopped! Dropping assign of " + regions.size() + " queued regions.");
        return;
      }
      Threads.sleep(250);
      servers = master.getServerManager().createDestinationServersList();
    }

    if (!systemHRIs.isEmpty()) {
      // System table regions requiring reassignment are present, get region servers
      // not available for system table regions
      final List<ServerName> excludeServers = getExcludedServersForSystemTable();
      List<ServerName> serversForSysTables = servers.stream()
          .filter(s -> !excludeServers.contains(s)).collect(Collectors.toList());
      if (serversForSysTables.isEmpty()) {
        LOG.warn("Filtering old server versions and the excluded produced an empty set; " +
            "instead considering all candidate servers!");
      }
      LOG.debug("Processing assignQueue; systemServersCount=" + serversForSysTables.size() +
          ", allServersCount=" + servers.size());
      processAssignmentPlans(regions, null, systemHRIs,
          serversForSysTables.isEmpty()? servers: serversForSysTables);
    }

    processAssignmentPlans(regions, retainMap, userHRIs, servers);
  }

  private void processAssignmentPlans(final HashMap<RegionInfo, RegionStateNode> regions,
      final HashMap<RegionInfo, ServerName> retainMap, final List<RegionInfo> hris,
      final List<ServerName> servers) {
    boolean isTraceEnabled = LOG.isTraceEnabled();
    if (isTraceEnabled) {
      LOG.trace("Available servers count=" + servers.size() + ": " + servers);
    }

    final LoadBalancer balancer = getBalancer();
    // ask the balancer where to place regions
    if (retainMap != null && !retainMap.isEmpty()) {
      if (isTraceEnabled) {
        LOG.trace("retain assign regions=" + retainMap);
      }
      try {
        acceptPlan(regions, balancer.retainAssignment(retainMap, servers));
      } catch (HBaseIOException e) {
        LOG.warn("unable to retain assignment", e);
        addToPendingAssignment(regions, retainMap.keySet());
      }
    }

    // TODO: Do we need to split retain and round-robin?
    // the retain seems to fallback to round-robin/random if the region is not in the map.
    if (!hris.isEmpty()) {
      Collections.sort(hris, RegionInfo.COMPARATOR);
      if (isTraceEnabled) {
        LOG.trace("round robin regions=" + hris);
      }
      try {
        acceptPlan(regions, balancer.roundRobinAssignment(hris, servers));
      } catch (HBaseIOException e) {
        LOG.warn("unable to round-robin assignment", e);
        addToPendingAssignment(regions, hris);
      }
    }
  }

  private void acceptPlan(final HashMap<RegionInfo, RegionStateNode> regions,
      final Map<ServerName, List<RegionInfo>> plan) throws HBaseIOException {
    final ProcedureEvent<?>[] events = new ProcedureEvent[regions.size()];
    final long st = System.currentTimeMillis();

    if (plan == null) {
      throw new HBaseIOException("unable to compute plans for regions=" + regions.size());
    }

    if (plan.isEmpty()) return;

    List<RegionInfo> bogusRegions = plan.remove(LoadBalancer.BOGUS_SERVER_NAME);
    if (bogusRegions != null && !bogusRegions.isEmpty()) {
      addToPendingAssignment(regions, bogusRegions);
    }

    int evcount = 0;
    for (Map.Entry<ServerName, List<RegionInfo>> entry: plan.entrySet()) {
      final ServerName server = entry.getKey();
      for (RegionInfo hri: entry.getValue()) {
        final RegionStateNode regionNode = regions.get(hri);
        regionNode.setRegionLocation(server);
        events[evcount++] = regionNode.getProcedureEvent();
      }
    }
    ProcedureEvent.wakeEvents(getProcedureScheduler(), events);

    final long et = System.currentTimeMillis();
    if (LOG.isTraceEnabled()) {
      LOG.trace("ASSIGN ACCEPT " + events.length + " -> " +
          StringUtils.humanTimeDiff(et - st));
    }
  }

  private void addToPendingAssignment(final HashMap<RegionInfo, RegionStateNode> regions,
      final Collection<RegionInfo> pendingRegions) {
    assignQueueLock.lock();
    try {
      for (RegionInfo hri: pendingRegions) {
        pendingAssignQueue.add(regions.get(hri));
      }
    } finally {
      assignQueueLock.unlock();
    }
  }

  /**
   * Get a list of servers that this region cannot be assigned to.
   * For system tables, we must assign them to a server with highest version.
   */
  public List<ServerName> getExcludedServersForSystemTable() {
    // TODO: This should be a cached list kept by the ServerManager rather than calculated on each
    // move or system region assign. The RegionServerTracker keeps list of online Servers with
    // RegionServerInfo that includes Version.
    List<Pair<ServerName, String>> serverList = master.getServerManager().getOnlineServersList()
        .stream()
        .map((s)->new Pair<>(s, master.getRegionServerVersion(s)))
        .collect(Collectors.toList());
    if (serverList.isEmpty()) {
      return Collections.emptyList();
    }
    String highestVersion = Collections.max(serverList,
        (o1, o2) -> VersionInfo.compareVersion(o1.getSecond(), o2.getSecond())).getSecond();
    return serverList.stream()
        .filter((p)->!p.getSecond().equals(highestVersion))
        .map(Pair::getFirst)
        .collect(Collectors.toList());
  }

  // ============================================================================================
  //  Server Helpers
  // ============================================================================================
  @Override
  public void serverAdded(final ServerName serverName) {
  }

  @Override
  public void serverRemoved(final ServerName serverName) {
    final ServerStateNode serverNode = regionStates.getServerNode(serverName);
    if (serverNode == null) return;

    // just in case, wake procedures waiting for this server report
    wakeServerReportEvent(serverNode);
  }

  private void killRegionServer(final ServerName serverName) {
    master.getServerManager().expireServer(serverName);
  }

  /**
   * <p>
   * This is a very particular check. The {@link org.apache.hadoop.hbase.master.ServerManager} is
   * where you go to check on state of 'Servers', what Servers are online, etc.
   * </p>
   * <p>
   * Here we are checking the state of a server that is post expiration, a ServerManager function
   * that moves a server from online to dead. Here we are seeing if the server has moved beyond a
   * particular point in the recovery process such that it is safe to move on with assigns; etc.
   * </p>
   * <p>
   * For now it is only used in
   * {@link UnassignProcedure#remoteCallFailed(MasterProcedureEnv, RegionStateNode, IOException)} to
   * see whether we can safely quit without losing data.
   * </p>
   * @param meta whether to check for meta log splitting
   * @return {@code true} if the server does not exist or the log splitting is done, i.e, the server
   *         is in OFFLINE state, or for meta log, is in SPLITTING_META_DONE state. If null,
   *         presumes the ServerStateNode was cleaned up by SCP.
   * @see UnassignProcedure#remoteCallFailed(MasterProcedureEnv, RegionStateNode, IOException)
   */
  boolean isLogSplittingDone(ServerName serverName, boolean meta) {
    ServerStateNode ssn = this.regionStates.getServerNode(serverName);
    if (ssn == null) {
      return true;
    }
    ServerState[] inState =
      meta
        ? new ServerState[] { ServerState.SPLITTING_META_DONE, ServerState.SPLITTING,
          ServerState.OFFLINE }
        : new ServerState[] { ServerState.OFFLINE };
    synchronized (ssn) {
      return ssn.isInState(inState);
    }
  }

  @VisibleForTesting
  MasterServices getMaster() {
    return master;
  }

  /**
   * @return a snapshot of rsReports
   */
  public Map<ServerName, Set<byte[]>> getRSReports() {
    Map<ServerName, Set<byte[]>> rsReportsSnapshot = new HashMap<>();
    synchronized (rsReports) {
      rsReports.entrySet().forEach(e -> rsReportsSnapshot.put(e.getKey(), e.getValue()));
    }
    return rsReportsSnapshot;
  }

  /**
   * Provide regions state count for given table.
   * e.g howmany regions of give table are opened/closed/rit etc
   *
   * @param tableName TableName
   * @return region states count
   */
  public RegionStatesCount getRegionStatesCount(TableName tableName) {
    int openRegionsCount = 0;
    int closedRegionCount = 0;
    int ritCount = 0;
    int splitRegionCount = 0;
    int totalRegionCount = 0;
    if (!isTableDisabled(tableName)) {
      final List<RegionState> states = regionStates.getTableRegionStates(tableName);
      for (RegionState regionState : states) {
        if (regionState.isOpened()) {
          openRegionsCount++;
        } else if (regionState.isClosed()) {
          closedRegionCount++;
        } else if (regionState.isSplit()) {
          splitRegionCount++;
        }
      }
      totalRegionCount = states.size();
      ritCount = totalRegionCount - openRegionsCount - splitRegionCount;
    }
    return new RegionStatesCount.RegionStatesCountBuilder()
      .setOpenRegions(openRegionsCount)
      .setClosedRegions(closedRegionCount)
      .setSplitRegions(splitRegionCount)
      .setRegionsInTransition(ritCount)
      .setTotalRegions(totalRegionCount)
      .build();
  }

}
