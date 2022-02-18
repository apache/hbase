/*
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.favored.FavoredNodesPromoter;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsAssignmentManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.balancer.FavoredStochasticBalancer;
import org.apache.hadoop.hbase.master.procedure.HBCKServerCrashProcedure;
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
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class AssignmentManager {
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

  public static final String DEAD_REGION_METRIC_CHORE_INTERVAL_MSEC_CONF_KEY =
      "hbase.assignment.dead.region.metric.chore.interval.msec";
  private static final int DEFAULT_DEAD_REGION_METRIC_CHORE_INTERVAL_MSEC = 120 * 1000;

  public static final String ASSIGN_MAX_ATTEMPTS =
      "hbase.assignment.maximum.attempts";
  private static final int DEFAULT_ASSIGN_MAX_ATTEMPTS = Integer.MAX_VALUE;

  public static final String ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS =
      "hbase.assignment.retry.immediately.maximum.attempts";
  private static final int DEFAULT_ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS = 3;

  /** Region in Transition metrics threshold time */
  public static final String METRICS_RIT_STUCK_WARNING_THRESHOLD =
      "hbase.metrics.rit.stuck.warning.threshold";
  private static final int DEFAULT_RIT_STUCK_WARNING_THRESHOLD = 60 * 1000;
  public static final String UNEXPECTED_STATE_REGION = "Unexpected state for ";

  private final ProcedureEvent<?> metaAssignEvent = new ProcedureEvent<>("meta assign");
  private final ProcedureEvent<?> metaLoadEvent = new ProcedureEvent<>("meta load");

  private final MetricsAssignmentManager metrics;
  private final RegionInTransitionChore ritChore;
  private final DeadServerMetricRegionChore deadMetricChore;
  private final MasterServices master;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final RegionStates regionStates = new RegionStates();
  private final RegionStateStore regionStateStore;

  /**
   * When the operator uses this configuration option, any version between
   * the current cluster version and the value of "hbase.min.version.move.system.tables"
   * does not trigger any auto-region movement. Auto-region movement here
   * refers to auto-migration of system table regions to newer server versions.
   * It is assumed that the configured range of versions does not require special
   * handling of moving system table regions to higher versioned RegionServer.
   * This auto-migration is done by {@link #checkIfShouldMoveSystemRegionAsync()}.
   * Example: Let's assume the cluster is on version 1.4.0 and we have
   * set "hbase.min.version.move.system.tables" as "2.0.0". Now if we upgrade
   * one RegionServer on 1.4.0 cluster to 1.6.0 (< 2.0.0), then AssignmentManager will
   * not move hbase:meta, hbase:namespace and other system table regions
   * to newly brought up RegionServer 1.6.0 as part of auto-migration.
   * However, if we upgrade one RegionServer on 1.4.0 cluster to 2.2.0 (> 2.0.0),
   * then AssignmentManager will move all system table regions to newly brought
   * up RegionServer 2.2.0 as part of auto-migration done by
   * {@link #checkIfShouldMoveSystemRegionAsync()}.
   * "hbase.min.version.move.system.tables" is introduced as part of HBASE-22923.
   */
  private final String minVersionToMoveSysTables;

  private static final String MIN_VERSION_MOVE_SYS_TABLES_CONFIG =
    "hbase.min.version.move.system.tables";
  private static final String DEFAULT_MIN_VERSION_MOVE_SYS_TABLES_CONFIG = "";

  private final Map<ServerName, Set<byte[]>> rsReports = new HashMap<>();

  private final boolean shouldAssignRegionsWithFavoredNodes;
  private final int assignDispatchWaitQueueMaxSize;
  private final int assignDispatchWaitMillis;
  private final int assignMaxAttempts;
  private final int assignRetryImmediatelyMaxAttempts;

  private final Object checkIfShouldMoveSystemRegionLock = new Object();

  private Thread assignThread;

  public AssignmentManager(final MasterServices master) {
    this(master, new RegionStateStore(master));
  }

  AssignmentManager(final MasterServices master, final RegionStateStore stateStore) {
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
    this.assignRetryImmediatelyMaxAttempts = conf.getInt(ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS,
        DEFAULT_ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS);

    int ritChoreInterval = conf.getInt(RIT_CHORE_INTERVAL_MSEC_CONF_KEY,
        DEFAULT_RIT_CHORE_INTERVAL_MSEC);
    this.ritChore = new RegionInTransitionChore(ritChoreInterval);

    int deadRegionChoreInterval = conf.getInt(DEAD_REGION_METRIC_CHORE_INTERVAL_MSEC_CONF_KEY,
        DEFAULT_DEAD_REGION_METRIC_CHORE_INTERVAL_MSEC);
    if (deadRegionChoreInterval > 0) {
      this.deadMetricChore = new DeadServerMetricRegionChore(deadRegionChoreInterval);
    } else {
      this.deadMetricChore = null;
    }
    minVersionToMoveSysTables = conf.get(MIN_VERSION_MOVE_SYS_TABLES_CONFIG,
      DEFAULT_MIN_VERSION_MOVE_SYS_TABLES_CONFIG);
  }

  public void start() throws IOException, KeeperException {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    LOG.trace("Starting assignment manager");

    // Start the Assignment Thread
    startAssignmentThread();

    // load meta region state
    ZKWatcher zkw = master.getZooKeeper();
    // it could be null in some tests
    if (zkw == null) {
      return;
    }
    List<String> metaZNodes = zkw.getMetaReplicaNodes();
    LOG.debug("hbase:meta replica znodes: {}", metaZNodes);
    for (String metaZNode : metaZNodes) {
      int replicaId = zkw.getZNodePaths().getMetaReplicaIdFromZNode(metaZNode);
      // here we are still in the early steps of active master startup. There is only one thread(us)
      // can access AssignmentManager and create region node, so here we do not need to lock the
      // region node.
      RegionState regionState = MetaTableLocator.getMetaRegionState(zkw, replicaId);
      RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(regionState.getRegion());
      regionNode.setRegionLocation(regionState.getServerName());
      regionNode.setState(regionState.getState());
      if (regionNode.getProcedure() != null) {
        regionNode.getProcedure().stateLoaded(this, regionNode);
      }
      if (regionState.getServerName() != null) {
        regionStates.addRegionToServer(regionNode);
      }
      if (RegionReplicaUtil.isDefaultReplica(replicaId)) {
        setMetaAssigned(regionState.getRegion(), regionState.getState() == State.OPEN);
      }
      LOG.debug("Loaded hbase:meta {}", regionNode);
    }
  }

  /**
   * Create RegionStateNode based on the TRSP list, and attach the TRSP to the RegionStateNode.
   * <p>
   * This is used to restore the RIT region list, so we do not need to restore it in the loadingMeta
   * method below. And it is also very important as now before submitting a TRSP, we need to attach
   * it to the RegionStateNode, which acts like a guard, so we need to restore this information at
   * the very beginning, before we start processing any procedures.
   */
  public void setupRIT(List<TransitRegionStateProcedure> procs) {
    procs.forEach(proc -> {
      RegionInfo regionInfo = proc.getRegion();
      RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(regionInfo);
      TransitRegionStateProcedure existingProc = regionNode.getProcedure();
      if (existingProc != null) {
        // This is possible, as we will detach the procedure from the RSN before we
        // actually finish the procedure. This is because that, we will detach the TRSP from the RSN
        // during execution, at that time, the procedure has not been marked as done in the pv2
        // framework yet, so it is possible that we schedule a new TRSP immediately and when
        // arriving here, we will find out that there are multiple TRSPs for the region. But we can
        // make sure that, only the last one can take the charge, the previous ones should have all
        // been finished already. So here we will compare the proc id, the greater one will win.
        if (existingProc.getProcId() < proc.getProcId()) {
          // the new one wins, unset and set it to the new one below
          regionNode.unsetProcedure(existingProc);
        } else {
          // the old one wins, skip
          return;
        }
      }
      LOG.info("Attach {} to {} to restore RIT", proc, regionNode);
      regionNode.setProcedure(proc);
    });
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
      if (this.deadMetricChore != null) {
        master.getMasterProcedureExecutor().removeChore(this.deadMetricChore);
      }
    }

    // Stop the Assignment Thread
    stopAssignmentThread();

    // Stop the RegionStateStore
    regionStates.clear();

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

  int getAssignRetryImmediatelyMaxAttempts() {
    return assignRetryImmediatelyMaxAttempts;
  }

  public RegionStates getRegionStates() {
    return regionStates;
  }

  /**
   * Returns the regions hosted by the specified server.
   * <p/>
   * Notice that, for SCP, after we submit the SCP, no one can change the region list for the
   * ServerStateNode so we do not need any locks here. And for other usage, this can only give you a
   * snapshot of the current region list for this server, which means, right after you get the
   * region list, new regions may be moved to this server or some regions may be moved out from this
   * server, so you should not use it critically if you need strong consistency.
   */
  public List<RegionInfo> getRegionsOnServer(ServerName serverName) {
    ServerStateNode serverInfo = regionStates.getServerNode(serverName);
    if (serverInfo == null) {
      return Collections.emptyList();
    }
    return serverInfo.getRegionInfoList();
  }

  public RegionStateStore getRegionStateStore() {
    return regionStateStore;
  }

  public List<ServerName> getFavoredNodes(final RegionInfo regionInfo) {
    return this.shouldAssignRegionsWithFavoredNodes
      ? ((FavoredStochasticBalancer) getBalancer()).getFavoredNodes(regionInfo)
      : ServerName.EMPTY_SERVER_LIST;
  }

  // ============================================================================================
  //  Table State Manager helpers
  // ============================================================================================
  private TableStateManager getTableStateManager() {
    return master.getTableStateManager();
  }

  private boolean isTableEnabled(final TableName tableName) {
    return getTableStateManager().isTableState(tableName, TableState.State.ENABLED);
  }

  private boolean isTableDisabled(final TableName tableName) {
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

  /**
   * This method will be called in master initialization method after calling
   * {@link #processOfflineRegions()}, as in processOfflineRegions we will generate assign
   * procedures for offline regions, which may be conflict with creating table.
   * <p/>
   * This is a bit dirty, should be reconsidered after we decide whether to keep the
   * {@link #processOfflineRegions()} method.
   */
  public void wakeMetaLoadedEvent() {
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
    ServerStateNode serverNode = regionStates.getServerNode(serverName);
    if (serverNode == null) {
      return Collections.emptyList();
    }
    return serverNode.getSystemRegionInfoList();
  }

  private void preTransitCheck(RegionStateNode regionNode, RegionState.State[] expectedStates)
      throws HBaseIOException {
    if (regionNode.getProcedure() != null) {
      throw new HBaseIOException(regionNode + " is currently in transition; pid=" +
        regionNode.getProcedure().getProcId());
    }
    if (!regionNode.isInState(expectedStates)) {
      throw new DoNotRetryRegionException(UNEXPECTED_STATE_REGION + regionNode);
    }
    if (isTableDisabled(regionNode.getTable())) {
      throw new DoNotRetryIOException(regionNode.getTable() + " is disabled for " + regionNode);
    }
  }

  /**
   * Create an assign TransitRegionStateProcedure. Makes sure of RegionState.
   * Throws exception if not appropriate UNLESS override is set. Used by hbck2 but also by
   * straightline {@link #assign(RegionInfo, ServerName)} and
   * {@link #assignAsync(RegionInfo, ServerName)}.
   * @see #createAssignProcedure(RegionStateNode, ServerName) for a version that does NO checking
   *   used when only when no checking needed.
   * @param override If false, check RegionState is appropriate for assign; if not throw exception.
   */
  private TransitRegionStateProcedure createAssignProcedure(RegionInfo regionInfo, ServerName sn,
      boolean override) throws IOException {
    RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(regionInfo);
    regionNode.lock();
    try {
      if (override) {
        if (regionNode.getProcedure() != null) {
          regionNode.unsetProcedure(regionNode.getProcedure());
        }
      } else {
        preTransitCheck(regionNode, STATES_EXPECTED_ON_ASSIGN);
      }
      assert regionNode.getProcedure() == null;
      return regionNode.setProcedure(TransitRegionStateProcedure.assign(getProcedureEnvironment(),
        regionInfo, sn));
    } finally {
      regionNode.unlock();
    }
  }

  /**
   * Create an assign TransitRegionStateProcedure. Does NO checking of RegionState.
   * Presumes appriopriate state ripe for assign.
   * @see #createAssignProcedure(RegionInfo, ServerName, boolean)
   */
  private TransitRegionStateProcedure createAssignProcedure(RegionStateNode regionNode,
      ServerName targetServer) {
    regionNode.lock();
    try {
      return regionNode.setProcedure(TransitRegionStateProcedure.assign(getProcedureEnvironment(),
        regionNode.getRegionInfo(), targetServer));
    } finally {
      regionNode.unlock();
    }
  }

  public long assign(RegionInfo regionInfo, ServerName sn) throws IOException {
    TransitRegionStateProcedure proc = createAssignProcedure(regionInfo, sn, false);
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
    return proc.getProcId();
  }

  public long assign(RegionInfo regionInfo) throws IOException {
    return assign(regionInfo, null);
  }

  /**
   * Submits a procedure that assigns a region to a target server without waiting for it to finish
   * @param regionInfo the region we would like to assign
   * @param sn target server name
   */
  public Future<byte[]> assignAsync(RegionInfo regionInfo, ServerName sn) throws IOException {
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(),
      createAssignProcedure(regionInfo, sn, false));
  }

  /**
   * Submits a procedure that assigns a region without waiting for it to finish
   * @param regionInfo the region we would like to assign
   */
  public Future<byte[]> assignAsync(RegionInfo regionInfo) throws IOException {
    return assignAsync(regionInfo, null);
  }

  public long unassign(RegionInfo regionInfo) throws IOException {
    RegionStateNode regionNode = regionStates.getRegionStateNode(regionInfo);
    if (regionNode == null) {
      throw new UnknownRegionException("No RegionState found for " + regionInfo.getEncodedName());
    }
    TransitRegionStateProcedure proc;
    regionNode.lock();
    try {
      preTransitCheck(regionNode, STATES_EXPECTED_ON_UNASSIGN_OR_MOVE);
      proc = TransitRegionStateProcedure.unassign(getProcedureEnvironment(), regionInfo);
      regionNode.setProcedure(proc);
    } finally {
      regionNode.unlock();
    }
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
    return proc.getProcId();
  }

  public TransitRegionStateProcedure createMoveRegionProcedure(RegionInfo regionInfo,
      ServerName targetServer) throws HBaseIOException {
    RegionStateNode regionNode = this.regionStates.getRegionStateNode(regionInfo);
    if (regionNode == null) {
      throw new UnknownRegionException("No RegionStateNode found for " +
          regionInfo.getEncodedName() + "(Closed/Deleted?)");
    }
    TransitRegionStateProcedure proc;
    regionNode.lock();
    try {
      preTransitCheck(regionNode, STATES_EXPECTED_ON_UNASSIGN_OR_MOVE);
      regionNode.checkOnline();
      proc = TransitRegionStateProcedure.move(getProcedureEnvironment(), regionInfo, targetServer);
      regionNode.setProcedure(proc);
    } finally {
      regionNode.unlock();
    }
    return proc;
  }

  public void move(RegionInfo regionInfo) throws IOException {
    TransitRegionStateProcedure proc = createMoveRegionProcedure(regionInfo, null);
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public Future<byte[]> moveAsync(RegionPlan regionPlan) throws HBaseIOException {
    TransitRegionStateProcedure proc =
      createMoveRegionProcedure(regionPlan.getRegionInfo(), regionPlan.getDestination());
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public Future<byte[]> balance(RegionPlan regionPlan) throws HBaseIOException {
    ServerName current =
      this.getRegionStates().getRegionAssignments().get(regionPlan.getRegionInfo());
    if (current == null || !current.equals(regionPlan.getSource())) {
      LOG.debug("Skip region plan {}, source server not match, current region location is {}",
        regionPlan, current == null ? "(null)" : current);
      return null;
    }
    return moveAsync(regionPlan);
  }

  // ============================================================================================
  //  RegionTransition procedures helpers
  // ============================================================================================

  /**
   * Create round-robin assigns. Use on table creation to distribute out regions across cluster.
   * @return AssignProcedures made out of the passed in <code>hris</code> and a call to the balancer
   *         to populate the assigns with targets chosen using round-robin (default balancer
   *         scheme). If at assign-time, the target chosen is no longer up, thats fine, the
   *         AssignProcedure will ask the balancer for a new target, and so on.
   */
  public TransitRegionStateProcedure[] createRoundRobinAssignProcedures(List<RegionInfo> hris,
      List<ServerName> serversToExclude) {
    if (hris.isEmpty()) {
      return new TransitRegionStateProcedure[0];
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
      return createAssignProcedures(assignments);
    } catch (HBaseIOException hioe) {
      LOG.warn("Failed roundRobinAssignment", hioe);
    }
    // If an error above, fall-through to this simpler assign. Last resort.
    return createAssignProcedures(hris);
  }

  /**
   * Create round-robin assigns. Use on table creation to distribute out regions across cluster.
   * @return AssignProcedures made out of the passed in <code>hris</code> and a call to the balancer
   *         to populate the assigns with targets chosen using round-robin (default balancer
   *         scheme). If at assign-time, the target chosen is no longer up, thats fine, the
   *         AssignProcedure will ask the balancer for a new target, and so on.
   */
  public TransitRegionStateProcedure[] createRoundRobinAssignProcedures(List<RegionInfo> hris) {
    return createRoundRobinAssignProcedures(hris, null);
  }

  static int compare(TransitRegionStateProcedure left, TransitRegionStateProcedure right) {
    if (left.getRegion().isMetaRegion()) {
      if (right.getRegion().isMetaRegion()) {
        return RegionInfo.COMPARATOR.compare(left.getRegion(), right.getRegion());
      }
      return -1;
    } else if (right.getRegion().isMetaRegion()) {
      return +1;
    }
    if (left.getRegion().getTable().isSystemTable()) {
      if (right.getRegion().getTable().isSystemTable()) {
        return RegionInfo.COMPARATOR.compare(left.getRegion(), right.getRegion());
      }
      return -1;
    } else if (right.getRegion().getTable().isSystemTable()) {
      return +1;
    }
    return RegionInfo.COMPARATOR.compare(left.getRegion(), right.getRegion());
  }

  /**
   * Create one TransitRegionStateProcedure to assign a region w/o specifying a target server.
   * This method is called from HBCK2.
   * @return an assign or null
   */
  public TransitRegionStateProcedure createOneAssignProcedure(RegionInfo ri, boolean override) {
    TransitRegionStateProcedure trsp = null;
    try {
      trsp = createAssignProcedure(ri, null, override);
    } catch (IOException ioe) {
      LOG.info("Failed {} assign, override={}" +
        (override? "": "; set override to by-pass state checks."),
        ri.getEncodedName(), override, ioe);
    }
    return trsp;
  }

  /**
   * Create one TransitRegionStateProcedure to unassign a region.
   * This method is called from HBCK2.
   * @return an unassign or null
   */
  public TransitRegionStateProcedure createOneUnassignProcedure(RegionInfo ri, boolean override) {
    RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(ri);
    TransitRegionStateProcedure trsp = null;
    regionNode.lock();
    try {
      if (override) {
        if (regionNode.getProcedure() != null) {
          regionNode.unsetProcedure(regionNode.getProcedure());
        }
      } else {
        // This is where we could throw an exception; i.e. override is false.
        preTransitCheck(regionNode, STATES_EXPECTED_ON_UNASSIGN_OR_MOVE);
      }
      assert regionNode.getProcedure() == null;
      trsp = TransitRegionStateProcedure.unassign(getProcedureEnvironment(),
        regionNode.getRegionInfo());
      regionNode.setProcedure(trsp);
    } catch (IOException ioe) {
      // 'override' must be false here.
      LOG.info("Failed {} unassign, override=false; set override to by-pass state checks.",
        ri.getEncodedName(), ioe);
    } finally{
      regionNode.unlock();
    }
    return trsp;
  }

  /**
   * Create an array of TransitRegionStateProcedure w/o specifying a target server.
   * Used as fallback of caller is unable to do {@link #createAssignProcedures(Map)}.
   * <p/>
   * If no target server, at assign time, we will try to use the former location of the region if
   * one exists. This is how we 'retain' the old location across a server restart.
   * <p/>
   * Should only be called when you can make sure that no one can touch these regions other than
   * you. For example, when you are creating or enabling table. Presumes all Regions are in
   * appropriate state ripe for assign; no checking of Region state is done in here.
   * @see #createAssignProcedures(Map)
   */
  public TransitRegionStateProcedure[] createAssignProcedures(List<RegionInfo> hris) {
    return hris.stream().map(hri -> regionStates.getOrCreateRegionStateNode(hri))
        .map(regionNode -> createAssignProcedure(regionNode, null))
        .sorted(AssignmentManager::compare).toArray(TransitRegionStateProcedure[]::new);
  }

  /**
   * Tied to {@link #createAssignProcedures(List)} in that it is called if caller is unable to run
   * this method. Presumes all Regions are in appropriate state ripe for assign; no checking
   * of Region state is done in here.
   * @param assignments Map of assignments from which we produce an array of AssignProcedures.
   * @return Assignments made from the passed in <code>assignments</code>
   * @see #createAssignProcedures(List)
   */
  private TransitRegionStateProcedure[] createAssignProcedures(
      Map<ServerName, List<RegionInfo>> assignments) {
    return assignments.entrySet().stream()
      .flatMap(e -> e.getValue().stream().map(hri -> regionStates.getOrCreateRegionStateNode(hri))
        .map(regionNode -> createAssignProcedure(regionNode, e.getKey())))
      .sorted(AssignmentManager::compare).toArray(TransitRegionStateProcedure[]::new);
  }

  // for creating unassign TRSP when disabling a table or closing excess region replicas
  private TransitRegionStateProcedure forceCreateUnssignProcedure(RegionStateNode regionNode) {
    regionNode.lock();
    try {
      if (regionNode.isInState(State.OFFLINE, State.CLOSED, State.SPLIT)) {
        return null;
      }
      // in general, a split parent should be in CLOSED or SPLIT state, but anyway, let's check it
      // here for safety
      if (regionNode.getRegionInfo().isSplit()) {
        LOG.warn("{} is a split parent but not in CLOSED or SPLIT state", regionNode);
        return null;
      }
      // As in DisableTableProcedure or ModifyTableProcedure, we will hold the xlock for table, so
      // we can make sure that this procedure has not been executed yet, as TRSP will hold the
      // shared lock for table all the time. So here we will unset it and when it is actually
      // executed, it will find that the attach procedure is not itself and quit immediately.
      if (regionNode.getProcedure() != null) {
        regionNode.unsetProcedure(regionNode.getProcedure());
      }
      return regionNode.setProcedure(TransitRegionStateProcedure.unassign(getProcedureEnvironment(),
        regionNode.getRegionInfo()));
    } finally {
      regionNode.unlock();
    }
  }

  /**
   * Called by DisableTableProcedure to unassign all the regions for a table.
   */
  public TransitRegionStateProcedure[] createUnassignProceduresForDisabling(TableName tableName) {
    return regionStates.getTableRegionStateNodes(tableName).stream()
      .map(this::forceCreateUnssignProcedure).filter(p -> p != null)
      .toArray(TransitRegionStateProcedure[]::new);
  }

  /**
   * Called by ModifyTableProcedures to unassign all the excess region replicas
   * for a table.
   */
  public TransitRegionStateProcedure[] createUnassignProceduresForClosingExcessRegionReplicas(
    TableName tableName, int newReplicaCount) {
    return regionStates.getTableRegionStateNodes(tableName).stream()
      .filter(regionNode -> regionNode.getRegionInfo().getReplicaId() >= newReplicaCount)
      .map(this::forceCreateUnssignProcedure).filter(p -> p != null)
      .toArray(TransitRegionStateProcedure[]::new);
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
  private void reportRegionStateTransition(ReportRegionStateTransitionResponse.Builder builder,
      ServerName serverName, List<RegionStateTransition> transitionList) throws IOException {
    for (RegionStateTransition transition : transitionList) {
      switch (transition.getTransitionCode()) {
        case OPENED:
        case FAILED_OPEN:
        case CLOSED:
          assert transition.getRegionInfoCount() == 1 : transition;
          final RegionInfo hri = ProtobufUtil.toRegionInfo(transition.getRegionInfo(0));
          long procId =
            transition.getProcIdCount() > 0 ? transition.getProcId(0) : Procedure.NO_PROC_ID;
          updateRegionTransition(serverName, transition.getTransitionCode(), hri,
            transition.hasOpenSeqNum() ? transition.getOpenSeqNum() : HConstants.NO_SEQNUM, procId);
          break;
        case READY_TO_SPLIT:
        case SPLIT:
        case SPLIT_REVERTED:
          assert transition.getRegionInfoCount() == 3 : transition;
          final RegionInfo parent = ProtobufUtil.toRegionInfo(transition.getRegionInfo(0));
          final RegionInfo splitA = ProtobufUtil.toRegionInfo(transition.getRegionInfo(1));
          final RegionInfo splitB = ProtobufUtil.toRegionInfo(transition.getRegionInfo(2));
          updateRegionSplitTransition(serverName, transition.getTransitionCode(), parent, splitA,
            splitB);
          break;
        case READY_TO_MERGE:
        case MERGED:
        case MERGE_REVERTED:
          assert transition.getRegionInfoCount() == 3 : transition;
          final RegionInfo merged = ProtobufUtil.toRegionInfo(transition.getRegionInfo(0));
          final RegionInfo mergeA = ProtobufUtil.toRegionInfo(transition.getRegionInfo(1));
          final RegionInfo mergeB = ProtobufUtil.toRegionInfo(transition.getRegionInfo(2));
          updateRegionMergeTransition(serverName, transition.getTransitionCode(), merged, mergeA,
            mergeB);
          break;
      }
    }
  }

  public ReportRegionStateTransitionResponse reportRegionStateTransition(
      final ReportRegionStateTransitionRequest req) throws PleaseHoldException {
    ReportRegionStateTransitionResponse.Builder builder =
        ReportRegionStateTransitionResponse.newBuilder();
    ServerName serverName = ProtobufUtil.toServerName(req.getServer());
    ServerStateNode serverNode = regionStates.getOrCreateServer(serverName);
    // here we have to acquire a read lock instead of a simple exclusive lock. This is because that
    // we should not block other reportRegionStateTransition call from the same region server. This
    // is not only about performance, but also to prevent dead lock. Think of the meta region is
    // also on the same region server and you hold the lock which blocks the
    // reportRegionStateTransition for meta, and since meta is not online, you will block inside the
    // lock protection to wait for meta online...
    serverNode.readLock().lock();
    try {
      // we only accept reportRegionStateTransition if the region server is online, see the comment
      // above in submitServerCrash method and HBASE-21508 for more details.
      if (serverNode.isInState(ServerState.ONLINE)) {
        try {
          reportRegionStateTransition(builder, serverName, req.getTransitionList());
        } catch (PleaseHoldException e) {
          LOG.trace("Failed transition ", e);
          throw e;
        } catch (UnsupportedOperationException | IOException e) {
          // TODO: at the moment we have a single error message and the RS will abort
          // if the master says that one of the region transitions failed.
          LOG.warn("Failed transition", e);
          builder.setErrorMessage("Failed transition " + e.getMessage());
        }
      } else {
        LOG.warn("The region server {} is already dead, skip reportRegionStateTransition call",
          serverName);
        builder.setErrorMessage("You are dead");
      }
    } finally {
      serverNode.readLock().unlock();
    }

    return builder.build();
  }

  private void updateRegionTransition(ServerName serverName, TransitionCode state,
      RegionInfo regionInfo, long seqId, long procId) throws IOException {
    checkMetaLoaded(regionInfo);

    RegionStateNode regionNode = regionStates.getRegionStateNode(regionInfo);
    if (regionNode == null) {
      // the table/region is gone. maybe a delete, split, merge
      throw new UnexpectedStateException(String.format(
        "Server %s was trying to transition region %s to %s. but Region is not known.",
        serverName, regionInfo, state));
    }
    LOG.trace("Update region transition serverName={} region={} regionState={}", serverName,
      regionNode, state);

    ServerStateNode serverNode = regionStates.getOrCreateServer(serverName);
    regionNode.lock();
    try {
      if (!reportTransition(regionNode, serverNode, state, seqId, procId)) {
        // Don't log WARN if shutting down cluster; during shutdown. Avoid the below messages:
        // 2018-08-13 10:45:10,551 WARN ...AssignmentManager: No matching procedure found for
        // rit=OPEN, location=ve0538.halxg.cloudera.com,16020,1533493000958,
        // table=IntegrationTestBigLinkedList, region=65ab289e2fc1530df65f6c3d7cde7aa5 transition
        // to CLOSED
        // These happen because on cluster shutdown, we currently let the RegionServers close
        // regions. This is the only time that region close is not run by the Master (so cluster
        // goes down fast). Consider changing it so Master runs all shutdowns.
        if (this.master.getServerManager().isClusterShutdown() &&
          state.equals(TransitionCode.CLOSED)) {
          LOG.info("RegionServer {} {}", state, regionNode.getRegionInfo().getEncodedName());
        } else {
          LOG.warn("No matching procedure found for {} transition on {} to {}",
              serverName, regionNode, state);
        }
      }
    } finally {
      regionNode.unlock();
    }
  }

  private boolean reportTransition(RegionStateNode regionNode, ServerStateNode serverNode,
      TransitionCode state, long seqId, long procId) throws IOException {
    ServerName serverName = serverNode.getServerName();
    TransitRegionStateProcedure proc = regionNode.getProcedure();
    if (proc == null) {
      return false;
    }
    proc.reportTransition(master.getMasterProcedureExecutor().getEnvironment(), regionNode,
      serverName, state, seqId, procId);
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
    // Processing this report happens asynchronously from other activities which can mutate
    // the region state. For example, a split procedure may already be running for this parent.
    // A split procedure cannot succeed if the parent region is no longer open, so we can
    // ignore it in that case.
    // Note that submitting more than one split procedure for a given region is
    // harmless -- the split is fenced in the procedure handling -- but it would be noisy in
    // the logs. Only one procedure can succeed. The other procedure(s) would abort during
    // initialization and report failure with WARN level logging.
    RegionState parentState = regionStates.getRegionState(parent);
    if (parentState != null && parentState.isOpened()) {
      master.getMasterProcedureExecutor().submitProcedure(createSplitProcedure(parent,
        splitKey));
    } else {
      LOG.info("Ignoring split request from " + serverName +
        ", parent=" + parent + " because parent is unknown or not open");
      return;
    }

    // If the RS is < 2.0 throw an exception to abort the operation, we are handling the split
    if (master.getServerManager().getVersionNumber(serverName) < 0x0200000) {
      throw new UnsupportedOperationException(String.format("Split handled by the master: " +
        "parent=%s hriA=%s hriB=%s", parent.getShortNameToLog(), hriA, hriB));
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
   * The master will call this method when the RS send the regionServerReport(). The report will
   * contains the "online regions". This method will check the the online regions against the
   * in-memory state of the AM, and we will log a warn message if there is a mismatch. This is
   * because that there is no fencing between the reportRegionStateTransition method and
   * regionServerReport method, so there could be race and introduce inconsistency here, but
   * actually there is no problem.
   * <p/>
   * Please see HBASE-21421 and HBASE-21463 for more details.
   */
  public void reportOnlineRegions(ServerName serverName, Set<byte[]> regionNames) {
    if (!isRunning()) {
      return;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("ReportOnlineRegions {} regionCount={}, metaLoaded={} {}", serverName,
        regionNames.size(), isMetaLoaded(),
        regionNames.stream().map(Bytes::toStringBinary).collect(Collectors.toList()));
    }

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
      LOG.trace("no online region found on {}", serverName);
      return;
    }
    if (!isMetaLoaded()) {
      // we are still on startup, skip checking
      return;
    }
    // The Heartbeat tells us of what regions are on the region serve, check the state.
    checkOnlineRegionsReport(serverNode, regionNames);
  }

  /**
   * Close <code>regionName</code> on <code>sn</code> silently and immediately without
   * using a Procedure or going via hbase:meta. For case where a RegionServer's hosting
   * of a Region is not aligned w/ the Master's accounting of Region state. This is for
   * cleaning up an error in accounting.
   */
  private void closeRegionSilently(ServerName sn, byte [] regionName) {
    try {
      RegionInfo ri = MetaTableAccessor.parseRegionInfoFromRegionName(regionName);
      // Pass -1 for timeout. Means do not wait.
      ServerManager.closeRegionSilentlyAndWait(this.master.getClusterConnection(), sn, ri, -1);
    } catch (Exception e) {
      LOG.error("Failed trying to close {} on {}", Bytes.toStringBinary(regionName), sn, e);
    }
  }

  /**
   * Check that what the RegionServer reports aligns with the Master's image.
   * If disagreement, we will tell the RegionServer to expediently close
   * a Region we do not think it should have.
   */
  private void checkOnlineRegionsReport(ServerStateNode serverNode, Set<byte[]> regionNames) {
    ServerName serverName = serverNode.getServerName();
    for (byte[] regionName : regionNames) {
      if (!isRunning()) {
        return;
      }
      RegionStateNode regionNode = regionStates.getRegionStateNodeFromName(regionName);
      if (regionNode == null) {
        String regionNameAsStr = Bytes.toStringBinary(regionName);
        LOG.warn("No RegionStateNode for {} but reported as up on {}; closing...",
            regionNameAsStr, serverName);
        closeRegionSilently(serverNode.getServerName(), regionName);
        continue;
      }
      final long lag = 1000;
      regionNode.lock();
      try {
        long diff = EnvironmentEdgeManager.currentTime() - regionNode.getLastUpdate();
        if (regionNode.isInState(State.OPENING, State.OPEN)) {
          // This is possible as a region server has just closed a region but the region server
          // report is generated before the closing, but arrive after the closing. Make sure there
          // is some elapsed time so less false alarms.
          if (!regionNode.getRegionLocation().equals(serverName) && diff > lag) {
            LOG.warn("Reporting {} server does not match {} (time since last " +
                    "update={}ms); closing...",
              serverName, regionNode, diff);
            closeRegionSilently(serverNode.getServerName(), regionName);
          }
        } else if (!regionNode.isInState(State.CLOSING, State.SPLITTING)) {
          // So, we can get report that a region is CLOSED or SPLIT because a heartbeat
          // came in at about same time as a region transition. Make sure there is some
          // elapsed time so less false alarms.
          if (diff > lag) {
            LOG.warn("Reporting {} state does not match {} (time since last update={}ms)",
              serverName, regionNode, diff);
          }
        }
      } finally {
        regionNode.unlock();
      }
    }
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

  private static class DeadServerMetricRegionChore
      extends ProcedureInMemoryChore<MasterProcedureEnv> {
    public DeadServerMetricRegionChore(final int timeoutMsec) {
      super(timeoutMsec);
    }

    @Override
    protected void periodicExecute(final MasterProcedureEnv env) {
      final ServerManager sm = env.getMasterServices().getServerManager();
      final AssignmentManager am = env.getAssignmentManager();
      // To minimize inconsistencies we are not going to snapshot live servers in advance in case
      // new servers are added; OTOH we don't want to add heavy sync for a consistent view since
      // this is for metrics. Instead, we're going to check each regions as we go; to avoid making
      // too many checks, we maintain a local lists of server, limiting us to false negatives. If
      // we miss some recently-dead server, we'll just see it next time.
      Set<ServerName> recentlyLiveServers = new HashSet<>();
      int deadRegions = 0, unknownRegions = 0;
      for (RegionStateNode rsn : am.getRegionStates().getRegionStateNodes()) {
        if (rsn.getState() != State.OPEN) {
          continue; // Opportunistic check, should quickly skip RITs, offline tables, etc.
        }
        // Do not need to acquire region state lock as this is only for showing metrics.
        ServerName sn = rsn.getRegionLocation();
        State state = rsn.getState();
        if (state != State.OPEN) {
          continue; // Mostly skipping RITs that are already being take care of.
        }
        if (sn == null) {
          ++unknownRegions; // Opened on null?
          continue;
        }
        if (recentlyLiveServers.contains(sn)) {
          continue;
        }
        ServerManager.ServerLiveState sls = sm.isServerKnownAndOnline(sn);
        switch (sls) {
          case LIVE:
            recentlyLiveServers.add(sn);
            break;
          case DEAD:
            ++deadRegions;
            break;
          case UNKNOWN:
            ++unknownRegions;
            break;
          default: throw new AssertionError("Unexpected " + sls);
        }
      }
      if (deadRegions > 0 || unknownRegions > 0) {
        LOG.info("Found {} OPEN regions on dead servers and {} OPEN regions on unknown servers",
          deadRegions, unknownRegions);
      }

      am.updateDeadServerRegionMetrics(deadRegions, unknownRegions);
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
      if (m == null) {
        return false;
      }
      final RegionState state = m.get(regionInfo.getEncodedName());
      if (state == null) {
        return false;
      }
      return (statTimestamp - state.getStamp()) > (ritThreshold * 2);
    }

    protected void update(final AssignmentManager am) {
      final RegionStates regionStates = am.getRegionStates();
      this.statTimestamp = EnvironmentEdgeManager.currentTime();
      update(regionStates.getRegionsStateInTransition(), statTimestamp);
      update(regionStates.getRegionFailedOpen(), statTimestamp);

      if (LOG.isDebugEnabled() && ritsOverThreshold != null && !ritsOverThreshold.isEmpty()) {
        LOG.debug("RITs over threshold: {}",
          ritsOverThreshold.entrySet().stream()
            .map(e -> e.getKey() + ":" + e.getValue().getState().name())
            .collect(Collectors.joining("\n")));
      }
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

  private void updateDeadServerRegionMetrics(int deadRegions, int unknownRegions) {
    metrics.updateDeadServerOpenRegions(deadRegions);
    metrics.updateUnknownServerOpenRegions(unknownRegions);
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

    // Start the chores
    master.getMasterProcedureExecutor().addChore(this.ritChore);
    master.getMasterProcedureExecutor().addChore(this.deadMetricChore);

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
    TransitRegionStateProcedure[] procs =
      regionStates.getRegionStateNodes().stream().filter(rsn -> rsn.isInState(State.OFFLINE))
        .filter(rsn -> isTableEnabled(rsn.getRegionInfo().getTable())).map(rsn -> {
          rsn.lock();
          try {
            if (rsn.getProcedure() != null) {
              return null;
            } else {
              return rsn.setProcedure(TransitRegionStateProcedure.assign(getProcedureEnvironment(),
                rsn.getRegionInfo(), null));
            }
          } finally {
            rsn.unlock();
          }
        }).filter(p -> p != null).toArray(TransitRegionStateProcedure[]::new);
    if (procs.length > 0) {
      master.getMasterProcedureExecutor().submitProcedures(procs);
    }
  }

  /* AM internal RegionStateStore.RegionStateVisitor implementation. To be used when
   * scanning META table for region rows, using RegionStateStore utility methods. RegionStateStore
   * methods will convert Result into proper RegionInfo instances, but those would still need to be
   * added into AssignmentManager.regionStates in-memory cache.
   * RegionMetaLoadingVisitor.visitRegionState method provides the logic for adding RegionInfo
   * instances as loaded from latest META scan into AssignmentManager.regionStates.
   */
  private class RegionMetaLoadingVisitor implements RegionStateStore.RegionStateVisitor  {

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
      RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(regionInfo);
      // Do not need to lock on regionNode, as we can make sure that before we finish loading
      // meta, all the related procedures can not be executed. The only exception is for meta
      // region related operations, but here we do not load the informations for meta region.
      regionNode.setState(localState);
      regionNode.setLastHost(lastHost);
      regionNode.setRegionLocation(regionLocation);
      regionNode.setOpenSeqNum(openSeqNum);

      // Note: keep consistent with other methods, see region(Opening|Opened|Closing)
      //       RIT/ServerCrash handling should take care of the transiting regions.
      if (localState.matches(State.OPEN, State.OPENING, State.CLOSING, State.SPLITTING,
        State.MERGING)) {
        assert regionLocation != null : "found null region location for " + regionNode;
        regionStates.addRegionToServer(regionNode);
      } else if (localState == State.OFFLINE || regionInfo.isOffline()) {
        regionStates.addToOfflineRegions(regionNode);
      }
      if (regionNode.getProcedure() != null) {
        regionNode.getProcedure().stateLoaded(AssignmentManager.this, regionNode);
      }
    }
  };

  /**
   * Query META if the given <code>RegionInfo</code> exists, adding to
   * <code>AssignmentManager.regionStateStore</code> cache if the region is found in META.
   * @param regionEncodedName encoded name for the region to be loaded from META into
   *                          <code>AssignmentManager.regionStateStore</code> cache
   * @return <code>RegionInfo</code> instance for the given region if it is present in META
   *          and got successfully loaded into <code>AssignmentManager.regionStateStore</code>
   *          cache, <b>null</b> otherwise.
   * @throws UnknownRegionException if any errors occur while querying meta.
   */
  public RegionInfo loadRegionFromMeta(String regionEncodedName) throws UnknownRegionException {
    try {
      RegionMetaLoadingVisitor visitor = new RegionMetaLoadingVisitor();
      regionStateStore.visitMetaForRegion(regionEncodedName, visitor);
      return regionStates.getRegionState(regionEncodedName) == null ? null :
        regionStates.getRegionState(regionEncodedName).getRegion();
    } catch (IOException e) {
      throw new UnknownRegionException(
          "Error trying to load region " + regionEncodedName + " from META", e);
    }
  }

  private void loadMeta() throws IOException {
    // TODO: use a thread pool
    regionStateStore.visitMeta(new RegionMetaLoadingVisitor());
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

  /**
   * Usually run by the Master in reaction to server crash during normal processing.
   * Can also be invoked via external RPC to effect repair; in the latter case,
   * the 'force' flag is set so we push through the SCP though context may indicate
   * already-running-SCP (An old SCP may have exited abnormally, or damaged cluster
   * may still have references in hbase:meta to 'Unknown Servers' -- servers that
   * are not online or in dead servers list, etc.)
   * @param force Set if the request came in externally over RPC (via hbck2). Force means
   *              run the SCP even if it seems as though there might be an outstanding
   *              SCP running.
   * @return pid of scheduled SCP or {@link Procedure#NO_PROC_ID} if none scheduled.
   */
  public long submitServerCrash(ServerName serverName, boolean shouldSplitWal, boolean force) {
    // May be an 'Unknown Server' so handle case where serverNode is null.
    ServerStateNode serverNode = regionStates.getServerNode(serverName);
    // Remove the in-memory rsReports result
    synchronized (rsReports) {
      rsReports.remove(serverName);
    }

    // We hold the write lock here for fencing on reportRegionStateTransition. Once we set the
    // server state to CRASHED, we will no longer accept the reportRegionStateTransition call from
    // this server. This is used to simplify the implementation for TRSP and SCP, where we can make
    // sure that, the region list fetched by SCP will not be changed any more.
    if (serverNode != null) {
      serverNode.writeLock().lock();
    }
    boolean carryingMeta;
    long pid;
    try {
      ProcedureExecutor<MasterProcedureEnv> procExec = this.master.getMasterProcedureExecutor();
      carryingMeta = isCarryingMeta(serverName);
      if (!force && serverNode != null && !serverNode.isInState(ServerState.ONLINE)) {
        LOG.info("Skip adding ServerCrashProcedure for {} (meta={}) -- running?",
          serverNode, carryingMeta);
        return Procedure.NO_PROC_ID;
      } else {
        MasterProcedureEnv mpe = procExec.getEnvironment();
        // If serverNode == null, then 'Unknown Server'. Schedule HBCKSCP instead.
        // HBCKSCP scours Master in-memory state AND hbase;meta for references to
        // serverName just-in-case. An SCP that is scheduled when the server is
        // 'Unknown' probably originated externally with HBCK2 fix-it tool.
        ServerState oldState = null;
        if (serverNode != null) {
          oldState = serverNode.getState();
          serverNode.setState(ServerState.CRASHED);
        }

        if (force) {
          pid = procExec.submitProcedure(
              new HBCKServerCrashProcedure(mpe, serverName, shouldSplitWal, carryingMeta));
        } else {
          pid = procExec.submitProcedure(
              new ServerCrashProcedure(mpe, serverName, shouldSplitWal, carryingMeta));
        }
        LOG.info("Scheduled ServerCrashProcedure pid={} for {} (carryingMeta={}){}.",
          pid, serverName, carryingMeta,
          serverNode == null? "": " " + serverNode.toString() + ", oldState=" + oldState);
      }
    } finally {
      if (serverNode != null) {
        serverNode.writeLock().unlock();
      }
    }
    return pid;
  }

  public void offlineRegion(final RegionInfo regionInfo) {
    // TODO used by MasterRpcServices
    RegionStateNode node = regionStates.getRegionStateNode(regionInfo);
    if (node != null) {
      node.offline();
    }
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
   * @return Pair indicating the status of the alter command (pending/total)
   */
  public Pair<Integer, Integer> getReopenStatus(TableName tableName) {
    if (isTableDisabled(tableName)) {
      return new Pair<Integer, Integer>(0, 0);
    }

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
  //  Expected states on region state transition.
  //  Notice that there is expected states for transiting to OPENING state, this is because SCP.
  //  See the comments in regionOpening method for more details.
  // ============================================================================================
  private static final State[] STATES_EXPECTED_ON_OPEN = {
    State.OPENING, // Normal case
    State.OPEN // Retrying
  };

  private static final State[] STATES_EXPECTED_ON_CLOSING = {
    State.OPEN, // Normal case
    State.CLOSING, // Retrying
    State.SPLITTING, // Offline the split parent
    State.MERGING // Offline the merge parents
  };

  private static final State[] STATES_EXPECTED_ON_CLOSED = {
    State.CLOSING, // Normal case
    State.CLOSED // Retrying
  };

  // This is for manually scheduled region assign, can add other states later if we find out other
  // usages
  private static final State[] STATES_EXPECTED_ON_ASSIGN = { State.CLOSED, State.OFFLINE };

  // We only allow unassign or move a region which is in OPEN state.
  private static final State[] STATES_EXPECTED_ON_UNASSIGN_OR_MOVE = { State.OPEN };

  // ============================================================================================
  // Region Status update
  // Should only be called in TransitRegionStateProcedure(and related procedures), as the locking
  // and pre-assumptions are very tricky.
  // ============================================================================================
  private void transitStateAndUpdate(RegionStateNode regionNode, RegionState.State newState,
      RegionState.State... expectedStates) throws IOException {
    RegionState.State state = regionNode.getState();
    regionNode.transitionState(newState, expectedStates);
    boolean succ = false;
    try {
      regionStateStore.updateRegionLocation(regionNode);
      succ = true;
    } finally {
      if (!succ) {
        // revert
        regionNode.setState(state);
      }
    }
  }

  // should be called within the synchronized block of RegionStateNode
  void regionOpening(RegionStateNode regionNode) throws IOException {
    // As in SCP, for performance reason, there is no TRSP attached with this region, we will not
    // update the region state, which means that the region could be in any state when we want to
    // assign it after a RS crash. So here we do not pass the expectedStates parameter.
    transitStateAndUpdate(regionNode, State.OPENING);
    regionStates.addRegionToServer(regionNode);
    // update the operation count metrics
    metrics.incrementOperationCounter();
  }

  // should be called under the RegionStateNode lock
  // The parameter 'giveUp' means whether we will try to open the region again, if it is true, then
  // we will persist the FAILED_OPEN state into hbase:meta.
  void regionFailedOpen(RegionStateNode regionNode, boolean giveUp) throws IOException {
    RegionState.State state = regionNode.getState();
    ServerName regionLocation = regionNode.getRegionLocation();
    if (giveUp) {
      regionNode.setState(State.FAILED_OPEN);
      regionNode.setRegionLocation(null);
      boolean succ = false;
      try {
        regionStateStore.updateRegionLocation(regionNode);
        succ = true;
      } finally {
        if (!succ) {
          // revert
          regionNode.setState(state);
          regionNode.setRegionLocation(regionLocation);
        }
      }
    }
    if (regionLocation != null) {
      regionStates.removeRegionFromServer(regionLocation, regionNode);
    }
  }

  // should be called under the RegionStateNode lock
  void regionClosing(RegionStateNode regionNode) throws IOException {
    transitStateAndUpdate(regionNode, State.CLOSING, STATES_EXPECTED_ON_CLOSING);

    RegionInfo hri = regionNode.getRegionInfo();
    // Set meta has not initialized early. so people trying to create/edit tables will wait
    if (isMetaRegion(hri)) {
      setMetaAssigned(hri, false);
    }
    regionStates.addRegionToServer(regionNode);
    // update the operation count metrics
    metrics.incrementOperationCounter();
  }

  // for open and close, they will first be persist to the procedure store in
  // RegionRemoteProcedureBase. So here we will first change the in memory state as it is considered
  // as succeeded if the persistence to procedure store is succeeded, and then when the
  // RegionRemoteProcedureBase is woken up, we will persist the RegionStateNode to hbase:meta.

  // should be called under the RegionStateNode lock
  void regionOpenedWithoutPersistingToMeta(RegionStateNode regionNode) throws IOException {
    regionNode.transitionState(State.OPEN, STATES_EXPECTED_ON_OPEN);
    RegionInfo regionInfo = regionNode.getRegionInfo();
    regionStates.addRegionToServer(regionNode);
    regionStates.removeFromFailedOpen(regionInfo);
  }

  // should be called under the RegionStateNode lock
  void regionClosedWithoutPersistingToMeta(RegionStateNode regionNode) throws IOException {
    ServerName regionLocation = regionNode.getRegionLocation();
    regionNode.transitionState(State.CLOSED, STATES_EXPECTED_ON_CLOSED);
    regionNode.setRegionLocation(null);
    if (regionLocation != null) {
      regionNode.setLastHost(regionLocation);
      regionStates.removeRegionFromServer(regionLocation, regionNode);
    }
  }

  // should be called under the RegionStateNode lock
  // for SCP
  public void regionClosedAbnormally(RegionStateNode regionNode) throws IOException {
    RegionState.State state = regionNode.getState();
    ServerName regionLocation = regionNode.getRegionLocation();
    regionNode.transitionState(State.ABNORMALLY_CLOSED);
    regionNode.setRegionLocation(null);
    boolean succ = false;
    try {
      regionStateStore.updateRegionLocation(regionNode);
      succ = true;
    } finally {
      if (!succ) {
        // revert
        regionNode.setState(state);
        regionNode.setRegionLocation(regionLocation);
      }
    }
    if (regionLocation != null) {
      regionNode.setLastHost(regionLocation);
      regionStates.removeRegionFromServer(regionLocation, regionNode);
    }
  }

  void persistToMeta(RegionStateNode regionNode) throws IOException {
    regionStateStore.updateRegionLocation(regionNode);
    RegionInfo regionInfo = regionNode.getRegionInfo();
    if (isMetaRegion(regionInfo) && regionNode.getState() == State.OPEN) {
      // Usually we'd set a table ENABLED at this stage but hbase:meta is ALWAYs enabled, it
      // can't be disabled -- so skip the RPC (besides... enabled is managed by TableStateManager
      // which is backed by hbase:meta... Avoid setting ENABLED to avoid having to update state
      // on table that contains state.
      setMetaAssigned(regionInfo, true);
    }
  }

  // ============================================================================================
  // The above methods can only be called in TransitRegionStateProcedure(and related procedures)
  // ============================================================================================

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

    // TODO: here we just update the parent region info in meta, to set split and offline to true,
    // without changing the one in the region node. This is a bit confusing but the region info
    // field in RegionStateNode is not expected to be changed in the current design. Need to find a
    // possible way to address this problem, or at least adding more comments about the trick to
    // deal with this problem, that when you want to filter out split parent, you need to check both
    // the RegionState on whether it is split, and also the region info. If one of them matches then
    // it is a split parent. And usually only one of them can match, as after restart, the region
    // state will be changed from SPLIT to CLOSED.
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
    assignThread = new Thread(master.getServerName().toShortString()) {
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

      if (!isRunning()) {
        return null;
      }
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
          serversForSysTables.isEmpty() && !containsBogusAssignments(regions, systemHRIs) ?
              servers: serversForSysTables);
    }

    processAssignmentPlans(regions, retainMap, userHRIs, servers);
  }

  private boolean containsBogusAssignments(Map<RegionInfo, RegionStateNode> regions,
      List<RegionInfo> hirs) {
    for (RegionInfo ri : hirs) {
      if (regions.get(ri).getRegionLocation() != null &&
          regions.get(ri).getRegionLocation().equals(LoadBalancer.BOGUS_SERVER_NAME)){
        return true;
      }
    }
    return false;
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

    if (plan.isEmpty()) {
      throw new HBaseIOException("unable to compute plans for regions=" + regions.size());
    }

    int evcount = 0;
    for (Map.Entry<ServerName, List<RegionInfo>> entry: plan.entrySet()) {
      final ServerName server = entry.getKey();
      for (RegionInfo hri: entry.getValue()) {
        final RegionStateNode regionNode = regions.get(hri);
        regionNode.setRegionLocation(server);
        if (server.equals(LoadBalancer.BOGUS_SERVER_NAME) && regionNode.isSystemTable()) {
          assignQueueLock.lock();
          try {
            pendingAssignQueue.add(regionNode);
          } finally {
            assignQueueLock.unlock();
          }
        }else {
          events[evcount++] = regionNode.getProcedureEvent();
        }
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
   * For a given cluster with mixed versions of servers, get a list of
   * servers with lower versions, where system table regions should not be
   * assigned to.
   * For system table, we must assign regions to a server with highest version.
   * However, we can disable this exclusion using config:
   * "hbase.min.version.move.system.tables" if checkForMinVersion is true.
   * Detailed explanation available with definition of minVersionToMoveSysTables.
   *
   * @return List of Excluded servers for System table regions.
   */
  public List<ServerName> getExcludedServersForSystemTable() {
    // TODO: This should be a cached list kept by the ServerManager rather than calculated on each
    // move or system region assign. The RegionServerTracker keeps list of online Servers with
    // RegionServerInfo that includes Version.
    List<Pair<ServerName, String>> serverList = master.getServerManager().getOnlineServersList()
      .stream()
      .map(s->new Pair<>(s, master.getRegionServerVersion(s)))
      .collect(Collectors.toList());
    if (serverList.isEmpty()) {
      return new ArrayList<>();
    }
    String highestVersion = Collections.max(serverList,
      (o1, o2) -> VersionInfo.compareVersion(o1.getSecond(), o2.getSecond())).getSecond();
    if (!DEFAULT_MIN_VERSION_MOVE_SYS_TABLES_CONFIG.equals(minVersionToMoveSysTables)) {
      int comparedValue = VersionInfo.compareVersion(minVersionToMoveSysTables,
        highestVersion);
      if (comparedValue > 0) {
        return new ArrayList<>();
      }
    }
    return serverList.stream()
      .filter(pair -> !pair.getSecond().equals(highestVersion))
      .map(Pair::getFirst)
      .collect(Collectors.toList());
  }


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
