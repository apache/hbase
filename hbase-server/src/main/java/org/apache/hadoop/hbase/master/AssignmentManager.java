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
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeLoadBalancer;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.quotas.RegionStateListener;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Manages and performs region assignment.
 * Related communications with regionserver are all done over RPC.
 */
@InterfaceAudience.Private
public class AssignmentManager {
  private static final Log LOG = LogFactory.getLog(AssignmentManager.class);

  protected final MasterServices server;

  private ServerManager serverManager;

  private boolean shouldAssignRegionsWithFavoredNodes;

  private LoadBalancer balancer;

  private final MetricsAssignmentManager metricsAssignmentManager;

  private final TableLockManager tableLockManager;

  private AtomicInteger numRegionsOpened = new AtomicInteger(0);

  final private KeyLocker<String> locker = new KeyLocker<String>();

  Set<HRegionInfo> replicasToClose = Collections.synchronizedSet(new HashSet<HRegionInfo>());

  /**
   * Map of regions to reopen after the schema of a table is changed. Key -
   * encoded region name, value - HRegionInfo
   */
  private final Map <String, HRegionInfo> regionsToReopen;

  /*
   * Maximum times we recurse an assignment/unassignment.
   * See below in {@link #assign()} and {@link #unassign()}.
   */
  private final int maximumAttempts;

  /**
   * The sleep time for which the assignment will wait before retrying in case of
   * hbase:meta assignment failure due to lack of availability of region plan or bad region plan
   */
  private final long sleepTimeBeforeRetryingMetaAssignment;

  /** Plans for region movement. Key is the encoded version of a region name*/
  // TODO: When do plans get cleaned out?  Ever? In server open and in server
  // shutdown processing -- St.Ack
  // All access to this Map must be synchronized.
  final NavigableMap<String, RegionPlan> regionPlans =
    new TreeMap<String, RegionPlan>();

  private final TableStateManager tableStateManager;

  private final ExecutorService executorService;

  // Thread pool executor service. TODO, consolidate with executorService?
  private java.util.concurrent.ExecutorService threadPoolExecutorService;

  private final RegionStates regionStates;

  // The threshold to use bulk assigning. Using bulk assignment
  // only if assigning at least this many regions to at least this
  // many servers. If assigning fewer regions to fewer servers,
  // bulk assigning may be not as efficient.
  private final int bulkAssignThresholdRegions;
  private final int bulkAssignThresholdServers;
  private final int bulkPerRegionOpenTimeGuesstimate;

  // Should bulk assignment wait till all regions are assigned,
  // or it is timed out?  This is useful to measure bulk assignment
  // performance, but not needed in most use cases.
  private final boolean bulkAssignWaitTillAllAssigned;

  /**
   * Indicator that AssignmentManager has recovered the region states so
   * that ServerShutdownHandler can be fully enabled and re-assign regions
   * of dead servers. So that when re-assignment happens, AssignmentManager
   * has proper region states.
   *
   * Protected to ease testing.
   */
  protected final AtomicBoolean failoverCleanupDone = new AtomicBoolean(false);

  /**
   * A map to track the count a region fails to open in a row.
   * So that we don't try to open a region forever if the failure is
   * unrecoverable.  We don't put this information in region states
   * because we don't expect this to happen frequently; we don't
   * want to copy this information over during each state transition either.
   */
  private final ConcurrentHashMap<String, AtomicInteger>
    failedOpenTracker = new ConcurrentHashMap<String, AtomicInteger>();

  // In case not using ZK for region assignment, region states
  // are persisted in meta with a state store
  private final RegionStateStore regionStateStore;

  /**
   * For testing only!  Set to true to skip handling of split.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="MS_SHOULD_BE_FINAL")
  public static boolean TEST_SKIP_SPLIT_HANDLING = false;

  /** Listeners that are called on assignment events. */
  private List<AssignmentListener> listeners = new CopyOnWriteArrayList<AssignmentListener>();

  private RegionStateListener regionStateListener;

  /**
   * Constructs a new assignment manager.
   *
   * @param server instance of HMaster this AM running inside
   * @param serverManager serverManager for associated HMaster
   * @param balancer implementation of {@link LoadBalancer}
   * @param service Executor service
   * @param metricsMaster metrics manager
   * @param tableLockManager TableLock manager
   * @throws IOException
   */
  public AssignmentManager(MasterServices server, ServerManager serverManager,
      final LoadBalancer balancer,
      final ExecutorService service, MetricsMaster metricsMaster,
      final TableLockManager tableLockManager,
      final TableStateManager tableStateManager)
          throws IOException {
    this.server = server;
    this.serverManager = serverManager;
    this.executorService = service;
    this.regionStateStore = new RegionStateStore(server);
    this.regionsToReopen = Collections.synchronizedMap
                           (new HashMap<String, HRegionInfo> ());
    Configuration conf = server.getConfiguration();
    // Only read favored nodes if using the favored nodes load balancer.
    this.shouldAssignRegionsWithFavoredNodes = conf.getClass(
           HConstants.HBASE_MASTER_LOADBALANCER_CLASS, Object.class).equals(
           FavoredNodeLoadBalancer.class);

    this.tableStateManager = tableStateManager;

    // This is the max attempts, not retries, so it should be at least 1.
    this.maximumAttempts = Math.max(1,
      this.server.getConfiguration().getInt("hbase.assignment.maximum.attempts", 10));
    this.sleepTimeBeforeRetryingMetaAssignment = this.server.getConfiguration().getLong(
        "hbase.meta.assignment.retry.sleeptime", 1000l);
    this.balancer = balancer;
    int maxThreads = conf.getInt("hbase.assignment.threads.max", 30);
    this.threadPoolExecutorService = Threads.getBoundedCachedThreadPool(
      maxThreads, 60L, TimeUnit.SECONDS, Threads.newDaemonThreadFactory("AM."));
    this.regionStates = new RegionStates(
      server, tableStateManager, serverManager, regionStateStore);

    this.bulkAssignWaitTillAllAssigned =
      conf.getBoolean("hbase.bulk.assignment.waittillallassigned", false);
    this.bulkAssignThresholdRegions = conf.getInt("hbase.bulk.assignment.threshold.regions", 7);
    this.bulkAssignThresholdServers = conf.getInt("hbase.bulk.assignment.threshold.servers", 3);
    this.bulkPerRegionOpenTimeGuesstimate =
      conf.getInt("hbase.bulk.assignment.perregion.open.time", 10000);

    this.metricsAssignmentManager = new MetricsAssignmentManager();
    this.tableLockManager = tableLockManager;
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

  /**
   * @return Instance of ZKTableStateManager.
   */
  public TableStateManager getTableStateManager() {
    // These are 'expensive' to make involving trip to zk ensemble so allow
    // sharing.
    return this.tableStateManager;
  }

  /**
   * This SHOULD not be public. It is public now
   * because of some unit tests.
   *
   * TODO: make it package private and keep RegionStates in the master package
   */
  public RegionStates getRegionStates() {
    return regionStates;
  }

  /**
   * Used in some tests to mock up region state in meta
   */
  @VisibleForTesting
  RegionStateStore getRegionStateStore() {
    return regionStateStore;
  }

  public RegionPlan getRegionReopenPlan(HRegionInfo hri) {
    return new RegionPlan(hri, null, regionStates.getRegionServerOfRegion(hri));
  }

  /**
   * Add a regionPlan for the specified region.
   * @param encodedName
   * @param plan
   */
  public void addPlan(String encodedName, RegionPlan plan) {
    synchronized (regionPlans) {
      regionPlans.put(encodedName, plan);
    }
  }

  /**
   * Add a map of region plans.
   */
  public void addPlans(Map<String, RegionPlan> plans) {
    synchronized (regionPlans) {
      regionPlans.putAll(plans);
    }
  }

  /**
   * Set the list of regions that will be reopened
   * because of an update in table schema
   *
   * @param regions
   *          list of regions that should be tracked for reopen
   */
  public void setRegionsToReopen(List <HRegionInfo> regions) {
    for(HRegionInfo hri : regions) {
      regionsToReopen.put(hri.getEncodedName(), hri);
    }
  }

  /**
   * Used by the client to identify if all regions have the schema updates
   *
   * @param tableName
   * @return Pair indicating the status of the alter command
   * @throws IOException
   */
  public Pair<Integer, Integer> getReopenStatus(TableName tableName)
      throws IOException {
    List<HRegionInfo> hris;
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      hris = new MetaTableLocator().getMetaRegions(server.getZooKeeper());
    } else {
      hris = MetaTableAccessor.getTableRegions(server.getConnection(), tableName, true);
    }

    Integer pending = 0;
    for (HRegionInfo hri : hris) {
      String name = hri.getEncodedName();
      // no lock concurrent access ok: sequential consistency respected.
      if (regionsToReopen.containsKey(name)
          || regionStates.isRegionInTransition(name)) {
        pending++;
      }
    }
    return new Pair<Integer, Integer>(pending, hris.size());
  }

  /**
   * Used by ServerShutdownHandler to make sure AssignmentManager has completed
   * the failover cleanup before re-assigning regions of dead servers. So that
   * when re-assignment happens, AssignmentManager has proper region states.
   */
  public boolean isFailoverCleanupDone() {
    return failoverCleanupDone.get();
  }

  /**
   * To avoid racing with AM, external entities may need to lock a region,
   * for example, when SSH checks what regions to skip re-assigning.
   */
  public Lock acquireRegionLock(final String encodedName) {
    return locker.acquireLock(encodedName);
  }

  /**
   * Now, failover cleanup is completed. Notify server manager to
   * process queued up dead servers processing, if any.
   */
  void failoverCleanupDone() {
    failoverCleanupDone.set(true);
    serverManager.processQueuedDeadServers();
  }

  /**
   * Called on startup.
   * Figures whether a fresh cluster start of we are joining extant running cluster.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws CoordinatedStateException
   */
  void joinCluster()
  throws IOException, KeeperException, InterruptedException, CoordinatedStateException {
    long startTime = System.currentTimeMillis();
    // Concurrency note: In the below the accesses on regionsInTransition are
    // outside of a synchronization block where usually all accesses to RIT are
    // synchronized.  The presumption is that in this case it is safe since this
    // method is being played by a single thread on startup.

    // TODO: Regions that have a null location and are not in regionsInTransitions
    // need to be handled.

    // Scan hbase:meta to build list of existing regions, servers, and assignment
    // Returns servers who have not checked in (assumed dead) that some regions
    // were assigned to (according to the meta)
    Set<ServerName> deadServers = rebuildUserRegions();

    // This method will assign all user regions if a clean server startup or
    // it will reconstruct master state and cleanup any leftovers from previous master process.
    boolean failover = processDeadServersAndRegionsInTransition(deadServers);

    recoverTableInDisablingState();
    recoverTableInEnablingState();
    LOG.info("Joined the cluster in " + (System.currentTimeMillis()
      - startTime) + "ms, failover=" + failover);
  }

  /**
   * Process all regions that are in transition in zookeeper and also
   * processes the list of dead servers.
   * Used by master joining an cluster.  If we figure this is a clean cluster
   * startup, will assign all user regions.
   * @param deadServers Set of servers that are offline probably legitimately that were carrying
   * regions according to a scan of hbase:meta. Can be null.
   * @throws IOException
   * @throws InterruptedException
   */
  boolean processDeadServersAndRegionsInTransition(final Set<ServerName> deadServers)
  throws KeeperException, IOException, InterruptedException, CoordinatedStateException {
    // TODO Needed? List<String> nodes = ZKUtil.listChildrenNoWatch(watcher, watcher.assignmentZNode);
    boolean failover = !serverManager.getDeadServers().isEmpty();
    if (failover) {
      // This may not be a failover actually, especially if meta is on this master.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found dead servers out on cluster " + serverManager.getDeadServers());
      }
    } else {
      // If any one region except meta is assigned, it's a failover.
      Set<ServerName> onlineServers = serverManager.getOnlineServers().keySet();
      for (Map.Entry<HRegionInfo, ServerName> en:
          regionStates.getRegionAssignments().entrySet()) {
        HRegionInfo hri = en.getKey();
        if (!hri.isMetaTable()
            && onlineServers.contains(en.getValue())) {
          LOG.debug("Found " + hri + " out on cluster");
          failover = true;
          break;
        }
      }
      if (!failover) {
        // If any region except meta is in transition on a live server, it's a failover.
        Map<String, RegionState> regionsInTransition = regionStates.getRegionsInTransition();
        if (!regionsInTransition.isEmpty()) {
          for (RegionState regionState: regionsInTransition.values()) {
            ServerName serverName = regionState.getServerName();
            if (!regionState.getRegion().isMetaRegion()
                && serverName != null && onlineServers.contains(serverName)) {
              LOG.debug("Found " + regionState + " in RITs");
              failover = true;
              break;
            }
          }
        }
      }
    }
    if (!failover) {
      // If we get here, we have a full cluster restart. It is a failover only
      // if there are some WALs are not split yet. For meta WALs, they should have
      // been split already, if any. We can walk through those queued dead servers,
      // if they don't have any WALs, this restart should be considered as a clean one
      Set<ServerName> queuedDeadServers = serverManager.getRequeuedDeadServers().keySet();
      if (!queuedDeadServers.isEmpty()) {
        Configuration conf = server.getConfiguration();
        Path rootdir = FSUtils.getRootDir(conf);
        FileSystem fs = rootdir.getFileSystem(conf);
        for (ServerName serverName: queuedDeadServers) {
          // In the case of a clean exit, the shutdown handler would have presplit any WALs and
          // removed empty directories.
          Path logDir = new Path(rootdir,
              DefaultWALProvider.getWALDirectoryName(serverName.toString()));
          Path splitDir = logDir.suffix(DefaultWALProvider.SPLITTING_EXT);
          if (fs.exists(logDir) || fs.exists(splitDir)) {
            LOG.debug("Found queued dead server " + serverName);
            failover = true;
            break;
          }
        }
        if (!failover) {
          // We figured that it's not a failover, so no need to
          // work on these re-queued dead servers any more.
          LOG.info("AM figured that it's not a failover and cleaned up "
            + queuedDeadServers.size() + " queued dead servers");
          serverManager.removeRequeuedDeadServers();
        }
      }
    }

    Set<TableName> disabledOrDisablingOrEnabling = null;
    Map<HRegionInfo, ServerName> allRegions = null;

    if (!failover) {
      disabledOrDisablingOrEnabling = tableStateManager.getTablesInStates(
        TableState.State.DISABLED, TableState.State.DISABLING,
        TableState.State.ENABLING);

      // Clean re/start, mark all user regions closed before reassignment
      allRegions = regionStates.closeAllUserRegions(
        disabledOrDisablingOrEnabling);
    }

    // Now region states are restored
    regionStateStore.start();

    if (failover) {
      if (deadServers != null && !deadServers.isEmpty()) {
        for (ServerName serverName: deadServers) {
          if (!serverManager.isServerDead(serverName)) {
            serverManager.expireServer(serverName); // Let SSH do region re-assign
          }
        }
      }
      processRegionsInTransition(regionStates.getRegionsInTransition().values());
    }

    // Now we can safely claim failover cleanup completed and enable
    // ServerShutdownHandler for further processing. The nodes (below)
    // in transition, if any, are for regions not related to those
    // dead servers at all, and can be done in parallel to SSH.
    failoverCleanupDone();
    if (!failover) {
      // Fresh cluster startup.
      LOG.info("Clean cluster startup. Assigning user regions");
      assignAllUserRegions(allRegions);
    }
    // unassign replicas of the split parents and the merged regions
    // the daughter replicas are opened in assignAllUserRegions if it was
    // not already opened.
    for (HRegionInfo h : replicasToClose) {
      unassign(h);
    }
    replicasToClose.clear();
    return failover;
  }

  /**
   * When a region is closed, it should be removed from the regionsToReopen
   * @param hri HRegionInfo of the region which was closed
   */
  public void removeClosedRegion(HRegionInfo hri) {
    if (regionsToReopen.remove(hri.getEncodedName()) != null) {
      LOG.debug("Removed region from reopening regions because it was closed");
    }
  }

  // TODO: processFavoredNodes might throw an exception, for e.g., if the
  // meta could not be contacted/updated. We need to see how seriously to treat
  // this problem as. Should we fail the current assignment. We should be able
  // to recover from this problem eventually (if the meta couldn't be updated
  // things should work normally and eventually get fixed up).
  void processFavoredNodes(List<HRegionInfo> regions) throws IOException {
    if (!shouldAssignRegionsWithFavoredNodes) return;
    // The AM gets the favored nodes info for each region and updates the meta
    // table with that info
    Map<HRegionInfo, List<ServerName>> regionToFavoredNodes =
        new HashMap<HRegionInfo, List<ServerName>>();
    for (HRegionInfo region : regions) {
      regionToFavoredNodes.put(region,
          ((FavoredNodeLoadBalancer)this.balancer).getFavoredNodes(region));
    }
    FavoredNodeAssignmentHelper.updateMetaWithFavoredNodesInfo(regionToFavoredNodes,
      this.server.getConnection());
  }

  /**
   * Marks the region as online.  Removes it from regions in transition and
   * updates the in-memory assignment information.
   * <p>
   * Used when a region has been successfully opened on a region server.
   * @param regionInfo
   * @param sn
   */
  void regionOnline(HRegionInfo regionInfo, ServerName sn) {
    regionOnline(regionInfo, sn, HConstants.NO_SEQNUM);
  }

  void regionOnline(HRegionInfo regionInfo, ServerName sn, long openSeqNum) {
    numRegionsOpened.incrementAndGet();
    regionStates.regionOnline(regionInfo, sn, openSeqNum);

    // Remove plan if one.
    clearRegionPlan(regionInfo);
    balancer.regionOnline(regionInfo, sn);

    // Tell our listeners that a region was opened
    sendRegionOpenedNotification(regionInfo, sn);
  }

  /**
   * Marks the region as offline.  Removes it from regions in transition and
   * removes in-memory assignment information.
   * <p>
   * Used when a region has been closed and should remain closed.
   * @param regionInfo
   */
  public void regionOffline(final HRegionInfo regionInfo) {
    regionOffline(regionInfo, null);
  }

  public void offlineDisabledRegion(HRegionInfo regionInfo) {
    replicasToClose.remove(regionInfo);
    regionOffline(regionInfo);
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
   * OFFLINE state or not in transition, and of course, the
   * chosen server is up and running (It may have just crashed!).
   *
   * @param region server to be assigned
   */
  public void assign(HRegionInfo region) {
    assign(region, false);
  }

  /**
   * Use care with forceNewPlan. It could cause double assignment.
   */
  public void assign(HRegionInfo region, boolean forceNewPlan) {
    if (isDisabledorDisablingRegionInRIT(region)) {
      return;
    }
    String encodedName = region.getEncodedName();
    Lock lock = locker.acquireLock(encodedName);
    try {
      RegionState state = forceRegionStateToOffline(region, forceNewPlan);
      if (state != null) {
        if (regionStates.wasRegionOnDeadServer(encodedName)) {
          LOG.info("Skip assigning " + region.getRegionNameAsString()
            + ", it's host " + regionStates.getLastRegionServerOfRegion(encodedName)
            + " is dead but not processed yet");
          return;
        }
        assign(state, forceNewPlan);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Bulk assign regions to <code>destination</code>.
   * @param destination
   * @param regions Regions to assign.
   * @return true if successful
   */
  boolean assign(final ServerName destination, final List<HRegionInfo> regions)
    throws InterruptedException {
    long startTime = EnvironmentEdgeManager.currentTime();
    try {
      int regionCount = regions.size();
      if (regionCount == 0) {
        return true;
      }
      LOG.info("Assigning " + regionCount + " region(s) to " + destination.toString());
      Set<String> encodedNames = new HashSet<String>(regionCount);
      for (HRegionInfo region : regions) {
        encodedNames.add(region.getEncodedName());
      }

      List<HRegionInfo> failedToOpenRegions = new ArrayList<HRegionInfo>();
      Map<String, Lock> locks = locker.acquireLocks(encodedNames);
      try {
        Map<String, RegionPlan> plans = new HashMap<String, RegionPlan>(regionCount);
        List<RegionState> states = new ArrayList<RegionState>(regionCount);
        for (HRegionInfo region : regions) {
          String encodedName = region.getEncodedName();
          if (!isDisabledorDisablingRegionInRIT(region)) {
            RegionState state = forceRegionStateToOffline(region, false);
            boolean onDeadServer = false;
            if (state != null) {
              if (regionStates.wasRegionOnDeadServer(encodedName)) {
                LOG.info("Skip assigning " + region.getRegionNameAsString()
                  + ", it's host " + regionStates.getLastRegionServerOfRegion(encodedName)
                  + " is dead but not processed yet");
                onDeadServer = true;
              } else {
                RegionPlan plan = new RegionPlan(region, state.getServerName(), destination);
                plans.put(encodedName, plan);
                states.add(state);
                continue;
              }
            }
            // Reassign if the region wasn't on a dead server
            if (!onDeadServer) {
              LOG.info("failed to force region state to offline, "
                + "will reassign later: " + region);
              failedToOpenRegions.add(region); // assign individually later
            }
          }
          // Release the lock, this region is excluded from bulk assign because
          // we can't update its state, or set its znode to offline.
          Lock lock = locks.remove(encodedName);
          lock.unlock();
        }

        if (server.isStopped()) {
          return false;
        }

        // Add region plans, so we can updateTimers when one region is opened so
        // that unnecessary timeout on RIT is reduced.
        this.addPlans(plans);

        List<Pair<HRegionInfo, List<ServerName>>> regionOpenInfos =
          new ArrayList<Pair<HRegionInfo, List<ServerName>>>(states.size());
        for (RegionState state: states) {
          HRegionInfo region = state.getRegion();
          regionStates.updateRegionState(
            region, State.PENDING_OPEN, destination);
          List<ServerName> favoredNodes = ServerName.EMPTY_SERVER_LIST;
          if (this.shouldAssignRegionsWithFavoredNodes) {
            favoredNodes = ((FavoredNodeLoadBalancer)this.balancer).getFavoredNodes(region);
          }
          regionOpenInfos.add(new Pair<HRegionInfo, List<ServerName>>(
            region, favoredNodes));
        }

        // Move on to open regions.
        try {
          // Send OPEN RPC. If it fails on a IOE or RemoteException,
          // regions will be assigned individually.
          Configuration conf = server.getConfiguration();
          long maxWaitTime = System.currentTimeMillis() +
            conf.getLong("hbase.regionserver.rpc.startup.waittime", 60000);
          for (int i = 1; i <= maximumAttempts && !server.isStopped(); i++) {
            try {
              List<RegionOpeningState> regionOpeningStateList = serverManager
                .sendRegionOpen(destination, regionOpenInfos);
              for (int k = 0, n = regionOpeningStateList.size(); k < n; k++) {
                RegionOpeningState openingState = regionOpeningStateList.get(k);
                if (openingState != RegionOpeningState.OPENED) {
                  HRegionInfo region = regionOpenInfos.get(k).getFirst();
                  LOG.info("Got opening state " + openingState
                    + ", will reassign later: " + region);
                  // Failed opening this region, reassign it later
                  forceRegionStateToOffline(region, true);
                  failedToOpenRegions.add(region);
                }
              }
              break;
            } catch (IOException e) {
              if (e instanceof RemoteException) {
                e = ((RemoteException)e).unwrapRemoteException();
              }
              if (e instanceof RegionServerStoppedException) {
                LOG.warn("The region server was shut down, ", e);
                // No need to retry, the region server is a goner.
                return false;
              } else if (e instanceof ServerNotRunningYetException) {
                long now = System.currentTimeMillis();
                if (now < maxWaitTime) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Server is not yet up; waiting up to " +
                      (maxWaitTime - now) + "ms", e);
                  }
                  Thread.sleep(100);
                  i--; // reset the try count
                  continue;
                }
              } else if (e instanceof java.net.SocketTimeoutException
                  && this.serverManager.isServerOnline(destination)) {
                // In case socket is timed out and the region server is still online,
                // the openRegion RPC could have been accepted by the server and
                // just the response didn't go through.  So we will retry to
                // open the region on the same server.
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Bulk assigner openRegion() to " + destination
                    + " has timed out, but the regions might"
                    + " already be opened on it.", e);
                }
                // wait and reset the re-try count, server might be just busy.
                Thread.sleep(100);
                i--;
                continue;
              } else if (e instanceof FailedServerException && i < maximumAttempts) {
                // In case the server is in the failed server list, no point to
                // retry too soon. Retry after the failed_server_expiry time
                long sleepTime = 1 + conf.getInt(RpcClient.FAILED_SERVER_EXPIRY_KEY,
                  RpcClient.FAILED_SERVER_EXPIRY_DEFAULT);
                if (LOG.isDebugEnabled()) {
                  LOG.debug(destination + " is on failed server list; waiting "
                    + sleepTime + "ms", e);
                }
                Thread.sleep(sleepTime);
                continue;
              }
              throw e;
            }
          }
        } catch (IOException e) {
          // Can be a socket timeout, EOF, NoRouteToHost, etc
          LOG.info("Unable to communicate with " + destination
            + " in order to assign regions, ", e);
          for (RegionState state: states) {
            HRegionInfo region = state.getRegion();
            forceRegionStateToOffline(region, true);
          }
          return false;
        }
      } finally {
        for (Lock lock : locks.values()) {
          lock.unlock();
        }
      }

      if (!failedToOpenRegions.isEmpty()) {
        for (HRegionInfo region : failedToOpenRegions) {
          if (!regionStates.isRegionOnline(region)) {
            invokeAssign(region);
          }
        }
      }

      // wait for assignment completion
      ArrayList<HRegionInfo> userRegionSet = new ArrayList<HRegionInfo>(regions.size());
      for (HRegionInfo region: regions) {
        if (!region.getTable().isSystemTable()) {
          userRegionSet.add(region);
        }
      }
      if (!waitForAssignment(userRegionSet, true, userRegionSet.size(),
            System.currentTimeMillis())) {
        LOG.debug("some user regions are still in transition: " + userRegionSet);
      }
      LOG.debug("Bulk assigning done for " + destination);
      return true;
    } finally {
      metricsAssignmentManager.updateBulkAssignTime(EnvironmentEdgeManager.currentTime() - startTime);
    }
  }

  /**
   * Send CLOSE RPC if the server is online, otherwise, offline the region.
   *
   * The RPC will be sent only to the region sever found in the region state
   * if it is passed in, otherwise, to the src server specified. If region
   * state is not specified, we don't update region state at all, instead
   * we just send the RPC call. This is useful for some cleanup without
   * messing around the region states (see handleRegion, on region opened
   * on an unexpected server scenario, for an example)
   */
  private void unassign(final HRegionInfo region,
      final ServerName server, final ServerName dest) {
    for (int i = 1; i <= this.maximumAttempts; i++) {
      if (this.server.isStopped() || this.server.isAborted()) {
        LOG.debug("Server stopped/aborted; skipping unassign of " + region);
        return;
      }
      if (!serverManager.isServerOnline(server)) {
        LOG.debug("Offline " + region.getRegionNameAsString()
          + ", no need to unassign since it's on a dead server: " + server);
        regionStates.updateRegionState(region, State.OFFLINE);
        return;
      }
      try {
        // Send CLOSE RPC
        if (serverManager.sendRegionClose(server, region, dest)) {
          LOG.debug("Sent CLOSE to " + server + " for region " +
            region.getRegionNameAsString());
          return;
        }
        // This never happens. Currently regionserver close always return true.
        // Todo; this can now happen (0.96) if there is an exception in a coprocessor
        LOG.warn("Server " + server + " region CLOSE RPC returned false for " +
          region.getRegionNameAsString());
      } catch (Throwable t) {
        long sleepTime = 0;
        Configuration conf = this.server.getConfiguration();
        if (t instanceof RemoteException) {
          t = ((RemoteException)t).unwrapRemoteException();
        }
        if (t instanceof RegionServerAbortedException) {
          // RS is aborting, we cannot offline the region since the region may need to do WAL
          // recovery. Until we see  the RS expiration, we should retry.
          sleepTime = 1 + conf.getInt(RpcClient.FAILED_SERVER_EXPIRY_KEY,
            RpcClient.FAILED_SERVER_EXPIRY_DEFAULT);

        } else if (t instanceof NotServingRegionException
            || t instanceof RegionServerStoppedException
            || t instanceof ServerNotRunningYetException) {
          LOG.debug("Offline " + region.getRegionNameAsString()
            + ", it's not any more on " + server, t);
          regionStates.updateRegionState(region, State.OFFLINE);
          return;
        } else if (t instanceof FailedServerException && i < maximumAttempts) {
          // In case the server is in the failed server list, no point to
          // retry too soon. Retry after the failed_server_expiry time
          sleepTime = 1 + conf.getInt(RpcClient.FAILED_SERVER_EXPIRY_KEY,
          RpcClient.FAILED_SERVER_EXPIRY_DEFAULT);
          if (LOG.isDebugEnabled()) {
            LOG.debug(server + " is on failed server list; waiting " + sleepTime + "ms", t);
          }
       }
       try {
         if (sleepTime > 0) {
           Thread.sleep(sleepTime);
         }
       } catch (InterruptedException ie) {
         LOG.warn("Interrupted unassign " + region.getRegionNameAsString(), ie);
         Thread.currentThread().interrupt();
         regionStates.updateRegionState(region, State.FAILED_CLOSE);
         return;
       }
       LOG.info("Server " + server + " returned " + t + " for "
         + region.getRegionNameAsString() + ", try=" + i
         + " of " + this.maximumAttempts, t);
      }
    }
    // Run out of attempts
    regionStates.updateRegionState(region, State.FAILED_CLOSE);
  }

  /**
   * Set region to OFFLINE unless it is opening and forceNewPlan is false.
   */
  private RegionState forceRegionStateToOffline(
      final HRegionInfo region, final boolean forceNewPlan) {
    RegionState state = regionStates.getRegionState(region);
    if (state == null) {
      LOG.warn("Assigning but not in region states: " + region);
      state = regionStates.createRegionState(region);
    }

    if (forceNewPlan && LOG.isDebugEnabled()) {
      LOG.debug("Force region state offline " + state);
    }

    switch (state.getState()) {
    case OPEN:
    case OPENING:
    case PENDING_OPEN:
    case CLOSING:
    case PENDING_CLOSE:
      if (!forceNewPlan) {
        LOG.debug("Skip assigning " +
          region + ", it is already " + state);
        return null;
      }
    case FAILED_CLOSE:
    case FAILED_OPEN:
      regionStates.updateRegionState(region, State.PENDING_CLOSE);
      unassign(region, state.getServerName(), null);
      state = regionStates.getRegionState(region);
      if (!state.isOffline() && !state.isClosed()) {
        // If the region isn't offline, we can't re-assign
        // it now. It will be assigned automatically after
        // the regionserver reports it's closed.
        return null;
      }
    case OFFLINE:
    case CLOSED:
      break;
    default:
      LOG.error("Trying to assign region " + region
        + ", which is " + state);
      return null;
    }
    return state;
  }

  /**
   * Caller must hold lock on the passed <code>state</code> object.
   * @param state
   * @param forceNewPlan
   */
  private void assign(RegionState state, boolean forceNewPlan) {
    long startTime = EnvironmentEdgeManager.currentTime();
    try {
      Configuration conf = server.getConfiguration();
      RegionPlan plan = null;
      long maxWaitTime = -1;
      HRegionInfo region = state.getRegion();
      Throwable previousException = null;
      for (int i = 1; i <= maximumAttempts; i++) {
        if (server.isStopped() || server.isAborted()) {
          LOG.info("Skip assigning " + region.getRegionNameAsString()
            + ", the server is stopped/aborted");
          return;
        }

        if (plan == null) { // Get a server for the region at first
          try {
            plan = getRegionPlan(region, forceNewPlan);
          } catch (HBaseIOException e) {
            LOG.warn("Failed to get region plan", e);
          }
        }

        if (plan == null) {
          LOG.warn("Unable to determine a plan to assign " + region);

          // For meta region, we have to keep retrying until succeeding
          if (region.isMetaRegion()) {
            if (i == maximumAttempts) {
              i = 0; // re-set attempt count to 0 for at least 1 retry

              LOG.warn("Unable to determine a plan to assign a hbase:meta region " + region +
                " after maximumAttempts (" + this.maximumAttempts +
                "). Reset attempts count and continue retrying.");
            }
            waitForRetryingMetaAssignment();
            continue;
          }

          regionStates.updateRegionState(region, State.FAILED_OPEN);
          return;
        }
        LOG.info("Assigning " + region.getRegionNameAsString() +
            " to " + plan.getDestination().toString());
        // Transition RegionState to PENDING_OPEN
       regionStates.updateRegionState(region,
          State.PENDING_OPEN, plan.getDestination());

        boolean needNewPlan = false;
        final String assignMsg = "Failed assignment of " + region.getRegionNameAsString() +
            " to " + plan.getDestination();
        try {
          List<ServerName> favoredNodes = ServerName.EMPTY_SERVER_LIST;
          if (this.shouldAssignRegionsWithFavoredNodes) {
            favoredNodes = ((FavoredNodeLoadBalancer)this.balancer).getFavoredNodes(region);
          }
          serverManager.sendRegionOpen(plan.getDestination(), region, favoredNodes);
          return; // we're done
        } catch (Throwable t) {
          if (t instanceof RemoteException) {
            t = ((RemoteException) t).unwrapRemoteException();
          }
          previousException = t;

          // Should we wait a little before retrying? If the server is starting it's yes.
          boolean hold = (t instanceof ServerNotRunningYetException);

          // In case socket is timed out and the region server is still online,
          // the openRegion RPC could have been accepted by the server and
          // just the response didn't go through.  So we will retry to
          // open the region on the same server.
          boolean retry = !hold && (t instanceof java.net.SocketTimeoutException
              && this.serverManager.isServerOnline(plan.getDestination()));

          if (hold) {
            LOG.warn(assignMsg + ", waiting a little before trying on the same region server " +
              "try=" + i + " of " + this.maximumAttempts, t);

            if (maxWaitTime < 0) {
              maxWaitTime = EnvironmentEdgeManager.currentTime()
                + this.server.getConfiguration().getLong(
                  "hbase.regionserver.rpc.startup.waittime", 60000);
            }
            try {
              long now = EnvironmentEdgeManager.currentTime();
              if (now < maxWaitTime) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Server is not yet up; waiting up to "
                    + (maxWaitTime - now) + "ms", t);
                }
                Thread.sleep(100);
                i--; // reset the try count
              } else {
                LOG.debug("Server is not up for a while; try a new one", t);
                needNewPlan = true;
              }
            } catch (InterruptedException ie) {
              LOG.warn("Failed to assign "
                  + region.getRegionNameAsString() + " since interrupted", ie);
              regionStates.updateRegionState(region, State.FAILED_OPEN);
              Thread.currentThread().interrupt();
              return;
            }
          } else if (retry) {
            i--; // we want to retry as many times as needed as long as the RS is not dead.
            if (LOG.isDebugEnabled()) {
              LOG.debug(assignMsg + ", trying to assign to the same region server due ", t);
            }
          } else {
            needNewPlan = true;
            LOG.warn(assignMsg + ", trying to assign elsewhere instead;" +
                " try=" + i + " of " + this.maximumAttempts, t);
          }
        }

        if (i == this.maximumAttempts) {
          // For meta region, we have to keep retrying until succeeding
          if (region.isMetaRegion()) {
            i = 0; // re-set attempt count to 0 for at least 1 retry
            LOG.warn(assignMsg +
                ", trying to assign a hbase:meta region reached to maximumAttempts (" +
                this.maximumAttempts + ").  Reset attempt counts and continue retrying.");
            waitForRetryingMetaAssignment();
          }
          else {
            // Don't reset the region state or get a new plan any more.
            // This is the last try.
            continue;
          }
        }

        // If region opened on destination of present plan, reassigning to new
        // RS may cause double assignments. In case of RegionAlreadyInTransitionException
        // reassigning to same RS.
        if (needNewPlan) {
          // Force a new plan and reassign. Will return null if no servers.
          // The new plan could be the same as the existing plan since we don't
          // exclude the server of the original plan, which should not be
          // excluded since it could be the only server up now.
          RegionPlan newPlan = null;
          try {
            newPlan = getRegionPlan(region, true);
          } catch (HBaseIOException e) {
            LOG.warn("Failed to get region plan", e);
          }
          if (newPlan == null) {
            regionStates.updateRegionState(region, State.FAILED_OPEN);
            LOG.warn("Unable to find a viable location to assign region " +
                region.getRegionNameAsString());
            return;
          }

          if (plan != newPlan && !plan.getDestination().equals(newPlan.getDestination())) {
            // Clean out plan we failed execute and one that doesn't look like it'll
            // succeed anyways; we need a new plan!
            // Transition back to OFFLINE
            regionStates.updateRegionState(region, State.OFFLINE);
            plan = newPlan;
          } else if(plan.getDestination().equals(newPlan.getDestination()) &&
              previousException instanceof FailedServerException) {
            try {
              LOG.info("Trying to re-assign " + region.getRegionNameAsString() +
                " to the same failed server.");
              Thread.sleep(1 + conf.getInt(RpcClient.FAILED_SERVER_EXPIRY_KEY,
                RpcClient.FAILED_SERVER_EXPIRY_DEFAULT));
            } catch (InterruptedException ie) {
              LOG.warn("Failed to assign "
                  + region.getRegionNameAsString() + " since interrupted", ie);
              regionStates.updateRegionState(region, State.FAILED_OPEN);
              Thread.currentThread().interrupt();
              return;
            }
          }
        }
      }
      // Run out of attempts
      regionStates.updateRegionState(region, State.FAILED_OPEN);
    } finally {
      metricsAssignmentManager.updateAssignmentTime(EnvironmentEdgeManager.currentTime() - startTime);
    }
  }

  private boolean isDisabledorDisablingRegionInRIT(final HRegionInfo region) {
    if (this.tableStateManager.isTableState(region.getTable(),
            TableState.State.DISABLED,
            TableState.State.DISABLING) || replicasToClose.contains(region)) {
      LOG.info("Table " + region.getTable() + " is disabled or disabling;"
        + " skipping assign of " + region.getRegionNameAsString());
      offlineDisabledRegion(region);
      return true;
    }
    return false;
  }

  /**
   * @param region the region to assign
   * @param forceNewPlan If true, then if an existing plan exists, a new plan
   * will be generated.
   * @return Plan for passed <code>region</code> (If none currently, it creates one or
   * if no servers to assign, it returns null).
   */
  private RegionPlan getRegionPlan(final HRegionInfo region,
      final boolean forceNewPlan) throws HBaseIOException {
    // Pickup existing plan or make a new one
    final String encodedName = region.getEncodedName();
    final List<ServerName> destServers =
      serverManager.createDestinationServersList();

    if (destServers.isEmpty()){
      LOG.warn("Can't move " + encodedName +
        ", there is no destination server available.");
      return null;
    }

    RegionPlan randomPlan = null;
    boolean newPlan = false;
    RegionPlan existingPlan;

    synchronized (this.regionPlans) {
      existingPlan = this.regionPlans.get(encodedName);

      if (existingPlan != null && existingPlan.getDestination() != null) {
        LOG.debug("Found an existing plan for " + region.getRegionNameAsString()
          + " destination server is " + existingPlan.getDestination() +
            " accepted as a dest server = " + destServers.contains(existingPlan.getDestination()));
      }

      if (forceNewPlan
          || existingPlan == null
          || existingPlan.getDestination() == null
          || !destServers.contains(existingPlan.getDestination())) {
        newPlan = true;
        randomPlan = new RegionPlan(region, null,
            balancer.randomAssignment(region, destServers));
        if (!region.isMetaTable() && shouldAssignRegionsWithFavoredNodes) {
          List<HRegionInfo> regions = new ArrayList<HRegionInfo>(1);
          regions.add(region);
          try {
            processFavoredNodes(regions);
          } catch (IOException ie) {
            LOG.warn("Ignoring exception in processFavoredNodes " + ie);
          }
        }
        this.regionPlans.put(encodedName, randomPlan);
      }
    }

    if (newPlan) {
      if (randomPlan.getDestination() == null) {
        LOG.warn("Can't find a destination for " + encodedName);
        return null;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("No previous transition plan found (or ignoring " +
          "an existing plan) for " + region.getRegionNameAsString() +
          "; generated random plan=" + randomPlan + "; " + destServers.size() +
          " (online=" + serverManager.getOnlineServers().size() +
          ") available servers, forceNewPlan=" + forceNewPlan);
      }
      return randomPlan;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using pre-existing plan for " +
        region.getRegionNameAsString() + "; plan=" + existingPlan);
    }
    return existingPlan;
  }

  /**
   * Wait for some time before retrying meta table region assignment
   */
  private void waitForRetryingMetaAssignment() {
    try {
      Thread.sleep(this.sleepTimeBeforeRetryingMetaAssignment);
    } catch (InterruptedException e) {
      LOG.error("Got exception while waiting for hbase:meta assignment");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Unassigns the specified region.
   * <p>
   * Updates the RegionState and sends the CLOSE RPC unless region is being
   * split by regionserver; then the unassign fails (silently) because we
   * presume the region being unassigned no longer exists (its been split out
   * of existence). TODO: What to do if split fails and is rolled back and
   * parent is revivified?
   * <p>
   * If a RegionPlan is already set, it will remain.
   *
   * @param region server to be unassigned
   */
  public void unassign(HRegionInfo region) {
    unassign(region, null);
  }


  /**
   * Unassigns the specified region.
   * <p>
   * Updates the RegionState and sends the CLOSE RPC unless region is being
   * split by regionserver; then the unassign fails (silently) because we
   * presume the region being unassigned no longer exists (its been split out
   * of existence). TODO: What to do if split fails and is rolled back and
   * parent is revivified?
   * <p>
   * If a RegionPlan is already set, it will remain.
   *
   * @param region server to be unassigned
   * @param dest the destination server of the region
   */
  public void unassign(HRegionInfo region, ServerName dest) {
    // TODO: Method needs refactoring.  Ugly buried returns throughout.  Beware!
    LOG.debug("Starting unassign of " + region.getRegionNameAsString()
      + " (offlining), current state: " + regionStates.getRegionState(region));

    String encodedName = region.getEncodedName();
    // Grab the state of this region and synchronize on it
    // We need a lock here as we're going to do a put later and we don't want multiple states
    //  creation
    ReentrantLock lock = locker.acquireLock(encodedName);
    RegionState state = regionStates.getRegionTransitionState(encodedName);
    try {
      if (state == null || state.isFailedClose()) {
        if (state == null) {
          // Region is not in transition.
          // We can unassign it only if it's not SPLIT/MERGED.
          state = regionStates.getRegionState(encodedName);
          if (state != null && state.isUnassignable()) {
            LOG.info("Attempting to unassign " + state + ", ignored");
            // Offline region will be reassigned below
            return;
          }
          if (state == null || state.getServerName() == null) {
            // We don't know where the region is, offline it.
            // No need to send CLOSE RPC
            LOG.warn("Attempting to unassign a region not in RegionStates "
              + region.getRegionNameAsString() + ", offlined");
            regionOffline(region);
            return;
          }
        }
        state = regionStates.updateRegionState(
          region, State.PENDING_CLOSE);
      } else if (state.isFailedOpen()) {
        // The region is not open yet
        regionOffline(region);
        return;
      } else {
        LOG.debug("Attempting to unassign " +
          region.getRegionNameAsString() + " but it is " +
          "already in transition (" + state.getState());
        return;
      }

      unassign(region, state.getServerName(), dest);
    } finally {
      lock.unlock();

      // Region is expected to be reassigned afterwards
      if (!replicasToClose.contains(region)
          && regionStates.isRegionInState(region, State.OFFLINE)) {
        assign(region);
      }
    }
  }

  /**
   * Used by unit tests. Return the number of regions opened so far in the life
   * of the master. Increases by one every time the master opens a region
   * @return the counter value of the number of regions opened so far
   */
  public int getNumRegionsOpened() {
    return numRegionsOpened.get();
  }

  /**
   * Waits until the specified region has completed assignment.
   * <p>
   * If the region is already assigned, returns immediately.  Otherwise, method
   * blocks until the region is assigned.
   * @param regionInfo region to wait on assignment for
   * @return true if the region is assigned false otherwise.
   * @throws InterruptedException
   */
  public boolean waitForAssignment(HRegionInfo regionInfo)
      throws InterruptedException {
    ArrayList<HRegionInfo> regionSet = new ArrayList<HRegionInfo>(1);
    regionSet.add(regionInfo);
    return waitForAssignment(regionSet, true, Long.MAX_VALUE);
  }

  /**
   * Waits until the specified region has completed assignment, or the deadline is reached.
   */
  protected boolean waitForAssignment(final Collection<HRegionInfo> regionSet,
      final boolean waitTillAllAssigned, final int reassigningRegions,
      final long minEndTime) throws InterruptedException {
    long deadline = minEndTime + bulkPerRegionOpenTimeGuesstimate * (reassigningRegions + 1);
    return waitForAssignment(regionSet, waitTillAllAssigned, deadline);
  }

  /**
   * Waits until the specified region has completed assignment, or the deadline is reached.
   * @param regionSet set of region to wait on. the set is modified and the assigned regions removed
   * @param waitTillAllAssigned true if we should wait all the regions to be assigned
   * @param deadline the timestamp after which the wait is aborted
   * @return true if all the regions are assigned false otherwise.
   * @throws InterruptedException
   */
  protected boolean waitForAssignment(final Collection<HRegionInfo> regionSet,
      final boolean waitTillAllAssigned, final long deadline) throws InterruptedException {
    // We're not synchronizing on regionsInTransition now because we don't use any iterator.
    while (!regionSet.isEmpty() && !server.isStopped() && deadline > System.currentTimeMillis()) {
      int failedOpenCount = 0;
      Iterator<HRegionInfo> regionInfoIterator = regionSet.iterator();
      while (regionInfoIterator.hasNext()) {
        HRegionInfo hri = regionInfoIterator.next();
        if (regionStates.isRegionOnline(hri) || regionStates.isRegionInState(hri,
            State.SPLITTING, State.SPLIT, State.MERGING, State.MERGED)) {
          regionInfoIterator.remove();
        } else if (regionStates.isRegionInState(hri, State.FAILED_OPEN)) {
          failedOpenCount++;
        }
      }
      if (!waitTillAllAssigned) {
        // No need to wait, let assignment going on asynchronously
        break;
      }
      if (!regionSet.isEmpty()) {
        if (failedOpenCount == regionSet.size()) {
          // all the regions we are waiting had an error on open.
          break;
        }
        regionStates.waitForUpdate(100);
      }
    }
    return regionSet.isEmpty();
  }

  /**
   * Assigns the hbase:meta region or a replica.
   * <p>
   * Assumes that hbase:meta is currently closed and is not being actively served by
   * any RegionServer.
   * @param hri TODO
   */
  public void assignMeta(HRegionInfo hri) throws KeeperException {
    regionStates.updateRegionState(hri, State.OFFLINE);
    assign(hri);
  }

  /**
   * Assigns specified regions retaining assignments, if any.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown
   * @throws InterruptedException
   * @throws IOException
   */
  public void assign(Map<HRegionInfo, ServerName> regions)
        throws IOException, InterruptedException {
    if (regions == null || regions.isEmpty()) {
      return;
    }
    List<ServerName> servers = serverManager.createDestinationServersList();
    if (servers == null || servers.isEmpty()) {
      throw new IOException("Found no destination server to assign region(s)");
    }

    // Reuse existing assignment info
    Map<ServerName, List<HRegionInfo>> bulkPlan =
      balancer.retainAssignment(regions, servers);
    if (bulkPlan == null) {
      throw new IOException("Unable to determine a plan to assign region(s)");
    }

    assign(regions.size(), servers.size(),
      "retainAssignment=true", bulkPlan);
  }

  /**
   * Assigns specified regions round robin, if any.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown
   * @throws InterruptedException
   * @throws IOException
   */
  public void assign(List<HRegionInfo> regions)
        throws IOException, InterruptedException {
    if (regions == null || regions.isEmpty()) {
      return;
    }

    List<ServerName> servers = serverManager.createDestinationServersList();
    if (servers == null || servers.isEmpty()) {
      throw new IOException("Found no destination server to assign region(s)");
    }

    // Generate a round-robin bulk assignment plan
    Map<ServerName, List<HRegionInfo>> bulkPlan = balancer.roundRobinAssignment(regions, servers);
    if (bulkPlan == null) {
      throw new IOException("Unable to determine a plan to assign region(s)");
    }

    processFavoredNodes(regions);
    assign(regions.size(), servers.size(), "round-robin=true", bulkPlan);
  }

  private void assign(int regions, int totalServers,
      String message, Map<ServerName, List<HRegionInfo>> bulkPlan)
          throws InterruptedException, IOException {

    int servers = bulkPlan.size();
    if (servers == 1 || (regions < bulkAssignThresholdRegions
        && servers < bulkAssignThresholdServers)) {

      // Not use bulk assignment.  This could be more efficient in small
      // cluster, especially mini cluster for testing, so that tests won't time out
      if (LOG.isTraceEnabled()) {
        LOG.trace("Not using bulk assignment since we are assigning only " + regions +
          " region(s) to " + servers + " server(s)");
      }

      // invoke assignment (async)
      ArrayList<HRegionInfo> userRegionSet = new ArrayList<HRegionInfo>(regions);
      for (Map.Entry<ServerName, List<HRegionInfo>> plan: bulkPlan.entrySet()) {
        if (!assign(plan.getKey(), plan.getValue()) && !server.isStopped()) {
          for (HRegionInfo region: plan.getValue()) {
            if (!regionStates.isRegionOnline(region)) {
              invokeAssign(region);
              if (!region.getTable().isSystemTable()) {
                userRegionSet.add(region);
              }
            }
          }
        }
      }

      // wait for assignment completion
      if (!waitForAssignment(userRegionSet, true, userRegionSet.size(),
            System.currentTimeMillis())) {
        LOG.debug("some user regions are still in transition: " + userRegionSet);
      }
    } else {
      LOG.info("Bulk assigning " + regions + " region(s) across "
        + totalServers + " server(s), " + message);

      // Use fixed count thread pool assigning.
      BulkAssigner ba = new GeneralBulkAssigner(
        this.server, bulkPlan, this, bulkAssignWaitTillAllAssigned);
      ba.bulkAssign();
      LOG.info("Bulk assigning done");
    }
  }

  /**
   * Assigns all user regions, if any exist.  Used during cluster startup.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown and the cluster
   * should be shutdown.
   * @throws InterruptedException
   * @throws IOException
   */
  private void assignAllUserRegions(Map<HRegionInfo, ServerName> allRegions)
      throws IOException, InterruptedException {
    if (allRegions == null || allRegions.isEmpty()) return;

    // Determine what type of assignment to do on startup
    boolean retainAssignment = server.getConfiguration().
      getBoolean("hbase.master.startup.retainassign", true);

    Set<HRegionInfo> regionsFromMetaScan = allRegions.keySet();
    if (retainAssignment) {
      assign(allRegions);
    } else {
      List<HRegionInfo> regions = new ArrayList<HRegionInfo>(regionsFromMetaScan);
      assign(regions);
    }

    for (HRegionInfo hri : regionsFromMetaScan) {
      TableName tableName = hri.getTable();
      if (!tableStateManager.isTableState(tableName,
              TableState.State.ENABLED)) {
        setEnabledTable(tableName);
      }
    }
    // assign all the replicas that were not recorded in the meta
    assign(replicaRegionsNotRecordedInMeta(regionsFromMetaScan, (MasterServices)server));
  }

  /**
   * Get a list of replica regions that are:
   * not recorded in meta yet. We might not have recorded the locations
   * for the replicas since the replicas may not have been online yet, master restarted
   * in the middle of assigning, ZK erased, etc.
   * @param regionsRecordedInMeta the list of regions we know are recorded in meta
   * either as a default, or, as the location of a replica
   * @param master
   * @return list of replica regions
   * @throws IOException
   */
  public static List<HRegionInfo> replicaRegionsNotRecordedInMeta(
      Set<HRegionInfo> regionsRecordedInMeta, MasterServices master)throws IOException {
    List<HRegionInfo> regionsNotRecordedInMeta = new ArrayList<HRegionInfo>();
    for (HRegionInfo hri : regionsRecordedInMeta) {
      TableName table = hri.getTable();
      HTableDescriptor htd = master.getTableDescriptors().get(table);
      // look at the HTD for the replica count. That's the source of truth
      int desiredRegionReplication = htd.getRegionReplication();
      for (int i = 0; i < desiredRegionReplication; i++) {
        HRegionInfo replica = RegionReplicaUtil.getRegionInfoForReplica(hri, i);
        if (regionsRecordedInMeta.contains(replica)) continue;
        regionsNotRecordedInMeta.add(replica);
      }
    }
    return regionsNotRecordedInMeta;
  }

  /**
   * Rebuild the list of user regions and assignment information.
   * Updates regionstates with findings as we go through list of regions.
   * @return set of servers not online that hosted some regions according to a scan of hbase:meta
   * @throws IOException
   */
  Set<ServerName> rebuildUserRegions() throws
          IOException, KeeperException {
    Set<TableName> disabledOrEnablingTables = tableStateManager.getTablesInStates(
            TableState.State.DISABLED, TableState.State.ENABLING);

    Set<TableName> disabledOrDisablingOrEnabling = tableStateManager.getTablesInStates(
            TableState.State.DISABLED,
            TableState.State.DISABLING,
            TableState.State.ENABLING);

    // Region assignment from META
    List<Result> results = MetaTableAccessor.fullScanRegions(server.getConnection());
    // Get any new but slow to checkin region server that joined the cluster
    Set<ServerName> onlineServers = serverManager.getOnlineServers().keySet();
    // Set of offline servers to be returned
    Set<ServerName> offlineServers = new HashSet<ServerName>();
    // Iterate regions in META
    for (Result result : results) {
      if (result == null && LOG.isDebugEnabled()){
        LOG.debug("null result from meta - ignoring but this is strange.");
        continue;
      }
      // keep a track of replicas to close. These were the replicas of the originally
      // unmerged regions. The master might have closed them before but it mightn't
      // maybe because it crashed.
      PairOfSameType<HRegionInfo> p = MetaTableAccessor.getMergeRegions(result);
      if (p.getFirst() != null && p.getSecond() != null) {
        int numReplicas = ((MasterServices)server).getTableDescriptors().get(p.getFirst().
            getTable()).getRegionReplication();
        for (HRegionInfo merge : p) {
          for (int i = 1; i < numReplicas; i++) {
            replicasToClose.add(RegionReplicaUtil.getRegionInfoForReplica(merge, i));
          }
        }
      }
      RegionLocations rl =  MetaTableAccessor.getRegionLocations(result);
      if (rl == null) continue;
      HRegionLocation[] locations = rl.getRegionLocations();
      if (locations == null) continue;
      for (HRegionLocation hrl : locations) {
        if (hrl == null) continue;
        HRegionInfo regionInfo = hrl.getRegionInfo();
        if (regionInfo == null) continue;
        int replicaId = regionInfo.getReplicaId();
        State state = RegionStateStore.getRegionState(result, replicaId);
        // keep a track of replicas to close. These were the replicas of the split parents
        // from the previous life of the master. The master should have closed them before
        // but it couldn't maybe because it crashed
        if (replicaId == 0 && state.equals(State.SPLIT)) {
          for (HRegionLocation h : locations) {
            replicasToClose.add(h.getRegionInfo());
          }
        }
        ServerName lastHost = hrl.getServerName();
        ServerName regionLocation = RegionStateStore.getRegionServer(result, replicaId);
        regionStates.createRegionState(regionInfo, state, regionLocation, lastHost);
        if (!regionStates.isRegionInState(regionInfo, State.OPEN)) {
          // Region is not open (either offline or in transition), skip
          continue;
        }
        TableName tableName = regionInfo.getTable();
        if (!onlineServers.contains(regionLocation)) {
          // Region is located on a server that isn't online
          offlineServers.add(regionLocation);
        } else if (!disabledOrEnablingTables.contains(tableName)) {
          // Region is being served and on an active server
          // add only if region not in disabled or enabling table
          regionStates.regionOnline(regionInfo, regionLocation);
          balancer.regionOnline(regionInfo, regionLocation);
        }
        // need to enable the table if not disabled or disabling or enabling
        // this will be used in rolling restarts
        if (!disabledOrDisablingOrEnabling.contains(tableName)
          && !getTableStateManager().isTableState(tableName,
                TableState.State.ENABLED)) {
          setEnabledTable(tableName);
        }
      }
    }
    return offlineServers;
  }

  /**
   * Recover the tables that were not fully moved to DISABLED state. These
   * tables are in DISABLING state when the master restarted/switched.
   *
   * @throws KeeperException
   * @throws TableNotFoundException
   * @throws IOException
   */
  private void recoverTableInDisablingState()
          throws KeeperException, IOException {
    Set<TableName> disablingTables =
            tableStateManager.getTablesInStates(TableState.State.DISABLING);
    if (disablingTables.size() != 0) {
      for (TableName tableName : disablingTables) {
        // Recover by calling DisableTableHandler
        LOG.info("The table " + tableName
            + " is in DISABLING state.  Hence recovering by moving the table"
            + " to DISABLED state.");
        new DisableTableHandler(this.server, tableName,
            this, tableLockManager, true).prepare().process();
      }
    }
  }

  /**
   * Recover the tables that are not fully moved to ENABLED state. These tables
   * are in ENABLING state when the master restarted/switched
   *
   * @throws KeeperException
   * @throws org.apache.hadoop.hbase.TableNotFoundException
   * @throws IOException
   */
  private void recoverTableInEnablingState()
          throws KeeperException, IOException {
    Set<TableName> enablingTables = tableStateManager.
            getTablesInStates(TableState.State.ENABLING);
    if (enablingTables.size() != 0) {
      for (TableName tableName : enablingTables) {
        // Recover by calling EnableTableHandler
        LOG.info("The table " + tableName
            + " is in ENABLING state.  Hence recovering by moving the table"
            + " to ENABLED state.");
        // enableTable in sync way during master startup,
        // no need to invoke coprocessor
        EnableTableHandler eth = new EnableTableHandler(this.server, tableName,
          this, tableLockManager, true);
        try {
          eth.prepare();
        } catch (TableNotFoundException e) {
          LOG.warn("Table " + tableName + " not found in hbase:meta to recover.");
          continue;
        }
        eth.process();
      }
    }
  }

  /**
   * Processes list of regions in transition at startup
   */
  void processRegionsInTransition(Collection<RegionState> regionsInTransition) {
    // We need to send RPC call again for PENDING_OPEN/PENDING_CLOSE regions
    // in case the RPC call is not sent out yet before the master was shut down
    // since we update the state before we send the RPC call. We can't update
    // the state after the RPC call. Otherwise, we don't know what's happened
    // to the region if the master dies right after the RPC call is out.
    for (RegionState regionState: regionsInTransition) {
      LOG.info("Processing " + regionState);
      ServerName serverName = regionState.getServerName();
      // Server could be null in case of FAILED_OPEN when master cannot find a region plan. In that
      // case, try assigning it here.
      if (serverName != null && !serverManager.getOnlineServers().containsKey(serverName)) {
        LOG.info("Server " + serverName + " isn't online. SSH will handle this");
        continue; // SSH will handle it
      }
      HRegionInfo regionInfo = regionState.getRegion();
      RegionState.State state = regionState.getState();
      switch (state) {
      case CLOSED:
        invokeAssign(regionState.getRegion());
        break;
      case PENDING_OPEN:
        retrySendRegionOpen(regionState);
        break;
      case PENDING_CLOSE:
        retrySendRegionClose(regionState);
        break;
      case FAILED_CLOSE:
      case FAILED_OPEN:
        invokeUnAssign(regionInfo);
        break;
      default:
        // No process for other states
      }
    }
  }

  /**
   * At master failover, for pending_open region, make sure
   * sendRegionOpen RPC call is sent to the target regionserver
   */
  private void retrySendRegionOpen(final RegionState regionState) {
    this.executorService.submit(
      new EventHandler(server, EventType.M_MASTER_RECOVERY) {
        @Override
        public void process() throws IOException {
          HRegionInfo hri = regionState.getRegion();
          ServerName serverName = regionState.getServerName();
          ReentrantLock lock = locker.acquireLock(hri.getEncodedName());
          try {
            for (int i = 1; i <= maximumAttempts; i++) {
              if (!serverManager.isServerOnline(serverName)
                  || server.isStopped() || server.isAborted()) {
                return; // No need any more
              }
              try {
                if (!regionState.equals(regionStates.getRegionState(hri))) {
                  return; // Region is not in the expected state any more
                }
                List<ServerName> favoredNodes = ServerName.EMPTY_SERVER_LIST;
                if (shouldAssignRegionsWithFavoredNodes) {
                  favoredNodes = ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(hri);
                }
                serverManager.sendRegionOpen(serverName, hri, favoredNodes);
                return; // we're done
              } catch (Throwable t) {
                if (t instanceof RemoteException) {
                  t = ((RemoteException) t).unwrapRemoteException();
                }
                if (t instanceof FailedServerException && i < maximumAttempts) {
                  // In case the server is in the failed server list, no point to
                  // retry too soon. Retry after the failed_server_expiry time
                  try {
                    Configuration conf = this.server.getConfiguration();
                    long sleepTime = 1 + conf.getInt(RpcClient.FAILED_SERVER_EXPIRY_KEY,
                      RpcClient.FAILED_SERVER_EXPIRY_DEFAULT);
                    if (LOG.isDebugEnabled()) {
                      LOG.debug(serverName + " is on failed server list; waiting "
                        + sleepTime + "ms", t);
                    }
                    Thread.sleep(sleepTime);
                    continue;
                  } catch (InterruptedException ie) {
                    LOG.warn("Failed to assign "
                      + hri.getRegionNameAsString() + " since interrupted", ie);
                    regionStates.updateRegionState(hri, State.FAILED_OPEN);
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
                if (serverManager.isServerOnline(serverName)
                    && t instanceof java.net.SocketTimeoutException) {
                  i--; // reset the try count
                } else {
                  LOG.info("Got exception in retrying sendRegionOpen for "
                    + regionState + "; try=" + i + " of " + maximumAttempts, t);
                }
                Threads.sleep(100);
              }
            }
            // Run out of attempts
            regionStates.updateRegionState(hri, State.FAILED_OPEN);
          } finally {
            lock.unlock();
          }
        }
      });
  }

  /**
   * At master failover, for pending_close region, make sure
   * sendRegionClose RPC call is sent to the target regionserver
   */
  private void retrySendRegionClose(final RegionState regionState) {
    this.executorService.submit(
      new EventHandler(server, EventType.M_MASTER_RECOVERY) {
        @Override
        public void process() throws IOException {
          HRegionInfo hri = regionState.getRegion();
          ServerName serverName = regionState.getServerName();
          ReentrantLock lock = locker.acquireLock(hri.getEncodedName());
          try {
            for (int i = 1; i <= maximumAttempts; i++) {
              if (!serverManager.isServerOnline(serverName)
                  || server.isStopped() || server.isAborted()) {
                return; // No need any more
              }
              try {
                if (!regionState.equals(regionStates.getRegionState(hri))) {
                  return; // Region is not in the expected state any more
                }
                serverManager.sendRegionClose(serverName, hri, null);
                return; // Done.
              } catch (Throwable t) {
                if (t instanceof RemoteException) {
                  t = ((RemoteException) t).unwrapRemoteException();
                }
                if (t instanceof FailedServerException && i < maximumAttempts) {
                  // In case the server is in the failed server list, no point to
                  // retry too soon. Retry after the failed_server_expiry time
                  try {
                    Configuration conf = this.server.getConfiguration();
                    long sleepTime = 1 + conf.getInt(RpcClient.FAILED_SERVER_EXPIRY_KEY,
                      RpcClient.FAILED_SERVER_EXPIRY_DEFAULT);
                    if (LOG.isDebugEnabled()) {
                      LOG.debug(serverName + " is on failed server list; waiting "
                        + sleepTime + "ms", t);
                    }
                    Thread.sleep(sleepTime);
                    continue;
                  } catch (InterruptedException ie) {
                    LOG.warn("Failed to unassign "
                      + hri.getRegionNameAsString() + " since interrupted", ie);
                    regionStates.updateRegionState(hri, RegionState.State.FAILED_CLOSE);
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
                if (serverManager.isServerOnline(serverName)
                    && t instanceof java.net.SocketTimeoutException) {
                  i--; // reset the try count
                } else {
                  LOG.info("Got exception in retrying sendRegionClose for "
                    + regionState + "; try=" + i + " of " + maximumAttempts, t);
                }
                Threads.sleep(100);
              }
            }
            // Run out of attempts
            regionStates.updateRegionState(hri, State.FAILED_CLOSE);
          } finally {
            lock.unlock();
          }
        }
      });
  }

  /**
   * Set Regions in transitions metrics.
   * This takes an iterator on the RegionInTransition map (CLSM), and is not synchronized.
   * This iterator is not fail fast, which may lead to stale read; but that's better than
   * creating a copy of the map for metrics computation, as this method will be invoked
   * on a frequent interval.
   */
  public void updateRegionsInTransitionMetrics() {
    long currentTime = System.currentTimeMillis();
    int totalRITs = 0;
    int totalRITsOverThreshold = 0;
    long oldestRITTime = 0;
    int ritThreshold = this.server.getConfiguration().
      getInt(HConstants.METRICS_RIT_STUCK_WARNING_THRESHOLD, 60000);
    for (RegionState state: regionStates.getRegionsInTransition().values()) {
      totalRITs++;
      long ritTime = currentTime - state.getStamp();
      if (ritTime > ritThreshold) { // more than the threshold
        totalRITsOverThreshold++;
      }
      if (oldestRITTime < ritTime) {
        oldestRITTime = ritTime;
      }
    }
    if (this.metricsAssignmentManager != null) {
      this.metricsAssignmentManager.updateRITOldestAge(oldestRITTime);
      this.metricsAssignmentManager.updateRITCount(totalRITs);
      this.metricsAssignmentManager.updateRITCountOverThreshold(totalRITsOverThreshold);
    }
  }

  /**
   * @param region Region whose plan we are to clear.
   */
  private void clearRegionPlan(final HRegionInfo region) {
    synchronized (this.regionPlans) {
      this.regionPlans.remove(region.getEncodedName());
    }
  }

  /**
   * Wait on region to clear regions-in-transition.
   * @param hri Region to wait on.
   * @throws IOException
   */
  public void waitOnRegionToClearRegionsInTransition(final HRegionInfo hri)
      throws IOException, InterruptedException {
    waitOnRegionToClearRegionsInTransition(hri, -1L);
  }

  /**
   * Wait on region to clear regions-in-transition or time out
   * @param hri
   * @param timeOut Milliseconds to wait for current region to be out of transition state.
   * @return True when a region clears regions-in-transition before timeout otherwise false
   * @throws InterruptedException
   */
  public boolean waitOnRegionToClearRegionsInTransition(final HRegionInfo hri, long timeOut)
      throws InterruptedException {
    if (!regionStates.isRegionInTransition(hri)) return true;
    long end = (timeOut <= 0) ? Long.MAX_VALUE : EnvironmentEdgeManager.currentTime()
        + timeOut;
    // There is already a timeout monitor on regions in transition so I
    // should not have to have one here too?
    LOG.info("Waiting for " + hri.getEncodedName() +
        " to leave regions-in-transition, timeOut=" + timeOut + " ms.");
    while (!this.server.isStopped() && regionStates.isRegionInTransition(hri)) {
      regionStates.waitForUpdate(100);
      if (EnvironmentEdgeManager.currentTime() > end) {
        LOG.info("Timed out on waiting for " + hri.getEncodedName() + " to be assigned.");
        return false;
      }
    }
    if (this.server.isStopped()) {
      LOG.info("Giving up wait on regions in transition because stoppable.isStopped is set");
      return false;
    }
    return true;
  }

  void invokeAssign(HRegionInfo regionInfo) {
    threadPoolExecutorService.submit(new AssignCallable(this, regionInfo));
  }

  void invokeUnAssign(HRegionInfo regionInfo) {
    threadPoolExecutorService.submit(new UnAssignCallable(this, regionInfo));
  }

  public boolean isCarryingMeta(ServerName serverName) {
    return isCarryingRegion(serverName, HRegionInfo.FIRST_META_REGIONINFO);
  }

  public boolean isCarryingMetaReplica(ServerName serverName, int replicaId) {
    return isCarryingRegion(serverName,
        RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO, replicaId));
  }

  public boolean isCarryingMetaReplica(ServerName serverName, HRegionInfo metaHri) {
    return isCarryingRegion(serverName, metaHri);
  }

  /**
   * Check if the shutdown server carries the specific region.
   * @return whether the serverName currently hosts the region
   */
  private boolean isCarryingRegion(ServerName serverName, HRegionInfo hri) {
    RegionState regionState = regionStates.getRegionTransitionState(hri);
    ServerName transitionAddr = regionState != null? regionState.getServerName(): null;
    if (transitionAddr != null) {
      boolean matchTransitionAddr = transitionAddr.equals(serverName);
      LOG.debug("Checking region=" + hri.getRegionNameAsString()
        + ", transitioning on server=" + matchTransitionAddr
        + " server being checked: " + serverName
        + ", matches=" + matchTransitionAddr);
      return matchTransitionAddr;
    }

    ServerName assignedAddr = regionStates.getRegionServerOfRegion(hri);
    boolean matchAssignedAddr = serverName.equals(assignedAddr);
    LOG.debug("based on AM, current region=" + hri.getRegionNameAsString()
      + " is on server=" + assignedAddr + ", server being checked: "
      + serverName);
    return matchAssignedAddr;
  }

  /**
   * Clean out crashed server removing any assignments.
   * @param sn Server that went down.
   * @return list of regions in transition on this server
   */
  public List<HRegionInfo> cleanOutCrashedServerReferences(final ServerName sn) {
    // Clean out any existing assignment plans for this server
    synchronized (this.regionPlans) {
      for (Iterator <Map.Entry<String, RegionPlan>> i = this.regionPlans.entrySet().iterator();
          i.hasNext();) {
        Map.Entry<String, RegionPlan> e = i.next();
        ServerName otherSn = e.getValue().getDestination();
        // The name will be null if the region is planned for a random assign.
        if (otherSn != null && otherSn.equals(sn)) {
          // Use iterator's remove else we'll get CME
          i.remove();
        }
      }
    }
    List<HRegionInfo> rits = regionStates.serverOffline(sn);
    for (Iterator<HRegionInfo> it = rits.iterator(); it.hasNext(); ) {
      HRegionInfo hri = it.next();
      String encodedName = hri.getEncodedName();

      // We need a lock on the region as we could update it
      Lock lock = locker.acquireLock(encodedName);
      try {
        RegionState regionState = regionStates.getRegionTransitionState(encodedName);
        if (regionState == null
            || (regionState.getServerName() != null && !regionState.isOnServer(sn))
            || !RegionStates.isOneOfStates(regionState, State.PENDING_OPEN,
                State.OPENING, State.FAILED_OPEN, State.FAILED_CLOSE, State.OFFLINE)) {
          LOG.info("Skip " + regionState + " since it is not opening/failed_close"
            + " on the dead server any more: " + sn);
          it.remove();
        } else {
          if (tableStateManager.isTableState(hri.getTable(),
                  TableState.State.DISABLED, TableState.State.DISABLING)) {
            regionStates.regionOffline(hri);
            it.remove();
            continue;
          }
          // Mark the region offline and assign it again by SSH
          regionStates.updateRegionState(hri, State.OFFLINE);
        }
      } finally {
        lock.unlock();
      }
    }
    return rits;
  }

  /**
   * @param plan Plan to execute.
   */
  public void balance(final RegionPlan plan) {

    HRegionInfo hri = plan.getRegionInfo();
    TableName tableName = hri.getTable();
    if (tableStateManager.isTableState(tableName,
            TableState.State.DISABLED, TableState.State.DISABLING)) {
      LOG.info("Ignored moving region of disabling/disabled table "
        + tableName);
      return;
    }

    // Move the region only if it's assigned
    String encodedName = hri.getEncodedName();
    ReentrantLock lock = locker.acquireLock(encodedName);
    try {
      if (!regionStates.isRegionOnline(hri)) {
        RegionState state = regionStates.getRegionState(encodedName);
        LOG.info("Ignored moving region not assigned: " + hri + ", "
          + (state == null ? "not in region states" : state));
        return;
      }
      synchronized (this.regionPlans) {
        this.regionPlans.put(plan.getRegionName(), plan);
      }
      unassign(hri, plan.getDestination());
    } finally {
      lock.unlock();
    }
  }

  public void stop() {
    // Shutdown the threadpool executor service
    threadPoolExecutorService.shutdownNow();
    regionStateStore.stop();
  }

  protected void setEnabledTable(TableName tableName) {
    try {
      this.tableStateManager.setTableState(tableName,
              TableState.State.ENABLED);
    } catch (IOException e) {
      // here we can abort as it is the start up flow
      String errorMsg = "Unable to ensure that the table " + tableName
          + " will be" + " enabled because of a ZooKeeper issue";
      LOG.error(errorMsg);
      this.server.abort(errorMsg, e);
    }
  }

  private String onRegionFailedOpen(final RegionState current,
      final HRegionInfo hri, final ServerName serverName) {
    // The region must be opening on this server.
    // If current state is failed_open on the same server,
    // it could be a reportRegionTransition RPC retry.
    if (current == null || !current.isOpeningOrFailedOpenOnServer(serverName)) {
      return hri.getShortNameToLog() + " is not opening on " + serverName;
    }

    // Just return in case of retrying
    if (current.isFailedOpen()) {
      return null;
    }

    String encodedName = hri.getEncodedName();
    AtomicInteger failedOpenCount = failedOpenTracker.get(encodedName);
    if (failedOpenCount == null) {
      failedOpenCount = new AtomicInteger();
      // No need to use putIfAbsent, or extra synchronization since
      // this whole handleRegion block is locked on the encoded region
      // name, and failedOpenTracker is updated only in this block
      failedOpenTracker.put(encodedName, failedOpenCount);
    }
    if (failedOpenCount.incrementAndGet() >= maximumAttempts && !hri.isMetaRegion()) {
      regionStates.updateRegionState(hri, State.FAILED_OPEN);
      // remove the tracking info to save memory, also reset
      // the count for next open initiative
      failedOpenTracker.remove(encodedName);
    } else {
      if (hri.isMetaRegion() && failedOpenCount.get() >= maximumAttempts) {
        // Log a warning message if a meta region failedOpenCount exceeds maximumAttempts
        // so that we are aware of potential problem if it persists for a long time.
        LOG.warn("Failed to open the hbase:meta region " +
            hri.getRegionNameAsString() + " after" +
            failedOpenCount.get() + " retries. Continue retrying.");
      }

      // Handle this the same as if it were opened and then closed.
      RegionState regionState = regionStates.updateRegionState(hri, State.CLOSED);
      if (regionState != null) {
        // When there are more than one region server a new RS is selected as the
        // destination and the same is updated in the region plan. (HBASE-5546)
        if (getTableStateManager().isTableState(hri.getTable(),
                TableState.State.DISABLED, TableState.State.DISABLING) ||
                replicasToClose.contains(hri)) {
          offlineDisabledRegion(hri);
          return null;
        }
        regionStates.updateRegionState(hri, RegionState.State.CLOSED);
        // This below has to do w/ online enable/disable of a table
        removeClosedRegion(hri);
        try {
          getRegionPlan(hri, true);
        } catch (HBaseIOException e) {
          LOG.warn("Failed to get region plan", e);
        }
        invokeAssign(hri);
      }
    }
    // Null means no error
    return null;
  }

  private String onRegionOpen(final RegionState current, final HRegionInfo hri,
      final ServerName serverName, final RegionStateTransition transition) {
    // The region must be opening on this server.
    // If current state is already opened on the same server,
    // it could be a reportRegionTransition RPC retry.
    if (current == null || !current.isOpeningOrOpenedOnServer(serverName)) {
      return hri.getShortNameToLog() + " is not opening on " + serverName;
    }

    // Just return in case of retrying
    if (current.isOpened()) {
      return null;
    }

    long openSeqNum = transition.hasOpenSeqNum()
      ? transition.getOpenSeqNum() : HConstants.NO_SEQNUM;
    if (openSeqNum < 0) {
      return "Newly opened region has invalid open seq num " + openSeqNum;
    }
    regionOnline(hri, serverName, openSeqNum);

    // reset the count, if any
    failedOpenTracker.remove(hri.getEncodedName());
    if (getTableStateManager().isTableState(hri.getTable(),
            TableState.State.DISABLED, TableState.State.DISABLING)) {
      invokeUnAssign(hri);
    }
    return null;
  }

  private String onRegionClosed(final RegionState current,
      final HRegionInfo hri, final ServerName serverName) {
    // Region will be usually assigned right after closed. When a RPC retry comes
    // in, the region may already have moved away from closed state. However, on the
    // region server side, we don't care much about the response for this transition.
    // We only make sure master has got and processed this report, either
    // successfully or not. So this is fine, not a problem at all.
    if (current == null || !current.isClosingOrClosedOnServer(serverName)) {
      return hri.getShortNameToLog() + " is not closing on " + serverName;
    }

    // Just return in case of retrying
    if (current.isClosed()) {
      return null;
    }

    if (getTableStateManager().isTableState(hri.getTable(), TableState.State.DISABLED,
        TableState.State.DISABLING) || replicasToClose.contains(hri)) {
      offlineDisabledRegion(hri);
      return null;
    }

    regionStates.updateRegionState(hri, RegionState.State.CLOSED);
    sendRegionClosedNotification(hri);
    // This below has to do w/ online enable/disable of a table
    removeClosedRegion(hri);
    invokeAssign(hri);
    return null;
  }

  private String onRegionReadyToSplit(final RegionState current, final HRegionInfo hri,
      final ServerName serverName, final RegionStateTransition transition) {
    // The region must be opened on this server.
    // If current state is already splitting on the same server,
    // it could be a reportRegionTransition RPC retry.
    if (current == null || !current.isSplittingOrOpenedOnServer(serverName)) {
      return hri.getShortNameToLog() + " is not opening on " + serverName;
    }

    // Just return in case of retrying
    if (current.isSplitting()) {
      return null;
    }

    final HRegionInfo a = HRegionInfo.convert(transition.getRegionInfo(1));
    final HRegionInfo b = HRegionInfo.convert(transition.getRegionInfo(2));
    RegionState rs_a = regionStates.getRegionState(a);
    RegionState rs_b = regionStates.getRegionState(b);
    if (rs_a != null || rs_b != null) {
      return "Some daughter is already existing. "
        + "a=" + rs_a + ", b=" + rs_b;
    }

    // Server holding is not updated at this stage.
    // It is done after PONR.
    regionStates.updateRegionState(hri, State.SPLITTING);
    regionStates.createRegionState(
      a, State.SPLITTING_NEW, serverName, null);
    regionStates.createRegionState(
      b, State.SPLITTING_NEW, serverName, null);
    return null;
  }

  private String onRegionSplitPONR(final RegionState current, final HRegionInfo hri,
      final ServerName serverName, final RegionStateTransition transition) {
    // The region must be splitting on this server, and the daughters must be in
    // splitting_new state. To check RPC retry, we use server holding info.
    if (current == null || !current.isSplittingOnServer(serverName)) {
      return hri.getShortNameToLog() + " is not splitting on " + serverName;
    }

    final HRegionInfo a = HRegionInfo.convert(transition.getRegionInfo(1));
    final HRegionInfo b = HRegionInfo.convert(transition.getRegionInfo(2));
    RegionState rs_a = regionStates.getRegionState(a);
    RegionState rs_b = regionStates.getRegionState(b);

    // Master could have restarted and lost the new region
    // states, if so, they must be lost together
    if (rs_a == null && rs_b == null) {
      rs_a = regionStates.createRegionState(
        a, State.SPLITTING_NEW, serverName, null);
      rs_b = regionStates.createRegionState(
        b, State.SPLITTING_NEW, serverName, null);
    }

    if (rs_a == null || !rs_a.isSplittingNewOnServer(serverName)
        || rs_b == null || !rs_b.isSplittingNewOnServer(serverName)) {
      return "Some daughter is not known to be splitting on " + serverName
        + ", a=" + rs_a + ", b=" + rs_b;
    }

    // Just return in case of retrying
    if (!regionStates.isRegionOnServer(hri, serverName)) {
      return null;
    }

    try {
      regionStates.splitRegion(hri, a, b, serverName);
    } catch (IOException ioe) {
      LOG.info("Failed to record split region " + hri.getShortNameToLog());
      return "Failed to record the splitting in meta";
    }
    return null;
  }

  private String onRegionSplit(final RegionState current, final HRegionInfo hri,
      final ServerName serverName, final RegionStateTransition transition) {
    // The region must be splitting on this server, and the daughters must be in
    // splitting_new state.
    // If current state is already split on the same server,
    // it could be a reportRegionTransition RPC retry.
    if (current == null || !current.isSplittingOrSplitOnServer(serverName)) {
      return hri.getShortNameToLog() + " is not splitting on " + serverName;
    }

    // Just return in case of retrying
    if (current.isSplit()) {
      return null;
    }

    final HRegionInfo a = HRegionInfo.convert(transition.getRegionInfo(1));
    final HRegionInfo b = HRegionInfo.convert(transition.getRegionInfo(2));
    RegionState rs_a = regionStates.getRegionState(a);
    RegionState rs_b = regionStates.getRegionState(b);
    if (rs_a == null || !rs_a.isSplittingNewOnServer(serverName)
        || rs_b == null || !rs_b.isSplittingNewOnServer(serverName)) {
      return "Some daughter is not known to be splitting on " + serverName
        + ", a=" + rs_a + ", b=" + rs_b;
    }

    if (TEST_SKIP_SPLIT_HANDLING) {
      return "Skipping split message, TEST_SKIP_SPLIT_HANDLING is set";
    }
    regionOffline(hri, State.SPLIT);
    regionOnline(a, serverName, 1);
    regionOnline(b, serverName, 1);

    // User could disable the table before master knows the new region.
    if (getTableStateManager().isTableState(hri.getTable(),
        TableState.State.DISABLED, TableState.State.DISABLING)) {
      invokeUnAssign(a);
      invokeUnAssign(b);
    } else {
      Callable<Object> splitReplicasCallable = new Callable<Object>() {
        @Override
        public Object call() {
          doSplittingOfReplicas(hri, a, b);
          return null;
        }
      };
      threadPoolExecutorService.submit(splitReplicasCallable);
    }
    return null;
  }

  private String onRegionSplitReverted(final RegionState current, final HRegionInfo hri,
      final ServerName serverName, final RegionStateTransition transition) {
    // The region must be splitting on this server, and the daughters must be in
    // splitting_new state.
    // If the region is in open state, it could be an RPC retry.
    if (current == null || !current.isSplittingOrOpenedOnServer(serverName)) {
      return hri.getShortNameToLog() + " is not splitting on " + serverName;
    }

    // Just return in case of retrying
    if (current.isOpened()) {
      return null;
    }

    final HRegionInfo a = HRegionInfo.convert(transition.getRegionInfo(1));
    final HRegionInfo b = HRegionInfo.convert(transition.getRegionInfo(2));
    RegionState rs_a = regionStates.getRegionState(a);
    RegionState rs_b = regionStates.getRegionState(b);
    if (rs_a == null || !rs_a.isSplittingNewOnServer(serverName)
        || rs_b == null || !rs_b.isSplittingNewOnServer(serverName)) {
      return "Some daughter is not known to be splitting on " + serverName
        + ", a=" + rs_a + ", b=" + rs_b;
    }

    regionOnline(hri, serverName);
    regionOffline(a);
    regionOffline(b);
    if (getTableStateManager().isTableState(hri.getTable(),
        TableState.State.DISABLED, TableState.State.DISABLING)) {
      invokeUnAssign(hri);
    }
    return null;
  }

  private String onRegionReadyToMerge(final RegionState current, final HRegionInfo hri,
      final ServerName serverName, final RegionStateTransition transition) {
    // The region must be new, and the daughters must be open on this server.
    // If the region is in merge_new state, it could be an RPC retry.
    if (current != null && !current.isMergingNewOnServer(serverName)) {
      return "Merging daughter region already exists, p=" + current;
    }

    // Just return in case of retrying
    if (current != null) {
      return null;
    }

    final HRegionInfo a = HRegionInfo.convert(transition.getRegionInfo(1));
    final HRegionInfo b = HRegionInfo.convert(transition.getRegionInfo(2));
    Set<String> encodedNames = new HashSet<String>(2);
    encodedNames.add(a.getEncodedName());
    encodedNames.add(b.getEncodedName());
    Map<String, Lock> locks = locker.acquireLocks(encodedNames);
    try {
      RegionState rs_a = regionStates.getRegionState(a);
      RegionState rs_b = regionStates.getRegionState(b);
      if (rs_a == null || !rs_a.isOpenedOnServer(serverName)
          || rs_b == null || !rs_b.isOpenedOnServer(serverName)) {
        return "Some daughter is not in a state to merge on " + serverName
          + ", a=" + rs_a + ", b=" + rs_b;
      }

      regionStates.updateRegionState(a, State.MERGING);
      regionStates.updateRegionState(b, State.MERGING);
      regionStates.createRegionState(
        hri, State.MERGING_NEW, serverName, null);
      return null;
    } finally {
      for (Lock lock: locks.values()) {
        lock.unlock();
      }
    }
  }

  private String onRegionMergePONR(final RegionState current, final HRegionInfo hri,
      final ServerName serverName, final RegionStateTransition transition) {
    // The region must be in merging_new state, and the daughters must be
    // merging. To check RPC retry, we use server holding info.
    if (current != null && !current.isMergingNewOnServer(serverName)) {
      return hri.getShortNameToLog() + " is not merging on " + serverName;
    }

    final HRegionInfo a = HRegionInfo.convert(transition.getRegionInfo(1));
    final HRegionInfo b = HRegionInfo.convert(transition.getRegionInfo(2));
    RegionState rs_a = regionStates.getRegionState(a);
    RegionState rs_b = regionStates.getRegionState(b);
    if (rs_a == null || !rs_a.isMergingOnServer(serverName)
        || rs_b == null || !rs_b.isMergingOnServer(serverName)) {
      return "Some daughter is not known to be merging on " + serverName
        + ", a=" + rs_a + ", b=" + rs_b;
    }

    // Master could have restarted and lost the new region state
    if (current == null) {
      regionStates.createRegionState(
        hri, State.MERGING_NEW, serverName, null);
    }

    // Just return in case of retrying
    if (regionStates.isRegionOnServer(hri, serverName)) {
      return null;
    }

    try {
      regionStates.mergeRegions(hri, a, b, serverName);
    } catch (IOException ioe) {
      LOG.info("Failed to record merged region " + hri.getShortNameToLog());
      return "Failed to record the merging in meta";
    }
    return null;
  }

  private String onRegionMerged(final RegionState current, final HRegionInfo hri,
      final ServerName serverName, final RegionStateTransition transition) {
    // The region must be in merging_new state, and the daughters must be
    // merging on this server.
    // If current state is already opened on the same server,
    // it could be a reportRegionTransition RPC retry.
    if (current == null || !current.isMergingNewOrOpenedOnServer(serverName)) {
      return hri.getShortNameToLog() + " is not merging on " + serverName;
    }

    // Just return in case of retrying
    if (current.isOpened()) {
      return null;
    }

    final HRegionInfo a = HRegionInfo.convert(transition.getRegionInfo(1));
    final HRegionInfo b = HRegionInfo.convert(transition.getRegionInfo(2));
    RegionState rs_a = regionStates.getRegionState(a);
    RegionState rs_b = regionStates.getRegionState(b);
    if (rs_a == null || !rs_a.isMergingOnServer(serverName)
        || rs_b == null || !rs_b.isMergingOnServer(serverName)) {
      return "Some daughter is not known to be merging on " + serverName
        + ", a=" + rs_a + ", b=" + rs_b;
    }

    regionOffline(a, State.MERGED);
    regionOffline(b, State.MERGED);
    regionOnline(hri, serverName, 1);

    // User could disable the table before master knows the new region.
    if (getTableStateManager().isTableState(hri.getTable(),
        TableState.State.DISABLED, TableState.State.DISABLING)) {
      invokeUnAssign(hri);
    } else {
      Callable<Object> mergeReplicasCallable = new Callable<Object>() {
        @Override
        public Object call() {
          doMergingOfReplicas(hri, a, b);
          return null;
        }
      };
      threadPoolExecutorService.submit(mergeReplicasCallable);
    }
    return null;
  }

  private String onRegionMergeReverted(final RegionState current, final HRegionInfo hri,
      final ServerName serverName, final RegionStateTransition transition) {
    // The region must be in merging_new state, and the daughters must be
    // merging on this server.
    // If the region is in offline state, it could be an RPC retry.
    if (current == null || !current.isMergingNewOrOfflineOnServer(serverName)) {
      return hri.getShortNameToLog() + " is not merging on " + serverName;
    }

    // Just return in case of retrying
    if (current.isOffline()) {
      return null;
    }

    final HRegionInfo a = HRegionInfo.convert(transition.getRegionInfo(1));
    final HRegionInfo b = HRegionInfo.convert(transition.getRegionInfo(2));
    RegionState rs_a = regionStates.getRegionState(a);
    RegionState rs_b = regionStates.getRegionState(b);
    if (rs_a == null || !rs_a.isMergingOnServer(serverName)
        || rs_b == null || !rs_b.isMergingOnServer(serverName)) {
      return "Some daughter is not known to be merging on " + serverName
        + ", a=" + rs_a + ", b=" + rs_b;
    }

    regionOnline(a, serverName);
    regionOnline(b, serverName);
    regionOffline(hri);

    if (getTableStateManager().isTableState(hri.getTable(),
        TableState.State.DISABLED, TableState.State.DISABLING)) {
      invokeUnAssign(a);
      invokeUnAssign(b);
    }
    return null;
  }

  private void doMergingOfReplicas(HRegionInfo mergedHri, final HRegionInfo hri_a,
      final HRegionInfo hri_b) {
    // Close replicas for the original unmerged regions. create/assign new replicas
    // for the merged parent.
    List<HRegionInfo> unmergedRegions = new ArrayList<HRegionInfo>();
    unmergedRegions.add(hri_a);
    unmergedRegions.add(hri_b);
    Map<ServerName, List<HRegionInfo>> map = regionStates.getRegionAssignments(unmergedRegions);
    Collection<List<HRegionInfo>> c = map.values();
    for (List<HRegionInfo> l : c) {
      for (HRegionInfo h : l) {
        if (!RegionReplicaUtil.isDefaultReplica(h)) {
          LOG.debug("Unassigning un-merged replica " + h);
          unassign(h);
        }
      }
    }
    int numReplicas = 1;
    try {
      numReplicas = ((MasterServices)server).getTableDescriptors().get(mergedHri.getTable()).
          getRegionReplication();
    } catch (IOException e) {
      LOG.warn("Couldn't get the replication attribute of the table " + mergedHri.getTable() +
          " due to " + e.getMessage() + ". The assignment of replicas for the merged region " +
          "will not be done");
    }
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    for (int i = 1; i < numReplicas; i++) {
      regions.add(RegionReplicaUtil.getRegionInfoForReplica(mergedHri, i));
    }
    try {
      assign(regions);
    } catch (IOException ioe) {
      LOG.warn("Couldn't assign all replica(s) of region " + mergedHri + " because of " +
                ioe.getMessage());
    } catch (InterruptedException ie) {
      LOG.warn("Couldn't assign all replica(s) of region " + mergedHri+ " because of " +
                ie.getMessage());
    }
  }

  private void doSplittingOfReplicas(final HRegionInfo parentHri, final HRegionInfo hri_a,
      final HRegionInfo hri_b) {
    // create new regions for the replica, and assign them to match with the
    // current replica assignments. If replica1 of parent is assigned to RS1,
    // the replica1s of daughters will be on the same machine
    int numReplicas = 1;
    try {
      numReplicas = ((MasterServices)server).getTableDescriptors().get(parentHri.getTable()).
          getRegionReplication();
    } catch (IOException e) {
      LOG.warn("Couldn't get the replication attribute of the table " + parentHri.getTable() +
          " due to " + e.getMessage() + ". The assignment of daughter replicas " +
          "replicas will not be done");
    }
    // unassign the old replicas
    List<HRegionInfo> parentRegion = new ArrayList<HRegionInfo>();
    parentRegion.add(parentHri);
    Map<ServerName, List<HRegionInfo>> currentAssign =
        regionStates.getRegionAssignments(parentRegion);
    Collection<List<HRegionInfo>> c = currentAssign.values();
    for (List<HRegionInfo> l : c) {
      for (HRegionInfo h : l) {
        if (!RegionReplicaUtil.isDefaultReplica(h)) {
          LOG.debug("Unassigning parent's replica " + h);
          unassign(h);
        }
      }
    }
    // assign daughter replicas
    Map<HRegionInfo, ServerName> map = new HashMap<HRegionInfo, ServerName>();
    for (int i = 1; i < numReplicas; i++) {
      prepareDaughterReplicaForAssignment(hri_a, parentHri, i, map);
      prepareDaughterReplicaForAssignment(hri_b, parentHri, i, map);
    }
    try {
      assign(map);
    } catch (IOException e) {
      LOG.warn("Caught exception " + e + " while trying to assign replica(s) of daughter(s)");
    } catch (InterruptedException e) {
      LOG.warn("Caught exception " + e + " while trying to assign replica(s) of daughter(s)");
    }
  }

  private void prepareDaughterReplicaForAssignment(HRegionInfo daughterHri, HRegionInfo parentHri,
      int replicaId, Map<HRegionInfo, ServerName> map) {
    HRegionInfo parentReplica = RegionReplicaUtil.getRegionInfoForReplica(parentHri, replicaId);
    HRegionInfo daughterReplica = RegionReplicaUtil.getRegionInfoForReplica(daughterHri,
        replicaId);
    LOG.debug("Created replica region for daughter " + daughterReplica);
    ServerName sn;
    if ((sn = regionStates.getRegionServerOfRegion(parentReplica)) != null) {
      map.put(daughterReplica, sn);
    } else {
      List<ServerName> servers = serverManager.getOnlineServersList();
      sn = servers.get((new Random(System.currentTimeMillis())).nextInt(servers.size()));
      map.put(daughterReplica, sn);
    }
  }

  public Set<HRegionInfo> getReplicasToClose() {
    return replicasToClose;
  }

  /**
   * A region is offline.  The new state should be the specified one,
   * if not null.  If the specified state is null, the new state is Offline.
   * The specified state can be Split/Merged/Offline/null only.
   */
  private void regionOffline(final HRegionInfo regionInfo, final State state) {
    regionStates.regionOffline(regionInfo, state);
    removeClosedRegion(regionInfo);
    // remove the region plan as well just in case.
    clearRegionPlan(regionInfo);
    balancer.regionOffline(regionInfo);

    // Tell our listeners that a region was closed
    sendRegionClosedNotification(regionInfo);
    // also note that all the replicas of the primary should be closed
    if (state != null && state.equals(State.SPLIT)) {
      Collection<HRegionInfo> c = new ArrayList<HRegionInfo>(1);
      c.add(regionInfo);
      Map<ServerName, List<HRegionInfo>> map = regionStates.getRegionAssignments(c);
      Collection<List<HRegionInfo>> allReplicas = map.values();
      for (List<HRegionInfo> list : allReplicas) {
        replicasToClose.addAll(list);
      }
    }
    else if (state != null && state.equals(State.MERGED)) {
      Collection<HRegionInfo> c = new ArrayList<HRegionInfo>(1);
      c.add(regionInfo);
      Map<ServerName, List<HRegionInfo>> map = regionStates.getRegionAssignments(c);
      Collection<List<HRegionInfo>> allReplicas = map.values();
      for (List<HRegionInfo> list : allReplicas) {
        replicasToClose.addAll(list);
      }
    }
  }

  private void sendRegionOpenedNotification(final HRegionInfo regionInfo,
      final ServerName serverName) {
    if (!this.listeners.isEmpty()) {
      for (AssignmentListener listener : this.listeners) {
        listener.regionOpened(regionInfo, serverName);
      }
    }
  }

  private void sendRegionClosedNotification(final HRegionInfo regionInfo) {
    if (!this.listeners.isEmpty()) {
      for (AssignmentListener listener : this.listeners) {
        listener.regionClosed(regionInfo);
      }
    }
  }

  /**
   * Try to update some region states. If the state machine prevents
   * such update, an error message is returned to explain the reason.
   *
   * It's expected that in each transition there should have just one
   * region for opening/closing, 3 regions for splitting/merging.
   * These regions should be on the server that requested the change.
   *
   * Region state machine. Only these transitions
   * are expected to be triggered by a region server.
   *
   * On the state transition:
   *  (1) Open/Close should be initiated by master
   *      (a) Master sets the region to pending_open/pending_close
   *        in memory and hbase:meta after sending the request
   *        to the region server
   *      (b) Region server reports back to the master
   *        after open/close is done (either success/failure)
   *      (c) If region server has problem to report the status
   *        to master, it must be because the master is down or some
   *        temporary network issue. Otherwise, the region server should
   *        abort since it must be a bug. If the master is not accessible,
   *        the region server should keep trying until the server is
   *        stopped or till the status is reported to the (new) master
   *      (d) If region server dies in the middle of opening/closing
   *        a region, SSH picks it up and finishes it
   *      (e) If master dies in the middle, the new master recovers
   *        the state during initialization from hbase:meta. Region server
   *        can report any transition that has not been reported to
   *        the previous active master yet
   *  (2) Split/merge is initiated by region servers
   *      (a) To split a region, a region server sends a request
   *        to master to try to set a region to splitting, together with
   *        two daughters (to be created) to splitting new. If approved
   *        by the master, the splitting can then move ahead
   *      (b) To merge two regions, a region server sends a request to
   *        master to try to set the new merged region (to be created) to
   *        merging_new, together with two regions (to be merged) to merging.
   *        If it is ok with the master, the merge can then move ahead
   *      (c) Once the splitting/merging is done, the region server
   *        reports the status back to the master either success/failure.
   *      (d) Other scenarios should be handled similarly as for
   *        region open/close
   */
  protected String onRegionTransition(final ServerName serverName,
      final RegionStateTransition transition) {
    TransitionCode code = transition.getTransitionCode();
    HRegionInfo hri = HRegionInfo.convert(transition.getRegionInfo(0));
    Lock lock = locker.acquireLock(hri.getEncodedName());
    try {
      RegionState current = regionStates.getRegionState(hri);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got transition " + code + " for "
          + (current != null ? current.toString() : hri.getShortNameToLog())
          + " from " + serverName);
      }
      String errorMsg = null;
      switch (code) {
      case OPENED:
        errorMsg = onRegionOpen(current, hri, serverName, transition);
        break;
      case FAILED_OPEN:
        errorMsg = onRegionFailedOpen(current, hri, serverName);
        break;
      case CLOSED:
        errorMsg = onRegionClosed(current, hri, serverName);
        break;
      case READY_TO_SPLIT:
        try {
          regionStateListener.onRegionSplit(hri);
          errorMsg = onRegionReadyToSplit(current, hri, serverName, transition);
        } catch (IOException exp) {
          errorMsg = StringUtils.stringifyException(exp);
        }
        break;
      case SPLIT_PONR:
        errorMsg = onRegionSplitPONR(current, hri, serverName, transition);
        break;
      case SPLIT:
        errorMsg = onRegionSplit(current, hri, serverName, transition);
        break;
      case SPLIT_REVERTED:
        errorMsg = onRegionSplitReverted(current, hri, serverName, transition);
        if (org.apache.commons.lang.StringUtils.isEmpty(errorMsg)) {
          try {
            regionStateListener.onRegionSplitReverted(hri);
          } catch (IOException exp) {
            LOG.warn(StringUtils.stringifyException(exp));
          }
        }
        break;
      case READY_TO_MERGE:
        errorMsg = onRegionReadyToMerge(current, hri, serverName, transition);
        break;
      case MERGE_PONR:
        errorMsg = onRegionMergePONR(current, hri, serverName, transition);
        break;
      case MERGED:
        try {
          errorMsg = onRegionMerged(current, hri, serverName, transition);
          regionStateListener.onRegionMerged(hri);
        } catch (IOException exp) {
          errorMsg = StringUtils.stringifyException(exp);
        }
        break;
      case MERGE_REVERTED:
        errorMsg = onRegionMergeReverted(current, hri, serverName, transition);
        break;

      default:
        errorMsg = "Unexpected transition code " + code;
      }
      if (errorMsg != null) {
        LOG.info("Could not transition region from " + current + " on "
          + code + " by " + serverName + ": " + errorMsg);
      }
      return errorMsg;
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return Instance of load balancer
   */
  public LoadBalancer getBalancer() {
    return this.balancer;
  }

  public Map<ServerName, List<HRegionInfo>>
    getSnapShotOfAssignment(Collection<HRegionInfo> infos) {
    return getRegionStates().getRegionAssignments(infos);
  }

  void setRegionStateListener(RegionStateListener listener) {
    this.regionStateListener = listener;
  }
}
