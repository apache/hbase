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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.OpenRegionCoordination;
import org.apache.hadoop.hbase.coordination.RegionMergeCoordination;
import org.apache.hadoop.hbase.coordination.SplitTransactionCoordination.SplitTransactionDetails;
import org.apache.hadoop.hbase.coordination.ZkOpenRegionCoordination;
import org.apache.hadoop.hbase.coordination.ZkRegionMergeCoordination;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeLoadBalancer;
import org.apache.hadoop.hbase.master.handler.ClosedRegionHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.OpenedRegionHandler;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.quotas.RegionStateListener;
import org.apache.hadoop.hbase.regionserver.RegionAlreadyInTransitionException;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.ConfigUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.LinkedHashMultimap;

/**
 * Manages and performs region assignment.
 * <p>
 * Monitors ZooKeeper for events related to regions in transition.
 * <p>
 * Handles existing regions in transition during master failover.
 */
@InterfaceAudience.Private
public class AssignmentManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(AssignmentManager.class);

  public static final ServerName HBCK_CODE_SERVERNAME = ServerName.valueOf(HConstants.HBCK_CODE_NAME,
      -1, -1L);

  static final String ALREADY_IN_TRANSITION_WAITTIME
    = "hbase.assignment.already.intransition.waittime";
  static final int DEFAULT_ALREADY_IN_TRANSITION_WAITTIME = 60000; // 1 minute

  protected final Server server;

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
   * Map of two merging regions from the region to be created.
   */
  private final Map<String, PairOfSameType<HRegionInfo>> mergingRegions
    = new HashMap<String, PairOfSameType<HRegionInfo>>();

  private final Map<HRegionInfo, PairOfSameType<HRegionInfo>> splitRegions
  = new HashMap<HRegionInfo, PairOfSameType<HRegionInfo>>();

  /**
   * The sleep time for which the assignment will wait before retrying in case of hbase:meta assignment
   * failure due to lack of availability of region plan or bad region plan
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

  // For unit tests, keep track of calls to ClosedRegionHandler
  private Map<HRegionInfo, AtomicBoolean> closedRegionHandlerCalled = null;

  // For unit tests, keep track of calls to OpenedRegionHandler
  private Map<HRegionInfo, AtomicBoolean> openedRegionHandlerCalled = null;

  //Thread pool executor service for timeout monitor
  private java.util.concurrent.ExecutorService threadPoolExecutorService;

  // A bunch of ZK events workers. Each is a single thread executor service
  private final java.util.concurrent.ExecutorService zkEventWorkers;

  private List<EventType> ignoreStatesRSOffline = Arrays.asList(
      EventType.RS_ZK_REGION_FAILED_OPEN, EventType.RS_ZK_REGION_CLOSED);

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

  // A flag to indicate if we are using ZK for region assignment
  private final boolean useZKForAssignment;

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

  public enum ServerHostRegion {
    NOT_HOSTING_REGION, HOSTING_REGION, UNKNOWN,
  }

  /**
   * Constructs a new assignment manager.
   *
   * @param server instance of HMaster this AM running inside
   * @param serverManager serverManager for associated HMaster
   * @param balancer implementation of {@link LoadBalancer}
   * @param service Executor service
   * @param metricsMaster metrics manager
   * @param tableLockManager TableLock manager
   * @throws KeeperException
   * @throws IOException
   */
  public AssignmentManager(Server server, ServerManager serverManager,
      final LoadBalancer balancer,
      final ExecutorService service, MetricsMaster metricsMaster,
      final TableLockManager tableLockManager) throws KeeperException,
        IOException, CoordinatedStateException {
    super(server.getZooKeeper());
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
    try {
      if (server.getCoordinatedStateManager() != null) {
        this.tableStateManager = server.getCoordinatedStateManager().getTableStateManager();
      } else {
        this.tableStateManager = null;
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
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

    int workers = conf.getInt("hbase.assignment.zkevent.workers", 20);
    ThreadFactory threadFactory = Threads.newDaemonThreadFactory("AM.ZK.Worker");
    zkEventWorkers = Threads.getBoundedCachedThreadPool(workers, 60L,
            TimeUnit.SECONDS, threadFactory);
    this.tableLockManager = tableLockManager;

    this.metricsAssignmentManager = new MetricsAssignmentManager();
    useZKForAssignment = ConfigUtil.useZKForAssignment(conf);
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
      hris = MetaTableAccessor.getTableRegions(server.getZooKeeper(),
        server.getConnection(), tableName, true);
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
  void joinCluster() throws IOException,
      KeeperException, InterruptedException, CoordinatedStateException {
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
    // it will reconstruct master state and cleanup any leftovers from
    // previous master process.
    boolean failover = processDeadServersAndRegionsInTransition(deadServers);

    if (!useZKForAssignment) {
      // Not use ZK for assignment any more, remove the ZNode
      ZKUtil.deleteNodeRecursively(watcher, watcher.assignmentZNode);
    }
    recoverTableInDisablingState();
    recoverTableInEnablingState();
    LOG.info("Joined the cluster in " + (System.currentTimeMillis()
      - startTime) + "ms, failover=" + failover);
  }

  /**
   * Process all regions that are in transition in zookeeper and also
   * processes the list of dead servers by scanning the META.
   * Used by master joining an cluster.  If we figure this is a clean cluster
   * startup, will assign all user regions.
   * @param deadServers
   *          Map of dead servers and their regions. Can be null.
   * @throws KeeperException
   * @throws IOException
   * @throws InterruptedException
   */
  boolean processDeadServersAndRegionsInTransition(
      final Set<ServerName> deadServers) throws KeeperException,
        IOException, InterruptedException, CoordinatedStateException {
    List<String> nodes = ZKUtil.listChildrenNoWatch(watcher,
      watcher.assignmentZNode);

    if (useZKForAssignment && nodes == null) {
      String errorMessage = "Failed to get the children from ZK";
      server.abort(errorMessage, new IOException(errorMessage));
      return true; // Doesn't matter in this case
    }

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
      if (!failover && nodes != null) {
        // If any one region except meta is in transition, it's a failover.
        for (String encodedName: nodes) {
          RegionState regionState = regionStates.getRegionState(encodedName);
          if (regionState != null && !regionState.getRegion().isMetaRegion()) {
            LOG.debug("Found " + regionState + " in RITs");
            failover = true;
            break;
          }
        }
      }
    }
    if (!failover && !useZKForAssignment) {
      // If any region except meta is in transition on a live server, it's a failover.
      Map<String, RegionState> regionsInTransition = regionStates.getRegionsInTransition();
      if (!regionsInTransition.isEmpty()) {
        Set<ServerName> onlineServers = serverManager.getOnlineServers().keySet();
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
        ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING,
        ZooKeeperProtos.Table.State.ENABLING);

      // Clean re/start, mark all user regions closed before reassignment
      allRegions = regionStates.closeAllUserRegions(
        disabledOrDisablingOrEnabling);
    }

    // Now region states are restored
    regionStateStore.start();

    // If we found user regions out on cluster, its a failover.
    if (failover) {
      LOG.info("Found regions out on cluster or in RIT; presuming failover");
      // Process list of dead servers and regions in RIT.
      // See HBASE-4580 for more information.
      processDeadServersAndRecoverLostRegions(deadServers);
    }

    if (!failover && useZKForAssignment) {
      // Cleanup any existing ZK nodes and start watching
      ZKAssign.deleteAllNodes(watcher);
      ZKUtil.listChildrenAndWatchForNewChildren(this.watcher,
        this.watcher.assignmentZNode);
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
   * If region is up in zk in transition, then do fixup and block and wait until
   * the region is assigned and out of transition.  Used on startup for
   * catalog regions.
   * @param hri Region to look for.
   * @return True if we processed a region in transition else false if region
   * was not up in zk in transition.
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   */
  boolean processRegionInTransitionAndBlockUntilAssigned(final HRegionInfo hri)
      throws InterruptedException, KeeperException, IOException {
    String encodedRegionName = hri.getEncodedName();
    if (!processRegionInTransition(encodedRegionName, hri)) {
      return false; // The region is not in transition
    }
    LOG.debug("Waiting on " + HRegionInfo.prettyPrint(encodedRegionName));
    while (!this.server.isStopped() &&
        this.regionStates.isRegionInTransition(encodedRegionName)) {
      RegionState state = this.regionStates.getRegionTransitionState(encodedRegionName);
      if (state == null || !serverManager.isServerOnline(state.getServerName())) {
        // The region is not in transition, or not in transition on an online
        // server. Doesn't help to block here any more. Caller need to
        // verify the region is actually assigned.
        break;
      }
      this.regionStates.waitForUpdate(100);
    }
    return true;
  }

  /**
   * Process failover of new master for region <code>encodedRegionName</code>
   * up in zookeeper.
   * @param encodedRegionName Region to process failover for.
   * @param regionInfo If null we'll go get it from meta table.
   * @return True if we processed <code>regionInfo</code> as a RIT.
   * @throws KeeperException
   * @throws IOException
   */
  boolean processRegionInTransition(final String encodedRegionName,
      final HRegionInfo regionInfo) throws KeeperException, IOException {
    // We need a lock here to ensure that we will not put the same region twice
    // It has no reason to be a lock shared with the other operations.
    // We can do the lock on the region only, instead of a global lock: what we want to ensure
    // is that we don't have two threads working on the same region.
    Lock lock = locker.acquireLock(encodedRegionName);
    try {
      Stat stat = new Stat();
      byte [] data = ZKAssign.getDataAndWatch(watcher, encodedRegionName, stat);
      if (data == null) return false;
      RegionTransition rt;
      try {
        rt = RegionTransition.parseFrom(data);
      } catch (DeserializationException e) {
        LOG.warn("Failed parse znode data", e);
        return false;
      }
      HRegionInfo hri = regionInfo;
      if (hri == null) {
        // The region info is not passed in. We will try to find the region
        // from region states map/meta based on the encoded region name. But we
        // may not be able to find it. This is valid for online merge that
        // the region may have not been created if the merge is not completed.
        // Therefore, it is not in meta at master recovery time.
        hri = regionStates.getRegionInfo(rt.getRegionName());
        EventType et = rt.getEventType();
        if (hri == null && et != EventType.RS_ZK_REGION_MERGING
            && et != EventType.RS_ZK_REQUEST_REGION_MERGE) {
          LOG.warn("Couldn't find the region in recovering " + rt);
          return false;
        }
      }

      // TODO: This code is tied to ZK anyway, so for now leaving it as is,
      // will refactor when whole region assignment will be abstracted from ZK
      BaseCoordinatedStateManager cp =
        (BaseCoordinatedStateManager) this.server.getCoordinatedStateManager();
      OpenRegionCoordination openRegionCoordination = cp.getOpenRegionCoordination();

      ZkOpenRegionCoordination.ZkOpenRegionDetails zkOrd =
        new ZkOpenRegionCoordination.ZkOpenRegionDetails();
      zkOrd.setVersion(stat.getVersion());
      zkOrd.setServerName(cp.getServer().getServerName());

      return processRegionsInTransition(
        rt, hri, openRegionCoordination, zkOrd);
    } finally {
      lock.unlock();
    }
  }

  /**
   * This call is invoked only (1) master assign meta;
   * (2) during failover mode startup, zk assignment node processing.
   * The locker is set in the caller. It returns true if the region
   * is in transition for sure, false otherwise.
   *
   * It should be private but it is used by some test too.
   */
  boolean processRegionsInTransition(
      final RegionTransition rt, final HRegionInfo regionInfo,
      OpenRegionCoordination coordination,
      final OpenRegionCoordination.OpenRegionDetails ord) throws KeeperException {
    EventType et = rt.getEventType();
    // Get ServerName.  Could not be null.
    final ServerName sn = rt.getServerName();
    final byte[] regionName = rt.getRegionName();
    final String encodedName = HRegionInfo.encodeRegionName(regionName);
    final String prettyPrintedRegionName = HRegionInfo.prettyPrint(encodedName);
    LOG.info("Processing " + prettyPrintedRegionName + " in state: " + et);

    if (regionStates.isRegionInTransition(encodedName)
        && (regionInfo.isMetaRegion() || !useZKForAssignment)) {
      LOG.info("Processed region " + prettyPrintedRegionName + " in state: "
        + et + ", does nothing since the region is already in transition "
        + regionStates.getRegionTransitionState(encodedName));
      // Just return
      return true;
    }
    if (!serverManager.isServerOnline(sn)) {
      // It was transitioning on a dead server, so it's closed now.
      // Force to OFFLINE and put it in transition, but not assign it
      // since log splitting for the dead server is not done yet.
      LOG.debug("RIT " + encodedName + " in state=" + rt.getEventType() +
        " was on deadserver; forcing offline");
      if (regionStates.isRegionOnline(regionInfo)) {
        // Meta could still show the region is assigned to the previous
        // server. If that server is online, when we reload the meta, the
        // region is put back to online, we need to offline it.
        regionStates.regionOffline(regionInfo);
        sendRegionClosedNotification(regionInfo);
      }
      // Put it back in transition so that SSH can re-assign it
      regionStates.updateRegionState(regionInfo, State.OFFLINE, sn);

      if (regionInfo.isMetaRegion()) {
        // If it's meta region, reset the meta location.
        // So that master knows the right meta region server.
        MetaTableLocator.setMetaLocation(watcher, sn, State.OPEN);
      } else {
        // No matter the previous server is online or offline,
        // we need to reset the last region server of the region.
        regionStates.setLastRegionServerOfRegion(sn, encodedName);
        // Make sure we know the server is dead.
        if (!serverManager.isServerDead(sn)) {
          serverManager.expireServer(sn);
        }
      }
      return false;
    }
    switch (et) {
      case M_ZK_REGION_CLOSING:
        // Insert into RIT & resend the query to the region server: may be the previous master
        // died before sending the query the first time.
        final RegionState rsClosing = regionStates.updateRegionState(rt, State.CLOSING);
        this.executorService.submit(
          new EventHandler(server, EventType.M_MASTER_RECOVERY) {
            @Override
            public void process() throws IOException {
              ReentrantLock lock = locker.acquireLock(regionInfo.getEncodedName());
              try {
                final int expectedVersion = ((ZkOpenRegionCoordination.ZkOpenRegionDetails) ord)
                  .getVersion();
                unassign(regionInfo, rsClosing, expectedVersion, null, useZKForAssignment, null);
                if (regionStates.isRegionOffline(regionInfo)) {
                  assign(regionInfo, true);
                }
              } finally {
                lock.unlock();
              }
            }
          });
        break;

      case RS_ZK_REGION_CLOSED:
      case RS_ZK_REGION_FAILED_OPEN:
        // Region is closed, insert into RIT and handle it
        regionStates.setLastRegionServerOfRegion(sn, encodedName);
        regionStates.updateRegionState(regionInfo, State.CLOSED, sn);
        if (!replicasToClose.contains(regionInfo)) {
          invokeAssign(regionInfo);
        } else {
          offlineDisabledRegion(regionInfo);
        }
        break;

      case M_ZK_REGION_OFFLINE:
        // Insert in RIT and resend to the regionserver
        regionStates.updateRegionState(rt, State.PENDING_OPEN);
        final RegionState rsOffline = regionStates.getRegionState(regionInfo);
        this.executorService.submit(
          new EventHandler(server, EventType.M_MASTER_RECOVERY) {
            @Override
            public void process() throws IOException {
              ReentrantLock lock = locker.acquireLock(regionInfo.getEncodedName());
              try {
                RegionPlan plan = new RegionPlan(regionInfo, null, sn);
                addPlan(encodedName, plan);
                assign(rsOffline, false, false);
              } finally {
                lock.unlock();
              }
            }
          });
        break;

      case RS_ZK_REGION_OPENING:
        regionStates.updateRegionState(rt, State.OPENING);
        break;

      case RS_ZK_REGION_OPENED:
        // Region is opened, insert into RIT and handle it
        // This could be done asynchronously, we would need then to acquire the lock in the
        //  handler.
        regionStates.updateRegionState(rt, State.OPEN);
        new OpenedRegionHandler(server, this, regionInfo, coordination, ord).process();
        break;
      case RS_ZK_REQUEST_REGION_SPLIT:
      case RS_ZK_REGION_SPLITTING:
      case RS_ZK_REGION_SPLIT:
        // Splitting region should be online. We could have skipped it during
        // user region rebuilding since we may consider the split is completed.
        // Put it in SPLITTING state to avoid complications.
        regionStates.regionOnline(regionInfo, sn);
        regionStates.updateRegionState(rt, State.SPLITTING);
        if (!handleRegionSplitting(
            rt, encodedName, prettyPrintedRegionName, sn)) {
          deleteSplittingNode(encodedName, sn);
        }
        break;
      case RS_ZK_REQUEST_REGION_MERGE:
      case RS_ZK_REGION_MERGING:
      case RS_ZK_REGION_MERGED:
        if (!handleRegionMerging(
            rt, encodedName, prettyPrintedRegionName, sn)) {
          deleteMergingNode(encodedName, sn);
        }
        break;
      default:
        throw new IllegalStateException("Received region in state:" + et + " is not valid.");
    }
    LOG.info("Processed region " + prettyPrintedRegionName + " in state "
      + et + ", on " + (serverManager.isServerOnline(sn) ? "" : "dead ")
      + "server: " + sn);
    return true;
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

  /**
   * Handles various states an unassigned node can be in.
   * <p>
   * Method is called when a state change is suspected for an unassigned node.
   * <p>
   * This deals with skipped transitions (we got a CLOSED but didn't see CLOSING
   * yet).
   * @param rt region transition
   * @param coordination coordination for opening region
   * @param ord details about opening region
   */
  void handleRegion(final RegionTransition rt, OpenRegionCoordination coordination,
                    OpenRegionCoordination.OpenRegionDetails ord) {
    if (rt == null) {
      LOG.warn("Unexpected NULL input for RegionTransition rt");
      return;
    }
    final ServerName sn = rt.getServerName();
    // Check if this is a special HBCK transition
    if (sn.equals(HBCK_CODE_SERVERNAME)) {
      handleHBCK(rt);
      return;
    }
    final long createTime = rt.getCreateTime();
    final byte[] regionName = rt.getRegionName();
    String encodedName = HRegionInfo.encodeRegionName(regionName);
    String prettyPrintedRegionName = HRegionInfo.prettyPrint(encodedName);
    // Verify this is a known server
    if (!serverManager.isServerOnline(sn)
      && !ignoreStatesRSOffline.contains(rt.getEventType())) {
      LOG.warn("Attempted to handle region transition for server but " +
        "it is not online: " + prettyPrintedRegionName + ", " + rt);
      return;
    }

    RegionState regionState =
      regionStates.getRegionState(encodedName);
    long startTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      boolean lateEvent = createTime < (startTime - 15000);
      LOG.debug("Handling " + rt.getEventType() +
        ", server=" + sn + ", region=" +
        (prettyPrintedRegionName == null ? "null" : prettyPrintedRegionName) +
        (lateEvent ? ", which is more than 15 seconds late" : "") +
        ", current_state=" + regionState);
    }
    // We don't do anything for this event,
    // so separate it out, no need to lock/unlock anything
    if (rt.getEventType() == EventType.M_ZK_REGION_OFFLINE) {
      return;
    }

    // We need a lock on the region as we could update it
    Lock lock = locker.acquireLock(encodedName);
    try {
      RegionState latestState =
        regionStates.getRegionState(encodedName);
      if ((regionState == null && latestState != null)
          || (regionState != null && latestState == null)
          || (regionState != null && latestState != null
            && latestState.getState() != regionState.getState())) {
        LOG.warn("Region state changed from " + regionState + " to "
          + latestState + ", while acquiring lock");
      }
      long waitedTime = System.currentTimeMillis() - startTime;
      if (waitedTime > 5000) {
        LOG.warn("Took " + waitedTime + "ms to acquire the lock");
      }
      regionState = latestState;
      switch (rt.getEventType()) {
      case RS_ZK_REQUEST_REGION_SPLIT:
      case RS_ZK_REGION_SPLITTING:
      case RS_ZK_REGION_SPLIT:
        if (!handleRegionSplitting(
            rt, encodedName, prettyPrintedRegionName, sn)) {
          deleteSplittingNode(encodedName, sn);
        }
        break;

      case RS_ZK_REQUEST_REGION_MERGE:
      case RS_ZK_REGION_MERGING:
      case RS_ZK_REGION_MERGED:
        // Merged region is a new region, we can't find it in the region states now.
        // However, the two merging regions are not new. They should be in state for merging.
        if (!handleRegionMerging(
            rt, encodedName, prettyPrintedRegionName, sn)) {
          deleteMergingNode(encodedName, sn);
        }
        break;

      case M_ZK_REGION_CLOSING:
        // Should see CLOSING after we have asked it to CLOSE or additional
        // times after already being in state of CLOSING
        if (regionState == null
            || !regionState.isPendingCloseOrClosingOnServer(sn)) {
          LOG.warn("Received CLOSING for " + prettyPrintedRegionName
            + " from " + sn + " but the region isn't PENDING_CLOSE/CLOSING here: "
            + regionStates.getRegionState(encodedName));
          return;
        }
        // Transition to CLOSING (or update stamp if already CLOSING)
        regionStates.updateRegionState(rt, State.CLOSING);
        break;

      case RS_ZK_REGION_CLOSED:
        // Should see CLOSED after CLOSING but possible after PENDING_CLOSE
        if (regionState == null
            || !regionState.isPendingCloseOrClosingOnServer(sn)) {
          LOG.warn("Received CLOSED for " + prettyPrintedRegionName
            + " from " + sn + " but the region isn't PENDING_CLOSE/CLOSING here: "
            + regionStates.getRegionState(encodedName));
          return;
        }
        // Handle CLOSED by assigning elsewhere or stopping if a disable
        // If we got here all is good.  Need to update RegionState -- else
        // what follows will fail because not in expected state.
        new ClosedRegionHandler(server, this, regionState.getRegion()).process();
        updateClosedRegionHandlerTracker(regionState.getRegion());
        break;

        case RS_ZK_REGION_FAILED_OPEN:
          if (regionState == null
              || !regionState.isPendingOpenOrOpeningOnServer(sn)) {
            LOG.warn("Received FAILED_OPEN for " + prettyPrintedRegionName
              + " from " + sn + " but the region isn't PENDING_OPEN/OPENING here: "
              + regionStates.getRegionState(encodedName));
            return;
          }
          AtomicInteger failedOpenCount = failedOpenTracker.get(encodedName);
          if (failedOpenCount == null) {
            failedOpenCount = new AtomicInteger();
            // No need to use putIfAbsent, or extra synchronization since
            // this whole handleRegion block is locked on the encoded region
            // name, and failedOpenTracker is updated only in this block
            failedOpenTracker.put(encodedName, failedOpenCount);
          }
          if (failedOpenCount.incrementAndGet() >= maximumAttempts) {
            regionStates.updateRegionState(rt, State.FAILED_OPEN);
            // remove the tracking info to save memory, also reset
            // the count for next open initiative
            failedOpenTracker.remove(encodedName);
          } else {
            // Handle this the same as if it were opened and then closed.
            regionState = regionStates.updateRegionState(rt, State.CLOSED);
            if (regionState != null) {
              // When there are more than one region server a new RS is selected as the
              // destination and the same is updated in the regionplan. (HBASE-5546)
              try {
                getRegionPlan(regionState.getRegion(), sn, true);
                new ClosedRegionHandler(server, this, regionState.getRegion()).process();
              } catch (HBaseIOException e) {
                LOG.warn("Failed to get region plan", e);
              }
            }
          }
          break;

        case RS_ZK_REGION_OPENING:
          // Should see OPENING after we have asked it to OPEN or additional
          // times after already being in state of OPENING
          if (regionState == null
              || !regionState.isPendingOpenOrOpeningOnServer(sn)) {
            LOG.warn("Received OPENING for " + prettyPrintedRegionName
              + " from " + sn + " but the region isn't PENDING_OPEN/OPENING here: "
              + regionStates.getRegionState(encodedName));
            return;
          }
          // Transition to OPENING (or update stamp if already OPENING)
          regionStates.updateRegionState(rt, State.OPENING);
          break;

        case RS_ZK_REGION_OPENED:
          // Should see OPENED after OPENING but possible after PENDING_OPEN.
          if (regionState == null
              || !regionState.isPendingOpenOrOpeningOnServer(sn)) {
            LOG.warn("Received OPENED for " + prettyPrintedRegionName
              + " from " + sn + " but the region isn't PENDING_OPEN/OPENING here: "
              + regionStates.getRegionState(encodedName));

            if (regionState != null) {
              // Close it without updating the internal region states,
              // so as not to create double assignments in unlucky scenarios
              // mentioned in OpenRegionHandler#process
              unassign(regionState.getRegion(), null, -1, null, false, sn);
            }
            return;
          }
          // Handle OPENED by removing from transition and deleted zk node
          regionState =
              regionStates.transitionOpenFromPendingOpenOrOpeningOnServer(rt,regionState, sn);
          if (regionState != null) {
            failedOpenTracker.remove(encodedName); // reset the count, if any
            new OpenedRegionHandler(
              server, this, regionState.getRegion(), coordination, ord).process();
            updateOpenedRegionHandlerTracker(regionState.getRegion());
          }
          break;

        default:
          throw new IllegalStateException("Received event is not valid.");
      }
    } finally {
      lock.unlock();
    }
  }

  //For unit tests only
  boolean wasClosedHandlerCalled(HRegionInfo hri) {
    AtomicBoolean b = closedRegionHandlerCalled.get(hri);
    //compareAndSet to be sure that unit tests don't see stale values. Means,
    //we will return true exactly once unless the handler code resets to true
    //this value.
    return b == null ? false : b.compareAndSet(true, false);
  }

  //For unit tests only
  boolean wasOpenedHandlerCalled(HRegionInfo hri) {
    AtomicBoolean b = openedRegionHandlerCalled.get(hri);
    //compareAndSet to be sure that unit tests don't see stale values. Means,
    //we will return true exactly once unless the handler code resets to true
    //this value.
    return b == null ? false : b.compareAndSet(true, false);
  }

  //For unit tests only
  void initializeHandlerTrackers() {
    closedRegionHandlerCalled = new HashMap<HRegionInfo, AtomicBoolean>();
    openedRegionHandlerCalled = new HashMap<HRegionInfo, AtomicBoolean>();
  }

  void updateClosedRegionHandlerTracker(HRegionInfo hri) {
    if (closedRegionHandlerCalled != null) { //only for unit tests this is true
      closedRegionHandlerCalled.put(hri, new AtomicBoolean(true));
    }
  }

  void updateOpenedRegionHandlerTracker(HRegionInfo hri) {
    if (openedRegionHandlerCalled != null) { //only for unit tests this is true
      openedRegionHandlerCalled.put(hri, new AtomicBoolean(true));
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
   * Handle a ZK unassigned node transition triggered by HBCK repair tool.
   * <p>
   * This is handled in a separate code path because it breaks the normal rules.
   * @param rt
   */
  @SuppressWarnings("deprecation")
  private void handleHBCK(RegionTransition rt) {
    String encodedName = HRegionInfo.encodeRegionName(rt.getRegionName());
    LOG.info("Handling HBCK triggered transition=" + rt.getEventType() +
      ", server=" + rt.getServerName() + ", region=" +
      HRegionInfo.prettyPrint(encodedName));
    RegionState regionState = regionStates.getRegionTransitionState(encodedName);
    switch (rt.getEventType()) {
      case M_ZK_REGION_OFFLINE:
        HRegionInfo regionInfo;
        if (regionState != null) {
          regionInfo = regionState.getRegion();
        } else {
          try {
            byte [] name = rt.getRegionName();
            Pair<HRegionInfo, ServerName> p = MetaTableAccessor.getRegion(
              this.server.getConnection(), name);
            regionInfo = p.getFirst();
          } catch (IOException e) {
            LOG.info("Exception reading hbase:meta doing HBCK repair operation", e);
            return;
          }
        }
        LOG.info("HBCK repair is triggering assignment of region=" +
            regionInfo.getRegionNameAsString());
        // trigger assign, node is already in OFFLINE so don't need to update ZK
        assign(regionInfo, false);
        break;

      default:
        LOG.warn("Received unexpected region state from HBCK: " + rt.toString());
        break;
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
    handleAssignmentEvent(path);
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
    handleAssignmentEvent(path);
  }


  // We  don't want to have two events on the same region managed simultaneously.
  // For this reason, we need to wait if an event on the same region is currently in progress.
  // So we track the region names of the events in progress, and we keep a waiting list.
  private final Set<String> regionsInProgress = new HashSet<String>();
  // In a LinkedHashMultimap, the put order is kept when we retrieve the collection back. We need
  //  this as we want the events to be managed in the same order as we received them.
  private final LinkedHashMultimap <String, RegionRunnable>
      zkEventWorkerWaitingList = LinkedHashMultimap.create();

  /**
   * A specific runnable that works only on a region.
   */
  private interface RegionRunnable extends Runnable{
    /**
     * @return - the name of the region it works on.
     */
    String getRegionName();
  }

  /**
   * Submit a task, ensuring that there is only one task at a time that working on a given region.
   * Order is respected.
   */
  protected void zkEventWorkersSubmit(final RegionRunnable regRunnable) {

    synchronized (regionsInProgress) {
      // If we're there is already a task with this region, we add it to the
      //  waiting list and return.
      if (regionsInProgress.contains(regRunnable.getRegionName())) {
        synchronized (zkEventWorkerWaitingList){
          zkEventWorkerWaitingList.put(regRunnable.getRegionName(), regRunnable);
        }
        return;
      }

      // No event in progress on this region => we can submit a new task immediately.
      regionsInProgress.add(regRunnable.getRegionName());
      zkEventWorkers.submit(new Runnable() {
        @Override
        public void run() {
          try {
            regRunnable.run();
          } finally {
            // now that we have finished, let's see if there is an event for the same region in the
            //  waiting list. If it's the case, we can now submit it to the pool.
            synchronized (regionsInProgress) {
              regionsInProgress.remove(regRunnable.getRegionName());
              synchronized (zkEventWorkerWaitingList) {
                java.util.Set<RegionRunnable> waiting = zkEventWorkerWaitingList.get(
                    regRunnable.getRegionName());
                if (!waiting.isEmpty()) {
                  // We want the first object only. The only way to get it is through an iterator.
                  RegionRunnable toSubmit = waiting.iterator().next();
                  zkEventWorkerWaitingList.remove(toSubmit.getRegionName(), toSubmit);
                  zkEventWorkersSubmit(toSubmit);
                }
              }
            }
          }
        }
      });
    }
  }

  @Override
  public void nodeDeleted(final String path) {
    if (path.startsWith(watcher.assignmentZNode)) {
      final String regionName = ZKAssign.getRegionName(watcher, path);
      zkEventWorkersSubmit(new RegionRunnable() {
        @Override
        public String getRegionName() {
          return regionName;
        }

        @Override
        public void run() {
          Lock lock = locker.acquireLock(regionName);
          try {
            RegionState rs = regionStates.getRegionTransitionState(regionName);
            if (rs == null) {
              rs = regionStates.getRegionState(regionName);
              if (rs == null || !rs.isMergingNew()) {
                // MergingNew is an offline state
                return;
              }
            }

            HRegionInfo regionInfo = rs.getRegion();
            String regionNameStr = regionInfo.getRegionNameAsString();
            LOG.debug("Znode " + regionNameStr + " deleted, state: " + rs);

            boolean disabled = getTableStateManager().isTableState(regionInfo.getTable(),
                ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING);

            ServerName serverName = rs.getServerName();
            if (serverManager.isServerOnline(serverName)) {
              if (rs.isOnServer(serverName) && (rs.isOpened() || rs.isSplitting())) {
                synchronized (regionStates) {
                  regionOnline(regionInfo, serverName);
                  if (rs.isSplitting() && splitRegions.containsKey(regionInfo)) {
                    // Check if the daugter regions are still there, if they are present, offline
                    // as its the case of a rollback.
                    HRegionInfo hri_a = splitRegions.get(regionInfo).getFirst();
                    HRegionInfo hri_b = splitRegions.get(regionInfo).getSecond();
                    if (!regionStates.isRegionInTransition(hri_a.getEncodedName())) {
                      LOG.warn("Split daughter region not in transition " + hri_a);
                    }
                    if (!regionStates.isRegionInTransition(hri_b.getEncodedName())) {
                      LOG.warn("Split daughter region not in transition" + hri_b);
                    }
                    regionOffline(hri_a);
                    regionOffline(hri_b);
                    splitRegions.remove(regionInfo);
                  }
                  if (disabled) {
                    // if server is offline, no hurt to unassign again
                    LOG.info("Opened " + regionNameStr
                        + "but this table is disabled, triggering close of region");
                    unassign(regionInfo);
                  }
                }
              } else if (rs.isMergingNew()) {
                synchronized (regionStates) {
                  String p = regionInfo.getEncodedName();
                  PairOfSameType<HRegionInfo> regions = mergingRegions.get(p);
                  if (regions != null) {
                    onlineMergingRegion(disabled, regions.getFirst(), serverName);
                    onlineMergingRegion(disabled, regions.getSecond(), serverName);
                  }
                }
              }
            }
          } finally {
            lock.unlock();
          }
        }

        private void onlineMergingRegion(boolean disabled,
            final HRegionInfo hri, final ServerName serverName) {
          RegionState regionState = regionStates.getRegionState(hri);
          if (regionState != null && regionState.isMerging()
              && regionState.isOnServer(serverName)) {
            regionOnline(regionState.getRegion(), serverName);
            if (disabled) {
              unassign(hri);
            }
          }
        }
      });
    }
  }

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING, SPLITTING or CLOSING of a
   * region by creating a znode.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further children changed events</li>
   *   <li>Watch all new children for changed events</li>
   * </ol>
   */
  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.assignmentZNode)) {
      zkEventWorkers.submit(new Runnable() {
        @Override
        public void run() {
          try {
            // Just make sure we see the changes for the new znodes
            List<String> children =
              ZKUtil.listChildrenAndWatchForNewChildren(
                watcher, watcher.assignmentZNode);
            if (children != null) {
              Stat stat = new Stat();
              for (String child : children) {
                // if region is in transition, we already have a watch
                // on it, so no need to watch it again. So, as I know for now,
                // this is needed to watch splitting nodes only.
                if (!regionStates.isRegionInTransition(child)) {
                  ZKAssign.getDataAndWatch(watcher, child, stat);
                }
              }
            }
          } catch (KeeperException e) {
            server.abort("Unexpected ZK exception reading unassigned children", e);
          }
        }
      });
    }
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
   * Pass the assignment event to a worker for processing.
   * Each worker is a single thread executor service.  The reason
   * for just one thread is to make sure all events for a given
   * region are processed in order.
   *
   * @param path
   */
  private void handleAssignmentEvent(final String path) {
    if (path.startsWith(watcher.assignmentZNode)) {
      final String regionName = ZKAssign.getRegionName(watcher, path);

      zkEventWorkersSubmit(new RegionRunnable() {
        @Override
        public String getRegionName() {
          return regionName;
        }

        @Override
        public void run() {
          try {
            Stat stat = new Stat();
            byte [] data = ZKAssign.getDataAndWatch(watcher, path, stat);
            if (data == null) return;

            RegionTransition rt = RegionTransition.parseFrom(data);

            // TODO: This code is tied to ZK anyway, so for now leaving it as is,
            // will refactor when whole region assignment will be abstracted from ZK
            BaseCoordinatedStateManager csm =
              (BaseCoordinatedStateManager) server.getCoordinatedStateManager();
            OpenRegionCoordination openRegionCoordination = csm.getOpenRegionCoordination();

            ZkOpenRegionCoordination.ZkOpenRegionDetails zkOrd =
              new ZkOpenRegionCoordination.ZkOpenRegionDetails();
            zkOrd.setVersion(stat.getVersion());
            zkOrd.setServerName(csm.getServer().getServerName());

            handleRegion(rt, openRegionCoordination, zkOrd);
          } catch (KeeperException e) {
            server.abort("Unexpected ZK exception reading unassigned node data", e);
          } catch (DeserializationException e) {
            server.abort("Unexpected exception deserializing node data", e);
          }
        }
      });
    }
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
    if (useZKForAssignment) {
      // Disabling so should not be reassigned, just delete the CLOSED node
      LOG.debug("Table being disabled so deleting ZK node and removing from " +
        "regions in transition, skipping assignment of region " +
          regionInfo.getRegionNameAsString());
      String encodedName = regionInfo.getEncodedName();
      deleteNodeInStates(encodedName, "closed", null,
        EventType.RS_ZK_REGION_CLOSED, EventType.M_ZK_REGION_OFFLINE);
    }
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
   * OFFLINE state or not in transition (in-memory not zk), and of course, the
   * chosen server is up and running (It may have just crashed!).  If the
   * in-memory checks pass, the zk node is forced to OFFLINE before assigning.
   *
   * @param region server to be assigned
   * @param setOfflineInZK whether ZK node should be created/transitioned to an
   *                       OFFLINE state before assigning the region
   */
  public void assign(HRegionInfo region, boolean setOfflineInZK) {
    assign(region, setOfflineInZK, false);
  }

  /**
   * Use care with forceNewPlan. It could cause double assignment.
   */
  public void assign(HRegionInfo region,
      boolean setOfflineInZK, boolean forceNewPlan) {
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
        assign(state, setOfflineInZK && useZKForAssignment, forceNewPlan);
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
        AtomicInteger counter = new AtomicInteger(0);
        Map<String, Integer> offlineNodesVersions = new ConcurrentHashMap<String, Integer>();
        OfflineCallback cb = new OfflineCallback(
          watcher, destination, counter, offlineNodesVersions);
        Map<String, RegionPlan> plans = new HashMap<String, RegionPlan>(regions.size());
        List<RegionState> states = new ArrayList<RegionState>(regions.size());
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
              } else if (!useZKForAssignment
                  || asyncSetOfflineInZooKeeper(state, cb, destination)) {
                RegionPlan plan = new RegionPlan(region, state.getServerName(), destination);
                plans.put(encodedName, plan);
                states.add(state);
                continue;
              }
            }
            // Reassign if the region wasn't on a dead server
            if (!onDeadServer) {
              LOG.info("failed to force region state to offline or "
                + "failed to set it offline in ZK, will reassign later: " + region);
              failedToOpenRegions.add(region); // assign individually later
            }
          }
          // Release the lock, this region is excluded from bulk assign because
          // we can't update its state, or set its znode to offline.
          Lock lock = locks.remove(encodedName);
          lock.unlock();
        }

        if (useZKForAssignment) {
          // Wait until all unassigned nodes have been put up and watchers set.
          int total = states.size();
          for (int oldCounter = 0; !server.isStopped();) {
            int count = counter.get();
            if (oldCounter != count) {
              LOG.debug(destination.toString() + " unassigned znodes=" + count +
                " of total=" + total + "; oldCounter=" + oldCounter);
              oldCounter = count;
            }
            if (count >= total) break;
            Thread.sleep(5);
          }
        }

        if (server.isStopped()) {
          return false;
        }

        // Add region plans, so we can updateTimers when one region is opened so
        // that unnecessary timeout on RIT is reduced.
        this.addPlans(plans);

        List<Triple<HRegionInfo, Integer, List<ServerName>>> regionOpenInfos =
          new ArrayList<Triple<HRegionInfo, Integer, List<ServerName>>>(states.size());
        for (RegionState state: states) {
          HRegionInfo region = state.getRegion();
          String encodedRegionName = region.getEncodedName();
          Integer nodeVersion = offlineNodesVersions.get(encodedRegionName);
          if (useZKForAssignment && (nodeVersion == null || nodeVersion == -1)) {
            LOG.warn("failed to offline in zookeeper: " + region);
            failedToOpenRegions.add(region); // assign individually later
            Lock lock = locks.remove(encodedRegionName);
            lock.unlock();
          } else {
            regionStates.updateRegionState(
              region, State.PENDING_OPEN, destination);
            List<ServerName> favoredNodes = ServerName.EMPTY_SERVER_LIST;
            if (this.shouldAssignRegionsWithFavoredNodes) {
              favoredNodes = ((FavoredNodeLoadBalancer)this.balancer).getFavoredNodes(region);
            }
            regionOpenInfos.add(new Triple<HRegionInfo, Integer,  List<ServerName>>(
              region, nodeVersion, favoredNodes));
          }
        }

        // Move on to open regions.
        try {
          // Send OPEN RPC. If it fails on a IOE or RemoteException,
          // regions will be assigned individually.
          long maxWaitTime = System.currentTimeMillis() +
            this.server.getConfiguration().
              getLong("hbase.regionserver.rpc.startup.waittime", 60000);
          for (int i = 1; i <= maximumAttempts && !server.isStopped(); i++) {
            try {
              // regionOpenInfos is empty if all regions are in failedToOpenRegions list
              if (regionOpenInfos.isEmpty()) {
                break;
              }
              List<RegionOpeningState> regionOpeningStateList = serverManager
                .sendRegionOpen(destination, regionOpenInfos);
              if (regionOpeningStateList == null) {
                // Failed getting RPC connection to this server
                return false;
              }
              for (int k = 0, n = regionOpeningStateList.size(); k < n; k++) {
                RegionOpeningState openingState = regionOpeningStateList.get(k);
                if (openingState != RegionOpeningState.OPENED) {
                  HRegionInfo region = regionOpenInfos.get(k).getFirst();
                  if (openingState == RegionOpeningState.ALREADY_OPENED) {
                    processAlreadyOpenedRegion(region, destination);
                  } else if (openingState == RegionOpeningState.FAILED_OPENING) {
                    // Failed opening this region, reassign it later
                    failedToOpenRegions.add(region);
                  } else {
                    LOG.warn("THIS SHOULD NOT HAPPEN: unknown opening state "
                      + openingState + " in assigning region " + region);
                  }
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
                  LOG.debug("Server is not yet up; waiting up to " +
                    (maxWaitTime - now) + "ms", e);
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
              }
              throw e;
            }
          }
        } catch (IOException e) {
          // Can be a socket timeout, EOF, NoRouteToHost, etc
          LOG.info("Unable to communicate with " + destination
            + " in order to assign regions, ", e);
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
      final RegionState state, final int versionOfClosingNode,
      final ServerName dest, final boolean transitionInZK,
      final ServerName src) {
    ServerName server = src;
    if (state != null) {
      server = state.getServerName();
    }
    long maxWaitTime = -1;
    for (int i = 1; i <= this.maximumAttempts; i++) {
      if (this.server.isStopped() || this.server.isAborted()) {
        LOG.debug("Server stopped/aborted; skipping unassign of " + region);
        return;
      }
      // ClosedRegionhandler can remove the server from this.regions
      if (!serverManager.isServerOnline(server)) {
        LOG.debug("Offline " + region.getRegionNameAsString()
          + ", no need to unassign since it's on a dead server: " + server);
        if (transitionInZK) {
          // delete the node. if no node exists need not bother.
          deleteClosingOrClosedNode(region, server);
        }
        if (state != null) {
          regionOffline(region);
        }
        return;
      }
      try {
        // Send CLOSE RPC
        if (serverManager.sendRegionClose(server, region,
          versionOfClosingNode, dest, transitionInZK)) {
          LOG.debug("Sent CLOSE to " + server + " for region " +
            region.getRegionNameAsString());
          if (useZKForAssignment && !transitionInZK && state != null) {
            // Retry to make sure the region is
            // closed so as to avoid double assignment.
            unassign(region, state, versionOfClosingNode,
              dest, transitionInZK, src);
          }
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
        boolean logRetries = true;
        if (t instanceof RegionServerAbortedException
            || t instanceof RegionServerStoppedException
            || t instanceof ServerNotRunningYetException) {
          // RS is aborting or stopping, we cannot offline the region since the region may need
          // to do WAL recovery. Until we see  the RS expiration, we should retry.
          sleepTime = 1 + conf.getInt(RpcClient.FAILED_SERVER_EXPIRY_KEY,
            RpcClient.FAILED_SERVER_EXPIRY_DEFAULT);

        } else if (t instanceof NotServingRegionException) {
          LOG.debug("Offline " + region.getRegionNameAsString()
            + ", it's not any more on " + server, t);
          if (transitionInZK) {
            deleteClosingOrClosedNode(region, server);
          }
          if (state != null) {
            regionOffline(region);
          }
          return;
        } else if ((t instanceof FailedServerException) || (state != null &&
            t instanceof RegionAlreadyInTransitionException)) {
          if(t instanceof FailedServerException) {
            sleepTime = 1 + conf.getInt(RpcClient.FAILED_SERVER_EXPIRY_KEY,
                  RpcClient.FAILED_SERVER_EXPIRY_DEFAULT);
          } else {
            // RS is already processing this region, only need to update the timestamp
            LOG.debug("update " + state + " the timestamp.");
            state.updateTimestampToNow();
            if (maxWaitTime < 0) {
              maxWaitTime =
                  EnvironmentEdgeManager.currentTime()
                      + conf.getLong(ALREADY_IN_TRANSITION_WAITTIME,
                        DEFAULT_ALREADY_IN_TRANSITION_WAITTIME);
            }
            long now = EnvironmentEdgeManager.currentTime();
            if (now < maxWaitTime) {
              LOG.debug("Region is already in transition; "
                + "waiting up to " + (maxWaitTime - now) + "ms", t);
              sleepTime = 100;
              i--; // reset the try count
              logRetries = false;
            }
          }
        }

        try {
          if (sleepTime > 0) {
            Thread.sleep(sleepTime);
          }
        } catch (InterruptedException ie) {
          LOG.warn("Failed to unassign "
            + region.getRegionNameAsString() + " since interrupted", ie);
          Thread.currentThread().interrupt();
          if (state != null) {
            regionStates.updateRegionState(region, State.FAILED_CLOSE);
          }
          return;
        }

        if (logRetries) {
          LOG.info("Server " + server + " returned " + t + " for "
            + region.getRegionNameAsString() + ", try=" + i
            + " of " + this.maximumAttempts, t);
          // Presume retry or server will expire.
        }
      }
    }
    // Run out of attempts
    if (state != null) {
      regionStates.updateRegionState(region, State.FAILED_CLOSE);
    }
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

    ServerName sn = state.getServerName();
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
      unassign(region, state, -1, null, false, null);
      state = regionStates.getRegionState(region);
      if (state.isFailedClose()) {
        // If we can't close the region, we can't re-assign
        // it so as to avoid possible double assignment/data loss.
        LOG.info("Skip assigning " +
          region + ", we couldn't close it: " + state);
        return null;
      }
    case OFFLINE:
      // This region could have been open on this server
      // for a while. If the server is dead and not processed
      // yet, we can move on only if the meta shows the
      // region is not on this server actually, or on a server
      // not dead, or dead and processed already.
      // In case not using ZK, we don't need this check because
      // we have the latest info in memory, and the caller
      // will do another round checking any way.
      if (useZKForAssignment
          && regionStates.isServerDeadAndNotProcessed(sn)
          && wasRegionOnDeadServerByMeta(region, sn)) {
        if (!regionStates.isRegionInTransition(region)) {
          LOG.info("Updating the state to " + State.OFFLINE + " to allow to be reassigned by SSH");
          regionStates.updateRegionState(region, State.OFFLINE);
        }
        LOG.info("Skip assigning " + region.getRegionNameAsString()
            + ", it is on a dead but not processed yet server: " + sn);
        return null;
      }
    case CLOSED:
      break;
    default:
      LOG.error("Trying to assign region " + region
        + ", which is " + state);
      return null;
    }
    return state;
  }

  @SuppressWarnings("deprecation")
  protected boolean wasRegionOnDeadServerByMeta(
      final HRegionInfo region, final ServerName sn) {
    try {
      if (region.isMetaRegion()) {
        ServerName server = this.server.getMetaTableLocator().
          getMetaRegionLocation(this.server.getZooKeeper());
        return regionStates.isServerDeadAndNotProcessed(server);
      }
      while (!server.isStopped()) {
        try {
          this.server.getMetaTableLocator().waitMetaRegionLocation(server.getZooKeeper());
          Result r = MetaTableAccessor.getRegionResult(server.getConnection(),
            region.getRegionName());
          if (r == null || r.isEmpty()) return false;
          ServerName server = HRegionInfo.getServerName(r);
          return regionStates.isServerDeadAndNotProcessed(server);
        } catch (IOException ioe) {
          LOG.info("Received exception accessing hbase:meta during force assign "
            + region.getRegionNameAsString() + ", retrying", ioe);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted accessing hbase:meta", e);
    }
    // Call is interrupted or server is stopped.
    return regionStates.isServerDeadAndNotProcessed(sn);
  }

  /**
   * Caller must hold lock on the passed <code>state</code> object.
   * @param state
   * @param setOfflineInZK
   * @param forceNewPlan
   */
  private void assign(RegionState state,
      final boolean setOfflineInZK, final boolean forceNewPlan) {
    long startTime = EnvironmentEdgeManager.currentTime();
    try {
      Configuration conf = server.getConfiguration();
      RegionState currentState = state;
      int versionOfOfflineNode = -1;
      RegionPlan plan = null;
      long maxWaitTime = -1;
      HRegionInfo region = state.getRegion();
      RegionOpeningState regionOpenState;
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
        if (setOfflineInZK && versionOfOfflineNode == -1) {
          // get the version of the znode after setting it to OFFLINE.
          // versionOfOfflineNode will be -1 if the znode was not set to OFFLINE
          versionOfOfflineNode = setOfflineInZooKeeper(currentState, plan.getDestination());
          if (versionOfOfflineNode != -1) {
            if (isDisabledorDisablingRegionInRIT(region)) {
              return;
            }
            // In case of assignment from EnableTableHandler table state is ENABLING. Any how
            // EnableTableHandler will set ENABLED after assigning all the table regions. If we
            // try to set to ENABLED directly then client API may think table is enabled.
            // When we have a case such as all the regions are added directly into hbase:meta and we call
            // assignRegion then we need to make the table ENABLED. Hence in such case the table
            // will not be in ENABLING or ENABLED state.
            TableName tableName = region.getTable();
            if (!tableStateManager.isTableState(tableName,
              ZooKeeperProtos.Table.State.ENABLED, ZooKeeperProtos.Table.State.ENABLING)) {
              LOG.debug("Setting table " + tableName + " to ENABLED state.");
              setEnabledTable(tableName);
            }
          }
        }
        if (setOfflineInZK && versionOfOfflineNode == -1) {
          LOG.info("Unable to set offline in ZooKeeper to assign " + region);
          // Setting offline in ZK must have been failed due to ZK racing or some
          // exception which may make the server to abort. If it is ZK racing,
          // we should retry since we already reset the region state,
          // existing (re)assignment will fail anyway.
          if (!server.isAborted()) {
            continue;
          }
        }
        LOG.info("Assigning " + region.getRegionNameAsString() +
            " to " + plan.getDestination().toString());
        // Transition RegionState to PENDING_OPEN
        currentState = regionStates.updateRegionState(region,
          State.PENDING_OPEN, plan.getDestination());

        boolean needNewPlan;
        final String assignMsg = "Failed assignment of " + region.getRegionNameAsString() +
            " to " + plan.getDestination();
        try {
          List<ServerName> favoredNodes = ServerName.EMPTY_SERVER_LIST;
          if (this.shouldAssignRegionsWithFavoredNodes) {
            favoredNodes = ((FavoredNodeLoadBalancer)this.balancer).getFavoredNodes(region);
          }
          regionOpenState = serverManager.sendRegionOpen(
              plan.getDestination(), region, versionOfOfflineNode, favoredNodes);

          if (regionOpenState == RegionOpeningState.FAILED_OPENING) {
            // Failed opening this region, looping again on a new server.
            needNewPlan = true;
            LOG.warn(assignMsg + ", regionserver says 'FAILED_OPENING', " +
                " trying to assign elsewhere instead; " +
                "try=" + i + " of " + this.maximumAttempts);
          } else {
            // we're done
            if (regionOpenState == RegionOpeningState.ALREADY_OPENED) {
              processAlreadyOpenedRegion(region, plan.getDestination());
            }
            return;
          }

        } catch (Throwable t) {
          if (t instanceof RemoteException) {
            t = ((RemoteException) t).unwrapRemoteException();
          }
          previousException = t;

          // Should we wait a little before retrying? If the server is starting it's yes.
          // If the region is already in transition, it's yes as well: we want to be sure that
          //  the region will get opened but we don't want a double assignment.
          boolean hold = (t instanceof RegionAlreadyInTransitionException ||
              t instanceof ServerNotRunningYetException);

          // In case socket is timed out and the region server is still online,
          // the openRegion RPC could have been accepted by the server and
          // just the response didn't go through.  So we will retry to
          // open the region on the same server to avoid possible
          // double assignment.
          boolean retry = !hold && (t instanceof java.net.SocketTimeoutException
              && this.serverManager.isServerOnline(plan.getDestination()));


          if (hold) {
            LOG.warn(assignMsg + ", waiting a little before trying on the same region server " +
              "try=" + i + " of " + this.maximumAttempts, t);

            if (maxWaitTime < 0) {
              if (t instanceof RegionAlreadyInTransitionException) {
                maxWaitTime = EnvironmentEdgeManager.currentTime()
                  + this.server.getConfiguration().getLong(ALREADY_IN_TRANSITION_WAITTIME,
                    DEFAULT_ALREADY_IN_TRANSITION_WAITTIME);
              } else {
                maxWaitTime = EnvironmentEdgeManager.currentTime()
                  + this.server.getConfiguration().getLong(
                    "hbase.regionserver.rpc.startup.waittime", 60000);
              }
            }
            try {
              needNewPlan = false;
              long now = EnvironmentEdgeManager.currentTime();
              if (now < maxWaitTime) {
                LOG.debug("Server is not yet up or region is already in transition; "
                  + "waiting up to " + (maxWaitTime - now) + "ms", t);
                Thread.sleep(100);
                i--; // reset the try count
              } else if (!(t instanceof RegionAlreadyInTransitionException)) {
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
            needNewPlan = false;
            i--; // we want to retry as many times as needed as long as the RS is not dead.
            LOG.warn(assignMsg + ", trying to assign to the same region server due ", t);
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
            currentState = regionStates.updateRegionState(region, State.OFFLINE);
            versionOfOfflineNode = -1;
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

  private void processAlreadyOpenedRegion(HRegionInfo region, ServerName sn) {
    // Remove region from in-memory transition and unassigned node from ZK
    // While trying to enable the table the regions of the table were
    // already enabled.
    LOG.debug("ALREADY_OPENED " + region.getRegionNameAsString()
      + " to " + sn);
    String encodedName = region.getEncodedName();
    
    //If use ZkForAssignment, region already Opened event should not be handled, 
    //leave it to zk event. See HBase-14407.
    if(useZKForAssignment){
      String node = ZKAssign.getNodeName(watcher, encodedName);
      Stat stat = new Stat();
      try {
        byte[] existingBytes = ZKUtil.getDataNoWatch(watcher, node, stat);
        if(existingBytes!=null){
          RegionTransition rt= RegionTransition.parseFrom(existingBytes);
          EventType et = rt.getEventType();
          if (et.equals(EventType.RS_ZK_REGION_OPENED)) {
            LOG.debug("ALREADY_OPENED " + region.getRegionNameAsString()
              + " and node in "+et+" state");
            return;
          }
        }
      } catch (KeeperException ke) {
        LOG.warn("Unexpected ZK exception getData " + node
          + " node for the region " + encodedName, ke);
      } catch (DeserializationException e) {
        LOG.warn("Get RegionTransition from zk deserialization failed! ", e);
      }
      
      deleteNodeInStates(encodedName, "offline", sn, EventType.M_ZK_REGION_OFFLINE);
    }
    
    regionStates.regionOnline(region, sn);
  }

  private boolean isDisabledorDisablingRegionInRIT(final HRegionInfo region) {
    if (this.tableStateManager.isTableState(region.getTable(),
        ZooKeeperProtos.Table.State.DISABLED,
        ZooKeeperProtos.Table.State.DISABLING) || replicasToClose.contains(region)) {
      LOG.info("Table " + region.getTable() + " is disabled or disabling;"
        + " skipping assign of " + region.getRegionNameAsString());
      offlineDisabledRegion(region);
      return true;
    }
    return false;
  }

  /**
   * Set region as OFFLINED up in zookeeper
   *
   * @param state
   * @return the version of the offline node if setting of the OFFLINE node was
   *         successful, -1 otherwise.
   */
  private int setOfflineInZooKeeper(final RegionState state, final ServerName destination) {
    if (!state.isClosed() && !state.isOffline()) {
      String msg = "Unexpected state : " + state + " .. Cannot transit it to OFFLINE.";
      this.server.abort(msg, new IllegalStateException(msg));
      return -1;
    }
    regionStates.updateRegionState(state.getRegion(), State.OFFLINE);
    int versionOfOfflineNode;
    try {
      // get the version after setting the znode to OFFLINE
      versionOfOfflineNode = ZKAssign.createOrForceNodeOffline(watcher,
        state.getRegion(), destination);
      if (versionOfOfflineNode == -1) {
        LOG.warn("Attempted to create/force node into OFFLINE state before "
            + "completing assignment but failed to do so for " + state);
        return -1;
      }
    } catch (KeeperException e) {
      server.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      return -1;
    }
    return versionOfOfflineNode;
  }

  /**
   * @param region the region to assign
   * @return Plan for passed <code>region</code> (If none currently, it creates one or
   * if no servers to assign, it returns null).
   */
  private RegionPlan getRegionPlan(final HRegionInfo region,
      final boolean forceNewPlan)  throws HBaseIOException {
    return getRegionPlan(region, null, forceNewPlan);
  }

  /**
   * @param region the region to assign
   * @param serverToExclude Server to exclude (we know its bad). Pass null if
   * all servers are thought to be assignable.
   * @param forceNewPlan If true, then if an existing plan exists, a new plan
   * will be generated.
   * @return Plan for passed <code>region</code> (If none currently, it creates one or
   * if no servers to assign, it returns null).
   */
  private RegionPlan getRegionPlan(final HRegionInfo region,
      final ServerName serverToExclude, final boolean forceNewPlan) throws HBaseIOException {
    // Pickup existing plan or make a new one
    final String encodedName = region.getEncodedName();
    final List<ServerName> destServers =
      serverManager.createDestinationServersList(serverToExclude);

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
      }
    }

    if (newPlan) {
      ServerName destination = balancer.randomAssignment(region, destServers);
      if (destination == null) {
        LOG.warn("Can't find a destination for " + encodedName);
        return null;
      }
      synchronized (this.regionPlans) {
        randomPlan = new RegionPlan(region, null, destination);
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
      LOG.debug("No previous transition plan found (or ignoring " + "an existing plan) for "
          + region.getRegionNameAsString() + "; generated random plan=" + randomPlan + "; "
          + destServers.size() + " (online=" + serverManager.getOnlineServers().size()
          + ") available servers, forceNewPlan=" + forceNewPlan);
      return randomPlan;
    }
    LOG.debug("Using pre-existing plan for " +
      region.getRegionNameAsString() + "; plan=" + existingPlan);
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
    unassign(region, false);
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
   * @param force if region should be closed even if already closing
   */
  public void unassign(HRegionInfo region, boolean force, ServerName dest) {
    // TODO: Method needs refactoring.  Ugly buried returns throughout.  Beware!
    LOG.debug("Starting unassign of " + region.getRegionNameAsString()
      + " (offlining), current state: " + regionStates.getRegionState(region));

    String encodedName = region.getEncodedName();
    // Grab the state of this region and synchronize on it
    int versionOfClosingNode = -1;
    // We need a lock here as we're going to do a put later and we don't want multiple states
    //  creation
    ReentrantLock lock = locker.acquireLock(encodedName);
    RegionState state = regionStates.getRegionTransitionState(encodedName);
    boolean reassign = true;
    try {
      if (state == null) {
        // Region is not in transition.
        // We can unassign it only if it's not SPLIT/MERGED.
        state = regionStates.getRegionState(encodedName);
        if (state != null && state.isUnassignable()) {
          LOG.info("Attempting to unassign " + state + ", ignored");
          // Offline region will be reassigned below
          return;
        }
        // Create the znode in CLOSING state
        try {
          if (state == null || state.getServerName() == null) {
            // We don't know where the region is, offline it.
            // No need to send CLOSE RPC
            LOG.warn("Attempting to unassign a region not in RegionStates "
              + region.getRegionNameAsString() + ", offlined");
            regionOffline(region);
            return;
          }
          if (useZKForAssignment) {
            versionOfClosingNode = ZKAssign.createNodeClosing(
              watcher, region, state.getServerName());
            if (versionOfClosingNode == -1) {
              LOG.info("Attempting to unassign " +
                region.getRegionNameAsString() + " but ZK closing node "
                + "can't be created.");
              reassign = false; // not unassigned at all
              return;
            }
          }
        } catch (KeeperException e) {
          if (e instanceof NodeExistsException) {
            // Handle race between master initiated close and regionserver
            // orchestrated splitting. See if existing node is in a
            // SPLITTING or SPLIT state.  If so, the regionserver started
            // an op on node before we could get our CLOSING in.  Deal.
            NodeExistsException nee = (NodeExistsException)e;
            String path = nee.getPath();
            try {
              if (isSplitOrSplittingOrMergedOrMerging(path)) {
                LOG.debug(path + " is SPLIT or SPLITTING or MERGED or MERGING; " +
                  "skipping unassign because region no longer exists -- its split or merge");
                reassign = false; // no need to reassign for split/merged region
                return;
              }
            } catch (KeeperException.NoNodeException ke) {
              LOG.warn("Failed getData on SPLITTING/SPLIT at " + path +
                "; presuming split and that the region to unassign, " +
                encodedName + ", no longer exists -- confirm", ke);
              return;
            } catch (KeeperException ke) {
              LOG.error("Unexpected zk state", ke);
            } catch (DeserializationException de) {
              LOG.error("Failed parse", de);
            }
          }
          // If we get here, don't understand whats going on -- abort.
          server.abort("Unexpected ZK exception creating node CLOSING", e);
          reassign = false; // heading out already
          return;
        }
        state = regionStates.updateRegionState(region, State.PENDING_CLOSE);
      } else if (state.isFailedOpen()) {
        // The region is not open yet
        regionOffline(region);
        return;
      } else if (force && state.isPendingCloseOrClosing()) {
        LOG.debug("Attempting to unassign " + region.getRegionNameAsString() +
          " which is already " + state.getState()  +
          " but forcing to send a CLOSE RPC again ");
        if (state.isFailedClose()) {
          state = regionStates.updateRegionState(region, State.PENDING_CLOSE);
        }
        state.updateTimestampToNow();
      } else {
        LOG.debug("Attempting to unassign " +
          region.getRegionNameAsString() + " but it is " +
          "already in transition (" + state.getState() + ", force=" + force + ")");
        return;
      }

      unassign(region, state, versionOfClosingNode, dest, useZKForAssignment, null);
    } finally {
      lock.unlock();

      // Region is expected to be reassigned afterwards
      if (!replicasToClose.contains(region) && reassign && regionStates.isRegionOffline(region)) {
        assign(region, true);
      }
    }
  }

  public void unassign(HRegionInfo region, boolean force){
     unassign(region, force, null);
  }

  /**
   * @param region regioninfo of znode to be deleted.
   */
  public void deleteClosingOrClosedNode(HRegionInfo region, ServerName sn) {
    String encodedName = region.getEncodedName();
    deleteNodeInStates(encodedName, "closing", sn, EventType.M_ZK_REGION_CLOSING,
      EventType.RS_ZK_REGION_CLOSED);
  }

  /**
   * @param path
   * @return True if znode is in SPLIT or SPLITTING or MERGED or MERGING state.
   * @throws KeeperException Can happen if the znode went away in meantime.
   * @throws DeserializationException
   */
  private boolean isSplitOrSplittingOrMergedOrMerging(final String path)
      throws KeeperException, DeserializationException {
    boolean result = false;
    // This may fail if the SPLIT or SPLITTING or MERGED or MERGING znode gets
    // cleaned up before we can get data from it.
    byte [] data = ZKAssign.getData(watcher, path);
    if (data == null) {
      LOG.info("Node " + path + " is gone");
      return false;
    }
    RegionTransition rt = RegionTransition.parseFrom(data);
    switch (rt.getEventType()) {
    case RS_ZK_REQUEST_REGION_SPLIT:
    case RS_ZK_REGION_SPLIT:
    case RS_ZK_REGION_SPLITTING:
    case RS_ZK_REQUEST_REGION_MERGE:
    case RS_ZK_REGION_MERGED:
    case RS_ZK_REGION_MERGING:
      result = true;
      break;
    default:
      LOG.info("Node " + path + " is in " + rt.getEventType());
      break;
    }
    return result;
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
   * <p>
   * Forcibly unsets the current meta region location in ZooKeeper and assigns
   * hbase:meta to a random RegionServer.
   * @param hri TODO
   * @throws KeeperException
   */
  public void assignMeta(HRegionInfo hri) throws KeeperException {
    this.server.getMetaTableLocator().deleteMetaLocation(this.watcher, hri.getReplicaId());
    assign(hri, true);
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
    Map<ServerName, List<HRegionInfo>> bulkPlan
      = balancer.roundRobinAssignment(regions, servers);
    if (bulkPlan == null) {
      throw new IOException("Unable to determine a plan to assign region(s)");
    }

    processFavoredNodes(regions);
    assign(regions.size(), servers.size(),
      "round-robin=true", bulkPlan);
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
        if (!assign(plan.getKey(), plan.getValue())) {
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
          ZooKeeperProtos.Table.State.ENABLED)) {
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
   * Wait until no regions in transition.
   * @param timeout How long to wait.
   * @return True if nothing in regions in transition.
   * @throws InterruptedException
   */
  boolean waitUntilNoRegionsInTransition(final long timeout)
      throws InterruptedException {
    // Blocks until there are no regions in transition. It is possible that
    // there
    // are regions in transition immediately after this returns but guarantees
    // that if it returns without an exception that there was a period of time
    // with no regions in transition from the point-of-view of the in-memory
    // state of the Master.
    final long endTime = System.currentTimeMillis() + timeout;

    while (!this.server.isStopped() && regionStates.isRegionsInTransition()
        && endTime > System.currentTimeMillis()) {
      regionStates.waitForUpdate(100);
    }

    return !regionStates.isRegionsInTransition();
  }

  /**
   * Rebuild the list of user regions and assignment information.
   * <p>
   * Returns a set of servers that are not found to be online that hosted
   * some regions.
   * @return set of servers not online that hosted some regions per meta
   * @throws IOException
   */
  Set<ServerName> rebuildUserRegions() throws
      IOException, KeeperException, CoordinatedStateException {
    Set<TableName> disabledOrEnablingTables = tableStateManager.getTablesInStates(
      ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.ENABLING);

    Set<TableName> disabledOrDisablingOrEnabling = tableStateManager.getTablesInStates(
      ZooKeeperProtos.Table.State.DISABLED,
      ZooKeeperProtos.Table.State.DISABLING,
      ZooKeeperProtos.Table.State.ENABLING);

    // Region assignment from META
    List<Result> results = MetaTableAccessor.fullScanOfMeta(server.getConnection());
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
        if (tableStateManager.isTableState(regionInfo.getTable(),
             ZooKeeperProtos.Table.State.DISABLED)) {
          // force region to forget it hosts for disabled/disabling tables.
          // see HBASE-13326
          lastHost = null;
          regionLocation = null;
        }
        regionStates.createRegionState(regionInfo, state, regionLocation, lastHost);
        if (!regionStates.isRegionInState(regionInfo, State.OPEN)) {
          // Region is not open (either offline or in transition), skip
          continue;
        }
        TableName tableName = regionInfo.getTable();
        if (!onlineServers.contains(regionLocation)) {
          // Region is located on a server that isn't online
          offlineServers.add(regionLocation);
          if (useZKForAssignment) {
            regionStates.regionOffline(regionInfo);
          }
        } else if (!disabledOrEnablingTables.contains(tableName)) {
          // Region is being served and on an active server
          // add only if region not in disabled or enabling table
          regionStates.regionOnline(regionInfo, regionLocation);
          balancer.regionOnline(regionInfo, regionLocation);
        } else if (useZKForAssignment) {
          regionStates.regionOffline(regionInfo);
        }
        // need to enable the table if not disabled or disabling or enabling
        // this will be used in rolling restarts
        if (!disabledOrDisablingOrEnabling.contains(tableName)
          && !getTableStateManager().isTableState(tableName,
            ZooKeeperProtos.Table.State.ENABLED)) {
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
      throws KeeperException, IOException, CoordinatedStateException {
    Set<TableName> disablingTables =
      tableStateManager.getTablesInStates(ZooKeeperProtos.Table.State.DISABLING);
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
      throws KeeperException, IOException, CoordinatedStateException {
    Set<TableName> enablingTables = tableStateManager.
      getTablesInStates(ZooKeeperProtos.Table.State.ENABLING);
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
   * Processes list of dead servers from result of hbase:meta scan and regions in RIT
   * <p>
   * This is used for failover to recover the lost regions that belonged to
   * RegionServers which failed while there was no active master or regions
   * that were in RIT.
   * <p>
   *
   *
   * @param deadServers
   *          The list of dead servers which failed while there was no active
   *          master. Can be null.
   * @throws IOException
   * @throws KeeperException
   */
  private void processDeadServersAndRecoverLostRegions(
      Set<ServerName> deadServers) throws IOException, KeeperException {
    if (deadServers != null && !deadServers.isEmpty()) {
      for (ServerName serverName: deadServers) {
        if (!serverManager.isServerDead(serverName)) {
          serverManager.expireServer(serverName); // Let SSH do region re-assign
        }
      }
    }

    List<String> nodes = useZKForAssignment ?
      ZKUtil.listChildrenAndWatchForNewChildren(watcher, watcher.assignmentZNode)
      : ZKUtil.listChildrenNoWatch(watcher, watcher.assignmentZNode);
    if (nodes != null && !nodes.isEmpty()) {
      for (String encodedRegionName : nodes) {
        processRegionInTransition(encodedRegionName, null);
      }
    } else if (!useZKForAssignment) {
      processRegionInTransitionZkLess();
    }
  }

  void processRegionInTransitionZkLess() {
 // We need to send RPC call again for PENDING_OPEN/PENDING_CLOSE regions
    // in case the RPC call is not sent out yet before the master was shut down
    // since we update the state before we send the RPC call. We can't update
    // the state after the RPC call. Otherwise, we don't know what's happened
    // to the region if the master dies right after the RPC call is out.
    Map<String, RegionState> rits = regionStates.getRegionsInTransition();
    for (RegionState regionState : rits.values()) {
      LOG.info("Processing " + regionState);
      ServerName serverName = regionState.getServerName();
      // Server could be null in case of FAILED_OPEN when master cannot find a region plan. In that
      // case, try assigning it here.
      if (serverName != null
          && !serverManager.getOnlineServers().containsKey(serverName)) {
        LOG.info("Server " + serverName + " isn't online. SSH will handle this");
        continue;
      }
      HRegionInfo regionInfo = regionState.getRegion();
      State state = regionState.getState();

      switch (state) {
      case CLOSED:
        invokeAssign(regionInfo);
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
                RegionOpeningState regionOpenState = serverManager.sendRegionOpen(
                  serverName, hri, -1, favoredNodes);

                if (regionOpenState == RegionOpeningState.FAILED_OPENING) {
                  // Failed opening this region, this means the target server didn't get
                  // the original region open RPC, so re-assign it with a new plan
                  LOG.debug("Got failed_opening in retry sendRegionOpen for "
                    + regionState + ", re-assign it");
                  invokeAssign(hri, true);
                }
                return; // Done.
              } catch (Throwable t) {
                if (t instanceof RemoteException) {
                  t = ((RemoteException) t).unwrapRemoteException();
                }
                // In case SocketTimeoutException/FailedServerException, retry
                if (t instanceof java.net.SocketTimeoutException
                    || t instanceof FailedServerException) {
                  Threads.sleep(100);
                  continue;
                }
                // For other exceptions, re-assign it
                LOG.debug("Got exception in retry sendRegionOpen for "
                  + regionState + ", re-assign it", t);
                invokeAssign(hri);
                return; // Done.
              }
            }
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
                if (!serverManager.sendRegionClose(serverName, hri, -1, null, false)) {
                  // This means the region is still on the target server
                  LOG.debug("Got false in retry sendRegionClose for "
                    + regionState + ", re-close it");
                  invokeUnAssign(hri);
                }
                return; // Done.
              } catch (Throwable t) {
                if (t instanceof RemoteException) {
                  t = ((RemoteException) t).unwrapRemoteException();
                }
                // In case SocketTimeoutException/FailedServerException, retry
                if (t instanceof java.net.SocketTimeoutException
                    || t instanceof FailedServerException) {
                  Threads.sleep(100);
                  continue;
                }
                if (!(t instanceof NotServingRegionException
                    || t instanceof RegionAlreadyInTransitionException)) {
                  // NotServingRegionException/RegionAlreadyInTransitionException
                  // means the target server got the original region close request.
                  // For other exceptions, re-close it
                  LOG.debug("Got exception in retry sendRegionClose for "
                    + regionState + ", re-close it", t);
                  invokeUnAssign(hri);
                }
                return; // Done.
              }
            }
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
  void clearRegionPlan(final HRegionInfo region) {
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
    invokeAssign(regionInfo, true);
  }

  void invokeAssign(HRegionInfo regionInfo, boolean newPlan) {
    threadPoolExecutorService.submit(new AssignCallable(this, regionInfo, newPlan));
  }

  void invokeUnAssign(HRegionInfo regionInfo) {
    threadPoolExecutorService.submit(new UnAssignCallable(this, regionInfo));
  }

  public ServerHostRegion isCarryingMeta(ServerName serverName) {
    return isCarryingRegion(serverName, HRegionInfo.FIRST_META_REGIONINFO);
  }

  public ServerHostRegion isCarryingMetaReplica(ServerName serverName, int replicaId) {
    return isCarryingRegion(serverName,
        RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO, replicaId));
  }

  public ServerHostRegion isCarryingMetaReplica(ServerName serverName, HRegionInfo metaHri) {
    return isCarryingRegion(serverName, metaHri);
  }

  /**
   * Check if the shutdown server carries the specific region.
   * We have a bunch of places that store region location
   * Those values aren't consistent. There is a delay of notification.
   * The location from zookeeper unassigned node has the most recent data;
   * but the node could be deleted after the region is opened by AM.
   * The AM's info could be old when OpenedRegionHandler
   * processing hasn't finished yet when server shutdown occurs.
   * @return whether the serverName currently hosts the region
   */
  private ServerHostRegion isCarryingRegion(ServerName serverName, HRegionInfo hri) {
    RegionTransition rt = null;
    try {
      byte [] data = ZKAssign.getData(watcher, hri.getEncodedName());
      // This call can legitimately come by null
      rt = data == null? null: RegionTransition.parseFrom(data);
    } catch (KeeperException e) {
      server.abort("Exception reading unassigned node for region=" + hri.getEncodedName(), e);
    } catch (DeserializationException e) {
      server.abort("Exception parsing unassigned node for region=" + hri.getEncodedName(), e);
    }

    ServerName addressFromZK = rt != null? rt.getServerName():  null;
    if (addressFromZK != null) {
      // if we get something from ZK, we will use the data
      boolean matchZK = addressFromZK.equals(serverName);
      LOG.debug("Checking region=" + hri.getRegionNameAsString() + ", zk server=" + addressFromZK +
        " current=" + serverName + ", matches=" + matchZK);
      return matchZK ? ServerHostRegion.HOSTING_REGION : ServerHostRegion.NOT_HOSTING_REGION;
    }

    ServerName addressFromAM = regionStates.getRegionServerOfRegion(hri);
    if (addressFromAM != null) {
      boolean matchAM = addressFromAM.equals(serverName);
      LOG.debug("based on AM, current region=" + hri.getRegionNameAsString() +
        " is on server=" + (addressFromAM != null ? addressFromAM : "null") +
        " server being checked: " + serverName);
      return matchAM ? ServerHostRegion.HOSTING_REGION : ServerHostRegion.NOT_HOSTING_REGION;
    }

    if (hri.isMetaRegion() && RegionReplicaUtil.isDefaultReplica(hri)) {
      // For the Meta region (default replica), we can do one more check on MetaTableLocator
      final ServerName serverNameInZK =
          server.getMetaTableLocator().getMetaRegionLocation(this.server.getZooKeeper());
      LOG.debug("Based on MetaTableLocator, the META region is on server="
          + (serverNameInZK == null ? "null" : serverNameInZK)
          + " server being checked: " + serverName);
      if (serverNameInZK != null) {
        return serverNameInZK.equals(serverName) ?
            ServerHostRegion.HOSTING_REGION : ServerHostRegion.NOT_HOSTING_REGION;
      }
    }
    // Checked everywhere, if reaching here, we are sure that the server is not
    // carrying region.
    return ServerHostRegion.UNKNOWN;
  }

  /**
   * Process shutdown server removing any assignments.
   * @param sn Server that went down.
   * @return list of regions in transition on this server
   */
  public List<HRegionInfo> processServerShutdown(final ServerName sn) {
    // Clean out any existing assignment plans for this server
    synchronized (this.regionPlans) {
      for (Iterator <Map.Entry<String, RegionPlan>> i =
          this.regionPlans.entrySet().iterator(); i.hasNext();) {
        Map.Entry<String, RegionPlan> e = i.next();
        ServerName otherSn = e.getValue().getDestination();
        // The name will be null if the region is planned for a random assign.
        if (otherSn != null && otherSn.equals(sn)) {
          // Use iterator's remove else we'll get CME
          i.remove();
        }
      }
    }
    List<HRegionInfo> regions = regionStates.serverOffline(watcher, sn);
    for (Iterator<HRegionInfo> it = regions.iterator(); it.hasNext(); ) {
      HRegionInfo hri = it.next();
      String encodedName = hri.getEncodedName();

      // We need a lock on the region as we could update it
      Lock lock = locker.acquireLock(encodedName);
      try {
        RegionState regionState =
          regionStates.getRegionTransitionState(encodedName);
        if (regionState == null
            || (regionState.getServerName() != null && !regionState.isOnServer(sn))
            || !(regionState.isFailedClose() || regionState.isOffline()
              || regionState.isPendingOpenOrOpening())) {
          LOG.info("Skip " + regionState + " since it is not opening/failed_close"
            + " on the dead server any more: " + sn);
          it.remove();
        } else {
          try {
            // Delete the ZNode if exists
            ZKAssign.deleteNodeFailSilent(watcher, hri);
          } catch (KeeperException ke) {
            server.abort("Unexpected ZK exception deleting node " + hri, ke);
          }
          if (tableStateManager.isTableState(hri.getTable(),
              ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING)) {
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
    return regions;
  }

  /**
   * @param plan Plan to execute.
   */
  public void balance(final RegionPlan plan) {

    HRegionInfo hri = plan.getRegionInfo();
    TableName tableName = hri.getTable();
    if (tableStateManager.isTableState(tableName,
      ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING)) {
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
      unassign(hri, false, plan.getDestination());
    } finally {
      lock.unlock();
    }
  }

  public void stop() {
    shutdown(); // Stop executor service, etc
  }

  /**
   * Shutdown the threadpool executor service
   */
  public void shutdown() {
    // It's an immediate shutdown, so we're clearing the remaining tasks.
    synchronized (zkEventWorkerWaitingList){
      zkEventWorkerWaitingList.clear();
    }

    // Shutdown the threadpool executor service
    threadPoolExecutorService.shutdownNow();
    zkEventWorkers.shutdownNow();
    regionStateStore.stop();
  }

  protected void setEnabledTable(TableName tableName) {
    try {
      this.tableStateManager.setTableState(tableName,
        ZooKeeperProtos.Table.State.ENABLED);
    } catch (CoordinatedStateException e) {
      // here we can abort as it is the start up flow
      String errorMsg = "Unable to ensure that the table " + tableName
          + " will be" + " enabled because of a ZooKeeper issue";
      LOG.error(errorMsg);
      this.server.abort(errorMsg, e);
    }
  }

  /**
   * Set region as OFFLINED up in zookeeper asynchronously.
   * @param state
   * @return True if we succeeded, false otherwise (State was incorrect or failed
   * updating zk).
   */
  private boolean asyncSetOfflineInZooKeeper(final RegionState state,
      final AsyncCallback.StringCallback cb, final ServerName destination) {
    if (!state.isClosed() && !state.isOffline()) {
      this.server.abort("Unexpected state trying to OFFLINE; " + state,
        new IllegalStateException());
      return false;
    }
    regionStates.updateRegionState(state.getRegion(), State.OFFLINE);
    try {
      ZKAssign.asyncCreateNodeOffline(watcher, state.getRegion(),
        destination, cb, state);
    } catch (KeeperException e) {
      if (e instanceof NodeExistsException) {
        LOG.warn("Node for " + state.getRegion() + " already exists");
      } else {
        server.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      }
      return false;
    }
    return true;
  }

  private boolean deleteNodeInStates(String encodedName,
      String desc, ServerName sn, EventType... types) {
    try {
      for (EventType et: types) {
        if (ZKAssign.deleteNode(watcher, encodedName, et, sn)) {
          return true;
        }
      }
      LOG.info("Failed to delete the " + desc + " node for "
        + encodedName + ". The node type may not match");
    } catch (NoNodeException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("The " + desc + " node for " + encodedName + " already deleted");
      }
    } catch (KeeperException ke) {
      server.abort("Unexpected ZK exception deleting " + desc
        + " node for the region " + encodedName, ke);
    }
    return false;
  }

  private void deleteMergingNode(String encodedName, ServerName sn) {
    deleteNodeInStates(encodedName, "merging", sn, EventType.RS_ZK_REGION_MERGING,
      EventType.RS_ZK_REQUEST_REGION_MERGE, EventType.RS_ZK_REGION_MERGED);
  }

  private void deleteSplittingNode(String encodedName, ServerName sn) {
    deleteNodeInStates(encodedName, "splitting", sn, EventType.RS_ZK_REGION_SPLITTING,
      EventType.RS_ZK_REQUEST_REGION_SPLIT, EventType.RS_ZK_REGION_SPLIT);
  }

  private void onRegionFailedOpen(
      final HRegionInfo hri, final ServerName sn) {
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
            ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING) ||
            replicasToClose.contains(hri)) {
          offlineDisabledRegion(hri);
          return;
        }
        // ZK Node is in CLOSED state, assign it.
         regionStates.updateRegionState(hri, RegionState.State.CLOSED);
        // This below has to do w/ online enable/disable of a table
        removeClosedRegion(hri);
        try {
          getRegionPlan(hri, sn, true);
        } catch (HBaseIOException e) {
          LOG.warn("Failed to get region plan", e);
        }
        invokeAssign(hri, false);
      }
    }
  }

  private void onRegionOpen(
      final HRegionInfo hri, final ServerName sn, long openSeqNum) {
    regionOnline(hri, sn, openSeqNum);
    if (useZKForAssignment) {
      try {
        // Delete the ZNode if exists
        ZKAssign.deleteNodeFailSilent(watcher, hri);
      } catch (KeeperException ke) {
        server.abort("Unexpected ZK exception deleting node " + hri, ke);
      }
    }

    // reset the count, if any
    failedOpenTracker.remove(hri.getEncodedName());
    if (getTableStateManager().isTableState(hri.getTable(),
        ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING)) {
      invokeUnAssign(hri);
    }
  }

  private void onRegionClosed(final HRegionInfo hri) {
    if (getTableStateManager().isTableState(hri.getTable(),
        ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING) ||
        replicasToClose.contains(hri)) {
      offlineDisabledRegion(hri);
      return;
    }
    regionStates.updateRegionState(hri, RegionState.State.CLOSED);
    sendRegionClosedNotification(hri);
    // This below has to do w/ online enable/disable of a table
    removeClosedRegion(hri);
    invokeAssign(hri, false);
  }

  private String onRegionSplit(ServerName sn, TransitionCode code,
      final HRegionInfo p, final HRegionInfo a, final HRegionInfo b) {
    final RegionState rs_p = regionStates.getRegionState(p);
    RegionState rs_a = regionStates.getRegionState(a);
    RegionState rs_b = regionStates.getRegionState(b);
    if (!(rs_p.isOpenOrSplittingOnServer(sn)
        && (rs_a == null || rs_a.isOpenOrSplittingNewOnServer(sn))
        && (rs_b == null || rs_b.isOpenOrSplittingNewOnServer(sn)))) {
      return "Not in state good for split";
    }

    regionStates.updateRegionState(a, State.SPLITTING_NEW, sn);
    regionStates.updateRegionState(b, State.SPLITTING_NEW, sn);
    regionStates.updateRegionState(p, State.SPLITTING);

    if (code == TransitionCode.SPLIT) {
      if (TEST_SKIP_SPLIT_HANDLING) {
        return "Skipping split message, TEST_SKIP_SPLIT_HANDLING is set";
      }
      regionOffline(p, State.SPLIT);
      regionOnline(a, sn, 1);
      regionOnline(b, sn, 1);

      // User could disable the table before master knows the new region.
      if (getTableStateManager().isTableState(p.getTable(),
          ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING)) {
        invokeUnAssign(a);
        invokeUnAssign(b);
      } else {
        Callable<Object> splitReplicasCallable = new Callable<Object>() {
          @Override
          public Object call() {
            doSplittingOfReplicas(p, a, b);
            return null;
          }
        };
        threadPoolExecutorService.submit(splitReplicasCallable);
      }
    } else if (code == TransitionCode.SPLIT_PONR) {
      try {
        regionStates.splitRegion(p, a, b, sn);
      } catch (IOException ioe) {
        LOG.info("Failed to record split region " + p.getShortNameToLog());
        return "Failed to record the splitting in meta";
      }
    } else if (code == TransitionCode.SPLIT_REVERTED) {
      regionOnline(p, sn);
      regionOffline(a);
      regionOffline(b);

      if (getTableStateManager().isTableState(p.getTable(),
          ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING)) {
        invokeUnAssign(p);
      }
    }
    return null;
  }

  private String onRegionMerge(ServerName sn, TransitionCode code,
      final HRegionInfo p, final HRegionInfo a, final HRegionInfo b) {
    RegionState rs_p = regionStates.getRegionState(p);
    RegionState rs_a = regionStates.getRegionState(a);
    RegionState rs_b = regionStates.getRegionState(b);
    if (!(rs_a.isOpenOrMergingOnServer(sn) && rs_b.isOpenOrMergingOnServer(sn)
        && (rs_p == null || rs_p.isOpenOrMergingNewOnServer(sn)))) {
      return "Not in state good for merge";
    }

    regionStates.updateRegionState(a, State.MERGING);
    regionStates.updateRegionState(b, State.MERGING);
    regionStates.updateRegionState(p, State.MERGING_NEW, sn);

    String encodedName = p.getEncodedName();
    if (code == TransitionCode.READY_TO_MERGE) {
      mergingRegions.put(encodedName,
        new PairOfSameType<HRegionInfo>(a, b));
    } else if (code == TransitionCode.MERGED) {
      mergingRegions.remove(encodedName);
      regionOffline(a, State.MERGED);
      regionOffline(b, State.MERGED);
      regionOnline(p, sn, 1);

      // User could disable the table before master knows the new region.
      if (getTableStateManager().isTableState(p.getTable(),
          ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING)) {
        invokeUnAssign(p);
      } else {
        Callable<Object> mergeReplicasCallable = new Callable<Object>() {
          @Override
          public Object call() {
            doMergingOfReplicas(p, a, b);
            return null;
          }
        };
        threadPoolExecutorService.submit(mergeReplicasCallable);
      }
    } else if (code == TransitionCode.MERGE_PONR) {
      try {
        regionStates.mergeRegions(p, a, b, sn);
      } catch (IOException ioe) {
        LOG.info("Failed to record merged region " + p.getShortNameToLog());
        return "Failed to record the merging in meta";
      }
    } else {
      mergingRegions.remove(encodedName);
      regionOnline(a, sn);
      regionOnline(b, sn);
      regionOffline(p);

      if (getTableStateManager().isTableState(p.getTable(),
          ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING)) {
        invokeUnAssign(a);
        invokeUnAssign(b);
      }
    }
    return null;
  }

  /**
   * A helper to handle region merging transition event.
   * It transitions merging regions to MERGING state.
   */
  private boolean handleRegionMerging(final RegionTransition rt, final String encodedName,
      final String prettyPrintedRegionName, final ServerName sn) {
    if (!serverManager.isServerOnline(sn)) {
      LOG.warn("Dropped merging! ServerName=" + sn + " unknown.");
      return false;
    }
    byte [] payloadOfMerging = rt.getPayload();
    List<HRegionInfo> mergingRegions;
    try {
      mergingRegions = HRegionInfo.parseDelimitedFrom(
        payloadOfMerging, 0, payloadOfMerging.length);
    } catch (IOException e) {
      LOG.error("Dropped merging! Failed reading "  + rt.getEventType()
        + " payload for " + prettyPrintedRegionName);
      return false;
    }
    assert mergingRegions.size() == 3;
    HRegionInfo p = mergingRegions.get(0);
    HRegionInfo hri_a = mergingRegions.get(1);
    HRegionInfo hri_b = mergingRegions.get(2);

    RegionState rs_p = regionStates.getRegionState(p);
    RegionState rs_a = regionStates.getRegionState(hri_a);
    RegionState rs_b = regionStates.getRegionState(hri_b);

    if (!((rs_a == null || rs_a.isOpenOrMergingOnServer(sn))
        && (rs_b == null || rs_b.isOpenOrMergingOnServer(sn))
        && (rs_p == null || rs_p.isOpenOrMergingNewOnServer(sn)))) {
      LOG.warn("Dropped merging! Not in state good for MERGING; rs_p="
        + rs_p + ", rs_a=" + rs_a + ", rs_b=" + rs_b);
      return false;
    }

    EventType et = rt.getEventType();
    if (et == EventType.RS_ZK_REQUEST_REGION_MERGE) {
      try {
        RegionMergeCoordination.RegionMergeDetails std =
            ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
                .getRegionMergeCoordination().getDefaultDetails();
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getRegionMergeCoordination().processRegionMergeRequest(p, hri_a, hri_b, sn, std);
        if (((ZkRegionMergeCoordination.ZkRegionMergeDetails) std).getZnodeVersion() == -1) {
          byte[] data = ZKAssign.getData(watcher, encodedName);
         EventType currentType = null;
          if (data != null) {
            RegionTransition newRt = RegionTransition.parseFrom(data);
            currentType = newRt.getEventType();
          }
          if (currentType == null || (currentType != EventType.RS_ZK_REGION_MERGED
              && currentType != EventType.RS_ZK_REGION_MERGING)) {
            LOG.warn("Failed to transition pending_merge node "
              + encodedName + " to merging, it's now " + currentType);
            return false;
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to transition pending_merge node "
          + encodedName + " to merging", e);
        return false;
      }
    }

    synchronized (regionStates) {
      regionStates.updateRegionState(hri_a, State.MERGING);
      regionStates.updateRegionState(hri_b, State.MERGING);
      regionStates.updateRegionState(p, State.MERGING_NEW, sn);

      if (et != EventType.RS_ZK_REGION_MERGED) {
        this.mergingRegions.put(encodedName,
          new PairOfSameType<HRegionInfo>(hri_a, hri_b));
      } else {
        this.mergingRegions.remove(encodedName);
        regionOffline(hri_a, State.MERGED);
        regionOffline(hri_b, State.MERGED);
        regionOnline(p, sn);
      }
    }

    if (et == EventType.RS_ZK_REGION_MERGED) {
      doMergingOfReplicas(p, hri_a, hri_b);
      LOG.debug("Handling MERGED event for " + encodedName + "; deleting node");
      // Remove region from ZK
      try {
        boolean successful = false;
        while (!successful) {
          // It's possible that the RS tickles in between the reading of the
          // znode and the deleting, so it's safe to retry.
          successful = ZKAssign.deleteNode(watcher, encodedName,
            EventType.RS_ZK_REGION_MERGED, sn);
        }
      } catch (KeeperException e) {
        if (e instanceof NoNodeException) {
          String znodePath = ZKUtil.joinZNode(watcher.splitLogZNode, encodedName);
          LOG.debug("The znode " + znodePath + " does not exist.  May be deleted already.");
        } else {
          server.abort("Error deleting MERGED node " + encodedName, e);
        }
      }
      LOG.info("Handled MERGED event; merged=" + p.getRegionNameAsString()
        + ", region_a=" + hri_a.getRegionNameAsString() + ", region_b="
        + hri_b.getRegionNameAsString() + ", on " + sn);

      // User could disable the table before master knows the new region.
      if (tableStateManager.isTableState(p.getTable(),
          ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING)) {
        unassign(p);
      }
    }
    return true;
  }

  /**
   * A helper to handle region splitting transition event.
   */
  private boolean handleRegionSplitting(final RegionTransition rt, final String encodedName,
      final String prettyPrintedRegionName, final ServerName sn) {
    if (!serverManager.isServerOnline(sn)) {
      LOG.warn("Dropped splitting! ServerName=" + sn + " unknown.");
      return false;
    }
    byte [] payloadOfSplitting = rt.getPayload();
    List<HRegionInfo> splittingRegions;
    try {
      splittingRegions = HRegionInfo.parseDelimitedFrom(
        payloadOfSplitting, 0, payloadOfSplitting.length);
    } catch (IOException e) {
      LOG.error("Dropped splitting! Failed reading " + rt.getEventType()
        + " payload for " + prettyPrintedRegionName);
      return false;
    }
    assert splittingRegions.size() == 2;
    HRegionInfo hri_a = splittingRegions.get(0);
    HRegionInfo hri_b = splittingRegions.get(1);

    RegionState rs_p = regionStates.getRegionState(encodedName);
    RegionState rs_a = regionStates.getRegionState(hri_a);
    RegionState rs_b = regionStates.getRegionState(hri_b);

    if (!((rs_p == null || rs_p.isOpenOrSplittingOnServer(sn))
        && (rs_a == null || rs_a.isOpenOrSplittingNewOnServer(sn))
        && (rs_b == null || rs_b.isOpenOrSplittingNewOnServer(sn)))) {
      LOG.warn("Dropped splitting! Not in state good for SPLITTING; rs_p="
        + rs_p + ", rs_a=" + rs_a + ", rs_b=" + rs_b);
      return false;
    }

    if (rs_p == null) {
      // Splitting region should be online
      rs_p = regionStates.updateRegionState(rt, State.OPEN);
      if (rs_p == null) {
        LOG.warn("Received splitting for region " + prettyPrintedRegionName
          + " from server " + sn + " but it doesn't exist anymore,"
          + " probably already processed its split");
        return false;
      }
      regionStates.regionOnline(rs_p.getRegion(), sn);
    }

    HRegionInfo p = rs_p.getRegion();
    EventType et = rt.getEventType();
    if (et == EventType.RS_ZK_REQUEST_REGION_SPLIT) {
      try {
        SplitTransactionDetails std =
            ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
                .getSplitTransactionCoordination().getDefaultDetails();
        if (((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitTransactionCoordination().processTransition(p, hri_a, hri_b, sn, std) == -1) {
          byte[] data = ZKAssign.getData(watcher, encodedName);
          EventType currentType = null;
          if (data != null) {
            RegionTransition newRt = RegionTransition.parseFrom(data);
            currentType = newRt.getEventType();
          }
          if (currentType == null
              || (currentType != EventType.RS_ZK_REGION_SPLIT && currentType != EventType.RS_ZK_REGION_SPLITTING)) {
            LOG.warn("Failed to transition pending_split node " + encodedName
                + " to splitting, it's now " + currentType);
            return false;
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to transition pending_split node " + encodedName + " to splitting", e);
        return false;
      }
    }

    synchronized (regionStates) {
      splitRegions.put(p, new PairOfSameType<HRegionInfo>(hri_a, hri_b));
      regionStates.updateRegionState(hri_a, State.SPLITTING_NEW, sn);
      regionStates.updateRegionState(hri_b, State.SPLITTING_NEW, sn);
      regionStates.updateRegionState(rt, State.SPLITTING);

      // The below is for testing ONLY!  We can't do fault injection easily, so
      // resort to this kinda uglyness -- St.Ack 02/25/2011.
      if (TEST_SKIP_SPLIT_HANDLING) {
        LOG.warn("Skipping split message, TEST_SKIP_SPLIT_HANDLING is set");
        return true; // return true so that the splitting node stays
      }

      if (et == EventType.RS_ZK_REGION_SPLIT) {
        regionOffline(p, State.SPLIT);
        regionOnline(hri_a, sn);
        regionOnline(hri_b, sn);
        splitRegions.remove(p);
      }
    }

    if (et == EventType.RS_ZK_REGION_SPLIT) {
      // split replicas
      doSplittingOfReplicas(rs_p.getRegion(), hri_a, hri_b);
      LOG.debug("Handling SPLIT event for " + encodedName + "; deleting node");
      // Remove region from ZK
      try {
        boolean successful = false;
        while (!successful) {
          // It's possible that the RS tickles in between the reading of the
          // znode and the deleting, so it's safe to retry.
          successful = ZKAssign.deleteNode(watcher, encodedName,
            EventType.RS_ZK_REGION_SPLIT, sn);
        }
      } catch (KeeperException e) {
        if (e instanceof NoNodeException) {
          String znodePath = ZKUtil.joinZNode(watcher.splitLogZNode, encodedName);
          LOG.debug("The znode " + znodePath + " does not exist.  May be deleted already.");
        } else {
          server.abort("Error deleting SPLIT node " + encodedName, e);
        }
      }
      LOG.info("Handled SPLIT event; parent=" + p.getRegionNameAsString()
        + ", daughter a=" + hri_a.getRegionNameAsString() + ", daughter b="
        + hri_b.getRegionNameAsString() + ", on " + sn);

      // User could disable the table before master knows the new region.
      if (tableStateManager.isTableState(p.getTable(),
          ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING)) {
        unassign(hri_a);
        unassign(hri_b);
      }
    }
    return true;
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
    RegionState current = regionStates.getRegionState(hri);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got transition " + code + " for "
        + (current != null ? current.toString() : hri.getShortNameToLog())
        + " from " + serverName);
    }
    String errorMsg = null;
    switch (code) {
    case OPENED:
      if (current != null && current.isOpened() && current.isOnServer(serverName)) {
        LOG.info("Region " + hri.getShortNameToLog() + " is already " + current.getState() + " on "
            + serverName);
        break;
      }
    case FAILED_OPEN:
      if (current == null
          || !current.isPendingOpenOrOpeningOnServer(serverName)) {
        errorMsg = hri.getShortNameToLog()
          + " is not pending open on " + serverName;
      } else if (code == TransitionCode.FAILED_OPEN) {
        onRegionFailedOpen(hri, serverName);
      } else {
        long openSeqNum = HConstants.NO_SEQNUM;
        if (transition.hasOpenSeqNum()) {
          openSeqNum = transition.getOpenSeqNum();
        }
        if (openSeqNum < 0) {
          errorMsg = "Newly opened region has invalid open seq num " + openSeqNum;
        } else {
          onRegionOpen(hri, serverName, openSeqNum);
        }
      }
      break;

    case CLOSED:
      if (current == null
          || !current.isPendingCloseOrClosingOnServer(serverName)) {
        errorMsg = hri.getShortNameToLog()
          + " is not pending close on " + serverName;
      } else {
        onRegionClosed(hri);
      }
      break;

    case READY_TO_SPLIT:
      try {
        regionStateListener.onRegionSplit(hri);
      } catch (IOException exp) {
        errorMsg = StringUtils.stringifyException(exp);
      }
    case SPLIT_PONR:
    case SPLIT:
    case SPLIT_REVERTED:
      errorMsg =
          onRegionSplit(serverName, code, hri, HRegionInfo.convert(transition.getRegionInfo(1)),
            HRegionInfo.convert(transition.getRegionInfo(2)));
      if (org.apache.commons.lang.StringUtils.isEmpty(errorMsg)) {
        try {
          regionStateListener.onRegionSplitReverted(hri);
        } catch (IOException exp) {
          LOG.warn(StringUtils.stringifyException(exp));
        }
      }
      break;
    case READY_TO_MERGE:
    case MERGE_PONR:
    case MERGED:
    case MERGE_REVERTED:
      errorMsg = onRegionMerge(serverName, code, hri,
        HRegionInfo.convert(transition.getRegionInfo(1)),
        HRegionInfo.convert(transition.getRegionInfo(2)));
      if (code == TransitionCode.MERGED && org.apache.commons.lang.StringUtils.isEmpty(errorMsg)) {
        try {
          regionStateListener.onRegionMerged(hri);
        } catch (IOException exp) {
          errorMsg = StringUtils.stringifyException(exp);
        }
      }
      break;

    default:
      errorMsg = "Unexpected transition code " + code;
    }
    if (errorMsg != null) {
      LOG.error("Failed to transtion region from " + current + " to "
        + code + " by " + serverName + ": " + errorMsg);
    }
    return errorMsg;
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
