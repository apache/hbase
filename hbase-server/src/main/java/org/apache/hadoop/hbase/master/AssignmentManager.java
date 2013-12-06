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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeLoadBalancer;
import org.apache.hadoop.hbase.master.handler.ClosedRegionHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.OpenedRegionHandler;
import org.apache.hadoop.hbase.regionserver.RegionAlreadyInTransitionException;
import org.apache.hadoop.hbase.regionserver.RegionMergeTransaction;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.regionserver.SplitTransaction;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.hbase.zookeeper.MetaRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Preconditions;
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

  public static final String ASSIGNMENT_TIMEOUT = "hbase.master.assignment.timeoutmonitor.timeout";
  public static final int DEFAULT_ASSIGNMENT_TIMEOUT_DEFAULT = 600000;
  public static final String ASSIGNMENT_TIMEOUT_MANAGEMENT = "hbase.assignment.timeout.management";
  public static final boolean DEFAULT_ASSIGNMENT_TIMEOUT_MANAGEMENT = false;

  public static final String ALREADY_IN_TRANSITION_WAITTIME
    = "hbase.assignment.already.intransition.waittime";
  public static final int DEFAULT_ALREADY_IN_TRANSITION_WAITTIME = 60000; // 1 minute

  protected final Server server;

  private ServerManager serverManager;

  private boolean shouldAssignRegionsWithFavoredNodes;

  private CatalogTracker catalogTracker;

  protected final TimeoutMonitor timeoutMonitor;

  private final TimerUpdater timerUpdater;

  private LoadBalancer balancer;

  private final MetricsAssignmentManager metricsAssignmentManager;

  private final TableLockManager tableLockManager;

  private AtomicInteger numRegionsOpened = new AtomicInteger(0);

  final private KeyLocker<String> locker = new KeyLocker<String>();

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

  /**
   * The sleep time for which the assignment will wait before retrying in case of hbase:meta assignment
   * failure due to lack of availability of region plan
   */
  private final long sleepTimeBeforeRetryingMetaAssignment;

  /** Plans for region movement. Key is the encoded version of a region name*/
  // TODO: When do plans get cleaned out?  Ever? In server open and in server
  // shutdown processing -- St.Ack
  // All access to this Map must be synchronized.
  final NavigableMap<String, RegionPlan> regionPlans =
    new TreeMap<String, RegionPlan>();

  private final ZKTable zkTable;

  /**
   * Contains the server which need to update timer, these servers will be
   * handled by {@link TimerUpdater}
   */
  private final ConcurrentSkipListSet<ServerName> serversInUpdatingTimer;

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

  /** Is the TimeOutManagement activated **/
  private final boolean tomActivated;

  /**
   * A map to track the count a region fails to open in a row.
   * So that we don't try to open a region forever if the failure is
   * unrecoverable.  We don't put this information in region states
   * because we don't expect this to happen frequently; we don't
   * want to copy this information over during each state transition either.
   */
  private final ConcurrentHashMap<String, AtomicInteger>
    failedOpenTracker = new ConcurrentHashMap<String, AtomicInteger>();

  /**
   * For testing only!  Set to true to skip handling of split.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="MS_SHOULD_BE_FINAL")
  public static boolean TEST_SKIP_SPLIT_HANDLING = false;

  /**
   * Constructs a new assignment manager.
   *
   * @param server
   * @param serverManager
   * @param catalogTracker
   * @param service
   * @throws KeeperException
   * @throws IOException
   */
  public AssignmentManager(Server server, ServerManager serverManager,
      CatalogTracker catalogTracker, final LoadBalancer balancer,
      final ExecutorService service, MetricsMaster metricsMaster,
      final TableLockManager tableLockManager) throws KeeperException, IOException {
    super(server.getZooKeeper());
    this.server = server;
    this.serverManager = serverManager;
    this.catalogTracker = catalogTracker;
    this.executorService = service;
    this.regionsToReopen = Collections.synchronizedMap
                           (new HashMap<String, HRegionInfo> ());
    Configuration conf = server.getConfiguration();
    // Only read favored nodes if using the favored nodes load balancer.
    this.shouldAssignRegionsWithFavoredNodes = conf.getClass(
           HConstants.HBASE_MASTER_LOADBALANCER_CLASS, Object.class).equals(
           FavoredNodeLoadBalancer.class);
    this.tomActivated = conf.getBoolean(
      ASSIGNMENT_TIMEOUT_MANAGEMENT, DEFAULT_ASSIGNMENT_TIMEOUT_MANAGEMENT);
    if (tomActivated){
      this.serversInUpdatingTimer =  new ConcurrentSkipListSet<ServerName>();
      this.timeoutMonitor = new TimeoutMonitor(
        conf.getInt("hbase.master.assignment.timeoutmonitor.period", 30000),
        server, serverManager,
        conf.getInt(ASSIGNMENT_TIMEOUT, DEFAULT_ASSIGNMENT_TIMEOUT_DEFAULT));
      this.timerUpdater = new TimerUpdater(conf.getInt(
        "hbase.master.assignment.timerupdater.period", 10000), server);
      Threads.setDaemonThreadRunning(timerUpdater.getThread(),
        server.getServerName() + ".timerUpdater");
    } else {
      this.serversInUpdatingTimer =  null;
      this.timeoutMonitor = null;
      this.timerUpdater = null;
    }
    this.zkTable = new ZKTable(this.watcher);
    // This is the max attempts, not retries, so it should be at least 1.
    this.maximumAttempts = Math.max(1,
      this.server.getConfiguration().getInt("hbase.assignment.maximum.attempts", 10));
    this.sleepTimeBeforeRetryingMetaAssignment = this.server.getConfiguration().getLong(
        "hbase.meta.assignment.retry.sleeptime", 1000l);
    this.balancer = balancer;
    int maxThreads = conf.getInt("hbase.assignment.threads.max", 30);
    this.threadPoolExecutorService = Threads.getBoundedCachedThreadPool(
      maxThreads, 60L, TimeUnit.SECONDS, Threads.newDaemonThreadFactory("AM."));
    this.regionStates = new RegionStates(server, serverManager);

    this.bulkAssignWaitTillAllAssigned =
      conf.getBoolean("hbase.bulk.assignment.waittillallassigned", false);
    this.bulkAssignThresholdRegions = conf.getInt("hbase.bulk.assignment.threshold.regions", 7);
    this.bulkAssignThresholdServers = conf.getInt("hbase.bulk.assignment.threshold.servers", 3);

    int workers = conf.getInt("hbase.assignment.zkevent.workers", 20);
    ThreadFactory threadFactory = Threads.newDaemonThreadFactory("AM.ZK.Worker");
    zkEventWorkers = Threads.getBoundedCachedThreadPool(workers, 60L,
            TimeUnit.SECONDS, threadFactory);
    this.tableLockManager = tableLockManager;

    this.metricsAssignmentManager = new MetricsAssignmentManager();
  }

  void startTimeOutMonitor() {
    if (tomActivated) {
      Threads.setDaemonThreadRunning(timeoutMonitor.getThread(), server.getServerName()
          + ".timeoutMonitor");
    }
  }

  /**
   * @return Instance of ZKTable.
   */
  public ZKTable getZKTable() {
    // These are 'expensive' to make involving trip to zk ensemble so allow
    // sharing.
    return this.zkTable;
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
    List <HRegionInfo> hris =
      MetaReader.getTableRegions(this.server.getCatalogTracker(), tableName, true);
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
   */
  void joinCluster() throws IOException,
      KeeperException, InterruptedException {
    // Concurrency note: In the below the accesses on regionsInTransition are
    // outside of a synchronization block where usually all accesses to RIT are
    // synchronized.  The presumption is that in this case it is safe since this
    // method is being played by a single thread on startup.

    // TODO: Regions that have a null location and are not in regionsInTransitions
    // need to be handled.

    // Scan hbase:meta to build list of existing regions, servers, and assignment
    // Returns servers who have not checked in (assumed dead) and their regions
    Map<ServerName, List<HRegionInfo>> deadServers = rebuildUserRegions();

    // This method will assign all user regions if a clean server startup or
    // it will reconstruct master state and cleanup any leftovers from
    // previous master process.
    processDeadServersAndRegionsInTransition(deadServers);

    recoverTableInDisablingState();
    recoverTableInEnablingState();
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
  void processDeadServersAndRegionsInTransition(
      final Map<ServerName, List<HRegionInfo>> deadServers)
          throws KeeperException, IOException, InterruptedException {
    List<String> nodes = ZKUtil.listChildrenNoWatch(watcher,
      watcher.assignmentZNode);

    if (nodes == null) {
      String errorMessage = "Failed to get the children from ZK";
      server.abort(errorMessage, new IOException(errorMessage));
      return;
    }

    boolean failover = (!serverManager.getDeadServers().isEmpty() || !serverManager
        .getRequeuedDeadServers().isEmpty());

    if (!failover) {
      // If any one region except meta is assigned, it's a failover.
      Map<HRegionInfo, ServerName> regions = regionStates.getRegionAssignments();
      for (HRegionInfo hri: regions.keySet()) {
        if (!hri.isMetaTable()) {
          LOG.debug("Found " + hri + " out on cluster");
          failover = true;
          break;
        }
      }
      if (!failover) {
        // If any one region except meta is in transition, it's a failover.
        for (String encodedName: nodes) {
          RegionState state = regionStates.getRegionState(encodedName);
          if (state != null && !state.getRegion().isMetaRegion()) {
            LOG.debug("Found " + state.getRegion().getRegionNameAsString() + " in RITs");
            failover = true;
            break;
          }
        }
      }
    }

    // If we found user regions out on cluster, its a failover.
    if (failover) {
      LOG.info("Found regions out on cluster or in RIT; presuming failover");
      // Process list of dead servers and regions in RIT.
      // See HBASE-4580 for more information.
      processDeadServersAndRecoverLostRegions(deadServers);
    } else {
      // Fresh cluster startup.
      LOG.info("Clean cluster startup. Assigning userregions");
      assignAllUserRegions();
    }
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
    boolean intransistion = processRegionInTransition(hri.getEncodedName(), hri);
    if (!intransistion) return intransistion;
    LOG.debug("Waiting on " + HRegionInfo.prettyPrint(hri.getEncodedName()));
    while (!this.server.isStopped() &&
      this.regionStates.isRegionInTransition(hri.getEncodedName())) {
      // We put a timeout because we may have the region getting in just between the test
      //  and the waitForUpdate
      this.regionStates.waitForUpdate(100);
    }
    return intransistion;
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
      return processRegionsInTransition(
        rt, hri, stat.getVersion());
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
      final int expectedVersion) throws KeeperException {
    EventType et = rt.getEventType();
    // Get ServerName.  Could not be null.
    final ServerName sn = rt.getServerName();
    final byte[] regionName = rt.getRegionName();
    final String encodedName = HRegionInfo.encodeRegionName(regionName);
    final String prettyPrintedRegionName = HRegionInfo.prettyPrint(encodedName);
    LOG.info("Processing " + prettyPrintedRegionName + " in state: " + et);

    if (regionStates.isRegionInTransition(encodedName)) {
      LOG.info("Processed region " + prettyPrintedRegionName + " in state: "
        + et + ", does nothing since the region is already in transition "
        + regionStates.getRegionTransitionState(encodedName));
      // Just return
      return true;
    }
    if (!serverManager.isServerOnline(sn)) {
      // It was on a dead server, it's closed now. Force to OFFLINE and put
      // it in transition. Try to re-assign it, but it will fail most likely,
      // since we have not done log splitting for the dead server yet.
      LOG.debug("RIT " + encodedName + " in state=" + rt.getEventType() +
        " was on deadserver; forcing offline");
      ZKAssign.createOrForceNodeOffline(this.watcher, regionInfo, sn);
      regionStates.updateRegionState(regionInfo, State.OFFLINE, sn);
      invokeAssign(regionInfo);
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
                unassign(regionInfo, rsClosing, expectedVersion, null, true, null);
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
        regionStates.updateRegionState(regionInfo, State.CLOSED, sn);
        invokeAssign(regionInfo);
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
        new OpenedRegionHandler(server, this, regionInfo, sn, expectedVersion).process();
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
   * @param rt
   * @param expectedVersion
   */
  void handleRegion(final RegionTransition rt, int expectedVersion) {
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

            // Close it without updating the internal region states,
            // so as not to create double assignments in unlucky scenarios
            // mentioned in OpenRegionHandler#process
            unassign(regionState.getRegion(), null, -1, null, false, sn);
            return;
          }
          // Handle OPENED by removing from transition and deleted zk node
          regionState = regionStates.updateRegionState(rt, State.OPEN);
          if (regionState != null) {
            failedOpenTracker.remove(encodedName); // reset the count, if any
            new OpenedRegionHandler(
              server, this, regionState.getRegion(), sn, expectedVersion).process();
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
    FavoredNodeAssignmentHelper.updateMetaWithFavoredNodesInfo(regionToFavoredNodes, catalogTracker);
  }

  /**
   * Handle a ZK unassigned node transition triggered by HBCK repair tool.
   * <p>
   * This is handled in a separate code path because it breaks the normal rules.
   * @param rt
   */
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
            Pair<HRegionInfo, ServerName> p = MetaReader.getRegion(catalogTracker, name);
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
            boolean disabled = getZKTable().isDisablingOrDisabledTable(regionInfo.getTable());
            ServerName serverName = rs.getServerName();
            if (serverManager.isServerOnline(serverName)) {
              if (rs.isOnServer(serverName)
                  && (rs.isOpened() || rs.isSplitting())) {
                regionOnline(regionInfo, serverName);
                if (disabled) {
                  // if server is offline, no hurt to unassign again
                  LOG.info("Opened " + regionNameStr
                    + "but this table is disabled, triggering close of region");
                  unassign(regionInfo);
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
    numRegionsOpened.incrementAndGet();
    regionStates.regionOnline(regionInfo, sn);

    // Remove plan if one.
    clearRegionPlan(regionInfo);
    // Add the server to serversInUpdatingTimer
    addToServersInUpdatingTimer(sn);
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
            handleRegion(rt, stat.getVersion());
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
   * Add the server to the set serversInUpdatingTimer, then {@link TimerUpdater}
   * will update timers for this server in background
   * @param sn
   */
  private void addToServersInUpdatingTimer(final ServerName sn) {
    if (tomActivated){
      this.serversInUpdatingTimer.add(sn);
    }
  }

  /**
   * Touch timers for all regions in transition that have the passed
   * <code>sn</code> in common.
   * Call this method whenever a server checks in.  Doing so helps the case where
   * a new regionserver has joined the cluster and its been given 1k regions to
   * open.  If this method is tickled every time the region reports in a
   * successful open then the 1k-th region won't be timed out just because its
   * sitting behind the open of 999 other regions.  This method is NOT used
   * as part of bulk assign -- there we have a different mechanism for extending
   * the regions in transition timer (we turn it off temporarily -- because
   * there is no regionplan involved when bulk assigning.
   * @param sn
   */
  private void updateTimers(final ServerName sn) {
    Preconditions.checkState(tomActivated);
    if (sn == null) return;

    // This loop could be expensive.
    // First make a copy of current regionPlan rather than hold sync while
    // looping because holding sync can cause deadlock.  Its ok in this loop
    // if the Map we're going against is a little stale
    List<Map.Entry<String, RegionPlan>> rps;
    synchronized(this.regionPlans) {
      rps = new ArrayList<Map.Entry<String, RegionPlan>>(regionPlans.entrySet());
    }

    for (Map.Entry<String, RegionPlan> e : rps) {
      if (e.getValue() != null && e.getKey() != null && sn.equals(e.getValue().getDestination())) {
        RegionState regionState = regionStates.getRegionTransitionState(e.getKey());
        if (regionState != null) {
          regionState.updateTimestampToNow();
        }
      }
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
    // Disabling so should not be reassigned, just delete the CLOSED node
    LOG.debug("Table being disabled so deleting ZK node and removing from " +
      "regions in transition, skipping assignment of region " +
        regionInfo.getRegionNameAsString());
    String encodedName = regionInfo.getEncodedName();
    deleteNodeInStates(encodedName, "closed", null,
      EventType.RS_ZK_REGION_CLOSED, EventType.M_ZK_REGION_OFFLINE);
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
    if (this.serverManager.isClusterShutdown()) {
      LOG.info("Cluster shutdown is set; skipping assign of " +
        region.getRegionNameAsString());
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
        assign(state, setOfflineInZK, forceNewPlan);
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
  boolean assign(final ServerName destination, final List<HRegionInfo> regions) {
    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    try {
      int regionCount = regions.size();
      if (regionCount == 0) {
        return true;
      }
      LOG.debug("Assigning " + regionCount + " region(s) to " + destination.toString());
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
              } else if (asyncSetOfflineInZooKeeper(state, cb, destination)) {
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

        // Wait until all unassigned nodes have been put up and watchers set.
        int total = states.size();
        for (int oldCounter = 0; !server.isStopped();) {
          int count = counter.get();
          if (oldCounter != count) {
            LOG.info(destination.toString() + " unassigned znodes=" + count +
              " of total=" + total);
            oldCounter = count;
          }
          if (count >= total) break;
          Threads.sleep(5);
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
          if (nodeVersion == null || nodeVersion == -1) {
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
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
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
      LOG.debug("Bulk assigning done for " + destination);
      return true;
    } finally {
      metricsAssignmentManager.updateBulkAssignTime(EnvironmentEdgeManager.currentTimeMillis() - startTime);
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
          if (!transitionInZK && state != null) {
            // Retry to make sure the region is
            // closed so as to avoid double assignment.
            unassign(region, state, versionOfClosingNode,
              dest, transitionInZK,src);
          }
          return;
        }
        // This never happens. Currently regionserver close always return true.
        // Todo; this can now happen (0.96) if there is an exception in a coprocessor
        LOG.warn("Server " + server + " region CLOSE RPC returned false for " +
          region.getRegionNameAsString());
      } catch (Throwable t) {
        if (t instanceof RemoteException) {
          t = ((RemoteException)t).unwrapRemoteException();
        }
        if (t instanceof NotServingRegionException
            || t instanceof RegionServerStoppedException) {
          LOG.debug("Offline " + region.getRegionNameAsString()
            + ", it's not any more on " + server, t);
          if (transitionInZK) {
            deleteClosingOrClosedNode(region, server);
          }
          if (state != null) {
            regionOffline(region);
          }
          return;
        } else if (state != null
            && t instanceof RegionAlreadyInTransitionException) {
          // RS is already processing this region, only need to update the timestamp
          LOG.debug("update " + state + " the timestamp.");
          state.updateTimestampToNow();
          if (maxWaitTime < 0) {
            maxWaitTime = EnvironmentEdgeManager.currentTimeMillis()
              + this.server.getConfiguration().getLong(ALREADY_IN_TRANSITION_WAITTIME,
                DEFAULT_ALREADY_IN_TRANSITION_WAITTIME);
          }
          try {
            long now = EnvironmentEdgeManager.currentTimeMillis();
            if (now < maxWaitTime) {
              LOG.debug("Region is already in transition; "
                + "waiting up to " + (maxWaitTime - now) + "ms", t);
              Thread.sleep(100);
              i--; // reset the try count
            }
          } catch (InterruptedException ie) {
            LOG.warn("Failed to unassign "
              + region.getRegionNameAsString() + " since interrupted", ie);
            Thread.currentThread().interrupt();
            if (!tomActivated) {
              regionStates.updateRegionState(region, State.FAILED_CLOSE);
            }
            return;
          }
        } else {
          LOG.info("Server " + server + " returned " + t + " for "
            + region.getRegionNameAsString() + ", try=" + i
            + " of " + this.maximumAttempts, t);
          // Presume retry or server will expire.
        }
      }
    }
    // Run out of attempts
    if (!tomActivated && state != null) {
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
      LOG.warn("Assigning a region not in region states: " + region);
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
      if (regionStates.isServerDeadAndNotProcessed(sn)
          && wasRegionOnDeadServerByMeta(region, sn)) {
        LOG.info("Skip assigning " + region.getRegionNameAsString()
          + ", it is on a dead but not processed yet server");
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

  private boolean wasRegionOnDeadServerByMeta(
      final HRegionInfo region, final ServerName sn) {
    try {
      if (region.isMetaRegion()) {
        ServerName server = catalogTracker.getMetaLocation();
        return regionStates.isServerDeadAndNotProcessed(server);
      }
      while (!server.isStopped()) {
        try {
          catalogTracker.waitForMeta();
          Pair<HRegionInfo, ServerName> r =
            MetaReader.getRegion(catalogTracker, region.getRegionName());
          ServerName server = r == null ? null : r.getSecond();
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
    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    try {
      RegionState currentState = state;
      int versionOfOfflineNode = -1;
      RegionPlan plan = null;
      long maxWaitTime = -1;
      HRegionInfo region = state.getRegion();
      RegionOpeningState regionOpenState;
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
          if (tomActivated){
            this.timeoutMonitor.setAllRegionServersOffline(true);
          } else {
            if (region.isMetaRegion()) {
              try {
                if (i != maximumAttempts) {
                  Thread.sleep(this.sleepTimeBeforeRetryingMetaAssignment);
                  continue;
                }
                // TODO : Ensure HBCK fixes this
                LOG.error("Unable to determine a plan to assign hbase:meta even after repeated attempts. Run HBCK to fix this");
              } catch (InterruptedException e) {
                LOG.error("Got exception while waiting for hbase:meta assignment");
                Thread.currentThread().interrupt();
              }
            }
            regionStates.updateRegionState(region, State.FAILED_OPEN);
          }
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
            if (!zkTable.isEnablingTable(tableName) && !zkTable.isEnabledTable(tableName)) {
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
                maxWaitTime = EnvironmentEdgeManager.currentTimeMillis()
                  + this.server.getConfiguration().getLong(ALREADY_IN_TRANSITION_WAITTIME,
                    DEFAULT_ALREADY_IN_TRANSITION_WAITTIME);
              } else {
                maxWaitTime = this.server.getConfiguration().
                  getLong("hbase.regionserver.rpc.startup.waittime", 60000);
              }
            }
            try {
              needNewPlan = false;
              long now = EnvironmentEdgeManager.currentTimeMillis();
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
              Thread.currentThread().interrupt();
              if (!tomActivated) {
                regionStates.updateRegionState(region, State.FAILED_OPEN);
              }
              return;
            }
          } else if (retry) {
            needNewPlan = false;
            LOG.warn(assignMsg + ", trying to assign to the same region server " +
                "try=" + i + " of " + this.maximumAttempts, t);
          } else {
            needNewPlan = true;
            LOG.warn(assignMsg + ", trying to assign elsewhere instead;" +
                " try=" + i + " of " + this.maximumAttempts, t);
          }
        }

        if (i == this.maximumAttempts) {
          // Don't reset the region state or get a new plan any more.
          // This is the last try.
          continue;
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
            if (tomActivated) {
              this.timeoutMonitor.setAllRegionServersOffline(true);
            } else {
              regionStates.updateRegionState(region, State.FAILED_OPEN);
            }
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
          }
        }
      }
      // Run out of attempts
      if (!tomActivated) {
        regionStates.updateRegionState(region, State.FAILED_OPEN);
      }
    } finally {
      metricsAssignmentManager.updateAssignmentTime(EnvironmentEdgeManager.currentTimeMillis() - startTime);
    }
  }

  private void processAlreadyOpenedRegion(HRegionInfo region, ServerName sn) {
    // Remove region from in-memory transition and unassigned node from ZK
    // While trying to enable the table the regions of the table were
    // already enabled.
    LOG.debug("ALREADY_OPENED " + region.getRegionNameAsString()
      + " to " + sn);
    String encodedName = region.getEncodedName();
    deleteNodeInStates(encodedName, "offline", sn, EventType.M_ZK_REGION_OFFLINE);
    regionStates.regionOnline(region, sn);
  }

  private boolean isDisabledorDisablingRegionInRIT(final HRegionInfo region) {
    TableName tableName = region.getTable();
    boolean disabled = this.zkTable.isDisabledTable(tableName);
    if (disabled || this.zkTable.isDisablingTable(tableName)) {
      LOG.info("Table " + tableName + (disabled ? " disabled;" : " disabling;") +
        " skipping assign of " + region.getRegionNameAsString());
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
      final boolean forceNewPlan)  throws HBaseIOException  {
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
      LOG.debug("No previous transition plan found (or ignoring " +
        "an existing plan) for " + region.getRegionNameAsString() +
        "; generated random plan=" + randomPlan + "; " +
        serverManager.countOfRegionServers() +
               " (online=" + serverManager.getOnlineServers().size() +
               ", available=" + destServers.size() + ") available servers" +
               ", forceNewPlan=" + forceNewPlan);
        return randomPlan;
      }
    LOG.debug("Using pre-existing plan for " +
      region.getRegionNameAsString() + "; plan=" + existingPlan);
    return existingPlan;
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
            LOG.warn("Attempting to unassign a region not in RegionStates"
              + region.getRegionNameAsString() + ", offlined");
            regionOffline(region);
            return;
          }
          versionOfClosingNode = ZKAssign.createNodeClosing(
            watcher, region, state.getServerName());
          if (versionOfClosingNode == -1) {
            LOG.info("Attempting to unassign " +
              region.getRegionNameAsString() + " but ZK closing node "
              + "can't be created.");
            reassign = false; // not unassigned at all
            return;
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

      unassign(region, state, versionOfClosingNode, dest, true, null);
    } finally {
      lock.unlock();

      // Region is expected to be reassigned afterwards
      if (reassign && regionStates.isRegionOffline(region)) {
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
   * @throws InterruptedException
   */
  public boolean waitForAssignment(HRegionInfo regionInfo)
      throws InterruptedException {
    while (!regionStates.isRegionOnline(regionInfo)) {
      if (regionStates.isRegionInState(regionInfo, State.FAILED_OPEN)
          || this.server.isStopped()) {
        return false;
      }

      // We should receive a notification, but it's
      //  better to have a timeout to recheck the condition here:
      //  it lowers the impact of a race condition if any
      regionStates.waitForUpdate(100);
    }
    return true;
  }

  /**
   * Assigns the hbase:meta region.
   * <p>
   * Assumes that hbase:meta is currently closed and is not being actively served by
   * any RegionServer.
   * <p>
   * Forcibly unsets the current meta region location in ZooKeeper and assigns
   * hbase:meta to a random RegionServer.
   * @throws KeeperException
   */
  public void assignMeta() throws KeeperException {
    MetaRegionTracker.deleteMetaLocation(this.watcher);
    assign(HRegionInfo.FIRST_META_REGIONINFO, true);
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
      for (Map.Entry<ServerName, List<HRegionInfo>> plan: bulkPlan.entrySet()) {
        if (!assign(plan.getKey(), plan.getValue())) {
          for (HRegionInfo region: plan.getValue()) {
            if (!regionStates.isRegionOnline(region)) {
              invokeAssign(region);
            }
          }
        }
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
   * @throws KeeperException
   */
  private void assignAllUserRegions()
      throws IOException, InterruptedException, KeeperException {
    // Cleanup any existing ZK nodes and start watching
    ZKAssign.deleteAllNodes(watcher);
    ZKUtil.listChildrenAndWatchForNewChildren(this.watcher,
      this.watcher.assignmentZNode);
    failoverCleanupDone();

    // Skip assignment for regions of tables in DISABLING state because during clean cluster startup
    // no RS is alive and regions map also doesn't have any information about the regions.
    // See HBASE-6281.
    Set<TableName> disabledOrDisablingOrEnabling = ZKTable.getDisabledOrDisablingTables(watcher);
    disabledOrDisablingOrEnabling.addAll(ZKTable.getEnablingTables(watcher));
    // Scan hbase:meta for all user regions, skipping any disabled tables
    Map<HRegionInfo, ServerName> allRegions;
    SnapshotOfRegionAssignmentFromMeta snapshotOfRegionAssignment =
       new SnapshotOfRegionAssignmentFromMeta(catalogTracker, disabledOrDisablingOrEnabling, true);
    snapshotOfRegionAssignment.initialize();
    allRegions = snapshotOfRegionAssignment.getRegionToRegionServerMap();
    if (allRegions == null || allRegions.isEmpty()) return;

    // Determine what type of assignment to do on startup
    boolean retainAssignment = server.getConfiguration().
      getBoolean("hbase.master.startup.retainassign", true);

    if (retainAssignment) {
      assign(allRegions);
    } else {
      List<HRegionInfo> regions = new ArrayList<HRegionInfo>(allRegions.keySet());
      assign(regions);
    }

    for (HRegionInfo hri : allRegions.keySet()) {
      TableName tableName = hri.getTable();
      if (!zkTable.isEnabledTable(tableName)) {
        setEnabledTable(tableName);
      }
    }
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
   * Returns a map of servers that are not found to be online and the regions
   * they were hosting.
   * @return map of servers not online to their assigned regions, as stored
   *         in META
   * @throws IOException
   */
  Map<ServerName, List<HRegionInfo>> rebuildUserRegions() throws IOException, KeeperException {
    Set<TableName> enablingTables = ZKTable.getEnablingTables(watcher);
    Set<TableName> disabledOrEnablingTables = ZKTable.getDisabledTables(watcher);
    disabledOrEnablingTables.addAll(enablingTables);
    Set<TableName> disabledOrDisablingOrEnabling = ZKTable.getDisablingTables(watcher);
    disabledOrDisablingOrEnabling.addAll(disabledOrEnablingTables);

    // Region assignment from META
    List<Result> results = MetaReader.fullScan(this.catalogTracker);
    // Get any new but slow to checkin region server that joined the cluster
    Set<ServerName> onlineServers = serverManager.getOnlineServers().keySet();
    // Map of offline servers and their regions to be returned
    Map<ServerName, List<HRegionInfo>> offlineServers =
      new TreeMap<ServerName, List<HRegionInfo>>();
    // Iterate regions in META
    for (Result result : results) {
      Pair<HRegionInfo, ServerName> region = HRegionInfo.getHRegionInfoAndServerName(result);
      if (region == null) continue;
      HRegionInfo regionInfo = region.getFirst();
      ServerName regionLocation = region.getSecond();
      if (regionInfo == null) continue;
      regionStates.createRegionState(regionInfo);
      if (regionStates.isRegionInState(regionInfo, State.SPLIT)) {
        // Split is considered to be completed. If the split znode still
        // exists, the region will be put back to SPLITTING state later
        LOG.debug("Region " + regionInfo.getRegionNameAsString()
           + " split is completed. Hence need not add to regions list");
        continue;
      }
      TableName tableName = regionInfo.getTable();
      if (regionLocation == null) {
        // regionLocation could be null if createTable didn't finish properly.
        // When createTable is in progress, HMaster restarts.
        // Some regions have been added to hbase:meta, but have not been assigned.
        // When this happens, the region's table must be in ENABLING state.
        // It can't be in ENABLED state as that is set when all regions are
        // assigned.
        // It can't be in DISABLING state, because DISABLING state transitions
        // from ENABLED state when application calls disableTable.
        // It can't be in DISABLED state, because DISABLED states transitions
        // from DISABLING state.
        if (!enablingTables.contains(tableName)) {
          LOG.warn("Region " + regionInfo.getEncodedName() +
            " has null regionLocation." + " But its table " + tableName +
            " isn't in ENABLING state.");
        }
      } else if (!onlineServers.contains(regionLocation)) {
        // Region is located on a server that isn't online
        List<HRegionInfo> offlineRegions = offlineServers.get(regionLocation);
        if (offlineRegions == null) {
          offlineRegions = new ArrayList<HRegionInfo>(1);
          offlineServers.put(regionLocation, offlineRegions);
        }
        offlineRegions.add(regionInfo);
        // need to enable the table if not disabled or disabling or enabling
        // this will be used in rolling restarts
        if (!disabledOrDisablingOrEnabling.contains(tableName)
            && !getZKTable().isEnabledTable(tableName)) {
          setEnabledTable(tableName);
        }
      } else {
        // Region is being served and on an active server
        // add only if region not in disabled or enabling table
        if (!disabledOrEnablingTables.contains(tableName)) {
          regionStates.updateRegionState(regionInfo, State.OPEN, regionLocation);
          regionStates.regionOnline(regionInfo, regionLocation);
        }
        // need to enable the table if not disabled or disabling or enabling
        // this will be used in rolling restarts
        if (!disabledOrDisablingOrEnabling.contains(tableName)
            && !getZKTable().isEnabledTable(tableName)) {
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
      throws KeeperException, TableNotFoundException, IOException {
    Set<TableName> disablingTables = ZKTable.getDisablingTables(watcher);
    if (disablingTables.size() != 0) {
      for (TableName tableName : disablingTables) {
        // Recover by calling DisableTableHandler
        LOG.info("The table " + tableName
            + " is in DISABLING state.  Hence recovering by moving the table"
            + " to DISABLED state.");
        new DisableTableHandler(this.server, tableName, catalogTracker,
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
      throws KeeperException, TableNotFoundException, IOException {
    Set<TableName> enablingTables = ZKTable.getEnablingTables(watcher);
    if (enablingTables.size() != 0) {
      for (TableName tableName : enablingTables) {
        // Recover by calling EnableTableHandler
        LOG.info("The table " + tableName
            + " is in ENABLING state.  Hence recovering by moving the table"
            + " to ENABLED state.");
        // enableTable in sync way during master startup,
        // no need to invoke coprocessor
        new EnableTableHandler(this.server, tableName,
            catalogTracker, this, tableLockManager, true).prepare().process();
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
      Map<ServerName, List<HRegionInfo>> deadServers)
          throws IOException, KeeperException {
    if (deadServers != null) {
      for (Map.Entry<ServerName, List<HRegionInfo>> server: deadServers.entrySet()) {
        ServerName serverName = server.getKey();
        // We need to keep such info even if the server is known dead
        regionStates.setLastRegionServerOfRegions(serverName, server.getValue());
        if (!serverManager.isServerDead(serverName)) {
          serverManager.expireServer(serverName); // Let SSH do region re-assign
        }
      }
    }
    List<String> nodes = ZKUtil.listChildrenAndWatchForNewChildren(
      this.watcher, this.watcher.assignmentZNode);
    if (!nodes.isEmpty()) {
      for (String encodedRegionName : nodes) {
        processRegionInTransition(encodedRegionName, null);
      }
    }

    // Now we can safely claim failover cleanup completed and enable
    // ServerShutdownHandler for further processing. The nodes (below)
    // in transition, if any, are for regions not related to those
    // dead servers at all, and can be done in parallel to SSH.
    failoverCleanupDone();
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
    long end = (timeOut <= 0) ? Long.MAX_VALUE : EnvironmentEdgeManager.currentTimeMillis()
        + timeOut;
    // There is already a timeout monitor on regions in transition so I
    // should not have to have one here too?
    LOG.info("Waiting for " + hri.getEncodedName() +
        " to leave regions-in-transition, timeOut=" + timeOut + " ms.");
    while (!this.server.isStopped() && regionStates.isRegionInTransition(hri)) {
      regionStates.waitForUpdate(100);
      if (EnvironmentEdgeManager.currentTimeMillis() > end) {
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

  /**
   * Update timers for all regions in transition going against the server in the
   * serversInUpdatingTimer.
   */
  public class TimerUpdater extends Chore {

    public TimerUpdater(final int period, final Stoppable stopper) {
      super("AssignmentTimerUpdater", period, stopper);
    }

    @Override
    protected void chore() {
      Preconditions.checkState(tomActivated);
      ServerName serverToUpdateTimer = null;
      while (!serversInUpdatingTimer.isEmpty() && !stopper.isStopped()) {
        if (serverToUpdateTimer == null) {
          serverToUpdateTimer = serversInUpdatingTimer.first();
        } else {
          serverToUpdateTimer = serversInUpdatingTimer
              .higher(serverToUpdateTimer);
        }
        if (serverToUpdateTimer == null) {
          break;
        }
        updateTimers(serverToUpdateTimer);
        serversInUpdatingTimer.remove(serverToUpdateTimer);
      }
    }
  }

  /**
   * Monitor to check for time outs on region transition operations
   */
  public class TimeoutMonitor extends Chore {
    private boolean allRegionServersOffline = false;
    private ServerManager serverManager;
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
        ServerManager serverManager,
        final int timeout) {
      super("AssignmentTimeoutMonitor", period, stopper);
      this.timeout = timeout;
      this.serverManager = serverManager;
    }

    private synchronized void setAllRegionServersOffline(
      boolean allRegionServersOffline) {
      this.allRegionServersOffline = allRegionServersOffline;
    }

    @Override
    protected void chore() {
      Preconditions.checkState(tomActivated);
      boolean noRSAvailable = this.serverManager.createDestinationServersList().isEmpty();

      // Iterate all regions in transition checking for time outs
      long now = System.currentTimeMillis();
      // no lock concurrent access ok: we will be working on a copy, and it's java-valid to do
      //  a copy while another thread is adding/removing items
      for (String regionName : regionStates.getRegionsInTransition().keySet()) {
        RegionState regionState = regionStates.getRegionTransitionState(regionName);
        if (regionState == null) continue;

        if (regionState.getStamp() + timeout <= now) {
          // decide on action upon timeout
          actOnTimeOut(regionState);
        } else if (this.allRegionServersOffline && !noRSAvailable) {
          RegionPlan existingPlan = regionPlans.get(regionName);
          if (existingPlan == null
              || !this.serverManager.isServerOnline(existingPlan
                  .getDestination())) {
            // if some RSs just came back online, we can start the assignment
            // right away
            actOnTimeOut(regionState);
          }
        }
      }
      setAllRegionServersOffline(noRSAvailable);
    }

    private void actOnTimeOut(RegionState regionState) {
      HRegionInfo regionInfo = regionState.getRegion();
      LOG.info("Regions in transition timed out:  " + regionState);
      // Expired! Do a retry.
      switch (regionState.getState()) {
      case CLOSED:
        LOG.info("Region " + regionInfo.getEncodedName()
            + " has been CLOSED for too long, waiting on queued "
            + "ClosedRegionHandler to run or server shutdown");
        // Update our timestamp.
        regionState.updateTimestampToNow();
        break;
      case OFFLINE:
        LOG.info("Region has been OFFLINE for too long, " + "reassigning "
            + regionInfo.getRegionNameAsString() + " to a random server");
        invokeAssign(regionInfo);
        break;
      case PENDING_OPEN:
        LOG.info("Region has been PENDING_OPEN for too "
            + "long, reassigning region=" + regionInfo.getRegionNameAsString());
        invokeAssign(regionInfo);
        break;
      case OPENING:
        processOpeningState(regionInfo);
        break;
      case OPEN:
        LOG.error("Region has been OPEN for too long, " +
            "we don't know where region was opened so can't do anything");
        regionState.updateTimestampToNow();
        break;

      case PENDING_CLOSE:
        LOG.info("Region has been PENDING_CLOSE for too "
            + "long, running forced unassign again on region="
            + regionInfo.getRegionNameAsString());
        invokeUnassign(regionInfo);
        break;
      case CLOSING:
        LOG.info("Region has been CLOSING for too " +
          "long, this should eventually complete or the server will " +
          "expire, send RPC again");
        invokeUnassign(regionInfo);
        break;

      case SPLIT:
      case SPLITTING:
      case FAILED_OPEN:
      case FAILED_CLOSE:
      case MERGING:
        break;

      default:
        throw new IllegalStateException("Received event is not valid.");
      }
    }
  }

  private void processOpeningState(HRegionInfo regionInfo) {
    LOG.info("Region has been OPENING for too long, reassigning region="
        + regionInfo.getRegionNameAsString());
    // Should have a ZK node in OPENING state
    try {
      String node = ZKAssign.getNodeName(watcher, regionInfo.getEncodedName());
      Stat stat = new Stat();
      byte [] data = ZKAssign.getDataNoWatch(watcher, node, stat);
      if (data == null) {
        LOG.warn("Data is null, node " + node + " no longer exists");
        return;
      }
      RegionTransition rt = RegionTransition.parseFrom(data);
      EventType et = rt.getEventType();
      if (et == EventType.RS_ZK_REGION_OPENED) {
        LOG.debug("Region has transitioned to OPENED, allowing "
            + "watched event handlers to process");
        return;
      } else if (et != EventType.RS_ZK_REGION_OPENING && et != EventType.RS_ZK_REGION_FAILED_OPEN ) {
        LOG.warn("While timing out a region, found ZK node in unexpected state: " + et);
        return;
      }
      invokeAssign(regionInfo);
    } catch (KeeperException ke) {
      LOG.error("Unexpected ZK exception timing out CLOSING region", ke);
    } catch (DeserializationException e) {
      LOG.error("Unexpected exception parsing CLOSING region", e);
    }
  }

  void invokeAssign(HRegionInfo regionInfo) {
    threadPoolExecutorService.submit(new AssignCallable(this, regionInfo));
  }

  private void invokeUnassign(HRegionInfo regionInfo) {
    threadPoolExecutorService.submit(new UnAssignCallable(this, regionInfo));
  }

  public boolean isCarryingMeta(ServerName serverName) {
    return isCarryingRegion(serverName, HRegionInfo.FIRST_META_REGIONINFO);
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
  private boolean isCarryingRegion(ServerName serverName, HRegionInfo hri) {
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
      return matchZK;
    }

    ServerName addressFromAM = regionStates.getRegionServerOfRegion(hri);
    boolean matchAM = (addressFromAM != null &&
      addressFromAM.equals(serverName));
    LOG.debug("based on AM, current region=" + hri.getRegionNameAsString() +
      " is on server=" + (addressFromAM != null ? addressFromAM : "null") +
      " server being checked: " + serverName);

    return matchAM;
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
            || !(regionState.isFailedClose() || regionState.isPendingOpenOrOpening() || regionState
                .isOffline())) {
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
          if (zkTable.isDisablingOrDisabledTable(hri.getTable())) {
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
    synchronized (this.regionPlans) {
      this.regionPlans.put(plan.getRegionName(), plan);
    }
    HRegionInfo hri = plan.getRegionInfo();
    TableName tableName = hri.getTable();
    if (zkTable.isDisablingOrDisabledTable(tableName)) {
      LOG.info("Ignored moving region of disabling/disabled table "
        + tableName);
      return;
    }
    unassign(hri, false, plan.getDestination());
  }

  public void stop() {
    if (tomActivated){
      this.timeoutMonitor.interrupt();
      this.timerUpdater.interrupt();
    }
  }

  /**
   * Shutdown the threadpool executor service
   */
  public void shutdown() {
    // It's an immediate shutdown, so we're clearing the remaining tasks.
    synchronized (zkEventWorkerWaitingList){
      zkEventWorkerWaitingList.clear();
    }
    threadPoolExecutorService.shutdownNow();
    zkEventWorkers.shutdownNow();
  }

  protected void setEnabledTable(TableName tableName) {
    try {
      this.zkTable.setEnabledTable(tableName);
    } catch (KeeperException e) {
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
        if (RegionMergeTransaction.transitionMergingNode(watcher, p,
            hri_a, hri_b, sn, -1, EventType.RS_ZK_REQUEST_REGION_MERGE,
            EventType.RS_ZK_REGION_MERGING) == -1) {
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
        regionStates.regionOffline(p, State.MERGING_NEW);
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
      if (zkTable.isDisablingOrDisabledTable(p.getTable())) {
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
        if (SplitTransaction.transitionSplittingNode(watcher, p,
            hri_a, hri_b, sn, -1, EventType.RS_ZK_REQUEST_REGION_SPLIT,
            EventType.RS_ZK_REGION_SPLITTING) == -1) {
          byte[] data = ZKAssign.getData(watcher, encodedName);
          EventType currentType = null;
          if (data != null) {
            RegionTransition newRt = RegionTransition.parseFrom(data);
            currentType = newRt.getEventType();
          }
          if (currentType == null || (currentType != EventType.RS_ZK_REGION_SPLIT
              && currentType != EventType.RS_ZK_REGION_SPLITTING)) {
            LOG.warn("Failed to transition pending_split node "
              + encodedName + " to splitting, it's now " + currentType);
            return false;
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to transition pending_split node "
          + encodedName + " to splitting", e);
        return false;
      }
    }

    synchronized (regionStates) {
      regionStates.updateRegionState(hri_a, State.SPLITTING_NEW, sn);
      regionStates.updateRegionState(hri_b, State.SPLITTING_NEW, sn);
      regionStates.regionOffline(hri_a, State.SPLITTING_NEW);
      regionStates.regionOffline(hri_b, State.SPLITTING_NEW);
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
      }
    }

    if (et == EventType.RS_ZK_REGION_SPLIT) {
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
      if (zkTable.isDisablingOrDisabledTable(p.getTable())) {
        unassign(hri_a);
        unassign(hri_b);
      }
    }
    return true;
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
  }
}
