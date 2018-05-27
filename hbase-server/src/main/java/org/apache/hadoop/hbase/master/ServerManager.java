/*
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

import static org.apache.hadoop.hbase.util.CollectionUtils.computeIfAbsent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;

/**
 * The ServerManager class manages info about region servers.
 * <p>
 * Maintains lists of online and dead servers.  Processes the startups,
 * shutdowns, and deaths of region servers.
 * <p>
 * Servers are distinguished in two different ways.  A given server has a
 * location, specified by hostname and port, and of which there can only be one
 * online at any given time.  A server instance is specified by the location
 * (hostname and port) as well as the startcode (timestamp from when the server
 * was started).  This is used to differentiate a restarted instance of a given
 * server from the original instance.
 * <p>
 * If a sever is known not to be running any more, it is called dead. The dead
 * server needs to be handled by a ServerShutdownHandler.  If the handler is not
 * enabled yet, the server can't be handled right away so it is queued up.
 * After the handler is enabled, the server will be submitted to a handler to handle.
 * However, the handler may be just partially enabled.  If so,
 * the server cannot be fully processed, and be queued up for further processing.
 * A server is fully processed only after the handler is fully enabled
 * and has completed the handling.
 */
@InterfaceAudience.Private
public class ServerManager {
  public static final String WAIT_ON_REGIONSERVERS_MAXTOSTART =
      "hbase.master.wait.on.regionservers.maxtostart";

  public static final String WAIT_ON_REGIONSERVERS_MINTOSTART =
      "hbase.master.wait.on.regionservers.mintostart";

  public static final String WAIT_ON_REGIONSERVERS_TIMEOUT =
      "hbase.master.wait.on.regionservers.timeout";

  public static final String WAIT_ON_REGIONSERVERS_INTERVAL =
      "hbase.master.wait.on.regionservers.interval";

  private static final Logger LOG = LoggerFactory.getLogger(ServerManager.class);

  // Set if we are to shutdown the cluster.
  private AtomicBoolean clusterShutdown = new AtomicBoolean(false);

  /**
   * The last flushed sequence id for a region.
   */
  private final ConcurrentNavigableMap<byte[], Long> flushedSequenceIdByRegion =
    new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * The last flushed sequence id for a store in a region.
   */
  private final ConcurrentNavigableMap<byte[], ConcurrentNavigableMap<byte[], Long>>
    storeFlushedSequenceIdsByRegion = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  /** Map of registered servers to their current load */
  private final ConcurrentNavigableMap<ServerName, ServerMetrics> onlineServers =
    new ConcurrentSkipListMap<>();

  /**
   * Map of admin interfaces per registered regionserver; these interfaces we use to control
   * regionservers out on the cluster
   */
  private final Map<ServerName, AdminService.BlockingInterface> rsAdmins = new HashMap<>();

  /** List of region servers that should not get any more new regions. */
  private final ArrayList<ServerName> drainingServers = new ArrayList<>();

  private final MasterServices master;
  private final ClusterConnection connection;

  private final DeadServer deadservers = new DeadServer();

  private final long maxSkew;
  private final long warningSkew;

  private final RpcControllerFactory rpcControllerFactory;

  /**
   * Set of region servers which are dead but not processed immediately. If one
   * server died before master enables ServerShutdownHandler, the server will be
   * added to this set and will be processed through calling
   * {@link ServerManager#processQueuedDeadServers()} by master.
   * <p>
   * A dead server is a server instance known to be dead, not listed in the /hbase/rs
   * znode any more. It may have not been submitted to ServerShutdownHandler yet
   * because the handler is not enabled.
   * <p>
   * A dead server, which has been submitted to ServerShutdownHandler while the
   * handler is not enabled, is queued up.
   * <p>
   * So this is a set of region servers known to be dead but not submitted to
   * ServerShutdownHandler for processing yet.
   */
  private Set<ServerName> queuedDeadServers = new HashSet<>();

  /**
   * Set of region servers which are dead and submitted to ServerShutdownHandler to process but not
   * fully processed immediately.
   * <p>
   * If one server died before assignment manager finished the failover cleanup, the server will be
   * added to this set and will be processed through calling
   * {@link ServerManager#processQueuedDeadServers()} by assignment manager.
   * <p>
   * The Boolean value indicates whether log split is needed inside ServerShutdownHandler
   * <p>
   * ServerShutdownHandler processes a dead server submitted to the handler after the handler is
   * enabled. It may not be able to complete the processing because meta is not yet online or master
   * is currently in startup mode. In this case, the dead server will be parked in this set
   * temporarily.
   */
  private Map<ServerName, Boolean> requeuedDeadServers = new ConcurrentHashMap<>();

  /** Listeners that are called on server events. */
  private List<ServerListener> listeners = new CopyOnWriteArrayList<>();

  /**
   * Constructor.
   * @param master
   * @throws ZooKeeperConnectionException
   */
  public ServerManager(final MasterServices master) {
    this(master, true);
  }

  ServerManager(final MasterServices master, final boolean connect) {
    this.master = master;
    Configuration c = master.getConfiguration();
    maxSkew = c.getLong("hbase.master.maxclockskew", 30000);
    warningSkew = c.getLong("hbase.master.warningclockskew", 10000);
    this.connection = connect? master.getClusterConnection(): null;
    this.rpcControllerFactory = this.connection == null? null: connection.getRpcControllerFactory();
  }

  /**
   * Add the listener to the notification list.
   * @param listener The ServerListener to register
   */
  public void registerListener(final ServerListener listener) {
    this.listeners.add(listener);
  }

  /**
   * Remove the listener from the notification list.
   * @param listener The ServerListener to unregister
   */
  public boolean unregisterListener(final ServerListener listener) {
    return this.listeners.remove(listener);
  }

  /**
   * Let the server manager know a new regionserver has come online
   * @param request the startup request
   * @param ia the InetAddress from which request is received
   * @return The ServerName we know this server as.
   * @throws IOException
   */
  ServerName regionServerStartup(RegionServerStartupRequest request, InetAddress ia)
      throws IOException {
    // Test for case where we get a region startup message from a regionserver
    // that has been quickly restarted but whose znode expiration handler has
    // not yet run, or from a server whose fail we are currently processing.
    // Test its host+port combo is present in serverAddressToServerInfo.  If it
    // is, reject the server and trigger its expiration. The next time it comes
    // in, it should have been removed from serverAddressToServerInfo and queued
    // for processing by ProcessServerShutdown.

    final String hostname = request.hasUseThisHostnameInstead() ?
        request.getUseThisHostnameInstead() :ia.getHostName();
    ServerName sn = ServerName.valueOf(hostname, request.getPort(),
      request.getServerStartCode());
    checkClockSkew(sn, request.getServerCurrentTime());
    checkIsDead(sn, "STARTUP");
    if (!checkAndRecordNewServer(sn, ServerMetricsBuilder.of(sn))) {
      LOG.warn("THIS SHOULD NOT HAPPEN, RegionServerStartup"
        + " could not record the server: " + sn);
    }
    return sn;
  }

  /**
   * Updates last flushed sequence Ids for the regions on server sn
   * @param sn
   * @param hsl
   */
  private void updateLastFlushedSequenceIds(ServerName sn, ServerMetrics hsl) {
    for (Entry<byte[], RegionMetrics> entry : hsl.getRegionMetrics().entrySet()) {
      byte[] encodedRegionName = Bytes.toBytes(RegionInfo.encodeRegionName(entry.getKey()));
      Long existingValue = flushedSequenceIdByRegion.get(encodedRegionName);
      long l = entry.getValue().getCompletedSequenceId();
      // Don't let smaller sequence ids override greater sequence ids.
      if (LOG.isTraceEnabled()) {
        LOG.trace(Bytes.toString(encodedRegionName) + ", existingValue=" + existingValue +
          ", completeSequenceId=" + l);
      }
      if (existingValue == null || (l != HConstants.NO_SEQNUM && l > existingValue)) {
        flushedSequenceIdByRegion.put(encodedRegionName, l);
      } else if (l != HConstants.NO_SEQNUM && l < existingValue) {
        LOG.warn("RegionServer " + sn + " indicates a last flushed sequence id ("
            + l + ") that is less than the previous last flushed sequence id ("
            + existingValue + ") for region " + Bytes.toString(entry.getKey()) + " Ignoring.");
      }
      ConcurrentNavigableMap<byte[], Long> storeFlushedSequenceId =
          computeIfAbsent(storeFlushedSequenceIdsByRegion, encodedRegionName,
            () -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
      for (Entry<byte[], Long> storeSeqId : entry.getValue().getStoreSequenceId().entrySet()) {
        byte[] family = storeSeqId.getKey();
        existingValue = storeFlushedSequenceId.get(family);
        l = storeSeqId.getValue();
        if (LOG.isTraceEnabled()) {
          LOG.trace(Bytes.toString(encodedRegionName) + ", family=" + Bytes.toString(family) +
            ", existingValue=" + existingValue + ", completeSequenceId=" + l);
        }
        // Don't let smaller sequence ids override greater sequence ids.
        if (existingValue == null || (l != HConstants.NO_SEQNUM && l > existingValue.longValue())) {
          storeFlushedSequenceId.put(family, l);
        }
      }
    }
  }

  @VisibleForTesting
  public void regionServerReport(ServerName sn,
    ServerMetrics sl) throws YouAreDeadException {
    checkIsDead(sn, "REPORT");
    if (null == this.onlineServers.replace(sn, sl)) {
      // Already have this host+port combo and its just different start code?
      // Just let the server in. Presume master joining a running cluster.
      // recordNewServer is what happens at the end of reportServerStartup.
      // The only thing we are skipping is passing back to the regionserver
      // the ServerName to use. Here we presume a master has already done
      // that so we'll press on with whatever it gave us for ServerName.
      if (!checkAndRecordNewServer(sn, sl)) {
        LOG.info("RegionServerReport ignored, could not record the server: " + sn);
        return; // Not recorded, so no need to move on
      }
    }
    updateLastFlushedSequenceIds(sn, sl);
  }

  /**
   * Check is a server of same host and port already exists,
   * if not, or the existed one got a smaller start code, record it.
   *
   * @param serverName the server to check and record
   * @param sl the server load on the server
   * @return true if the server is recorded, otherwise, false
   */
  boolean checkAndRecordNewServer(final ServerName serverName, final ServerMetrics sl) {
    ServerName existingServer = null;
    synchronized (this.onlineServers) {
      existingServer = findServerWithSameHostnamePortWithLock(serverName);
      if (existingServer != null && (existingServer.getStartcode() > serverName.getStartcode())) {
        LOG.info("Server serverName=" + serverName + " rejected; we already have "
            + existingServer.toString() + " registered with same hostname and port");
        return false;
      }
      recordNewServerWithLock(serverName, sl);
    }

    // Tell our listeners that a server was added
    if (!this.listeners.isEmpty()) {
      for (ServerListener listener : this.listeners) {
        listener.serverAdded(serverName);
      }
    }

    // Note that we assume that same ts means same server, and don't expire in that case.
    //  TODO: ts can theoretically collide due to clock shifts, so this is a bit hacky.
    if (existingServer != null &&
        (existingServer.getStartcode() < serverName.getStartcode())) {
      LOG.info("Triggering server recovery; existingServer " +
          existingServer + " looks stale, new server:" + serverName);
      expireServer(existingServer);
    }
    return true;
  }

  /**
   * Checks if the clock skew between the server and the master. If the clock skew exceeds the
   * configured max, it will throw an exception; if it exceeds the configured warning threshold,
   * it will log a warning but start normally.
   * @param serverName Incoming servers's name
   * @param serverCurrentTime
   * @throws ClockOutOfSyncException if the skew exceeds the configured max value
   */
  private void checkClockSkew(final ServerName serverName, final long serverCurrentTime)
  throws ClockOutOfSyncException {
    long skew = Math.abs(System.currentTimeMillis() - serverCurrentTime);
    if (skew > maxSkew) {
      String message = "Server " + serverName + " has been " +
        "rejected; Reported time is too far out of sync with master.  " +
        "Time difference of " + skew + "ms > max allowed of " + maxSkew + "ms";
      LOG.warn(message);
      throw new ClockOutOfSyncException(message);
    } else if (skew > warningSkew){
      String message = "Reported time for server " + serverName + " is out of sync with master " +
        "by " + skew + "ms. (Warning threshold is " + warningSkew + "ms; " +
        "error threshold is " + maxSkew + "ms)";
      LOG.warn(message);
    }
  }

  /**
   * If this server is on the dead list, reject it with a YouAreDeadException.
   * If it was dead but came back with a new start code, remove the old entry
   * from the dead list.
   * @param serverName
   * @param what START or REPORT
   * @throws org.apache.hadoop.hbase.YouAreDeadException
   */
  private void checkIsDead(final ServerName serverName, final String what)
      throws YouAreDeadException {
    if (this.deadservers.isDeadServer(serverName)) {
      // host name, port and start code all match with existing one of the
      // dead servers. So, this server must be dead.
      String message = "Server " + what + " rejected; currently processing " +
          serverName + " as dead server";
      LOG.debug(message);
      throw new YouAreDeadException(message);
    }
    // remove dead server with same hostname and port of newly checking in rs after master
    // initialization.See HBASE-5916 for more information.
    if ((this.master == null || this.master.isInitialized())
        && this.deadservers.cleanPreviousInstance(serverName)) {
      // This server has now become alive after we marked it as dead.
      // We removed it's previous entry from the dead list to reflect it.
      LOG.debug(what + ":" + " Server " + serverName + " came back up," +
          " removed it from the dead servers list");
    }
  }

  /**
   * Assumes onlineServers is locked.
   * @return ServerName with matching hostname and port.
   */
  private ServerName findServerWithSameHostnamePortWithLock(
      final ServerName serverName) {
    ServerName end = ServerName.valueOf(serverName.getHostname(), serverName.getPort(),
        Long.MAX_VALUE);

    ServerName r = onlineServers.lowerKey(end);
    if (r != null) {
      if (ServerName.isSameAddress(r, serverName)) {
        return r;
      }
    }
    return null;
  }

  /**
   * Adds the onlineServers list. onlineServers should be locked.
   * @param serverName The remote servers name.
   */
  @VisibleForTesting
  void recordNewServerWithLock(final ServerName serverName, final ServerMetrics sl) {
    LOG.info("Registering regionserver=" + serverName);
    this.onlineServers.put(serverName, sl);
    this.rsAdmins.remove(serverName);
  }

  public RegionStoreSequenceIds getLastFlushedSequenceId(byte[] encodedRegionName) {
    RegionStoreSequenceIds.Builder builder = RegionStoreSequenceIds.newBuilder();
    Long seqId = flushedSequenceIdByRegion.get(encodedRegionName);
    builder.setLastFlushedSequenceId(seqId != null ? seqId.longValue() : HConstants.NO_SEQNUM);
    Map<byte[], Long> storeFlushedSequenceId =
        storeFlushedSequenceIdsByRegion.get(encodedRegionName);
    if (storeFlushedSequenceId != null) {
      for (Map.Entry<byte[], Long> entry : storeFlushedSequenceId.entrySet()) {
        builder.addStoreSequenceId(StoreSequenceId.newBuilder()
            .setFamilyName(UnsafeByteOperations.unsafeWrap(entry.getKey()))
            .setSequenceId(entry.getValue().longValue()).build());
      }
    }
    return builder.build();
  }

  /**
   * @param serverName
   * @return ServerMetrics if serverName is known else null
   */
  public ServerMetrics getLoad(final ServerName serverName) {
    return this.onlineServers.get(serverName);
  }

  /**
   * Compute the average load across all region servers.
   * Currently, this uses a very naive computation - just uses the number of
   * regions being served, ignoring stats about number of requests.
   * @return the average load
   */
  public double getAverageLoad() {
    int totalLoad = 0;
    int numServers = 0;
    for (ServerMetrics sl : this.onlineServers.values()) {
      numServers++;
      totalLoad += sl.getRegionMetrics().size();
    }
    return numServers == 0 ? 0 :
      (double)totalLoad / (double)numServers;
  }

  /** @return the count of active regionservers */
  public int countOfRegionServers() {
    // Presumes onlineServers is a concurrent map
    return this.onlineServers.size();
  }

  /**
   * @return Read-only map of servers to serverinfo
   */
  public Map<ServerName, ServerMetrics> getOnlineServers() {
    // Presumption is that iterating the returned Map is OK.
    synchronized (this.onlineServers) {
      return Collections.unmodifiableMap(this.onlineServers);
    }
  }

  public DeadServer getDeadServers() {
    return this.deadservers;
  }

  /**
   * Checks if any dead servers are currently in progress.
   * @return true if any RS are being processed as dead, false if not
   */
  public boolean areDeadServersInProgress() {
    return this.deadservers.areDeadServersInProgress();
  }

  void letRegionServersShutdown() {
    long previousLogTime = 0;
    ServerName sn = master.getServerName();
    ZKWatcher zkw = master.getZooKeeper();
    int onlineServersCt;
    while ((onlineServersCt = onlineServers.size()) > 0){

      if (System.currentTimeMillis() > (previousLogTime + 1000)) {
        Set<ServerName> remainingServers = onlineServers.keySet();
        synchronized (onlineServers) {
          if (remainingServers.size() == 1 && remainingServers.contains(sn)) {
            // Master will delete itself later.
            return;
          }
        }
        StringBuilder sb = new StringBuilder();
        // It's ok here to not sync on onlineServers - merely logging
        for (ServerName key : remainingServers) {
          if (sb.length() > 0) {
            sb.append(", ");
          }
          sb.append(key);
        }
        LOG.info("Waiting on regionserver(s) " + sb.toString());
        previousLogTime = System.currentTimeMillis();
      }

      try {
        List<String> servers = getRegionServersInZK(zkw);
        if (servers == null || servers.isEmpty() || (servers.size() == 1
            && servers.contains(sn.toString()))) {
          LOG.info("ZK shows there is only the master self online, exiting now");
          // Master could have lost some ZK events, no need to wait more.
          break;
        }
      } catch (KeeperException ke) {
        LOG.warn("Failed to list regionservers", ke);
        // ZK is malfunctioning, don't hang here
        break;
      }
      synchronized (onlineServers) {
        try {
          if (onlineServersCt == onlineServers.size()) onlineServers.wait(100);
        } catch (InterruptedException ignored) {
          // continue
        }
      }
    }
  }

  private List<String> getRegionServersInZK(final ZKWatcher zkw)
  throws KeeperException {
    return ZKUtil.listChildrenNoWatch(zkw, zkw.getZNodePaths().rsZNode);
  }

  /*
   * Expire the passed server.  Add it to list of dead servers and queue a
   * shutdown processing.
   * @return True if we queued a ServerCrashProcedure else false if we did not (could happen
   * for many reasons including the fact that its this server that is going down or we already
   * have queued an SCP for this server or SCP processing is currently disabled because we are
   * in startup phase).
   */
  public synchronized boolean expireServer(final ServerName serverName) {
    // THIS server is going down... can't handle our own expiration.
    if (serverName.equals(master.getServerName())) {
      if (!(master.isAborted() || master.isStopped())) {
        master.stop("We lost our znode?");
      }
      return false;
    }
    // No SCP handling during startup.
    if (!master.isServerCrashProcessingEnabled()) {
      LOG.info("Master doesn't enable ServerShutdownHandler during initialization, "
          + "delay expiring server " + serverName);
      // Even though we delay expire of this server, we still need to handle Meta's RIT
      // that are against the crashed server; since when we do RecoverMetaProcedure,
      // the SCP is not enabled yet and Meta's RIT may be suspend forever. See HBase-19287
      master.getAssignmentManager().handleMetaRITOnCrashedServer(serverName);
      this.queuedDeadServers.add(serverName);
      // Return true because though on SCP queued, there will be one queued later.
      return true;
    }
    if (this.deadservers.isDeadServer(serverName)) {
      LOG.warn("Expiration called on {} but crash processing already in progress", serverName);
      return false;
    }
    moveFromOnlineToDeadServers(serverName);

    // If cluster is going down, yes, servers are going to be expiring; don't
    // process as a dead server
    if (isClusterShutdown()) {
      LOG.info("Cluster shutdown set; " + serverName +
        " expired; onlineServers=" + this.onlineServers.size());
      if (this.onlineServers.isEmpty()) {
        master.stop("Cluster shutdown set; onlineServer=0");
      }
      return false;
    }
    LOG.info("Processing expiration of " + serverName + " on " + this.master.getServerName());
    master.getAssignmentManager().submitServerCrash(serverName, true);

    // Tell our listeners that a server was removed
    if (!this.listeners.isEmpty()) {
      for (ServerListener listener : this.listeners) {
        listener.serverRemoved(serverName);
      }
    }
    return true;
  }

  @VisibleForTesting
  public void moveFromOnlineToDeadServers(final ServerName sn) {
    synchronized (onlineServers) {
      if (!this.onlineServers.containsKey(sn)) {
        LOG.warn("Expiration of " + sn + " but server not online");
      }
      // Remove the server from the known servers lists and update load info BUT
      // add to deadservers first; do this so it'll show in dead servers list if
      // not in online servers list.
      this.deadservers.add(sn);
      this.onlineServers.remove(sn);
      onlineServers.notifyAll();
    }
    this.rsAdmins.remove(sn);
  }

  public synchronized void processDeadServer(final ServerName serverName, boolean shouldSplitWal) {
    // When assignment manager is cleaning up the zookeeper nodes and rebuilding the
    // in-memory region states, region servers could be down. Meta table can and
    // should be re-assigned, log splitting can be done too. However, it is better to
    // wait till the cleanup is done before re-assigning user regions.
    //
    // We should not wait in the server shutdown handler thread since it can clog
    // the handler threads and meta table could not be re-assigned in case
    // the corresponding server is down. So we queue them up here instead.
    if (!master.getAssignmentManager().isFailoverCleanupDone()) {
      requeuedDeadServers.put(serverName, shouldSplitWal);
      return;
    }

    this.deadservers.add(serverName);
    master.getAssignmentManager().submitServerCrash(serverName, shouldSplitWal);
  }

  /**
   * Process the servers which died during master's initialization. It will be
   * called after HMaster#assignMeta and AssignmentManager#joinCluster.
   * */
  synchronized void processQueuedDeadServers() {
    if (!master.isServerCrashProcessingEnabled()) {
      LOG.info("Master hasn't enabled ServerShutdownHandler");
    }
    Iterator<ServerName> serverIterator = queuedDeadServers.iterator();
    while (serverIterator.hasNext()) {
      ServerName tmpServerName = serverIterator.next();
      expireServer(tmpServerName);
      serverIterator.remove();
      requeuedDeadServers.remove(tmpServerName);
    }

    if (!master.getAssignmentManager().isFailoverCleanupDone()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("AssignmentManager failover cleanup not done.");
      }
    }

    for (Map.Entry<ServerName, Boolean> entry : requeuedDeadServers.entrySet()) {
      processDeadServer(entry.getKey(), entry.getValue());
    }
    requeuedDeadServers.clear();
  }

  /*
   * Remove the server from the drain list.
   */
  public synchronized boolean removeServerFromDrainList(final ServerName sn) {
    // Warn if the server (sn) is not online.  ServerName is of the form:
    // <hostname> , <port> , <startcode>

    if (!this.isServerOnline(sn)) {
      LOG.warn("Server " + sn + " is not currently online. " +
               "Removing from draining list anyway, as requested.");
    }
    // Remove the server from the draining servers lists.
    return this.drainingServers.remove(sn);
  }

  /**
   * Add the server to the drain list.
   * @param sn
   * @return True if the server is added or the server is already on the drain list.
   */
  public synchronized boolean addServerToDrainList(final ServerName sn) {
    // Warn if the server (sn) is not online.  ServerName is of the form:
    // <hostname> , <port> , <startcode>

    if (!this.isServerOnline(sn)) {
      LOG.warn("Server " + sn + " is not currently online. " +
               "Ignoring request to add it to draining list.");
      return false;
    }
    // Add the server to the draining servers lists, if it's not already in
    // it.
    if (this.drainingServers.contains(sn)) {
      LOG.warn("Server " + sn + " is already in the draining server list." +
               "Ignoring request to add it again.");
      return true;
    }
    LOG.info("Server " + sn + " added to draining server list.");
    return this.drainingServers.add(sn);
  }

  // RPC methods to region servers

  private HBaseRpcController newRpcController() {
    return rpcControllerFactory == null ? null : rpcControllerFactory.newController();
  }

  /**
   * Sends a WARMUP RPC to the specified server to warmup the specified region.
   * <p>
   * A region server could reject the close request because it either does not
   * have the specified region or the region is being split.
   * @param server server to warmup a region
   * @param region region to  warmup
   */
  public void sendRegionWarmup(ServerName server,
      RegionInfo region) {
    if (server == null) return;
    try {
      AdminService.BlockingInterface admin = getRsAdmin(server);
      HBaseRpcController controller = newRpcController();
      ProtobufUtil.warmupRegion(controller, admin, region);
    } catch (IOException e) {
      LOG.error("Received exception in RPC for warmup server:" +
        server + "region: " + region +
        "exception: " + e);
    }
  }

  /**
   * Contacts a region server and waits up to timeout ms
   * to close the region.  This bypasses the active hmaster.
   */
  public static void closeRegionSilentlyAndWait(ClusterConnection connection,
    ServerName server, RegionInfo region, long timeout) throws IOException, InterruptedException {
    AdminService.BlockingInterface rs = connection.getAdmin(server);
    HBaseRpcController controller = connection.getRpcControllerFactory().newController();
    try {
      ProtobufUtil.closeRegion(controller, rs, server, region.getRegionName());
    } catch (IOException e) {
      LOG.warn("Exception when closing region: " + region.getRegionNameAsString(), e);
    }
    long expiration = timeout + System.currentTimeMillis();
    while (System.currentTimeMillis() < expiration) {
      controller.reset();
      try {
        RegionInfo rsRegion =
          ProtobufUtil.getRegionInfo(controller, rs, region.getRegionName());
        if (rsRegion == null) return;
      } catch (IOException ioe) {
        if (ioe instanceof NotServingRegionException) // no need to retry again
          return;
        LOG.warn("Exception when retrieving regioninfo from: "
          + region.getRegionNameAsString(), ioe);
      }
      Thread.sleep(1000);
    }
    throw new IOException("Region " + region + " failed to close within"
        + " timeout " + timeout);
  }

  /**
   * @param sn
   * @return Admin interface for the remote regionserver named <code>sn</code>
   * @throws IOException
   * @throws RetriesExhaustedException wrapping a ConnectException if failed
   */
  public AdminService.BlockingInterface getRsAdmin(final ServerName sn)
  throws IOException {
    AdminService.BlockingInterface admin = this.rsAdmins.get(sn);
    if (admin == null) {
      LOG.debug("New admin connection to " + sn.toString());
      if (sn.equals(master.getServerName()) && master instanceof HRegionServer) {
        // A master is also a region server now, see HBASE-10569 for details
        admin = ((HRegionServer)master).getRSRpcServices();
      } else {
        admin = this.connection.getAdmin(sn);
      }
      this.rsAdmins.put(sn, admin);
    }
    return admin;
  }

  /**
   * Calculate min necessary to start. This is not an absolute. It is just
   * a friction that will cause us hang around a bit longer waiting on
   * RegionServers to check-in.
   */
  private int getMinToStart() {
    // One server should be enough to get us off the ground.
    int requiredMinToStart = 1;
    if (LoadBalancer.isTablesOnMaster(master.getConfiguration())) {
      if (LoadBalancer.isSystemTablesOnlyOnMaster(master.getConfiguration())) {
        // If Master is carrying regions but NOT user-space regions, it
        // still shows as a 'server'. We need at least one more server to check
        // in before we can start up so set defaultMinToStart to 2.
        requiredMinToStart = requiredMinToStart + 1;
      }
    }
    int minToStart = this.master.getConfiguration().getInt(WAIT_ON_REGIONSERVERS_MINTOSTART, -1);
    // Ensure we are never less than requiredMinToStart else stuff won't work.
    return minToStart == -1 || minToStart < requiredMinToStart? requiredMinToStart: minToStart;
  }

  /**
   * Wait for the region servers to report in.
   * We will wait until one of this condition is met:
   *  - the master is stopped
   *  - the 'hbase.master.wait.on.regionservers.maxtostart' number of
   *    region servers is reached
   *  - the 'hbase.master.wait.on.regionservers.mintostart' is reached AND
   *   there have been no new region server in for
   *      'hbase.master.wait.on.regionservers.interval' time AND
   *   the 'hbase.master.wait.on.regionservers.timeout' is reached
   *
   * @throws InterruptedException
   */
  public void waitForRegionServers(MonitoredTask status) throws InterruptedException {
    final long interval = this.master.getConfiguration().
        getLong(WAIT_ON_REGIONSERVERS_INTERVAL, 1500);
    final long timeout = this.master.getConfiguration().
        getLong(WAIT_ON_REGIONSERVERS_TIMEOUT, 4500);
    // Min is not an absolute; just a friction making us wait longer on server checkin.
    int minToStart = getMinToStart();
    int maxToStart = this.master.getConfiguration().
        getInt(WAIT_ON_REGIONSERVERS_MAXTOSTART, Integer.MAX_VALUE);
    if (maxToStart < minToStart) {
      LOG.warn(String.format("The value of '%s' (%d) is set less than '%s' (%d), ignoring.",
          WAIT_ON_REGIONSERVERS_MAXTOSTART, maxToStart,
          WAIT_ON_REGIONSERVERS_MINTOSTART, minToStart));
      maxToStart = Integer.MAX_VALUE;
    }

    long now =  System.currentTimeMillis();
    final long startTime = now;
    long slept = 0;
    long lastLogTime = 0;
    long lastCountChange = startTime;
    int count = countOfRegionServers();
    int oldCount = 0;
    // This while test is a little hard to read. We try to comment it in below but in essence:
    // Wait if Master is not stopped and the number of regionservers that have checked-in is
    // less than the maxToStart. Both of these conditions will be true near universally.
    // Next, we will keep cycling if ANY of the following three conditions are true:
    // 1. The time since a regionserver registered is < interval (means servers are actively checking in).
    // 2. We are under the total timeout.
    // 3. The count of servers is < minimum.
    for (ServerListener listener: this.listeners) {
      listener.waiting();
    }
    while (!this.master.isStopped() && !isClusterShutdown() && count < maxToStart &&
        ((lastCountChange + interval) > now || timeout > slept || count < minToStart)) {
      // Log some info at every interval time or if there is a change
      if (oldCount != count || lastLogTime + interval < now) {
        lastLogTime = now;
        String msg =
            "Waiting on regionserver count=" + count + "; waited="+
                slept + "ms, expecting min=" + minToStart + " server(s), max="+ getStrForMax(maxToStart) +
                " server(s), " + "timeout=" + timeout + "ms, lastChange=" + (lastCountChange - now) + "ms";
        LOG.info(msg);
        status.setStatus(msg);
      }

      // We sleep for some time
      final long sleepTime = 50;
      Thread.sleep(sleepTime);
      now =  System.currentTimeMillis();
      slept = now - startTime;

      oldCount = count;
      count = countOfRegionServers();
      if (count != oldCount) {
        lastCountChange = now;
      }
    }
    // Did we exit the loop because cluster is going down?
    if (isClusterShutdown()) {
      this.master.stop("Cluster shutdown");
    }
    LOG.info("Finished waiting on RegionServer count=" + count + "; waited=" + slept + "ms," +
        " expected min=" + minToStart + " server(s), max=" +  getStrForMax(maxToStart) + " server(s),"+
        " master is "+ (this.master.isStopped() ? "stopped.": "running"));
  }

  private String getStrForMax(final int max) {
    return max == Integer.MAX_VALUE? "NO_LIMIT": Integer.toString(max);
  }

  /**
   * @return A copy of the internal list of online servers.
   */
  public List<ServerName> getOnlineServersList() {
    // TODO: optimize the load balancer call so we don't need to make a new list
    // TODO: FIX. THIS IS POPULAR CALL.
    return new ArrayList<>(this.onlineServers.keySet());
  }

  /**
   * @param keys The target server name
   * @param idleServerPredicator Evaluates the server on the given load
   * @return A copy of the internal list of online servers matched by the predicator
   */
  public List<ServerName> getOnlineServersListWithPredicator(List<ServerName> keys,
    Predicate<ServerMetrics> idleServerPredicator) {
    List<ServerName> names = new ArrayList<>();
    if (keys != null && idleServerPredicator != null) {
      keys.forEach(name -> {
        ServerMetrics load = onlineServers.get(name);
        if (load != null) {
          if (idleServerPredicator.test(load)) {
            names.add(name);
          }
        }
      });
    }
    return names;
  }

  /**
   * @return A copy of the internal list of draining servers.
   */
  public List<ServerName> getDrainingServersList() {
    return new ArrayList<>(this.drainingServers);
  }

  /**
   * @return A copy of the internal set of deadNotExpired servers.
   */
  Set<ServerName> getDeadNotExpiredServers() {
    return new HashSet<>(this.queuedDeadServers);
  }

  public boolean isServerOnline(ServerName serverName) {
    return serverName != null && onlineServers.containsKey(serverName);
  }

  /**
   * Check if a server is known to be dead.  A server can be online,
   * or known to be dead, or unknown to this manager (i.e, not online,
   * not known to be dead either. it is simply not tracked by the
   * master any more, for example, a very old previous instance).
   */
  public synchronized boolean isServerDead(ServerName serverName) {
    return serverName == null || deadservers.isDeadServer(serverName)
      || queuedDeadServers.contains(serverName)
      || requeuedDeadServers.containsKey(serverName);
  }

  public void shutdownCluster() {
    String statusStr = "Cluster shutdown requested of master=" + this.master.getServerName();
    LOG.info(statusStr);
    this.clusterShutdown.set(true);
    if (onlineServers.isEmpty()) {
      // we do not synchronize here so this may cause a double stop, but not a big deal
      master.stop("OnlineServer=0 right after cluster shutdown set");
    }
  }

  boolean isClusterShutdown() {
    return this.clusterShutdown.get();
  }

  /**
   * Stop the ServerManager.
   */
  public void stop() {
    // Nothing to do.
  }

  /**
   * Creates a list of possible destinations for a region. It contains the online servers, but not
   *  the draining or dying servers.
   *  @param serversToExclude can be null if there is no server to exclude
   */
  public List<ServerName> createDestinationServersList(final List<ServerName> serversToExclude){
    final List<ServerName> destServers = getOnlineServersList();

    if (serversToExclude != null) {
      destServers.removeAll(serversToExclude);
    }

    // Loop through the draining server list and remove them from the server list
    final List<ServerName> drainingServersCopy = getDrainingServersList();
    destServers.removeAll(drainingServersCopy);

    // Remove the deadNotExpired servers from the server list.
    removeDeadNotExpiredServers(destServers);
    return destServers;
  }

  /**
   * Calls {@link #createDestinationServersList} without server to exclude.
   */
  public List<ServerName> createDestinationServersList(){
    return createDestinationServersList(null);
  }

    /**
    * Loop through the deadNotExpired server list and remove them from the
    * servers.
    * This function should be used carefully outside of this class. You should use a high level
    *  method such as {@link #createDestinationServersList()} instead of managing you own list.
    */
  void removeDeadNotExpiredServers(List<ServerName> servers) {
    Set<ServerName> deadNotExpiredServersCopy = this.getDeadNotExpiredServers();
    if (!deadNotExpiredServersCopy.isEmpty()) {
      for (ServerName server : deadNotExpiredServersCopy) {
        LOG.debug("Removing dead but not expired server: " + server
          + " from eligible server pool.");
        servers.remove(server);
      }
    }
  }

  /**
   * To clear any dead server with same host name and port of any online server
   */
  void clearDeadServersWithSameHostNameAndPortOfOnlineServer() {
    for (ServerName serverName : getOnlineServersList()) {
      deadservers.cleanAllPreviousInstances(serverName);
    }
  }

  /**
   * Called by delete table and similar to notify the ServerManager that a region was removed.
   */
  public void removeRegion(final RegionInfo regionInfo) {
    final byte[] encodedName = regionInfo.getEncodedNameAsBytes();
    storeFlushedSequenceIdsByRegion.remove(encodedName);
    flushedSequenceIdByRegion.remove(encodedName);
  }

  @VisibleForTesting
  public boolean isRegionInServerManagerStates(final RegionInfo hri) {
    final byte[] encodedName = hri.getEncodedNameAsBytes();
    return (storeFlushedSequenceIdsByRegion.containsKey(encodedName)
        || flushedSequenceIdByRegion.containsKey(encodedName));
  }

  /**
   * Called by delete table and similar to notify the ServerManager that a region was removed.
   */
  public void removeRegions(final List<RegionInfo> regions) {
    for (RegionInfo hri: regions) {
      removeRegion(hri);
    }
  }
}
