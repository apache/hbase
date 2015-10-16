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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.handler.MetaServerShutdownHandler;
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

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

  private static final Log LOG = LogFactory.getLog(ServerManager.class);

  // Set if we are to shutdown the cluster.
  private volatile boolean clusterShutdown = false;

  private final ConcurrentNavigableMap<byte[], Long> flushedSequenceIdByRegion =
    new ConcurrentSkipListMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

  private final ConcurrentNavigableMap<byte[], ConcurrentNavigableMap<byte[], Long>>
    storeFlushedSequenceIdsByRegion =
    new ConcurrentSkipListMap<byte[], ConcurrentNavigableMap<byte[], Long>>(Bytes.BYTES_COMPARATOR);

  /** Map of registered servers to their current load */
  private final ConcurrentHashMap<ServerName, ServerLoad> onlineServers =
    new ConcurrentHashMap<ServerName, ServerLoad>();

  /**
   * Map of admin interfaces per registered regionserver; these interfaces we use to control
   * regionservers out on the cluster
   */
  private final Map<ServerName, AdminService.BlockingInterface> rsAdmins =
    new HashMap<ServerName, AdminService.BlockingInterface>();

  /**
   * List of region servers <ServerName> that should not get any more new
   * regions.
   */
  private final ArrayList<ServerName> drainingServers =
    new ArrayList<ServerName>();

  private final Server master;
  private final MasterServices services;
  private final ClusterConnection connection;

  private final DeadServer deadservers = new DeadServer();

  private final long maxSkew;
  private final long warningSkew;

  private final RetryCounterFactory pingRetryCounterFactory;

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
   * ServerShutdownHander for processing yet.
   */
  private Set<ServerName> queuedDeadServers = new HashSet<ServerName>();

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
  private Map<ServerName, Boolean> requeuedDeadServers
    = new ConcurrentHashMap<ServerName, Boolean>();

  /** Listeners that are called on server events. */
  private List<ServerListener> listeners = new CopyOnWriteArrayList<ServerListener>();

  /**
   * Constructor.
   * @param master
   * @param services
   * @throws ZooKeeperConnectionException
   */
  public ServerManager(final Server master, final MasterServices services)
      throws IOException {
    this(master, services, true);
  }

  ServerManager(final Server master, final MasterServices services,
      final boolean connect) throws IOException {
    this.master = master;
    this.services = services;
    Configuration c = master.getConfiguration();
    maxSkew = c.getLong("hbase.master.maxclockskew", 30000);
    warningSkew = c.getLong("hbase.master.warningclockskew", 10000);
    this.connection = connect ? (ClusterConnection)ConnectionFactory.createConnection(c) : null;
    int pingMaxAttempts = Math.max(1, master.getConfiguration().getInt(
      "hbase.master.maximum.ping.server.attempts", 10));
    int pingSleepInterval = Math.max(1, master.getConfiguration().getInt(
      "hbase.master.ping.server.retry.sleep.interval", 100));
    this.pingRetryCounterFactory = new RetryCounterFactory(pingMaxAttempts, pingSleepInterval);
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
    // Test its host+port combo is present in serverAddresstoServerInfo.  If it
    // is, reject the server and trigger its expiration. The next time it comes
    // in, it should have been removed from serverAddressToServerInfo and queued
    // for processing by ProcessServerShutdown.

    final String hostname = request.hasUseThisHostnameInstead() ?
        request.getUseThisHostnameInstead() :ia.getHostName();
    ServerName sn = ServerName.valueOf(hostname, request.getPort(),
      request.getServerStartCode());
    checkClockSkew(sn, request.getServerCurrentTime());
    checkIsDead(sn, "STARTUP");
    if (!checkAndRecordNewServer(sn, ServerLoad.EMPTY_SERVERLOAD)) {
      LOG.warn("THIS SHOULD NOT HAPPEN, RegionServerStartup"
        + " could not record the server: " + sn);
    }
    return sn;
  }

  private ConcurrentNavigableMap<byte[], Long> getOrCreateStoreFlushedSequenceId(
    byte[] regionName) {
    ConcurrentNavigableMap<byte[], Long> storeFlushedSequenceId =
        storeFlushedSequenceIdsByRegion.get(regionName);
    if (storeFlushedSequenceId != null) {
      return storeFlushedSequenceId;
    }
    storeFlushedSequenceId = new ConcurrentSkipListMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    ConcurrentNavigableMap<byte[], Long> alreadyPut =
        storeFlushedSequenceIdsByRegion.putIfAbsent(regionName, storeFlushedSequenceId);
    return alreadyPut == null ? storeFlushedSequenceId : alreadyPut;
  }
  /**
   * Updates last flushed sequence Ids for the regions on server sn
   * @param sn
   * @param hsl
   */
  private void updateLastFlushedSequenceIds(ServerName sn, ServerLoad hsl) {
    Map<byte[], RegionLoad> regionsLoad = hsl.getRegionsLoad();
    for (Entry<byte[], RegionLoad> entry : regionsLoad.entrySet()) {
      byte[] encodedRegionName = Bytes.toBytes(HRegionInfo.encodeRegionName(entry.getKey()));
      Long existingValue = flushedSequenceIdByRegion.get(encodedRegionName);
      long l = entry.getValue().getCompleteSequenceId();
      // Don't let smaller sequence ids override greater sequence ids.
      if (existingValue == null || (l != HConstants.NO_SEQNUM && l > existingValue)) {
        flushedSequenceIdByRegion.put(encodedRegionName, l);
      } else if (l != HConstants.NO_SEQNUM && l < existingValue) {
        LOG.warn("RegionServer " + sn + " indicates a last flushed sequence id ("
            + l + ") that is less than the previous last flushed sequence id ("
            + existingValue + ") for region " + Bytes.toString(entry.getKey()) + " Ignoring.");
      }
      ConcurrentNavigableMap<byte[], Long> storeFlushedSequenceId =
          getOrCreateStoreFlushedSequenceId(encodedRegionName);
      for (StoreSequenceId storeSeqId : entry.getValue().getStoreCompleteSequenceId()) {
        byte[] family = storeSeqId.getFamilyName().toByteArray();
        existingValue = storeFlushedSequenceId.get(family);
        l = storeSeqId.getSequenceId();
        // Don't let smaller sequence ids override greater sequence ids.
        if (existingValue == null || (l != HConstants.NO_SEQNUM && l > existingValue.longValue())) {
          storeFlushedSequenceId.put(family, l);
        }
      }
    }
  }

  void regionServerReport(ServerName sn,
      ServerLoad sl) throws YouAreDeadException {
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
   * @param sn the server to check and record
   * @param sl the server load on the server
   * @return true if the server is recorded, otherwise, false
   */
  boolean checkAndRecordNewServer(
      final ServerName serverName, final ServerLoad sl) {
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
    if (existingServer != null && (existingServer.getStartcode() < serverName.getStartcode())) {
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
    if ((this.services == null || ((HMaster) this.services).isInitialized())
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
    for (ServerName sn: this.onlineServers.keySet()) {
      if (ServerName.isSameHostnameAndPort(serverName, sn)) return sn;
    }
    return null;
  }

  /**
   * Adds the onlineServers list. onlineServers should be locked.
   * @param serverName The remote servers name.
   * @param sl
   * @return Server load from the removed server, if any.
   */
  @VisibleForTesting
  void recordNewServerWithLock(final ServerName serverName, final ServerLoad sl) {
    LOG.info("Registering server=" + serverName);
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
            .setFamilyName(ByteString.copyFrom(entry.getKey()))
            .setSequenceId(entry.getValue().longValue()).build());
      }
    }
    return builder.build();
  }

  /**
   * @param serverName
   * @return ServerLoad if serverName is known else null
   */
  public ServerLoad getLoad(final ServerName serverName) {
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
    for (ServerLoad sl: this.onlineServers.values()) {
        numServers++;
        totalLoad += sl.getNumberOfRegions();
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
  public Map<ServerName, ServerLoad> getOnlineServers() {
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
    ZooKeeperWatcher zkw = master.getZooKeeper();
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
        LOG.info("Waiting on regionserver(s) to go down " + sb.toString());
        previousLogTime = System.currentTimeMillis();
      }

      try {
        List<String> servers = ZKUtil.listChildrenNoWatch(zkw, zkw.rsZNode);
        if (servers == null || servers.size() == 0 || (servers.size() == 1
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

  /*
   * Expire the passed server.  Add it to list of dead servers and queue a
   * shutdown processing.
   */
  public synchronized void expireServer(final ServerName serverName) {
    if (serverName.equals(master.getServerName())) {
      if (!(master.isAborted() || master.isStopped())) {
        master.stop("We lost our znode?");
      }
      return;
    }
    if (!services.isServerShutdownHandlerEnabled()) {
      LOG.info("Master doesn't enable ServerShutdownHandler during initialization, "
          + "delay expiring server " + serverName);
      this.queuedDeadServers.add(serverName);
      return;
    }
    if (this.deadservers.isDeadServer(serverName)) {
      // TODO: Can this happen?  It shouldn't be online in this case?
      LOG.warn("Expiration of " + serverName +
          " but server shutdown already in progress");
      return;
    }
    synchronized (onlineServers) {
      if (!this.onlineServers.containsKey(serverName)) {
        LOG.warn("Expiration of " + serverName + " but server not online");
      }
      // Remove the server from the known servers lists and update load info BUT
      // add to deadservers first; do this so it'll show in dead servers list if
      // not in online servers list.
      this.deadservers.add(serverName);
      this.onlineServers.remove(serverName);
      onlineServers.notifyAll();
    }
    this.rsAdmins.remove(serverName);
    // If cluster is going down, yes, servers are going to be expiring; don't
    // process as a dead server
    if (this.clusterShutdown) {
      LOG.info("Cluster shutdown set; " + serverName +
        " expired; onlineServers=" + this.onlineServers.size());
      if (this.onlineServers.isEmpty()) {
        master.stop("Cluster shutdown set; onlineServer=0");
      }
      return;
    }

    boolean carryingMeta = services.getAssignmentManager().isCarryingMeta(serverName) ==
        AssignmentManager.ServerHostRegion.HOSTING_REGION;
    if (carryingMeta) {
      this.services.getExecutorService().submit(new MetaServerShutdownHandler(this.master,
        this.services, this.deadservers, serverName));
    } else {
      this.services.getExecutorService().submit(new ServerShutdownHandler(this.master,
        this.services, this.deadservers, serverName, true));
    }
    LOG.debug("Added=" + serverName +
      " to dead servers, submitted shutdown handler to be executed meta=" + carryingMeta);

    // Tell our listeners that a server was removed
    if (!this.listeners.isEmpty()) {
      for (ServerListener listener : this.listeners) {
        listener.serverRemoved(serverName);
      }
    }
  }

  public synchronized void processDeadServer(final ServerName serverName) {
    this.processDeadServer(serverName, false);
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
    if (!services.getAssignmentManager().isFailoverCleanupDone()) {
      requeuedDeadServers.put(serverName, shouldSplitWal);
      return;
    }

    this.deadservers.add(serverName);
    this.services.getExecutorService().submit(
      new ServerShutdownHandler(this.master, this.services, this.deadservers, serverName,
          shouldSplitWal));
  }

  /**
   * Process the servers which died during master's initialization. It will be
   * called after HMaster#assignMeta and AssignmentManager#joinCluster.
   * */
  synchronized void processQueuedDeadServers() {
    if (!services.isServerShutdownHandlerEnabled()) {
      LOG.info("Master hasn't enabled ServerShutdownHandler");
    }
    Iterator<ServerName> serverIterator = queuedDeadServers.iterator();
    while (serverIterator.hasNext()) {
      ServerName tmpServerName = serverIterator.next();
      expireServer(tmpServerName);
      serverIterator.remove();
      requeuedDeadServers.remove(tmpServerName);
    }

    if (!services.getAssignmentManager().isFailoverCleanupDone()) {
      LOG.info("AssignmentManager hasn't finished failover cleanup; waiting");
    }

    for(ServerName tmpServerName : requeuedDeadServers.keySet()){
      processDeadServer(tmpServerName, requeuedDeadServers.get(tmpServerName));
    }
    requeuedDeadServers.clear();
  }

  /*
   * Remove the server from the drain list.
   */
  public boolean removeServerFromDrainList(final ServerName sn) {
    // Warn if the server (sn) is not online.  ServerName is of the form:
    // <hostname> , <port> , <startcode>

    if (!this.isServerOnline(sn)) {
      LOG.warn("Server " + sn + " is not currently online. " +
               "Removing from draining list anyway, as requested.");
    }
    // Remove the server from the draining servers lists.
    return this.drainingServers.remove(sn);
  }

  /*
   * Add the server to the drain list.
   */
  public boolean addServerToDrainList(final ServerName sn) {
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
      return false;
    }
    return this.drainingServers.add(sn);
  }

  // RPC methods to region servers

  /**
   * Sends an OPEN RPC to the specified server to open the specified region.
   * <p>
   * Open should not fail but can if server just crashed.
   * <p>
   * @param server server to open a region
   * @param region region to open
   * @param versionOfOfflineNode that needs to be present in the offline node
   * when RS tries to change the state from OFFLINE to other states.
   * @param favoredNodes
   */
  public RegionOpeningState sendRegionOpen(final ServerName server,
      HRegionInfo region, int versionOfOfflineNode, List<ServerName> favoredNodes)
  throws IOException {
    AdminService.BlockingInterface admin = getRsAdmin(server);
    if (admin == null) {
      LOG.warn("Attempting to send OPEN RPC to server " + server.toString() +
        " failed because no RPC connection found to this server");
      return RegionOpeningState.FAILED_OPENING;
    }
    OpenRegionRequest request = RequestConverter.buildOpenRegionRequest(server, 
      region, versionOfOfflineNode, favoredNodes, 
      (RecoveryMode.LOG_REPLAY == this.services.getMasterFileSystem().getLogRecoveryMode()));
    try {
      OpenRegionResponse response = admin.openRegion(null, request);
      return ResponseConverter.getRegionOpeningState(response);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Sends an OPEN RPC to the specified server to open the specified region.
   * <p>
   * Open should not fail but can if server just crashed.
   * <p>
   * @param server server to open a region
   * @param regionOpenInfos info of a list of regions to open
   * @return a list of region opening states
   */
  public List<RegionOpeningState> sendRegionOpen(ServerName server,
      List<Triple<HRegionInfo, Integer, List<ServerName>>> regionOpenInfos)
  throws IOException {
    AdminService.BlockingInterface admin = getRsAdmin(server);
    if (admin == null) {
      LOG.warn("Attempting to send OPEN RPC to server " + server.toString() +
        " failed because no RPC connection found to this server");
      return null;
    }

    OpenRegionRequest request = RequestConverter.buildOpenRegionRequest(server, regionOpenInfos,
      (RecoveryMode.LOG_REPLAY == this.services.getMasterFileSystem().getLogRecoveryMode()));
    try {
      OpenRegionResponse response = admin.openRegion(null, request);
      return ResponseConverter.getRegionOpeningStateList(response);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Sends an CLOSE RPC to the specified server to close the specified region.
   * <p>
   * A region server could reject the close request because it either does not
   * have the specified region or the region is being split.
   * @param server server to open a region
   * @param region region to open
   * @param versionOfClosingNode
   *   the version of znode to compare when RS transitions the znode from
   *   CLOSING state.
   * @param dest - if the region is moved to another server, the destination server. null otherwise.
   * @return true if server acknowledged close, false if not
   * @throws IOException
   */
  public boolean sendRegionClose(ServerName server, HRegionInfo region,
    int versionOfClosingNode, ServerName dest, boolean transitionInZK) throws IOException {
    if (server == null) throw new NullPointerException("Passed server is null");
    AdminService.BlockingInterface admin = getRsAdmin(server);
    if (admin == null) {
      throw new IOException("Attempting to send CLOSE RPC to server " +
        server.toString() + " for region " +
        region.getRegionNameAsString() +
        " failed because no RPC connection found to this server");
    }
    return ProtobufUtil.closeRegion(admin, server, region.getRegionName(),
      versionOfClosingNode, dest, transitionInZK);
  }

  public boolean sendRegionClose(ServerName server,
      HRegionInfo region, int versionOfClosingNode) throws IOException {
    return sendRegionClose(server, region, versionOfClosingNode, null, true);
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
      HRegionInfo region) {
    if (server == null) return;
    try {
      AdminService.BlockingInterface admin = getRsAdmin(server);
      ProtobufUtil.warmupRegion(admin, region);
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
    ServerName server, HRegionInfo region, long timeout) throws IOException, InterruptedException {
    AdminService.BlockingInterface rs = connection.getAdmin(server);
    try {
      ProtobufUtil.closeRegion(rs, server, region.getRegionName(), false);
    } catch (IOException e) {
      LOG.warn("Exception when closing region: " + region.getRegionNameAsString(), e);
    }
    long expiration = timeout + System.currentTimeMillis();
    while (System.currentTimeMillis() < expiration) {
      try {
        HRegionInfo rsRegion =
          ProtobufUtil.getRegionInfo(rs, region.getRegionName());
        if (rsRegion == null) return;
      } catch (IOException ioe) {
        if (ioe instanceof NotServingRegionException) // no need to retry again
          return;
        LOG.warn("Exception when retrieving regioninfo from: " + region.getRegionNameAsString(), ioe);
      }
      Thread.sleep(1000);
    }
    throw new IOException("Region " + region + " failed to close within"
        + " timeout " + timeout);
  }

  /**
   * Sends an MERGE REGIONS RPC to the specified server to merge the specified
   * regions.
   * <p>
   * A region server could reject the close request because it either does not
   * have the specified region.
   * @param server server to merge regions
   * @param region_a region to merge
   * @param region_b region to merge
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          two adjacent regions
   * @throws IOException
   */
  public void sendRegionsMerge(ServerName server, HRegionInfo region_a,
      HRegionInfo region_b, boolean forcible) throws IOException {
    if (server == null)
      throw new NullPointerException("Passed server is null");
    if (region_a == null || region_b == null)
      throw new NullPointerException("Passed region is null");
    AdminService.BlockingInterface admin = getRsAdmin(server);
    if (admin == null) {
      throw new IOException("Attempting to send MERGE REGIONS RPC to server "
          + server.toString() + " for region "
          + region_a.getRegionNameAsString() + ","
          + region_b.getRegionNameAsString()
          + " failed because no RPC connection found to this server");
    }
    ProtobufUtil.mergeRegions(admin, region_a, region_b, forcible);
  }

  /**
   * Check if a region server is reachable and has the expected start code
   */
  public boolean isServerReachable(ServerName server) {
    if (server == null) throw new NullPointerException("Passed server is null");

    RetryCounter retryCounter = pingRetryCounterFactory.create();
    while (retryCounter.shouldRetry()) {
      synchronized (this.onlineServers) {
        if (this.deadservers.isDeadServer(server)) {
          return false;
        }
      }
      try {
        AdminService.BlockingInterface admin = getRsAdmin(server);
        if (admin != null) {
          ServerInfo info = ProtobufUtil.getServerInfo(admin);
          return info != null && info.hasServerName()
            && server.getStartcode() == info.getServerName().getStartCode();
        }
      } catch (IOException ioe) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Couldn't reach " + server + ", try=" + retryCounter.getAttemptTimes() + " of "
              + retryCounter.getMaxAttempts(), ioe);
        }
        try {
          retryCounter.sleepUntilNextRetry();
        } catch(InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    return false;
  }

    /**
    * @param sn
    * @return Admin interface for the remote regionserver named <code>sn</code>
    * @throws IOException
    * @throws RetriesExhaustedException wrapping a ConnectException if failed
    */
  private AdminService.BlockingInterface getRsAdmin(final ServerName sn)
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
  public void waitForRegionServers(MonitoredTask status)
  throws InterruptedException {
    final long interval = this.master.getConfiguration().
      getLong(WAIT_ON_REGIONSERVERS_INTERVAL, 1500);
    final long timeout = this.master.getConfiguration().
      getLong(WAIT_ON_REGIONSERVERS_TIMEOUT, 4500);
    int defaultMinToStart = 1;
    if (BaseLoadBalancer.tablesOnMaster(master.getConfiguration())) {
      // If we assign regions to master, we'd like to start
      // at least another region server so that we don't
      // assign all regions to master if other region servers
      // don't come up in time.
      defaultMinToStart = 2;
    }
    int minToStart = this.master.getConfiguration().
      getInt(WAIT_ON_REGIONSERVERS_MINTOSTART, defaultMinToStart);
    if (minToStart < 1) {
      LOG.warn(String.format(
        "The value of '%s' (%d) can not be less than 1, ignoring.",
        WAIT_ON_REGIONSERVERS_MINTOSTART, minToStart));
      minToStart = 1;
    }
    int maxToStart = this.master.getConfiguration().
      getInt(WAIT_ON_REGIONSERVERS_MAXTOSTART, Integer.MAX_VALUE);
    if (maxToStart < minToStart) {
        LOG.warn(String.format(
            "The value of '%s' (%d) is set less than '%s' (%d), ignoring.",
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
    while (!this.master.isStopped() && count < maxToStart
        && (lastCountChange+interval > now || timeout > slept || count < minToStart)) {
      // Log some info at every interval time or if there is a change
      if (oldCount != count || lastLogTime+interval < now){
        lastLogTime = now;
        String msg =
          "Waiting for region servers count to settle; currently"+
            " checked in " + count + ", slept for " + slept + " ms," +
            " expecting minimum of " + minToStart + ", maximum of "+ maxToStart+
            ", timeout of "+timeout+" ms, interval of "+interval+" ms.";
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

    LOG.info("Finished waiting for region servers count to settle;" +
      " checked in " + count + ", slept for " + slept + " ms," +
      " expecting minimum of " + minToStart + ", maximum of "+ maxToStart+","+
      " master is "+ (this.master.isStopped() ? "stopped.": "running")
    );
  }

  /**
   * @return A copy of the internal list of online servers.
   */
  public List<ServerName> getOnlineServersList() {
    // TODO: optimize the load balancer call so we don't need to make a new list
    // TODO: FIX. THIS IS POPULAR CALL.
    return new ArrayList<ServerName>(this.onlineServers.keySet());
  }

  /**
   * @return A copy of the internal list of draining servers.
   */
  public List<ServerName> getDrainingServersList() {
    return new ArrayList<ServerName>(this.drainingServers);
  }

  /**
   * @return A copy of the internal set of deadNotExpired servers.
   */
  Set<ServerName> getDeadNotExpiredServers() {
    return new HashSet<ServerName>(this.queuedDeadServers);
  }

  /**
   * During startup, if we figure it is not a failover, i.e. there is
   * no more WAL files to split, we won't try to recover these dead servers.
   * So we just remove them from the queue. Use caution in calling this.
   */
  void removeRequeuedDeadServers() {
    requeuedDeadServers.clear();
  }

  /**
   * @return A copy of the internal map of requeuedDeadServers servers and their corresponding
   *         splitlog need flag.
   */
  Map<ServerName, Boolean> getRequeuedDeadServers() {
    return Collections.unmodifiableMap(this.requeuedDeadServers);
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
    this.clusterShutdown = true;
    this.master.stop("Cluster shutdown requested");
  }

  public boolean isClusterShutdown() {
    return this.clusterShutdown;
  }

  /**
   * Stop the ServerManager.  Currently closes the connection to the master.
   */
  public void stop() {
    if (connection != null) {
      try {
        connection.close();
      } catch (IOException e) {
        LOG.error("Attempt to close connection to master failed", e);
      }
    }
  }

  /**
   * Creates a list of possible destinations for a region. It contains the online servers, but not
   *  the draining or dying servers.
   *  @param serverToExclude can be null if there is no server to exclude
   */
  public List<ServerName> createDestinationServersList(final ServerName serverToExclude){
    final List<ServerName> destServers = getOnlineServersList();

    if (serverToExclude != null){
      destServers.remove(serverToExclude);
    }

    // Loop through the draining server list and remove them from the server list
    final List<ServerName> drainingServersCopy = getDrainingServersList();
    if (!drainingServersCopy.isEmpty()) {
      for (final ServerName server: drainingServersCopy) {
        destServers.remove(server);
      }
    }

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
}
