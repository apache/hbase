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

import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.FlushedRegionSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.FlushedSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.FlushedStoreSequenceId;
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

  /**
   * see HBASE-20727
   * if set to true, flushedSequenceIdByRegion and storeFlushedSequenceIdsByRegion
   * will be persisted to HDFS and loaded when master restart to speed up log split
   */
  public static final String PERSIST_FLUSHEDSEQUENCEID =
      "hbase.master.persist.flushedsequenceid.enabled";

  public static final boolean PERSIST_FLUSHEDSEQUENCEID_DEFAULT = true;

  public static final String FLUSHEDSEQUENCEID_FLUSHER_INTERVAL =
      "hbase.master.flushedsequenceid.flusher.interval";

  public static final int FLUSHEDSEQUENCEID_FLUSHER_INTERVAL_DEFAULT =
      3 * 60 * 60 * 1000; // 3 hours

  public static final String MAX_CLOCK_SKEW_MS = "hbase.master.maxclockskew";

  private static final Logger LOG = LoggerFactory.getLogger(ServerManager.class);

  // Set if we are to shutdown the cluster.
  private AtomicBoolean clusterShutdown = new AtomicBoolean(false);

  /**
   * The last flushed sequence id for a region.
   */
  private final ConcurrentNavigableMap<byte[], Long> flushedSequenceIdByRegion =
    new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  private boolean persistFlushedSequenceId = true;
  private volatile boolean isFlushSeqIdPersistInProgress = false;
  /** File on hdfs to store last flushed sequence id of regions */
  private static final String LAST_FLUSHED_SEQ_ID_FILE = ".lastflushedseqids";
  private  FlushedSequenceIdFlusher flushedSeqIdFlusher;


  /**
   * The last flushed sequence id for a store in a region.
   */
  private final ConcurrentNavigableMap<byte[], ConcurrentNavigableMap<byte[], Long>>
    storeFlushedSequenceIdsByRegion = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  /** Map of registered servers to their current load */
  private final ConcurrentNavigableMap<ServerName, ServerMetrics> onlineServers =
    new ConcurrentSkipListMap<>();

  /** List of region servers that should not get any more new regions. */
  private final ArrayList<ServerName> drainingServers = new ArrayList<>();

  private final MasterServices master;
  private final RegionServerList storage;

  private final DeadServer deadservers = new DeadServer();

  private final long maxSkew;
  private final long warningSkew;

  /** Listeners that are called on server events. */
  private List<ServerListener> listeners = new CopyOnWriteArrayList<>();

  /**
   * Constructor.
   */
  public ServerManager(final MasterServices master, RegionServerList storage) {
    this.master = master;
    this.storage = storage;
    Configuration c = master.getConfiguration();
    maxSkew = c.getLong(MAX_CLOCK_SKEW_MS, 30000);
    warningSkew = c.getLong("hbase.master.warningclockskew", 10000);
    persistFlushedSequenceId =
      c.getBoolean(PERSIST_FLUSHEDSEQUENCEID, PERSIST_FLUSHEDSEQUENCEID_DEFAULT);
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
   * @param versionNumber the version number of the new regionserver
   * @param version the version of the new regionserver, could contain strings like "SNAPSHOT"
   * @param ia the InetAddress from which request is received
   * @return The ServerName we know this server as.
   */
  ServerName regionServerStartup(RegionServerStartupRequest request, int versionNumber,
      String version, InetAddress ia) throws IOException {
    // Test for case where we get a region startup message from a regionserver
    // that has been quickly restarted but whose znode expiration handler has
    // not yet run, or from a server whose fail we are currently processing.
    // Test its host+port combo is present in serverAddressToServerInfo. If it
    // is, reject the server and trigger its expiration. The next time it comes
    // in, it should have been removed from serverAddressToServerInfo and queued
    // for processing by ProcessServerShutdown.

    final String hostname =
      request.hasUseThisHostnameInstead() ? request.getUseThisHostnameInstead() : ia.getHostName();
    ServerName sn = ServerName.valueOf(hostname, request.getPort(), request.getServerStartCode());
    checkClockSkew(sn, request.getServerCurrentTime());
    checkIsDead(sn, "STARTUP");
    if (!checkAndRecordNewServer(sn, ServerMetricsBuilder.of(sn, versionNumber, version))) {
      LOG.warn(
        "THIS SHOULD NOT HAPPEN, RegionServerStartup" + " could not record the server: " + sn);
    }
    storage.started(sn);
    return sn;
  }

  /**
   * Updates last flushed sequence Ids for the regions on server sn
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
   * Find out the region servers crashed between the crash of the previous master instance and the
   * current master instance and schedule SCP for them.
   * <p/>
   * Since the {@code RegionServerTracker} has already helped us to construct the online servers set
   * by scanning zookeeper, now we can compare the online servers with {@code liveServersFromWALDir}
   * to find out whether there are servers which are already dead.
   * <p/>
   * Must be called inside the initialization method of {@code RegionServerTracker} to avoid
   * concurrency issue.
   * @param deadServersFromPE the region servers which already have a SCP associated.
   * @param liveServersFromWALDir the live region servers from wal directory.
   */
  void findDeadServersAndProcess(Set<ServerName> deadServersFromPE,
      Set<ServerName> liveServersFromWALDir) {
    deadServersFromPE.forEach(deadservers::putIfAbsent);
    liveServersFromWALDir.stream().filter(sn -> !onlineServers.containsKey(sn))
      .forEach(this::expireServer);
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
    long skew = Math.abs(EnvironmentEdgeManager.currentTime() - serverCurrentTime);
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
   * Called when RegionServer first reports in for duty and thereafter each
   * time it heartbeats to make sure it is has not been figured for dead.
   * If this server is on the dead list, reject it with a YouAreDeadException.
   * If it was dead but came back with a new start code, remove the old entry
   * from the dead list.
   * @param what START or REPORT
   */
  private void checkIsDead(final ServerName serverName, final String what)
      throws YouAreDeadException {
    if (this.deadservers.isDeadServer(serverName)) {
      // Exact match: host name, port and start code all match with existing one of the
      // dead servers. So, this server must be dead. Tell it to kill itself.
      String message = "Server " + what + " rejected; currently processing " +
          serverName + " as dead server";
      LOG.debug(message);
      throw new YouAreDeadException(message);
    }
    // Remove dead server with same hostname and port of newly checking in rs after master
    // initialization. See HBASE-5916 for more information.
    if ((this.master == null || this.master.isInitialized()) &&
        this.deadservers.cleanPreviousInstance(serverName)) {
      // This server has now become alive after we marked it as dead.
      // We removed it's previous entry from the dead list to reflect it.
      LOG.debug("{} {} came back up, removed it from the dead servers list", what, serverName);
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
  void recordNewServerWithLock(final ServerName serverName, final ServerMetrics sl) {
    LOG.info("Registering regionserver=" + serverName);
    this.onlineServers.put(serverName, sl);
  }

  public ConcurrentNavigableMap<byte[], Long> getFlushedSequenceIdByRegion() {
    return flushedSequenceIdByRegion;
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
  public boolean areDeadServersInProgress() throws IOException {
    return master.getProcedures().stream()
      .anyMatch(p -> !p.isFinished() && p instanceof ServerCrashProcedure);
  }

  void letRegionServersShutdown() {
    long previousLogTime = 0;
    ServerName sn = master.getServerName();
    ZKWatcher zkw = master.getZooKeeper();
    int onlineServersCt;
    while ((onlineServersCt = onlineServers.size()) > 0){
      if (EnvironmentEdgeManager.currentTime() > (previousLogTime + 1000)) {
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
        previousLogTime = EnvironmentEdgeManager.currentTime();
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

  /**
   * Expire the passed server. Add it to list of dead servers and queue a shutdown processing.
   * @return pid if we queued a ServerCrashProcedure else {@link Procedure#NO_PROC_ID} if we did
   *         not (could happen for many reasons including the fact that its this server that is
   *         going down or we already have queued an SCP for this server or SCP processing is
   *         currently disabled because we are in startup phase).
   */
  // Redo test so we can make this protected.
  public synchronized long expireServer(final ServerName serverName) {
    return expireServer(serverName, false);

  }

  synchronized long expireServer(final ServerName serverName, boolean force) {
    // THIS server is going down... can't handle our own expiration.
    if (serverName.equals(master.getServerName())) {
      if (!(master.isAborted() || master.isStopped())) {
        master.stop("We lost our znode?");
      }
      return Procedure.NO_PROC_ID;
    }
    if (this.deadservers.isDeadServer(serverName)) {
      LOG.warn("Expiration called on {} but already in DeadServer", serverName);
      return Procedure.NO_PROC_ID;
    }
    moveFromOnlineToDeadServers(serverName);

    // If server is in draining mode, remove corresponding znode
    // In some tests, the mocked HM may not have ZK Instance, hence null check
    if (master.getZooKeeper() != null) {
      String drainingZnode = ZNodePaths
        .joinZNode(master.getZooKeeper().getZNodePaths().drainingZNode, serverName.getServerName());
      try {
        ZKUtil.deleteNodeFailSilent(master.getZooKeeper(), drainingZnode);
      } catch (KeeperException e) {
        LOG.warn("Error deleting the draining znode for stopping server " + serverName.getServerName(), e);
      }
    }
    
    // If cluster is going down, yes, servers are going to be expiring; don't
    // process as a dead server
    if (isClusterShutdown()) {
      LOG.info("Cluster shutdown set; " + serverName +
        " expired; onlineServers=" + this.onlineServers.size());
      if (this.onlineServers.isEmpty()) {
        master.stop("Cluster shutdown set; onlineServer=0");
      }
      return Procedure.NO_PROC_ID;
    }
    LOG.info("Processing expiration of " + serverName + " on " + this.master.getServerName());
    long pid = master.getAssignmentManager().submitServerCrash(serverName, true, force);
    storage.expired(serverName);
    // Tell our listeners that a server was removed
    if (!this.listeners.isEmpty()) {
      this.listeners.stream().forEach(l -> l.serverRemoved(serverName));
    }
    // trigger a persist of flushedSeqId
    if (flushedSeqIdFlusher != null) {
      flushedSeqIdFlusher.triggerNow();
    }
    return pid;
  }

  /**
   * Called when server has expired.
   */
  // Locking in this class needs cleanup.
  public synchronized void moveFromOnlineToDeadServers(final ServerName sn) {
    synchronized (this.onlineServers) {
      boolean online = this.onlineServers.containsKey(sn);
      if (online) {
        // Remove the server from the known servers lists and update load info BUT
        // add to deadservers first; do this so it'll show in dead servers list if
        // not in online servers list.
        this.deadservers.putIfAbsent(sn);
        this.onlineServers.remove(sn);
        onlineServers.notifyAll();
      } else {
        // If not online, that is odd but may happen if 'Unknown Servers' -- where meta
        // has references to servers not online nor in dead servers list. If
        // 'Unknown Server', don't add to DeadServers else will be there for ever.
        LOG.trace("Expiration of {} but server not online", sn);
      }
    }
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

  /**
   * Contacts a region server and waits up to timeout ms
   * to close the region.  This bypasses the active hmaster.
   * Pass -1 as timeout if you do not want to wait on result.
   */
  public static void closeRegionSilentlyAndWait(AsyncClusterConnection connection,
      ServerName server, RegionInfo region, long timeout) throws IOException, InterruptedException {
    AsyncRegionServerAdmin admin = connection.getRegionServerAdmin(server);
    try {
      FutureUtils.get(
        admin.closeRegion(ProtobufUtil.buildCloseRegionRequest(server, region.getRegionName())));
    } catch (IOException e) {
      LOG.warn("Exception when closing region: " + region.getRegionNameAsString(), e);
    }
    if (timeout < 0) {
      return;
    }
    long expiration = timeout + EnvironmentEdgeManager.currentTime();
    while (EnvironmentEdgeManager.currentTime() < expiration) {
      try {
        RegionInfo rsRegion = ProtobufUtil.toRegionInfo(FutureUtils
          .get(
            admin.getRegionInfo(RequestConverter.buildGetRegionInfoRequest(region.getRegionName())))
          .getRegionInfo());
        if (rsRegion == null) {
          return;
        }
      } catch (IOException ioe) {
        if (ioe instanceof NotServingRegionException ||
          (ioe instanceof RemoteWithExtrasException &&
            ((RemoteWithExtrasException)ioe).unwrapRemoteException()
              instanceof NotServingRegionException)) {
          // no need to retry again
          return;
        }
        LOG.warn("Exception when retrieving regioninfo from: " + region.getRegionNameAsString(),
          ioe);
      }
      Thread.sleep(1000);
    }
    throw new IOException("Region " + region + " failed to close within" + " timeout " + timeout);
  }

  /**
   * Calculate min necessary to start. This is not an absolute. It is just
   * a friction that will cause us hang around a bit longer waiting on
   * RegionServers to check-in.
   */
  private int getMinToStart() {
    if (master.isInMaintenanceMode()) {
      // If in maintenance mode, then in process region server hosting meta will be the only server
      // available
      return 1;
    }

    int minimumRequired = 1;
    int minToStart = this.master.getConfiguration().getInt(WAIT_ON_REGIONSERVERS_MINTOSTART, -1);
    // Ensure we are never less than minimumRequired else stuff won't work.
    return Math.max(minToStart, minimumRequired);
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

    long now = EnvironmentEdgeManager.currentTime();
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
                slept + "ms, expecting min=" + minToStart + " server(s), max="
                + getStrForMax(maxToStart) + " server(s), " + "timeout=" + timeout
                + "ms, lastChange=" + (now - lastCountChange) + "ms";
        LOG.info(msg);
        status.setStatus(msg);
      }

      // We sleep for some time
      final long sleepTime = 50;
      Thread.sleep(sleepTime);
      now = EnvironmentEdgeManager.currentTime();
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

  public boolean isServerOnline(ServerName serverName) {
    return serverName != null && onlineServers.containsKey(serverName);
  }

  public enum ServerLiveState {
    LIVE,
    DEAD,
    UNKNOWN
  }

  /**
   * @return whether the server is online, dead, or unknown.
   */
  public synchronized ServerLiveState isServerKnownAndOnline(ServerName serverName) {
    return onlineServers.containsKey(serverName) ? ServerLiveState.LIVE
      : (deadservers.isDeadServer(serverName) ? ServerLiveState.DEAD : ServerLiveState.UNKNOWN);
  }

  /**
   * Check if a server is known to be dead.  A server can be online,
   * or known to be dead, or unknown to this manager (i.e, not online,
   * not known to be dead either; it is simply not tracked by the
   * master any more, for example, a very old previous instance).
   */
  public synchronized boolean isServerDead(ServerName serverName) {
    return serverName == null || deadservers.isDeadServer(serverName);
  }

  /**
   * Check if a server is unknown.  A server can be online,
   * or known to be dead, or unknown to this manager (i.e, not online,
   * not known to be dead either; it is simply not tracked by the
   * master any more, for example, a very old previous instance).
   */
  public boolean isServerUnknown(ServerName serverName) {
    return serverName == null
      || (!onlineServers.containsKey(serverName) && !deadservers.isDeadServer(serverName));
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

  public boolean isClusterShutdown() {
    return this.clusterShutdown.get();
  }

  /**
   * start chore in ServerManager
   */
  public void startChore() {
    Configuration c = master.getConfiguration();
    if (persistFlushedSequenceId) {
      new Thread(() -> {
        // after AM#loadMeta, RegionStates should be loaded, and some regions are
        // deleted by drop/split/merge during removeDeletedRegionFromLoadedFlushedSequenceIds,
        // but these deleted regions are not added back to RegionStates,
        // so we can safely remove deleted regions.
        removeDeletedRegionFromLoadedFlushedSequenceIds();
      }, "RemoveDeletedRegionSyncThread").start();
      int flushPeriod = c.getInt(FLUSHEDSEQUENCEID_FLUSHER_INTERVAL,
          FLUSHEDSEQUENCEID_FLUSHER_INTERVAL_DEFAULT);
      flushedSeqIdFlusher = new FlushedSequenceIdFlusher(
          "FlushedSequenceIdFlusher", flushPeriod);
      master.getChoreService().scheduleChore(flushedSeqIdFlusher);
    }
  }

  /**
   * Stop the ServerManager.
   */
  public void stop() {
    if (flushedSeqIdFlusher != null) {
      flushedSeqIdFlusher.shutdown();
    }
    if (persistFlushedSequenceId) {
      try {
        persistRegionLastFlushedSequenceIds();
      } catch (IOException e) {
        LOG.warn("Failed to persist last flushed sequence id of regions"
            + " to file system", e);
      }
    }
  }

  /**
   * Creates a list of possible destinations for a region. It contains the online servers, but not
   * the draining or dying servers.
   * @param serversToExclude can be null if there is no server to exclude
   */
  public List<ServerName> createDestinationServersList(final List<ServerName> serversToExclude) {
    Set<ServerName> destServers = new HashSet<>();
    onlineServers.forEach((sn, sm) -> {
      if (sm.getLastReportTimestamp() > 0) {
        // This means we have already called regionServerReport at leaset once, then let's include
        // this server for region assignment. This is an optimization to avoid assigning regions to
        // an uninitialized server. See HBASE-25032 for more details.
        destServers.add(sn);
      }
    });

    if (serversToExclude != null) {
      destServers.removeAll(serversToExclude);
    }

    // Loop through the draining server list and remove them from the server list
    final List<ServerName> drainingServersCopy = getDrainingServersList();
    destServers.removeAll(drainingServersCopy);

    return new ArrayList<>(destServers);
  }

  /**
   * Calls {@link #createDestinationServersList} without server to exclude.
   */
  public List<ServerName> createDestinationServersList(){
    return createDestinationServersList(null);
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

  /**
   * May return 0 when server is not online.
   */
  public int getVersionNumber(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.get(serverName);
    return serverMetrics != null ? serverMetrics.getVersionNumber() : 0;
  }

  /**
   * May return "0.0.0" when server is not online
   */
  public String getVersion(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.get(serverName);
    return serverMetrics != null ? serverMetrics.getVersion() : "0.0.0";
  }

  public int getInfoPort(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.get(serverName);
    return serverMetrics != null ? serverMetrics.getInfoServerPort() : 0;
  }

  /**
   * Persist last flushed sequence id of each region to HDFS
   * @throws IOException if persit to HDFS fails
   */
  private void persistRegionLastFlushedSequenceIds() throws IOException {
    if (isFlushSeqIdPersistInProgress) {
      return;
    }
    isFlushSeqIdPersistInProgress = true;
    try {
      Configuration conf = master.getConfiguration();
      Path rootDir = CommonFSUtils.getRootDir(conf);
      Path lastFlushedSeqIdPath = new Path(rootDir, LAST_FLUSHED_SEQ_ID_FILE);
      FileSystem fs = FileSystem.get(conf);
      if (fs.exists(lastFlushedSeqIdPath)) {
        LOG.info("Rewriting .lastflushedseqids file at: "
            + lastFlushedSeqIdPath);
        if (!fs.delete(lastFlushedSeqIdPath, false)) {
          throw new IOException("Unable to remove existing "
              + lastFlushedSeqIdPath);
        }
      } else {
        LOG.info("Writing .lastflushedseqids file at: " + lastFlushedSeqIdPath);
      }
      FSDataOutputStream out = fs.create(lastFlushedSeqIdPath);
      FlushedSequenceId.Builder flushedSequenceIdBuilder =
          FlushedSequenceId.newBuilder();
      try {
        for (Entry<byte[], Long> entry : flushedSequenceIdByRegion.entrySet()) {
          FlushedRegionSequenceId.Builder flushedRegionSequenceIdBuilder =
              FlushedRegionSequenceId.newBuilder();
          flushedRegionSequenceIdBuilder.setRegionEncodedName(
              ByteString.copyFrom(entry.getKey()));
          flushedRegionSequenceIdBuilder.setSeqId(entry.getValue());
          ConcurrentNavigableMap<byte[], Long> storeSeqIds =
              storeFlushedSequenceIdsByRegion.get(entry.getKey());
          if (storeSeqIds != null) {
            for (Entry<byte[], Long> store : storeSeqIds.entrySet()) {
              FlushedStoreSequenceId.Builder flushedStoreSequenceIdBuilder =
                  FlushedStoreSequenceId.newBuilder();
              flushedStoreSequenceIdBuilder.setFamily(ByteString.copyFrom(store.getKey()));
              flushedStoreSequenceIdBuilder.setSeqId(store.getValue());
              flushedRegionSequenceIdBuilder.addStores(flushedStoreSequenceIdBuilder);
            }
          }
          flushedSequenceIdBuilder.addRegionSequenceId(flushedRegionSequenceIdBuilder);
        }
        flushedSequenceIdBuilder.build().writeDelimitedTo(out);
      } finally {
        if (out != null) {
          out.close();
        }
      }
    } finally {
      isFlushSeqIdPersistInProgress = false;
    }
  }

  /**
   * Load last flushed sequence id of each region from HDFS, if persisted
   */
  public void loadLastFlushedSequenceIds() throws IOException {
    if (!persistFlushedSequenceId) {
      return;
    }
    Configuration conf = master.getConfiguration();
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path lastFlushedSeqIdPath = new Path(rootDir, LAST_FLUSHED_SEQ_ID_FILE);
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(lastFlushedSeqIdPath)) {
      LOG.info("No .lastflushedseqids found at" + lastFlushedSeqIdPath
          + " will record last flushed sequence id"
          + " for regions by regionserver report all over again");
      return;
    } else {
      LOG.info("begin to load .lastflushedseqids at " + lastFlushedSeqIdPath);
    }
    FSDataInputStream in = fs.open(lastFlushedSeqIdPath);
    try {
      FlushedSequenceId flushedSequenceId =
          FlushedSequenceId.parseDelimitedFrom(in);
      if (flushedSequenceId == null) {
        LOG.info(".lastflushedseqids found at {} is empty", lastFlushedSeqIdPath);
        return;
      }
      for (FlushedRegionSequenceId flushedRegionSequenceId : flushedSequenceId
          .getRegionSequenceIdList()) {
        byte[] encodedRegionName = flushedRegionSequenceId
            .getRegionEncodedName().toByteArray();
        flushedSequenceIdByRegion
            .putIfAbsent(encodedRegionName, flushedRegionSequenceId.getSeqId());
        if (flushedRegionSequenceId.getStoresList() != null
            && flushedRegionSequenceId.getStoresList().size() != 0) {
          ConcurrentNavigableMap<byte[], Long> storeFlushedSequenceId =
              computeIfAbsent(storeFlushedSequenceIdsByRegion, encodedRegionName,
                () -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
          for (FlushedStoreSequenceId flushedStoreSequenceId : flushedRegionSequenceId
              .getStoresList()) {
            storeFlushedSequenceId
                .put(flushedStoreSequenceId.getFamily().toByteArray(),
                    flushedStoreSequenceId.getSeqId());
          }
        }
      }
    } finally {
      in.close();
    }
  }

  /**
   * Regions may have been removed between latest persist of FlushedSequenceIds
   * and master abort. So after loading FlushedSequenceIds from file, and after
   * meta loaded, we need to remove the deleted region according to RegionStates.
   */
  public void removeDeletedRegionFromLoadedFlushedSequenceIds() {
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    Iterator<byte[]> it = flushedSequenceIdByRegion.keySet().iterator();
    while(it.hasNext()) {
      byte[] regionEncodedName = it.next();
      if (regionStates.getRegionState(Bytes.toStringBinary(regionEncodedName)) == null) {
        it.remove();
        storeFlushedSequenceIdsByRegion.remove(regionEncodedName);
      }
    }
  }

  private class FlushedSequenceIdFlusher extends ScheduledChore {

    public FlushedSequenceIdFlusher(String name, int p) {
      super(name, master, p, 60 * 1000); //delay one minute before first execute
    }

    @Override
    protected void chore() {
      try {
        persistRegionLastFlushedSequenceIds();
      } catch (IOException e) {
        LOG.debug("Failed to persist last flushed sequence id of regions"
            + " to file system", e);
      }
    }
  }
}
