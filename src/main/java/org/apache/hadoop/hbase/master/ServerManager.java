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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.regionserver.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.util.Threads;

/**
 * The ServerManager class manages info about region servers - HServerInfo,
 * load numbers, dying servers, etc.
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
 */
public class ServerManager {
  private static final Log LOG = LogFactory.getLog(ServerManager.class);

  private final AtomicInteger quiescedServers = new AtomicInteger(0);
  private final AtomicInteger availableServers = new AtomicInteger(0);
  // Set if we are to shutdown the cluster.
  private volatile boolean clusterShutdown = false;

  /** The map of known server names to server info */
  private final Map<String, HServerInfo> onlineServers =
    new ConcurrentHashMap<String, HServerInfo>();

  // TODO: This is strange to have two maps but HSI above is used on both sides
  /**
   * Map from full server-instance name to the RPC connection for this server.
   */
  private final Map<String, HRegionInterface> serverConnections =
    new HashMap<String, HRegionInterface>();

  /**
   * Set of known dead servers.  On znode expiration, servers are added here.
   * This is needed in case of a network partitioning where the server's lease
   * expires, but the server is still running. After the network is healed,
   * and it's server logs are recovered, it will be told to call server startup
   * because by then, its regions have probably been reassigned.
   */
  private final Set<String> deadServers =
    Collections.synchronizedSet(new HashSet<String>());

  private Server master;

  private MasterMetrics masterMetrics;

  private final ServerMonitor serverMonitorThread;

  private int minimumServerCount;

  private final OldLogsCleaner oldLogCleaner;

  /**
   * Dumps into log current stats on dead servers and number of servers
   * TODO: Make this a metric; dump metrics into log.
   */
  class ServerMonitor extends Chore {
    ServerMonitor(final int period, final Stoppable stopper) {
      super("ServerMonitor", period, stopper);
    }

    @Override
    protected void chore() {
      int numServers = availableServers.get();
      int numDeadServers = deadServers.size();
      double averageLoad = getAverageLoad();
      String deadServersList = null;
      if (numDeadServers > 0) {
        StringBuilder sb = new StringBuilder("Dead Server [");
        boolean first = true;
        synchronized (deadServers) {
          for (String server: deadServers) {
            if (!first) {
              sb.append(",  ");
              first = false;
            }
            sb.append(server);
          }
        }
        sb.append("]");
        deadServersList = sb.toString();
      }
      LOG.info(numServers + " region servers, " + numDeadServers +
        " dead, average load " + averageLoad +
        (deadServersList != null? deadServers: ""));
    }
  }

  /**
   * Constructor.
   * @param master
   * @param masterMetrics If null, we won't pass metrics.
   * @param masterFileSystem
   */
  public ServerManager(Server master,
      MasterMetrics masterMetrics,
      MasterFileSystem masterFileSystem) {
    this.master = master;
    this.masterMetrics = masterMetrics;
    Configuration c = master.getConfiguration();
    int metaRescanInterval = c.getInt("hbase.master.meta.thread.rescanfrequency",
      60 * 1000);
    this.minimumServerCount = c.getInt("hbase.regions.server.count.min", 1);
    this.serverMonitorThread = new ServerMonitor(metaRescanInterval, master);
    String n = Thread.currentThread().getName();
    Threads.setDaemonThreadRunning(this.serverMonitorThread,
      n + ".serverMonitor");
    this.oldLogCleaner = new OldLogsCleaner(
      c.getInt("hbase.master.meta.thread.rescanfrequency",60 * 1000),
      master, c, masterFileSystem.getFileSystem(),
      masterFileSystem.getOldLogDir());
    Threads.setDaemonThreadRunning(oldLogCleaner,
      n + ".oldLogCleaner");
  }

  /**
   * Let the server manager know a new regionserver has come online
   * @param serverInfo
   * @throws IOException
   */
  void regionServerStartup(final HServerInfo serverInfo)
  throws IOException {
    // Test for case where we get a region startup message from a regionserver
    // that has been quickly restarted but whose znode expiration handler has
    // not yet run, or from a server whose fail we are currently processing.
    // Test its host+port combo is present in serverAddresstoServerInfo.  If it
    // is, reject the server and trigger its expiration. The next time it comes
    // in, it should have been removed from serverAddressToServerInfo and queued
    // for processing by ProcessServerShutdown.
    HServerInfo info = new HServerInfo(serverInfo);
    String hostAndPort = info.getServerAddress().toString();
    HServerInfo existingServer = haveServerWithSameHostAndPortAlready(info.getHostnamePort());
    if (existingServer != null) {
      String message = "Server start rejected; we already have " + hostAndPort +
        " registered; existingServer=" + existingServer + ", newServer=" + info;
      LOG.info(message);
      if (existingServer.getStartCode() < info.getStartCode()) {
        LOG.info("Triggering server recovery; existingServer looks stale");
        expireServer(existingServer);
      }
      throw new PleaseHoldException(message);
    }
    checkIsDead(info.getServerName(), "STARTUP");
    LOG.info("Received start message from: " + info.getServerName());
    recordNewServer(info);
  }

  private HServerInfo haveServerWithSameHostAndPortAlready(final String hostnamePort) {
    synchronized (this.onlineServers) {
      for (Map.Entry<String, HServerInfo> e: this.onlineServers.entrySet()) {
        if (e.getValue().getHostnamePort().equals(hostnamePort)) {
          return e.getValue();
        }
      }
    }
    return null;
  }

  /**
   * If this server is on the dead list, reject it with a LeaseStillHeldException
   * @param serverName Server name formatted as host_port_startcode.
   * @param what START or REPORT
   * @throws LeaseStillHeldException
   */
  private void checkIsDead(final String serverName, final String what)
  throws YouAreDeadException {
    if (!isDead(serverName)) {
      return;
    }
    String message = "Server " + what + " rejected; currently processing " +
      serverName + " as dead server";
    LOG.debug(message);
    throw new YouAreDeadException(message);
  }

  /**
   * Adds the HSI to the RS list and creates an empty load
   * @param info The region server informations
   */
  public void recordNewServer(HServerInfo info) {
    recordNewServer(info, false, null);
  }

  /**
   * Adds the HSI to the RS list
   * @param info The region server informations
   * @param useInfoLoad True if the load from the info should be used
   *                    like under a master failover
   */
  void recordNewServer(HServerInfo info, boolean useInfoLoad,
      HRegionInterface hri) {
    HServerLoad load = useInfoLoad ? info.getLoad() : new HServerLoad();
    String serverName = info.getServerName();
    info.setLoad(load);
    // TODO: Why did we update the RS location ourself?  Shouldn't RS do this?
//    masterStatus.getZooKeeper().updateRSLocationGetWatch(info, watcher);
    onlineServers.put(serverName, info);
    availableServers.incrementAndGet();
    if(hri == null) {
      serverConnections.remove(serverName);
    } else {
      serverConnections.put(serverName, hri);
    }
  }

  /**
   * Called to process the messages sent from the region server to the master
   * along with the heart beat.
   *
   * @param serverInfo
   * @param msgs
   * @param mostLoadedRegions Array of regions the region server is submitting
   * as candidates to be rebalanced, should it be overloaded
   * @return messages from master to region server indicating what region
   * server should do.
   *
   * @throws IOException
   */
  HMsg [] regionServerReport(final HServerInfo serverInfo,
    final HMsg msgs[], final HRegionInfo[] mostLoadedRegions)
  throws IOException {
    HServerInfo info = new HServerInfo(serverInfo);
    checkIsDead(info.getServerName(), "REPORT");
    if (msgs.length > 0) {
      if (msgs[0].isType(HMsg.Type.MSG_REPORT_EXITING)) {
        processRegionServerExit(info, msgs);
        return HMsg.EMPTY_HMSG_ARRAY;
      } else if (msgs[0].isType(HMsg.Type.MSG_REPORT_QUIESCED)) {
        LOG.info("Region server " + info.getServerName() + " quiesced");
        this.quiescedServers.incrementAndGet();
      }
    }
    if (this.clusterShutdown) {
      if (quiescedServers.get() >= availableServers.get()) {
        // If the only servers we know about are meta servers, then we can
        // proceed with shutdown
        this.master.stop("All user tables quiesced. Proceeding with shutdown");
        notifyOnlineServers();
      } else if (!this.master.isStopped()) {
        if (msgs.length > 0 &&
            msgs[0].isType(HMsg.Type.MSG_REPORT_QUIESCED)) {
          // Server is already quiesced, but we aren't ready to shut down
          // return empty response
          return HMsg.EMPTY_HMSG_ARRAY;
        }
        // Tell the server to stop serving any user regions
        return new HMsg [] {HMsg.REGIONSERVER_QUIESCE};
      }
    }
    if (this.master.isStopped()) {
      // Tell server to shut down if we are shutting down.  This should
      // happen after check of MSG_REPORT_EXITING above, since region server
      // will send us one of these messages after it gets MSG_REGIONSERVER_STOP
      return new HMsg [] {HMsg.REGIONSERVER_STOP};
    }

    HServerInfo storedInfo = this.onlineServers.get(info.getServerName());
    if (storedInfo == null) {
      LOG.warn("Received report from unknown server -- telling it " +
        "to " + HMsg.REGIONSERVER_STOP + ": " + info.getServerName());
      // The HBaseMaster may have been restarted.
      // Tell the RegionServer to abort!
      return new HMsg[] {HMsg.REGIONSERVER_STOP};
    } else if (storedInfo.getStartCode() != info.getStartCode()) {
      // This state is reachable if:
      //
      // 1) RegionServer A started
      // 2) RegionServer B started on the same machine, then
      //    clobbered A in regionServerStartup.
      // 3) RegionServer A returns, expecting to work as usual.
      //
      // The answer is to ask A to shut down for good.

      if (LOG.isDebugEnabled()) {
        LOG.debug("region server race condition detected: " +
            info.getServerName());
      }

      synchronized (this.onlineServers) {
        removeServerInfo(info.getServerName());
        notifyOnlineServers();
      }

      return new HMsg[] {HMsg.REGIONSERVER_STOP};
    } else {
      return processRegionServerAllsWell(info, mostLoadedRegions, msgs);
    }
  }

  /**
   * Region server is exiting with a clean shutdown.
   *
   * In this case, the server sends MSG_REPORT_EXITING in msgs[0] followed by
   * a MSG_REPORT_CLOSE for each region it was serving.
   * @param serverInfo
   * @param msgs
   */
  private void processRegionServerExit(HServerInfo serverInfo, HMsg[] msgs) {
    // TODO: won't send MSG_REPORT_CLOSE for each region
    // TODO: Change structure/naming of removeServerInfo (does much more)
    // TODO: should we keep this?  seems like master could issue closes
    //       and then rs would stagger rather than closing all before
    //       reporting back to master they are closed?
    synchronized (this.onlineServers) {
      // This method removes ROOT/META from the list and marks them to be
      // reassigned in addition to other housework.
      if (removeServerInfo(serverInfo.getServerName())) {
        // Only process the exit message if the server still has registered info.
        // Otherwise we could end up processing the server exit twice.
        LOG.info("Region server " + serverInfo.getServerName() +
          ": MSG_REPORT_EXITING");
        // Get all the regions the server was serving reassigned
        // (if we are not shutting down).
        if (!master.isStopped()) {
          for (int i = 1; i < msgs.length; i++) {
            LOG.info("Processing " + msgs[i] + " from " +
              serverInfo.getServerName());
            assert msgs[i].getType() == HMsg.Type.MSG_REGION_CLOSE;
            HRegionInfo info = msgs[i].getRegionInfo();
            // Meta/root region offlining is handed in removeServerInfo above.
            if (!info.isMetaRegion()) {
//              synchronized (masterStatus.getRegionManager()) {
//                if (!masterStatus.getRegionManager().isOfflined(info.getRegionNameAsString())) {
//                  masterStatus.getRegionManager().setUnassigned(info, true);
//                } else {
//                  masterStatus.getRegionManager().removeRegion(info);
//                }
//              }
            }
          }
        }
        // There should not be any regions in transition for this server - the
        // server should finish transitions itself before closing
//        Map<String, RegionState> inTransition = masterStatus.getRegionManager()
//            .getRegionsInTransitionOnServer(serverInfo.getServerName());
//        for (Map.Entry<String, RegionState> entry : inTransition.entrySet()) {
//          LOG.warn("Region server " + serverInfo.getServerName()
//              + " shut down with region " + entry.getKey() + " in transition "
//              + "state " + entry.getValue());
//          masterStatus.getRegionManager().setUnassigned(entry.getValue().getRegionInfo(),
//              true);
//        }
      }
    }
  }

  /**
   *  RegionServer is checking in, no exceptional circumstances
   * @param serverInfo
   * @param mostLoadedRegions
   * @param msgs
   * @return
   * @throws IOException
   */
  private HMsg[] processRegionServerAllsWell(HServerInfo serverInfo,
      final HRegionInfo[] mostLoadedRegions, HMsg[] msgs)
  throws IOException {
    // Refresh the info object and the load information
    this.onlineServers.put(serverInfo.getServerName(), serverInfo);
    HServerLoad load = serverInfo.getLoad();
    if(load != null && this.masterMetrics != null) {
      masterMetrics.incrementRequests(load.getNumberOfRequests());
    }
    // No more piggyback messages on heartbeats for other stuff
    return msgs;
  }

  /**
   * @param serverName
   * @return True if we removed server from the list.
   */
  private boolean removeServerInfo(final String serverName) {
    HServerInfo info = this.onlineServers.remove(serverName);
    if (info != null) {
      this.availableServers.decrementAndGet();
      return true;
    }
    return false;
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
    double averageLoad = 0.0;
    for (HServerInfo hsi : onlineServers.values()) {
        numServers++;
        totalLoad += hsi.getLoad().getNumberOfRegions();
    }
    averageLoad = (double)totalLoad / (double)numServers;
    return averageLoad;
  }

  /** @return the number of active servers */
  public int numServers() {
    return availableServers.get();
  }

  /**
   * @param name server name
   * @return HServerInfo for the given server address
   */
  public HServerInfo getServerInfo(String name) {
    return this.onlineServers.get(name);
  }

  /**
   * @return Read-only map of servers to serverinfo.
   */
  public Map<String, HServerInfo> getOnlineServers() {
    synchronized (this.onlineServers) {
      return Collections.unmodifiableMap(this.onlineServers);
    }
  }

  /**
   * @param hsa
   * @return The HServerInfo whose HServerAddress is <code>hsa</code> or null
   * if nothing found.
   */
  public HServerInfo getHServerInfo(final HServerAddress hsa) {
    synchronized(this.onlineServers) {
      // TODO: This is primitive.  Do a better search.
      for (Map.Entry<String, HServerInfo> e: this.onlineServers.entrySet()) {
        if (e.getValue().getServerAddress().equals(hsa)) {
          return e.getValue();
        }
      }
    }
    return null;
  }

  private void notifyOnlineServers() {
    synchronized (this.onlineServers) {
      this.onlineServers.notifyAll();
    }
  }

  /*
   * Wait on regionservers to report in
   * with {@link #regionServerReport(HServerInfo, HMsg[])} so they get notice
   * the master is going down.  Waits until all region servers come back with
   * a MSG_REGIONSERVER_STOP.
   */
  void letRegionServersShutdown() {
    synchronized (onlineServers) {
      while (onlineServers.size() > 0) {
        LOG.info("Waiting on following regionserver(s) to go down " +
          this.onlineServers.values());
        try {
          this.onlineServers.wait(500);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
  }

  /*
   * Expire the passed server.  Add it to list of deadservers and queue a
   * shutdown processing.
   */
  public synchronized void expireServer(final HServerInfo hsi) {
    // First check a server to expire.  ServerName is of the form:
    // <hostname> , <port> , <startcode>
    String serverName = hsi.getServerName();
    HServerInfo info = this.onlineServers.get(serverName);
    if (info == null) {
      LOG.warn("Received expiration of " + hsi.getServerName() +
      " but server is not currently online");
      return;
    }
    if (this.deadServers.contains(serverName)) {
      // TODO: Can this happen?  It shouldn't be online in this case?
      LOG.warn("Received expiration of " + hsi.getServerName() +
          " but server shutdown is already in progress");
      return;
    }
    // Remove the server from the known servers lists and update load info
    this.onlineServers.remove(serverName);
    this.availableServers.decrementAndGet();
    this.serverConnections.remove(serverName);
    // Add to dead servers and queue a shutdown processing.
    this.deadServers.add(serverName);
    new ServerShutdownHandler(master).submit();
    LOG.debug("Added=" + serverName +
      " to dead servers, submitted shutdown handler to be executed");
  }

  /**
   * @param serverName
   */
  void removeDeadServer(String serverName) {
    this.deadServers.remove(serverName);
  }

  /**
   * @param serverName
   * @return true if server is dead
   */
  public boolean isDead(final String serverName) {
    return isDead(serverName, false);
  }

  /**
   * @param serverName Servername as either <code>host:port</code> or
   * <code>host,port,startcode</code>.
   * @param hostAndPortOnly True if <code>serverName</code> is host and
   * port only (<code>host:port</code>) and if so, then we do a prefix compare
   * (ignoring start codes) looking for dead server.
   * @return true if server is dead
   */
  boolean isDead(final String serverName, final boolean hostAndPortOnly) {
    return isDead(this.deadServers, serverName, hostAndPortOnly);
  }

  static boolean isDead(final Set<String> deadServers,
      final String serverName, final boolean hostAndPortOnly) {
    return HServerInfo.isServer(deadServers, serverName, hostAndPortOnly);
  }

  Set<String> getDeadServers() {
    return this.deadServers;
  }

  public boolean canAssignUserRegions() {
    if (minimumServerCount == 0) {
      return true;
    }
    return (numServers() >= minimumServerCount);
  }

  public void setMinimumServerCount(int minimumServerCount) {
    this.minimumServerCount = minimumServerCount;
  }

  // RPC methods to region servers

  /**
   * Sends an OPEN RPC to the specified server to open the specified region.
   * <p>
   * There is no reason a region server should reject this open.
   * <p>
   * @param server server to open a region
   * @param regionName region to open
   */
  public void sendRegionOpen(HServerInfo server, HRegionInfo region) {
    HRegionInterface hri = getServerConnection(server);
    if(hri == null) {
      LOG.warn("Attempting to send OPEN RPC to server " + server.getServerName()
          + " failed because no RPC connection found to this server");
      return;
    }
    hri.openRegion(region);
  }

  /**
   * Sends an CLOSE RPC to the specified server to close the specified region.
   * <p>
   * A region server could reject the close request because it either does not
   * have the specified region or the region is being split.
   * @param server server to open a region
   * @param regionName region to open
   * @return true if server acknowledged close, false if not
   * @throws NotServingRegionException
   */
  public void sendRegionClose(HServerInfo server, HRegionInfo region)
  throws NotServingRegionException {
    HRegionInterface hri = getServerConnection(server);
    if(hri == null) {
      LOG.warn("Attempting to send CLOSE RPC to server " + server.getServerName()
          + " failed because no RPC connection found to this server");
      return;
    }
    hri.closeRegion(region);
  }

  private HRegionInterface getServerConnection(HServerInfo info) {
    try {
      HRegionInterface hri = serverConnections.get(info.getServerName());
      if(hri == null) {
        LOG.info("new connection");
        hri = master.getServerConnection().getHRegionConnection(
          info.getServerAddress(), false);
        serverConnections.put(info.getServerName(), hri);
      }
      return hri;
    } catch (IOException e) {
      LOG.error("Error connecting to region server", e);
      throw new RuntimeException("Fatal error connection to RS", e);
    }
  }

  /**
   * Waits for the minimum number of servers to be running.
   */
  public void waitForMinServers() {
    while(numServers() < minimumServerCount) {
//        !masterStatus.getShutdownRequested().get()) {
      LOG.info("Waiting for enough servers to check in.  Currently have " +
          numServers() + " but need at least " + minimumServerCount);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted waiting for servers to check in, looping");
      }
    }
  }

  public List<HServerInfo> getOnlineServersList() {
    // TODO: optimize the load balancer call so we don't need to make a new list
    return new ArrayList<HServerInfo>(onlineServers.values());
  }

  public boolean isServerOnline(String serverName) {
    return onlineServers.containsKey(serverName);
  }

  public void shutdownCluster() {
    LOG.info("Cluster shutdown requested. Starting to quiesce servers");
    this.clusterShutdown = true;
  }
}