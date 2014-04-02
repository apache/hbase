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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.RegionManager.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * The ServerManager class manages info about region servers - HServerInfo,
 * load numbers, dying servers, etc.
 */
public class ServerManager implements ConfigurationObserver {
  private static final Log LOG =
    LogFactory.getLog(ServerManager.class.getName());

  public static final Pattern HOST_PORT_RE =
      Pattern.compile("^[0-9a-zA-Z-_.]+:[0-9]{1,5}$");

  private final AtomicInteger quiescedServers = new AtomicInteger(0);

  // The map of known server names to server info
  private final Map<String, HServerInfo> serversToServerInfo =
    new ConcurrentHashMap<String, HServerInfo>();

  /*
   * Set of known dead servers.  On znode expiration, servers are added here.
   * This is needed in case of a network partitioning where the server's lease
   * expires, but the server is still running. After the network is healed,
   * and it's server logs are recovered, it will be told to call server startup
   * because by then, its regions have probably been reassigned.
   */
  private final Set<String> deadServers =
    Collections.synchronizedSet(new HashSet<String>());

  /*
   * Set of messages that we want to send to a server.
   *
   * Most of the messages that we wish to send to a server is generated and
   * sent during processMsgs. The only exception is when we process a region
   * open. processRegionOpen gets called from handleRegionOpenedEvent in
   * response to the ZK event. It stores the intended message (for example:
   *  MSG_REGION_CLOSE_WITHOUT_REPORT, in the case of duplicate assignment)
   *  to be piggybacked upon the next processMsgs;
   */
  private ConcurrentHashMap<HServerInfo, ArrayList<HMsg>> pendingMsgsToSvrsMap;

  /** A bidirectional map between server names and their load */
  private ServerLoadMap<HServerLoad> serversToLoad = new ServerLoadMap<HServerLoad>();

  private HMaster master;

  /* The regionserver will not be assigned or asked close regions if it
   * is currently opening >= this many regions.
   */
  private final int nobalancingCount;

  private final ServerMonitor serverMonitorThread;

  private int minimumServerCount;

  private final OldLogsCleaner oldLogCleaner;

  private final long blacklistNodeExpirationTimeWindow;

  private final long blacklistUpdateInterval;

  private final long timeBlackListWasUpdated = 0;

  // Default interval after which node will no longer be in blacklist and will start
  // receiving regions. (1 hr)
  private static final int DEFAULT_BLACKLIST_NODE_EXPIRATION_WINDOW = 3600000;

  // Default interval after which the backlist will be checked and updated. (10 minutes)
  private static final int DEFAULT_BLACKLIST_UPDATE_WINDOW = 600000;

  /**
   * A lock that controls simultaneous changes and lookup to the dead server set and the server to
   * server info map. Required so that we don't reassign the same region both in expireServer
   * and in the base scanner.
   */
  final Object deadServerStatusLock = new Object();

  private final AtomicBoolean resendDroppedMessages = new AtomicBoolean();

  /**
   * A set of host:port pairs representing regionservers that are blacklisted
   * from region assignment. Used for unit tests only. Please do not use this
   * for production, because in a real situation a blacklisted server might
   * be the underloaded one, so the master will keep trying to assign regions
   * to it.
   */
  private static final ConcurrentHashMap<String, Long> blacklistedRSHostPortMap =
      new ConcurrentHashMap<String, Long>();

  private final RegionChecker regionChecker;

  /*
   * Dumps into log current stats on dead servers and number of servers
   * TODO: Make this a metric; dump metrics into log.
   */
  class ServerMonitor extends Chore {
    ServerMonitor(final int period, final Stoppable stop) {
      super("ServerMonitor", period, stop);
    }

    @Override
    protected void chore() {
      int numServers = serversToServerInfo.size();
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
        (deadServersList != null? deadServersList: ""));
      long currentTime = System.currentTimeMillis();
      if ((currentTime - timeBlackListWasUpdated) >
        blacklistUpdateInterval) {
        updateBlacklistedServersList(currentTime);
      }
    }

    private void updateBlacklistedServersList(long currentTime) {

      Iterator<ConcurrentHashMap.Entry<String, Long>> iterator =
          ServerManager.blacklistedRSHostPortMap.entrySet().iterator();
      ConcurrentHashMap.Entry<String, Long> entry = null;
      while (iterator.hasNext()) {
        entry = iterator.next();
        if ((currentTime - entry.getValue()) > blacklistNodeExpirationTimeWindow) {
          LOG.info("Removing " + entry + " from blacklist server list.");
          iterator.remove();
        }
      }
    }
  }

  /**
   * Constructor.
   * @param master
   */
  public ServerManager(HMaster master) {
    this.master = master;
    Configuration c = master.getConfiguration();
    this.nobalancingCount = c.getInt("hbase.regions.nobalancing.count", 4);
    int metaRescanInterval = c.getInt("hbase.master.meta.thread.rescanfrequency",
      60 * 1000);
    this.minimumServerCount = c.getInt("hbase.regions.server.count.min", 0);
    this.serverMonitorThread = new ServerMonitor(metaRescanInterval,
      master.getStopper());
    String n = Thread.currentThread().getName();
    Threads.setDaemonThreadRunning(this.serverMonitorThread,
      n + ".serverMonitor");
    this.oldLogCleaner = new OldLogsCleaner(
      c.getInt("hbase.master.meta.thread.rescanfrequency",60 * 1000),
        master.getStopper(), c, master.getFileSystem(), master.getOldLogDir());
    Threads.setDaemonThreadRunning(oldLogCleaner,
      n + ".oldLogCleaner");
    rackManager = new RackManager(c);
    Threads.setDaemonThreadRunning(new ServerTimeoutMonitor(c),
        n + "ServerManager-Timeout-Monitor");

    this.pendingMsgsToSvrsMap = new ConcurrentHashMap<HServerInfo, ArrayList<HMsg>>();

    this.regionChecker = new RegionChecker(master);

    this.blacklistNodeExpirationTimeWindow = c.getLong("hbase.master.blacklist.expiration.window",
        DEFAULT_BLACKLIST_NODE_EXPIRATION_WINDOW);
    this.blacklistUpdateInterval = c.getLong("hbase.master.blacklist.update.interval",
        DEFAULT_BLACKLIST_UPDATE_WINDOW);

    this.resendDroppedMessages.set(c.getBoolean("hbase.master.msgs.resend-openclose", false));
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
      String message = "Duplicate regionserver check-in for host/port: " + hostAndPort +
          "; existingServer=" + existingServer + ", newServer=" + info;
      LOG.info(message);
      long existingStartCode = existingServer.getStartCode();
      long newStartCode = info.getStartCode();
      if (existingStartCode < newStartCode) {
        LOG.info("Existing regionserver looks stale, expiring: " + existingServer);
        expireServer(existingServer);
      } else if (existingStartCode == newStartCode) {
        LOG.debug("Duplicate region server check-in with start code " + existingStartCode + ": " +
            info.getServerName() + ", processing normally");
      } else {
        LOG.info("Newer server has already checked in, telling the old server to stop");
        throw new YouAreDeadException("A new server with start code " + existingStartCode
            + " is already online for " + info.getHostnamePort());
      }
    }
    checkIsDead(info.getServerName(), "STARTUP");
    LOG.info("Received start message from: " + info.getServerName());
    recordNewServer(info);
  }

  private HServerInfo haveServerWithSameHostAndPortAlready(final String hostnamePort) {
    synchronized (this.serversToServerInfo) {
      for (Map.Entry<String, HServerInfo> e: this.serversToServerInfo.entrySet()) {
        if (e.getValue().getHostnamePort().equals(hostnamePort)) {
          return e.getValue();
        }
      }
    }
    return null;
  }

  /*
   * If this server is on the dead list, reject it with a LeaseStillHeldException
   * @param serverName Server name formatted as host_port_startcode.
   * @param what START or REPORT
   * @throws LeaseStillHeldException
   */
  private void checkIsDead(final String serverName, final String what)
  throws YouAreDeadException {
    if (!isDeadProcessingPending(serverName)) return;
    String message = "Server " + what + " rejected; currently processing " +
      serverName + " as dead server";
    LOG.debug(message);
    throw new YouAreDeadException(message);
  }

  /**
   * Adds the HSI to the RS list and creates an empty load
   * @param info The region server informations
   */
  public void recordNewServer(HServerInfo info) throws IOException {
    recordNewServer(info, false);
  }

  /** Restore the old value for the given key in a map */
  private static <K, V> void undoMapUpdate(Map<K, V> m, K key, V oldValue) {
    if (oldValue == null) {
      m.remove(key);
    } else {
      m.put(key, oldValue);
    }
  }

  /**
   * Adds the HSI to the RS list
   * @param info The region server informations
   * @param useInfoLoad True if the load from the info should be used
   *                    like under a master failover
   */
  void recordNewServer(HServerInfo info, boolean useInfoLoad) throws IOException {
    HServerLoad load = useInfoLoad ? info.getLoad() : new HServerLoad();
    String serverName = info.getServerName();
    info.setLoad(load);
    // We must set this watcher here because it can be set on a fresh start
    // or on a failover
    Watcher watcher = new ServerExpirer(new HServerInfo(info));

    // Save the old values so we can rollback if we fail setting the ZK watch
    HServerInfo oldServerInfo = serversToServerInfo.get(serverName);
    HServerLoad oldServerLoad = serversToLoad.get(serverName);

    this.serversToServerInfo.put(serverName, info);
    serversToLoad.updateServerLoad(serverName, load);

    // Setting a watch after making changes to internal server to server info / load data
    // structures because the watch can fire immediately after being set.
    try {
      master.getZooKeeperWrapper().setRSLocationWatch(info, watcher);
    } catch (IOException ex) {
      // Could not set a watch, undo the above changes and re-throw.
      serversToLoad.updateServerLoad(serverName, oldServerLoad);
      undoMapUpdate(serversToServerInfo, serverName, oldServerInfo);
      LOG.error("Could not set watch on regionserver znode for " + serverName);
      throw ex;
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
    HServerInfo storedInfo = this.serversToServerInfo.get(info.getServerName());
    if (storedInfo == null) {
      LOG.warn("Received report from unknown server -- telling it " +
        "to " + HMsg.REGIONSERVER_STOP + ": " + info.getServerName());
      // The HBaseMaster may have been restarted.
      // Tell the RegionServer to abort!
      return new HMsg[] {HMsg.REGIONSERVER_STOP};
    }
    if (msgs.length > 0) {
      if (msgs[0].isType(HMsg.Type.MSG_REPORT_BEGINNING_OF_THE_END)) {
        // region server is going to shut down. do not expect any more reports
        // from this server
        HServerLoad load = this.serversToLoad.get(info.getServerName());
        if (load != null) {
          load.lastLoadRefreshTime = 0;
          LOG.info("Server " + serverInfo.getServerName() +
              " is preparing to shutdown");
        } else {
          LOG.info("Server " + serverInfo.getServerName() +
              " sent preparing to shutdown, " +
              "but that server probably already exited");
        }
        return HMsg.EMPTY_HMSG_ARRAY;
      } else if (msgs[0].isType(HMsg.Type.MSG_REPORT_EXITING)) {
        processRegionServerExit(info, msgs);
        return HMsg.EMPTY_HMSG_ARRAY;
      } else if (msgs[0].isType(HMsg.Type.MSG_REPORT_EXITING_FOR_RESTART)) {
        processRegionServerRestart(serverInfo, msgs);
        return HMsg.EMPTY_HMSG_ARRAY;
      } else if (msgs[0].isType(HMsg.Type.MSG_REPORT_QUIESCED)) {
        LOG.info("Region server " + info.getServerName() + " quiesced");
        this.quiescedServers.incrementAndGet();
      }
    }
    if (this.master.isClusterShutdownRequested()) {
      if (quiescedServers.get() >= serversToServerInfo.size()) {
        // If the only servers we know about are meta servers, then we can
        // proceed with shutdown
        LOG.info("All user tables quiesced. Proceeding with shutdown");
        this.master.startShutdown();
      }
      if (!this.master.isClosed()) {
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
    if (this.master.isClosed() && master.isClusterShutdownRequested()) {
      // Tell server to shut down if we are shutting down.  This should
      // happen after check of MSG_REPORT_EXITING above, since region server
      // will send us one of these messages after it gets MSG_REGIONSERVER_STOP
      return new HMsg [] {HMsg.REGIONSERVER_STOP};
    }
    this.master.updateLastFlushedSequenceIds(info);
    return processRegionServerAllsWell(info, mostLoadedRegions, msgs);
  }

  /**
   * Region server is going down for a restart, trying to preserve locality of
   * its regions for a fixed amount of time.
   *
   * @param serverInfo
   *          the server that is restarting
   * @param msgs
   *          the initial restart message, followed by the regions it was
   *          serving
   */
  private void processRegionServerRestart(final HServerInfo serverInfo,
      HMsg msgs[]) {

    Set<HRegionInfo> regions = new TreeSet<HRegionInfo>();

    // skipping first message
    for (int i = 1; i < msgs.length; ++i) {
      if (msgs[i].getRegionInfo().isMetaRegion()
          || msgs[i].getRegionInfo().isRootRegion()) {
        continue;
      }
      regions.add(msgs[i].getRegionInfo());
    }

    LOG.info("Region server " + serverInfo.getServerName()
        + ": MSG_REPORT_EXITING_FOR_RESTART");

    // set info for restarting -- maybe not do this if regions is empty...
    // CRUCIAL that this is done before calling processRegionServerExit
    master.getRegionManager().addRegionServerForRestart(serverInfo, regions);

    // process normal HRegion closing
    msgs[0] = new HMsg(HMsg.Type.MSG_REPORT_EXITING);
    processRegionServerExit(serverInfo, msgs);
  }

  /*
   * Region server is exiting with a clean shutdown.
   *
   * In this case, the server sends MSG_REPORT_EXITING in msgs[0] followed by
   * a MSG_REPORT_CLOSE for each region it was serving.
   * @param serverInfo
   * @param msgs
   */
  private void processRegionServerExit(HServerInfo serverInfo, HMsg[] msgs) {

    for(int i = 1; i < msgs.length; i++) {
      this.regionChecker.becameClosed(msgs[i].getRegionInfo());
    }

    // This method removes ROOT/META from the list and marks them to be
    // reassigned in addition to other housework.
    processServerInfoOnShutdown(serverInfo);
    // Only process the exit message if the server still has registered info.
    // Otherwise we could end up processing the server exit twice.
    LOG.info("Region server " + serverInfo.getServerName() +
        ": MSG_REPORT_EXITING");

    LOG.info("Removing server's info " + serverInfo.getServerName());
    this.serversToServerInfo.remove(serverInfo.getServerName());
    serversToLoad.removeServerLoad(serverInfo.getServerName());
    if (this.master.getSplitLogManager() != null) {
      this.master.getSplitLogManager().handleDeadServer(serverInfo.getServerName());
    }

    // Get all the regions the server was serving reassigned
    // (if we are not shutting down).
    if (!master.isClosed()) {
      for (int i = 1; i < msgs.length; i++) {
        LOG.info("Processing " + msgs[i] + " from " +
            serverInfo.getServerName());
        assert msgs[i].getType() == HMsg.Type.MSG_REPORT_CLOSE;
        HRegionInfo info = msgs[i].getRegionInfo();
        // Meta/root region offlining is handed in removeServerInfo above.
        if (!info.isMetaRegion()) {
          synchronized (master.getRegionManager()) {
            if (!master.getRegionManager().isOfflined(info.getRegionNameAsString())) {
              master.getRegionManager().setUnassigned(info, true);
            } else {
              master.getRegionManager().removeRegion(info);
            }
          }
        }
      }
    }
    // There should not be any regions in transition for this server - the
    // server should finish transitions itself before closing
    Map<String, RegionState> inTransition = master.getRegionManager()
        .getRegionsInTransitionOnServer(serverInfo.getServerName());
    for (Map.Entry<String, RegionState> entry : inTransition.entrySet()) {
      LOG.warn("Region server " + serverInfo.getServerName()
          + " shut down with region " + entry.getKey() + " in transition "
          + "state " + entry.getValue());
      master.getRegionManager().setUnassigned(entry.getValue().getRegionInfo(),
          true);
    }
  }

  /*
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
    this.serversToServerInfo.put(serverInfo.getServerName(), serverInfo);
    HServerLoad oldLoad = serversToLoad.get(serverInfo.getServerName());
    HServerLoad newLoad = serverInfo.getLoad();
    if (oldLoad != null) {
      // XXX why are we using oldLoad to update metrics
      this.master.getMetrics().incrementRequests(oldLoad.getNumberOfRequests());
    }

    // Set the current load information
    newLoad.lastLoadRefreshTime = EnvironmentEdgeManager.currentTimeMillis();
    if (oldLoad != null && (oldLoad.expireAfter != Long.MAX_VALUE)) {
      LOG.info("Restarted receiving reports from server " +
          serverInfo.getServerName());
    }
    newLoad.expireAfter = Long.MAX_VALUE;
    serversToLoad.updateServerLoad(serverInfo.getServerName(), newLoad);

    // Next, process messages for this server
    return processMsgs(serverInfo, mostLoadedRegions, msgs);
  }

  /*
   * Process all the incoming messages from a server that's contacted us.
   * Note that we never need to update the server's load information because
   * that has already been done in regionServerReport.
   * @param serverInfo
   * @param mostLoadedRegions
   * @param incomingMsgs
   * @return
   */
  private HMsg[] processMsgs(HServerInfo serverInfo,
      HRegionInfo[] mostLoadedRegions, HMsg incomingMsgs[]) {
    ArrayList<HMsg> returnMsgs = new ArrayList<HMsg>();
    if (serverInfo.getServerAddress() == null) {
      throw new NullPointerException("Server address cannot be null; " +
        "hbase-958 debugging");
    }

    // Get reports on what the RegionServer did.
    // Be careful that in message processors we don't throw exceptions that
    // break the switch below because then we might drop messages on the floor.
    int openingCount = 0;
    HashSet<String> openingRegions = null;
    HashSet<String> closingRegions = null;
    for (int i = 0; i < incomingMsgs.length; i++) {
      HRegionInfo region = incomingMsgs[i].getRegionInfo();
      LOG.info("Processing " + incomingMsgs[i] + " from " +
        serverInfo.getServerName() + "; " + (i + 1) + " of " +
        incomingMsgs.length);
      if (!this.master.getRegionServerOperationQueue().
          process(serverInfo, incomingMsgs[i])) {
        LOG.debug("Not proceeding further for " + incomingMsgs[i] + " from " + serverInfo);
        continue;
      }
      switch (incomingMsgs[i].getType()) {
        case MSG_REPORT_PROCESS_OPEN:
          openingCount++;
          if (openingRegions == null)
            openingRegions = new HashSet<String>();
          openingRegions.add(incomingMsgs[i].getRegionInfo().getEncodedName());
          LOG.debug("Added to openingRegions " + incomingMsgs[i] + " from " + serverInfo);
          break;

        case MSG_REPORT_OPEN:
          LOG.error("MSG_REPORT_OPEN is not expected to be received from RS.");
          break;

        case MSG_REPORT_PROCESS_CLOSE:
          if (closingRegions == null)
            closingRegions = new HashSet<String>();
          closingRegions.add(incomingMsgs[i].getRegionInfo().getEncodedName());
          break;

        case MSG_REPORT_CLOSE:
          processRegionClose(serverInfo, region);
          break;

        case MSG_REPORT_SPLIT:
          processSplitRegion(region, incomingMsgs[++i].getRegionInfo(),
            incomingMsgs[++i].getRegionInfo());
          break;

        case MSG_REPORT_SPLIT_INCLUDES_DAUGHTERS:
          processSplitRegion(region, incomingMsgs[i].getDaughterA(),
            incomingMsgs[i].getDaughterB());
          break;

        default:
          LOG.warn("Impossible state during message processing. Instruction: " +
            incomingMsgs[i].getType());
      }
    }

    synchronized (this.master.getRegionManager()) {
      // Tell the region server to close regions that we have marked for closing.
      for (HRegionInfo i:
        this.master.getRegionManager().getMarkedToClose(serverInfo.getServerName())) {
        if (resendDroppedMessages.get()) {
          if (closingRegions == null || !closingRegions.contains(i.getEncodedName())) {
            HMsg msg = new HMsg(HMsg.Type.MSG_REGION_CLOSE, i);
            LOG.info("HMsg " + msg.toString() + " was lost earlier. Resending to " + serverInfo.getServerName());
            returnMsgs.add(msg);
          } else {
            // Transition the region from toClose to closing state
            this.master.getRegionManager().setPendingClose(i.getRegionNameAsString());
          }
        } else {
            // old code path for backward compatability during rolling restart.
            // TODO: Amit: get rid of this after all clusters have been pushed
            HMsg msg = new HMsg(HMsg.Type.MSG_REGION_CLOSE, i);
            returnMsgs.add(msg);
            this.master.getRegionManager().setPendingClose(i.getRegionNameAsString());
        }
      }

      // Figure out what the RegionServer ought to do, and write back.

      if (resendDroppedMessages.get()) {
        // 1. Remind the server to open the regions that the RS has not acked for
        // Normally, the master shouldn't need to do this. But, this may be required
        // if there was a network Incident, in which the master's message to OPEN a
        // region was lost.
        for (HRegionInfo i:
          this.master.getRegionManager().getRegionsInPendingOpenUnacked(serverInfo.getServerName())) {
          if (openingRegions == null || !openingRegions.contains(i.getEncodedName())) {
            HMsg msg = new HMsg(HMsg.Type.MSG_REGION_OPEN, i);
            LOG.info("HMsg " + msg.toString() + " was lost earlier. Resending to " + serverInfo.getServerName());
            returnMsgs.add(msg);
            openingCount++;
          } else {
            LOG.info("Region " + i.getEncodedName() + " is reported to be opening " + serverInfo.getServerName());
            this.processRegionOpening(i.getRegionNameAsString());
          }
        }
      }

      // Should we tell it close regions because its overloaded?  If its
      // currently opening regions, leave it alone till all are open.
      if (openingCount < this.nobalancingCount) {
          master.getRegionManager().assignRegions(serverInfo,
              mostLoadedRegions, returnMsgs);
      }

      // Send any pending table actions.
      this.master.getRegionManager().applyActions(serverInfo, returnMsgs);
      // add any pending messages that we may be holding for the server
      piggyBackPendingMessages(serverInfo, returnMsgs);
    }
    return returnMsgs.toArray(new HMsg[returnMsgs.size()]);
  }

  /*
   * Holds a set of messages that we want to send to a server.
   *
   * Most of the messages that we wish to send to a server is generated and sent
   * during processMsgs. The only exception is when we process a region open.
   * In this case, processRegionOpen gets called from handleRegionOpenedEvent in
   * response to the ZK event.
   *
   * This method stores the intended message (for example:
   * MSG_REGION_CLOSE_WITHOUT_REPORT, in the case of duplicate assignment ) to be
   * piggybacked upon the next processMsgs;
   *
   * @param serverInfo  The server for whom the messages are intended
   * @param msgsToSend  Messages to send
   */
  public void holdMessages(HServerInfo serverInfo, ArrayList<HMsg> msgsToSend) {
    ArrayList<HMsg> msgsForServer = pendingMsgsToSvrsMap.get(serverInfo);

    if (msgsForServer == null) {
      msgsForServer = new ArrayList<HMsg>();
      ArrayList<HMsg> newMsgsForServer =
          pendingMsgsToSvrsMap.putIfAbsent(serverInfo, msgsForServer);
      if (newMsgsForServer != null) {
        // There is already a list of messages for this server, use it.
        msgsForServer = newMsgsForServer;
      }
    }

    synchronized(msgsForServer) {
      msgsForServer.addAll(msgsToSend);
    }
  }

  /*
   * Get the set of messages that we want to send to a server.
   *
   * @param serverInfo  The server whose messages we want to get
   * @param returnMsgs  List to which pending messages are added.
   */
  public void piggyBackPendingMessages(HServerInfo serverInfo,
      List<HMsg> returnMsgs) {
    ArrayList<HMsg> msgsForServer = pendingMsgsToSvrsMap.get(serverInfo);

    if (msgsForServer != null) {
      synchronized(msgsForServer) {
        returnMsgs.addAll(msgsForServer);
        msgsForServer.clear();
      }
    }
  }

  /*
   * A region has split.
   *
   * @param region
   * @param splitA
   * @param splitB
   * @param returnMsgs
   */
  private void processSplitRegion(HRegionInfo region, HRegionInfo a, HRegionInfo b) {
    synchronized (master.getRegionManager()) {
      // Cancel any actions pending for the affected region.
      // This prevents the master from sending a SPLIT message if the table
      // has already split by the region server.
      this.master.getRegionManager().endActions(region.getRegionName());
      assignSplitDaughter(a);
      assignSplitDaughter(b);
      if (region.isMetaTable()) {
        // A meta region has split.
        this. master.getRegionManager().offlineMetaRegionWithStartKey(region.getStartKey());
        this.master.getRegionManager().incrementNumMetaRegions();
      }
    }
  }

  /*
   * Assign new daughter-of-a-split UNLESS its already been assigned.
   * It could have been assigned already in rare case where there was a large
   * gap between insertion of the daughter region into .META. by the
   * splitting regionserver and receipt of the split message in master (See
   * HBASE-1784).
   * @param hri Region to assign.
   */
  private void assignSplitDaughter(final HRegionInfo hri) {
    MetaRegion mr =
      this.master.getRegionManager().getFirstMetaRegionForRegion(hri);
    Get g = new Get(hri.getRegionName());
    g.addFamily(HConstants.CATALOG_FAMILY);
    try {
      HRegionInterface server =
        this.master.getServerConnection().getHRegionConnection(mr.getServer());
      Result r = server.get(mr.getRegionName(), g);
      // If size > 3 -- presume regioninfo, startcode and server -- then presume
      // that this daughter already assigned and return.
      if (r.size() >= 3) return;
    } catch (IOException e) {
      LOG.warn("Failed get on " + HConstants.CATALOG_FAMILY_STR +
        "; possible double-assignment?", e);
    }
    this.master.getRegionManager().setUnassigned(hri, false);
  }


  /*
   * Region server is reporting that a region is now opening
   * consider this an ack for the open request.
   *
   * Master could have received this through the ZK notification.
   * Or, through the heartbeat.
   *
   * @param regionName
   */
  public void processRegionOpening(String regionName) {
    this.master.getRegionManager().setPendingOpenAcked(regionName);
  }

  /*
   * Region server is reporting that a region is now opened
   * @param serverInfo
   * @param region
   * @param returnMsgs
   */
  public void processRegionOpen(HServerInfo serverInfo,
      HRegionInfo region, ArrayList<HMsg> returnMsgs) {

    boolean duplicateAssignment = false;
    RegionManager regionManager = master.getRegionManager();
    synchronized (regionManager) {
      if (!regionManager.isUnassigned(region) &&
          !regionManager.isPendingOpenAckedOrUnacked(region.getRegionNameAsString())) {
        if (region.isRootRegion()) {
          // Root region
          HServerAddress rootServer =
            regionManager.getRootRegionLocation();
          if (rootServer != null) {
            if (rootServer.compareTo(serverInfo.getServerAddress()) == 0) {
              // A duplicate open report from the correct server
              return;
            }
            // We received an open report on the root region, but it is
            // assigned to a different server
            duplicateAssignment = true;
          }
        } else {
          // Not root region. If it is not a pending region, then we are
          // going to treat it as a duplicate assignment, although we can't
          // tell for certain that's the case.
          if (regionManager.isPendingOpenAckedOrUnacked(
              region.getRegionNameAsString())) {
            // A duplicate report from the correct server
            return;
          }

          // Do not consider this a duplicate assignment if we are getting a notification
          // for the same server twice
          if (regionManager.lastOpenedAt(region.getRegionNameAsString(),
              serverInfo.getServerName())) {
            LOG.warn("Multiple REGION_OPENED notifications for region: " + region + " opened on " +
              serverInfo);
            return;
          }
          duplicateAssignment = true;
        }
      }

      if (duplicateAssignment) {
        LOG.warn("region server " + serverInfo.getServerAddress().toString()
            + " should not have opened region "
            + Bytes.toStringBinary(region.getRegionName()));

        // This Region should not have been opened.
        // Ask the server to shut it down, but don't report it as closed.
        // Otherwise the HMaster will think the Region was closed on purpose,
        // and then try to reopen it elsewhere; that's not what we want.
        returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_CLOSE_WITHOUT_REPORT,
          region, "Duplicate assignment".getBytes()));
      } else {
        if (region.isRootRegion()) {
          // it was assigned, and it's not a duplicate assignment, so take it out
          // of the unassigned list.
          regionChecker.becameOpened(region);
          regionManager.removeRegion(region);

          // Store the Root Region location (in memory)
          HServerAddress rootServer = serverInfo.getServerAddress();
          this.master.getServerConnection().setRootRegionLocation(
            new HRegionLocation(region, rootServer));
          regionManager.setRootRegionLocation(serverInfo);
          // Increase the region opened counter
          this.master.getMetrics().incRegionsOpened();
        } else {
          // Note that the table has been assigned and is waiting for the
          // meta table to be updated.
          regionManager.setOpen(region.getRegionNameAsString());
          RegionServerOperation op =
              new ProcessRegionOpen(master, serverInfo, region);
          master.getRegionServerOperationQueue().put(op);
        }
      }
      regionManager.notifyRegionReopened(region);
    }
  }

  /**
   * @param serverInfo
   * @param region
   */
  public void processRegionClose(HServerInfo serverInfo, HRegionInfo region) {
    this.regionChecker.becameClosed(region);

    synchronized (this.master.getRegionManager()) {
      if (region.isRootRegion()) {
        // Root region
        this.master.getRegionManager().unsetRootRegion();
        if (region.isOffline()) {
          // Can't proceed without root region. Shutdown.
          LOG.fatal("root region is marked offline, shutting down the cluster");
          master.requestClusterShutdown();
          return;
        }

      } else if (region.isMetaTable()) {
        // Region is part of the meta table. Remove it from onlineMetaRegions
        this.master.getRegionManager().offlineMetaRegionWithStartKey(region.getStartKey());
      }

      boolean offlineRegion =
        this.master.getRegionManager().isOfflined(region.getRegionNameAsString());
      boolean reassignRegion = !region.isOffline() && !offlineRegion;

      // NOTE: If the region was just being closed and not offlined, we cannot
      //       mark the region unassignedRegions as that changes the ordering of
      //       the messages we've received. In this case, a close could be
      //       processed before an open resulting in the master not agreeing on
      //       the region's state.

      // setClosed works for both CLOSING, and PENDING_CLOSE
      this.master.getRegionManager().setClosed(region.getRegionNameAsString());
      RegionServerOperation op =
        new ProcessRegionClose(master, serverInfo.getServerName(),
            region, offlineRegion, reassignRegion);
      this.master.getRegionServerOperationQueue().put(op);

      if (reassignRegion) {
        // add this region back to preferred assignment
        ThrottledRegionReopener reopener = this.master.getRegionManager().
          getThrottledReopener(region.getTableDesc().getNameAsString());
        if (reopener != null) {
          reopener.addPreferredAssignmentForReopen(region, serverInfo);
        }
      }
    }
  }

  private void processServerInfoOnShutdown(HServerInfo info) {
    String serverName = info.getServerName();
    this.master.getRegionManager().offlineMetaServer(info.getServerAddress());

    //HBASE-1928: Check whether this server has been transitioning the ROOT table
    if (this.master.getRegionManager().isRootInTransitionOnThisServer(serverName)) {
      this.master.getRegionManager().unsetRootRegion();
      this.master.getRegionManager().reassignRootRegion();
    }

    //HBASE-1928: Check whether this server has been transitioning the META table
    HRegionInfo metaServerRegionInfo = this.master.getRegionManager().getMetaServerRegionInfo (serverName);
    if (metaServerRegionInfo != null) {
      this.master.getRegionManager().setUnassigned(metaServerRegionInfo, true);
    }

    return;
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
    synchronized (serversToLoad) {
      for (Map.Entry<String, HServerLoad> entry : serversToLoad.entries()) {
        HServerInfo hsi = serversToServerInfo.get(entry.getKey());
        if (null != hsi) {
          totalLoad += entry.getValue().getNumberOfRegions();
          numServers++;
        }
      }
      if (numServers > 0) {
        averageLoad = (double)totalLoad / (double)numServers;
      }
    }
    return averageLoad;
  }

  /** @return the number of active servers */
  public int numServers() {
    return this.serversToServerInfo.size();
  }

  /**
   * @param name server name
   * @return HServerInfo for the given server address
   */
  public HServerInfo getServerInfo(String name) {
    return this.serversToServerInfo.get(name);
  }

  /**
   * @return Read-only map of servers to serverinfo.
   */
  public Map<String, HServerInfo> getServersToServerInfo() {
    return Collections.unmodifiableMap(this.serversToServerInfo);
  }

  /**
   * @param hsa
   * @return The HServerInfo whose HServerAddress is <code>hsa</code> or null
   * if nothing found.
   */
  public HServerInfo getHServerInfo(final HServerAddress hsa) {
    // TODO: This is primitive.  Do a better search.
    for (Map.Entry<String, HServerInfo> e: this.serversToServerInfo.entrySet()) {
      if (e.getValue().getServerAddress().equals(hsa)) return e.getValue();
    }
    return null;
  }

  /**
   * There is no way to guarantee that the returned servers are really online
   *
   * @return the list of the HServerAddress for all the online region servers.
   */
  public List<HServerAddress> getOnlineRegionServerList() {
    List<HServerAddress> serverList = new ArrayList<HServerAddress>();
    for (HServerInfo serverInfo: this.serversToServerInfo.values()) {
      serverList.add(serverInfo.getServerAddress());
    }
    return serverList;
  }

  /**
   * Wakes up threads waiting on serversToServerInfo
   */
  public void notifyServers() {
    synchronized (this.serversToServerInfo) {
      this.serversToServerInfo.notifyAll();
    }
  }

  /*
   * Wait on regionservers to report in
   * with {@link #regionServerReport(HServerInfo, HMsg[])} so they get notice
   * the master is going down.  Waits until all region servers come back with
   * a MSG_REGIONSERVER_STOP.
   */
  void letRegionServersShutdown() {
    if (!master.checkFileSystem(true)) {
      // Forget waiting for the region servers if the file system has gone
      // away. Just exit as quickly as possible.
      return;
    }
    synchronized (serversToServerInfo) {
      while (serversToServerInfo.size() > 0) {
        LOG.info("Waiting on following regionserver(s) to go down " +
          this.serversToServerInfo.values());
        try {
          this.serversToServerInfo.wait(500);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
  }

  /** Watcher triggered when a RS znode is deleted */
  private class ServerExpirer implements Watcher {
    private HServerInfo server;

    ServerExpirer(final HServerInfo hsi) {
      this.server = hsi;
    }

    @Override
    public void process(WatchedEvent event) {
      if (!event.getType().equals(EventType.NodeDeleted)) {
        LOG.warn("Unexpected event=" + event);
        return;
      }
      LOG.info(this.server.getServerName() + " znode expired");
      expireServer(this.server);
    }
  }

  /*
   * Expire the passed server.  Add it to list of deadservers and queue a
   * shutdown processing.
   */
  private synchronized void expireServer(final HServerInfo hsi) {
    // First check a server to expire.  ServerName is of the form:
    // <hostname> , <port> , <startcode>
    String serverName = hsi.getServerName();

    HServerInfo info = this.serversToServerInfo.get(serverName);
    if (info == null) {
      LOG.warn("No HServerInfo for " + serverName);
      return;
    }
    if (this.deadServers.contains(serverName)) {
      LOG.warn("Already processing shutdown of " + serverName);
      return;
    }

    long expiredSince = serversToLoad.get(serverName).lastLoadRefreshTime;

    synchronized (deadServerStatusLock) {
      // Remove the server from the known servers lists and update load info
      this.serversToServerInfo.remove(serverName);
      serversToLoad.removeServerLoad(serverName);
    }
    // Add to dead servers and queue a shutdown processing.
    LOG.debug("Added=" + serverName +
      " to dead servers, added shutdown processing operation");
    this.deadServers.add(serverName);
    if (this.master.getSplitLogManager() != null) {
      this.master.getSplitLogManager().handleDeadServer(serverName);
    }
    this.master.getRegionServerOperationQueue().
      put(new ProcessServerShutdown(master, info, expiredSince));
    this.master.getMetrics().incRegionServerExpired();
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
  public boolean isDeadProcessingPending(final String serverName) {
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

  public RegionChecker getRegionChecker() {
    return this.regionChecker;
  }

  public ServerLoadMap<HServerLoad> getServersToLoad() {
    return serversToLoad;
  }

  public boolean hasEnoughRegionServers() {
    if (minimumServerCount == 0) {
      return true;
    }
    return (numServers() >= minimumServerCount);
  }

  public void setMinimumServerCount(int minimumServerCount) {
    this.minimumServerCount = minimumServerCount;
  }

  public static void blacklistRSHostPort(String hostPort) {
    if (!HOST_PORT_RE.matcher(hostPort).matches()) {
      throw new IllegalArgumentException("host:port pair expected but got " +
          hostPort);
    }
    LOG.debug("Blacklisting the regionserver " + hostPort + " so that " +
        "no regions are assigned to it.");

    blacklistedRSHostPortMap.put(hostPort, new Long(System.currentTimeMillis()));
  }

  public static void removeServerFromBlackList(String hostPort) {
    LOG.debug("Removing the regionserver " + hostPort + " from the blacklist");
    blacklistedRSHostPortMap.remove(hostPort);
  }

  public static Set<String> getBlacklistedServers() {
    return blacklistedRSHostPortMap.keySet();
  }
  public void joinThreads() {
    oldLogCleaner.triggerNow();
    Threads.shutdown(oldLogCleaner);
    serverMonitorThread.triggerNow();
    Threads.shutdown(serverMonitorThread);
  }

  public void requestShutdown() {
    oldLogCleaner.stopThread();
    serverMonitorThread.stopThread();
    ServerManager.clearRSBlacklist();
  }

  // should ServerTimeoutMonitor and ServerMonitor be merged XXX?
  private class ServerTimeoutMonitor extends HasThread {
    private final Log LOG =
        LogFactory.getLog(ServerTimeoutMonitor.class.getName());
    int timeout;
    int shortTimeout;
    int maxServersToExpirePerRack;

    public ServerTimeoutMonitor(Configuration conf) {
      this.shortTimeout = Math.max(2000,
          2 * conf.getInt("hbase.regionserver.msginterval",
                          HConstants.REGION_SERVER_MSG_INTERVAL));
      this.timeout =
          conf.getInt("hbase.region.server.missed.report.timeout", 10000);
      if (shortTimeout > timeout) {
        timeout = shortTimeout;
      }
      // XXX what should be the default value of maxServersToExpirePerRack? We
      // could set it to the max number of servers that will go down in a rack
      // if a 'line card' in a rack switch were to go bad.
      maxServersToExpirePerRack =
          conf.getInt("hbase.region.server.missed.report.max.expired.per.rack",
                      1);
      LOG.info("hbase.region.server.missed.report.max.expired.per.rack=" +
          maxServersToExpirePerRack);
      LOG.info("hbase.region.server.missed.report.timeout=" +
          timeout + "ms shortTimeout=" + shortTimeout + "ms");
    }

    @Override
    public void run() {
      try {
        while (true) {
          boolean waitingForMoreServersInRackToTimeOut =
              expireTimedOutServers(timeout, maxServersToExpirePerRack);
          if (waitingForMoreServersInRackToTimeOut) {
            sleep(shortTimeout/2);
          } else {
            sleep(timeout/2);
          }
        }
      } catch (Exception e) {
        // even InterruptedException is unexpected
        LOG.fatal("ServerManager Timeout Monitor thread, unexpected exception",
            e);
      }
      return;
    }
  }

  private long lastDetailedLogAt = 0;
  private long lastLoggedServerCount = 0;
  private HashSet<String> inaccessibleRacks = new HashSet<String>();
  private RackManager rackManager;
  /**
   * @param timeout
   * @param maxServersToExpire If more than these many servers expire in a rack
   * then do nothing. Most likely there is something wrong with rack
   * connectivity. Wait for rack to recover or wait for region servers to
   * loose their zk sessions (which typically should have a much longer
   * timeout)
   * @return true if servers timed out but were not expired because
   * we would like to wait and see whether more servers in the rack time out or
   * not
   */
  boolean expireTimedOutServers(long timeout, int maxServersToExpire) {
    long curTime = EnvironmentEdgeManager.currentTimeMillis();
    boolean waitingForMoreServersInRackToTimeOut = false;
    boolean reportDetails = LOG.isTraceEnabled();
    int serverCount = serversToLoad.size();
    if ((curTime > lastDetailedLogAt + (3600 * 1000)) ||
        lastLoggedServerCount != serverCount) {
      reportDetails = true;
      lastDetailedLogAt = curTime;
      lastLoggedServerCount = serverCount;
    }
    // rack -> time of last report from rack
    Map<String, Long> rackLastReportAtMap = new HashMap<String, Long>();
    // rack -> list of timed out servers in rack
    Map<String, List<HServerInfo>> rackTimedOutServersMap =
        new HashMap<String, List<HServerInfo>>();
    // rack -> #servers in rack
    Map<String, Integer> rackNumServersMap =
        new HashMap<String, Integer>();
    for (Map.Entry<String, HServerLoad> e : this.serversToLoad.entries()) {
      HServerInfo si = this.serversToServerInfo.get(e.getKey());
      if (si == null) {
        LOG.debug("ServerTimeoutMonitor : no si for " + e.getKey());
        continue; // server removed
      }
      HServerLoad load = e.getValue();
      String rack = rackManager.getRack(si);
      Integer numServers = rackNumServersMap.get(rack);
      if (numServers == null) {
        numServers = new Integer(1);
      } else {
        numServers = new Integer(numServers + 1);
      }
      rackNumServersMap.put(rack, numServers);
      long timeOfLastPingFromThisServer = load.lastLoadRefreshTime;
      if (timeOfLastPingFromThisServer <= 0 ) {
        // invalid value implies that the master has discovered the rs
        // but hasn't yet had the first report from the rs. It is usually
        // in the master failover path. It might be a while before the rs
        // discovers the new master and starts reporting to the new master
        //
        // could also mean that the region server is shutting down
        continue;
      }
      Long timeOfLastPingFromThisRack = rackLastReportAtMap.get(rack);
      if (timeOfLastPingFromThisRack == null ||
          (timeOfLastPingFromThisServer > timeOfLastPingFromThisRack)) {
        rackLastReportAtMap.put(rack, timeOfLastPingFromThisServer);
      }
      boolean timedOut = curTime > timeOfLastPingFromThisServer + timeout;
      boolean expired = curTime > load.expireAfter;
      if (reportDetails) {
        LOG.debug("server=" + si.getServerName() + " rack=" + rack +
            " timed-out=" + timedOut + " expired=" + expired +
            " timeOfLastPingFromServer=" + timeOfLastPingFromThisServer +
            " timeOfLastPingFromThisRack=" + timeOfLastPingFromThisRack +
            " load.expireAfter =" + load.expireAfter +
            " server in load map " + e.getKey()
            );
      }
      if (!timedOut) {
        continue;
      }
      List<HServerInfo> timedOutServersInThisRack =
          rackTimedOutServersMap.get(rack);
      if (timedOutServersInThisRack == null) {
        timedOutServersInThisRack = new ArrayList<HServerInfo>();
        rackTimedOutServersMap.put(rack, timedOutServersInThisRack);
      }
      timedOutServersInThisRack.add(si);
    }
    if (reportDetails) {
      for (Map.Entry<String, Integer > ent : rackNumServersMap.entrySet()) {
        int t = Math.min(maxServersToExpire, ent.getValue() - 1);
        LOG.info("Rack = " + ent.getKey() + " Servers = " + ent.getValue() +
            " maxServersToExpire = " + t);
      }
    }
    // In rackTimedOutServersMap[rack] we have all the timed-out servers in the
    // rack. All of these servers might not yet be ready to be expired

    // In rackLastReportAtMap[rack] we have the time when the last report from
    // this rack was received

    for (String rack : rackLastReportAtMap.keySet()) {
      if (!rackTimedOutServersMap.keySet().contains(rack)) {
        if (inaccessibleRacks.remove(rack)) {
          LOG.info("rack " + rack + " has become accessible");
        }
      }
    }

    Set<HServerAddress> specialServers = this.getRootAndMetaServers();

    for (Map.Entry<String, List<HServerInfo>> e:
      rackTimedOutServersMap.entrySet()) {
      String rack = e.getKey();
      List<HServerInfo> timedOutServers = e.getValue();
      // if not already set, set the expiry time for all the timed out servers.
      // We look at the last report we have recived from this rack and then
      // set the expiry time for these servers based on that.
      long lastHeardFromRackAt = rackLastReportAtMap.get(rack);
      int numExpired = 0;
      next_timedout_server:
      for (HServerInfo si : timedOutServers) {
        HServerLoad load = serversToLoad.get(si.getServerName());
        if (load == null) {
          LOG.debug("ServerTimeoutMonitor " + si.getServerName() +
              "load-info missing, assuming expired");
          numExpired++;
          continue next_timedout_server;
        }
        if (load.expireAfter == Long.MAX_VALUE) {
          load.expireAfter = lastHeardFromRackAt + timeout;
          long timeToExpiry = load.expireAfter - curTime;
          LOG.debug("Setting load.expireAfter to " + load.expireAfter +
              " for " + si.getServerName() +
              " timeToExpiry  is " + timeToExpiry);
          if (timeToExpiry > 0) { // is first time
            LOG.info("No report from server " + si.getServerName() +
                " for last " + (curTime - load.lastLoadRefreshTime) +
                "ms, will expire in " + timeToExpiry + "ms");
          }
        }
        if (curTime > load.expireAfter) {
          numExpired++;
          LOG.debug("server=" + si.getServerName() + " rack=" + rack +
              " curTime=" + curTime +
              " load.expireAfter =" + load.expireAfter
              + "numExpired++"
              );
        } else {
          // wait for all the timed-out servers to become ready to expire
          waitingForMoreServersInRackToTimeOut = true;
          LOG.debug("server=" + si.getServerName() + " rack=" + rack +
            " curTime=" + curTime +
            " load.expireAfter =" + load.expireAfter
            + " waitingForMoreServersInRackToTimeOut set to true");
        }
      }
      int cappedMaxServersToExpire = Math.min(maxServersToExpire,
          rackNumServersMap.get(rack) - 1);
      if (numExpired > cappedMaxServersToExpire) {
        boolean specialServersInRack = false;
        // Too many servers are ready to be expired in this rack. We expect
        // something is wrong with the rack and not with the servers.
        // We will not expire these servers.

        // There is a risk that when the rack is recovering and less than
        // maxServersToExpire have not reported back, then we might kill those
        // servers whose report is lagging. We wipe out the expireAfter info
        // so that we will wait longer before expiring servers on rack recovery
        for (HServerInfo si : timedOutServers) {
          HServerLoad load = serversToLoad.get(si.getServerName());
          if (load == null) {
            LOG.debug("ServerTimeoutMonitor : no load info for " +
                si.getServerName());
            continue; //server vanished
          }
          // Let us allow RS's serving Meta regions to fast fail
          if (specialServers.contains(si.getServerAddress())) {
            specialServersInRack = true;
          } else  {
            load.expireAfter = Long.MAX_VALUE;
            LOG.debug("Resetting load.expireAfter : to Long.MAX_VALUE for " +
                  si.getServerName());
          }
        }
        if (!inaccessibleRacks.contains(rack)) {
          inaccessibleRacks.add(rack);
          LOG.info("Too many servers count=" + timedOutServers.size() +
              " timed out in rack " + rack + ". Timed out Servers = " +
              timedOutServers +
              (specialServersInRack? "Only failing servers with ROOT/META": "")
              + ". Not expiring"
              + (specialServersInRack? " the rest": " any")
              + ", hoping for rack" + " to become accessible");
        }
      }
      for (HServerInfo si : timedOutServers) {
        HServerLoad load = serversToLoad.get(si.getServerName());
        if (load == null) {
          LOG.debug("ServerTimeoutMonitor : no load info for " +
              si.getServerName() + ", therefore can't expire");
          continue; //server vanished
        }
        // re-check - just in case the server reported
        if (curTime > load.expireAfter) {  // debug
          LOG.info("Expiring server " + si.getServerName() +
              " no report for last " + (curTime - load.lastLoadRefreshTime)
              + " (no znode expired yet)");
          this.expireServer(si);
        } else {
          LOG.debug("Checking again server=" + si.getServerName() +
              " curTime=" + curTime + " load.expireAfter =" + load.expireAfter);
        }
      }
    }
    return waitingForMoreServersInRackToTimeOut;
  }

  private Set<HServerAddress> getRootAndMetaServers() {
    Set<HServerAddress> set = new HashSet<HServerAddress>(2);

    if (master.getRegionManager() != null) { // RegionManager is initialized after ServerManager
      set.add(master.getRegionManager().getRootRegionLocation());
      List<MetaRegion> metaRegions = master.getRegionManager().getListOfOnlineMetaRegions();
      for(MetaRegion metaRegion: metaRegions) {
        set.add(metaRegion.getServer());
      }
    }

    return set;

  }

  public static boolean hasBlacklistedServers() {
    return !blacklistedRSHostPortMap.isEmpty();
  }

  static boolean isServerBlackListed(String hostAndPort) {
    return blacklistedRSHostPortMap.containsKey(hostAndPort);
  }

  public static void clearRSBlacklist() {
    LOG.debug("Cleared all the blacklisted servers");
    blacklistedRSHostPortMap.clear();
  }

  @Override
  public void notifyOnChange(Configuration conf) {
    boolean oldValue = this.resendDroppedMessages.get();
    this.resendDroppedMessages.set(conf.getBoolean("hbase.master.msgs.resend-openclose", oldValue));
  }

  public boolean getResendDroppedMessages() {
    return this.resendDroppedMessages.get();
  }
}
