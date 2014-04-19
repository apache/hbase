/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.executor.RegionTransitionEventData;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

import static org.apache.zookeeper.Watcher.Event.EventType;

/**
 * A utility to recover previous cluster state from ZK on master startup.
 */
public class ZKClusterStateRecovery {

  private static final Log LOG =
      LogFactory.getLog(ZKClusterStateRecovery.class.getName());

  /**
   * We read the list of live region servers from ZK at startup. This is used
   * to decide what logs to split and to select regionservers to assign ROOT
   * and META on master failover.
   */
  private Set<String> liveRSNamesAtStartup;

  /** An unmodifiable wrapper around {@link #liveRSNamesAtStartup} */
  private Set<String> liveRSNamesAtStartupUnmodifiable;

  private final HMaster master;
  private final RegionManager regionManager;
  private final ServerManager serverManager;
  private final ZooKeeperWrapper zkw;
  private final String unassignedZNode;

  /** A snapshot of the list of unassigned znodes */
  private List<String> unassignedNodes;

  public ZKClusterStateRecovery(HMaster master, ServerConnection connection) {
    this.master = master;
    zkw = master.getZooKeeperWrapper();
    regionManager = master.getRegionManager();
    serverManager = master.getServerManager();
    this.unassignedZNode = zkw.getRegionInTransitionZNode();
  }

  /**
   * Register live regionservers that we read from ZK with ServerManager. We do this after starting
   * RPC threads but before log splitting.
   */
  void registerLiveRegionServers() throws IOException {
    liveRSNamesAtStartup = zkw.getLiveRSNames();
    liveRSNamesAtStartupUnmodifiable = Collections.unmodifiableSet(liveRSNamesAtStartup);

    Set<String> rsNamesToAdd = new TreeSet<String>(liveRSNamesAtStartup);

    boolean needToSleep = false;  // no need to sleep at the first iteration

    while (!rsNamesToAdd.isEmpty()) {
      if (needToSleep) {
        Threads.sleepRetainInterrupt(HConstants.SOCKET_RETRY_WAIT_MS);
      }

      // We cannot modify rsNamesToAdd as we iterate it, so we add RS names to retry to this list.
      Set<String> rsLeftToAdd = new TreeSet<String>();

      for (String rsName : rsNamesToAdd) {
        if (master.isStopped()) {
          throw new IOException("Master is shutting down");
        }
        if (Thread.interrupted()) {
          throw new IOException("Interrupted when scanning live RS directory in ZK");
        }

        HServerInfo serverInfo;
        try {
          serverInfo = HServerInfo.fromServerName(rsName);
        } catch (IllegalArgumentException ex) {
          // This error is probably not going to fix itself automatically. Exit the retry loop.
          throw new IOException("Read invalid server name for live RS directory in ZK: " + rsName,
              ex);
        }

        try {
          serverManager.recordNewServer(serverInfo);
        } catch (IOException ex) {
          if (ex.getCause() instanceof NoNodeException) {
            // This regionserver has disappeared, don't try to register it. This will also ensure
            // that we split the logs for this regionserver as part of initial log splitting.
            LOG.info("Regionserver znode " + rsName + " disappeared, not registering");
            liveRSNamesAtStartup.remove(rsName);
          } else {
            LOG.error("Error recording a new regionserver: " + serverInfo.getServerName()
                + ", will retry", ex);
            rsLeftToAdd.add(rsName);
          }
        }
      }

      rsNamesToAdd = rsLeftToAdd;
      needToSleep = true;  // will sleep before the re-trying
    }
  }

  public Set<String> liveRegionServersAtStartup() {
    return liveRSNamesAtStartupUnmodifiable;
  }

  private HRegionInfo parseUnassignedZNode(String regionName, byte[] nodeData,
      RegionTransitionEventData hbEventData) throws IOException {
    String znodePath = zkw.getZNode(unassignedZNode, regionName);
    if (nodeData == null) {
      // This znode does not seem to exist anymore.
      LOG.error("znode for region " + regionName + " disappeared while scanning unassigned " +
          "directory, skipping");
      return null;
    }

    Writables.getWritable(nodeData, hbEventData);

    HMsg msg = hbEventData.getHmsg();
    if (msg == null) {
      LOG.warn("HMsg is not present in unassigned znode data, skipping: " + znodePath);
      return null;
    }

    HRegionInfo hri = msg.getRegionInfo();
    if (hri == null) {
      LOG.warn("Region info read from znode is null, skipping: " + znodePath);
      return null;
    }

    if (!hri.getEncodedName().equals(regionName)) {
      LOG.warn("Region name read from znode data (" + hri.getEncodedName() + ") " +
          "must be the same as znode name: " + regionName + ". Skipping.");
      return null;
    }
    return hri;
  }

  /**
   * Read znode path as part of scanning the unassigned directory.
   * @param regionName the region name to read the unassigned znode for
   * @return znode data or null if the znode no longer exists
   * @throws IOException in case of a ZK error
   */
  private byte[] getUnassignedZNodeAndSetWatch(String regionName)
      throws IOException {
    final String znodePath = zkw.getZNode(unassignedZNode, regionName);
    try {
      return zkw.readUnassignedZNodeAndSetWatch(znodePath);
    } catch (IOException ex) {
      if (ex.getCause() instanceof KeeperException) {
        KeeperException ke = (KeeperException) ex.getCause();
        if (ke.code() == KeeperException.Code.NONODE) {
          LOG.warn("Unassigned node is missing: " + znodePath + ", ignoring");
          return null;
        }
      }
      throw ex;
    }
  }

  /**
   * Goes through the unassigned node directory in ZK.
   */
  private void processUnassignedNodes() throws IOException {
    LOG.info("Processing unassigned znode directory on master startup");
    for (String unassignedRegion : unassignedNodes) {
      if (master.isStopped()) {
        break;
      }

      final String znodePath = zkw.getZNode(unassignedZNode, unassignedRegion);
      // Get znode and set watch
      byte[] nodeData = getUnassignedZNodeAndSetWatch(znodePath);
      if (nodeData == null) {
        // The node disappeared.
        continue;
      }

      HBaseEventType rsState = HBaseEventType.fromByte(nodeData[0]);
      RegionTransitionEventData hbEventData = new RegionTransitionEventData();
      HRegionInfo hri = parseUnassignedZNode(unassignedRegion, nodeData, hbEventData);
      if (hri == null) {
        // Could not parse the znode. Error message already logged.
        continue;
      }

      LOG.info("Found unassigned znode: state=" + rsState + ", region=" +
            hri.getRegionNameAsString() + ", rs=" + hbEventData.getRsName());
      boolean openedOrClosed = rsState == HBaseEventType.RS2ZK_REGION_OPENED ||
          rsState == HBaseEventType.RS2ZK_REGION_CLOSED;

      if (rsState == HBaseEventType.RS2ZK_REGION_CLOSING ||
          rsState == HBaseEventType.RS2ZK_REGION_OPENING ||
          openedOrClosed) {
        regionManager.setRegionStateOnRecovery(rsState, hri, hbEventData.getRsName());
        if (openedOrClosed) {
          master.getUnassignedWatcher().handleRegionStateInZK(EventType.NodeDataChanged,
              znodePath, nodeData, false);
        }
      } else if (rsState == HBaseEventType.M2ZK_REGION_OFFLINE) {
        // Write to ZK = false; override previous state ("force") = true. 
        regionManager.setUnassignedGeneral(false, hri, true);
      } else {
        LOG.warn("Invalid unassigned znode state: " + rsState + " for region " + unassignedRegion);
      }
    }
  }

  /**
   * Ensures that -ROOT- and .META. are assigned and persists region locations from OPENED and
   * CLOSED nodes in the ZK unassigned directory in respectively -ROOT- (for .META. regions) and
   * .META. (for user regions).
   */
  private void recoverRegionStateFromZK() throws IOException {
    if (!isStopped()) {
      regionManager.recoverRootRegionLocationFromZK();
    }

    if (!isStopped()) {
      unassignedNodes = master.getUnassignedWatcher().getUnassignedDirSnapshot();
    }

    if (!isStopped()) {
      processUnassignedNodes();
    }

    if (!isStopped()) {
      master.getUnassignedWatcher().drainZKEventQueue();
    }

    if (!isStopped()) {
      ensureRootAssigned();
    }
  }

  private void ensureRootAssigned() {
    HServerInfo rootServerInfo = regionManager.getRootServerInfo();
    boolean reassignRoot = true;
    if (rootServerInfo != null) {
      // Root appears assigned. Check if it is assigned to an unknown server that we are not
      // processing as dead. In that case we do need to reassign. This logic is similar to
      // what is done in BaseScanner.checkAssigned.
      String serverName = rootServerInfo.getServerName();
      // ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN has the same regionName
      if (regionManager.regionIsInTransition(
          HRegionInfo.ROOT_REGIONINFO.getRegionNameAsString())) {
        // Already in transition, we will wait until it is assigned.
        reassignRoot = false;
        LOG.info("Not assigning root because it is already in transition");
      } else {
        boolean processingRootServerAsDead;
        HServerInfo rootRSInfo;
        synchronized (serverManager.deadServerStatusLock) {
          // Synchronizing to avoid a race condition with ServerManager.expireServer.
          processingRootServerAsDead =
              serverManager.isDeadProcessingPending(serverName);
          rootRSInfo = serverManager.getServerInfo(serverName);
        }
        reassignRoot = !processingRootServerAsDead && rootRSInfo == null;
        LOG.info("reassignRoot=" + reassignRoot +
            ", processingRootServerAsDead=" + processingRootServerAsDead +
            ", rootRSInfo=" + rootRSInfo);
      }
    }

    if (reassignRoot) {
      regionManager.reassignRootRegion();
      // Let us not wait for (max) 1 min to assign META and other
      // user regions.
      regionManager.forceScans();
    }
  }

  public void backgroundRecoverRegionStateFromZK() {
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          recoverRegionStateFromZK();
        } catch (Throwable ex) {
          LOG.error(ex);
          master.stop("Failed to recover region assignment from ZK");
        }
      }
    }, "backgroundRecoverRegionStateFromZK");
    t.setDaemon(true);
    t.start();
  }

  /**
   * Return true if there are no live regionservers. Assumes that
   * {@link #registerLiveRegionServers} has been called. Only used for testing. No decisions are
   * made based on the boolean "is cluster startup" flag.
   */
  boolean isClusterStartup() throws IOException {
    return liveRSNamesAtStartup.isEmpty();
  }

  private boolean isStopped() {
    return master.isStopped();
  }

  public boolean wasLiveRegionServerAtStartup(String serverName) {
    return liveRSNamesAtStartup.contains(serverName);
  }

}
