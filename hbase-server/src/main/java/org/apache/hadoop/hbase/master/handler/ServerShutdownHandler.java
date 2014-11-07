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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.DeadServer;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Process server shutdown.
 * Server-to-handle must be already in the deadservers lists.  See
 * {@link ServerManager#expireServer(ServerName)}
 */
@InterfaceAudience.Private
public class ServerShutdownHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(ServerShutdownHandler.class);
  protected final ServerName serverName;
  protected final MasterServices services;
  protected final DeadServer deadServers;
  protected final boolean shouldSplitHlog; // whether to split HLog or not
  protected final int regionAssignmentWaitTimeout;

  public ServerShutdownHandler(final Server server, final MasterServices services,
      final DeadServer deadServers, final ServerName serverName,
      final boolean shouldSplitHlog) {
    this(server, services, deadServers, serverName, EventType.M_SERVER_SHUTDOWN,
        shouldSplitHlog);
  }

  ServerShutdownHandler(final Server server, final MasterServices services,
      final DeadServer deadServers, final ServerName serverName, EventType type,
      final boolean shouldSplitHlog) {
    super(server, type);
    this.serverName = serverName;
    this.server = server;
    this.services = services;
    this.deadServers = deadServers;
    if (!this.deadServers.isDeadServer(this.serverName)) {
      LOG.warn(this.serverName + " is NOT in deadservers; it should be!");
    }
    this.shouldSplitHlog = shouldSplitHlog;
    this.regionAssignmentWaitTimeout = server.getConfiguration().getInt(
      HConstants.LOG_REPLAY_WAIT_REGION_TIMEOUT, 15000);
  }

  @Override
  public String getInformativeName() {
    if (serverName != null) {
      return this.getClass().getSimpleName() + " for " + serverName;
    } else {
      return super.getInformativeName();
    }
  }

  /**
   * @return True if the server we are processing was carrying <code>hbase:meta</code>
   */
  boolean isCarryingMeta() {
    return false;
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid();
  }

  @Override
  public void process() throws IOException {
    boolean hasLogReplayWork = false;
    final ServerName serverName = this.serverName;
    try {

      // We don't want worker thread in the MetaServerShutdownHandler
      // executor pool to block by waiting availability of hbase:meta
      // Otherwise, it could run into the following issue:
      // 1. The current MetaServerShutdownHandler instance For RS1 waits for the hbase:meta
      //    to come online.
      // 2. The newly assigned hbase:meta region server RS2 was shutdown right after
      //    it opens the hbase:meta region. So the MetaServerShutdownHandler
      //    instance For RS1 will still be blocked.
      // 3. The new instance of MetaServerShutdownHandler for RS2 is queued.
      // 4. The newly assigned hbase:meta region server RS3 was shutdown right after
      //    it opens the hbase:meta region. So the MetaServerShutdownHandler
      //    instance For RS1 and RS2 will still be blocked.
      // 5. The new instance of MetaServerShutdownHandler for RS3 is queued.
      // 6. Repeat until we run out of MetaServerShutdownHandler worker threads
      // The solution here is to resubmit a ServerShutdownHandler request to process
      // user regions on that server so that MetaServerShutdownHandler
      // executor pool is always available.
      //
      // If AssignmentManager hasn't finished rebuilding user regions,
      // we are not ready to assign dead regions either. So we re-queue up
      // the dead server for further processing too.
      AssignmentManager am = services.getAssignmentManager();
      if (isCarryingMeta() // hbase:meta
          || !am.isFailoverCleanupDone()) {
        this.services.getServerManager().processDeadServer(serverName, this.shouldSplitHlog);
        return;
      }

      // Wait on meta to come online; we need it to progress.
      // TODO: Best way to hold strictly here?  We should build this retry logic
      // into the MetaReader operations themselves.
      // TODO: Is the reading of hbase:meta necessary when the Master has state of
      // cluster in its head?  It should be possible to do without reading hbase:meta
      // in all but one case. On split, the RS updates the hbase:meta
      // table and THEN informs the master of the split via zk nodes in
      // 'unassigned' dir.  Currently the RS puts ephemeral nodes into zk so if
      // the regionserver dies, these nodes do not stick around and this server
      // shutdown processing does fixup (see the fixupDaughters method below).
      // If we wanted to skip the hbase:meta scan, we'd have to change at least the
      // final SPLIT message to be permanent in zk so in here we'd know a SPLIT
      // completed (zk is updated after edits to hbase:meta have gone in).  See
      // {@link SplitTransaction}.  We'd also have to be figure another way for
      // doing the below hbase:meta daughters fixup.
      Set<HRegionInfo> hris = null;
      while (!this.server.isStopped()) {
        try {
          this.server.getCatalogTracker().waitForMeta();
          // Skip getting user regions if the server is stopped.
          if (!this.server.isStopped()) {
            hris = MetaReader.getServerUserRegions(this.server.getCatalogTracker(),
              this.serverName).keySet();
          }
          break;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        } catch (IOException ioe) {
          LOG.info("Received exception accessing hbase:meta during server shutdown of " +
            serverName + ", retrying hbase:meta read", ioe);
        }
      }
      if (this.server.isStopped()) {
        throw new IOException("Server is stopped");
      }

      // delayed to set recovery mode based on configuration only after all outstanding splitlogtask
      // drained
      this.services.getMasterFileSystem().setLogRecoveryMode();
      boolean distributedLogReplay = 
        (this.services.getMasterFileSystem().getLogRecoveryMode() == RecoveryMode.LOG_REPLAY);

      try {
        if (this.shouldSplitHlog) {
          if (distributedLogReplay) {
            LOG.info("Mark regions in recovery for crashed server " + serverName +
              " before assignment; regions=" + hris);
            MasterFileSystem mfs = this.services.getMasterFileSystem();
            mfs.prepareLogReplay(serverName, hris);
          } else {
            LOG.info("Splitting logs for " + serverName +
              " before assignment; region count=" + (hris == null ? 0 : hris.size()));
            this.services.getMasterFileSystem().splitLog(serverName);
          }
          am.getRegionStates().logSplit(serverName);
        } else {
          LOG.info("Skipping log splitting for " + serverName);
        }
      } catch (IOException ioe) {
        resubmit(serverName, ioe);
      }

      // Clean out anything in regions in transition.  Being conservative and
      // doing after log splitting.  Could do some states before -- OPENING?
      // OFFLINE? -- and then others after like CLOSING that depend on log
      // splitting.
      List<HRegionInfo> regionsInTransition = am.processServerShutdown(serverName);
      LOG.info("Reassigning " + ((hris == null)? 0: hris.size()) +
        " region(s) that " + (serverName == null? "null": serverName)  +
        " was carrying (and " + regionsInTransition.size() +
        " regions(s) that were opening on this server)");

      List<HRegionInfo> toAssignRegions = new ArrayList<HRegionInfo>();
      toAssignRegions.addAll(regionsInTransition);

      // Iterate regions that were on this server and assign them
      if (hris != null && !hris.isEmpty()) {
        RegionStates regionStates = am.getRegionStates();
        for (HRegionInfo hri: hris) {
          if (regionsInTransition.contains(hri)) {
            continue;
          }
          String encodedName = hri.getEncodedName();
          Lock lock = am.acquireRegionLock(encodedName);
          try {
            RegionState rit = regionStates.getRegionTransitionState(hri);
            if (processDeadRegion(hri, am, server.getCatalogTracker())) {
              ServerName addressFromAM = regionStates.getRegionServerOfRegion(hri);
              if (addressFromAM != null && !addressFromAM.equals(this.serverName)) {
                // If this region is in transition on the dead server, it must be
                // opening or pending_open, which should have been covered by AM#processServerShutdown
                LOG.info("Skip assigning region " + hri.getRegionNameAsString()
                  + " because it has been opened in " + addressFromAM.getServerName());
                continue;
              }
              if (rit != null) {
                if (rit.getServerName() != null && !rit.isOnServer(serverName)) {
                  // Skip regions that are in transition on other server
                  LOG.info("Skip assigning region in transition on other server" + rit);
                  continue;
                }
                try{
                  //clean zk node
                  LOG.info("Reassigning region with rs = " + rit + " and deleting zk node if exists");
                  ZKAssign.deleteNodeFailSilent(services.getZooKeeper(), hri);
                  regionStates.updateRegionState(hri, State.OFFLINE);
                } catch (KeeperException ke) {
                  this.server.abort("Unexpected ZK exception deleting unassigned node " + hri, ke);
                  return;
                }
              } else if (regionStates.isRegionInState(
                  hri, State.SPLITTING_NEW, State.MERGING_NEW)) {
                regionStates.updateRegionState(hri, State.OFFLINE);
              }
              toAssignRegions.add(hri);
            } else if (rit != null) {
              if ((rit.isPendingCloseOrClosing() || rit.isOffline())
                  && am.getZKTable().isDisablingOrDisabledTable(hri.getTable())) {
                // If the table was partially disabled and the RS went down, we should clear the RIT
                // and remove the node for the region.
                // The rit that we use may be stale in case the table was in DISABLING state
                // but though we did assign we will not be clearing the znode in CLOSING state.
                // Doing this will have no harm. See HBASE-5927
                regionStates.updateRegionState(hri, State.OFFLINE);
                am.deleteClosingOrClosedNode(hri, rit.getServerName());
                am.offlineDisabledRegion(hri);
              } else {
                LOG.warn("THIS SHOULD NOT HAPPEN: unexpected region in transition "
                  + rit + " not to be assigned by SSH of server " + serverName);
              }
            }
          } finally {
            lock.unlock();
          }
        }
      }

      try {
        am.assign(toAssignRegions);
      } catch (InterruptedException ie) {
        LOG.error("Caught " + ie + " during round-robin assignment");
        throw (InterruptedIOException)new InterruptedIOException().initCause(ie);
      }

      if (this.shouldSplitHlog && distributedLogReplay) {
        // wait for region assignment completes
        for (HRegionInfo hri : toAssignRegions) {
          try {
            if (!am.waitOnRegionToClearRegionsInTransition(hri, regionAssignmentWaitTimeout)) {
              // Wait here is to avoid log replay hits current dead server and incur a RPC timeout
              // when replay happens before region assignment completes.
              LOG.warn("Region " + hri.getEncodedName()
                  + " didn't complete assignment in time");
            }
          } catch (InterruptedException ie) {
            throw new InterruptedIOException("Caught " + ie
                + " during waitOnRegionToClearRegionsInTransition");
          }
        }
        // submit logReplay work
        this.services.getExecutorService().submit(
          new LogReplayHandler(this.server, this.services, this.deadServers, this.serverName));
        hasLogReplayWork = true;
      }
    } finally {
      this.deadServers.finish(serverName);
    }

    if (!hasLogReplayWork) {
      LOG.info("Finished processing of shutdown of " + serverName);
    }
  }

  private void resubmit(final ServerName serverName, IOException ex) throws IOException {
    // typecast to SSH so that we make sure that it is the SSH instance that
    // gets submitted as opposed to MSSH or some other derived instance of SSH
    this.services.getExecutorService().submit((ServerShutdownHandler) this);
    this.deadServers.add(serverName);
    throw new IOException("failed log splitting for " + serverName + ", will retry", ex);
  }

  /**
   * Process a dead region from a dead RS. Checks if the region is disabled or
   * disabling or if the region has a partially completed split.
   * @param hri
   * @param assignmentManager
   * @param catalogTracker
   * @return Returns true if specified region should be assigned, false if not.
   * @throws IOException
   */
  public static boolean processDeadRegion(HRegionInfo hri,
      AssignmentManager assignmentManager, CatalogTracker catalogTracker)
  throws IOException {
    boolean tablePresent = assignmentManager.getZKTable().isTablePresent(hri.getTable());
    if (!tablePresent) {
      LOG.info("The table " + hri.getTable()
          + " was deleted.  Hence not proceeding.");
      return false;
    }
    // If table is not disabled but the region is offlined,
    boolean disabled = assignmentManager.getZKTable().isDisabledTable(hri.getTable());
    if (disabled){
      LOG.info("The table " + hri.getTable()
          + " was disabled.  Hence not proceeding.");
      return false;
    }
    if (hri.isOffline() && hri.isSplit()) {
      //HBASE-7721: Split parent and daughters are inserted into hbase:meta as an atomic operation.
      //If the meta scanner saw the parent split, then it should see the daughters as assigned
      //to the dead server. We don't have to do anything.
      return false;
    }
    boolean disabling = assignmentManager.getZKTable().isDisablingTable(hri.getTable());
    if (disabling) {
      LOG.info("The table " + hri.getTable()
          + " is disabled.  Hence not assigning region" + hri.getEncodedName());
      return false;
    }
    return true;
  }
}
