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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.RegionManager.RegionState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;

/**
 * Instantiated when a server's lease has expired, meaning it has crashed.
 * The region server's log file needs to be split up for each region it was
 * serving, and the regions need to get reassigned.
 */
class ProcessServerShutdown extends RegionServerOperation {
  // Server name made of the concatenation of hostname, port and startcode
  // formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
  private final String deadServer;
  private long deadServerStartCode;
  private boolean isRootServer;
  private List<MetaRegion> metaRegions, metaRegionsUnassigned;
  private boolean rootRescanned;
  private HServerAddress deadServerAddress;
  private final long expiredSince;

  public enum LogSplitResult {
    NOT_RUNNING,
    RUNNING,
    SUCCESS,
    FAILED
  };

  private volatile LogSplitResult logSplitResult = LogSplitResult.NOT_RUNNING;
  private Set<String> successfulMetaScans;

  private static class ToDoEntry {
    boolean regionOffline;
    final HRegionInfo info;

    ToDoEntry(final HRegionInfo info) {
      this.regionOffline = false;
      this.info = info;
    }
  }

  /**
   * @param master
   * @param serverInfo
   */
  public ProcessServerShutdown(HMaster master, HServerInfo serverInfo, long expiredSince) {
    super(master, serverInfo.getServerName());
    this.deadServer = serverInfo.getServerName();
    this.deadServerAddress = serverInfo.getServerAddress();
    this.deadServerStartCode = serverInfo.getStartCode();
    this.rootRescanned = false;
    this.successfulMetaScans = new HashSet<String>();
    // check to see if I am responsible for either ROOT or any of the META tables.

    this.expiredSince = expiredSince;

    // TODO Why do we do this now instead of at processing time?
    closeMetaRegions();
  }

  private void closeMetaRegions() {
    this.isRootServer =
      this.master.getRegionManager().isRootServer(this.deadServerAddress) ||
      this.master.getRegionManager().isRootInTransitionOnThisServer(deadServer);
    if (this.isRootServer) {
      this.master.getRegionManager().unsetRootRegion();
    }
    List<byte[]> metaStarts =
      this.master.getRegionManager().listMetaRegionsForServer(deadServerAddress);

    this.metaRegions = new ArrayList<MetaRegion>();
    this.metaRegionsUnassigned = new ArrayList<MetaRegion>();
    for (byte [] startKey: metaStarts) {
      LOG.debug(this.toString() + " setting RegionManager.offlineMetaReginWithStartKey : "
          + Bytes.toString(startKey));
      MetaRegion r = master.getRegionManager().offlineMetaRegionWithStartKey(startKey);
      this.metaRegions.add(r);
    }

    //HBASE-1928: Check whether this server has been transitioning the META table
    HRegionInfo metaServerRegionInfo = master.getRegionManager().getMetaServerRegionInfo (deadServer);
    if (metaServerRegionInfo != null) {
      metaRegions.add (new MetaRegion (deadServerAddress, metaServerRegionInfo));
    }
  }

  /**
   * @return Name of server we are processing.
   */
  public HServerAddress getDeadServerAddress() {
    return this.deadServerAddress;
  }

  private void closeRegionsInTransition() {
    Map<String, RegionState> inTransition =
      master.getRegionManager().getRegionsInTransitionOnServer(deadServer);
    for (Map.Entry<String, RegionState> entry : inTransition.entrySet()) {
      String regionName = entry.getKey();
      RegionState state = entry.getValue();

      LOG.info("Region " + regionName + " was in transition " +
          state + " on dead server " + deadServer + " - marking unassigned");
      LOG.info(this.toString() + " setting to unassigned: " + state.getRegionInfo().toString());
      master.getRegionManager().setUnassigned(state.getRegionInfo(), true);
    }
  }

  @Override
  public String toString() {
    return "ProcessServerShutdown of " + this.deadServer;
  }

  /** Finds regions that the dead region server was serving
   */
  protected boolean scanMetaRegion(HRegionInterface server, Scan scan,
    byte [] regionName)
  throws IOException {
    long scannerId = server.openScanner(regionName, scan);

    int rows = scan.getCaching();
    // The default caching if not set for scans is -1. Handle it
    if (rows < 1) rows = 1;

    List<ToDoEntry> toDoList = new ArrayList<ToDoEntry>();
    Set<HRegionInfo> regions = new HashSet<HRegionInfo>();
    List<byte []> emptyRows = new ArrayList<byte []>();
    long t0, t1, t2;
    try {
      t0 = System.currentTimeMillis();
      while (true) {
        Result[] values = null;
        try {
          values = server.next(scannerId, rows);
        } catch (IOException e) {
          LOG.error("Shutdown scanning of meta region",
            RemoteExceptionHandler.checkIOException(e));
          return false;
        }
        if (values == null || values.length == 0) {
          break;
        }
        for(Result value: values) {
          if (value.size() == 0) continue;
          byte [] row = value.getRow();
          // Check server name.  If null, skip (We used to consider it was on
          // shutdown server but that would mean that we'd reassign regions that
          // were already out being assigned, ones that were product of a split
          // that happened while the shutdown was being processed).
          String serverAddress = BaseScanner.getServerAddress(value);
          long startCode = BaseScanner.getStartCode(value);

          String serverName = null;
          if (serverAddress != null && serverAddress.length() > 0) {
            serverName = HServerInfo.getServerName(serverAddress, startCode);
          }
          if (serverName == null || !deadServer.equals(serverName)) {
            // This isn't the server you're looking for - move along
            continue;
          }

          if (LOG.isDebugEnabled() && row != null) {
            LOG.debug("Shutdown scanner for " + serverName + " processing " +
              Bytes.toString(row));
          }

          HRegionInfo info = master.getHRegionInfo(row, value);
          if (info == null) {
            emptyRows.add(row);
            continue;
          }

          ToDoEntry todo = new ToDoEntry(info);
          toDoList.add(todo);
        }
      }
    } finally {
      t1 = System.currentTimeMillis();
      if (scannerId != -1L) {
        try {
          server.close(scannerId);
        } catch (IOException e) {
          LOG.error("Closing scanner",
            RemoteExceptionHandler.checkIOException(e));
        }
      }
    }
    LOG.debug("Took " + (t1 - t0) + " ms. to fetch entries during scan meta region for " + this);

    // Scan complete. Remove any rows which had empty HRegionInfos

    if (emptyRows.size() > 0) {
      LOG.warn("Found " + emptyRows.size() +
        " rows with empty HRegionInfo while scanning meta region " +
        Bytes.toString(regionName) + " for " + this);
      master.deleteEmptyMetaRows(server, regionName, emptyRows);
    }

    t0 = System.currentTimeMillis();
    synchronized (master.getRegionManager()) {
      t1 = System.currentTimeMillis();
    // Update server in root/meta entries
      for (ToDoEntry e: toDoList) {
        if (e.info.isMetaTable() && !this.metaRegionsUnassigned.contains(
              new MetaRegion(this.deadServerAddress, e.info))) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("removing meta region " +
                Bytes.toString(e.info.getRegionName()) +
            " from online meta regions");
          }
          LOG.debug(this.toString() + " setting RegionManager.offlineMetaReginWithStartKey : "
              + Bytes.toString(e.info.getStartKey()));
          master.getRegionManager().offlineMetaRegionWithStartKey(e.info.getStartKey());
        }

        if (master.getRegionManager().isOfflined(e.info.getRegionNameAsString()) ||
            e.info.isOffline()) {
          master.getRegionManager().removeRegion(e.info);
          // Mark region offline
          if (!e.info.isOffline()) {
            e.regionOffline = true;
          }
        } else {
          if (!e.info.isOffline() && !e.info.isSplit()) {
            // Get region reassigned
            regions.add(e.info);
          }
        }
      }
    }
    t2 = System.currentTimeMillis();
    if (LOG.isDebugEnabled())
      LOG.debug("Took " + this.toString() + " " + (t2 - t0)
          + " ms. to update RegionManager. And,"
        + (t1 - t0) + " ms. to get the lock.");

    // Let us not do this while we hold the lock on the regionManager.
    t0 = System.currentTimeMillis();
    for (ToDoEntry e: toDoList) {
      if (e.regionOffline) {
        LOG.debug(this.toString() + " setting offlineRegionInMETA : " + e.info.toString());
        HRegion.offlineRegionInMETA(server, regionName, e.info);
      }
    }
    t1 = System.currentTimeMillis();

    // Get regions reassigned
    for (HRegionInfo info: regions) {
      if (info.isMetaTable()) {
        MetaRegion mr = new MetaRegion(this.deadServerAddress, info);
        boolean skip = this.metaRegionsUnassigned.contains(mr);

        LOG.debug(this.toString() +
            (skip? "skipping set " : "setting ") + " unassigned: " + info.toString());
        if (skip)
          continue;

        this.setRegionUnassigned(info, false);
        this.metaRegionsUnassigned.add(mr);
      }
      else {
        LOG.debug(this.toString() + "setting " + " unassigned: " + info.toString());
        this.setRegionUnassigned(info, false);
      }
    }
    t2 = System.currentTimeMillis();

    if (LOG.isDebugEnabled())
      LOG.debug("Took " + this.toString() + " "
          + (t1 - t0 ) + " ms. to mark regions offlineInMeta"
          + (t2 - t1) + " ms. to set " + regions.size() + " regions unassigned");
    return true;
  }

  private class ScanRootRegion extends RetryableMetaOperation<Boolean> {
    ScanRootRegion(MetaRegion m, HMaster master) {
      super(m, master);
    }

    public Boolean call() throws IOException {
      if (LOG.isDebugEnabled()) {
        HServerAddress addr = master.getRegionManager().getRootRegionLocation();
        if (addr != null) {
          if (addr.equals(deadServerAddress)) {
            // We should not happen unless the master has restarted recently, because we
            // explicitly call unsetRootRegion() in closeMetaRegions, which is called when
            // ProcessServerShutdown was instantiated.
            // However, in the case of a recovery by ZKClusterStateRecovery, it is possible that
            // the rootRegion was updated after closeMetaRegions() was called. If we let the rootRegion
            // point to a dead server,  the cluster might just block, because all ScanRootRegion calls
            // will continue to fail. Let us fix this, by ensuring that the root gets reassigned.
            if (deadServerStartCode == master.getRegionManager().getRootServerInfo().getStartCode()) {
              LOG.error(ProcessServerShutdown.this.toString() + " unsetting root because it is on the dead server being processed" );
              master.getRegionManager().reassignRootRegion();
              return false;
            } else {
              LOG.info(ProcessServerShutdown.this.toString() + " NOT unsetting root because it is on the dead server, but different start code" );
            }
          }
          LOG.debug(ProcessServerShutdown.this.toString() + " scanning root region on " +
              addr.getBindAddress());
        } else {
          LOG.debug(ProcessServerShutdown.this.toString() + " scanning root, but root is null");
        }
      }
      Scan scan = new Scan();
      scan.addFamily(HConstants.CATALOG_FAMILY);
      scan.setCaching(1000);
      scan.setCacheBlocks(true);
      return scanMetaRegion(server, scan,
          HRegionInfo.ROOT_REGIONINFO.getRegionName());
    }
  }

  private class ScanMetaRegions extends RetryableMetaOperation<Boolean> {
    ScanMetaRegions(MetaRegion m, HMaster master) {
      super(m, master);
    }

    public Boolean call() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug(ProcessServerShutdown.this.toString() + " scanning " +
          Bytes.toString(m.getRegionName()) + " on " + m.getServer());
      }
      Scan scan = new Scan();
      scan.addFamily(HConstants.CATALOG_FAMILY);
      scan.setCaching(1000);
      scan.setCacheBlocks(true);
      return scanMetaRegion(server, scan, m.getRegionName());
    }
  }

  /**
   * Start a new thread to split the log for the dead server.
   * @param deadServer
   */
  public void startSplitDeadServerLog(final String deadServer) {
    this.logSplitResult = LogSplitResult.RUNNING;
    this.master.getLogSplitThreadPool().submit(new Runnable() {
      @Override
      public void run() {
        try {
          master.splitDeadServerLog(deadServer);
          logSplitResult = LogSplitResult.SUCCESS;
        } catch (Exception e) {
          LOG.error(
              "Exception when spliting log for dead server " + deadServer, e);
          logSplitResult = LogSplitResult.FAILED;
        }
      }
    });
  }

  @Override
  protected RegionServerOperationResult process() throws IOException {
    InjectionHandler.processEvent(InjectionEvent.HMASTER_START_PROCESS_DEAD_SERVER);

    switch (this.logSplitResult) {
    case NOT_RUNNING:
      LOG.info("Process server shut down for dead server " + deadServer);
      startSplitDeadServerLog(deadServer);
      return RegionServerOperationResult.OPERATION_DELAYED;

    case RUNNING:
      return RegionServerOperationResult.OPERATION_DELAYED;

    case SUCCESS:
      LOG.info("Succeeded in splitting log for dead server " + deadServer);
      break;

    case FAILED:
      logSplitResult = LogSplitResult.NOT_RUNNING;
      throw new IOException("Failed splitting log for dead server " +
        deadServer);

    default:
      throw new RuntimeException("Invalid split log result: "
          + this.logSplitResult);
    }

    LOG.info("Log split is completed for " + deadServer
        + ", meta reassignment and scanning: "
      + "rootRescanned: " + rootRescanned + ", numberOfMetaRegions: "
      + master.getRegionManager().numMetaRegions()
      + ", onlineMetaRegions.size(): "
      + master.getRegionManager().numOnlineMetaRegions());

    if (this.isRootServer) {
      LOG.info(this.toString() + " reassigning ROOT region");
      master.getRegionManager().reassignRootRegion();
      isRootServer = false;  // prevent double reassignment... heh.
      LOG.debug(this.toString() + " reassigning ROOT done");
    }


    // Why are we un-assigning meta regions here, and not just leave it
    // up to ScanRootRegion to do it for us.
    // Looks like we are worried about the case where there were some
    // meta regions that were transitioning onto the deadServer (but,
    // not yet updated into ROOT). Need to make sure that they get
    // reassigned.
    for (MetaRegion metaRegion : metaRegions) {
      if (metaRegionsUnassigned.contains(metaRegion)) continue;

      LOG.info(this.toString() + " setting to unassigned: " + metaRegion.toString());
      this.setRegionUnassigned(metaRegion.getRegionInfo(), true);
      metaRegionsUnassigned.add(metaRegion);
    }

    if (!rootRescanned) {
      if (!rootAvailable()) {
        // We can't proceed because the root region is not online.
        LOG.debug("Root unavailable -- delaying operation " + this);
        return RegionServerOperationResult.OPERATION_DELAYED;
      }

      // Scan the ROOT region
      LOG.debug(this.toString() + ". Begin rescan Root ");
      Boolean result = new ScanRootRegion(
          new MetaRegion(master.getRegionManager().getRootRegionLocation(),
              HRegionInfo.ROOT_REGIONINFO), this.master).doWithRetries();
      if (result == null || result.booleanValue() == false) {
        LOG.debug("Root scan failed " + this);
        return RegionServerOperationResult.OPERATION_DELAYED;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Process server shutdown scanning root region on " +
          master.getRegionManager().getRootRegionLocation().getBindAddress() +
          " finished " + Thread.currentThread().getName());
      }
      rootRescanned = true;
    }

    LOG.debug(this.toString() + ". Scanning META");
    if (!metaTableAvailable()) {
      // We can't proceed because not all meta regions are online.
      LOG.debug(this.toString() + ". Could not scan meta. Meta Unavailable");
      return RegionServerOperationResult.OPERATION_DELAYED;
    }

    List<MetaRegion> regions = master.getRegionManager().getListOfOnlineMetaRegions();
    for (MetaRegion r: regions) {
      // If we have already scanned successfully, let us skip it
      if (successfulMetaScans.contains(Bytes.toString(r.getRegionName()))) continue;

      if (LOG.isDebugEnabled()) {
        LOG.debug(this.toString() + ". Begin scan meta region " +
          Bytes.toString(r.getRegionName()) + " on " + r.getServer());
      }
      Boolean result = new ScanMetaRegions(r, this.master).doWithRetries();
      if (result == null || result.booleanValue() == false) {
        LOG.debug("Meta scan failed " +
          Bytes.toString(r.getRegionName()) + " on " + r.getServer());
        return RegionServerOperationResult.OPERATION_DELAYED;
      }

      successfulMetaScans.add(Bytes.toString(r.getRegionName()));
      if (LOG.isDebugEnabled()) {
        LOG.debug(this.toString() + ".  finished scanning " +
          Bytes.toString(r.getRegionName()) + " on " + r.getServer());
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(this.toString() + ". Closing regions in transition ");
    }
    closeRegionsInTransition();
    if (LOG.isDebugEnabled()) {
      LOG.debug(this.toString() + ". Removing dead server from the serverManager");
    }
    this.master.getServerManager().removeDeadServer(deadServer);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + this.toString() + " Succeded. "
          + "Removed " + deadServer + " from deadservers Map");
    }
    return RegionServerOperationResult.OPERATION_SUCCEEDED;
  }

  @Override
  protected int getPriority() {
    return 2; // high but not highest priority
  }

  /**
   * For test purpose
   * @return logSplitResult
   */
  public LogSplitResult getLogSplitResult() {
    return this.logSplitResult;
  }

  private void setRegionUnassigned(HRegionInfo info, boolean force) {
    this.master.getServerManager().getRegionChecker().becameClosed(info, this.expiredSince);
    this.master.getRegionManager().setUnassigned(info, force);
  }
}
