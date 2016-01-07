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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.master.DeadServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Process server shutdown.
 * Server-to-handle must be already in the deadservers lists.  See
 * {@link ServerManager#expireServer(ServerName)}
 */
public class ServerShutdownHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(ServerShutdownHandler.class);
  protected final ServerName serverName;
  protected final MasterServices services;
  protected final DeadServer deadServers;
  protected final boolean shouldSplitHlog; // whether to split HLog or not

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
    if (!this.deadServers.contains(this.serverName)) {
      LOG.warn(this.serverName + " is NOT in deadservers; it should be!");
    }
    this.shouldSplitHlog = shouldSplitHlog;
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
   * @return True if the server we are processing was carrying <code>-ROOT-</code>
   */
  boolean isCarryingRoot() {
    return false;
  }

  /**
   * @return True if the server we are processing was carrying <code>.META.</code>
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
    final ServerName serverName = this.serverName;
    try {
      if (this.server.isStopped()) {
        throw new IOException("Server is stopped");
      }

      try {
        if (this.shouldSplitHlog) {
          LOG.info("Splitting logs for " + serverName);
          this.services.getMasterFileSystem().splitLog(serverName);
        } else {
          LOG.info("Skipping log splitting for " + serverName);
        }
      } catch (IOException ioe) {
        //typecast to SSH so that we make sure that it is the SSH instance that
        //gets submitted as opposed to MSSH or some other derived instance of SSH
        this.services.getExecutorService().submit((ServerShutdownHandler)this);
        this.deadServers.add(serverName);
        throw new IOException("failed log splitting for " +
          serverName + ", will retry", ioe);
      }

      // We don't want worker thread in the MetaServerShutdownHandler
      // executor pool to block by waiting availability of -ROOT-
      // and .META. server. Otherwise, it could run into the following issue:
      // 1. The current MetaServerShutdownHandler instance For RS1 waits for the .META.
      //    to come online.
      // 2. The newly assigned .META. region server RS2 was shutdown right after
      //    it opens the .META. region. So the MetaServerShutdownHandler
      //    instance For RS1 will still be blocked.
      // 3. The new instance of MetaServerShutdownHandler for RS2 is queued.
      // 4. The newly assigned .META. region server RS3 was shutdown right after
      //    it opens the .META. region. So the MetaServerShutdownHandler
      //    instance For RS1 and RS2 will still be blocked.
      // 5. The new instance of MetaServerShutdownHandler for RS3 is queued.
      // 6. Repeat until we run out of MetaServerShutdownHandler worker threads
      // The solution here is to resubmit a ServerShutdownHandler request to process
      // user regions on that server so that MetaServerShutdownHandler
      // executor pool is always available.
      if (isCarryingRoot() || isCarryingMeta()) { // -ROOT- or .META.
        this.services.getExecutorService().submit(new ServerShutdownHandler(
          this.server, this.services, this.deadServers, serverName, false));
        this.deadServers.add(serverName);
        return;
      }


      // Wait on meta to come online; we need it to progress.
      // TODO: Best way to hold strictly here?  We should build this retry logic
      // into the MetaReader operations themselves.
      // TODO: Is the reading of .META. necessary when the Master has state of
      // cluster in its head?  It should be possible to do without reading .META.
      // in all but one case. On split, the RS updates the .META.
      // table and THEN informs the master of the split via zk nodes in
      // 'unassigned' dir.  Currently the RS puts ephemeral nodes into zk so if
      // the regionserver dies, these nodes do not stick around and this server
      // shutdown processing does fixup (see the fixupDaughters method below).
      // If we wanted to skip the .META. scan, we'd have to change at least the
      // final SPLIT message to be permanent in zk so in here we'd know a SPLIT
      // completed (zk is updated after edits to .META. have gone in).  See
      // {@link SplitTransaction}.  We'd also have to be figure another way for
      // doing the below .META. daughters fixup.
      NavigableMap<HRegionInfo, Result> hris = null;
      while (!this.server.isStopped()) {
        try {
          this.server.getCatalogTracker().waitForMeta();
          // Skip getting user regions if the server is stopped.
          if (!this.server.isStopped()) {
            hris = MetaReader.getServerUserRegions(this.server.getCatalogTracker(),
                this.serverName);
          }
          break;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted", e);
        } catch (IOException ioe) {
          LOG.info("Received exception accessing META during server shutdown of " +
              serverName + ", retrying META read", ioe);
        }
      }

      // Returns set of regions that had regionplans against the downed server and a list of
      // the intersection of regions-in-transition and regions that were on the server that died.
      Pair<Set<HRegionInfo>, List<RegionState>> p = this.services.getAssignmentManager()
          .processServerShutdown(this.serverName);
      Set<HRegionInfo> ritsGoingToServer = p.getFirst();
      List<RegionState> ritsOnServer = p.getSecond();

      List<HRegionInfo> regionsToAssign = getRegionsToAssign(hris, ritsOnServer, ritsGoingToServer);
      for (HRegionInfo hri : ritsGoingToServer) {
        if (!this.services.getAssignmentManager().isRegionAssigned(hri)) {
          if (!regionsToAssign.contains(hri)) {
            regionsToAssign.add(hri);
            RegionState rit =
                services.getAssignmentManager().getRegionsInTransition().get(hri.getEncodedName());
            removeRITsOfRregionInDisablingOrDisabledTables(regionsToAssign, rit,
              services.getAssignmentManager(), hri);
          }
        }
      }

      // re-assign regions
      for (HRegionInfo hri : regionsToAssign) {
        this.services.getAssignmentManager().assign(hri, true);
      }
      LOG.info(regionsToAssign.size() + " regions which were planned to open on " + this.serverName
          + " have been re-assigned.");
    } finally {
      this.deadServers.finish(serverName);
    }
    LOG.info("Finished processing of shutdown of " + serverName);
  }

  /**
   * Figure what to assign from the dead server considering state of RIT and whats up in .META.
   * @param metaHRIs Regions that .META. says were assigned to the dead server
   * @param ritsOnServer Regions that were in transition, and on the dead server.
   * @param ritsGoingToServer Regions that were in transition to the dead server.
   * @return List of regions to assign or null if aborting.
   * @throws IOException
   */
  private List<HRegionInfo> getRegionsToAssign(final NavigableMap<HRegionInfo, Result> metaHRIs,
      final List<RegionState> ritsOnServer, Set<HRegionInfo> ritsGoingToServer) throws IOException {
    List<HRegionInfo> toAssign = new ArrayList<HRegionInfo>();
    // If no regions on the server, then nothing to assign (Regions that were currently being
    // assigned will be retried over in the AM#assign method).
    if (metaHRIs == null || metaHRIs.isEmpty()) return toAssign;
    // Remove regions that we do not want to reassign such as regions that are
    // OFFLINE. If region is OFFLINE against this server, its probably being assigned over
    // in the single region assign method in AM; do not assign it here too. TODO: VERIFY!!!
    // TODO: Currently OFFLINE is too messy. Its done on single assign but bulk done when bulk
    // assigning and then there is special handling when master joins a cluster.
    //
    // If split, the zk callback will have offlined. Daughters will be in the
    // list of hris we got from scanning the .META. These should be reassigned. Not the parent.
    for (RegionState rs : ritsOnServer) {
      if (!rs.isClosing() && !rs.isPendingClose() && !rs.isSplitting()) {
        LOG.debug("Removed " + rs.getRegion().getRegionNameAsString()
            + " from list of regions to assign because region state: " + rs.getState());
        metaHRIs.remove(rs.getRegion());
      }
    }

    AssignmentManager assignmentManager = this.services.getAssignmentManager();
    for (Map.Entry<HRegionInfo, Result> e : metaHRIs.entrySet()) {
      RegionState rit =
          assignmentManager.getRegionsInTransition().get(e.getKey().getEncodedName());

      if (processDeadRegion(e.getKey(), e.getValue(), assignmentManager,
        this.server.getCatalogTracker())) {
        ServerName addressFromAM = assignmentManager.getRegionServerOfRegion(e.getKey());
        if (rit != null && !rit.isClosing() && !rit.isPendingClose() && !rit.isSplitting()
            && !ritsGoingToServer.contains(e.getKey())) {
          // Skip regions that were in transition unless CLOSING or
          // PENDING_CLOSE
          LOG.info("Skip assigning region " + rit.toString());
        } else if (addressFromAM != null && !addressFromAM.equals(this.serverName)) {
          LOG.debug("Skip assigning region " + e.getKey().getRegionNameAsString()
              + " because it has been opened in " + addressFromAM.getServerName());
          ritsGoingToServer.remove(e.getKey());
        } else {
          if (rit != null) {
            // clean zk node
            try {
              LOG.info("Reassigning region with rs =" + rit + " and deleting zk node if exists");
              ZKAssign.deleteNodeFailSilent(services.getZooKeeper(), e.getKey());
            } catch (KeeperException ke) {
              this.server.abort("Unexpected ZK exception deleting unassigned node " + e.getKey(),
                ke);
              return null;
            }
          }
          toAssign.add(e.getKey());
        }
      } else if (rit != null && (rit.isSplitting() || rit.isSplit())) {
        // This will happen when the RS went down and the call back for the SPLIITING or SPLIT
        // has not yet happened for node Deleted event. In that case if the region was actually
        // split but the RS had gone down before completing the split process then will not try
        // to assign the parent region again. In that case we should make the region offline
        // and also delete the region from RIT.
        HRegionInfo region = rit.getRegion();
        AssignmentManager am = assignmentManager;
        am.regionOffline(region);
        ritsGoingToServer.remove(region);
      }
      // If the table was partially disabled and the RS went down, we should clear the RIT
      // and remove the node for the region. The rit that we use may be stale in case the table
      // was in DISABLING state but though we did assign we will not be clearing the znode in
      // CLOSING state. Doing this will have no harm. The rit can be null if region server went
      // down during master startup. In that case If any znodes' exists for partially disabled 
      // table regions deleting them during startup only. See HBASE-8127. 
      removeRITsOfRregionInDisablingOrDisabledTables(toAssign, rit, assignmentManager, e.getKey());
    }

    return toAssign;
  }

  private void removeRITsOfRregionInDisablingOrDisabledTables(List<HRegionInfo> toAssign,
      RegionState rit, AssignmentManager assignmentManager, HRegionInfo hri) {

    if (!assignmentManager.getZKTable().isDisablingOrDisabledTable(hri.getTableNameAsString())) {
      return;
    }

    // To avoid region assignment if table is in disabling or disabled state.
    toAssign.remove(hri);

    if (rit != null) {
      assignmentManager.deleteNodeAndOfflineRegion(hri);
    }
  }

  /**
   * Process a dead region from a dead RS. Checks if the region is disabled or
   * disabling or if the region has a partially completed split.
   * @param hri
   * @param result
   * @param assignmentManager
   * @param catalogTracker
   * @return Returns true if specified region should be assigned, false if not.
   * @throws IOException
   */
  public static boolean processDeadRegion(HRegionInfo hri, Result result,
      AssignmentManager assignmentManager, CatalogTracker catalogTracker)
  throws IOException {
    boolean tablePresent = assignmentManager.getZKTable().isTablePresent(
        hri.getTableNameAsString());
    if (!tablePresent) {
      LOG.info("The table " + hri.getTableNameAsString()
          + " was deleted.  Hence not proceeding.");
      return false;
    }
    // If table is not disabled but the region is offlined,
    boolean disabled = assignmentManager.getZKTable().isDisabledTable(
        hri.getTableNameAsString());
    if (disabled){
      LOG.info("The table " + hri.getTableNameAsString()
          + " was disabled.  Hence not proceeding.");
      return false;
    }
    if (hri.isOffline() && hri.isSplit()) {
      LOG.debug("Offlined and split region " + hri.getRegionNameAsString() +
        "; checking daughter presence");
      if (MetaReader.getRegion(catalogTracker, hri.getRegionName()) == null) {
        return false;
      }
      fixupDaughters(result, assignmentManager, catalogTracker);
      return false;
    }
    boolean disabling = assignmentManager.getZKTable().isDisablingTable(
        hri.getTableNameAsString());
    if (disabling) {
      LOG.info("The table " + hri.getTableNameAsString()
          + " is disabled.  Hence not assigning region" + hri.getEncodedName());
      return false;
    }
    return true;
  }

  /**
   * Check that daughter regions are up in .META. and if not, add them.
   * @param hris All regions for this server in meta.
   * @param result The contents of the parent row in .META.
   * @return the number of daughters missing and fixed
   * @throws IOException
   */
  public static int fixupDaughters(final Result result,
      final AssignmentManager assignmentManager,
      final CatalogTracker catalogTracker)
  throws IOException {
    int fixedA = fixupDaughter(result, HConstants.SPLITA_QUALIFIER,
      assignmentManager, catalogTracker);
    int fixedB = fixupDaughter(result, HConstants.SPLITB_QUALIFIER,
      assignmentManager, catalogTracker);
    return fixedA + fixedB;
  }

  /**
   * Check individual daughter is up in .META.; fixup if its not.
   * @param result The contents of the parent row in .META.
   * @param qualifier Which daughter to check for.
   * @return 1 if the daughter is missing and fixed. Otherwise 0
   * @throws IOException
   */
  static int fixupDaughter(final Result result, final byte [] qualifier,
      final AssignmentManager assignmentManager,
      final CatalogTracker catalogTracker)
  throws IOException {
    HRegionInfo daughter =
      MetaReader.parseHRegionInfoFromCatalogResult(result, qualifier);
    if (daughter == null) return 0;
    if (isDaughterMissing(catalogTracker, daughter)) {
      LOG.info("Fixup; missing daughter " + daughter.getRegionNameAsString());
      MetaEditor.addDaughter(catalogTracker, daughter, null);

      // TODO: Log WARN if the regiondir does not exist in the fs.  If its not
      // there then something wonky about the split -- things will keep going
      // but could be missing references to parent region.

      // And assign it.
      assignmentManager.assign(daughter, true);
      return 1;
    } else {
      LOG.debug("Daughter " + daughter.getRegionNameAsString() + " present");
    }
    return 0;
  }

  /**
   * Look for presence of the daughter OR of a split of the daughter in .META.
   * Daughter could have been split over on regionserver before a run of the
   * catalogJanitor had chance to clear reference from parent.
   * @param daughter Daughter region to search for.
   * @throws IOException 
   */
  private static boolean isDaughterMissing(final CatalogTracker catalogTracker,
      final HRegionInfo daughter) throws IOException {
    FindDaughterVisitor visitor = new FindDaughterVisitor(daughter);
    // Start the scan at what should be the daughter's row in the .META.
    // We will either 1., find the daughter or some derivative split of the
    // daughter (will have same table name and start row at least but will sort
    // after because has larger regionid -- the regionid is timestamp of region
    // creation), OR, we will not find anything with same table name and start
    // row.  If the latter, then assume daughter missing and do fixup.
    byte [] startrow = daughter.getRegionName();
    MetaReader.fullScan(catalogTracker, visitor, startrow);
    return !visitor.foundDaughter();
  }

  /**
   * Looks for daughter.  Sets a flag if daughter or some progeny of daughter
   * is found up in <code>.META.</code>.
   */
  static class FindDaughterVisitor implements MetaReader.Visitor {
    private final HRegionInfo daughter;
    private boolean found = false;

    FindDaughterVisitor(final HRegionInfo daughter) {
      this.daughter = daughter;
    }

    /**
     * @return True if we found a daughter region during our visiting.
     */
    boolean foundDaughter() {
      return this.found;
    }

    @Override
    public boolean visit(Result r) throws IOException {
      HRegionInfo hri =
        MetaReader.parseHRegionInfoFromCatalogResult(r, HConstants.REGIONINFO_QUALIFIER);
      if (hri == null) {
        LOG.warn("No serialized HRegionInfo in " + r);
        return true;
      }
      byte [] value = r.getValue(HConstants.CATALOG_FAMILY,
          HConstants.SERVER_QUALIFIER);
      // See if daughter is assigned to some server
      if (value == null) return false;

      // Now see if we have gone beyond the daughter's startrow.
      if (!Bytes.equals(daughter.getTableName(),
          hri.getTableName())) {
        // We fell into another table.  Stop scanning.
        return false;
      }
      // If our start rows do not compare, move on.
      if (!Bytes.equals(daughter.getStartKey(), hri.getStartKey())) {
        return false;
      }
      // Else, table name and start rows compare.  It means that the daughter
      // or some derivative split of the daughter is up in .META.  Daughter
      // exists.
      this.found = true;
      return false;
    }
  }
}
