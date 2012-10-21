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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
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
import org.apache.hadoop.hbase.master.DeadServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
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
  private final ServerName serverName;
  private final MasterServices services;
  private final DeadServer deadServers;
  private final boolean shouldSplitHlog; // whether to split HLog or not

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
   * Before assign the ROOT region, ensure it haven't
   *  been assigned by other place
   * <p>
   * Under some scenarios, the ROOT region can be opened twice, so it seemed online
   * in two regionserver at the same time.
   * If the ROOT region has been assigned, so the operation can be canceled.
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  private void verifyAndAssignRoot()
  throws InterruptedException, IOException, KeeperException {
    long timeout = this.server.getConfiguration().
      getLong("hbase.catalog.verification.timeout", 1000);
    if (!this.server.getCatalogTracker().verifyRootRegionLocation(timeout)) {
      this.services.getAssignmentManager().assignRoot();
    }
  }

  /**
   * Failed many times, shutdown processing
   * @throws IOException
   */
  private void verifyAndAssignRootWithRetries() throws IOException {
    int iTimes = this.server.getConfiguration().getInt(
        "hbase.catalog.verification.retries", 10);

    long waitTime = this.server.getConfiguration().getLong(
        "hbase.catalog.verification.timeout", 1000);

    int iFlag = 0;
    while (true) {
      try {
        verifyAndAssignRoot();
        break;
      } catch (KeeperException e) {
        this.server.abort("In server shutdown processing, assigning root", e);
        throw new IOException("Aborting", e);
      } catch (Exception e) {
        if (iFlag >= iTimes) {
          this.server.abort("verifyAndAssignRoot failed after" + iTimes
              + " times retries, aborting", e);
          throw new IOException("Aborting", e);
        }
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e1) {
          LOG.warn("Interrupted when is the thread sleep", e1);
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted", e1);
        }
        iFlag++;
      }
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
      try {
        if (this.shouldSplitHlog) {
          LOG.info("Splitting logs for " + serverName);
          this.services.getMasterFileSystem().splitLog(serverName);
        } else {
          LOG.info("Skipping log splitting for " + serverName);
        }
      } catch (IOException ioe) {
        this.services.getExecutorService().submit(this);
        this.deadServers.add(serverName);
        throw new IOException("failed log splitting for " +
          serverName + ", will retry", ioe);
      }

      // Assign root and meta if we were carrying them.
      if (isCarryingRoot()) { // -ROOT-
        LOG.info("Server " + serverName +
            " was carrying ROOT. Trying to assign.");
        this.services.getAssignmentManager().
          regionOffline(HRegionInfo.ROOT_REGIONINFO);
        verifyAndAssignRootWithRetries();
      }

      // Carrying meta?
      if (isCarryingMeta()) {
        LOG.info("Server " + serverName +
          " was carrying META. Trying to assign.");
        this.services.getAssignmentManager().
          regionOffline(HRegionInfo.FIRST_META_REGIONINFO);
        this.services.getAssignmentManager().assignMeta();
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
      //
      // If AssignmentManager hasn't finished rebuilding user regions,
      // we are not ready to assign dead regions either. So we re-queue up
      // the dead server for further processing too.
      if (isCarryingRoot() || isCarryingMeta() // -ROOT- or .META.
          || !services.getAssignmentManager().isFailoverCleanupDone()) {
        this.services.getServerManager().processDeadServer(serverName);
        return;
      }

      // Clean out anything in regions in transition.  Being conservative and
      // doing after log splitting.  Could do some states before -- OPENING?
      // OFFLINE? -- and then others after like CLOSING that depend on log
      // splitting.
      List<RegionState> regionsInTransition =
        this.services.getAssignmentManager().
          processServerShutdown(this.serverName);

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
          hris = MetaReader.getServerUserRegions(this.server.getCatalogTracker(),
            this.serverName);
          break;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted", e);
        } catch (IOException ioe) {
          LOG.info("Received exception accessing META during server shutdown of " +
              serverName + ", retrying META read", ioe);
        }
      }
      if (this.server.isStopped()) {
        throw new IOException("Server is stopped");
      }

      // Skip regions that were in transition unless CLOSING or PENDING_CLOSE
      for (RegionState rit : regionsInTransition) {
        if (!rit.isClosing() && !rit.isPendingClose() && !rit.isSplitting()) {
          LOG.debug("Removed " + rit.getRegion().getRegionNameAsString() +
          " from list of regions to assign because in RIT; region state: " +
          rit.getState());
          if (hris != null) hris.remove(rit.getRegion());
        }
      }

      assert regionsInTransition != null;
      LOG.info("Reassigning " + ((hris == null)? 0: hris.size()) +
        " region(s) that " + (serverName == null? "null": serverName)  +
        " was carrying (skipping " +
        regionsInTransition.size() +
        " regions(s) that are already in transition)");

      // Iterate regions that were on this server and assign them
      if (hris != null) {
        List<HRegionInfo> toAssignRegions = new ArrayList<HRegionInfo>();
        for (Map.Entry<HRegionInfo, Result> e: hris.entrySet()) {
          RegionState rit = services.getAssignmentManager()
            .getRegionStates().getRegionTransitionState(e.getKey());
          if (processDeadRegion(e.getKey(), e.getValue(),
              this.services.getAssignmentManager(),
              this.server.getCatalogTracker())) {
            ServerName addressFromAM = this.services.getAssignmentManager()
              .getRegionStates().getRegionServerOfRegion(e.getKey());
            if (rit != null && !rit.isClosing() && !rit.isPendingClose() && !rit.isSplitting()) {
              // Skip regions that were in transition unless CLOSING or
              // PENDING_CLOSE
              LOG.info("Skip assigning region " + rit.toString());
            } else if (addressFromAM != null
                && !addressFromAM.equals(this.serverName)) {
              LOG.debug("Skip assigning region "
                    + e.getKey().getRegionNameAsString()
                    + " because it has been opened in "
                    + addressFromAM.getServerName());
              } else {
                if (rit != null) {
                  //clean zk node
                  try{
                    LOG.info("Reassigning region with rs =" + rit + " and deleting zk node if exists");
                    ZKAssign.deleteNodeFailSilent(services.getZooKeeper(), e.getKey());
                  }catch (KeeperException ke) {
                    this.server.abort("Unexpected ZK exception deleting unassigned node " + e.getKey(), ke);
                    return;
                  }
                }
                toAssignRegions.add(e.getKey());
              }
          } else if (rit != null && (rit.isSplitting() || rit.isSplit())) {
            // This will happen when the RS went down and the call back for the SPLIITING or SPLIT
            // has not yet happened for node Deleted event. In that case if the region was actually
            // split
            // but the RS had gone down before completing the split process then will not try to
            // assign the parent region again. In that case we should make the region offline and
            // also delete the region from RIT.
            HRegionInfo region = rit.getRegion();
            AssignmentManager am = this.services.getAssignmentManager();
            am.regionOffline(region);
          }
          // If the table was partially disabled and the RS went down, we should clear the RIT
          // and remove the node for the region.
          // The rit that we use may be stale in case the table was in DISABLING state
          // but though we did assign we will not be clearing the znode in CLOSING state.
          // Doing this will have no harm. See HBASE-5927
          if (rit != null
              && (rit.isClosing() || rit.isPendingClose())
              && this.services.getAssignmentManager().getZKTable()
                  .isDisablingOrDisabledTable(rit.getRegion().getTableNameAsString())) {
            HRegionInfo hri = rit.getRegion();
            AssignmentManager am = this.services.getAssignmentManager();
            am.deleteClosingOrClosedNode(hri);
            am.regionOffline(hri);
            // To avoid region assignment if table is in disabling or disabled state.
            toAssignRegions.remove(hri);
          }
        }
        try {
          this.services.getAssignmentManager().assign(toAssignRegions);
        } catch (InterruptedException ie) {
          LOG.error("Caught " + ie + " during round-robin assignment");
          throw new IOException(ie);
        }
      }
    } finally {
      this.deadServers.finish(serverName);
    }
    LOG.info("Finished processing of shutdown of " + serverName);
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
    PairOfSameType<HRegionInfo> daughters = HRegionInfo.getDaughterRegions(result);
    int fixedA = fixupDaughter(result, daughters.getFirst(),
      assignmentManager, catalogTracker);
    int fixedB = fixupDaughter(result, daughters.getSecond(),
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
  static int fixupDaughter(final Result result, HRegionInfo daughter,
      final AssignmentManager assignmentManager,
      final CatalogTracker catalogTracker)
  throws IOException {
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
        HRegionInfo.getHRegionInfo(r);
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
