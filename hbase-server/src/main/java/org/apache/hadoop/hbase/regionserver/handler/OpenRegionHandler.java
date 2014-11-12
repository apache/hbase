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
package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ConfigUtil;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;
/**
 * Handles opening of a region on a region server.
 * <p>
 * This is executed after receiving an OPEN RPC from the master or client.
 */
@InterfaceAudience.Private
public class OpenRegionHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(OpenRegionHandler.class);

  protected final RegionServerServices rsServices;

  private final HRegionInfo regionInfo;
  private final HTableDescriptor htd;

  private boolean tomActivated;
  private int assignmentTimeout;

  // We get version of our znode at start of open process and monitor it across
  // the total open. We'll fail the open if someone hijacks our znode; we can
  // tell this has happened if version is not as expected.
  private volatile int version = -1;
  //version of the offline node that was set by the master
  private volatile int versionOfOfflineNode = -1;

  private final boolean useZKForAssignment;

  public OpenRegionHandler(final Server server,
      final RegionServerServices rsServices, HRegionInfo regionInfo,
      HTableDescriptor htd) {
    this(server, rsServices, regionInfo, htd, EventType.M_RS_OPEN_REGION, -1);
  }
  public OpenRegionHandler(final Server server,
      final RegionServerServices rsServices, HRegionInfo regionInfo,
      HTableDescriptor htd, int versionOfOfflineNode) {
    this(server, rsServices, regionInfo, htd, EventType.M_RS_OPEN_REGION,
        versionOfOfflineNode);
  }

  protected OpenRegionHandler(final Server server,
      final RegionServerServices rsServices, final HRegionInfo regionInfo,
      final HTableDescriptor htd, EventType eventType,
      final int versionOfOfflineNode) {
    super(server, eventType);
    this.rsServices = rsServices;
    this.regionInfo = regionInfo;
    this.htd = htd;
    this.versionOfOfflineNode = versionOfOfflineNode;
    tomActivated = this.server.getConfiguration().
      getBoolean(AssignmentManager.ASSIGNMENT_TIMEOUT_MANAGEMENT,
        AssignmentManager.DEFAULT_ASSIGNMENT_TIMEOUT_MANAGEMENT);
    assignmentTimeout = this.server.getConfiguration().
      getInt(AssignmentManager.ASSIGNMENT_TIMEOUT,
        AssignmentManager.DEFAULT_ASSIGNMENT_TIMEOUT_DEFAULT);
    useZKForAssignment = ConfigUtil.useZKForAssignment(server.getConfiguration());
  }

  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }

  @Override
  public void process() throws IOException {
    boolean openSuccessful = false;
    boolean transitionedToOpening = false;
    final String regionName = regionInfo.getRegionNameAsString();
    HRegion region = null;

    try {
      if (this.server.isStopped() || this.rsServices.isStopping()) {
        return;
      }
      final String encodedName = regionInfo.getEncodedName();

      // 3 different difficult situations can occur
      // 1) The opening was cancelled. This is an expected situation
      // 2) The region was hijacked, we no longer have the znode
      // 3) The region is now marked as online while we're suppose to open. This would be a bug.

      // Check that this region is not already online
      if (this.rsServices.getFromOnlineRegions(encodedName) != null) {
        LOG.error("Region " + encodedName +
            " was already online when we started processing the opening. " +
            "Marking this new attempt as failed");
        return;
      }

      // Check that we're still supposed to open the region and transition.
      // If fails, just return.  Someone stole the region from under us.
      // Calling transitionZookeeperOfflineToOpening initializes this.version.
      if (!isRegionStillOpening()){
        LOG.error("Region " + encodedName + " opening cancelled");
        return;
      }

      if (useZKForAssignment
          && !transitionZookeeperOfflineToOpening(encodedName, versionOfOfflineNode)) {
        LOG.warn("Region was hijacked? Opening cancelled for encodedName=" + encodedName);
        // This is a desperate attempt: the znode is unlikely to be ours. But we can't do more.
        return;
      }
      transitionedToOpening = true;
      // Open region.  After a successful open, failures in subsequent
      // processing needs to do a close as part of cleanup.
      region = openRegion();
      if (region == null) {
        return;
      }

      boolean failed = true;
      if (isRegionStillOpening() && (!useZKForAssignment || tickleOpening("post_region_open"))) {
        if (updateMeta(region)) {
          failed = false;
        }
      }
      if (failed || this.server.isStopped() ||
          this.rsServices.isStopping()) {
        return;
      }


      if (!isRegionStillOpening() || (useZKForAssignment && !transitionToOpened(region))) {
        // If we fail to transition to opened, it's because of one of two cases:
        //    (a) we lost our ZK lease
        // OR (b) someone else opened the region before us
        // OR (c) someone cancelled the open
        // In all cases, we try to transition to failed_open to be safe.
        return;
      }

      // We have a znode in the opened state now. We can't really delete it as the master job.
      // Transitioning to failed open would create a race condition if the master has already
      // acted the transition to opened.
      // Cancelling the open is dangerous, because we would have a state where the master thinks
      // the region is opened while the region is actually closed. It is a dangerous state
      // to be in. For this reason, from now on, we're not going back. There is a message in the
      // finally close to let the admin knows where we stand.


      // Successful region open, and add it to OnlineRegions
      this.rsServices.addToOnlineRegions(region);
      openSuccessful = true;

      // Done!  Successful region open
      LOG.debug("Opened " + regionName + " on " +
        this.server.getServerName());


    } finally {
      // Do all clean up here
      if (!openSuccessful) {
        doCleanUpOnFailedOpen(region, transitionedToOpening);
      }
      final Boolean current = this.rsServices.getRegionsInTransitionInRS().
          remove(this.regionInfo.getEncodedNameAsBytes());

      // Let's check if we have met a race condition on open cancellation....
      // A better solution would be to not have any race condition.
      // this.rsServices.getRegionsInTransitionInRS().remove(
      //  this.regionInfo.getEncodedNameAsBytes(), Boolean.TRUE);
      // would help, but we would still have a consistency issue to manage with
      // 1) this.rsServices.addToOnlineRegions(region);
      // 2) the ZK state.
      if (openSuccessful) {
        if (current == null) { // Should NEVER happen, but let's be paranoid.
          LOG.error("Bad state: we've just opened a region that was NOT in transition. Region="
              + regionName);
        } else if (Boolean.FALSE.equals(current)) { // Can happen, if we're
                                                    // really unlucky.
          LOG.error("Race condition: we've finished to open a region, while a close was requested "
              + " on region=" + regionName + ". It can be a critical error, as a region that"
              + " should be closed is now opened. Closing it now");
          cleanupFailedOpen(region);
        }
      }
    }
  }

  private void doCleanUpOnFailedOpen(HRegion region, boolean transitionedToOpening)
      throws IOException {
    if (transitionedToOpening) {
      try {
        if (region != null) {
          cleanupFailedOpen(region);
        }
      } finally {
        if (!useZKForAssignment) {
          rsServices.reportRegionStateTransition(TransitionCode.FAILED_OPEN, regionInfo);
        } else {
        // Even if cleanupFailed open fails we need to do this transition
        // See HBASE-7698
        tryTransitionFromOpeningToFailedOpen(regionInfo);
        }
      }
    } else if (!useZKForAssignment) {
      rsServices.reportRegionStateTransition(TransitionCode.FAILED_OPEN, regionInfo);
    } else {
      // If still transition to OPENING is not done, we need to transition znode
      // to FAILED_OPEN
      tryTransitionFromOfflineToFailedOpen(this.rsServices, regionInfo, versionOfOfflineNode);
    }
  }

  /**
   * Update ZK or META.  This can take a while if for example the
   * hbase:meta is not available -- if server hosting hbase:meta crashed and we are
   * waiting on it to come back -- so run in a thread and keep updating znode
   * state meantime so master doesn't timeout our region-in-transition.
   * Caller must cleanup region if this fails.
   */
  boolean updateMeta(final HRegion r) {
    if (this.server.isStopped() || this.rsServices.isStopping()) {
      return false;
    }
    // Object we do wait/notify on.  Make it boolean.  If set, we're done.
    // Else, wait.
    final AtomicBoolean signaller = new AtomicBoolean(false);
    PostOpenDeployTasksThread t = new PostOpenDeployTasksThread(r,
      this.server, this.rsServices, signaller);
    t.start();
    // Total timeout for meta edit.  If we fail adding the edit then close out
    // the region and let it be assigned elsewhere.
    long timeout = assignmentTimeout * 10;
    long now = System.currentTimeMillis();
    long endTime = now + timeout;
    // Let our period at which we update OPENING state to be be 1/3rd of the
    // regions-in-transition timeout period.
    long period = Math.max(1, assignmentTimeout/ 3);
    long lastUpdate = now;
    boolean tickleOpening = true;
    while (!signaller.get() && t.isAlive() && !this.server.isStopped() &&
        !this.rsServices.isStopping() && (endTime > now)) {
      long elapsed = now - lastUpdate;
      if (elapsed > period) {
        // Only tickle OPENING if postOpenDeployTasks is taking some time.
        lastUpdate = now;
        if (useZKForAssignment) {
          tickleOpening = tickleOpening("post_open_deploy");
        }
      }
      synchronized (signaller) {
        try {
          if (!signaller.get()) signaller.wait(period);
        } catch (InterruptedException e) {
          // Go to the loop check.
        }
      }
      now = System.currentTimeMillis();
    }
    // Is thread still alive?  We may have left above loop because server is
    // stopping or we timed out the edit.  Is so, interrupt it.
    if (t.isAlive()) {
      if (!signaller.get()) {
        // Thread still running; interrupt
        LOG.debug("Interrupting thread " + t);
        t.interrupt();
      }
      try {
        t.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted joining " +
          r.getRegionInfo().getRegionNameAsString(), ie);
        Thread.currentThread().interrupt();
      }
    }

    // Was there an exception opening the region?  This should trigger on
    // InterruptedException too.  If so, we failed.  Even if tickle opening fails
    // then it is a failure.
    return ((!Thread.interrupted() && t.getException() == null) && tickleOpening);
  }

  /**
   * Thread to run region post open tasks. Call {@link #getException()} after
   * the thread finishes to check for exceptions running
   * {@link RegionServerServices#postOpenDeployTasks(
   * HRegion, org.apache.hadoop.hbase.catalog.CatalogTracker)}
   * .
   */
  static class PostOpenDeployTasksThread extends Thread {
    private Throwable exception = null;
    private final Server server;
    private final RegionServerServices services;
    private final HRegion region;
    private final AtomicBoolean signaller;

    PostOpenDeployTasksThread(final HRegion region, final Server server,
        final RegionServerServices services, final AtomicBoolean signaller) {
      super("PostOpenDeployTasks:" + region.getRegionInfo().getEncodedName());
      this.setDaemon(true);
      this.server = server;
      this.services = services;
      this.region = region;
      this.signaller = signaller;
    }

    public void run() {
      try {
        this.services.postOpenDeployTasks(this.region,
          this.server.getCatalogTracker());
      } catch (Throwable e) {
        String msg =
            "Exception running postOpenDeployTasks; region="
                + this.region.getRegionInfo().getEncodedName();
        this.exception = e;
        if (e instanceof IOException && isRegionStillOpening(region.getRegionInfo(), services)) {
          server.abort(msg, e);
        } else {
          LOG.warn(msg, e);
        }
      }
      // We're done.  Set flag then wake up anyone waiting on thread to complete.
      this.signaller.set(true);
      synchronized (this.signaller) {
        this.signaller.notify();
      }
    }

    /**
     * @return Null or the run exception; call this method after thread is done.
     */
    Throwable getException() {
      return this.exception;
    }
  }


  /**
   * @param r Region we're working on.
   * @return whether znode is successfully transitioned to OPENED state.
   * @throws IOException
   */
  boolean transitionToOpened(final HRegion r) throws IOException {
    boolean result = false;
    HRegionInfo hri = r.getRegionInfo();
    final String name = hri.getRegionNameAsString();
    // Finally, Transition ZK node to OPENED
    try {
      if (ZKAssign.transitionNodeOpened(this.server.getZooKeeper(), hri,
          this.server.getServerName(), this.version) == -1) {
        String warnMsg = "Completed the OPEN of region " + name +
          " but when transitioning from " + " OPENING to OPENED ";
        try {
          String node = ZKAssign.getNodeName(this.server.getZooKeeper(), hri.getEncodedName());
          if (ZKUtil.checkExists(this.server.getZooKeeper(), node) < 0) {
            // if the znode 
            rsServices.abort(warnMsg + "the znode disappeared", null);
          } else {
            LOG.warn(warnMsg + "got a version mismatch, someone else clashed; " +
          "so now unassigning -- closing region on server: " + this.server.getServerName());
          }
        } catch (KeeperException ke) {
          rsServices.abort(warnMsg, ke);
        }
      } else {
        LOG.debug("Transitioned " + r.getRegionInfo().getEncodedName() +
          " to OPENED in zk on " + this.server.getServerName());
        result = true;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node " + name +
        " from OPENING to OPENED -- closing region", e);
    }
    return result;
  }

  /**
   * This is not guaranteed to succeed, we just do our best.
   * @param hri Region we're working on.
   * @return whether znode is successfully transitioned to FAILED_OPEN state.
   */
  private boolean tryTransitionFromOpeningToFailedOpen(final HRegionInfo hri) {
    boolean result = false;
    final String name = hri.getRegionNameAsString();
    try {
      LOG.info("Opening of region " + hri + " failed, transitioning" +
          " from OPENING to FAILED_OPEN in ZK, expecting version " + this.version);
      if (ZKAssign.transitionNode(
          this.server.getZooKeeper(), hri,
          this.server.getServerName(),
          EventType.RS_ZK_REGION_OPENING,
          EventType.RS_ZK_REGION_FAILED_OPEN,
          this.version) == -1) {
        LOG.warn("Unable to mark region " + hri + " as FAILED_OPEN. " +
            "It's likely that the master already timed out this open " +
            "attempt, and thus another RS already has the region.");
      } else {
        result = true;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node " + name +
        " from OPENING to FAILED_OPEN", e);
    }
    return result;
  }

  /**
   * Try to transition to open. This function is static to make it usable before creating the
   *  handler.
   *
   * This is not guaranteed to succeed, we just do our best.
   *
   * @param rsServices
   * @param hri Region we're working on.
   * @param versionOfOfflineNode version to checked.
   * @return whether znode is successfully transitioned to FAILED_OPEN state.
   */
  public static boolean tryTransitionFromOfflineToFailedOpen(RegionServerServices rsServices,
       final HRegionInfo hri, final int versionOfOfflineNode) {
    boolean result = false;
    final String name = hri.getRegionNameAsString();
    try {
      LOG.info("Opening of region " + hri + " failed, transitioning" +
          " from OFFLINE to FAILED_OPEN in ZK, expecting version " + versionOfOfflineNode);
      if (ZKAssign.transitionNode(
          rsServices.getZooKeeper(), hri,
          rsServices.getServerName(),
          EventType.M_ZK_REGION_OFFLINE,
          EventType.RS_ZK_REGION_FAILED_OPEN,
          versionOfOfflineNode) == -1) {
        LOG.warn("Unable to mark region " + hri + " as FAILED_OPEN. " +
            "It's likely that the master already timed out this open " +
            "attempt, and thus another RS already has the region.");
      } else {
        result = true;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node " + name + " from OFFLINE to FAILED_OPEN", e);
    }
    return result;
  }


  /**
   * @return Instance of HRegion if successful open else null.
   */
  HRegion openRegion() {
    HRegion region = null;
    try {
      // Instantiate the region.  This also periodically tickles our zk OPENING
      // state so master doesn't timeout this region in transition.
      region = HRegion.openHRegion(this.regionInfo, this.htd,
          this.rsServices.getWAL(this.regionInfo),
          this.server.getConfiguration(),
          this.rsServices,
        new CancelableProgressable() {
              public boolean progress() {
                if (useZKForAssignment) {
                  // We may lose the znode ownership during the open. Currently its
                  // too hard interrupting ongoing region open. Just let it complete
                  // and check we still have the znode after region open.
                  // if tickle failed, we need to cancel opening region.
                  return tickleOpening("open_region_progress");
                }
                if (!isRegionStillOpening()) {
                  LOG.warn("Open region aborted since it isn't opening any more");
                  return false;
                }
                return true;
              }
        });
    } catch (Throwable t) {
      // We failed open. Our caller will see the 'null' return value
      // and transition the node back to FAILED_OPEN. If that fails,
      // we rely on the Timeout Monitor in the master to reassign.
      LOG.error(
          "Failed open of region=" + this.regionInfo.getRegionNameAsString()
              + ", starting to roll back the global memstore size.", t);
      // Decrease the global memstore size.
      if (this.rsServices != null) {
        RegionServerAccounting rsAccounting =
          this.rsServices.getRegionServerAccounting();
        if (rsAccounting != null) {
          rsAccounting.rollbackRegionReplayEditsSize(this.regionInfo.getRegionName());
        }
      }
    }
    return region;
  }

  void cleanupFailedOpen(final HRegion region) throws IOException {
    if (region != null) {
      byte[] encodedName = regionInfo.getEncodedNameAsBytes();
      try {
        rsServices.getRegionsInTransitionInRS().put(encodedName,Boolean.FALSE);
        this.rsServices.removeFromOnlineRegions(region, null);
        region.close();
      } finally {
        rsServices.getRegionsInTransitionInRS().remove(encodedName);
      }
    }
  }

  private static boolean isRegionStillOpening(HRegionInfo regionInfo,
      RegionServerServices rsServices) {
    byte[] encodedName = regionInfo.getEncodedNameAsBytes();
    Boolean action = rsServices.getRegionsInTransitionInRS().get(encodedName);
    return Boolean.TRUE.equals(action); // true means opening for RIT
  }

  private boolean isRegionStillOpening() {
    return isRegionStillOpening(regionInfo, rsServices);
  }

  /**
   * Transition ZK node from OFFLINE to OPENING.
   * @param encodedName Name of the znode file (Region encodedName is the znode
   * name).
   * @param versionOfOfflineNode - version Of OfflineNode that needs to be compared
   * before changing the node's state from OFFLINE
   * @return True if successful transition.
   */
  boolean transitionZookeeperOfflineToOpening(final String encodedName,
      int versionOfOfflineNode) {
    // TODO: should also handle transition from CLOSED?
    try {
      // Initialize the znode version.
      this.version = ZKAssign.transitionNode(server.getZooKeeper(), regionInfo,
          server.getServerName(), EventType.M_ZK_REGION_OFFLINE,
          EventType.RS_ZK_REGION_OPENING, versionOfOfflineNode);
    } catch (KeeperException e) {
      LOG.error("Error transition from OFFLINE to OPENING for region=" +
        encodedName, e);
      this.version = -1;
      return false;
    }
    boolean b = isGoodVersion();
    if (!b) {
      LOG.warn("Failed transition from OFFLINE to OPENING for region=" +
        encodedName);
    }
    return b;
  }

  /**
   * Update our OPENING state in zookeeper.
   * Do this so master doesn't timeout this region-in-transition.
   * @param context Some context to add to logs if failure
   * @return True if successful transition.
   */
  boolean tickleOpening(final String context) {
    if (!isRegionStillOpening()) {
      LOG.warn("Open region aborted since it isn't opening any more");
      return false;
    }
    // If previous checks failed... do not try again.
    if (!isGoodVersion()) return false;
    String encodedName = this.regionInfo.getEncodedName();
    try {
      this.version =
        ZKAssign.retransitionNodeOpening(server.getZooKeeper(),
          this.regionInfo, this.server.getServerName(), this.version, tomActivated);
    } catch (KeeperException e) {
      server.abort("Exception refreshing OPENING; region=" + encodedName +
        ", context=" + context, e);
      this.version = -1;
      return false;
    }
    boolean b = isGoodVersion();
    if (!b) {
      LOG.warn("Failed refreshing OPENING; region=" + encodedName +
        ", context=" + context);
    }
    return b;
  }

  private boolean isGoodVersion() {
    return this.version != -1;
  }
}
