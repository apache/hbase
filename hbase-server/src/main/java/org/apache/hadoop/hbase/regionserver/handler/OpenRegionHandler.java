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
import org.apache.hadoop.hbase.coordination.OpenRegionCoordination;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.RegionServerServices.PostOpenDeployContext;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ConfigUtil;
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
  private final long masterSystemTime;

  private OpenRegionCoordination coordination;
  private OpenRegionCoordination.OpenRegionDetails ord;

  private final boolean useZKForAssignment;

  public OpenRegionHandler(final Server server,
      final RegionServerServices rsServices, HRegionInfo regionInfo,
      HTableDescriptor htd, long masterSystemTime, OpenRegionCoordination coordination,
      OpenRegionCoordination.OpenRegionDetails ord) {
    this(server, rsServices, regionInfo, htd, EventType.M_RS_OPEN_REGION,
        masterSystemTime, coordination, ord);
  }

  protected OpenRegionHandler(final Server server,
      final RegionServerServices rsServices, final HRegionInfo regionInfo,
      final HTableDescriptor htd, EventType eventType, long masterSystemTime,
      OpenRegionCoordination coordination, OpenRegionCoordination.OpenRegionDetails ord) {
    super(server, eventType);
    this.rsServices = rsServices;
    this.regionInfo = regionInfo;
    this.htd = htd;
    this.coordination = coordination;
    this.ord = ord;
    useZKForAssignment = ConfigUtil.useZKForAssignment(server.getConfiguration());
    this.masterSystemTime = masterSystemTime;
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
      // Calling transitionFromOfflineToOpening initializes this.version.
      if (!isRegionStillOpening()){
        LOG.error("Region " + encodedName + " opening cancelled");
        return;
      }

      if (useZKForAssignment
          && !coordination.transitionFromOfflineToOpening(regionInfo, ord)) {
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
      if (isRegionStillOpening() && (!useZKForAssignment ||
           coordination.tickleOpening(ord, regionInfo, rsServices, "post_region_open"))) {
        if (updateMeta(region, masterSystemTime)) {
          failed = false;
        }
      }
      if (failed || this.server.isStopped() ||
          this.rsServices.isStopping()) {
        return;
      }

      if (!isRegionStillOpening() ||
          (useZKForAssignment && !coordination.transitionToOpened(region, ord))) {
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
        doCleanUpOnFailedOpen(region, transitionedToOpening, ord);
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

  private void doCleanUpOnFailedOpen(HRegion region, boolean transitionedToOpening,
                                     OpenRegionCoordination.OpenRegionDetails ord)
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
          coordination.tryTransitionFromOpeningToFailedOpen(regionInfo, ord);
        }
      }
    } else if (!useZKForAssignment) {
      rsServices.reportRegionStateTransition(TransitionCode.FAILED_OPEN, regionInfo);
    } else {
      // If still transition to OPENING is not done, we need to transition znode
      // to FAILED_OPEN
      coordination.tryTransitionFromOfflineToFailedOpen(this.rsServices, regionInfo, ord);
    }
  }

  /**
   * Update ZK or META.  This can take a while if for example the
   * hbase:meta is not available -- if server hosting hbase:meta crashed and we are
   * waiting on it to come back -- so run in a thread and keep updating znode
   * state meantime so master doesn't timeout our region-in-transition.
   * Caller must cleanup region if this fails.
   */
  boolean updateMeta(final HRegion r, long masterSystemTime) {
    if (this.server.isStopped() || this.rsServices.isStopping()) {
      return false;
    }
    // Object we do wait/notify on.  Make it boolean.  If set, we're done.
    // Else, wait.
    final AtomicBoolean signaller = new AtomicBoolean(false);
    PostOpenDeployTasksThread t = new PostOpenDeployTasksThread(r,
      this.server, this.rsServices, signaller, masterSystemTime);
    t.start();
    // Post open deploy task:
    //   meta => update meta location in ZK
    //   other region => update meta
    // It could fail if ZK/meta is not available and
    // the update runs out of retries.
    long now = System.currentTimeMillis();
    long lastUpdate = now;
    boolean tickleOpening = true;
    while (!signaller.get() && t.isAlive() && !this.server.isStopped() &&
        !this.rsServices.isStopping() && isRegionStillOpening()) {
      long elapsed = now - lastUpdate;
      if (elapsed > 120000) { // 2 minutes, no need to tickleOpening too often
        // Only tickle OPENING if postOpenDeployTasks is taking some time.
        lastUpdate = now;
        if (useZKForAssignment) {
          tickleOpening = coordination.tickleOpening(
            ord, regionInfo, rsServices, "post_open_deploy");
        }
      }
      synchronized (signaller) {
        try {
          // Wait for 10 seconds, so that server shutdown
          // won't take too long if this thread happens to run.
          if (!signaller.get()) signaller.wait(10000);
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
   * {@link RegionServerServices#postOpenDeployTasks(HRegion)
   */
  static class PostOpenDeployTasksThread extends Thread {
    private Throwable exception = null;
    private final Server server;
    private final RegionServerServices services;
    private final HRegion region;
    private final AtomicBoolean signaller;
    private final long masterSystemTime;

    PostOpenDeployTasksThread(final HRegion region, final Server server,
        final RegionServerServices services, final AtomicBoolean signaller, long masterSystemTime) {
      super("PostOpenDeployTasks:" + region.getRegionInfo().getEncodedName());
      this.setDaemon(true);
      this.server = server;
      this.services = services;
      this.region = region;
      this.signaller = signaller;
      this.masterSystemTime = masterSystemTime;
    }

    @Override
    public void run() {
      try {
        this.services.postOpenDeployTasks(new PostOpenDeployContext(region, masterSystemTime));
      } catch (Throwable e) {
        String msg = "Exception running postOpenDeployTasks; region=" +
          this.region.getRegionInfo().getEncodedName();
        this.exception = e;
        if (e instanceof IOException
            && isRegionStillOpening(region.getRegionInfo(), services)) {
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
   * @return Instance of HRegion if successful open else null.
   */
  HRegion openRegion() {
    HRegion region = null;
    try {
      // Instantiate the region.  This also periodically tickles OPENING
      // state so master doesn't timeout this region in transition.
      region = HRegion.openHRegion(this.regionInfo, this.htd,
        this.rsServices.getWAL(this.regionInfo),
        this.server.getConfiguration(),
        this.rsServices,
        new CancelableProgressable() {
          @Override
          public boolean progress() {
            if (useZKForAssignment) {
              // if tickle failed, we need to cancel opening region.
              return coordination.tickleOpening(ord, regionInfo,
                rsServices, "open_region_progress");
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

  private static boolean isRegionStillOpening(
      HRegionInfo regionInfo, RegionServerServices rsServices) {
    byte[] encodedName = regionInfo.getEncodedNameAsBytes();
    Boolean action = rsServices.getRegionsInTransitionInRS().get(encodedName);
    return Boolean.TRUE.equals(action); // true means opening for RIT
  }

  private boolean isRegionStillOpening() {
    return isRegionStillOpening(regionInfo, rsServices);
  }
}
