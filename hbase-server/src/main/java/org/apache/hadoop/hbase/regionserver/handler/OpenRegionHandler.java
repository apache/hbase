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
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.RegionServerServices.PostOpenDeployContext;
import org.apache.hadoop.hbase.util.CancelableProgressable;
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

  public OpenRegionHandler(final Server server,
      final RegionServerServices rsServices, HRegionInfo regionInfo,
      HTableDescriptor htd, long masterSystemTime) {
    this(server, rsServices, regionInfo, htd, masterSystemTime, EventType.M_RS_OPEN_REGION);
  }

  protected OpenRegionHandler(final Server server,
      final RegionServerServices rsServices, final HRegionInfo regionInfo,
      final HTableDescriptor htd, long masterSystemTime, EventType eventType) {
    super(server, eventType);
    this.rsServices = rsServices;
    this.regionInfo = regionInfo;
    this.htd = htd;
    this.masterSystemTime = masterSystemTime;
  }

  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }

  @Override
  public void process() throws IOException {
    boolean openSuccessful = false;
    final String regionName = regionInfo.getRegionNameAsString();
    HRegion region = null;

    try {
      if (this.server.isStopped() || this.rsServices.isStopping()) {
        return;
      }
      final String encodedName = regionInfo.getEncodedName();

      // 2 different difficult situations can occur
      // 1) The opening was cancelled. This is an expected situation
      // 2) The region is now marked as online while we're suppose to open. This would be a bug.

      // Check that this region is not already online
      if (this.rsServices.getFromOnlineRegions(encodedName) != null) {
        LOG.error("Region " + encodedName +
            " was already online when we started processing the opening. " +
            "Marking this new attempt as failed");
        return;
      }

      // Check that we're still supposed to open the region.
      // If fails, just return.  Someone stole the region from under us.
      if (!isRegionStillOpening()){
        LOG.error("Region " + encodedName + " opening cancelled");
        return;
      }

      // Open region.  After a successful open, failures in subsequent
      // processing needs to do a close as part of cleanup.
      region = openRegion();
      if (region == null) {
        return;
      }

      if (!updateMeta(region, masterSystemTime) || this.server.isStopped() ||
          this.rsServices.isStopping()) {
        return;
      }

      if (!isRegionStillOpening()) {
        return;
      }

      // Successful region open, and add it to OnlineRegions
      this.rsServices.addToOnlineRegions(region);
      openSuccessful = true;

      // Done!  Successful region open
      LOG.debug("Opened " + regionName + " on " +
        this.server.getServerName());
    } finally {
      // Do all clean up here
      if (!openSuccessful) {
        doCleanUpOnFailedOpen(region);
      }
      final Boolean current = this.rsServices.getRegionsInTransitionInRS().
          remove(this.regionInfo.getEncodedNameAsBytes());

      // Let's check if we have met a race condition on open cancellation....
      // A better solution would be to not have any race condition.
      // this.rsServices.getRegionsInTransitionInRS().remove(
      //  this.regionInfo.getEncodedNameAsBytes(), Boolean.TRUE);
      // would help.
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

  private void doCleanUpOnFailedOpen(HRegion region)
      throws IOException {
    try {
      if (region != null) {
        cleanupFailedOpen(region);
      }
    } finally {
      rsServices.reportRegionStateTransition(TransitionCode.FAILED_OPEN, regionInfo);
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
    while (!signaller.get() && t.isAlive() && !this.server.isStopped() &&
        !this.rsServices.isStopping() && isRegionStillOpening()) {
      synchronized (signaller) {
        try {
          // Wait for 10 seconds, so that server shutdown
          // won't take too long if this thread happens to run.
          if (!signaller.get()) signaller.wait(10000);
        } catch (InterruptedException e) {
          // Go to the loop check.
        }
      }
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
    // InterruptedException too.  If so, we failed.
    return (!Thread.interrupted() && t.getException() == null);
  }

  /**
   * Thread to run region post open tasks. Call {@link #getException()} after the thread finishes
   * to check for exceptions running {@link RegionServerServices#postOpenDeployTasks(Region)}.
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
      this.rsServices.removeFromOnlineRegions(region, null);
      region.close();
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
