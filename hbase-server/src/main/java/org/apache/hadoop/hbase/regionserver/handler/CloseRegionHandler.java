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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coordination.CloseRegionCoordination;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.ConfigUtil;

/**
 * Handles closing of a region on a region server.
 */
@InterfaceAudience.Private
public class CloseRegionHandler extends EventHandler {
  // NOTE on priorities shutting down.  There are none for close. There are some
  // for open.  I think that is right.  On shutdown, we want the meta to close
  // before root and both to close after the user regions have closed.  What
  // about the case where master tells us to shutdown a catalog region and we
  // have a running queue of user regions to close?
  private static final Log LOG = LogFactory.getLog(CloseRegionHandler.class);

  protected final RegionServerServices rsServices;
  protected final HRegionInfo regionInfo;
  protected HRegion region;

  // If true, the hosting server is aborting.  Region close process is different
  // when we are aborting.
  private final boolean abort;
  private ServerName destination;
  private CloseRegionCoordination closeRegionCoordination;
  private CloseRegionCoordination.CloseRegionDetails closeRegionDetails;
  private final boolean useZKForAssignment;

  /**
   * This method used internally by the RegionServer to close out regions.
   * @param server
   * @param rsServices
   * @param regionInfo
   * @param abort If the regionserver is aborting.
   * @param closeRegionCoordination consensus for closing regions
   * @param crd object carrying details about region close task.
   */
  public CloseRegionHandler(final Server server,
      final RegionServerServices rsServices,
      final HRegionInfo regionInfo, final boolean abort,
      CloseRegionCoordination closeRegionCoordination,
      CloseRegionCoordination.CloseRegionDetails crd) {
    this(server, rsServices,  regionInfo, abort, closeRegionCoordination, crd,
      EventType.M_RS_CLOSE_REGION, null);
  }

  public CloseRegionHandler(final Server server,
      final RegionServerServices rsServices,
      final HRegionInfo regionInfo, final boolean abort,
      CloseRegionCoordination closeRegionCoordination,
      CloseRegionCoordination.CloseRegionDetails crd,
      ServerName destination) {
    this(server, rsServices, regionInfo, abort, closeRegionCoordination, crd,
      EventType.M_RS_CLOSE_REGION, destination);
  }

  public CloseRegionHandler(final Server server,
      final RegionServerServices rsServices, HRegionInfo regionInfo,
      boolean abort, CloseRegionCoordination closeRegionCoordination,
      CloseRegionCoordination.CloseRegionDetails crd, EventType eventType) {
    this(server, rsServices, regionInfo, abort, closeRegionCoordination, crd, eventType, null);
  }

    protected CloseRegionHandler(final Server server,
      final RegionServerServices rsServices, HRegionInfo regionInfo,
      boolean abort, CloseRegionCoordination closeRegionCoordination,
      CloseRegionCoordination.CloseRegionDetails crd,
      EventType eventType, ServerName destination) {
    super(server, eventType);
    this.server = server;
    this.rsServices = rsServices;
    this.regionInfo = regionInfo;
    this.abort = abort;
    this.destination = destination;
    this.closeRegionCoordination = closeRegionCoordination;
    this.closeRegionDetails = crd;
    useZKForAssignment = ConfigUtil.useZKForAssignment(server.getConfiguration());
  }

  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }

  @Override
  public void process() {
    try {
      String name = regionInfo.getRegionNameAsString();
      LOG.debug("Processing close of " + name);
      String encodedRegionName = regionInfo.getEncodedName();
      // Check that this region is being served here
      region = (HRegion)rsServices.getFromOnlineRegions(encodedRegionName);
      if (region == null) {
        LOG.warn("Received CLOSE for region " + name + " but currently not serving - ignoring");
        // TODO: do better than a simple warning
        return;
      }

      // Close the region
      try {
        if (useZKForAssignment && closeRegionCoordination.checkClosingState(
            regionInfo, closeRegionDetails)) {
          return;
        }

        // TODO: If we need to keep updating CLOSING stamp to prevent against
        // a timeout if this is long-running, need to spin up a thread?
        if (region.close(abort) == null) {
          // This region got closed.  Most likely due to a split. So instead
          // of doing the setClosedState() below, let's just ignore cont
          // The split message will clean up the master state.
          LOG.warn("Can't close region: was already closed during close(): " +
            regionInfo.getRegionNameAsString());
          return;
        }

        if (!abort) {
          releaseWALIfNeeded();
        }
      } catch (IOException ioe) {
        // An IOException here indicates that we couldn't successfully flush the
        // memstore before closing. So, we need to abort the server and allow
        // the master to split our logs in order to recover the data.
        server.abort("Unrecoverable exception while closing region " +
          regionInfo.getRegionNameAsString() + ", still finishing close", ioe);
        throw new RuntimeException(ioe);
      }

      this.rsServices.removeFromOnlineRegions(region, destination);
      if (!useZKForAssignment) {
        rsServices.reportRegionStateTransition(TransitionCode.CLOSED, regionInfo);
      } else {
        closeRegionCoordination.setClosedState(region, this.server.getServerName(),
          closeRegionDetails);
      }

      // Done!  Region is closed on this RS
      LOG.debug("Closed " + region.getRegionInfo().getRegionNameAsString());
    } finally {
      this.rsServices.getRegionsInTransitionInRS().
          remove(this.regionInfo.getEncodedNameAsBytes());
    }
  }

  protected void releaseWALIfNeeded() throws IOException {
    // release the WAL if needed. Only meta does this for now.
  }
}
