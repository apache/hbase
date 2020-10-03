/*
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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices.RegionStateTransitionContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Handles closing of a region on a region server.
 * <p/>
 * Just done the same thing with the old {@link CloseRegionHandler}, with some modifications on
 * fencing and retrying. But we need to keep the {@link CloseRegionHandler} as is to keep compatible
 * with the zk less assignment for 1.x, otherwise it is not possible to do rolling upgrade.
 */
@InterfaceAudience.Private
public class UnassignRegionHandler extends EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(UnassignRegionHandler.class);

  private final String encodedName;

  private final long closeProcId;
  // If true, the hosting server is aborting. Region close process is different
  // when we are aborting.
  // TODO: not used yet, we still use the old CloseRegionHandler when aborting
  private final boolean abort;

  private final ServerName destination;

  private final RetryCounter retryCounter;

  public UnassignRegionHandler(HRegionServer server, String encodedName, long closeProcId,
      boolean abort, @Nullable ServerName destination, EventType eventType) {
    super(server, eventType);
    this.encodedName = encodedName;
    this.closeProcId = closeProcId;
    this.abort = abort;
    this.destination = destination;
    this.retryCounter = HandlerUtil.getRetryCounter();
  }

  private HRegionServer getServer() {
    return (HRegionServer) server;
  }

  @Override
  public void process() throws IOException {
    HRegionServer rs = getServer();
    byte[] encodedNameBytes = Bytes.toBytes(encodedName);
    Boolean previous = rs.getRegionsInTransitionInRS().putIfAbsent(encodedNameBytes, Boolean.FALSE);
    if (previous != null) {
      if (previous) {
        // This could happen as we will update the region state to OPEN when calling
        // reportRegionStateTransition, so the HMaster will think the region is online, before we
        // actually open the region, as reportRegionStateTransition is part of the opening process.
        long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
        LOG.warn("Received CLOSE for {} which we are already " +
          "trying to OPEN; try again after {}ms", encodedName, backoff);
        rs.getExecutorService().delayedSubmit(this, backoff, TimeUnit.MILLISECONDS);
      } else {
        LOG.info("Received CLOSE for {} which we are already trying to CLOSE," +
          " but not completed yet", encodedName);
      }
      return;
    }
    HRegion region = rs.getRegion(encodedName);
    if (region == null) {
      LOG.debug("Received CLOSE for {} which is not ONLINE and we're not opening/closing.",
        encodedName);
      rs.getRegionsInTransitionInRS().remove(encodedNameBytes, Boolean.FALSE);
      return;
    }
    String regionName = region.getRegionInfo().getEncodedName();
    LOG.info("Close {}", regionName);
    if (region.getCoprocessorHost() != null) {
      // XXX: The behavior is a bit broken. At master side there is no FAILED_CLOSE state, so if
      // there are exception thrown from the CP, we can not report the error to master, and if
      // here we just return without calling reportRegionStateTransition, the TRSP at master side
      // will hang there for ever. So here if the CP throws an exception out, the only way is to
      // abort the RS...
      region.getCoprocessorHost().preClose(abort);
    }
    if (region.close(abort) == null) {
      // XXX: Is this still possible? The old comment says about split, but now split is done at
      // master side, so...
      LOG.warn("Can't close {}, already closed during close()", regionName);
      rs.getRegionsInTransitionInRS().remove(encodedNameBytes, Boolean.FALSE);
      return;
    }

    rs.removeRegion(region, destination);
    if (ServerRegionReplicaUtil.isMetaRegionReplicaReplicationEnabled(rs.getConfiguration(),
        region.getTableDescriptor().getTableName())) {
      if (RegionReplicaUtil.isDefaultReplica(region.getRegionInfo().getReplicaId())) {
        // If hbase:meta read replicas enabled, remove replication source for hbase:meta Regions.
        // See assign region handler where we add the replication source on open.
        rs.getReplicationSourceService().getReplicationManager().
          removeCatalogReplicationSource(region.getRegionInfo());
      }
    }
    if (!rs.reportRegionStateTransition(
      new RegionStateTransitionContext(TransitionCode.CLOSED, HConstants.NO_SEQNUM, closeProcId,
        -1, region.getRegionInfo()))) {
      throw new IOException("Failed to report close to master: " + regionName);
    }
    // Cache the close region procedure id after report region transition succeed.
    rs.finishRegionProcedure(closeProcId);
    rs.getRegionsInTransitionInRS().remove(encodedNameBytes, Boolean.FALSE);
    LOG.info("Closed {}", regionName);
  }

  @Override
  protected void handleException(Throwable t) {
    LOG.warn("Fatal error occurred while closing region {}, aborting...", encodedName, t);
    // Clear any reference in getServer().getRegionsInTransitionInRS() otherwise can hold up
    // regionserver abort on cluster shutdown. HBASE-23984.
    getServer().getRegionsInTransitionInRS().remove(Bytes.toBytes(this.encodedName));
    getServer().abort("Failed to close region " + encodedName + " and can not recover", t);
  }

  public static UnassignRegionHandler create(HRegionServer server, String encodedName,
      long closeProcId, boolean abort, @Nullable ServerName destination) {
    // Just try our best to determine whether it is for closing meta. It is not the end of the world
    // if we put the handler into a wrong executor.
    Region region = server.getRegion(encodedName);
    EventType eventType =
      region != null && region.getRegionInfo().isMetaRegion() ? EventType.M_RS_CLOSE_META
        : EventType.M_RS_CLOSE_REGION;
    return new UnassignRegionHandler(server, encodedName, closeProcId, abort, destination,
      eventType);
  }
}
