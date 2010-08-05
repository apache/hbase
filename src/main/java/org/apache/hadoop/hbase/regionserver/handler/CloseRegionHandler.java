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
package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerController;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Handles closing of a region on a region server.
 * <p>
 * This is executed after receiving an CLOSE RPC from the master.
 */
public class CloseRegionHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(CloseRegionHandler.class);

  private final RegionServerController server;

  private final HRegionInfo regionInfo;

  private final boolean abort;

  public CloseRegionHandler(RegionServerController server,
      HRegionInfo regionInfo) {
    this(server, regionInfo, false);
  }

  public CloseRegionHandler(RegionServerController server,
      HRegionInfo regionInfo, boolean abort) {
    this(server, regionInfo, abort, EventType.M2RS_CLOSE_REGION);
  }

  protected CloseRegionHandler(RegionServerController server,
      HRegionInfo regionInfo, boolean abort, EventType eventType) {
    super(server, eventType);
    this.server = server;
    this.regionInfo = regionInfo;
    this.abort = abort;
  }

  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }

  @Override
  public void process() {
    LOG.debug("Processing close region of " +
        regionInfo.getRegionNameAsString());

    String regionName = regionInfo.getEncodedName();

    // Check that this region is being served here
    HRegion region = server.getOnlineRegion(regionName);
    if(region == null) {
      LOG.warn("Received a CLOSE for the region " +
          regionInfo.getRegionNameAsString() + " but not currently serving " +
          "this region");
      return;
    }

    // Create ZK node in CLOSING state
    int expectedVersion;
    try {
      if((expectedVersion = ZKAssign.createNodeClosing(server.getZooKeeper(),
          regionInfo, server.getServerName())) == -1) {
        LOG.warn("Error creating node in CLOSING state, aborting close of " +
            regionInfo.getRegionNameAsString());
        return;
      }
    } catch (KeeperException e) {
      LOG.warn("Error creating node in CLOSING state, aborting close of " +
          regionInfo.getRegionNameAsString());
      return;
    }

    // Close the region
    try {
      // TODO: If we need to keep updating CLOSING stamp to prevent against
      //       a timeout if this is long-running, need to spin up a thread?
      server.removeFromOnlineRegions(regionInfo);
      region.close(abort);
    } catch (IOException e) {
      LOG.error("IOException closing region for " + regionInfo);
      LOG.debug("Deleting transition node that was in CLOSING");
      try {
        ZKAssign.deleteClosingNode(server.getZooKeeper(), regionName);
      } catch (KeeperException e1) {
        LOG.error("Error deleting CLOSING node");
        return;
      }
      return;
    }

    // Transition ZK node to CLOSED
    try {
      if(ZKAssign.transitionNodeClosed(server.getZooKeeper(), regionInfo,
          server.getServerName(), expectedVersion) == -1) {
        LOG.warn("Completed the OPEN of a region but when transitioning from " +
            " OPENING to OPENED got a version mismatch, someone else clashed " +
            "so now unassigning");
        region.close();
        return;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node from OPENING to OPENED", e);
      return;
    } catch (IOException e) {
      LOG.error("Failed to close region after failing to transition", e);
      return;
    }

    // Done!  Successful region open
    LOG.debug("Completed region close and successfully transitioned node to " +
        "CLOSED for region " + region.getRegionNameAsString() + " (" +
        regionName + ")");
  }
}
