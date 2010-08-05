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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.RootLocationEditor;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerController;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.util.Progressable;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

/**
 * Handles opening of a region on a region server.
 * <p>
 * This is executed after receiving an OPEN RPC from the master.
 */
public class OpenRegionHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(OpenRegionHandler.class);

  private final RegionServerController server;

  private final CatalogTracker catalogTracker;

  private final HRegionInfo regionInfo;

  public OpenRegionHandler(RegionServerController server,
      CatalogTracker catalogTracker, HRegionInfo regionInfo) {
    this(server, catalogTracker, regionInfo, EventType.M2RS_OPEN_REGION);
  }

  protected OpenRegionHandler(RegionServerController server,
      CatalogTracker catalogTracker, HRegionInfo regionInfo,
      EventType eventType) {
    super(server, eventType);
    this.server = server;
    this.catalogTracker = catalogTracker;
    this.regionInfo = regionInfo;
  }

  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }

  @Override
  public void process() {
    LOG.debug("Processing open region of " +
        regionInfo.getRegionNameAsString());

    final String regionName = regionInfo.getEncodedName();

    // Previously we would check for root region availability (but only that it
    // was initially available, does not check if it later went away)
    // Do we need to wait on both root and meta to be available to open a region
    // now since we edit meta?

    // Check that this region is not already online
    HRegion region = server.getOnlineRegion(regionName);
    if(region != null) {
      LOG.warn("Attemping to open region " + regionInfo.getRegionNameAsString()
          + " but it's already online on this server");
      return;
    }

    // Transition ZK node from OFFLINE to OPENING
    // TODO: should also handle transition from CLOSED?
    int openingVersion;
    try {
      if((openingVersion = ZKAssign.transitionNodeOpening(server.getZooKeeper(),
          regionInfo, server.getServerName())) == -1) {
        LOG.warn("Error transitioning node from OFFLINE to OPENING, " +
            "aborting open");
        return;
      }
    } catch (KeeperException e) {
      LOG.error("Error transitioning node from OFFLINE to OPENING for region " +
      		regionName, e);
      return;
    }

    // Open the region
    final AtomicInteger openingInteger = new AtomicInteger(openingVersion);
    try {
      // Instantiate the region.  This also periodically updates OPENING.
      region = server.instantiateRegion(regionInfo, server.getLog(),
          new Progressable() {
            public void progress() {
              try {
                int vsn = ZKAssign.retransitionNodeOpening(
                    server.getZooKeeper(), regionInfo, server.getServerName(),
                    openingInteger.get());
                if (vsn == -1) {
                  throw KeeperException.create(Code.BADVERSION);
                }
                openingInteger.set(vsn);
              } catch (KeeperException e) {
                LOG.error("ZK exception refreshing OPENING node", e);
                server.abort();
              }
            }
      });
    } catch (IOException e) {
      LOG.error("IOException instantiating region for " + regionInfo);
      LOG.debug("Resetting state of transition node from OPENING to OFFLINE");
      try {
        // TODO: We should rely on the master timing out OPENING instead of this
        ZKAssign.forceNodeOffline(server.getZooKeeper(), regionInfo,
            server.getServerName());
      } catch (KeeperException e1) {
        LOG.error("Error forcing node back to OFFLINE from OPENING");
        return;
      }
      return;
    }

    // Re-transition node to OPENING again to verify someone else has not
    // stomped on us
    openingVersion = openingInteger.get();
    try {
      if((openingVersion = ZKAssign.retransitionNodeOpening(
          server.getZooKeeper(), regionInfo, server.getServerName(),
          openingVersion)) == -1) {
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

    LOG.debug("Re-transitioned node to OPENING, completing OPEN by adding to " +
        "online regions, doing on-open checks, and updating ROOT or META " +
        "for region " + region.getRegionNameAsString());

    // Do checks to see if we need to compact (references or too many files)
    if(region.hasReferences() || region.hasTooManyStoreFiles()) {
      server.getCompactSplitThread().compactionRequested(region,
          region.hasReferences() ? "Region has references on open" :
                                   "Region has too many store files");
    }

    // Add to online regions
    server.addToOnlineRegions(region);

    // Update ZK, ROOT or META
    try {
      if(regionInfo.isRootRegion()) {
        RootLocationEditor.setRootLocation(server.getZooKeeper(),
            server.getServerInfo().getServerAddress());
      } else if(regionInfo.isMetaRegion()) {
        // TODO: doh, this has weird naming between RootEditor/MetaEditor
        MetaEditor.updateMetaLocation(catalogTracker, regionInfo,
            server.getServerInfo());
      } else {
        MetaEditor.updateRegionLocation(catalogTracker, region.getRegionInfo(),
          server.getServerInfo());
      }
    } catch (IOException e) {
      // TODO: rollback the open?
      LOG.error("Error updating region location in catalog table", e);
    } catch (KeeperException e) {
      // TODO: rollback the open?
      LOG.error("ZK Error updating region location in catalog table", e);
    }

    // Finally, Transition ZK node to OPENED
    try {
      if(ZKAssign.transitionNodeOpened(server.getZooKeeper(), regionInfo,
          server.getServerName(), openingVersion) == -1) {
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
    LOG.debug("Completed region open and successfully transitioned node to " +
        "OPENED for region " + region.getRegionNameAsString());
  }
}