/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.CatalogJanitor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Handles MERGE regions request on master: move the regions together(on the
 * same regionserver) and send MERGE RPC to regionserver.
 *
 * NOTE:The real merge is executed on the regionserver
 *
 */
@InterfaceAudience.Private
public class DispatchMergingRegionHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(DispatchMergingRegionHandler.class);
  private final MasterServices masterServices;
  private final CatalogJanitor catalogJanitor;
  private HRegionInfo region_a;
  private HRegionInfo region_b;
  private final boolean forcible;
  private final int timeout;
  private final User user;

  public DispatchMergingRegionHandler(final MasterServices services,
      final CatalogJanitor catalogJanitor, final HRegionInfo region_a,
      final HRegionInfo region_b, final boolean forcible, final User user) {
    super(services, EventType.C_M_MERGE_REGION);
    this.masterServices = services;
    this.catalogJanitor = catalogJanitor;
    this.region_a = region_a;
    this.region_b = region_b;
    this.forcible = forcible;
    this.user = user;
    this.timeout = server.getConfiguration().getInt(
        "hbase.master.regionmerge.timeout", 120 * 1000);
  }

  @Override
  public void process() throws IOException {
    boolean regionAHasMergeQualifier = !catalogJanitor.cleanMergeQualifier(region_a);
    if (regionAHasMergeQualifier
        || !catalogJanitor.cleanMergeQualifier(region_b)) {
      LOG.info("Skip merging regions " + region_a.getRegionNameAsString()
          + ", " + region_b.getRegionNameAsString() + ", because region "
          + (regionAHasMergeQualifier ? region_a.getEncodedName() : region_b
              .getEncodedName()) + " has merge qualifier");
      return;
    }

    RegionStates regionStates = masterServices.getAssignmentManager()
        .getRegionStates();
    ServerName region_a_location = regionStates.getRegionServerOfRegion(region_a);
    ServerName region_b_location = regionStates.getRegionServerOfRegion(region_b);
    if (region_a_location == null || region_b_location == null) {
      LOG.info("Skip merging regions " + region_a.getRegionNameAsString()
          + ", " + region_b.getRegionNameAsString() + ", because region "
          + (region_a_location == null ? region_a.getEncodedName() : region_b
              .getEncodedName()) + " is not online now");
      return;
    }
    long startTime = EnvironmentEdgeManager.currentTime();
    boolean onSameRS = region_a_location.equals(region_b_location);

    // Make sure regions are on the same regionserver before send merge
    // regions request to regionserver
    if (!onSameRS) {
      // Move region_b to region a's location, switch region_a and region_b if
      // region_a's load lower than region_b's, so we will always move lower
      // load region
      RegionLoad loadOfRegionA = getRegionLoad(region_a_location, region_a);
      RegionLoad loadOfRegionB = getRegionLoad(region_b_location, region_b);
      if (loadOfRegionA != null && loadOfRegionB != null
          && loadOfRegionA.getRequestsCount() < loadOfRegionB
              .getRequestsCount()) {
        // switch region_a and region_b
        HRegionInfo tmpRegion = this.region_a;
        this.region_a = this.region_b;
        this.region_b = tmpRegion;
        ServerName tmpLocation = region_a_location;
        region_a_location = region_b_location;
        region_b_location = tmpLocation;
      }

      RegionPlan regionPlan = new RegionPlan(region_b, region_b_location,
          region_a_location);
      LOG.info("Moving regions to same server for merge: " + regionPlan.toString());
      masterServices.getAssignmentManager().balance(regionPlan);
      while (!masterServices.isStopped()) {
        try {
          Thread.sleep(20);
          // Make sure check RIT first, then get region location, otherwise
          // we would make a wrong result if region is online between getting
          // region location and checking RIT
          boolean isRIT = regionStates.isRegionInTransition(region_b);
          region_b_location = masterServices.getAssignmentManager()
              .getRegionStates().getRegionServerOfRegion(region_b);
          onSameRS = region_a_location.equals(region_b_location);
          if (onSameRS || !isRIT) {
            // Regions are on the same RS, or region_b is not in
            // RegionInTransition any more
            break;
          }
          if ((EnvironmentEdgeManager.currentTime() - startTime) > timeout) break;
        } catch (InterruptedException e) {
          InterruptedIOException iioe = new InterruptedIOException();
          iioe.initCause(e);
          throw iioe;
        }
      }
    }

    if (onSameRS) {
      startTime = EnvironmentEdgeManager.currentTime();
      while (!masterServices.isStopped()) {
        try {
          masterServices.getServerManager().sendRegionsMerge(region_a_location,
              region_a, region_b, forcible, user);
          LOG.info("Sent merge to server " + region_a_location + " for region " +
            region_a.getEncodedName() + "," + region_b.getEncodedName() + ", focible=" + forcible);
          break;
        } catch (RegionOpeningException roe) {
          if ((EnvironmentEdgeManager.currentTime() - startTime) > timeout) {
            LOG.warn("Failed sending merge to " + region_a_location + " after " + timeout + "ms",
              roe);
            break;
          }
          // Do a retry since region should be online on RS immediately
        } catch (IOException ie) {
          LOG.warn("Failed sending merge to " + region_a_location + " for region " +
            region_a.getEncodedName() + "," + region_b.getEncodedName() + ", focible=" + forcible,
            ie);
          break;
        }
      }
    } else {
      LOG.info("Cancel merging regions " + region_a.getRegionNameAsString()
          + ", " + region_b.getRegionNameAsString()
          + ", because can't move them together after "
          + (EnvironmentEdgeManager.currentTime() - startTime) + "ms");
    }
  }

  private RegionLoad getRegionLoad(ServerName sn, HRegionInfo hri) {
    ServerManager serverManager =  masterServices.getServerManager();
    ServerLoad load = serverManager.getLoad(sn);
    if (load != null) {
      Map<byte[], RegionLoad> regionsLoad = load.getRegionsLoad();
      if (regionsLoad != null) {
        return regionsLoad.get(hri.getRegionName());
      }
    }
    return null;
  }
}
