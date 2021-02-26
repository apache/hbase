/*
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

package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Move Regions and make sure that they are up on the target server.If a region movement fails we
 * exit as failure
 */
@InterfaceAudience.Private
class MoveWithAck implements Callable<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(MoveWithAck.class);

  private final RegionInfo region;
  private final ServerName targetServer;
  private final List<RegionInfo> movedRegions;
  private final ServerName sourceServer;
  private final Connection conn;
  private final Admin admin;

  MoveWithAck(Connection conn, RegionInfo regionInfo, ServerName sourceServer,
    ServerName targetServer, List<RegionInfo> movedRegions) throws IOException {
    this.conn = conn;
    this.region = regionInfo;
    this.targetServer = targetServer;
    this.movedRegions = movedRegions;
    this.sourceServer = sourceServer;
    this.admin = conn.getAdmin();
  }

  @Override
  public Boolean call() throws IOException, InterruptedException {
    boolean moved = false;
    int count = 0;
    int retries = admin.getConfiguration()
      .getInt(RegionMover.MOVE_RETRIES_MAX_KEY, RegionMover.DEFAULT_MOVE_RETRIES_MAX);
    int maxWaitInSeconds = admin.getConfiguration()
      .getInt(RegionMover.MOVE_WAIT_MAX_KEY, RegionMover.DEFAULT_MOVE_WAIT_MAX);
    long startTime = EnvironmentEdgeManager.currentTime();
    boolean sameServer = true;
    // Assert we can scan the region in its current location
    isSuccessfulScan(region);
    LOG.info("Moving region: {} from {} to {}", region.getRegionNameAsString(), sourceServer,
      targetServer);
    while (count < retries && sameServer) {
      if (count > 0) {
        LOG.debug("Retry {} of maximum {} for region: {}", count, retries,
          region.getRegionNameAsString());
      }
      count = count + 1;
      admin.move(region.getEncodedNameAsBytes(), targetServer);
      long maxWait = startTime + (maxWaitInSeconds * 1000);
      while (EnvironmentEdgeManager.currentTime() < maxWait) {
        sameServer = isSameServer(region, sourceServer);
        if (!sameServer) {
          break;
        }
        Thread.sleep(1000);
      }
    }
    if (sameServer) {
      LOG.error("Region: {} stuck on {} for {} sec , newServer={}", region.getRegionNameAsString(),
        this.sourceServer, getTimeDiffInSec(startTime), this.targetServer);
    } else {
      isSuccessfulScan(region);
      LOG.info("Moved Region {} , cost (sec): {}", region.getRegionNameAsString(),
        getTimeDiffInSec(startTime));
      moved = true;
      movedRegions.add(region);
    }
    return moved;
  }

  private static String getTimeDiffInSec(long startTime) {
    return String.format("%.3f", (float) (EnvironmentEdgeManager.currentTime() - startTime) / 1000);
  }

  /**
   * Tries to scan a row from passed region
   */
  private void isSuccessfulScan(RegionInfo region) throws IOException {
    Scan scan = new Scan().withStartRow(region.getStartKey()).setRaw(true).setOneRowLimit()
      .setMaxResultSize(1L).setCaching(1).setFilter(new FirstKeyOnlyFilter())
      .setCacheBlocks(false);
    try (Table table = conn.getTable(region.getTable());
      ResultScanner scanner = table.getScanner(scan)) {
      scanner.next();
    } catch (IOException e) {
      LOG.error("Could not scan region: {}", region.getEncodedName(), e);
      throw e;
    }
  }

  /**
   * Returns true if passed region is still on serverName when we look at hbase:meta.
   * @return true if region is hosted on serverName otherwise false
   */
  private boolean isSameServer(RegionInfo region, ServerName serverName)
    throws IOException {
    ServerName serverForRegion = getServerNameForRegion(region, admin, conn);
    return serverForRegion != null && serverForRegion.equals(serverName);
  }

  /**
   * Get servername that is up in hbase:meta hosting the given region. this is hostname + port +
   * startcode comma-delimited. Can return null
   * @return regionServer hosting the given region
   */
  static ServerName getServerNameForRegion(RegionInfo region, Admin admin, Connection conn)
      throws IOException {
    if (!admin.isTableEnabled(region.getTable())) {
      return null;
    }
    HRegionLocation loc;
    try {
      loc = conn.getRegionLocator(region.getTable())
        .getRegionLocation(region.getStartKey(), region.getReplicaId(), true);
    } catch (IOException e) {
      if (e.getMessage() != null && e.getMessage().startsWith("Unable to find region for")) {
        return null;
      }
      throw e;
    }
    if (loc != null) {
      return loc.getServerName();
    } else {
      return null;
    }
  }

}
