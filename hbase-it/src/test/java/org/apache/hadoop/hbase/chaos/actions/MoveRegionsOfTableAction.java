/**
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

package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.chaos.factories.MonkeyConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;

/**
* Action that tries to move every region of a table.
*/
public class MoveRegionsOfTableAction extends Action {
  private final long sleepTime;
  private final TableName tableName;
  private final long maxTime;

  public MoveRegionsOfTableAction(TableName tableName) {
    this(-1, MonkeyConstants.DEFAULT_MOVE_REGIONS_MAX_TIME, tableName);
  }

  public MoveRegionsOfTableAction(long sleepTime, long maxSleepTime, TableName tableName) {
    this.sleepTime = sleepTime;
    this.tableName = tableName;
    this.maxTime = maxSleepTime;
  }

  @Override
  public void perform() throws Exception {
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }

    Admin admin = this.context.getHBaseIntegrationTestingUtility().getAdmin();
    ServerName[] servers = getServers(admin);

    LOG.info("Performing action: Move regions of table {}", tableName);
    List<RegionInfo> regions = admin.getRegions(tableName);
    if (regions == null || regions.isEmpty()) {
      LOG.info("Table {} doesn't have regions to move", tableName);
      return;
    }

    Collections.shuffle(regions);

    long start = System.currentTimeMillis();
    for (RegionInfo regionInfo : regions) {
      // Don't try the move if we're stopping
      if (context.isStopping()) {
        return;
      }

      moveRegion(admin, servers, regionInfo);
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }

      // put a limit on max num regions. Otherwise, this won't finish
      // with a sleep time of 10sec, 100 regions will finish in 16min
      if (System.currentTimeMillis() - start > maxTime) {
        break;
      }
    }
  }

  static ServerName [] getServers(Admin admin) throws IOException {
    Collection<ServerName> serversList =
        admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics().keySet();
    return serversList.toArray(new ServerName[serversList.size()]);
  }

  static void moveRegion(Admin admin, ServerName [] servers, RegionInfo regionInfo) {
    try {
      ServerName destServerName = servers[RandomUtils.nextInt(0, servers.length)];
      LOG.debug("Moving {} to {}", regionInfo.getRegionNameAsString(), destServerName);
      admin.move(regionInfo.getEncodedNameAsBytes(), destServerName);
    } catch (Exception ex) {
      LOG.warn("Move failed, might be caused by other chaos: {}", ex.getMessage());
    }
  }
}
