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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.chaos.factories.MonkeyConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;

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

    Admin admin = this.context.getHBaseIntegrationTestingUtility().getHBaseAdmin();
    Collection<ServerName> serversList = admin.getClusterStatus().getServers();
    ServerName[] servers = serversList.toArray(new ServerName[serversList.size()]);

    LOG.info("Performing action: Move regions of table " + tableName);
    List<HRegionInfo> regions = admin.getTableRegions(tableName);
    if (regions == null || regions.isEmpty()) {
      LOG.info("Table " + tableName + " doesn't have regions to move");
      return;
    }

    Collections.shuffle(regions);

    long start = System.currentTimeMillis();
    for (HRegionInfo regionInfo:regions) {

      // Don't try the move if we're stopping
      if (context.isStopping()) {
        return;
      }

      try {
        String destServerName =
          servers[RandomUtils.nextInt(servers.length)].getServerName();
        LOG.debug("Moving " + regionInfo.getRegionNameAsString() + " to " + destServerName);
        admin.move(regionInfo.getEncodedNameAsBytes(), Bytes.toBytes(destServerName));
      } catch (Exception ex) {
        LOG.warn("Move failed, might be caused by other chaos: " + ex.getMessage());
      }
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
}
