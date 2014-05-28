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

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Action that tries to move every region of a table.
 */
public class MoveRegionsOfTableAction extends Action {
  private final long sleepTime;
  private final byte[] tableNameBytes;
  private final String tableName;
  private final long maxTime;

  public MoveRegionsOfTableAction(String tableName) {
    this(-1, tableName);
  }

  public MoveRegionsOfTableAction(long sleepTime, String tableName) {
    this.sleepTime = sleepTime;
    this.tableNameBytes = Bytes.toBytes(tableName);
    this.tableName = tableName;
    this.maxTime = 10 * 60 * 1000; // 10 min default
  }

  @Override
  public void perform() throws Exception {
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }

    final IntegrationTestingUtility util = this.context.getHBaseIntegrationTestingUtility();
    HBaseAdmin admin = util.getHBaseAdmin();
    Collection<HServerInfo> serversList = admin.getClusterStatus().getServerInfo();
    HServerInfo[] servers = serversList.toArray(new HServerInfo[serversList.size()]);
    HTable ht = new HTable(util.getConfiguration(), tableName);

    LOG.info("Performing action: Move regions of table " + tableName);
    List<HRegionInfo> regions = Lists.newArrayList(ht.getRegionsInfo().keySet());
    if (regions == null || regions.isEmpty()) {
      LOG.info("Table " + tableName + " doesn't have regions to move");
      return;
    }

    ht.close();

    Collections.shuffle(regions);

    long start = System.currentTimeMillis();
    for (HRegionInfo regionInfo : regions) {
      try {
        String destHServerAddress =
            servers[RandomUtils.nextInt(servers.length)].getHostnamePort();
        LOG.debug("Moving " + regionInfo.getRegionNameAsString() + " to " + destHServerAddress);
        admin.moveRegion(regionInfo.getRegionName(), destHServerAddress);
      } catch (Exception e) {
        LOG.debug("Error moving region", e);
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
