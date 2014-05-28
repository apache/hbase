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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collection;

/**
 * Action that tries to move a random region of a table.
 */
public class MoveRandomRegionOfTableAction extends Action {
  private final long sleepTime;
  private final byte[] tableNameBytes;
  private final String tableName;

  public MoveRandomRegionOfTableAction(String tableName) {
    this(-1, tableName);
  }

  public MoveRandomRegionOfTableAction(long sleepTime, String tableName) {
    this.sleepTime = sleepTime;
    this.tableNameBytes = Bytes.toBytes(tableName);
    this.tableName = tableName;
  }

  @Override
  public void perform() throws Exception {
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }

    IntegrationTestingUtility util = context.getHBaseIntegrationTestingUtility();

    HBaseAdmin admin = util.getHBaseAdmin();
    HTable ht = new HTable(util.getConfiguration(), tableName);

    LOG.info("Performing action: Move random region of table " + tableName);
    Collection<HRegionInfo> regions = regions = ht.getRegionsInfo().keySet();
    HServerAddress[] servers = getCurrentServers();
    if (regions == null || regions.isEmpty()) {
      LOG.info("Table " + tableName + " doesn't have regions to move");
      return;
    }

    HRegionInfo region = PolicyBasedChaosMonkey.selectRandomItem(
        regions.toArray(new HRegionInfo[regions.size()]));

    HServerAddress server = PolicyBasedChaosMonkey.selectRandomItem(servers);

    LOG.debug("Moving region " + region.getRegionNameAsString());
    admin.moveRegion(region.getRegionName(), server.getHostNameWithPort());
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }
}
