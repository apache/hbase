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

package org.apache.hadoop.hbase.chaos.actions;

import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Action that tries to split a random region of a table.
*/
public class SplitRandomRegionOfTableAction extends Action {
  private static final Logger LOG =
      LoggerFactory.getLogger(SplitRandomRegionOfTableAction.class);
  private final long sleepTime;
  private final TableName tableName;

  public SplitRandomRegionOfTableAction(TableName tableName) {
    this(-1, tableName);
  }

  public SplitRandomRegionOfTableAction(int sleepTime, TableName tableName) {
    this.sleepTime = sleepTime;
    this.tableName = tableName;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
    Admin admin = util.getAdmin();

    getLogger().info("Performing action: Split random region of table " + tableName);
    List<RegionInfo> regions = admin.getRegions(tableName);
    if (regions == null || regions.isEmpty()) {
      getLogger().info("Table " + tableName + " doesn't have regions to split");
      return;
    }
    // Don't try the split if we're stopping
    if (context.isStopping()) {
      return;
    }

    RegionInfo region = PolicyBasedChaosMonkey.selectRandomItem(
        regions.toArray(new RegionInfo[0]));
    getLogger().debug("Splitting region " + region.getRegionNameAsString());
    try {
      admin.splitRegionAsync(region.getRegionName()).get();
    } catch (Exception ex) {
      getLogger().warn("Split failed, might be caused by other chaos: " + ex.getMessage());
    }
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }
}
