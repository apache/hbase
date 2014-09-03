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

import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.client.Admin;

/**
 * Region that queues a compaction of a random region from the table.
 */
public class CompactRandomRegionOfTableAction extends Action {
  private final int majorRatio;
  private final long sleepTime;
  private final TableName tableName;

  public CompactRandomRegionOfTableAction(
      TableName tableName, float majorRatio) {
    this(-1, tableName, majorRatio);
  }

  public CompactRandomRegionOfTableAction(
      int sleepTime, TableName tableName, float majorRatio) {
    this.majorRatio = (int) (100 * majorRatio);
    this.sleepTime = sleepTime;
    this.tableName = tableName;
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
    Admin admin = util.getHBaseAdmin();
    boolean major = RandomUtils.nextInt(100) < majorRatio;

    LOG.info("Performing action: Compact random region of table "
      + tableName + ", major=" + major);
    List<HRegionInfo> regions = admin.getTableRegions(tableName);
    if (regions == null || regions.isEmpty()) {
      LOG.info("Table " + tableName + " doesn't have regions to compact");
      return;
    }

    HRegionInfo region = PolicyBasedChaosMonkey.selectRandomItem(
      regions.toArray(new HRegionInfo[regions.size()]));

    try {
      if (major) {
        LOG.debug("Major compacting region " + region.getRegionNameAsString());
        admin.majorCompactRegion(region.getRegionName());
      } else {
        LOG.debug("Compacting region " + region.getRegionNameAsString());
        admin.compactRegion(region.getRegionName());
      }
    } catch (Exception ex) {
      LOG.warn("Compaction failed, might be caused by other chaos: " + ex.getMessage());
    }
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }
}
