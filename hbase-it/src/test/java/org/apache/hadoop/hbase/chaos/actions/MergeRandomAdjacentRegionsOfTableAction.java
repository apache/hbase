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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

/**
 * Action to merge regions of a table.
 */
public class MergeRandomAdjacentRegionsOfTableAction extends Action {
  private final TableName tableName;
  private final long sleepTime;

  public MergeRandomAdjacentRegionsOfTableAction(TableName tableName) {
    this(-1, tableName);
  }

  public MergeRandomAdjacentRegionsOfTableAction(int sleepTime, TableName tableName) {
    this.tableName = tableName;
    this.sleepTime = sleepTime;
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
    Admin admin = util.getAdmin();

    LOG.info("Performing action: Merge random adjacent regions of table " + tableName);
    List<HRegionInfo> regions = admin.getTableRegions(tableName);
    if (regions == null || regions.size() < 2) {
      LOG.info("Table " + tableName + " doesn't have enough regions to merge");
      return;
    }

    int i = RandomUtils.nextInt(0, regions.size() - 1);
    HRegionInfo a = regions.get(i++);
    HRegionInfo b = regions.get(i);
    LOG.debug("Merging " + a.getRegionNameAsString() + " and " + b.getRegionNameAsString());

    // Don't try the merge if we're stopping
    if (context.isStopping()) {
      return;
    }

    try {
      admin.mergeRegionsAsync(a.getEncodedNameAsBytes(), b.getEncodedNameAsBytes(), false);
    } catch (Exception ex) {
      LOG.warn("Merge failed, might be caused by other chaos: " + ex.getMessage());
    }
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }
}
