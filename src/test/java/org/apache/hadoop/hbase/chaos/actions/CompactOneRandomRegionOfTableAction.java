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
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collection;

/**
 * Region that queues a compaction of a random region from the table.
 */
public class CompactOneRandomRegionOfTableAction extends Action {
  private final byte[] tableNameBytes;
  private final int majorRatio;
  private final long sleepTime;
  private final String tableName;

  public CompactOneRandomRegionOfTableAction(
      String tableName, float majorRatio) {
    this(-1, tableName, majorRatio);
  }

  public CompactOneRandomRegionOfTableAction(
      int sleepTime, String tableName, float majorRatio) {
    this.tableNameBytes = Bytes.toBytes(tableName);
    this.majorRatio = (int) (100 * majorRatio);
    this.sleepTime = sleepTime;
    this.tableName = tableName;
  }

  @Override
  public void perform() throws Exception {
    IntegrationTestingUtility util = context.getHBaseIntegrationTestingUtility();
    HBaseAdmin admin = util.getHBaseAdmin();
    HTable ht = new HTable(util.getConfiguration(), tableName);

    boolean major = RandomUtils.nextInt(100) < majorRatio;

    LOG.info("Performing action: Compact random region of table "
        + tableName + ", major=" + major);

    Collection<HRegionInfo> regions = ht.getRegionsInfo().keySet();
    if (regions == null || regions.isEmpty()) {
      LOG.info("Table " + tableName + " doesn't have regions to compact");
      return;
    }

    HRegionInfo region = PolicyBasedChaosMonkey.selectRandomItem(
        regions.toArray(new HRegionInfo[regions.size()]));

    ht.close();

    if (major) {
      LOG.debug("Major compacting region " + region.getRegionNameAsString());
      admin.majorCompact(region.getRegionName());
    } else {
      LOG.debug("Compacting region " + region.getRegionNameAsString());
      admin.compact(region.getRegionName());
    }
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }

}
