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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Action that queues a table compaction.
 */
public class CompactTableAction extends Action {
  private final byte[] tableNameBytes;
  private final int majorRatio;
  private final long sleepTime;
  private final String tableName;

  public CompactTableAction(String tableName, float majorRatio) {
    this(-1, tableName, majorRatio);
  }

  public CompactTableAction(
      int sleepTime, String tableName, float majorRatio) {
    this.tableNameBytes = Bytes.toBytes(tableName);
    this.majorRatio = (int) (100 * majorRatio);
    this.sleepTime = sleepTime;
    this.tableName = tableName;
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
    HBaseAdmin admin = util.getHBaseAdmin();
    boolean major = RandomUtils.nextInt(100) < majorRatio;

    LOG.info("Performing action: Compact table " + tableName + ", major=" + major);
    try {
      if (major) {
        admin.majorCompact(tableNameBytes);
      } else {
        admin.compact(tableNameBytes);
      }
    } catch (Exception ex) {
      LOG.warn("Compaction failed, might be caused by other chaos: " + ex.getMessage());
    }
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }
}
