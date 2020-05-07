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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Action that tries to flush a table.
*/
public class FlushTableAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(FlushTableAction.class);
  private final long sleepTime;
  private final TableName tableName;

  public FlushTableAction(TableName tableName) {
    this(-1, tableName);
  }

  public FlushTableAction(int sleepTime, TableName tableName) {
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

    // Don't try the flush if we're stopping
    if (context.isStopping()) {
      return;
    }

    getLogger().info("Performing action: Flush table " + tableName);
    try {
      admin.flush(tableName);
    } catch (Exception ex) {
      getLogger().warn("Flush failed, might be caused by other chaos: " + ex.getMessage());
    }
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }
}
