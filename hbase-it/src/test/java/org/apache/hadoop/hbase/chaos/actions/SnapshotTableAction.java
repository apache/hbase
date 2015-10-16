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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

/**
* Action that tries to take a snapshot of a table.
*/
public class SnapshotTableAction extends Action {
  private final TableName tableName;
  private final long sleepTime;

  public SnapshotTableAction(TableName tableName) {
    this(-1, tableName);
  }

  public SnapshotTableAction(int sleepTime, TableName tableName) {
    this.tableName = tableName;
    this.sleepTime = sleepTime;
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
    String snapshotName = tableName + "-it-" + System.currentTimeMillis();
    Admin admin = util.getHBaseAdmin();

    // Don't try the snapshot if we're stopping
    if (context.isStopping()) {
      return;
    }

    LOG.info("Performing action: Snapshot table " + tableName);
    admin.snapshot(snapshotName, tableName);
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }
}
