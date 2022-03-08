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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action that tries to truncate of a table.
 */
public class TruncateTableAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(TruncateTableAction.class);
  private final TableName tableName;

  public TruncateTableAction(String tableName) {
    this.tableName = TableName.valueOf(tableName);
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
    Admin admin = util.getAdmin();

    // Don't try the truncate if we're stopping
    if (context.isStopping()) {
      return;
    }

    boolean preserveSplits = ThreadLocalRandom.current().nextBoolean();
    getLogger().info("Performing action: Truncate table {} preserve splits {}",
      tableName.getNameAsString(), preserveSplits);
    admin.truncateTable(tableName, preserveSplits);
  }
}
