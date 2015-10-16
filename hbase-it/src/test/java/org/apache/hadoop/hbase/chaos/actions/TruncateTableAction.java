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

import java.util.Random;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;

/**
 * Action that tries to truncate of a table.
 */
public class TruncateTableAction extends Action {
  private final TableName tableName;
  private final Random random;

  public TruncateTableAction(String tableName) {
    this.tableName = TableName.valueOf(tableName);
    this.random = new Random();
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
    Admin admin = util.getHBaseAdmin();

    // Don't try the truncate if we're stopping
    if (context.isStopping()) {
      return;
    }

    boolean preserveSplits = random.nextBoolean();
    LOG.info("Performing action: Truncate table " + tableName.getNameAsString() +
             "preserve splits " + preserveSplits);
    admin.truncateTable(tableName, preserveSplits);
  }
}
