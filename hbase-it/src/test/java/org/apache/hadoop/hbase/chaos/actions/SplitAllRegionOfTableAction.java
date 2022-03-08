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

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitAllRegionOfTableAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(SplitAllRegionOfTableAction.class);
  private static final int DEFAULT_MAX_SPLITS = 3;
  private static final String MAX_SPLIT_KEY = "hbase.chaosmonkey.action.maxFullTableSplits";

  private final TableName tableName;
  private int maxFullTableSplits = DEFAULT_MAX_SPLITS;
  private int splits = 0;

  public SplitAllRegionOfTableAction(TableName tableName) {
    this.tableName = tableName;
  }

  public void init(ActionContext context) throws IOException {
    super.init(context);
    this.maxFullTableSplits = getConf().getInt(MAX_SPLIT_KEY, DEFAULT_MAX_SPLITS);
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
    Admin admin = util.getAdmin();
    // Don't try the split if we're stopping
    if (context.isStopping()) {
      return;
    }
    // Don't always split. This should allow splitting of a full table later in the run
    if (ThreadLocalRandom.current().nextDouble()
        < (((double) splits) / ((double) maxFullTableSplits)) / ((double) 2)) {
      splits++;
      getLogger().info("Performing action: Split all regions of {}", tableName);
      admin.split(tableName);
    } else {
      getLogger().info("Skipping split of all regions.");
    }
  }
}
