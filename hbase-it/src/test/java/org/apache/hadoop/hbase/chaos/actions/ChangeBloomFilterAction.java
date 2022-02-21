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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action that tries to adjust the bloom filter setting on all the columns of a
 * table
 */
public class ChangeBloomFilterAction extends Action {
  private final long sleepTime;
  private final TableName tableName;
  private static final Logger LOG = LoggerFactory.getLogger(ChangeBloomFilterAction.class);

  public ChangeBloomFilterAction(TableName tableName) {
    this(-1, tableName);
  }

  public ChangeBloomFilterAction(int sleepTime, TableName tableName) {
    this.sleepTime = sleepTime;
    this.tableName = tableName;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    final BloomType[] bloomArray = BloomType.values();
    final int bloomArraySize = bloomArray.length;

    getLogger().info("Performing action: Change bloom filter on all columns of table " + tableName);

    modifyAllTableColumns(tableName, (columnName, columnBuilder) -> {
      BloomType bloomType = bloomArray[ThreadLocalRandom.current().nextInt(bloomArraySize)];
      getLogger().debug("Performing action: About to set bloom filter type to "
          + bloomType + " on column " + columnName + " of table " + tableName);
      columnBuilder.setBloomFilterType(bloomType);
      if (bloomType == BloomType.ROWPREFIX_FIXED_LENGTH) {
        columnBuilder.setConfiguration(BloomFilterUtil.PREFIX_LENGTH_KEY, "10");
      }
    });

    getLogger().debug("Performing action: Just set bloom filter types on table " + tableName);
  }
}
