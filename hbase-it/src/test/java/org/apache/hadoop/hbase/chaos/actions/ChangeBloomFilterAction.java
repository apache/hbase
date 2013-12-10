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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Action that tries to adjust the bloom filter setting on all the columns of a
 * table
 */
public class ChangeBloomFilterAction extends Action {
  private final byte[] tableNameBytes;
  private final long sleepTime;
  private final String tableName;

  public ChangeBloomFilterAction(String tableName) {
    this(-1, tableName);
  }

  public ChangeBloomFilterAction(int sleepTime, String tableName) {
    this.tableNameBytes = Bytes.toBytes(tableName);
    this.sleepTime = sleepTime;
    this.tableName = tableName;
  }

  @Override
  public void perform() throws Exception {
    Random random = new Random();
    HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
    HBaseAdmin admin = util.getHBaseAdmin();

    LOG.info("Performing action: Change bloom filter on all columns of table "
        + tableName);
    HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes
        .toBytes(tableName));
    HColumnDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();

    if (columnDescriptors == null || columnDescriptors.length == 0) {
      return;
    }

    final BloomType[] bloomArray = BloomType.values();
    final int bloomArraySize = bloomArray.length;

    for (HColumnDescriptor descriptor : columnDescriptors) {
      int bloomFilterIndex = random.nextInt(bloomArraySize);
      LOG.debug("Performing action: About to set bloom filter type to "
          + bloomArray[bloomFilterIndex] + " on column "
          + descriptor.getNameAsString() + " of table " + tableName);
      descriptor.setBloomFilterType(bloomArray[bloomFilterIndex]);
      LOG.debug("Performing action: Just set bloom filter type to "
          + bloomArray[bloomFilterIndex] + " on column "
          + descriptor.getNameAsString() + " of table " + tableName);
    }

    admin.modifyTable(tableName, tableDescriptor);
  }
}
