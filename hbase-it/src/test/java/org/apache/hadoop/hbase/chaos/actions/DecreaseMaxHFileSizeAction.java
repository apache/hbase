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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.util.Random;

public class DecreaseMaxHFileSizeAction extends Action {

  private static final long minFileSize = 1 *  1024 * 1024 * 1024L;

  private final long sleepTime;
  private final TableName tableName;
  private final Random random;

  public DecreaseMaxHFileSizeAction(long sleepTime, TableName tableName) {
    this.sleepTime = sleepTime;
    this.tableName = tableName;
    this.random = new Random();
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtility util = context.getHBaseIntegrationTestingUtility();
    Admin admin = util.getHBaseAdmin();
    HTableDescriptor htd = admin.getTableDescriptor(tableName);

    // Try and get the current value.
    long currentValue = htd.getMaxFileSize();

    // If the current value is not set use the default for the cluster.
    // If configs are really weird this might not work.
    // That's ok. We're trying to cause chaos.
    if (currentValue <= 0) {
      currentValue =
          context.getHBaseCluster().getConf().getLong(HConstants.HREGION_MAX_FILESIZE,
              HConstants.DEFAULT_MAX_FILE_SIZE);
    }

    // Decrease by 10% at a time.
    long newValue = (long) (currentValue * 0.9);

    // We don't want to go too far below 1gb.
    // So go to about 1gb +/- 512 on each side.
    newValue = Math.max(minFileSize, newValue) - (512 - random.nextInt(1024));

    // Change the table descriptor.
    htd.setMaxFileSize(newValue);

    // Don't try the modify if we're stopping
    if (context.isStopping()) {
      return;
    }

    // modify the table.
    admin.modifyTable(tableName, htd);

    // Sleep some time.
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }
}
