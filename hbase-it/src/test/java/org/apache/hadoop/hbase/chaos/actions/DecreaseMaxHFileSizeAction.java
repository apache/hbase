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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecreaseMaxHFileSizeAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(DecreaseMaxHFileSizeAction.class);

  private static final long minFileSize = 1024 * 1024 * 1024L;

  private final long sleepTime;
  private final TableName tableName;
  private Admin admin;

  public DecreaseMaxHFileSizeAction(long sleepTime, TableName tableName) {
    this.sleepTime = sleepTime;
    this.tableName = tableName;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void init(ActionContext context) throws IOException {
    super.init(context);
    this.admin = context.getHBaseIntegrationTestingUtility().getAdmin();
  }

  @Override
  public void perform() throws Exception {
    TableDescriptor td = admin.getDescriptor(tableName);

    // Try and get the current value.
    long currentValue = td.getMaxFileSize();

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
    newValue = Math.max(minFileSize, newValue) -
        (512 - ThreadLocalRandom.current().nextInt(1024));

    // Change the table descriptor.
    TableDescriptor modifiedTable =
        TableDescriptorBuilder.newBuilder(td).setMaxFileSize(newValue).build();

    // Don't try the modify if we're stopping
    if (context.isStopping()) {
      return;
    }

    // modify the table.
    admin.modifyTable(modifiedTable);

    // Sleep some time.
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }
}
