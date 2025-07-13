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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Action that tries to take a snapshot of a table.
 */
public class SnapshotTableAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotTableAction.class);
  private final TableName tableName;
  private final long sleepTime;
  private final Map<String, Object> snapshotProps;

  public SnapshotTableAction(TableName tableName, long ttl) {
    this(-1, tableName, ttl);
  }

  public SnapshotTableAction(int sleepTime, TableName tableName, long ttl) {
    this.tableName = tableName;
    this.sleepTime = sleepTime;
    if (ttl > 0) {
      snapshotProps = ImmutableMap.of("TTL", ttl);
    } else {
      snapshotProps = Collections.emptyMap();
    }
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    HBaseTestingUtil util = context.getHBaseIntegrationTestingUtility();
    String snapshotName = tableName + "-it-" + EnvironmentEdgeManager.currentTime();
    Admin admin = util.getAdmin();

    // Don't try the snapshot if we're stopping
    if (context.isStopping()) {
      return;
    }

    getLogger().info("Performing action: Snapshot table {}", tableName);
    SnapshotType type =
      ThreadLocalRandom.current().nextBoolean() ? SnapshotType.FLUSH : SnapshotType.SKIPFLUSH;
    admin.snapshot(snapshotName, tableName, type, snapshotProps);
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
  }
}
