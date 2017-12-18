/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Connection;

/**
 * A SpaceQuotaSnapshotNotifier implementation for testing.
 */
@InterfaceAudience.Private
public class SpaceQuotaSnapshotNotifierForTest implements SpaceQuotaSnapshotNotifier {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpaceQuotaSnapshotNotifierForTest.class);

  private final Map<TableName,SpaceQuotaSnapshot> tableQuotaSnapshots = new HashMap<>();

  @Override
  public void initialize(Connection conn) {}

  @Override
  public synchronized void transitionTable(TableName tableName, SpaceQuotaSnapshot snapshot) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Persisting " + tableName + "=>" + snapshot);
    }
    tableQuotaSnapshots.put(tableName, snapshot);
  }

  public synchronized Map<TableName,SpaceQuotaSnapshot> copySnapshots() {
    return new HashMap<>(this.tableQuotaSnapshots);
  }

  public synchronized void clearSnapshots() {
    this.tableQuotaSnapshots.clear();
  }
}
