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

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SpaceQuotaSnapshotNotifier} which uses the hbase:quota table.
 */
@InterfaceAudience.Private
public class TableSpaceQuotaSnapshotNotifier implements SpaceQuotaSnapshotNotifier {
  private static final Logger LOG = LoggerFactory.getLogger(TableSpaceQuotaSnapshotNotifier.class);

  private Connection conn;

  @Override
  public void transitionTable(
      TableName tableName, SpaceQuotaSnapshot snapshot) throws IOException {
    final Put p = QuotaTableUtil.createPutForSpaceSnapshot(tableName, snapshot);
    try (Table quotaTable = conn.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Persisting a space quota snapshot " + snapshot + " for " + tableName);
      }
      quotaTable.put(p);
    }
  }

  @Override
  public void initialize(Connection conn) {
    this.conn = conn;
  }
}
