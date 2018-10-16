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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementations are installed on a Replication Sink called from inside
 * ReplicationSink#replicateEntries to filter replicated WALEntries based off WALEntry attributes.
 * Currently only table name and replication write time are exposed (WALEntry is a private,
 * internal class so we cannot pass it here). To install, set
 * <code>hbase.replication.sink.walentryfilter</code> to the name of the implementing
 * class. Implementing class must have a no-param Constructor.
 * <p>This filter is of limited use. It is better to filter on the replication source rather than
 * here after the edits have been shipped on the replication sink. That said, applications such
 * as the hbase-indexer want to filter out any edits that were made before replication was enabled.
 * @see org.apache.hadoop.hbase.replication.WALEntryFilter for filtering on the replication
 * source-side.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public interface WALEntrySinkFilter {
  /**
   * Name of configuration to set with name of implementing WALEntrySinkFilter class.
   */
  public static final String WAL_ENTRY_FILTER_KEY = "hbase.replication.sink.walentrysinkfilter";

  /**
   * Called after Construction.
   * Use passed Connection to keep any context the filter might need.
   */
  void init(Connection connection);

  /**
   * @param table Table edit is destined for.
   * @param writeTime Time at which the edit was created on the source.
   * @return True if we are to filter out the edit.
   */
  boolean filter(TableName table, long writeTime);
}
