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
package org.apache.hadoop.hbase.replication;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationGlobalSourceSource;
import org.apache.hadoop.hbase.replication.regionserver.RecoveredReplicationSource;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to control all replication sources inside one RegionServer or ReplicationServer.
 * Used by {@link org.apache.hadoop.hbase.replication.regionserver.ReplicationSource} or
 * {@link RecoveredReplicationSource}.
 */
@InterfaceAudience.Private
public interface ReplicationSourceController {

  /**
   * Returns the maximum size in bytes of edits held in memory which are pending replication
   * across all sources inside this RegionServer or ReplicationServer.
   */
  long getTotalBufferLimit();

  AtomicLong getTotalBufferUsed();

  MetricsReplicationGlobalSourceSource getGlobalMetrics();

  /**
   * Call this when the recovered replication source replicated all WALs.
   */
  void finishRecoveredSource(RecoveredReplicationSource src);
}
