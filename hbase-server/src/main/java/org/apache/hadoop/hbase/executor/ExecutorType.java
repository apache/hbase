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
package org.apache.hadoop.hbase.executor;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * The following is a list of all executor types, both those that run in the
 * master and those that run in the regionserver.
 */
@InterfaceAudience.Private
public enum ExecutorType {

  // Master executor services
  MASTER_CLOSE_REGION        (1),
  MASTER_OPEN_REGION         (2),
  MASTER_SERVER_OPERATIONS   (3),
  MASTER_TABLE_OPERATIONS    (4),
  MASTER_RS_SHUTDOWN         (5),
  MASTER_META_SERVER_OPERATIONS (6),
  M_LOG_REPLAY_OPS           (7),

  // RegionServer executor services
  RS_OPEN_REGION             (20),
  RS_OPEN_ROOT               (21),
  RS_OPEN_META               (22),
  RS_CLOSE_REGION            (23),
  RS_CLOSE_ROOT              (24),
  RS_CLOSE_META              (25),
  RS_PARALLEL_SEEK           (26),
  RS_LOG_REPLAY_OPS          (27),
  RS_REGION_REPLICA_FLUSH_OPS  (28),
  RS_COMPACTED_FILES_DISCHARGER (29),
  RS_OPEN_PRIORITY_REGION    (30),
  RS_REFRESH_PEER(31),
  RS_REPLAY_SYNC_REPLICATION_WAL(32);

  ExecutorType(int value) {
  }

  /**
   * @return Conflation of the executor type and the passed {@code serverName}.
   */
  String getExecutorName(String serverName) {
    return this.toString() + "-" + serverName.replace("%", "%%");
  }
}
