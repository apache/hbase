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
package org.apache.hadoop.hbase.replication.regionserver;

import java.util.Optional;
import java.util.function.BiPredicate;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Get the information for a sync replication peer.
 */
@InterfaceAudience.Private
public interface SyncReplicationPeerInfoProvider {

  /**
   * Return the peer id and remote WAL directory if the table is synchronously replicated and the
   * state is {@link SyncReplicationState#ACTIVE}.
   */
  Optional<Pair<String, String>> getPeerIdAndRemoteWALDir(TableName table);

  /**
   * Check whether the given table is contained in a sync replication peer which can pass the state
   * checker.
   * <p>
   * Will call the checker with current sync replication state and new sync replication state.
   */
  boolean checkState(TableName table,
      BiPredicate<SyncReplicationState, SyncReplicationState> checker);
}
