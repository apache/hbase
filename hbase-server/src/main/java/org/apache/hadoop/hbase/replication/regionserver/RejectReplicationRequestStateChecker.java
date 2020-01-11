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

import java.util.function.BiPredicate;

import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Check whether we need to reject the replication request from source cluster.
 */
@InterfaceAudience.Private
public class RejectReplicationRequestStateChecker
    implements BiPredicate<SyncReplicationState, SyncReplicationState> {

  private static final RejectReplicationRequestStateChecker INST =
      new RejectReplicationRequestStateChecker();

  @Override
  public boolean test(SyncReplicationState state, SyncReplicationState newState) {
    return state == SyncReplicationState.ACTIVE || state == SyncReplicationState.DOWNGRADE_ACTIVE
        || newState == SyncReplicationState.ACTIVE
        || newState == SyncReplicationState.DOWNGRADE_ACTIVE;
  }

  public static RejectReplicationRequestStateChecker get() {
    return INST;
  }
}
