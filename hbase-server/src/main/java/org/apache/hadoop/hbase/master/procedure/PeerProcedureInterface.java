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
package org.apache.hadoop.hbase.master.procedure;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface PeerProcedureInterface {

  enum PeerOperationType {
    ADD, REMOVE, ENABLE, DISABLE, UPDATE_CONFIG, REFRESH, TRANSIT_SYNC_REPLICATION_STATE,
    RECOVER_STANDBY, SYNC_REPLICATION_REPLAY_WAL, SYNC_REPLICATION_REPLAY_WAL_REMOTE
  }

  String getPeerId();

  PeerOperationType getPeerOperationType();
}
