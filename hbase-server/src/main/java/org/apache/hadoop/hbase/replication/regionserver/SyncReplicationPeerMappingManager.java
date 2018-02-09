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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to map region to sync replication peer id.
 * <p>
 * TODO: now only support include table options.
 */
@InterfaceAudience.Private
class SyncReplicationPeerMappingManager {

  private final ConcurrentMap<TableName, String> table2PeerId = new ConcurrentHashMap<>();

  void add(String peerId, ReplicationPeerConfig peerConfig) {
    peerConfig.getTableCFsMap().keySet().forEach(tn -> table2PeerId.put(tn, peerId));
  }

  void remove(String peerId, ReplicationPeerConfig peerConfig) {
    peerConfig.getTableCFsMap().keySet().forEach(table2PeerId::remove);
  }

  String getPeerId(RegionInfo info) {
    return table2PeerId.get(info.getTable());
  }
}
