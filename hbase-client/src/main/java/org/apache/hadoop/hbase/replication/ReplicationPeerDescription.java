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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * The POJO equivalent of ReplicationProtos.ReplicationPeerDescription.
 * <p>
 * To developer, here we do not store the new sync replication state since it is just an
 * intermediate state and this class is public.
 */
@InterfaceAudience.Public
public class ReplicationPeerDescription {

  private final String id;
  private final boolean enabled;
  private final ReplicationPeerConfig config;
  private final SyncReplicationState syncReplicationState;

  public ReplicationPeerDescription(String id, boolean enabled, ReplicationPeerConfig config,
      SyncReplicationState syncReplicationState) {
    this.id = id;
    this.enabled = enabled;
    this.config = config;
    this.syncReplicationState = syncReplicationState;
  }

  public String getPeerId() {
    return this.id;
  }

  public boolean isEnabled() {
    return this.enabled;
  }

  public ReplicationPeerConfig getPeerConfig() {
    return this.config;
  }

  public SyncReplicationState getSyncReplicationState() {
    return this.syncReplicationState;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("id : ").append(id);
    builder.append(", enabled : " + enabled);
    builder.append(", config : " + config);
    builder.append(", syncReplicationState : " + syncReplicationState);
    return builder.toString();
  }
}
