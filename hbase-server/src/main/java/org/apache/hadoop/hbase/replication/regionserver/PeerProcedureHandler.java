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

import java.io.IOException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A handler for modifying replication peer in peer procedures.
 */
@InterfaceAudience.Private
public interface PeerProcedureHandler {

  void addPeer(String peerId) throws ReplicationException, IOException;

  void removePeer(String peerId) throws ReplicationException, IOException;

  void disablePeer(String peerId) throws ReplicationException, IOException;

  void enablePeer(String peerId) throws ReplicationException, IOException;

  void updatePeerConfig(String peerId) throws ReplicationException, IOException;

  void transitSyncReplicationPeerState(String peerId, int stage, HRegionServer rs)
    throws ReplicationException, IOException;

  void claimReplicationQueue(ReplicationQueueId queueId) throws ReplicationException, IOException;
}
