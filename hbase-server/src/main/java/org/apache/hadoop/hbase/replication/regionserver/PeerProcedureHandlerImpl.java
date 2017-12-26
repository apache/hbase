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

import java.io.IOException;

import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class PeerProcedureHandlerImpl implements PeerProcedureHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PeerProcedureHandlerImpl.class);

  private ReplicationSourceManager replicationSourceManager;

  public PeerProcedureHandlerImpl(ReplicationSourceManager replicationSourceManager) {
    this.replicationSourceManager = replicationSourceManager;
  }

  @Override
  public void addPeer(String peerId) throws ReplicationException, IOException {
    replicationSourceManager.addPeer(peerId);
  }

  @Override
  public void removePeer(String peerId) throws ReplicationException, IOException {
    replicationSourceManager.removePeer(peerId);
  }

  @Override
  public void disablePeer(String peerId) throws ReplicationException, IOException {
    ReplicationPeerImpl peer =
        replicationSourceManager.getReplicationPeers().getConnectedPeer(peerId);
    if (peer != null) {
      peer.refreshPeerState();
      LOG.info("disable replication peer, id: " + peerId + ", new state: " + peer.getPeerState());
    } else {
      throw new ReplicationException("No connected peer found, peerId=" + peerId);
    }
  }

  @Override
  public void enablePeer(String peerId) throws ReplicationException, IOException {
    ReplicationPeerImpl peer =
        replicationSourceManager.getReplicationPeers().getConnectedPeer(peerId);
    if (peer != null) {
      peer.refreshPeerState();
      LOG.info("enable replication peer, id: " + peerId + ", new state: " + peer.getPeerState());
    } else {
      throw new ReplicationException("No connected peer found, peerId=" + peerId);
    }
  }

  @Override
  public void updatePeerConfig(String peerId) throws ReplicationException, IOException {
    ReplicationPeerImpl peer =
        replicationSourceManager.getReplicationPeers().getConnectedPeer(peerId);
    if (peer == null) {
      throw new ReplicationException("No connected peer found, peerId=" + peerId);
    }
    peer.refreshPeerConfig();
  }
}
