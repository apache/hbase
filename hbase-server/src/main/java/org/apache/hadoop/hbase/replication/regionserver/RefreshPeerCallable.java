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

import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.RSProcedureCallable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshPeerParameter;

/**
 * The callable executed at RS side to refresh the peer config/state. <br/>
 */
@InterfaceAudience.Private
public class RefreshPeerCallable implements RSProcedureCallable {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshPeerCallable.class);

  private HRegionServer rs;

  private String peerId;

  private PeerModificationType type;

  private Exception initError;

  @Override
  public Void call() throws Exception {
    if (initError != null) {
      throw initError;
    }

    LOG.info("Received a peer change event, peerId=" + peerId + ", type=" + type);
    PeerProcedureHandler handler = rs.getReplicationSourceService().getPeerProcedureHandler();
    switch (type) {
      case ADD_PEER:
        handler.addPeer(this.peerId);
        break;
      case REMOVE_PEER:
        handler.removePeer(this.peerId);
        break;
      case ENABLE_PEER:
        handler.enablePeer(this.peerId);
        break;
      case DISABLE_PEER:
        handler.disablePeer(this.peerId);
        break;
      case UPDATE_PEER_CONFIG:
        handler.updatePeerConfig(this.peerId);
        break;
      default:
        throw new IllegalArgumentException("Unknown peer modification type: " + type);
    }
    return null;
  }

  @Override
  public void init(byte[] parameter, HRegionServer rs) {
    this.rs = rs;
    try {
      RefreshPeerParameter param = RefreshPeerParameter.parseFrom(parameter);
      this.peerId = param.getPeerId();
      this.type = param.getType();
    } catch (InvalidProtocolBufferException e) {
      initError = e;
    }
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_REFRESH_PEER;
  }
}
