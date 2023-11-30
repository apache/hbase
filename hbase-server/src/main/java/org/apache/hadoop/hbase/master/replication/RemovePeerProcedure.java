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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RemovePeerStateData;

/**
 * The procedure for removing a replication peer.
 */
@InterfaceAudience.Private
public class RemovePeerProcedure extends ModifyPeerProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(RemovePeerProcedure.class);

  private ReplicationPeerConfig peerConfig;

  private List<Long> ongoingAssignReplicationQueuesProcIds = Collections.emptyList();

  public RemovePeerProcedure() {
  }

  public RemovePeerProcedure(String peerId) {
    super(peerId);
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.REMOVE;
  }

  @Override
  protected void prePeerModification(MasterProcedureEnv env) throws IOException {
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preRemoveReplicationPeer(peerId);
    }
    peerConfig = env.getReplicationPeerManager().preRemovePeer(peerId);
  }

  @Override
  protected void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException {
    env.getReplicationPeerManager().removePeer(peerId);
    // record ongoing AssignReplicationQueuesProcedures after we update the peer storage
    ongoingAssignReplicationQueuesProcIds = env.getMasterServices().getMasterProcedureExecutor()
      .getProcedures().stream().filter(p -> p instanceof AssignReplicationQueuesProcedure)
      .filter(p -> !p.isFinished()).map(Procedure::getProcId).collect(Collectors.toList());
  }

  private void removeRemoteWALs(MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getSyncReplicationReplayWALManager().removePeerRemoteWALs(peerId);
  }

  private void checkAssignReplicationQueuesFinished(MasterProcedureEnv env)
    throws ProcedureSuspendedException {
    if (ongoingAssignReplicationQueuesProcIds.isEmpty()) {
      LOG.info("No ongoing assign replication queues procedures when removing peer {}, move on",
        peerId);
    }
    ProcedureExecutor<MasterProcedureEnv> procExec =
      env.getMasterServices().getMasterProcedureExecutor();
    long[] unfinishedProcIds =
      ongoingAssignReplicationQueuesProcIds.stream().map(procExec::getProcedure)
        .filter(p -> p != null && !p.isFinished()).mapToLong(Procedure::getProcId).toArray();
    if (unfinishedProcIds.length == 0) {
      LOG.info(
        "All assign replication queues procedures are finished when removing peer {}, move on",
        peerId);
    } else {
      throw suspend(env.getMasterConfiguration(), backoff -> LOG.info(
        "There are still {} pending assign replication queues procedures {} when removing peer {}, sleep {} secs",
        unfinishedProcIds.length, Arrays.toString(unfinishedProcIds), peerId, backoff / 1000));
    }
  }

  @Override
  protected void postPeerModification(MasterProcedureEnv env)
    throws IOException, ReplicationException, ProcedureSuspendedException {
    checkAssignReplicationQueuesFinished(env);

    if (peerConfig.isSyncReplication()) {
      removeRemoteWALs(env);
    }
    env.getReplicationPeerManager().removeAllQueuesAndHFileRefs(peerId);
    if (peerConfig.isSerial()) {
      env.getReplicationPeerManager().removeAllLastPushedSeqIds(peerId);
    }
    LOG.info("Successfully removed peer {}", peerId);
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postRemoveReplicationPeer(peerId);
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    RemovePeerStateData.Builder builder = RemovePeerStateData.newBuilder();
    if (peerConfig != null) {
      builder.setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig));
    }
    builder.addAllOngoingAssignReplicationQueuesProcIds(ongoingAssignReplicationQueuesProcIds);
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    RemovePeerStateData data = serializer.deserialize(RemovePeerStateData.class);
    if (data.hasPeerConfig()) {
      this.peerConfig = ReplicationPeerConfigUtil.convert(data.getPeerConfig());
    }
    ongoingAssignReplicationQueuesProcIds = data.getOngoingAssignReplicationQueuesProcIdsList();
  }
}
