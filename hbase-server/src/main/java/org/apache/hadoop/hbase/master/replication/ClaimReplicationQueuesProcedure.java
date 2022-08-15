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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ClaimReplicationQueuesStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Used to assign the replication queues of a dead server to other region servers.
 * @deprecated Use {@link AssignReplicationQueuesProcedure} instead, kept only for keeping
 *             compatibility.
 */
@Deprecated
@InterfaceAudience.Private
public class ClaimReplicationQueuesProcedure extends Procedure<MasterProcedureEnv>
  implements ServerProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(ClaimReplicationQueuesProcedure.class);

  private ServerName crashedServer;

  private RetryCounter retryCounter;

  public ClaimReplicationQueuesProcedure() {
  }

  public ClaimReplicationQueuesProcedure(ServerName crashedServer) {
    this.crashedServer = crashedServer;
  }

  @Override
  public ServerName getServerName() {
    return crashedServer;
  }

  @Override
  public boolean hasMetaTableRegion() {
    return false;
  }

  @Override
  public ServerOperationType getServerOperationType() {
    return ServerOperationType.CLAIM_REPLICATION_QUEUES;
  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    ReplicationQueueStorage storage = env.getReplicationPeerManager().getQueueStorage();
    try {
      List<ReplicationQueueId> queues = storage.listAllQueueIds(crashedServer);
      Set<String> existQueuePeerIds = new HashSet<>();
      // this is for upgrading to the new region replication framework, where we will delete the
      // legacy region_replica_replication peer directly, without deleting the replication queues
      for (Iterator<ReplicationQueueId> iter = queues.iterator(); iter.hasNext();) {
        ReplicationQueueId queueId = iter.next();
        if (queueId.getPeerId().equals(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER)) {
          LOG.info("Found replication queue {} for legacy region replication peer, "
            + "skipping claiming and removing...", queueId);
          iter.remove();
          storage.removeQueue(queueId);
        } else if (!queueId.isRecovered()) {
          existQueuePeerIds.add(queueId.getPeerId());
        }
      }
      List<ReplicationPeerDescription> peers = env.getReplicationPeerManager().listPeers(null);
      // TODO: the implementation is not enough yet, if there are retries, we need to know whether
      // the replication queue for the given peer has been claimed or not, otherwise this logic will
      // introduce redundant replication queues for the same peer. Add this logic to make some UTs
      // pass first.
      for (ReplicationPeerDescription peer : peers) {
        if (!existQueuePeerIds.contains(peer.getPeerId())) {
          ReplicationQueueId queueId = new ReplicationQueueId(crashedServer, peer.getPeerId());
          env.getReplicationPeerManager().getQueueStorage().setOffset(queueId,
            crashedServer.toString(), ReplicationGroupOffset.BEGIN, Collections.emptyMap());
          queues.add(queueId);
        }
      }
      if (queues.isEmpty()) {
        LOG.debug("Finish claiming replication queues for {}", crashedServer);
        // we are done
        return null;
      }
      LOG.debug("There are {} replication queues need to be claimed for {}", queues.size(),
        crashedServer);
      List<ServerName> targetServers =
        env.getMasterServices().getServerManager().getOnlineServersList();
      if (targetServers.isEmpty()) {
        throw new ReplicationException("no region server available");
      }
      Collections.shuffle(targetServers);
      ClaimReplicationQueueRemoteProcedure[] procs =
        new ClaimReplicationQueueRemoteProcedure[Math.min(queues.size(), targetServers.size())];
      for (int i = 0; i < procs.length; i++) {
        procs[i] = new ClaimReplicationQueueRemoteProcedure(queues.get(i), targetServers.get(i));
      }
      return procs;
    } catch (ReplicationException e) {
      if (retryCounter == null) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.warn("Failed to claim replication queues for {}, suspend {}secs {}; {};", crashedServer,
        backoff / 1000, e);
      setTimeout(Math.toIntExact(backoff));
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    serializer.serialize(ClaimReplicationQueuesStateData.newBuilder()
      .setCrashedServer(ProtobufUtil.toServerName(crashedServer)).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    ClaimReplicationQueuesStateData data =
      serializer.deserialize(ClaimReplicationQueuesStateData.class);
    crashedServer = ProtobufUtil.toServerName(data.getCrashedServer());
  }
}
