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

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_PEER;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_PEER;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_MIGRATE;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_PREPARE;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_WAIT_UPGRADING;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.master.procedure.GlobalProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * A procedure for migrating replication queue data from zookeeper to hbase:replication table.
 */
@InterfaceAudience.Private
public class MigrateReplicationQueueFromZkToTableProcedure
  extends StateMachineProcedure<MasterProcedureEnv, MigrateReplicationQueueFromZkToTableState>
  implements GlobalProcedureInterface {

  private static final Logger LOG =
    LoggerFactory.getLogger(MigrateReplicationQueueFromZkToTableProcedure.class);

  private static final int MIN_MAJOR_VERSION = 3;

  private List<String> disabledPeerIds;

  private List<Future<?>> futures;

  private ExecutorService executor;

  @Override
  public String getGlobalId() {
    return getClass().getSimpleName();
  }

  private ExecutorService getExecutorService() {
    if (executor == null) {
      executor = Executors.newFixedThreadPool(3, new ThreadFactoryBuilder()
        .setNameFormat(getClass().getSimpleName() + "-%d").setDaemon(true).build());
    }
    return executor;
  }

  private void shutdownExecutorService() {
    if (executor != null) {
      executor.shutdown();
      executor = null;
    }
  }

  private void waitUntilNoPeerProcedure(MasterProcedureEnv env) throws ProcedureSuspendedException {
    long peerProcCount;
    try {
      peerProcCount = env.getMasterServices().getProcedures().stream()
        .filter(p -> p instanceof PeerProcedureInterface).filter(p -> !p.isFinished()).count();
    } catch (IOException e) {
      LOG.warn("failed to check peer procedure status", e);
      throw suspend(5000, true);
    }
    if (peerProcCount > 0) {
      LOG.info("There are still {} pending peer procedures, will sleep and check later",
        peerProcCount);
      throw suspend(10_000, true);
    }
    LOG.info("No pending peer procedures found, continue...");
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env,
    MigrateReplicationQueueFromZkToTableState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_PREPARE:
        waitUntilNoPeerProcedure(env);
        List<ReplicationPeerDescription> peers = env.getReplicationPeerManager().listPeers(null);
        if (peers.isEmpty()) {
          LOG.info("No active replication peer found, delete old replication queue data and quit");
          ZKReplicationQueueStorageForMigration oldStorage =
            new ZKReplicationQueueStorageForMigration(env.getMasterServices().getZooKeeper(),
              env.getMasterConfiguration());
          try {
            oldStorage.deleteAllData();
          } catch (KeeperException e) {
            LOG.warn("failed to delete old replication queue data, sleep and retry later", e);
            suspend(10_000, true);
          }
          return Flow.NO_MORE_STATE;
        }
        // here we do not care the peers which have already been disabled, as later we do not need
        // to enable them
        disabledPeerIds = peers.stream().filter(ReplicationPeerDescription::isEnabled)
          .map(ReplicationPeerDescription::getPeerId).collect(Collectors.toList());
        setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_PEER);
        return Flow.HAS_MORE_STATE;
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_PEER:
        for (String peerId : disabledPeerIds) {
          addChildProcedure(new DisablePeerProcedure(peerId));
        }
        setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_MIGRATE);
        return Flow.HAS_MORE_STATE;
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_MIGRATE:
        if (futures != null) {
          // wait until all futures done
          long notDone = futures.stream().filter(f -> !f.isDone()).count();
          if (notDone == 0) {
            boolean succ = true;
            for (Future<?> future : futures) {
              try {
                future.get();
              } catch (Exception e) {
                succ = false;
                LOG.warn("Failed to migrate", e);
              }
            }
            if (succ) {
              shutdownExecutorService();
              setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_WAIT_UPGRADING);
              return Flow.HAS_MORE_STATE;
            }
            // reschedule to retry migration again
            futures = null;
          } else {
            LOG.info("There still {} pending migration tasks, will sleep and check later", notDone);
            throw suspend(10_000, true);
          }
        }
        try {
          futures = env.getReplicationPeerManager()
            .migrateQueuesFromZk(env.getMasterServices().getZooKeeper(), getExecutorService());
        } catch (IOException e) {
          LOG.warn("failed to submit migration tasks", e);
          throw suspend(10_000, true);
        }
        throw suspend(10_000, true);
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_WAIT_UPGRADING:
        long rsWithLowerVersion =
          env.getMasterServices().getServerManager().getOnlineServers().values().stream()
            .filter(sm -> VersionInfo.getMajorVersion(sm.getVersion()) < MIN_MAJOR_VERSION).count();
        if (rsWithLowerVersion == 0) {
          setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_PEER);
          return Flow.HAS_MORE_STATE;
        } else {
          LOG.info("There are still {} region servers which have a major version less than {}, "
            + "will sleep and check later", rsWithLowerVersion, MIN_MAJOR_VERSION);
          throw suspend(10_000, true);
        }
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_PEER:
        for (String peerId : disabledPeerIds) {
          addChildProcedure(new EnablePeerProcedure(peerId));
        }
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env,
    MigrateReplicationQueueFromZkToTableState state) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected MigrateReplicationQueueFromZkToTableState getState(int stateId) {
    return MigrateReplicationQueueFromZkToTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(MigrateReplicationQueueFromZkToTableState state) {
    return state.getNumber();
  }

  @Override
  protected MigrateReplicationQueueFromZkToTableState getInitialState() {
    return MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    MigrateReplicationQueueFromZkToTableStateData.Builder builder =
      MigrateReplicationQueueFromZkToTableStateData.newBuilder();
    if (disabledPeerIds != null) {
      builder.addAllDisabledPeerId(disabledPeerIds);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    MigrateReplicationQueueFromZkToTableStateData data =
      serializer.deserialize(MigrateReplicationQueueFromZkToTableStateData.class);
    disabledPeerIds = data.getDisabledPeerIdList().stream().collect(Collectors.toList());
  }
}
