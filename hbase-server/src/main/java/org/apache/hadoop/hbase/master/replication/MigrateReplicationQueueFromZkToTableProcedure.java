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

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_CLEAN_UP;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_CLEANER;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_PEER;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_CLEANER;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_PEER;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_MIGRATE;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_PREPARE;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_WAIT_UPGRADING;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.procedure.GlobalProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.procedure2.ProcedureFutureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

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

  private CompletableFuture<Void> future;

  private ExecutorService executor;

  private RetryCounter retryCounter;

  @Override
  public String getGlobalId() {
    return getClass().getSimpleName();
  }

  private CompletableFuture<Void> getFuture() {
    return future;
  }

  private void setFuture(CompletableFuture<Void> f) {
    future = f;
  }

  private ProcedureSuspendedException suspend(Configuration conf, LongConsumer backoffConsumer)
    throws ProcedureSuspendedException {
    if (retryCounter == null) {
      retryCounter = ProcedureUtil.createRetryCounter(conf);
    }
    long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
    backoffConsumer.accept(backoff);
    throw suspend(Math.toIntExact(backoff), true);
  }

  private void resetRetry() {
    retryCounter = null;
  }

  private ExecutorService getExecutorService() {
    if (executor == null) {
      executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
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

  private void disableReplicationLogCleaner(MasterProcedureEnv env)
    throws ProcedureSuspendedException {
    if (!env.getMasterServices().getReplicationLogCleanerBarrier().disable()) {
      // it is not likely that we can reach here as we will schedule this procedure immediately
      // after master restarting, where ReplicationLogCleaner should have not started its first run
      // yet. But anyway, let's make the code more robust. And it is safe to wait a bit here since
      // there will be no data in the new replication queue storage before we execute this procedure
      // so ReplicationLogCleaner will quit immediately without doing anything.
      throw suspend(env.getMasterConfiguration(),
        backoff -> LOG.info(
          "Can not disable replication log cleaner, sleep {} secs and retry later",
          backoff / 1000));
    }
    resetRetry();
  }

  private void enableReplicationLogCleaner(MasterProcedureEnv env) {
    env.getMasterServices().getReplicationLogCleanerBarrier().enable();
  }

  private void waitUntilNoPeerProcedure(MasterProcedureEnv env) throws ProcedureSuspendedException {
    long peerProcCount;
    try {
      peerProcCount = env.getMasterServices().getProcedures().stream()
        .filter(p -> p instanceof PeerProcedureInterface).filter(p -> !p.isFinished()).count();
    } catch (IOException e) {
      throw suspend(env.getMasterConfiguration(),
        backoff -> LOG.warn("failed to check peer procedure status, sleep {} secs and retry later",
          backoff / 1000, e));
    }
    if (peerProcCount > 0) {
      throw suspend(env.getMasterConfiguration(),
        backoff -> LOG.info(
          "There are still {} pending peer procedures, sleep {} secs and retry later",
          peerProcCount, backoff / 1000));
    }
    resetRetry();
    LOG.info("No pending peer procedures found, continue...");
  }

  private void finishMigartion() {
    shutdownExecutorService();
    setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_WAIT_UPGRADING);
    resetRetry();
  }

  private void cleanup(MasterProcedureEnv env) throws ProcedureSuspendedException {
    ZKReplicationQueueStorageForMigration oldStorage = new ZKReplicationQueueStorageForMigration(
      env.getMasterServices().getZooKeeper(), env.getMasterConfiguration());
    try {
      oldStorage.deleteAllData();
      env.getReplicationPeerManager().deleteLegacyRegionReplicaReplicationPeer();
    } catch (KeeperException | ReplicationException e) {
      throw suspend(env.getMasterConfiguration(),
        backoff -> LOG.warn(
          "failed to delete old replication queue data, sleep {} secs and retry later",
          backoff / 1000, e));
    }
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env,
    MigrateReplicationQueueFromZkToTableState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_CLEANER:
        disableReplicationLogCleaner(env);
        setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_PREPARE);
        return Flow.HAS_MORE_STATE;
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_PREPARE:
        waitUntilNoPeerProcedure(env);
        List<ReplicationPeerDescription> peers = env.getReplicationPeerManager().listPeers(null);
        if (peers.isEmpty()) {
          // we will not load the region_replica_replication peer, so here we need to check the
          // storage directly
          try {
            if (env.getReplicationPeerManager().hasRegionReplicaReplicationPeer()) {
              LOG.info(
                "No active replication peer found but we still have '{}' peer, need to"
                  + "wait until all region servers are upgraded",
                ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER);
              setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_WAIT_UPGRADING);
              return Flow.HAS_MORE_STATE;
            }
          } catch (ReplicationException e) {
            throw suspend(env.getMasterConfiguration(), backoff -> LOG
              .warn("failed to list peer ids, sleep {} secs and retry later", backoff / 1000, e));
          }
          LOG.info("No active replication peer found, just clean up all replication queue data");
          setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_CLEANER);
          return Flow.HAS_MORE_STATE;
        }
        disabledPeerIds = peers.stream().filter(ReplicationPeerDescription::isEnabled)
          .map(ReplicationPeerDescription::getPeerId).collect(Collectors.toList());
        setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_PEER);
        resetRetry();
        return Flow.HAS_MORE_STATE;
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_PEER:
        for (String peerId : disabledPeerIds) {
          addChildProcedure(new DisablePeerProcedure(peerId));
        }
        setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_MIGRATE);
        return Flow.HAS_MORE_STATE;
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_MIGRATE:
        try {
          if (
            ProcedureFutureUtil.checkFuture(this, this::getFuture, this::setFuture,
              this::finishMigartion)
          ) {
            return Flow.HAS_MORE_STATE;
          }
          ProcedureFutureUtil.suspendIfNecessary(this, this::setFuture,
            env.getReplicationPeerManager()
              .migrateQueuesFromZk(env.getMasterServices().getZooKeeper(), getExecutorService()),
            env, this::finishMigartion);
        } catch (IOException e) {
          throw suspend(env.getMasterConfiguration(),
            backoff -> LOG.warn("failed to migrate queue data, sleep {} secs and retry later",
              backoff / 1000, e));
        }
        return Flow.HAS_MORE_STATE;
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_WAIT_UPGRADING:
        long rsWithLowerVersion =
          env.getMasterServices().getServerManager().getOnlineServers().values().stream()
            .filter(sm -> VersionInfo.getMajorVersion(sm.getVersion()) < MIN_MAJOR_VERSION).count();
        if (rsWithLowerVersion == 0) {
          setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_PEER);
          return Flow.HAS_MORE_STATE;
        } else {
          throw suspend(env.getMasterConfiguration(),
            backoff -> LOG.warn(
              "There are still {} region servers which have a major version"
                + " less than {}, sleep {} secs and check later",
              rsWithLowerVersion, MIN_MAJOR_VERSION, backoff / 1000));
        }
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_PEER:
        if (CollectionUtils.isNotEmpty(disabledPeerIds)) {
          for (String peerId : disabledPeerIds) {
            addChildProcedure(new EnablePeerProcedure(peerId));
          }
        }
        setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_CLEANER);
        return Flow.HAS_MORE_STATE;
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_CLEANER:
        enableReplicationLogCleaner(env);
        setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_CLEAN_UP);
        return Flow.HAS_MORE_STATE;
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_CLEAN_UP:
        // this is mainly for deleting the region replica replication queue data, but anyway, since
        // we should have migrated all data, here we can simply delete everything
        cleanup(env);
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
    return MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_CLEANER;
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    if (getCurrentState() == getInitialState()) {
      // do not need to disable log cleaner or acquire lock if we are in the initial state, later
      // when executing the procedure we will try to disable and acquire.
      return;
    }
    if (!env.getMasterServices().getReplicationLogCleanerBarrier().disable()) {
      throw new IllegalStateException("can not disable log cleaner, this should not happen");
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    MigrateReplicationQueueFromZkToTableStateData.Builder builder =
      MigrateReplicationQueueFromZkToTableStateData.newBuilder();
    if (CollectionUtils.isNotEmpty(disabledPeerIds)) {
      builder.addAllDisabledPeerId(disabledPeerIds);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    MigrateReplicationQueueFromZkToTableStateData data =
      serializer.deserialize(MigrateReplicationQueueFromZkToTableStateData.class);
    if (data.getDisabledPeerIdCount() > 0) {
      disabledPeerIds = data.getDisabledPeerIdList().stream().collect(Collectors.toList());
    }
  }
}
