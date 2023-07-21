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
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.hbase.util.RetryCounter;
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

  private CompletableFuture<?> future;

  private ExecutorService executor;

  private RetryCounter retryCounter;

  @Override
  public String getGlobalId() {
    return getClass().getSimpleName();
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
          LOG.info("No active replication peer found, delete old replication queue data and quit");
          ZKReplicationQueueStorageForMigration oldStorage =
            new ZKReplicationQueueStorageForMigration(env.getMasterServices().getZooKeeper(),
              env.getMasterConfiguration());
          try {
            oldStorage.deleteAllData();
          } catch (KeeperException e) {
            throw suspend(env.getMasterConfiguration(),
              backoff -> LOG.warn(
                "failed to delete old replication queue data, sleep {} secs and retry later",
                backoff / 1000, e));
          }
          setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_CLEANER);
          return Flow.HAS_MORE_STATE;
        }
        // here we do not care the peers which have already been disabled, as later we do not need
        // to enable them
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
        if (future != null) {
          // should have finished when we arrive here
          assert future.isDone();
          try {
            future.get();
          } catch (Exception e) {
            future = null;
            throw suspend(env.getMasterConfiguration(),
              backoff -> LOG.warn("failed to migrate queue data, sleep {} secs and retry later",
                backoff / 1000, e));
          }
          shutdownExecutorService();
          setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_WAIT_UPGRADING);
          resetRetry();
          return Flow.HAS_MORE_STATE;
        }
        future = env.getReplicationPeerManager()
          .migrateQueuesFromZk(env.getMasterServices().getZooKeeper(), getExecutorService());
        FutureUtils.addListener(future, (r, e) -> {
          // should acquire procedure execution lock to make sure that the procedure executor has
          // finished putting this procedure to the WAITING_TIMEOUT state, otherwise there could be
          // race and cause unexpected result
          IdLock procLock =
            env.getMasterServices().getMasterProcedureExecutor().getProcExecutionLock();
          IdLock.Entry lockEntry;
          try {
            lockEntry = procLock.getLockEntry(getProcId());
          } catch (IOException ioe) {
            LOG.error("Error while acquiring execution lock for procedure {}"
              + " when trying to wake it up, aborting...", this, ioe);
            env.getMasterServices().abort("Can not acquire procedure execution lock", e);
            return;
          }
          try {
            setTimeoutFailure(env);
          } finally {
            procLock.releaseLockEntry(lockEntry);
          }
        });
        // here we set timeout to -1 so the ProcedureExecutor will not schedule a Timer for us
        setTimeout(-1);
        setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
        // skip persistence is a must now since when restarting, if the procedure is in
        // WAITING_TIMEOUT state and has -1 as timeout, it will block there forever...
        skipPersistence();
        throw new ProcedureSuspendedException();
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
        for (String peerId : disabledPeerIds) {
          addChildProcedure(new EnablePeerProcedure(peerId));
        }
        setNextState(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_CLEANER);
        return Flow.HAS_MORE_STATE;
      case MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_ENABLE_CLEANER:
        enableReplicationLogCleaner(env);
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
