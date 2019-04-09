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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.TableStateManager.TableStateNotFoundException;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The base class for all replication peer related procedure.
 */
@InterfaceAudience.Private
public abstract class AbstractPeerProcedure<TState> extends AbstractPeerNoLockProcedure<TState>
    implements PeerProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractPeerProcedure.class);

  protected static final int UPDATE_LAST_SEQ_ID_BATCH_SIZE = 1000;

  // The sleep interval when waiting table to be enabled or disabled.
  protected static final int SLEEP_INTERVAL_MS = 1000;

  // used to keep compatible with old client where we can only returns after updateStorage.
  protected ProcedurePrepareLatch latch;

  protected AbstractPeerProcedure() {
  }

  protected AbstractPeerProcedure(String peerId) {
    super(peerId);
    this.latch = ProcedurePrepareLatch.createLatch(2, 1);
  }

  public ProcedurePrepareLatch getLatch() {
    return latch;
  }

  @Override
  protected LockState acquireLock(MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitPeerExclusiveLock(this, peerId)) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(MasterProcedureEnv env) {
    env.getProcedureScheduler().wakePeerExclusiveLock(this, peerId);
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
    return true;
  }

  protected final void refreshPeer(MasterProcedureEnv env, PeerOperationType type) {
    addChildProcedure(env.getMasterServices().getServerManager().getOnlineServersList().stream()
      .map(sn -> new RefreshPeerProcedure(peerId, type, sn)).toArray(RefreshPeerProcedure[]::new));
  }

  // will be override in test to simulate error
  @VisibleForTesting
  protected void enablePeer(MasterProcedureEnv env) throws ReplicationException {
    env.getReplicationPeerManager().enablePeer(peerId);
  }

  private void addToMap(Map<String, Long> lastSeqIds, String encodedRegionName, long barrier,
      ReplicationQueueStorage queueStorage) throws ReplicationException {
    if (barrier >= 0) {
      lastSeqIds.put(encodedRegionName, barrier);
      if (lastSeqIds.size() >= UPDATE_LAST_SEQ_ID_BATCH_SIZE) {
        queueStorage.setLastSequenceIds(peerId, lastSeqIds);
        lastSeqIds.clear();
      }
    }
  }

  protected final void setLastPushedSequenceId(MasterProcedureEnv env,
      ReplicationPeerConfig peerConfig) throws IOException, ReplicationException {
    Map<String, Long> lastSeqIds = new HashMap<String, Long>();
    for (TableDescriptor td : env.getMasterServices().getTableDescriptors().getAll().values()) {
      if (!td.hasGlobalReplicationScope()) {
        continue;
      }
      TableName tn = td.getTableName();
      if (!ReplicationUtils.contains(peerConfig, tn)) {
        continue;
      }
      setLastPushedSequenceIdForTable(env, tn, lastSeqIds);
    }
    if (!lastSeqIds.isEmpty()) {
      env.getReplicationPeerManager().getQueueStorage().setLastSequenceIds(peerId, lastSeqIds);
    }
  }

  // If the table is currently disabling, then we need to wait until it is disabled.We will write
  // replication barrier for a disabled table. And return whether we need to update the last pushed
  // sequence id, if the table has been deleted already, i.e, we hit TableStateNotFoundException,
  // then we do not need to update last pushed sequence id for this table.
  private boolean needSetLastPushedSequenceId(TableStateManager tsm, TableName tn)
      throws IOException {
    for (;;) {
      try {
        if (!tsm.getTableState(tn).isDisabling()) {
          return true;
        }
        Thread.sleep(SLEEP_INTERVAL_MS);
      } catch (TableStateNotFoundException e) {
        return false;
      } catch (InterruptedException e) {
        throw (IOException) new InterruptedIOException(e.getMessage()).initCause(e);
      }
    }
  }

  // Will put the encodedRegionName->lastPushedSeqId pair into the map passed in, if the map is
  // large enough we will call queueStorage.setLastSequenceIds and clear the map. So the caller
  // should not forget to check whether the map is empty at last, if not you should call
  // queueStorage.setLastSequenceIds to write out the remaining entries in the map.
  protected final void setLastPushedSequenceIdForTable(MasterProcedureEnv env, TableName tableName,
      Map<String, Long> lastSeqIds) throws IOException, ReplicationException {
    TableStateManager tsm = env.getMasterServices().getTableStateManager();
    ReplicationQueueStorage queueStorage = env.getReplicationPeerManager().getQueueStorage();
    Connection conn = env.getMasterServices().getConnection();
    if (!needSetLastPushedSequenceId(tsm, tableName)) {
      LOG.debug("Skip settting last pushed sequence id for {}", tableName);
      return;
    }
    for (Pair<String, Long> name2Barrier : MetaTableAccessor
      .getTableEncodedRegionNameAndLastBarrier(conn, tableName)) {
      LOG.trace("Update last pushed sequence id for {}, {}", tableName, name2Barrier);
      addToMap(lastSeqIds, name2Barrier.getFirst(), name2Barrier.getSecond().longValue() - 1,
        queueStorage);
    }
  }
}
