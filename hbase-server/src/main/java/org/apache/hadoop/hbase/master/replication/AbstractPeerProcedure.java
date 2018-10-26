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

import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The base class for all replication peer related procedure.
 */
@InterfaceAudience.Private
public abstract class AbstractPeerProcedure<TState>
    extends AbstractPeerNoLockProcedure<TState> implements PeerProcedureInterface {

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
}
