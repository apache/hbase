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
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerProcedureStateData;

/**
 * The base class for all replication peer related procedure.
 */
@InterfaceAudience.Private
public abstract class AbstractPeerProcedure<TState>
    extends StateMachineProcedure<MasterProcedureEnv, TState> implements PeerProcedureInterface {

  protected String peerId;

  // used to keep compatible with old client where we can only returns after updateStorage.
  protected ProcedurePrepareLatch latch;

  protected AbstractPeerProcedure() {
  }

  protected AbstractPeerProcedure(String peerId) {
    this.peerId = peerId;
    this.latch = ProcedurePrepareLatch.createLatch(2, 0);
  }

  public ProcedurePrepareLatch getLatch() {
    return latch;
  }

  @Override
  public String getPeerId() {
    return peerId;
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
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

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(PeerProcedureStateData.newBuilder().setPeerId(peerId).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    peerId = serializer.deserialize(PeerProcedureStateData.class).getPeerId();
  }
}
