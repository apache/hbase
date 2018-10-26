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
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Base class for replication peer related procedures which do not need to hold locks(for most of
 * the sub procedures).
 */
@InterfaceAudience.Private
public abstract class AbstractPeerNoLockProcedure<TState>
    extends StateMachineProcedure<MasterProcedureEnv, TState> implements PeerProcedureInterface {

  protected String peerId;

  protected int attempts;

  protected AbstractPeerNoLockProcedure() {
  }

  protected AbstractPeerNoLockProcedure(String peerId) {
    this.peerId = peerId;
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
  protected void rollbackState(MasterProcedureEnv env, TState state)
      throws IOException, InterruptedException {
    if (state == getInitialState()) {
      // actually the peer related operations has no rollback, but if we haven't done any
      // modifications on the peer storage yet, we can just return.
      return;
    }
    throw new UnsupportedOperationException();
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

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  protected final ProcedureSuspendedException suspend(long backoff)
      throws ProcedureSuspendedException {
    attempts++;
    setTimeout(Math.toIntExact(backoff));
    setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
    skipPersistence();
    throw new ProcedureSuspendedException();
  }
}
