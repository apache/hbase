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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Leave here only for checking if we can successfully start the master.
 * @deprecated Do not use any more.
 * @see TransitRegionStateProcedure
 */
@Deprecated
@InterfaceAudience.Private
public abstract class RegionTransitionProcedure extends Procedure<MasterProcedureEnv>
    implements TableProcedureInterface, RemoteProcedure<MasterProcedureEnv, ServerName> {

  protected final AtomicBoolean aborted = new AtomicBoolean(false);

  private RegionTransitionState transitionState = RegionTransitionState.REGION_TRANSITION_QUEUE;

  private RegionInfo regionInfo;

  private int attempt;

  // Required by the Procedure framework to create the procedure on replay
  public RegionTransitionProcedure() {
  }

  public RegionTransitionProcedure(final RegionInfo regionInfo) {
    this.regionInfo = regionInfo;
  }

  public RegionInfo getRegionInfo() {
    return regionInfo;
  }

  public void setRegionInfo(final RegionInfo regionInfo) {
    this.regionInfo = regionInfo;
  }

  protected void setAttempt(int attempt) {
    this.attempt = attempt;
  }

  protected int getAttempt() {
    return this.attempt;
  }

  @Override
  public TableName getTableName() {
    RegionInfo hri = getRegionInfo();
    return hri != null ? hri.getTable() : null;
  }

  public boolean isMeta() {
    return TableName.isMetaTableName(getTableName());
  }

  @Override
  public void toStringClassDetails(final StringBuilder sb) {
    sb.append(getProcName());
  }

  @Override public String getProcName() {
    RegionInfo r = getRegionInfo();
    return getClass().getSimpleName() + " " + getTableName() + (r != null? r.getEncodedName(): "");
  }

  public RegionStateNode getRegionState(final MasterProcedureEnv env) {
    return env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(getRegionInfo());
  }

  void setTransitionState(final RegionTransitionState state) {
    this.transitionState = state;
  }

  RegionTransitionState getTransitionState() {
    return transitionState;
  }

  protected abstract boolean startTransition(MasterProcedureEnv env, RegionStateNode regionNode)
      throws IOException, ProcedureSuspendedException;

  protected abstract boolean updateTransition(MasterProcedureEnv env, RegionStateNode regionNode)
      throws IOException, ProcedureSuspendedException;

  protected abstract void finishTransition(MasterProcedureEnv env, RegionStateNode regionNode)
      throws IOException, ProcedureSuspendedException;

  protected abstract void reportTransition(MasterProcedureEnv env, RegionStateNode regionNode,
      TransitionCode code, long seqId) throws UnexpectedStateException;

  @Override
  public abstract Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env,
      ServerName serverName);

  protected abstract boolean remoteCallFailed(MasterProcedureEnv env, RegionStateNode regionNode,
      IOException exception);

  @Override
  public synchronized void remoteCallFailed(final MasterProcedureEnv env,
      final ServerName serverName, final IOException exception) {
  }

  @Override
  protected void toStringState(StringBuilder builder) {
    super.toStringState(builder);
    RegionTransitionState ts = this.transitionState;
    if (!isFinished() && ts != null) {
      builder.append(":").append(ts);
    }
  }

  @Override
  protected Procedure[] execute(final MasterProcedureEnv env) {
    return null;
  }

  @Override
  protected void rollback(MasterProcedureEnv env) {
  }

  protected abstract boolean isRollbackSupported(final RegionTransitionState state);

  @Override
  protected boolean abort(final MasterProcedureEnv env) {
    if (isRollbackSupported(transitionState)) {
      aborted.set(true);
      return true;
    }
    return false;
  }

  @Override
  public void remoteOperationCompleted(MasterProcedureEnv env) {
    // should not be called for region operation until we modified the open/close region procedure
    throw new UnsupportedOperationException();
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, RemoteProcedureException error) {
    // should not be called for region operation until we modified the open/close region procedure
    throw new UnsupportedOperationException();
  }

}
