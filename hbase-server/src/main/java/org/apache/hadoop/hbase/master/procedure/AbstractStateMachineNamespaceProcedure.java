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

package org.apache.hadoop.hbase.master.procedure;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class for all the Namespace procedures that want to use a StateMachineProcedure.
 * It provide some basic helpers like basic locking and basic toStringClassDetails().
 */
@InterfaceAudience.Private
public abstract class AbstractStateMachineNamespaceProcedure<TState>
    extends StateMachineProcedure<MasterProcedureEnv, TState>
    implements TableProcedureInterface {

  private final ProcedurePrepareLatch syncLatch;

  protected AbstractStateMachineNamespaceProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    syncLatch = null;
  }

  protected AbstractStateMachineNamespaceProcedure(final MasterProcedureEnv env) {
    this(env, null);
  }

  protected AbstractStateMachineNamespaceProcedure(final MasterProcedureEnv env,
      final ProcedurePrepareLatch latch) {
    this.setOwner(env.getRequestUser());
    this.syncLatch = latch;
  }

  protected abstract String getNamespaceName();

  @Override
  public TableName getTableName() {
    return TableName.NAMESPACE_TABLE_NAME;
  }

  @Override
  public abstract TableOperationType getTableOperationType();

  @Override
  public void toStringClassDetails(final StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(", namespace=");
    sb.append(getNamespaceName());
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitNamespaceExclusiveLock(this, getNamespaceName())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeNamespaceExclusiveLock(this, getNamespaceName());
  }

  protected void releaseSyncLatch() {
    ProcedurePrepareLatch.releaseLatch(syncLatch, this);
  }
}
