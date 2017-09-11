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

import java.io.IOException;

import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.security.User;

/**
 * Base class for all the Table procedures that want to use a StateMachineProcedure.
 * It provides helpers like basic locking, sync latch, and toStringClassDetails().
 */
@InterfaceAudience.Private
public abstract class AbstractStateMachineTableProcedure<TState>
    extends StateMachineProcedure<MasterProcedureEnv, TState>
    implements TableProcedureInterface {

  // used for compatibility with old clients
  private final ProcedurePrepareLatch syncLatch;

  private User user;

  protected AbstractStateMachineTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    syncLatch = null;
  }

  protected AbstractStateMachineTableProcedure(final MasterProcedureEnv env) {
    this(env, null);
  }

  /**
   * @param env Uses this to set Procedure Owner at least.
   */
  protected AbstractStateMachineTableProcedure(final MasterProcedureEnv env,
      final ProcedurePrepareLatch latch) {
    if (env != null) {
      this.user = env.getRequestUser();
      this.setOwner(user);
    }
    // used for compatibility with clients without procedures
    // they need a sync TableExistsException, TableNotFoundException, TableNotDisabledException, ...
    this.syncLatch = latch;
  }

  @Override
  public abstract TableName getTableName();

  @Override
  public abstract TableOperationType getTableOperationType();

  @Override
  public void toStringClassDetails(final StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" table=");
    sb.append(getTableName());
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (env.waitInitialized(this)) return LockState.LOCK_EVENT_WAIT;
    if (env.getProcedureScheduler().waitTableExclusiveLock(this, getTableName())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeTableExclusiveLock(this, getTableName());
  }

  protected User getUser() {
    return user;
  }

  protected void setUser(final User user) {
    this.user = user;
  }

  protected void releaseSyncLatch() {
    ProcedurePrepareLatch.releaseLatch(syncLatch, this);
  }

  /**
   * Check whether a table is modifiable - exists and either offline or online with config set
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  protected void checkTableModifiable(final MasterProcedureEnv env) throws IOException {
    // Checks whether the table exists
    if (!MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), getTableName())) {
      throw new TableNotFoundException(getTableName());
    }
  }
}
