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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Base class for all the Region procedures that want to use a StateMachine.
 * It provides some basic helpers like basic locking, sync latch, and toStringClassDetails().
 * Defaults to holding the lock for the life of the procedure.
 */
@InterfaceAudience.Private
public abstract class AbstractStateMachineRegionProcedure<TState>
    extends AbstractStateMachineTableProcedure<TState> {
  private HRegionInfo hri;
  private volatile boolean lock = false;

  public AbstractStateMachineRegionProcedure(final MasterProcedureEnv env,
      final HRegionInfo hri) {
    super(env);
    this.hri = hri;
  }

  public AbstractStateMachineRegionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  /**
   * @return The HRegionInfo of the region we are operating on.
   */
  protected HRegionInfo getRegion() {
    return this.hri;
  }

  /**
   * Used when deserializing. Otherwise, DON'T TOUCH IT!
   */
  protected void setRegion(final HRegionInfo hri) {
    this.hri = hri;
  }

  @Override
  public TableName getTableName() {
    return getRegion().getTable();
  }

  @Override
  public abstract TableOperationType getTableOperationType();

  @Override
  public void toStringClassDetails(final StringBuilder sb) {
    super.toStringClassDetails(sb);
    sb.append(", region=").append(getRegion().getShortNameToLog());
  }

  /**
   * Check whether a table is modifiable - exists and either offline or online with config set
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  @Override
  protected void checkTableModifiable(final MasterProcedureEnv env) throws IOException {
    // Checks whether the table exists
    if (!MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), getTableName())) {
      throw new TableNotFoundException(getTableName());
    }
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
    return true;
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (env.waitInitialized(this)) return LockState.LOCK_EVENT_WAIT;
    if (env.getProcedureScheduler().waitRegions(this, getTableName(), getRegion())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    this.lock = true;
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    this.lock = false;
    env.getProcedureScheduler().wakeRegions(this, getTableName(), getRegion());
  }

  @Override
  protected boolean hasLock(final MasterProcedureEnv env) {
    return this.lock;
  }

  protected void setFailure(Throwable cause) {
    super.setFailure(getClass().getSimpleName(), cause);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(HRegionInfo.convert(getRegion()));
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);
    this.hri = HRegionInfo.convert(serializer.deserialize(HBaseProtos.RegionInfo.class));
  }
}
