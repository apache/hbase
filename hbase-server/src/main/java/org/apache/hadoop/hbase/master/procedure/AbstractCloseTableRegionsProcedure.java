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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Base class for unassigning table regions.
 */
@InterfaceAudience.Private
public abstract class AbstractCloseTableRegionsProcedure<TState extends Enum<?>>
  extends AbstractStateMachineTableProcedure<TState> {

  private static final Logger LOG =
    LoggerFactory.getLogger(AbstractCloseTableRegionsProcedure.class);

  protected TableName tableName;

  private RetryCounter retryCounter;

  protected AbstractCloseTableRegionsProcedure() {
  }

  protected AbstractCloseTableRegionsProcedure(TableName tableName) {
    this.tableName = tableName;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_EDIT;
  }

  private Flow schedule(MasterProcedureEnv env) throws ProcedureSuspendedException {
    MutableBoolean submitted = new MutableBoolean(false);
    int inTransitionCount = submitUnassignProcedure(env, p -> {
      submitted.setTrue();
      addChildProcedure(p);
    });
    if (inTransitionCount > 0 && submitted.isFalse()) {
      // we haven't scheduled any unassign procedures and there are still regions in
      // transition, sleep for a while and try again
      if (retryCounter == null) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoffMillis = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.info(
        "There are still {} region(s) in transition for closing regions of table {}"
          + " when executing {}, suspend {}secs and try again later",
        inTransitionCount, tableName, getClass().getSimpleName(), backoffMillis / 1000);
      suspend((int) backoffMillis, true);
    }
    setNextState(getConfirmState());
    return Flow.HAS_MORE_STATE;
  }

  private Flow confirm(MasterProcedureEnv env) {
    int unclosedCount = numberOfUnclosedRegions(env);
    if (unclosedCount > 0) {
      LOG.info(
        "There are still {} unclosed region(s) for closing regions of table {}"
          + " when executing {}, continue...",
        unclosedCount, tableName, getClass().getSimpleName());
      setNextState(getInitialState());
      return Flow.HAS_MORE_STATE;
    } else {
      return Flow.NO_MORE_STATE;
    }
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, TState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.trace("{} execute state={}", this, state);
    if (state == getInitialState()) {
      return schedule(env);
    } else if (state == getConfirmState()) {
      return confirm(env);
    } else {
      throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, TState state)
    throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  /**
   * We have two state for this type of procedures, the initial state for scheduling unassign
   * procedures, and the confirm state for checking whether we have unassigned all the regions.
   * @return the confirm state
   */
  protected abstract TState getConfirmState();

  /**
   * Submit TRSP for unassigning regions. Return the number of regions in RIT state that we can not
   * schedule TRSP for them.
   */
  protected abstract int submitUnassignProcedure(MasterProcedureEnv env,
    Consumer<TransitRegionStateProcedure> submit);

  /**
   * Return the number of uncloses regions. Returning {@code 0} means we are done.
   */
  protected abstract int numberOfUnclosedRegions(MasterProcedureEnv env);
}
