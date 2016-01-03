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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Map Future Interface on to Procedure result processing.
 */
// Has no extra methods as of now beyond Future<ProcedureInfo>. Use #toString if you want to log
// procId of procedure.
// TODO: This should be in Procedure? Have it in master package for now. Lets out ProcedureInfo.
// Implementation informed by HBaseAdmin#ProcedureFuture.
@InterfaceAudience.Private
class ProcedureFuture implements Future<ProcedureInfo> {
  // Save exception so we can rethrow if called again. Same for result.
  private ExecutionException exception = null;
  private ProcedureInfo result = null;
  private boolean done = false;
  private boolean cancelled = false;
  private final Long procId;
  private final ProcedureExecutor<MasterProcedureEnv> procedureExecutor;

  ProcedureFuture(final ProcedureExecutor<MasterProcedureEnv> procedureExecutor,
      final long procId) {
    this.procedureExecutor = procedureExecutor;
    this.procId = procId;
  }

  @Override
  public String toString() {
    return "procId=" + this.procId;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (!this.cancelled) {
      this.cancelled = this.procedureExecutor.abort(this.procId, mayInterruptIfRunning);
    }
    return this.cancelled;
  }

  @Override
  public boolean isCancelled() {
    return this.cancelled;
  }

  @Override
  public boolean isDone() {
    return done;
  }

  /**
   * This method is unsupported. We will throw an UnsupportedOperationException. Only the lazy
   * would call this method because they can avoid thinking through implication of a Procedure that
   * might never return so this is disallowed. Use {@link #get(long, TimeUnit)}.
   */
  @Override
  public ProcedureInfo get() throws InterruptedException, ExecutionException {
    // TODO: should we ever spin forever?
    throw new UnsupportedOperationException();
  }

  @Override
  public ProcedureInfo get(long timeout, TimeUnit unit)
  throws InterruptedException, ExecutionException, TimeoutException {
    if (!this.done) {
      // TODO: add this sort of facility to EnvironmentEdgeManager
      long deadlineTs = EnvironmentEdgeManager.currentTime() + unit.toMillis(timeout);
      try {
        this.result = waitProcedureResult(procId, deadlineTs);
      } catch (IOException e) {
        this.exception = new ExecutionException(e);
      }
      this.done = true;
    }
    if (exception != null) {
      throw exception;
    }
    return result;
  }

  /**
   * @param procId
   * @param deadlineTs
   * @return A ProcedureInfo instance or null if procedure not found.
   * @throws IOException
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private ProcedureInfo waitProcedureResult(long procId, long deadlineTs)
  throws IOException, TimeoutException, InterruptedException {
    while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
      Pair<ProcedureInfo, Procedure> pair = this.procedureExecutor.getResultOrProcedure(procId);
      if (pair.getFirst() != null) {
        this.procedureExecutor.removeResult(procId);
        return pair.getFirst();
      } else {
        if (pair.getSecond() == null) return null;
      }
      // TODO: Add a wait.
    }
    throw new TimeoutException("The procedure " + procId + " is still running");
  }
}