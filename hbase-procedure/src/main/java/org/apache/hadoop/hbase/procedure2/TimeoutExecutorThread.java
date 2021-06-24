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
package org.apache.hadoop.hbase.procedure2;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.procedure2.util.DelayedUtil;
import org.apache.hadoop.hbase.procedure2.util.DelayedUtil.DelayedWithTimeout;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs task on a period such as check for stuck workers.
 * @see InlineChore
 */
@InterfaceAudience.Private
class TimeoutExecutorThread<TEnvironment> extends StoppableThread {

  private static final Logger LOG = LoggerFactory.getLogger(TimeoutExecutorThread.class);

  private final ProcedureExecutor<TEnvironment> executor;

  private final DelayQueue<DelayedWithTimeout> queue = new DelayQueue<>();

  public TimeoutExecutorThread(ProcedureExecutor<TEnvironment> executor, ThreadGroup group,
    String name) {
    super(group, name);
    setDaemon(true);
    this.executor = executor;
  }

  @Override
  public void sendStopSignal() {
    queue.add(DelayedUtil.DELAYED_POISON);
  }

  @Override
  public void run() {
    while (executor.isRunning()) {
      final DelayedWithTimeout task = DelayedUtil.takeWithoutInterrupt(queue, 20,
        TimeUnit.SECONDS);
      if (task == null || task == DelayedUtil.DELAYED_POISON) {
        // the executor may be shutting down,
        // and the task is just the shutdown request
        continue;
      }
      LOG.trace("Executing {}", task);

      // execute the task
      if (task instanceof InlineChore) {
        execInlineChore((InlineChore) task);
      } else if (task instanceof DelayedProcedure) {
        execDelayedProcedure((DelayedProcedure<TEnvironment>) task);
      } else {
        LOG.error("CODE-BUG unknown timeout task type {}", task);
      }
    }
  }

  public void add(InlineChore chore) {
    chore.refreshTimeout();
    queue.add(chore);
  }

  public void add(Procedure<TEnvironment> procedure) {
    LOG.info("ADDED {}; timeout={}, timestamp={}", procedure, procedure.getTimeout(),
      procedure.getTimeoutTimestamp());
    queue.add(new DelayedProcedure<>(procedure));
  }

  public boolean remove(Procedure<TEnvironment> procedure) {
    return queue.remove(new DelayedProcedure<>(procedure));
  }

  private void execInlineChore(InlineChore chore) {
    chore.run();
    add(chore);
  }

  private void execDelayedProcedure(DelayedProcedure<TEnvironment> delayed) {
    // TODO: treat this as a normal procedure, add it to the scheduler and
    // let one of the workers handle it.
    // Today we consider ProcedureInMemoryChore as InlineChores
    Procedure<TEnvironment> procedure = delayed.getObject();
    if (procedure instanceof ProcedureInMemoryChore) {
      executeInMemoryChore((ProcedureInMemoryChore<TEnvironment>) procedure);
      // if the procedure is in a waiting state again, put it back in the queue
      procedure.updateTimestamp();
      if (procedure.isWaiting()) {
        delayed.setTimeout(procedure.getTimeoutTimestamp());
        queue.add(delayed);
      }
    } else {
      executeTimedoutProcedure(procedure);
    }
  }

  private void executeInMemoryChore(ProcedureInMemoryChore<TEnvironment> chore) {
    if (!chore.isWaiting()) {
      return;
    }

    // The ProcedureInMemoryChore is a special case, and it acts as a chore.
    // instead of bringing the Chore class in, we reuse this timeout thread for
    // this special case.
    try {
      chore.periodicExecute(executor.getEnvironment());
    } catch (Throwable e) {
      LOG.error("Ignoring {} exception: {}", chore, e.getMessage(), e);
    }
  }

  protected void executeTimedoutProcedure(Procedure<TEnvironment> proc) {
    // The procedure received a timeout. if the procedure itself does not handle it,
    // call abort() and add the procedure back in the queue for rollback.
    if (proc.setTimeoutFailure(executor.getEnvironment())) {
      long rootProcId = executor.getRootProcedureId(proc);
      RootProcedureState<TEnvironment> procStack = executor.getProcStack(rootProcId);
      procStack.abort();
      executor.getStore().update(proc);
      executor.getScheduler().addFront(proc);
    }
  }
}
