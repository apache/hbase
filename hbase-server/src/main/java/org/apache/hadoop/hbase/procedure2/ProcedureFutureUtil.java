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
package org.apache.hadoop.hbase.procedure2;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class for switching procedure out(yielding) while it is doing some time consuming
 * operation, such as updating meta, where we can get a {@link CompletableFuture} about the
 * operation.
 */
@InterfaceAudience.Private
public final class ProcedureFutureUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureFutureUtil.class);

  private ProcedureFutureUtil() {
  }

  public static boolean checkFuture(Procedure<?> proc, Supplier<CompletableFuture<Void>> getFuture,
    Consumer<CompletableFuture<Void>> setFuture, Runnable actionAfterDone) throws IOException {
    CompletableFuture<Void> future = getFuture.get();
    if (future == null) {
      return false;
    }
    // reset future
    setFuture.accept(null);
    FutureUtils.get(future);
    actionAfterDone.run();
    return true;
  }

  public static void suspendIfNecessary(Procedure<?> proc,
    Consumer<CompletableFuture<Void>> setFuture, CompletableFuture<Void> future,
    MasterProcedureEnv env, Runnable actionAfterDone)
    throws IOException, ProcedureSuspendedException {
    MutableBoolean completed = new MutableBoolean(false);
    Thread currentThread = Thread.currentThread();
    // This is for testing. In ProcedureTestingUtility, we will restart a ProcedureExecutor and
    // reuse it, for performance, so we need to make sure that all the procedure have been stopped.
    // But here, the callback of this future is not executed in a PEWorker, so in ProcedureExecutor
    // we have no way to stop it. So here, we will get the asyncTaskExecutor first, in the PEWorker
    // thread, where the ProcedureExecutor should have not been stopped yet, then when calling the
    // callback, if the ProcedureExecutor have already been stopped and restarted, the
    // asyncTaskExecutor will also be shutdown so we can not add anything back to the scheduler.
    ExecutorService asyncTaskExecutor = env.getAsyncTaskExecutor();
    FutureUtils.addListener(future, (r, e) -> {
      if (Thread.currentThread() == currentThread) {
        LOG.debug("The future has completed while adding callback, give up suspending procedure {}",
          proc);
        // this means the future has already been completed, as we call the callback directly while
        // calling addListener, so here we just set completed to true without doing anything
        completed.setTrue();
        return;
      }
      LOG.debug("Going to wake up procedure {} because future has completed", proc);
      // This callback may be called inside netty's event loop, so we should not block it for a long
      // time. The worker executor will hold the execution lock while executing the procedure, and
      // we may persist the procedure state inside the lock, which is a time consuming operation.
      // And what makes things worse is that, we persist procedure state to master local region,
      // where the AsyncFSWAL implementation will use the same netty's event loop for dealing with
      // I/O, which could even cause dead lock.
      asyncTaskExecutor.execute(() -> wakeUp(proc, env));
    });
    if (completed.getValue()) {
      FutureUtils.get(future);
      actionAfterDone.run();
    } else {
      // suspend the procedure
      setFuture.accept(future);
      proc.skipPersistence();
      suspend(proc);
    }
  }

  public static void suspend(Procedure<?> proc) throws ProcedureSuspendedException {
    proc.skipPersistence();
    throw new ProcedureSuspendedException();
  }

  public static void wakeUp(Procedure<?> proc, MasterProcedureEnv env) {
    // should acquire procedure execution lock to make sure that the procedure executor has
    // finished putting this procedure to the WAITING_TIMEOUT state, otherwise there could be
    // race and cause unexpected result
    IdLock procLock = env.getMasterServices().getMasterProcedureExecutor().getProcExecutionLock();
    IdLock.Entry lockEntry;
    try {
      lockEntry = procLock.getLockEntry(proc.getProcId());
    } catch (IOException e) {
      LOG.error("Error while acquiring execution lock for procedure {}"
        + " when trying to wake it up, aborting...", proc, e);
      env.getMasterServices().abort("Can not acquire procedure execution lock", e);
      return;
    }
    try {
      env.getProcedureScheduler().addFront(proc);
    } finally {
      procLock.releaseLockEntry(lockEntry);
    }
  }
}
