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
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Helper to synchronously wait on conditions.
 * This will be removed in the future (mainly when the AssignmentManager will be
 * replaced with a Procedure version) by using ProcedureYieldException,
 * and the queue will handle waiting and scheduling based on events.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ProcedureSyncWait {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureSyncWait.class);

  private ProcedureSyncWait() {}

  @InterfaceAudience.Private
  public interface Predicate<T> {
    T evaluate() throws IOException;
  }

  private static class ProcedureFuture implements Future<byte[]> {
    private final ProcedureExecutor<MasterProcedureEnv> procExec;
    private final Procedure<?> proc;

    private boolean hasResult = false;
    private byte[] result = null;

    public ProcedureFuture(ProcedureExecutor<MasterProcedureEnv> procExec, Procedure<?> proc) {
      this.procExec = procExec;
      this.proc = proc;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return hasResult;
    }

    @Override
    public byte[] get() throws InterruptedException, ExecutionException {
      if (hasResult) {
        return result;
      }
      try {
        return waitForProcedureToComplete(procExec, proc, Long.MAX_VALUE);
      } catch (Exception e) {
        throw new ExecutionException(e);
      }
    }

    @Override
    public byte[] get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (hasResult) {
        return result;
      }
      try {
        result = waitForProcedureToComplete(procExec, proc, unit.toMillis(timeout));
        hasResult = true;
        return result;
      } catch (TimeoutIOException e) {
        throw new TimeoutException(e.getMessage());
      } catch (Exception e) {
        throw new ExecutionException(e);
      }
    }
  }

  public static Future<byte[]> submitProcedure(final ProcedureExecutor<MasterProcedureEnv> procExec,
      final Procedure<MasterProcedureEnv> proc) {
    if (proc.isInitializing()) {
      procExec.submitProcedure(proc);
    }
    return new ProcedureFuture(procExec, proc);
  }

  public static byte[] submitAndWaitProcedure(ProcedureExecutor<MasterProcedureEnv> procExec,
      final Procedure<MasterProcedureEnv> proc) throws IOException {
    if (proc.isInitializing()) {
      procExec.submitProcedure(proc);
    }
    return waitForProcedureToCompleteIOE(procExec, proc, Long.MAX_VALUE);
  }

  public static byte[] waitForProcedureToCompleteIOE(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final Procedure<?> proc,
      final long timeout) throws IOException {
    try {
      return waitForProcedureToComplete(procExec, proc, timeout);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static byte[] waitForProcedureToComplete(
      final ProcedureExecutor<MasterProcedureEnv> procExec, final Procedure<?> proc,
      final long timeout) throws IOException {
    waitFor(procExec.getEnvironment(), timeout, "pid=" + proc.getProcId(),
      new ProcedureSyncWait.Predicate<Boolean>() {
        @Override
        public Boolean evaluate() throws IOException {
          if (!procExec.isRunning()) {
            return true;
          }
          ProcedureState state = proc.getState();
          if (state == ProcedureState.INITIALIZING || state == ProcedureState.RUNNABLE) {
            // under these states the procedure may have not been added to procExec yet, so do not
            // use isFinished to test whether it is finished, as this method will just check if the
            // procedure is in the running procedure list
            return false;
          }
          return procExec.isFinished(proc.getProcId());
        }
      });
    if (!procExec.isRunning()) {
      throw new IOException("The Master is Aborting");
    }

    // If the procedure fails, we should always have an exception captured. Throw it.
    // Needs to be an IOE to get out of here.
    if (proc.hasException()) {
      throw MasterProcedureUtil.unwrapRemoteIOException(proc);
    } else {
      return proc.getResult();
    }
  }

  public static <T> T waitFor(MasterProcedureEnv env, String purpose, Predicate<T> predicate)
      throws IOException {
    Configuration conf = env.getMasterConfiguration();
    long waitTime = conf.getLong("hbase.master.wait.on.region", 5 * 60 * 1000);
    return waitFor(env, waitTime, purpose, predicate);
  }

  public static <T> T waitFor(MasterProcedureEnv env, long waitTime, String purpose,
      Predicate<T> predicate) throws IOException {
    Configuration conf = env.getMasterConfiguration();
    long waitingTimeForEvents = conf.getInt("hbase.master.event.waiting.time", 1000);
    return waitFor(env, waitTime, waitingTimeForEvents, purpose, predicate);
  }

  public static <T> T waitFor(MasterProcedureEnv env, long waitTime, long waitingTimeForEvents,
      String purpose, Predicate<T> predicate) throws IOException {
    long done = EnvironmentEdgeManager.currentTime() + waitTime;
    if (done <= 0) {
      // long overflow, usually this means we pass Long.MAX_VALUE as waitTime
      done = Long.MAX_VALUE;
    }
    boolean logged = false;
    do {
      T result = predicate.evaluate();
      if (result != null && !result.equals(Boolean.FALSE)) {
        return result;
      }
      try {
        Thread.sleep(waitingTimeForEvents);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while sleeping, waiting on " + purpose);
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("waitFor " + purpose);
      } else {
        if (!logged) LOG.debug("waitFor " + purpose);
      }
      logged = true;
    } while (EnvironmentEdgeManager.currentTime() < done && env.isRunning());

    throw new TimeoutIOException("Timed out while waiting on " + purpose);
  }

  protected static void waitMetaRegions(final MasterProcedureEnv env) throws IOException {
    int timeout = env.getMasterConfiguration().getInt("hbase.client.catalog.timeout", 10000);
    try {
      if (MetaTableLocator.waitMetaRegionLocation(env.getMasterServices().getZooKeeper(),
        timeout) == null) {
        throw new NotAllMetaRegionsOnlineException();
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }
  }

  protected static void waitRegionInTransition(final MasterProcedureEnv env,
      final List<RegionInfo> regions) throws IOException {
    final RegionStates states = env.getAssignmentManager().getRegionStates();
    for (final RegionInfo region : regions) {
      ProcedureSyncWait.waitFor(env, "regions " + region.getRegionNameAsString() + " in transition",
          new ProcedureSyncWait.Predicate<Boolean>() {
        @Override
        public Boolean evaluate() throws IOException {
          return !states.isRegionInTransition(region);
        }
      });
    }
  }

  protected static MasterQuotaManager getMasterQuotaManager(final MasterProcedureEnv env)
      throws IOException {
    return ProcedureSyncWait.waitFor(env, "quota manager to be available",
        new ProcedureSyncWait.Predicate<MasterQuotaManager>() {
      @Override
      public MasterQuotaManager evaluate() throws IOException {
        return env.getMasterServices().getMasterQuotaManager();
      }
    });
  }
}
