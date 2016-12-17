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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.hadoop.hbase.procedure2.store.NoopProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.Threads;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProcedureTestingUtility {
  private static final Log LOG = LogFactory.getLog(ProcedureTestingUtility.class);

  private ProcedureTestingUtility() {
  }

  public static ProcedureStore createStore(final Configuration conf, final FileSystem fs,
      final Path baseDir) throws IOException {
    return createWalStore(conf, fs, baseDir);
  }

  public static WALProcedureStore createWalStore(final Configuration conf, final FileSystem fs,
      final Path logDir) throws IOException {
    return new WALProcedureStore(conf, fs, logDir, new WALProcedureStore.LeaseRecovery() {
      @Override
      public void recoverFileLease(FileSystem fs, Path path) throws IOException {
        // no-op
      }
    });
  }

  public static <TEnv> void restart(ProcedureExecutor<TEnv> procExecutor)
      throws Exception {
    restart(procExecutor, null, true);
  }

  public static <TEnv> void restart(ProcedureExecutor<TEnv> procExecutor,
      Runnable beforeStartAction, boolean failOnCorrupted) throws Exception {
    ProcedureStore procStore = procExecutor.getStore();
    int storeThreads = procExecutor.getCorePoolSize();
    int execThreads = procExecutor.getCorePoolSize();
    // stop
    procExecutor.stop();
    procExecutor.join();
    procStore.stop(false);
    // nothing running...
    if (beforeStartAction != null) {
      beforeStartAction.run();
    }
    // re-start
    procStore.start(storeThreads);
    procExecutor.start(execThreads, failOnCorrupted);
  }

  public static void storeRestart(ProcedureStore procStore, ProcedureStore.ProcedureLoader loader)
      throws Exception {
    procStore.stop(false);
    procStore.start(procStore.getNumThreads());
    procStore.recoverLease();
    procStore.load(loader);
  }

  public static LoadCounter storeRestartAndAssert(ProcedureStore procStore, long maxProcId,
      long runnableCount, int completedCount, int corruptedCount) throws Exception {
    final LoadCounter loader = new LoadCounter();
    storeRestart(procStore, loader);
    assertEquals(maxProcId, loader.getMaxProcId());
    assertEquals(runnableCount, loader.getRunnableCount());
    assertEquals(completedCount, loader.getCompletedCount());
    assertEquals(corruptedCount, loader.getCorruptedCount());
    return loader;
  }

  private static <TEnv> void createExecutorTesting(final ProcedureExecutor<TEnv> procExecutor) {
    if (procExecutor.testing == null) {
      procExecutor.testing = new ProcedureExecutor.Testing();
    }
  }

  public static <TEnv> void setKillIfSuspended(ProcedureExecutor<TEnv> procExecutor,
      boolean value) {
    createExecutorTesting(procExecutor);
    procExecutor.testing.killIfSuspended = value;
  }

  public static <TEnv> void setKillBeforeStoreUpdate(ProcedureExecutor<TEnv> procExecutor,
      boolean value) {
    createExecutorTesting(procExecutor);
    procExecutor.testing.killBeforeStoreUpdate = value;
    LOG.warn("Set Kill before store update to: " + procExecutor.testing.killBeforeStoreUpdate);
    assertSingleExecutorForKillTests(procExecutor);
  }

  public static <TEnv> void setToggleKillBeforeStoreUpdate(ProcedureExecutor<TEnv> procExecutor,
      boolean value) {
    createExecutorTesting(procExecutor);
    procExecutor.testing.toggleKillBeforeStoreUpdate = value;
    assertSingleExecutorForKillTests(procExecutor);
  }

  public static <TEnv> void toggleKillBeforeStoreUpdate(ProcedureExecutor<TEnv> procExecutor) {
    createExecutorTesting(procExecutor);
    procExecutor.testing.killBeforeStoreUpdate = !procExecutor.testing.killBeforeStoreUpdate;
    LOG.warn("Set Kill before store update to: " + procExecutor.testing.killBeforeStoreUpdate);
    assertSingleExecutorForKillTests(procExecutor);
  }

  public static <TEnv> void setKillAndToggleBeforeStoreUpdate(ProcedureExecutor<TEnv> procExecutor,
      boolean value) {
    ProcedureTestingUtility.setKillBeforeStoreUpdate(procExecutor, value);
    ProcedureTestingUtility.setToggleKillBeforeStoreUpdate(procExecutor, value);
    assertSingleExecutorForKillTests(procExecutor);
  }

  private static <TEnv> void assertSingleExecutorForKillTests(
      final ProcedureExecutor<TEnv> procExecutor) {
    if (procExecutor.testing == null) return;
    if (procExecutor.testing.killBeforeStoreUpdate ||
        procExecutor.testing.toggleKillBeforeStoreUpdate) {
      assertEquals("expected only one executor running during test with kill/restart",
        1, procExecutor.getCorePoolSize());
    }
  }

  public static <TEnv> long submitAndWait(Configuration conf, TEnv env, Procedure<TEnv> proc)
      throws IOException {
    NoopProcedureStore procStore = new NoopProcedureStore();
    ProcedureExecutor<TEnv> procExecutor = new ProcedureExecutor<TEnv>(conf, env, procStore);
    procStore.start(1);
    procExecutor.start(1, false);
    try {
      return submitAndWait(procExecutor, proc, HConstants.NO_NONCE, HConstants.NO_NONCE);
    } finally {
      procStore.stop(false);
      procExecutor.stop();
    }
  }

  public static <TEnv> long submitAndWait(ProcedureExecutor<TEnv> procExecutor, Procedure proc) {
    return submitAndWait(procExecutor, proc, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  public static <TEnv> long submitAndWait(ProcedureExecutor<TEnv> procExecutor, Procedure proc,
      final long nonceGroup, final long nonce) {
    long procId = submitProcedure(procExecutor, proc, nonceGroup, nonce);
    waitProcedure(procExecutor, procId);
    return procId;
  }

  public static <TEnv> long submitProcedure(ProcedureExecutor<TEnv> procExecutor, Procedure proc,
      final long nonceGroup, final long nonce) {
    final NonceKey nonceKey = procExecutor.createNonceKey(nonceGroup, nonce);
    long procId = procExecutor.registerNonce(nonceKey);
    assertFalse(procId >= 0);
    return procExecutor.submitProcedure(proc, nonceKey);
  }

  public static <TEnv> void waitProcedure(ProcedureExecutor<TEnv> procExecutor, Procedure proc) {
    while (proc.getState() == ProcedureState.INITIALIZING) {
      Threads.sleepWithoutInterrupt(250);
    }
    waitProcedure(procExecutor, proc.getProcId());
  }

  public static <TEnv> void waitProcedure(ProcedureExecutor<TEnv> procExecutor, long procId) {
    while (!procExecutor.isFinished(procId) && procExecutor.isRunning()) {
      Threads.sleepWithoutInterrupt(250);
    }
  }

  public static <TEnv> void waitNoProcedureRunning(ProcedureExecutor<TEnv> procExecutor) {
    int stableRuns = 0;
    while (stableRuns < 10) {
      if (procExecutor.getActiveExecutorCount() > 0 || procExecutor.getScheduler().size() > 0) {
        stableRuns = 0;
        Threads.sleepWithoutInterrupt(100);
      } else {
        stableRuns++;
        Threads.sleepWithoutInterrupt(25);
      }
    }
  }

  public static <TEnv> void assertProcNotYetCompleted(ProcedureExecutor<TEnv> procExecutor,
      long procId) {
    assertFalse("expected a running proc", procExecutor.isFinished(procId));
    assertEquals(null, procExecutor.getResult(procId));
  }

  public static <TEnv> void assertProcNotFailed(ProcedureExecutor<TEnv> procExecutor,
      long procId) {
    ProcedureInfo result = procExecutor.getResult(procId);
    assertTrue("expected procedure result", result != null);
    assertProcNotFailed(result);
  }

  public static void assertProcNotFailed(final ProcedureInfo result) {
    assertFalse("found exception: " + result.getException(), result.isFailed());
  }

  public static <TEnv> Throwable assertProcFailed(final ProcedureExecutor<TEnv> procExecutor,
      final long procId) {
    ProcedureInfo result = procExecutor.getResult(procId);
    assertTrue("expected procedure result", result != null);
    return assertProcFailed(result);
  }

  public static Throwable assertProcFailed(final ProcedureInfo result) {
    assertEquals(true, result.isFailed());
    LOG.info("procId=" + result.getProcId() + " exception: " + result.getException().getMessage());
    return getExceptionCause(result);
  }

  public static void assertIsAbortException(final ProcedureInfo result) {
    Throwable cause = assertProcFailed(result);
    assertTrue("expected abort exception, got "+ cause, cause instanceof ProcedureAbortedException);
  }

  public static void assertIsTimeoutException(final ProcedureInfo result) {
    Throwable cause = assertProcFailed(result);
    assertTrue("expected TimeoutIOException, got " + cause, cause instanceof TimeoutIOException);
  }

  public static void assertIsIllegalArgumentException(final ProcedureInfo result) {
    Throwable cause = assertProcFailed(result);
    assertTrue("expected IllegalArgumentIOException, got " + cause,
      cause instanceof IllegalArgumentIOException);
  }

  public static Throwable getExceptionCause(final ProcedureInfo procInfo) {
    assert procInfo.isFailed();
    Throwable cause = procInfo.getException().getCause();
    return cause == null ? procInfo.getException() : cause;
  }

  /**
   * Run through all procedure flow states TWICE while also restarting
   * procedure executor at each step; i.e force a reread of procedure store.
   *
   *<p>It does
   * <ol><li>Execute step N - kill the executor before store update
   * <li>Restart executor/store
   * <li>Execute step N - and then save to store
   * </ol>
   *
   *<p>This is a good test for finding state that needs persisting and steps that are not
   * idempotent.
   */
  public static <TEnv> void testRecoveryAndDoubleExecution(final ProcedureExecutor<TEnv> procExec,
      final long procId) throws Exception {
    testRecoveryAndDoubleExecution(procExec, procId, false);
  }

  public static <TEnv> void testRecoveryAndDoubleExecution(final ProcedureExecutor<TEnv> procExec,
      final long procId, final boolean expectFailure) throws Exception {
    testRecoveryAndDoubleExecution(procExec, procId, expectFailure, null);
  }

  public static <TEnv> void testRecoveryAndDoubleExecution(final ProcedureExecutor<TEnv> procExec,
      final long procId, final boolean expectFailure, final Runnable customRestart)
      throws Exception {
    final Procedure proc = procExec.getProcedure(procId);
    waitProcedure(procExec, procId);
    assertEquals(false, procExec.isRunning());

    for (int i = 0; !procExec.isFinished(procId); ++i) {
      LOG.info("Restart " + i + " exec state: " + proc);
      if (customRestart != null) {
        customRestart.run();
      } else {
        restart(procExec);
      }
      waitProcedure(procExec, procId);
    }

    assertEquals(true, procExec.isRunning());
    if (expectFailure) {
      assertProcFailed(procExec, procId);
    } else {
      assertProcNotFailed(procExec, procId);
    }
  }

  public static class NoopProcedure<TEnv> extends Procedure<TEnv> {
    public NoopProcedure() {}

    @Override
    protected Procedure[] execute(TEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      return null;
    }

    @Override
    protected void rollback(TEnv env) throws IOException, InterruptedException {
    }

    @Override
    protected boolean abort(TEnv env) { return false; }

    @Override
    protected void serializeStateData(final OutputStream stream) throws IOException {
    }

    @Override
    protected void deserializeStateData(final InputStream stream) throws IOException {
    }
  }

  public static class TestProcedure extends NoopProcedure<Void> {
    private byte[] data = null;

    public TestProcedure() {}

    public TestProcedure(long procId) {
      this(procId, 0);
    }

    public TestProcedure(long procId, long parentId) {
      this(procId, parentId, null);
    }

    public TestProcedure(long procId, long parentId, byte[] data) {
      this(procId, parentId, parentId, data);
    }

    public TestProcedure(long procId, long parentId, long rootId, byte[] data) {
      setData(data);
      setProcId(procId);
      if (parentId > 0) {
        setParentProcId(parentId);
      }
      if (rootId > 0 || parentId > 0) {
        setRootProcId(rootId);
      }
    }

    public void addStackId(final int index) {
      addStackIndex(index);
    }

    public void setFinishedState() {
      setState(ProcedureState.FINISHED);
    }

    public void setData(final byte[] data) {
      this.data = data;
    }

    @Override
    protected void serializeStateData(final OutputStream stream) throws IOException {
      StreamUtils.writeRawVInt32(stream, data != null ? data.length : 0);
      if (data != null) stream.write(data);
    }

    @Override
    protected void deserializeStateData(final InputStream stream) throws IOException {
      int len = StreamUtils.readRawVarint32(stream);
      if (len > 0) {
        data = new byte[len];
        stream.read(data);
      } else {
        data = null;
      }
    }

    // Mark acquire/release lock functions public for test uses.
    @Override
    public boolean acquireLock(Void env) {
      return true;
    }

    @Override
    public void releaseLock(Void env) {
      // no-op
    }
  }

  public static class LoadCounter implements ProcedureStore.ProcedureLoader {
    private final ArrayList<Procedure> corrupted = new ArrayList<Procedure>();
    private final ArrayList<ProcedureInfo> completed = new ArrayList<ProcedureInfo>();
    private final ArrayList<Procedure> runnable = new ArrayList<Procedure>();

    private Set<Long> procIds;
    private long maxProcId = 0;

    public LoadCounter() {
      this(null);
    }

    public LoadCounter(final Set<Long> procIds) {
      this.procIds = procIds;
    }

    public void reset() {
      reset(null);
    }

    public void reset(final Set<Long> procIds) {
      corrupted.clear();
      completed.clear();
      runnable.clear();
      this.procIds = procIds;
      this.maxProcId = 0;
    }

    public long getMaxProcId() {
      return maxProcId;
    }

    public ArrayList<Procedure> getRunnables() {
      return runnable;
    }

    public int getRunnableCount() {
      return runnable.size();
    }

    public ArrayList<ProcedureInfo> getCompleted() {
      return completed;
    }

    public int getCompletedCount() {
      return completed.size();
    }

    public int getLoadedCount() {
      return runnable.size() + completed.size();
    }

    public ArrayList<Procedure> getCorrupted() {
      return corrupted;
    }

    public int getCorruptedCount() {
      return corrupted.size();
    }

    public boolean isRunnable(final long procId) {
      for (Procedure proc: runnable) {
        if (proc.getProcId() == procId) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void setMaxProcId(long maxProcId) {
      this.maxProcId = maxProcId;
    }

    @Override
    public void load(ProcedureIterator procIter) throws IOException {
      while (procIter.hasNext()) {
        long procId;
        if (procIter.isNextCompleted()) {
          ProcedureInfo proc = procIter.nextAsProcedureInfo();
          procId = proc.getProcId();
          LOG.debug("loading completed procId=" + procId + ": " + proc);
          completed.add(proc);
        } else {
          Procedure proc = procIter.nextAsProcedure();
          procId = proc.getProcId();
          LOG.debug("loading runnable procId=" + procId + ": " + proc);
          runnable.add(proc);
        }
        if (procIds != null) {
          assertTrue("procId=" + procId + " unexpected", procIds.contains(procId));
        }
      }
    }

    @Override
    public void handleCorrupted(ProcedureIterator procIter) throws IOException {
      while (procIter.hasNext()) {
        Procedure proc = procIter.nextAsProcedure();
        LOG.debug("corrupted procId=" + proc.getProcId() + ": " + proc);
        corrupted.add(proc);
      }
    }
  }
}
