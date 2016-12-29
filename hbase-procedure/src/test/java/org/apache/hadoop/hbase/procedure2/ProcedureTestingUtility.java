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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.protobuf.generated.ErrorHandlingProtos.ForeignExceptionMessage;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.util.NonceKey;
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
    restart(procExecutor, null);
  }

  public static <TEnv> void restart(ProcedureExecutor<TEnv> procExecutor,
      Runnable beforeStartAction) throws Exception {
    ProcedureStore procStore = procExecutor.getStore();
    int storeThreads = procExecutor.getNumThreads();
    int execThreads = procExecutor.getNumThreads();
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
    procExecutor.start(execThreads);
  }

  public static <TEnv> void setKillBeforeStoreUpdate(ProcedureExecutor<TEnv> procExecutor,
      boolean value) {
    if (procExecutor.testing == null) {
      procExecutor.testing = new ProcedureExecutor.Testing();
    }
    procExecutor.testing.killBeforeStoreUpdate = value;
    LOG.warn("Set Kill before store update to: " + procExecutor.testing.killBeforeStoreUpdate);
    assertSingleExecutorForKillTests(procExecutor);
  }

  public static <TEnv> void setToggleKillBeforeStoreUpdate(ProcedureExecutor<TEnv> procExecutor,
      boolean value) {
    if (procExecutor.testing == null) {
      procExecutor.testing = new ProcedureExecutor.Testing();
    }
    procExecutor.testing.toggleKillBeforeStoreUpdate = value;
    assertSingleExecutorForKillTests(procExecutor);
  }

  public static <TEnv> void toggleKillBeforeStoreUpdate(ProcedureExecutor<TEnv> procExecutor) {
    if (procExecutor.testing == null) {
      procExecutor.testing = new ProcedureExecutor.Testing();
    }
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

  private static <TEnv> void assertSingleExecutorForKillTests(final ProcedureExecutor<TEnv> procExecutor) {
    if (procExecutor.testing == null) return;
    if (procExecutor.testing.killBeforeStoreUpdate ||
        procExecutor.testing.toggleKillBeforeStoreUpdate) {
      assertEquals("expected only one executor running during test with kill/restart",
        1, procExecutor.getNumThreads());
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

  public static <TEnv> void waitProcedure(ProcedureExecutor<TEnv> procExecutor, long procId) {
    while (!procExecutor.isFinished(procId) && procExecutor.isRunning()) {
      Threads.sleepWithoutInterrupt(250);
    }
  }

  public static <TEnv> void waitNoProcedureRunning(ProcedureExecutor<TEnv> procExecutor) {
    int stableRuns = 0;
    while (stableRuns < 10) {
      if (procExecutor.getActiveExecutorCount() > 0 || procExecutor.getRunnableSet().size() > 0) {
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
    ForeignExceptionMessage exception = result.getForeignExceptionMessage();
    String msg = exception != null ? result.getExceptionFullMessage() : "no exception found";
    assertFalse(msg, result.isFailed());
  }

  public static Throwable assertProcFailed(final ProcedureInfo result) {
    assertEquals(true, result.isFailed());
    LOG.info("procId=" + result.getProcId() + " exception: " + result.getException().getMessage());
    return getExceptionCause(result);
  }

  public static void assertIsAbortException(final ProcedureInfo result) {
    assertEquals(true, result.isFailed());
    LOG.info(result.getExceptionFullMessage());
    Throwable cause = getExceptionCause(result);
    assertTrue("expected abort exception, got " + cause,
      cause instanceof ProcedureAbortedException);
  }

  public static void assertIsTimeoutException(final ProcedureInfo result) {
    assertEquals(true, result.isFailed());
    LOG.info(result.getExceptionFullMessage());
    Throwable cause = getExceptionCause(result);
    assertTrue("expected TimeoutIOException, got " + cause, cause instanceof TimeoutIOException);
  }

  public static void assertIsIllegalArgumentException(final ProcedureInfo result) {
    assertEquals(true, result.isFailed());
    LOG.info(result.getExceptionFullMessage());
    Throwable cause = ProcedureTestingUtility.getExceptionCause(result);
    assertTrue("expected IllegalArgumentIOException, got " + cause,
      cause instanceof IllegalArgumentIOException);
  }

  public static Throwable getExceptionCause(final ProcedureInfo procInfo) {
    assert procInfo.getForeignExceptionMessage() != null;
    return RemoteProcedureException.fromProto(procInfo.getForeignExceptionMessage()).getCause();
  }

  public static class TestProcedure extends Procedure<Void> {
    public TestProcedure() {}

    public TestProcedure(long procId) {
      this(procId, 0);
    }

    public TestProcedure(long procId, long parentId) {
      setProcId(procId);
      if (parentId > 0) {
        setParentProcId(parentId);
      }
    }

    public void addStackId(final int index) {
      addStackIndex(index);
    }

    @Override
    protected Procedure[] execute(Void env) { return null; }

    @Override
    protected void rollback(Void env) { }

    @Override
    protected boolean abort(Void env) { return false; }

    @Override
    protected void serializeStateData(final OutputStream stream) throws IOException { }

    @Override
    protected void deserializeStateData(final InputStream stream) throws IOException { }
  }
}
