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

import static org.apache.hadoop.hbase.procedure2.ProcedureUtil.DEFAULT_PROCEDURE_RETRY_MAX_SLEEP_TIME_MS;
import static org.apache.hadoop.hbase.procedure2.ProcedureUtil.DEFAULT_PROCEDURE_RETRY_SLEEP_INTERVAL_MS;
import static org.apache.hadoop.hbase.procedure2.ProcedureUtil.PROCEDURE_RETRY_MAX_SLEEP_TIME_MS;
import static org.apache.hadoop.hbase.procedure2.ProcedureUtil.PROCEDURE_RETRY_SLEEP_INTERVAL_MS;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.EnabledTableSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.master.snapshot.TakeSnapshotHandler;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounter.BackoffPolicy;
import org.apache.hadoop.hbase.util.RetryCounter.RetryConfig;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@InterfaceAudience.Private
public abstract class AbstractSnapshottingStateMachineTableProcedure<TState>
  extends AbstractStateMachineTableProcedure<TState> {

  /** If true, snapshot the table before making a destructive schema change. */
  public static final String SNAPSHOT_BEFORE_DELETE_ENABLED_KEY =
    "hbase.snapshot.before.delete.enabled";
  public static final boolean SNAPSHOT_BEFORE_DELETE_ENABLED_DEFAULT = false;

  /** TTL for snapshots taken before a destructive schema change, in seconds, default 86400. */
  public static final String SNAPSHOT_BEFORE_DELETE_TTL_KEY = "hbase.snapshot.before.delete.ttl";
  public static final int SNAPSHOT_BEFORE_DELETE_TTL_DEFAULT = 86400;

  private static final Logger LOG =
    LoggerFactory.getLogger(AbstractSnapshottingStateMachineTableProcedure.class);

  private MasterProcedureEnv env;
  private boolean isSnapshotEnabled;
  private long snapshotTtl;
  private RetryCounter retryCounter;
  private long retrySleepTime;
  private long retryMaxAttempts;
  private String snapshotName;
  private TakeSnapshotHandler snapshotHandler;

  public AbstractSnapshottingStateMachineTableProcedure() {
    super();
  }

  public AbstractSnapshottingStateMachineTableProcedure(MasterProcedureEnv env,
    ProcedurePrepareLatch latch) {
    super(env, latch);
    this.env = env;
    Configuration conf = env.getMasterConfiguration();
    snapshotTtl = conf.getLong(SNAPSHOT_BEFORE_DELETE_TTL_KEY, SNAPSHOT_BEFORE_DELETE_TTL_DEFAULT);
    retrySleepTime =
      conf.getLong(PROCEDURE_RETRY_SLEEP_INTERVAL_MS, DEFAULT_PROCEDURE_RETRY_SLEEP_INTERVAL_MS);
    retryMaxAttempts =
      conf.getLong(PROCEDURE_RETRY_MAX_SLEEP_TIME_MS, DEFAULT_PROCEDURE_RETRY_MAX_SLEEP_TIME_MS)
        / retrySleepTime;
    try {
      env.getMasterServices().getSnapshotManager().checkSnapshotSupport();
      // If we got past checkSnapshotSupport then we are capable of taking snapshots
      isSnapshotEnabled =
        conf.getBoolean(SNAPSHOT_BEFORE_DELETE_ENABLED_KEY, SNAPSHOT_BEFORE_DELETE_ENABLED_DEFAULT);
    } catch (UnsupportedOperationException e) {
      isSnapshotEnabled = false;
    }
  }

  protected boolean isSnapshotEnabled() {
    return isSnapshotEnabled;
  }

  protected String getSnapshotName() {
    return snapshotName;
  }

  // Used by tests
  protected void setSnapshotName(String snapshotName) {
    this.snapshotName = snapshotName;
  }

  // Used by tests
  protected void setSnapshotHandler(Class<? extends TakeSnapshotHandler> clazz,
    SnapshotDescription snapshot) {
    this.snapshotHandler = ReflectionUtils.newInstance(clazz, snapshot, env.getMasterServices(),
      env.getMasterServices().getSnapshotManager());
  }

  protected void takeSnapshot() throws ProcedureSuspendedException, IOException {
    if (getTableName() == null) {
      throw new NullPointerException("Cannot take snapshot because tableName is null");
    }
    long now = EnvironmentEdgeManager.currentTime();
    // Make the snapshot name if one has not been set
    if (snapshotName == null) {
      StringBuilder sb = new StringBuilder();
      sb.append("auto_");
      sb.append(getTableName());
      sb.append("_");
      sb.append(now);
      snapshotName = sb.toString();
    }
    // Make the snapshot description
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName(snapshotName)
      .setCreationTime(now).setVersion(SnapshotDescriptionUtils.SNAPSHOT_LAYOUT_VERSION)
      .setTable(getTableName().getNameAsString()).setTtl(snapshotTtl)
      .setType(SnapshotDescription.Type.FLUSH).build();
    takeSnapshot(snapshot);
  }

  // Used by tests
  protected void takeSnapshot(SnapshotDescription snapshot)
    throws ProcedureSuspendedException, IOException {
    SnapshotManager snapshotManager = env.getMasterServices().getSnapshotManager();
    boolean isDone;
    try {
      // isSnapshotDone will return true if the snapshot is completed and ready, false otherwise.
      // It will also throw an HBaseSnapshotException if the snapshot failed, which is what we
      // want, so this failure can propagate and fail the procedure execution.
      isDone = snapshotManager.isSnapshotDone(snapshot);
      LOG.debug("isSnapshotDone returns {}", isDone);
    } catch (UnknownSnapshotException e) {
      // We may not have requested the snapshot yet.
      LOG.debug("isSnapshotDone throws UnknownSnapshotException, we have not requested it yet");
      isDone = false;
    } catch (IOException e) {
      LOG.debug("isSnapshotDone throws an exception indicating a snapshotting failure", e);
      throw e; // rethrow
    }
    if (!isDone) {
      // A snapshot may be running on the table. It is either ours or someone else's but either way
      // we can handle it using the same logic.
      if (snapshotManager.isTakingSnapshot(getTableName())) {
        LOG.debug("Snapshot is in progress for table {}", getTableName());
      } else {
        // Our snapshot is not in completed state and no snapshot is running on the table. This
        // means we haven't made the request yet and need to do that now.
        LOG.info("Taking recovery snapshot {} on {}", snapshotName, getTableName());
        // We bypass the main entry point for SnapshotManager's take snapshot function in order
        // to supply a custom snapshot handler that avoids taking an exclusive lock on the table.
        // We already are holding the exclusive lock so that wouldn't work. So now we must upcall
        // to any coprocessors registered on the snapshot hook to preserve that semantic. This
        // should be refactored.
        // prepareWorkingDirectory prepares the working directory for the snapshot. We must call
        // this first.
        snapshotManager.prepareWorkingDirectory(snapshot);
        // call pre coproc hook
        TableDescriptor desc = null;
        MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
        MasterServices masterServices = env.getMasterServices();
        org.apache.hadoop.hbase.client.SnapshotDescription snapshotPOJO = null;
        if (cpHost != null) {
          snapshotPOJO = ProtobufUtil.createSnapshotDesc(snapshot);
          cpHost.preSnapshot(snapshotPOJO, desc, null);
        }
        if (snapshotHandler == null) {
          TableState state = masterServices.getTableStateManager().getTableState(getTableName());
          if (state.isEnabled()) {
            snapshotHandler =
              new NoLockEnabledTableSnapshotHandler(snapshot, masterServices, snapshotManager);
          } else {
            snapshotHandler =
              new NoLockDisabledTableSnapshotHandler(snapshot, masterServices, snapshotManager);
          }
        }
        snapshotManager.snapshotTable(snapshot, snapshotHandler);
        // call post coproc hook
        if (cpHost != null) {
          cpHost.postSnapshot(snapshotPOJO, desc, null);
        }
      }
      // If we have come back from suspension a retryCounter exists and is in progress.
      if (retryCounter == null) {
        // If we are suspending for the first time create the retry counter.
        RetryConfig retryConfig = new RetryConfig().setMaxAttempts((int) retryMaxAttempts)
          .setSleepInterval(retrySleepTime).setBackoffPolicy(new BackoffPolicy());
        retryCounter = new RetryCounter(retryConfig);
      }
      // If we cannot wait any longer for the snapshot we need to fail the procedure.
      if (!retryCounter.shouldRetry()) {
        String message = "Retries exceeded waiting for recovery snapshot on " + getTableName();
        LOG.warn(message);
        retryCounter = null;
        throw new DoNotRetryIOException(message);
      }
      // Suspend the procedure for the time calculated by the retry counter.
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.debug("Yielding for recovery snapshot {} on {}, suspend {} ms", snapshotName,
        getTableName(), backoff);
      throw suspend(Math.toIntExact(backoff), true);
    } else {
      // A snapshot with our name is complete.
      LOG.info("Recovery snapshot {} completed for {} with ttl {} seconds", snapshotName,
        getTableName(), snapshotTtl);
      retryCounter = null; // We are finished with the retry counter, clean it up
    }
  }

  protected void deleteSnapshot() {
    // No snapshot to delete if snapshot name is not found in state. This is weird but not a fatal
    // problem.
    if (snapshotName == null) {
      LOG.warn("Cannot delete snapshot because snapshotName is null. This is probably a bug.");
      return;
    }
    SnapshotManager snapshotManager = env.getMasterServices().getSnapshotManager();
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName(snapshotName).build();
    try {
      LOG.info("Deleting recovery snapshot {} on {}", snapshotName, getTableName());
      snapshotManager.deleteSnapshot(snapshot);
    } catch (SnapshotDoesNotExistException e) {
      LOG.debug("No recovery snapshot {} on {}, nothing to do", snapshotName, getTableName());
    } catch (SnapshotTTLExpiredException e) {
      LOG.debug("Expired recovery snapshot {} on {}, nothing to do", snapshotName, getTableName());
    } catch (IOException e) {
      // Some filesystem level failure occurred while deleting the snapshot but we should not fail
      // the rollback over this. Warn about it. If there is garbage in the fs the operator will
      // need to clean it up.
      LOG.warn(
        "Exception while deleting recovery snapshot " + snapshotName + " on " + getTableName(), e);
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    // We request ourselves suspended by throwing a ProcedureSuspendedException from yield() with a
    // timeout. When that timeout elapses the procedure framework will call this method. We need to
    // mark ourselves runnable and insert ourselves at the front of the queue in order to resume.
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  public static class NoLockEnabledTableSnapshotHandler extends EnabledTableSnapshotHandler {

    public NoLockEnabledTableSnapshotHandler(SnapshotDescription snapshot,
      MasterServices masterServices, SnapshotManager snapshotManager) throws IOException {
      super(snapshot, masterServices, snapshotManager);
    }

    @Override
    public NoLockEnabledTableSnapshotHandler prepare() throws IOException {
      // We skip acquiring this.tableLock, which is constructed as an exclusive lock on the table,
      // because we already have an exclusive lock on the table. This works and is safe because any
      // release() calls on the lock are a no-op if it is never taken.
      // So the only thing we need to do is load the table descriptor.
      this.htd = loadTableDescriptor(); // check that .tableinfo is present
      return this;
    }

  }

  public static class NoLockDisabledTableSnapshotHandler extends DisabledTableSnapshotHandler {

    public NoLockDisabledTableSnapshotHandler(SnapshotDescription snapshot,
      MasterServices masterServices, SnapshotManager snapshotManager) throws IOException {
      super(snapshot, masterServices, snapshotManager);
    }

    @Override
    public NoLockDisabledTableSnapshotHandler prepare() throws IOException {
      // We skip acquiring this.tableLock, which is constructed as an exclusive lock on the table,
      // because we already have an exclusive lock on the table. This works and is safe because any
      // release() calls on the lock are a no-op if it is never taken.
      // So the only thing we need to do is load the table descriptor.
      this.htd = loadTableDescriptor(); // check that .tableinfo is present
      return this;
    }
  }

}
