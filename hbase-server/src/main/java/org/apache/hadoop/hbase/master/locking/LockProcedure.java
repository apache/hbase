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
 * WITHOUTKey WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master.locking;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockProcedureData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@InterfaceAudience.Private
public final class LockProcedure extends Procedure<MasterProcedureEnv>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(LockProcedure.class);

  public static final int DEFAULT_REMOTE_LOCKS_TIMEOUT_MS = 30000;  // timeout in ms
  public static final String REMOTE_LOCKS_TIMEOUT_MS_CONF =
      "hbase.master.procedure.remote.locks.timeout.ms";
  // 10 min. Same as old ZK lock timeout.
  public static final int DEFAULT_LOCAL_MASTER_LOCKS_TIMEOUT_MS = 600000;
  public static final String LOCAL_MASTER_LOCKS_TIMEOUT_MS_CONF =
      "hbase.master.procedure.local.master.locks.timeout.ms";

  // Also used in serialized states, changes will affect backward compatibility.
  public enum LockType { SHARED, EXCLUSIVE }

  private String namespace;
  private TableName tableName;
  private HRegionInfo[] regionInfos;
  private LockType type;
  // underlying namespace/table/region lock.
  private LockInterface lock;
  private TableOperationType opType;
  private String description;
  // True when recovery of master lock from WALs
  private boolean recoveredMasterLock;
  // this is for internal working
  private boolean hasLock;

  private final ProcedureEvent<LockProcedure> event = new ProcedureEvent<LockProcedure>(this);
  // True if this proc acquired relevant locks. This value is for client checks.
  private final AtomicBoolean locked = new AtomicBoolean(false);
  // Last system time (in ms) when client sent the heartbeat.
  // Initialize to system time for non-null value in case of recovery.
  private final AtomicLong lastHeartBeat = new AtomicLong();
  // Set to true when unlock request is received.
  private final AtomicBoolean unlock = new AtomicBoolean(false);
  // decreased when locks are acquired. Only used for local (with master process) purposes.
  // Setting latch to non-null value increases default timeout to
  // DEFAULT_LOCAL_MASTER_LOCKS_TIMEOUT_MS (10 min) so that there is no need to heartbeat.
  private final CountDownLatch lockAcquireLatch;

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return opType;
  }

  private interface LockInterface {
    boolean acquireLock(MasterProcedureEnv env);
    void releaseLock(MasterProcedureEnv env);
  }

  public LockProcedure() {
    lockAcquireLatch = null;
  }

  private LockProcedure(final Configuration conf, final LockType type,
      final String description, final CountDownLatch lockAcquireLatch) {
    this.type = type;
    this.description = description;
    this.lockAcquireLatch = lockAcquireLatch;
    if (lockAcquireLatch == null) {
      setTimeout(conf.getInt(REMOTE_LOCKS_TIMEOUT_MS_CONF, DEFAULT_REMOTE_LOCKS_TIMEOUT_MS));
    } else {
      setTimeout(conf.getInt(LOCAL_MASTER_LOCKS_TIMEOUT_MS_CONF,
          DEFAULT_LOCAL_MASTER_LOCKS_TIMEOUT_MS));
    }
  }

  /**
   * Constructor for namespace lock.
   * @param lockAcquireLatch if not null, the latch is decreased when lock is acquired.
   */
  public LockProcedure(final Configuration conf, final String namespace, final LockType type,
      final String description, final CountDownLatch lockAcquireLatch)
      throws IllegalArgumentException {
    this(conf, type, description, lockAcquireLatch);

    if (namespace.isEmpty()) {
      throw new IllegalArgumentException("Empty namespace");
    }

    this.namespace = namespace;
    this.lock = setupNamespaceLock();
  }

  /**
   * Constructor for table lock.
   * @param lockAcquireLatch if not null, the latch is decreased when lock is acquired.
   */
  public LockProcedure(final Configuration conf, final TableName tableName, final LockType type,
      final String description, final CountDownLatch lockAcquireLatch)
      throws IllegalArgumentException {
    this(conf, type, description, lockAcquireLatch);

    this.tableName = tableName;
    this.lock = setupTableLock();
  }

  /**
   * Constructor for region lock(s).
   * @param lockAcquireLatch if not null, the latch is decreased when lock is acquired.
   *                        Useful for locks acquired locally from master process.
   * @throws IllegalArgumentException if all regions are not from same table.
   */
  public LockProcedure(final Configuration conf, final HRegionInfo[] regionInfos,
      final LockType type, final String description, final CountDownLatch lockAcquireLatch)
      throws IllegalArgumentException {
    this(conf, type, description, lockAcquireLatch);

    // Build HRegionInfo from region names.
    if (regionInfos.length == 0) {
      throw new IllegalArgumentException("No regions specified for region lock");
    }

    // check all regions belong to same table.
    final TableName regionTable = regionInfos[0].getTable();
    for (int i = 1; i < regionInfos.length; ++i) {
      if (!regionInfos[i].getTable().equals(regionTable)) {
        throw new IllegalArgumentException("All regions should be from same table");
      }
    }

    this.regionInfos = regionInfos;
    this.lock = setupRegionLock();
  }

  private boolean hasHeartbeatExpired() {
    return System.currentTimeMillis() - lastHeartBeat.get() >= getTimeout();
  }

  /**
   * Updates timeout deadline for the lock.
   */
  public void updateHeartBeat() {
    lastHeartBeat.set(System.currentTimeMillis());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Update heartbeat. Proc: " + toString());
    }
  }

  /**
   * Re run the procedure after every timeout to write new WAL entries so we don't hold back old
   * WALs.
   * @return false, so procedure framework doesn't mark this procedure as failure.
   */
  protected boolean setTimeoutFailure(final MasterProcedureEnv env) {
    synchronized (event) {
      if (!event.isReady()) {  // maybe unlock() awakened the event.
        setState(ProcedureProtos.ProcedureState.RUNNABLE);
        env.getProcedureScheduler().wakeEvent(event);
      }
    }
    return false;  // false: do not mark the procedure as failed.
  }

  // Can be called before procedure gets scheduled, in which case, the execute() will finish
  // immediately and release the underlying locks.
  public void unlock(final MasterProcedureEnv env) {
    unlock.set(true);
    locked.set(false);
    // Maybe timeout already awakened the event and the procedure has finished.
    synchronized (event) {
      if (!event.isReady()) {
        setState(ProcedureProtos.ProcedureState.RUNNABLE);
        env.getProcedureScheduler().wakeEvent(event);
      }
    }
  }

  @Override
  protected Procedure[] execute(final MasterProcedureEnv env) throws ProcedureSuspendedException {
    // Local master locks don't store any state, so on recovery, simply finish this procedure
    // immediately.
    if (recoveredMasterLock) return null;
    if (lockAcquireLatch != null) {
      lockAcquireLatch.countDown();
    }
    if (unlock.get() || hasHeartbeatExpired()) {
      locked.set(false);
      LOG.debug((unlock.get() ? "UNLOCKED - " : "TIMED OUT - ") + toString());
      return null;
    }
    synchronized (event) {
      env.getProcedureScheduler().suspendEvent(event);
      env.getProcedureScheduler().waitEvent(event, this);
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
    }
    throw new ProcedureSuspendedException();
  }

  @Override
  protected void rollback(final MasterProcedureEnv env) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(final MasterProcedureEnv env) {
    unlock(env);
    return true;
  }

  @Override
  protected void serializeStateData(final OutputStream stream) throws IOException {
    final LockProcedureData.Builder builder = LockProcedureData.newBuilder()
          .setLockType(LockServiceProtos.LockType.valueOf(type.name()))
          .setDescription(description);
    if (regionInfos != null) {
      for (int i = 0; i < regionInfos.length; ++i) {
        builder.addRegionInfo(HRegionInfo.convert(regionInfos[i]));
      }
    } else if (namespace != null) {
      builder.setNamespace(namespace);
    } else if (tableName != null) {
      builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    if (lockAcquireLatch != null) {
      builder.setIsMasterLock(true);
    }
    builder.build().writeDelimitedTo(stream);
  }

  @Override
  protected void deserializeStateData(final InputStream stream) throws IOException {
    final LockProcedureData state = LockProcedureData.parseDelimitedFrom(stream);
    type = LockType.valueOf(state.getLockType().name());
    description = state.getDescription();
    if (state.getRegionInfoCount() > 0) {
      regionInfos = new HRegionInfo[state.getRegionInfoCount()];
      for (int i = 0; i < state.getRegionInfoCount(); ++i) {
        regionInfos[i] = HRegionInfo.convert(state.getRegionInfo(i));
      }
    } else if (state.hasNamespace()) {
      namespace = state.getNamespace();
    } else if (state.hasTableName()) {
      tableName = ProtobufUtil.toTableName(state.getTableName());
    }
    recoveredMasterLock = state.getIsMasterLock();
    this.lock = setupLock();
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    boolean ret = lock.acquireLock(env);
    locked.set(ret);
    hasLock = ret;
    if (ret) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("LOCKED - " + toString());
      }
      lastHeartBeat.set(System.currentTimeMillis());
    }
    return ret;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    lock.releaseLock(env);
    hasLock = false;
  }

  /**
   * On recovery, re-execute from start to acquire the locks.
   * Need to explicitly set it to RUNNABLE because the procedure might have been in WAITING_TIMEOUT
   * state when crash happened. In which case, it'll be sent back to timeout queue on recovery,
   * which we don't want since we want to require locks.
   */
  @Override
  protected void beforeReplay(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
  }

  protected void toStringClassDetails(final StringBuilder builder) {
    super.toStringClassDetails(builder);
    if (regionInfos != null) {
      builder.append(" regions=");
      for (int i = 0; i < regionInfos.length; ++i) {
        if (i > 0) builder.append(",");
        builder.append(regionInfos[i].getShortNameToLog());
      }
    } else if (namespace != null) {
      builder.append(", namespace=").append(namespace);
    } else if (tableName != null) {
      builder.append(", tableName=").append(tableName);
    }
    builder.append(", type=").append(type);
  }

  private LockInterface setupLock() throws IllegalArgumentException {
    if (regionInfos != null) {
      return setupRegionLock();
    } else if (namespace != null) {
      return setupNamespaceLock();
    } else if (tableName != null) {
      return setupTableLock();
    } else {
      LOG.error("Unknown level specified in proc - " + toString());
      throw new IllegalArgumentException("no namespace/table/region provided");
    }
  }

  private LockInterface setupNamespaceLock() throws IllegalArgumentException {
    this.tableName = TableName.NAMESPACE_TABLE_NAME;
    switch (type) {
      case EXCLUSIVE:
        this.opType = TableOperationType.EDIT;
        return new NamespaceExclusiveLock();
      case SHARED:
        LOG.error("Shared lock on namespace not supported. Proc - " + toString());
        throw new IllegalArgumentException("Shared lock on namespace not supported");
      default:
        LOG.error("Unexpected lock type in proc - " + toString());
        throw new IllegalArgumentException("Wrong lock type: " + type.toString());
    }
  }

  private LockInterface setupTableLock() throws IllegalArgumentException {
    switch (type) {
      case EXCLUSIVE:
        this.opType = TableOperationType.EDIT;
        return new TableExclusiveLock();
      case SHARED:
        this.opType = TableOperationType.READ;
        return new TableSharedLock();
      default:
        LOG.error("Unexpected lock type in proc - " + toString());
        throw new IllegalArgumentException("Wrong lock type:" + type.toString());
    }
  }

  private LockInterface setupRegionLock() throws IllegalArgumentException {
    this.tableName = regionInfos[0].getTable();
    switch (type) {
      case EXCLUSIVE:
        this.opType = TableOperationType.REGION_EDIT;
        return new RegionExclusiveLock();
      default:
        LOG.error("Only exclusive lock supported on regions. Proc - " + toString());
        throw new IllegalArgumentException("Only exclusive lock supported on regions.");
    }
  }

  public String getDescription() {
    return description;
  }

  public boolean isLocked() {
    return locked.get();
  }

  @Override
  public boolean holdLock(final MasterProcedureEnv env) {
    return true;
  }

  @Override
  public boolean hasLock(final MasterProcedureEnv env) {
    return hasLock;
  }

  ///////////////////////
  // LOCK IMPLEMENTATIONS
  ///////////////////////

  private class TableExclusiveLock implements LockInterface {
    @Override
    public boolean acquireLock(final MasterProcedureEnv env) {
      return env.getProcedureScheduler().tryAcquireTableExclusiveLock(LockProcedure.this, tableName);
    }

    @Override
    public void releaseLock(final MasterProcedureEnv env) {
      env.getProcedureScheduler().releaseTableExclusiveLock(LockProcedure.this, tableName);
    }
  }

  private class TableSharedLock implements LockInterface {
    @Override
    public boolean acquireLock(final MasterProcedureEnv env) {
      return env.getProcedureScheduler().tryAcquireTableSharedLock(LockProcedure.this, tableName);
    }

    @Override
    public void releaseLock(final MasterProcedureEnv env) {
      env.getProcedureScheduler().releaseTableSharedLock(LockProcedure.this, tableName);
    }
  }

  private class NamespaceExclusiveLock implements LockInterface {
    @Override
    public boolean acquireLock(final MasterProcedureEnv env) {
      return env.getProcedureScheduler().tryAcquireNamespaceExclusiveLock(
          LockProcedure.this, namespace);
    }

    @Override
    public void releaseLock(final MasterProcedureEnv env) {
      env.getProcedureScheduler().releaseNamespaceExclusiveLock(
          LockProcedure.this, namespace);
    }
  }

  private class RegionExclusiveLock implements LockInterface {
    @Override
    public boolean acquireLock(final MasterProcedureEnv env) {
      return !env.getProcedureScheduler().waitRegions(LockProcedure.this, tableName, regionInfos);
    }

    @Override
    public void releaseLock(final MasterProcedureEnv env) {
      env.getProcedureScheduler().wakeRegions(LockProcedure.this, tableName, regionInfos);
    }
  }
}
