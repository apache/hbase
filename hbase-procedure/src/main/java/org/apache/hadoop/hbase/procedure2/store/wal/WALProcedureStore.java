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

package org.apache.hadoop.hbase.procedure2.store.wal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreBase;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.hadoop.hbase.procedure2.util.ByteSlot;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureWALHeader;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;

import com.google.common.annotations.VisibleForTesting;

/**
 * WAL implementation of the ProcedureStore.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class WALProcedureStore extends ProcedureStoreBase {
  private static final Log LOG = LogFactory.getLog(WALProcedureStore.class);

  public interface LeaseRecovery {
    void recoverFileLease(FileSystem fs, Path path) throws IOException;
  }

  public static final String MAX_RETRIES_BEFORE_ROLL_CONF_KEY =
    "hbase.procedure.store.wal.max.retries.before.roll";
  private static final int DEFAULT_MAX_RETRIES_BEFORE_ROLL = 3;

  public static final String WAIT_BEFORE_ROLL_CONF_KEY =
    "hbase.procedure.store.wal.wait.before.roll";
  private static final int DEFAULT_WAIT_BEFORE_ROLL = 500;

  public static final String ROLL_RETRIES_CONF_KEY =
    "hbase.procedure.store.wal.max.roll.retries";
  private static final int DEFAULT_ROLL_RETRIES = 3;

  public static final String MAX_SYNC_FAILURE_ROLL_CONF_KEY =
    "hbase.procedure.store.wal.sync.failure.roll.max";
  private static final int DEFAULT_MAX_SYNC_FAILURE_ROLL = 3;

  public static final String PERIODIC_ROLL_CONF_KEY =
    "hbase.procedure.store.wal.periodic.roll.msec";
  private static final int DEFAULT_PERIODIC_ROLL = 60 * 60 * 1000; // 1h

  public static final String SYNC_WAIT_MSEC_CONF_KEY = "hbase.procedure.store.wal.sync.wait.msec";
  private static final int DEFAULT_SYNC_WAIT_MSEC = 100;

  public static final String USE_HSYNC_CONF_KEY = "hbase.procedure.store.wal.use.hsync";
  private static final boolean DEFAULT_USE_HSYNC = true;

  public static final String ROLL_THRESHOLD_CONF_KEY = "hbase.procedure.store.wal.roll.threshold";
  private static final long DEFAULT_ROLL_THRESHOLD = 32 * 1024 * 1024; // 32M

  public static final String STORE_WAL_SYNC_STATS_COUNT =
      "hbase.procedure.store.wal.sync.stats.count";
  private static final int DEFAULT_SYNC_STATS_COUNT = 10;

  private final LinkedList<ProcedureWALFile> logs = new LinkedList<>();
  private final ProcedureStoreTracker storeTracker = new ProcedureStoreTracker();
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition waitCond = lock.newCondition();
  private final Condition slotCond = lock.newCondition();
  private final Condition syncCond = lock.newCondition();

  private final LeaseRecovery leaseRecovery;
  private final Configuration conf;
  private final FileSystem fs;
  private final Path logDir;

  private final AtomicReference<Throwable> syncException = new AtomicReference<Throwable>();
  private final AtomicBoolean loading = new AtomicBoolean(true);
  private final AtomicBoolean inSync = new AtomicBoolean(false);
  private final AtomicLong totalSynced = new AtomicLong(0);
  private final AtomicLong lastRollTs = new AtomicLong(0);
  private final AtomicLong syncId = new AtomicLong(0);

  private LinkedTransferQueue<ByteSlot> slotsCache = null;
  private Set<ProcedureWALFile> corruptedLogs = null;
  private FSDataOutputStream stream = null;
  private long flushLogId = 0;
  private int slotIndex = 0;
  private Thread syncThread;
  private ByteSlot[] slots;

  private int maxRetriesBeforeRoll;
  private int maxSyncFailureRoll;
  private int waitBeforeRoll;
  private int rollRetries;
  private int periodicRollMsec;
  private long rollThreshold;
  private boolean useHsync;
  private int syncWaitMsec;

  // Variables used for UI display
  private CircularFifoBuffer syncMetricsBuffer;

  public static class SyncMetrics {
    private long timestamp;
    private long syncWaitMs;
    private long totalSyncedBytes;
    private int syncedEntries;
    private float syncedPerSec;

    public long getTimestamp() {
      return timestamp;
    }

    public long getSyncWaitMs() {
      return syncWaitMs;
    }

    public long getTotalSyncedBytes() {
      return totalSyncedBytes;
    }

    public long getSyncedEntries() {
      return syncedEntries;
    }

    public float getSyncedPerSec() {
      return syncedPerSec;
    }
  }

  public WALProcedureStore(final Configuration conf, final FileSystem fs, final Path logDir,
      final LeaseRecovery leaseRecovery) {
    this.fs = fs;
    this.conf = conf;
    this.logDir = logDir;
    this.leaseRecovery = leaseRecovery;
  }

  @Override
  public void start(int numSlots) throws IOException {
    if (!setRunning(true)) {
      return;
    }

    // Init buffer slots
    loading.set(true);
    slots = new ByteSlot[numSlots];
    slotsCache = new LinkedTransferQueue();
    while (slotsCache.size() < numSlots) {
      slotsCache.offer(new ByteSlot());
    }

    // Tunings
    maxRetriesBeforeRoll =
      conf.getInt(MAX_RETRIES_BEFORE_ROLL_CONF_KEY, DEFAULT_MAX_RETRIES_BEFORE_ROLL);
    maxSyncFailureRoll = conf.getInt(MAX_SYNC_FAILURE_ROLL_CONF_KEY, DEFAULT_MAX_SYNC_FAILURE_ROLL);
    waitBeforeRoll = conf.getInt(WAIT_BEFORE_ROLL_CONF_KEY, DEFAULT_WAIT_BEFORE_ROLL);
    rollRetries = conf.getInt(ROLL_RETRIES_CONF_KEY, DEFAULT_ROLL_RETRIES);
    rollThreshold = conf.getLong(ROLL_THRESHOLD_CONF_KEY, DEFAULT_ROLL_THRESHOLD);
    periodicRollMsec = conf.getInt(PERIODIC_ROLL_CONF_KEY, DEFAULT_PERIODIC_ROLL);
    syncWaitMsec = conf.getInt(SYNC_WAIT_MSEC_CONF_KEY, DEFAULT_SYNC_WAIT_MSEC);
    useHsync = conf.getBoolean(USE_HSYNC_CONF_KEY, DEFAULT_USE_HSYNC);

    // WebUI
    syncMetricsBuffer = new CircularFifoBuffer(
      conf.getInt(STORE_WAL_SYNC_STATS_COUNT, DEFAULT_SYNC_STATS_COUNT));

    // Init sync thread
    syncThread = new Thread("WALProcedureStoreSyncThread") {
      @Override
      public void run() {
        try {
          syncLoop();
        } catch (Throwable e) {
          LOG.error("Got an exception from the sync-loop", e);
          if (!isSyncAborted()) {
            sendAbortProcessSignal();
          }
        }
      }
    };
    syncThread.start();
  }

  @Override
  public void stop(final boolean abort) {
    if (!setRunning(false)) {
      return;
    }

    LOG.info("Stopping the WAL Procedure Store, isAbort=" + abort +
      (isSyncAborted() ? " (self aborting)" : ""));
    sendStopSignal();
    if (!isSyncAborted()) {
      try {
        while (syncThread.isAlive()) {
          sendStopSignal();
          syncThread.join(250);
        }
      } catch (InterruptedException e) {
        LOG.warn("join interrupted", e);
        Thread.currentThread().interrupt();
      }
    }

    // Close the writer
    closeCurrentLogStream();

    // Close the old logs
    // they should be already closed, this is just in case the load fails
    // and we call start() and then stop()
    for (ProcedureWALFile log: logs) {
      log.close();
    }
    logs.clear();
  }

  private void sendStopSignal() {
    if (lock.tryLock()) {
      try {
        waitCond.signalAll();
        syncCond.signalAll();
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public int getNumThreads() {
    return slots == null ? 0 : slots.length;
  }

  public ProcedureStoreTracker getStoreTracker() {
    return storeTracker;
  }

  public ArrayList<ProcedureWALFile> getActiveLogs() {
    lock.lock();
    try {
      return new ArrayList<ProcedureWALFile>(logs);
    } finally {
      lock.unlock();
    }
  }

  public Set<ProcedureWALFile> getCorruptedLogs() {
    return corruptedLogs;
  }

  @Override
  public void recoverLease() throws IOException {
    lock.lock();
    try {
      LOG.info("Starting WAL Procedure Store lease recovery");
      FileStatus[] oldLogs = getLogFiles();
      while (isRunning()) {
        // Get Log-MaxID and recover lease on old logs
        try {
          flushLogId = initOldLogs(oldLogs);
        } catch (FileNotFoundException e) {
          LOG.warn("someone else is active and deleted logs. retrying.", e);
          oldLogs = getLogFiles();
          continue;
        }

        // Create new state-log
        if (!rollWriter(flushLogId + 1)) {
          // someone else has already created this log
          LOG.debug("someone else has already created log " + flushLogId);
          continue;
        }

        // We have the lease on the log
        oldLogs = getLogFiles();
        if (getMaxLogId(oldLogs) > flushLogId) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Someone else created new logs. Expected maxLogId < " + flushLogId);
          }
          logs.getLast().removeFile();
          continue;
        }

        LOG.info("Lease acquired for flushLogId: " + flushLogId);
        break;
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void load(final ProcedureLoader loader) throws IOException {
    if (logs.isEmpty()) {
      throw new RuntimeException("recoverLease() must be called before loading data");
    }

    // Nothing to do, If we have only the current log.
    if (logs.size() == 1) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No state logs to replay.");
      }
      loader.setMaxProcId(0);
      loading.set(false);
      return;
    }

    // Load the old logs
    Iterator<ProcedureWALFile> it = logs.descendingIterator();
    it.next(); // Skip the current log
    try {
      ProcedureWALFormat.load(it, storeTracker, new ProcedureWALFormat.Loader() {
        @Override
        public void setMaxProcId(long maxProcId) {
          loader.setMaxProcId(maxProcId);
        }

        @Override
        public void load(ProcedureIterator procIter) throws IOException {
          loader.load(procIter);
        }

        @Override
        public void handleCorrupted(ProcedureIterator procIter) throws IOException {
          loader.handleCorrupted(procIter);
        }

        @Override
        public void markCorruptedWAL(ProcedureWALFile log, IOException e) {
          if (corruptedLogs == null) {
            corruptedLogs = new HashSet<ProcedureWALFile>();
          }
          corruptedLogs.add(log);
          // TODO: sideline corrupted log
        }
      });
    } finally {
      loading.set(false);
    }
  }

  @Override
  public void insert(final Procedure proc, final Procedure[] subprocs) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Insert " + proc + ", subproc=" + Arrays.toString(subprocs));
    }

    ByteSlot slot = acquireSlot();
    try {
      // Serialize the insert
      long[] subProcIds = null;
      if (subprocs != null) {
        ProcedureWALFormat.writeInsert(slot, proc, subprocs);
        subProcIds = new long[subprocs.length];
        for (int i = 0; i < subprocs.length; ++i) {
          subProcIds[i] = subprocs[i].getProcId();
        }
      } else {
        assert !proc.hasParent();
        ProcedureWALFormat.writeInsert(slot, proc);
      }

      // Push the transaction data and wait until it is persisted
      pushData(PushType.INSERT, slot, proc.getProcId(), subProcIds);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize one of the procedure: proc=" + proc +
                ", subprocs=" + Arrays.toString(subprocs), e);
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }
  }

  @Override
  public void insert(final Procedure[] procs) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Insert " + Arrays.toString(procs));
    }

    ByteSlot slot = acquireSlot();
    try {
      // Serialize the insert
      long[] procIds = new long[procs.length];
      for (int i = 0; i < procs.length; ++i) {
        assert !procs[i].hasParent();
        procIds[i] = procs[i].getProcId();
        ProcedureWALFormat.writeInsert(slot, procs[i]);
      }

      // Push the transaction data and wait until it is persisted
      pushData(PushType.INSERT, slot, Procedure.NO_PROC_ID, procIds);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize one of the procedure: " + Arrays.toString(procs), e);
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }
  }

  @Override
  public void update(final Procedure proc) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Update " + proc);
    }

    ByteSlot slot = acquireSlot();
    try {
      // Serialize the update
      ProcedureWALFormat.writeUpdate(slot, proc);

      // Push the transaction data and wait until it is persisted
      pushData(PushType.UPDATE, slot, proc.getProcId(), null);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize the procedure: " + proc, e);
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }
  }

  @Override
  public void delete(final long procId) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Delete " + procId);
    }

    ByteSlot slot = acquireSlot();
    try {
      // Serialize the delete
      ProcedureWALFormat.writeDelete(slot, procId);

      // Push the transaction data and wait until it is persisted
      pushData(PushType.DELETE, slot, procId, null);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize the procedure: " + procId, e);
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }
  }

  @Override
  public void delete(final Procedure proc, final long[] subProcIds) {
    assert proc != null : "expected a non-null procedure";
    assert subProcIds != null && subProcIds.length > 0 : "expected subProcIds";
    if (LOG.isTraceEnabled()) {
      LOG.trace("Update " + proc + " and Delete " + Arrays.toString(subProcIds));
    }

    ByteSlot slot = acquireSlot();
    try {
      // Serialize the delete
      ProcedureWALFormat.writeDelete(slot, proc, subProcIds);

      // Push the transaction data and wait until it is persisted
      pushData(PushType.DELETE, slot, proc.getProcId(), subProcIds);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize the procedure: " + proc, e);
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }
  }

  @Override
  public void delete(final long[] procIds, final int offset, final int count) {
    if (count == 0) return;
    if (offset == 0 && count == procIds.length) {
      delete(procIds);
    } else if (count == 1) {
      delete(procIds[offset]);
    } else {
      delete(Arrays.copyOfRange(procIds, offset, offset + count));
    }
  }

  private void delete(final long[] procIds) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Delete " + Arrays.toString(procIds));
    }

    final ByteSlot slot = acquireSlot();
    try {
      // Serialize the delete
      for (int i = 0; i < procIds.length; ++i) {
        ProcedureWALFormat.writeDelete(slot, procIds[i]);
      }

      // Push the transaction data and wait until it is persisted
      pushData(PushType.DELETE, slot, Procedure.NO_PROC_ID, procIds);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize the procedures: " + Arrays.toString(procIds), e);
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }
  }

  private ByteSlot acquireSlot() {
    ByteSlot slot = slotsCache.poll();
    return slot != null ? slot : new ByteSlot();
  }

  private void releaseSlot(final ByteSlot slot) {
    slot.reset();
    slotsCache.offer(slot);
  }

  private enum PushType { INSERT, UPDATE, DELETE };

  private long pushData(final PushType type, final ByteSlot slot,
      final long procId, final long[] subProcIds) {
    if (!isRunning()) {
      throw new RuntimeException("the store must be running before inserting data");
    }
    if (logs.isEmpty()) {
      throw new RuntimeException("recoverLease() must be called before inserting data");
    }

    long logId = -1;
    lock.lock();
    try {
      // Wait for the sync to be completed
      while (true) {
        if (!isRunning()) {
          throw new RuntimeException("store no longer running");
        } else if (isSyncAborted()) {
          throw new RuntimeException("sync aborted", syncException.get());
        } else if (inSync.get()) {
          syncCond.await();
        } else if (slotIndex == slots.length) {
          slotCond.signal();
          syncCond.await();
        } else {
          break;
        }
      }

      final long pushSyncId = syncId.get();
      updateStoreTracker(type, procId, subProcIds);
      slots[slotIndex++] = slot;
      logId = flushLogId;

      // Notify that there is new data
      if (slotIndex == 1) {
        waitCond.signal();
      }

      // Notify that the slots are full
      if (slotIndex == slots.length) {
        waitCond.signal();
        slotCond.signal();
      }

      while (pushSyncId == syncId.get() && isRunning()) {
        syncCond.await();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      sendAbortProcessSignal();
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
      if (isSyncAborted()) {
        throw new RuntimeException("sync aborted", syncException.get());
      }
    }
    return logId;
  }

  private void updateStoreTracker(final PushType type,
      final long procId, final long[] subProcIds) {
    switch (type) {
      case INSERT:
        if (subProcIds == null) {
          storeTracker.insert(procId);
        } else if (procId == Procedure.NO_PROC_ID) {
          storeTracker.insert(subProcIds);
        } else {
          storeTracker.insert(procId, subProcIds);
        }
        break;
      case UPDATE:
        storeTracker.update(procId);
        break;
      case DELETE:
        if (subProcIds != null && subProcIds.length > 0) {
          storeTracker.delete(subProcIds);
        } else {
          storeTracker.delete(procId);
        }
        break;
      default:
        throw new RuntimeException("invalid push type " + type);
    }
  }

  private boolean isSyncAborted() {
    return syncException.get() != null;
  }

  private void syncLoop() throws Throwable {
    long totalSyncedToStore = 0;
    inSync.set(false);
    lock.lock();
    try {
      while (isRunning()) {
        try {
          // Wait until new data is available
          if (slotIndex == 0) {
            if (!loading.get()) {
              periodicRoll();
            }

            if (LOG.isTraceEnabled()) {
              float rollTsSec = getMillisFromLastRoll() / 1000.0f;
              LOG.trace(String.format("Waiting for data. flushed=%s (%s/sec)",
                        StringUtils.humanSize(totalSynced.get()),
                        StringUtils.humanSize(totalSynced.get() / rollTsSec)));
            }

            waitCond.await(getMillisToNextPeriodicRoll(), TimeUnit.MILLISECONDS);
            if (slotIndex == 0) {
              // no data.. probably a stop() or a periodic roll
              continue;
            }
          }
          // Wait SYNC_WAIT_MSEC or the signal of "slots full" before flushing
          final long syncWaitSt = System.currentTimeMillis();
          if (slotIndex != slots.length) {
            slotCond.await(syncWaitMsec, TimeUnit.MILLISECONDS);
          }

          final long currentTs = System.currentTimeMillis();
          final long syncWaitMs = currentTs - syncWaitSt;
          final float rollSec = getMillisFromLastRoll() / 1000.0f;
          final float syncedPerSec = totalSyncedToStore / rollSec;
          if (LOG.isTraceEnabled() && (syncWaitMs > 10 || slotIndex < slots.length)) {
            LOG.trace(String.format("Sync wait %s, slotIndex=%s , totalSynced=%s (%s/sec)",
                      StringUtils.humanTimeDiff(syncWaitMs), slotIndex,
                      StringUtils.humanSize(totalSyncedToStore),
                      StringUtils.humanSize(syncedPerSec)));
          }

          // update webui circular buffers (TODO: get rid of allocations)
          final SyncMetrics syncMetrics = new SyncMetrics();
          syncMetrics.timestamp = currentTs;
          syncMetrics.syncWaitMs = syncWaitMs;
          syncMetrics.syncedEntries = slotIndex;
          syncMetrics.totalSyncedBytes = totalSyncedToStore;
          syncMetrics.syncedPerSec = syncedPerSec;
          syncMetricsBuffer.add(syncMetrics);

          // sync
          inSync.set(true);
          long slotSize = syncSlots();
          logs.getLast().addToSize(slotSize);
          totalSyncedToStore = totalSynced.addAndGet(slotSize);
          slotIndex = 0;
          inSync.set(false);
          syncId.incrementAndGet();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          syncException.compareAndSet(null, e);
          sendAbortProcessSignal();
          throw e;
        } catch (Throwable t) {
          syncException.compareAndSet(null, t);
          sendAbortProcessSignal();
          throw t;
        } finally {
          syncCond.signalAll();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public ArrayList<SyncMetrics> getSyncMetrics() {
    lock.lock();
    try {
      return new ArrayList<SyncMetrics>(syncMetricsBuffer);
    } finally {
      lock.unlock();
    }
  }

  private long syncSlots() throws Throwable {
    int retry = 0;
    int logRolled = 0;
    long totalSynced = 0;
    do {
      try {
        totalSynced = syncSlots(stream, slots, 0, slotIndex);
        break;
      } catch (Throwable e) {
        LOG.warn("unable to sync slots, retry=" + retry);
        if (++retry >= maxRetriesBeforeRoll) {
          if (logRolled >= maxSyncFailureRoll && isRunning()) {
            LOG.error("Sync slots after log roll failed, abort.", e);
            throw e;
          }

          if (!rollWriterWithRetries()) {
            throw e;
          }

          logRolled++;
          retry = 0;
        }
      }
    } while (isRunning());
    return totalSynced;
  }

  protected long syncSlots(FSDataOutputStream stream, ByteSlot[] slots, int offset, int count)
      throws IOException {
    long totalSynced = 0;
    for (int i = 0; i < count; ++i) {
      ByteSlot data = slots[offset + i];
      data.writeTo(stream);
      totalSynced += data.size();
    }

    if (useHsync) {
      stream.hsync();
    } else {
      stream.hflush();
    }
    sendPostSyncSignal();

    if (LOG.isTraceEnabled()) {
      LOG.trace("Sync slots=" + count + '/' + slots.length +
                ", flushed=" + StringUtils.humanSize(totalSynced));
    }
    return totalSynced;
  }

  private boolean rollWriterWithRetries() {
    for (int i = 0; i < rollRetries && isRunning(); ++i) {
      if (i > 0) Threads.sleepWithoutInterrupt(waitBeforeRoll * i);

      try {
        if (rollWriter()) {
          return true;
        }
      } catch (IOException e) {
        LOG.warn("Unable to roll the log, attempt=" + (i + 1), e);
      }
    }
    LOG.fatal("Unable to roll the log");
    return false;
  }

  private boolean tryRollWriter() {
    try {
      return rollWriter();
    } catch (IOException e) {
      LOG.warn("Unable to roll the log", e);
      return false;
    }
  }

  public long getMillisToNextPeriodicRoll() {
    if (lastRollTs.get() > 0 && periodicRollMsec > 0) {
      return periodicRollMsec - getMillisFromLastRoll();
    }
    return Long.MAX_VALUE;
  }

  public long getMillisFromLastRoll() {
    return (System.currentTimeMillis() - lastRollTs.get());
  }

  @VisibleForTesting
  protected void periodicRollForTesting() throws IOException {
    lock.lock();
    try {
      periodicRoll();
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  protected boolean rollWriterForTesting() throws IOException {
    lock.lock();
    try {
      return rollWriter();
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  protected void removeInactiveLogsForTesting() throws Exception {
    lock.lock();
    try {
      removeInactiveLogs();
    } finally  {
      lock.unlock();
    }
  }

  private void periodicRoll() throws IOException {
    if (storeTracker.isEmpty()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("no active procedures");
      }
      tryRollWriter();
      removeAllLogs(flushLogId - 1);
    } else {
      if (storeTracker.isUpdated()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("all the active procedures are in the latest log");
        }
        removeAllLogs(flushLogId - 1);
      }

      // if the log size has exceeded the roll threshold
      // or the periodic roll timeout is expired, try to roll the wal.
      if (totalSynced.get() > rollThreshold || getMillisToNextPeriodicRoll() <= 0) {
        tryRollWriter();
      }

      removeInactiveLogs();
    }
  }

  private boolean rollWriter() throws IOException {
    if (!isRunning()) return false;

    // Create new state-log
    if (!rollWriter(flushLogId + 1)) {
      LOG.warn("someone else has already created log " + flushLogId);
      return false;
    }

    // We have the lease on the log,
    // but we should check if someone else has created new files
    if (getMaxLogId(getLogFiles()) > flushLogId) {
      LOG.warn("Someone else created new logs. Expected maxLogId < " + flushLogId);
      logs.getLast().removeFile();
      return false;
    }

    // We have the lease on the log
    return true;
  }

  private boolean rollWriter(final long logId) throws IOException {
    assert logId > flushLogId : "logId=" + logId + " flushLogId=" + flushLogId;
    assert lock.isHeldByCurrentThread() : "expected to be the lock owner. " + lock.isLocked();

    ProcedureWALHeader header = ProcedureWALHeader.newBuilder()
      .setVersion(ProcedureWALFormat.HEADER_VERSION)
      .setType(ProcedureWALFormat.LOG_TYPE_STREAM)
      .setMinProcId(storeTracker.getActiveMinProcId())
      .setLogId(logId)
      .build();

    FSDataOutputStream newStream = null;
    Path newLogFile = null;
    long startPos = -1;
    newLogFile = getLogFilePath(logId);
    try {
      newStream = fs.create(newLogFile, false);
    } catch (FileAlreadyExistsException e) {
      LOG.error("Log file with id=" + logId + " already exists", e);
      return false;
    } catch (RemoteException re) {
      LOG.warn("failed to create log file with id=" + logId, re);
      return false;
    }
    try {
      ProcedureWALFormat.writeHeader(newStream, header);
      startPos = newStream.getPos();
    } catch (IOException ioe) {
      LOG.warn("Encountered exception writing header", ioe);
      newStream.close();
      return false;
    }

    closeCurrentLogStream();

    storeTracker.resetUpdates();
    stream = newStream;
    flushLogId = logId;
    totalSynced.set(0);
    long rollTs = System.currentTimeMillis();
    lastRollTs.set(rollTs);
    logs.add(new ProcedureWALFile(fs, newLogFile, header, startPos, rollTs));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Roll new state log: " + logId);
    }
    return true;
  }

  private void closeCurrentLogStream() {
    if (stream == null) return;
    try {
      ProcedureWALFile log = logs.getLast();
      log.setProcIds(storeTracker.getUpdatedMinProcId(), storeTracker.getUpdatedMaxProcId());
      log.updateLocalTracker(storeTracker);
      long trailerSize = ProcedureWALFormat.writeTrailer(stream, storeTracker);
      log.addToSize(trailerSize);
    } catch (IOException e) {
      LOG.warn("Unable to write the trailer: " + e.getMessage());
    }
    try {
      stream.close();
    } catch (IOException e) {
      LOG.error("Unable to close the stream", e);
    }
    stream = null;
  }

  // ==========================================================================
  //  Log Files cleaner helpers
  // ==========================================================================

  /**
   * Iterates over log files from latest (ignoring currently active one) to oldest, deleting the
   * ones which don't contain anything useful for recovery.
   * @throws IOException
   */
  private void removeInactiveLogs() throws IOException {
    // TODO: can we somehow avoid first iteration (starting from newest log) and still figure out
    // efficient way to cleanup old logs.
    // Alternatively, a complex and maybe more efficient method would be using this iteration to
    // rewrite latest states of active procedures to a new log file and delete all old ones.
    if (logs.size() <= 1) return;
    ProcedureStoreTracker runningTracker = new ProcedureStoreTracker();
    runningTracker.resetTo(storeTracker);
    List<ProcedureWALFile> logsToBeDeleted = new ArrayList<>();
    for (int i = logs.size() - 2; i >= 0; i--) {
      ProcedureWALFile log = logs.get(i);
      // If nothing was subtracted, delete the log file since it doesn't contain any useful proc
      // states.
      if (!runningTracker.subtract(log.getTracker())) {
        logsToBeDeleted.add(log);
      }
    }
    // Delete the logs from oldest to newest and stop at first log that can't be deleted to avoid
    // holes in the log file sequence (for better debug capability).
    while (true) {
      ProcedureWALFile log = logs.getFirst();
      if (logsToBeDeleted.contains(log)) {
        removeLogFile(log);
      } else {
        break;
      }
    }
  }

  /**
   * Remove all logs with logId <= {@code lastLogId}.
   */
  private void removeAllLogs(long lastLogId) {
    if (logs.size() <= 1) return;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Remove all state logs with ID less than " + lastLogId);
    }
    while (logs.size() > 1) {
      ProcedureWALFile log = logs.getFirst();
      if (lastLogId < log.getLogId()) {
        break;
      }
      removeLogFile(log);
    }
  }

  private boolean removeLogFile(final ProcedureWALFile log) {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Removing log=" + log);
      }
      log.removeFile();
      logs.remove(log);
      if (LOG.isDebugEnabled()) {
        LOG.info("Removed log=" + log + " activeLogs=" + logs);
      }
      assert logs.size() > 0 : "expected at least one log";
    } catch (IOException e) {
      LOG.error("Unable to remove log: " + log, e);
      return false;
    }
    return true;
  }

  // ==========================================================================
  //  FileSystem Log Files helpers
  // ==========================================================================
  public Path getLogDir() {
    return this.logDir;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  protected Path getLogFilePath(final long logId) throws IOException {
    return new Path(logDir, String.format("state-%020d.log", logId));
  }

  private static long getLogIdFromName(final String name) {
    int end = name.lastIndexOf(".log");
    int start = name.lastIndexOf('-') + 1;
    return Long.parseLong(name.substring(start, end));
  }

  private static final PathFilter WALS_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return name.startsWith("state-") && name.endsWith(".log");
    }
  };

  private static final Comparator<FileStatus> FILE_STATUS_ID_COMPARATOR =
      new Comparator<FileStatus>() {
    @Override
    public int compare(FileStatus a, FileStatus b) {
      final long aId = getLogIdFromName(a.getPath().getName());
      final long bId = getLogIdFromName(b.getPath().getName());
      return Long.compare(aId, bId);
    }
  };

  private FileStatus[] getLogFiles() throws IOException {
    try {
      FileStatus[] files = fs.listStatus(logDir, WALS_PATH_FILTER);
      Arrays.sort(files, FILE_STATUS_ID_COMPARATOR);
      return files;
    } catch (FileNotFoundException e) {
      LOG.warn("Log directory not found: " + e.getMessage());
      return null;
    }
  }

  private static long getMaxLogId(final FileStatus[] logFiles) {
    long maxLogId = 0;
    if (logFiles != null && logFiles.length > 0) {
      for (int i = 0; i < logFiles.length; ++i) {
        maxLogId = Math.max(maxLogId, getLogIdFromName(logFiles[i].getPath().getName()));
      }
    }
    return maxLogId;
  }

  /**
   * @return Max-LogID of the specified log file set
   */
  private long initOldLogs(final FileStatus[] logFiles) throws IOException {
    this.logs.clear();

    long maxLogId = 0;
    if (logFiles != null && logFiles.length > 0) {
      for (int i = 0; i < logFiles.length; ++i) {
        final Path logPath = logFiles[i].getPath();
        leaseRecovery.recoverFileLease(fs, logPath);
        if (!isRunning()) {
          throw new IOException("wal aborting");
        }

        maxLogId = Math.max(maxLogId, getLogIdFromName(logPath.getName()));
        ProcedureWALFile log = initOldLog(logFiles[i]);
        if (log != null) {
          this.logs.add(log);
        }
      }
      Collections.sort(this.logs);
      initTrackerFromOldLogs();
    }
    return maxLogId;
  }

  /**
   * If last log's tracker is not null, use it as {@link #storeTracker}.
   * Otherwise, set storeTracker as partial, and let {@link ProcedureWALFormatReader} rebuild
   * it using entries in the log.
   */
  private void initTrackerFromOldLogs() {
    if (logs.isEmpty() || !isRunning()) return;
    ProcedureWALFile log = logs.getLast();
    if (!log.getTracker().isPartial()) {
      storeTracker.resetTo(log.getTracker());
    } else {
      storeTracker.reset();
      storeTracker.setPartialFlag(true);
    }
  }

  /**
   * Loads given log file and it's tracker.
   */
  private ProcedureWALFile initOldLog(final FileStatus logFile) throws IOException {
    final ProcedureWALFile log = new ProcedureWALFile(fs, logFile);
    if (logFile.getLen() == 0) {
      LOG.warn("Remove uninitialized log: " + logFile);
      log.removeFile();
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening state-log: " + logFile);
    }
    try {
      log.open();
    } catch (ProcedureWALFormat.InvalidWALDataException e) {
      LOG.warn("Remove uninitialized log: " + logFile, e);
      log.removeFile();
      return null;
    } catch (IOException e) {
      String msg = "Unable to read state log: " + logFile;
      LOG.error(msg, e);
      throw new IOException(msg, e);
    }

    try {
      log.readTracker();
    } catch (IOException e) {
      log.getTracker().reset();
      log.getTracker().setPartialFlag(true);
      LOG.warn("Unable to read tracker for " + log + " - " + e.getMessage());
    }

    log.close();
    return log;
  }
}
