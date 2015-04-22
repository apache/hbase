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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

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
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.hadoop.hbase.procedure2.util.ByteSlot;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALHeader;

/**
 * WAL implementation of the ProcedureStore.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class WALProcedureStore implements ProcedureStore {
  private static final Log LOG = LogFactory.getLog(WALProcedureStore.class);

  public interface LeaseRecovery {
    void recoverFileLease(FileSystem fs, Path path) throws IOException;
  }

  private static final int MAX_RETRIES_BEFORE_ABORT = 3;

  private static final String SYNC_WAIT_MSEC_CONF_KEY = "hbase.procedure.store.wal.sync.wait.msec";
  private static final int DEFAULT_SYNC_WAIT_MSEC = 100;

  private static final String USE_HSYNC_CONF_KEY = "hbase.procedure.store.wal.use.hsync";
  private static final boolean DEFAULT_USE_HSYNC = true;

  private static final String ROLL_THRESHOLD_CONF_KEY = "hbase.procedure.store.wal.roll.threshold";
  private static final long DEFAULT_ROLL_THRESHOLD = 32 * 1024 * 1024; // 32M

  private final CopyOnWriteArrayList<ProcedureStoreListener> listeners =
    new CopyOnWriteArrayList<ProcedureStoreListener>();

  private final LinkedList<ProcedureWALFile> logs = new LinkedList<ProcedureWALFile>();
  private final ProcedureStoreTracker storeTracker = new ProcedureStoreTracker();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition waitCond = lock.newCondition();
  private final Condition slotCond = lock.newCondition();
  private final Condition syncCond = lock.newCondition();

  private final LeaseRecovery leaseRecovery;
  private final Configuration conf;
  private final FileSystem fs;
  private final Path logDir;

  private AtomicBoolean inSync = new AtomicBoolean(false);
  private LinkedTransferQueue<ByteSlot> slotsCache = null;
  private Set<ProcedureWALFile> corruptedLogs = null;
  private AtomicLong totalSynced = new AtomicLong(0);
  private FSDataOutputStream stream = null;
  private long lastRollTs = 0;
  private long flushLogId = 0;
  private int slotIndex = 0;
  private Thread syncThread;
  private ByteSlot[] slots;

  private long rollThreshold;
  private boolean useHsync;
  private int syncWaitMsec;

  public WALProcedureStore(final Configuration conf, final FileSystem fs, final Path logDir,
      final LeaseRecovery leaseRecovery) {
    this.fs = fs;
    this.conf = conf;
    this.logDir = logDir;
    this.leaseRecovery = leaseRecovery;
  }

  @Override
  public void start(int numSlots) throws IOException {
    if (running.getAndSet(true)) {
      return;
    }

    // Init buffer slots
    slots = new ByteSlot[numSlots];
    slotsCache = new LinkedTransferQueue();
    while (slotsCache.size() < numSlots) {
      slotsCache.offer(new ByteSlot());
    }

    // Tunings
    rollThreshold = conf.getLong(ROLL_THRESHOLD_CONF_KEY, DEFAULT_ROLL_THRESHOLD);
    syncWaitMsec = conf.getInt(SYNC_WAIT_MSEC_CONF_KEY, DEFAULT_SYNC_WAIT_MSEC);
    useHsync = conf.getBoolean(USE_HSYNC_CONF_KEY, DEFAULT_USE_HSYNC);

    // Init sync thread
    syncThread = new Thread("WALProcedureStoreSyncThread") {
      @Override
      public void run() {
        while (running.get()) {
          try {
            syncLoop();
          } catch (IOException e) {
            LOG.error("got an exception from the sync-loop", e);
            sendAbortProcessSignal();
          }
        }
      }
    };
    syncThread.start();
  }

  @Override
  public void stop(boolean abort) {
    if (!running.getAndSet(false)) {
      return;
    }

    LOG.info("Stopping the WAL Procedure Store");
    if (lock.tryLock()) {
      try {
        waitCond.signalAll();
      } finally {
        lock.unlock();
      }
    }

    if (!abort) {
      try {
        syncThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // Close the writer
    closeStream();

    // Close the old logs
    // they should be already closed, this is just in case the load fails
    // and we call start() and then stop()
    for (ProcedureWALFile log: logs) {
      log.close();
    }
    logs.clear();
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public int getNumThreads() {
    return slots == null ? 0 : slots.length;
  }

  public ProcedureStoreTracker getStoreTracker() {
    return storeTracker;
  }

  @Override
  public void registerListener(ProcedureStoreListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public boolean unregisterListener(ProcedureStoreListener listener) {
    return this.listeners.remove(listener);
  }

  @Override
  public void recoverLease() throws IOException {
    LOG.info("Starting WAL Procedure Store lease recovery");
    FileStatus[] oldLogs = getLogFiles();
    while (running.get()) {
      // Get Log-MaxID and recover lease on old logs
      flushLogId = initOldLogs(oldLogs) + 1;

      // Create new state-log
      if (!rollWriter(flushLogId)) {
        // someone else has already created this log
        LOG.debug("someone else has already created log " + flushLogId);
        continue;
      }

      // We have the lease on the log
      oldLogs = getLogFiles();
      if (getMaxLogId(oldLogs) > flushLogId) {
        // Someone else created new logs
        LOG.debug("someone else created new logs. expected maxLogId < " + flushLogId);
        logs.getLast().removeFile();
        continue;
      }

      LOG.info("lease acquired flushLogId=" + flushLogId);
      break;
    }
  }

  @Override
  public Iterator<Procedure> load() throws IOException {
    if (logs.isEmpty()) {
      throw new RuntimeException("recoverLease() must be called before loading data");
    }

    // Nothing to do, If we have only the current log.
    if (logs.size() == 1) {
      LOG.debug("No state logs to replay");
      return null;
    }

    // Load the old logs
    final ArrayList<ProcedureWALFile> toRemove = new ArrayList<ProcedureWALFile>();
    Iterator<ProcedureWALFile> it = logs.descendingIterator();
    it.next(); // Skip the current log
    try {
      return ProcedureWALFormat.load(it, storeTracker, new ProcedureWALFormat.Loader() {
        @Override
        public void removeLog(ProcedureWALFile log) {
          toRemove.add(log);
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
      if (!toRemove.isEmpty()) {
        for (ProcedureWALFile log: toRemove) {
          removeLogFile(log);
        }
      }
    }
  }

  @Override
  public void insert(final Procedure proc, final Procedure[] subprocs) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("insert " + proc + " subproc=" + Arrays.toString(subprocs));
    }

    ByteSlot slot = acquireSlot();
    long logId = -1;
    try {
      // Serialize the insert
      if (subprocs != null) {
        ProcedureWALFormat.writeInsert(slot, proc, subprocs);
      } else {
        assert !proc.hasParent();
        ProcedureWALFormat.writeInsert(slot, proc);
      }

      // Push the transaction data and wait until it is persisted
      logId = pushData(slot);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize one of the procedure: proc=" + proc +
                " subprocs=" + Arrays.toString(subprocs), e);
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }

    // Update the store tracker
    synchronized (storeTracker) {
      storeTracker.insert(proc, subprocs);
    }
  }

  @Override
  public void update(final Procedure proc) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("update " + proc);
    }

    ByteSlot slot = acquireSlot();
    long logId = -1;
    try {
      // Serialize the update
      ProcedureWALFormat.writeUpdate(slot, proc);

      // Push the transaction data and wait until it is persisted
      logId = pushData(slot);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize the procedure: " + proc, e);
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }

    // Update the store tracker
    boolean removeOldLogs = false;
    synchronized (storeTracker) {
      storeTracker.update(proc);
      if (logId == flushLogId) {
        removeOldLogs = storeTracker.isUpdated();
      }
    }

    if (removeOldLogs) {
      removeAllLogs(logId - 1);
    }
  }

  @Override
  public void delete(final long procId) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("delete " + procId);
    }

    ByteSlot slot = acquireSlot();
    long logId = -1;
    try {
      // Serialize the delete
      ProcedureWALFormat.writeDelete(slot, procId);

      // Push the transaction data and wait until it is persisted
      logId = pushData(slot);
    } catch (IOException e) {
      // We are not able to serialize the procedure.
      // this is a code error, and we are not able to go on.
      LOG.fatal("Unable to serialize the procedure: " + procId, e);
      throw new RuntimeException(e);
    } finally {
      releaseSlot(slot);
    }

    boolean removeOldLogs = false;
    synchronized (storeTracker) {
      storeTracker.delete(procId);
      if (logId == flushLogId) {
        if (storeTracker.isEmpty() && totalSynced.get() > rollThreshold) {
          removeOldLogs = rollWriterOrDie(logId + 1);
        }
      }
    }

    if (removeOldLogs) {
      removeAllLogs(logId);
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

  private long pushData(final ByteSlot slot) {
    assert isRunning() && !logs.isEmpty() : "recoverLease() must be called before inserting data";
    long logId = -1;

    lock.lock();
    try {
      // Wait for the sync to be completed
      while (true) {
        if (inSync.get()) {
          syncCond.await();
        } else if (slotIndex == slots.length) {
          slotCond.signal();
          syncCond.await();
        } else {
          break;
        }
      }

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

      syncCond.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      sendAbortProcessSignal();
    } finally {
      lock.unlock();
    }
    return logId;
  }

  private void syncLoop() throws IOException {
    inSync.set(false);
    while (running.get()) {
      lock.lock();
      try {
        // Wait until new data is available
        if (slotIndex == 0) {
          if (LOG.isTraceEnabled()) {
            float rollTsSec = (System.currentTimeMillis() - lastRollTs) / 1000.0f;
            LOG.trace(String.format("Waiting for data. flushed=%s (%s/sec)",
                      StringUtils.humanSize(totalSynced.get()),
                      StringUtils.humanSize(totalSynced.get() / rollTsSec)));
          }
          waitCond.await();
          if (slotIndex == 0) {
            // no data.. probably a stop()
            continue;
          }
        }

        // Wait SYNC_WAIT_MSEC or the signal of "slots full" before flushing
        long syncWaitSt = System.currentTimeMillis();
        if (slotIndex != slots.length) {
          slotCond.await(syncWaitMsec, TimeUnit.MILLISECONDS);
        }
        long syncWaitMs = System.currentTimeMillis() - syncWaitSt;
        if (LOG.isTraceEnabled() && (syncWaitMs > 10 || slotIndex < slots.length)) {
          float rollSec = (System.currentTimeMillis() - lastRollTs) / 1000.0f;
          LOG.trace("sync wait " + StringUtils.humanTimeDiff(syncWaitMs) +
                    " slotIndex=" + slotIndex +
                    " totalSynced=" + StringUtils.humanSize(totalSynced.get()) +
                    " " + StringUtils.humanSize(totalSynced.get() / rollSec) + "/sec");
        }


        inSync.set(true);
        totalSynced.addAndGet(syncSlots());
        slotIndex = 0;
        inSync.set(false);
        syncCond.signalAll();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        sendAbortProcessSignal();
      } finally {
        lock.unlock();
      }
    }
  }

  private long syncSlots() {
    int retry = 0;
    long totalSynced = 0;
    do {
      try {
        totalSynced = syncSlots(stream, slots, 0, slotIndex);
        break;
      } catch (Throwable e) {
        if (++retry == MAX_RETRIES_BEFORE_ABORT) {
          LOG.error("sync slot failed, abort.", e);
          sendAbortProcessSignal();
        }
      }
    } while (running.get());
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

    if (LOG.isTraceEnabled()) {
      LOG.trace("Sync slots=" + count + '/' + slots.length +
                " flushed=" + StringUtils.humanSize(totalSynced));
    }
    return totalSynced;
  }

  private void sendAbortProcessSignal() {
    if (!this.listeners.isEmpty()) {
      for (ProcedureStoreListener listener : this.listeners) {
        listener.abortProcess();
      }
    }
  }

  private boolean rollWriterOrDie(final long logId) {
    try {
      return rollWriter(logId);
    } catch (IOException e) {
      LOG.warn("Unable to roll the log", e);
      sendAbortProcessSignal();
      return false;
    }
  }

  private boolean rollWriter(final long logId) throws IOException {
    ProcedureWALHeader header = ProcedureWALHeader.newBuilder()
      .setVersion(ProcedureWALFormat.HEADER_VERSION)
      .setType(ProcedureWALFormat.LOG_TYPE_STREAM)
      .setMinProcId(storeTracker.getMinProcId())
      .setLogId(logId)
      .build();

    FSDataOutputStream newStream = null;
    Path newLogFile = null;
    long startPos = -1;
    try {
      newLogFile = getLogFilePath(logId);
      newStream = fs.create(newLogFile, false);
      ProcedureWALFormat.writeHeader(newStream, header);
      startPos = newStream.getPos();
    } catch (FileAlreadyExistsException e) {
      LOG.error("Log file with id=" + logId + " already exists", e);
      return false;
    }
    lock.lock();
    try {
      closeStream();
      synchronized (storeTracker) {
        storeTracker.resetUpdates();
      }
      stream = newStream;
      flushLogId = logId;
      totalSynced.set(0);
      lastRollTs = System.currentTimeMillis();
      logs.add(new ProcedureWALFile(fs, newLogFile, header, startPos));
    } finally {
      lock.unlock();
    }
    LOG.info("Roll new state log: " + logId);
    return true;
  }

  private void closeStream() {
    try {
      if (stream != null) {
        try {
          ProcedureWALFormat.writeTrailer(stream, storeTracker);
        } catch (IOException e) {
          LOG.warn("Unable to write the trailer: " + e.getMessage());
        }
        stream.close();
      }
    } catch (IOException e) {
      LOG.error("Unable to close the stream", e);
    } finally {
      stream = null;
    }
  }

  private void removeAllLogs(long lastLogId) {
    LOG.info("Remove all state logs with ID less then " + lastLogId);
    while (!logs.isEmpty()) {
      ProcedureWALFile log = logs.getFirst();
      if (lastLogId < log.getLogId()) {
        break;
      }

      removeLogFile(log);
    }
  }

  private boolean removeLogFile(final ProcedureWALFile log) {
    try {
      LOG.debug("remove log: " + log);
      log.removeFile();
      logs.remove(log);
    } catch (IOException e) {
      LOG.error("unable to remove log " + log, e);
      return false;
    }
    return true;
  }

  public Set<ProcedureWALFile> getCorruptedLogs() {
    return corruptedLogs;
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
    while (start < end) {
      if (name.charAt(start) != '0')
        break;
      start++;
    }
    return Long.parseLong(name.substring(start, end));
  }

  private FileStatus[] getLogFiles() throws IOException {
    try {
      return fs.listStatus(logDir, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          String name = path.getName();
          return name.startsWith("state-") && name.endsWith(".log");
        }
      });
    } catch (FileNotFoundException e) {
      LOG.warn("log directory not found: " + e.getMessage());
      return null;
    }
  }

  private long getMaxLogId(final FileStatus[] logFiles) {
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

  private void initTrackerFromOldLogs() {
    // TODO: Load the most recent tracker available
    if (!logs.isEmpty()) {
      ProcedureWALFile log = logs.getLast();
      try {
        log.readTracker(storeTracker);
      } catch (IOException e) {
        LOG.warn("Unable to read tracker for " + log + " - " + e.getMessage());
        // try the next one...
        storeTracker.clear();
        storeTracker.setPartialFlag(true);
      }
    }
  }

  private ProcedureWALFile initOldLog(final FileStatus logFile) throws IOException {
    ProcedureWALFile log = new ProcedureWALFile(fs, logFile);
    if (logFile.getLen() == 0) {
      LOG.warn("Remove uninitialized log " + logFile);
      log.removeFile();
      return null;
    }

    LOG.debug("opening state-log: " + logFile);
    try {
      log.open();
    } catch (ProcedureWALFormat.InvalidWALDataException e) {
      LOG.warn("Remove uninitialized log " + logFile, e);
      log.removeFile();
      return null;
    } catch (IOException e) {
      String msg = "Unable to read state log: " + logFile;
      LOG.error(msg, e);
      throw new IOException(msg, e);
    }

    if (log.isCompacted()) {
      try {
        log.readTrailer();
      } catch (IOException e) {
        // unfinished compacted log throw it away
        LOG.warn("Unfinished compacted log " + logFile, e);
        log.removeFile();
        return null;
      }
    }
    return log;
  }
}