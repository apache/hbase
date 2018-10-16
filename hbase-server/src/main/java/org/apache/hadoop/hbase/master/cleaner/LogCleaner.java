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
package org.apache.hadoop.hbase.master.cleaner;

import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * This Chore, every time it runs, will attempt to delete the WALs and Procedure WALs in the old
 * logs folder. The WAL is only deleted if none of the cleaner delegates says otherwise.
 * @see BaseLogCleanerDelegate
 */
@InterfaceAudience.Private
public class LogCleaner extends CleanerChore<BaseLogCleanerDelegate> {
  private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class.getName());

  public static final String OLD_WALS_CLEANER_THREAD_SIZE = "hbase.oldwals.cleaner.thread.size";
  public static final int DEFAULT_OLD_WALS_CLEANER_THREAD_SIZE = 2;

  public static final String OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC =
      "hbase.oldwals.cleaner.thread.timeout.msec";
  @VisibleForTesting
  static final long DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC = 60 * 1000L;

  public static final String OLD_WALS_CLEANER_THREAD_CHECK_INTERVAL_MSEC =
      "hbase.oldwals.cleaner.thread.check.interval.msec";
  @VisibleForTesting
  static final long DEFAULT_OLD_WALS_CLEANER_THREAD_CHECK_INTERVAL_MSEC = 500L;


  private final LinkedBlockingQueue<CleanerContext> pendingDelete;
  private List<Thread> oldWALsCleaner;
  private long cleanerThreadTimeoutMsec;
  private long cleanerThreadCheckIntervalMsec;

  /**
   * @param period the period of time to sleep between each run
   * @param stopper the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param oldLogDir the path to the archived logs
   */
  public LogCleaner(final int period, final Stoppable stopper, Configuration conf, FileSystem fs,
      Path oldLogDir) {
    super("LogsCleaner", period, stopper, conf, fs, oldLogDir, HBASE_MASTER_LOGCLEANER_PLUGINS);
    this.pendingDelete = new LinkedBlockingQueue<>();
    int size = conf.getInt(OLD_WALS_CLEANER_THREAD_SIZE, DEFAULT_OLD_WALS_CLEANER_THREAD_SIZE);
    this.oldWALsCleaner = createOldWalsCleaner(size);
    this.cleanerThreadTimeoutMsec = conf.getLong(OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC,
        DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC);
    this.cleanerThreadCheckIntervalMsec = conf.getLong(OLD_WALS_CLEANER_THREAD_CHECK_INTERVAL_MSEC,
        DEFAULT_OLD_WALS_CLEANER_THREAD_CHECK_INTERVAL_MSEC);
  }

  @Override
  protected boolean validate(Path file) {
    return AbstractFSWALProvider.validateWALFilename(file.getName())
        || MasterProcedureUtil.validateProcedureWALFilename(file.getName());
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    super.onConfigurationChange(conf);

    int newSize = conf.getInt(OLD_WALS_CLEANER_THREAD_SIZE, DEFAULT_OLD_WALS_CLEANER_THREAD_SIZE);
    if (newSize == oldWALsCleaner.size()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Size from configuration is the same as previous which is " +
          newSize + ", no need to update.");
      }
      return;
    }
    interruptOldWALsCleaner();
    oldWALsCleaner = createOldWalsCleaner(newSize);
    cleanerThreadTimeoutMsec = conf.getLong(OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC,
        DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC);
    cleanerThreadCheckIntervalMsec = conf.getLong(OLD_WALS_CLEANER_THREAD_CHECK_INTERVAL_MSEC,
        DEFAULT_OLD_WALS_CLEANER_THREAD_CHECK_INTERVAL_MSEC);
  }

  @Override
  protected int deleteFiles(Iterable<FileStatus> filesToDelete) {
    List<CleanerContext> results = new LinkedList<>();
    for (FileStatus toDelete : filesToDelete) {
      CleanerContext context = CleanerContext.createCleanerContext(toDelete,
          cleanerThreadTimeoutMsec);
      if (context != null) {
        pendingDelete.add(context);
        results.add(context);
      }
    }

    int deletedFiles = 0;
    for (CleanerContext res : results) {
      deletedFiles += res.getResult(cleanerThreadCheckIntervalMsec) ? 1 : 0;
    }
    return deletedFiles;
  }

  @Override
  public synchronized void cleanup() {
    super.cleanup();
    interruptOldWALsCleaner();
  }

  @VisibleForTesting
  int getSizeOfCleaners() {
    return oldWALsCleaner.size();
  }

  @VisibleForTesting
  long getCleanerThreadTimeoutMsec() {
    return cleanerThreadTimeoutMsec;
  }

  @VisibleForTesting
  long getCleanerThreadCheckIntervalMsec() {
    return cleanerThreadCheckIntervalMsec;
  }

  private List<Thread> createOldWalsCleaner(int size) {
    LOG.info("Creating OldWALs cleaners with size=" + size);

    List<Thread> oldWALsCleaner = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      Thread cleaner = new Thread(() -> deleteFile());
      cleaner.setName("OldWALsCleaner-" + i);
      cleaner.setDaemon(true);
      cleaner.start();
      oldWALsCleaner.add(cleaner);
    }
    return oldWALsCleaner;
  }

  private void interruptOldWALsCleaner() {
    for (Thread cleaner : oldWALsCleaner) {
      cleaner.interrupt();
    }
    oldWALsCleaner.clear();
  }

  private void deleteFile() {
    while (true) {
      CleanerContext context = null;
      boolean succeed = false;
      boolean interrupted = false;
      try {
        context = pendingDelete.take();
        if (context != null) {
          FileStatus toClean = context.getTargetToClean();
          succeed = this.fs.delete(toClean.getPath(), false);
        }
      } catch (InterruptedException ite) {
        // It's most likely from configuration changing request
        if (context != null) {
          LOG.warn("Interrupted while cleaning oldWALs " +
              context.getTargetToClean() + ", try to clean it next round.");
        }
        interrupted = true;
      } catch (IOException e) {
        // fs.delete() fails.
        LOG.warn("Failed to clean oldwals with exception: " + e);
        succeed = false;
      } finally {
        if (context != null) {
          context.setResult(succeed);
        }
        if (interrupted) {
          // Restore interrupt status
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Exiting cleaner.");
    }
  }

  @Override
  public synchronized void cancel(boolean mayInterruptIfRunning) {
    super.cancel(mayInterruptIfRunning);
    for (Thread t : oldWALsCleaner) {
      t.interrupt();
    }
  }

  private static final class CleanerContext {

    final FileStatus target;
    volatile boolean result;
    volatile boolean setFromCleaner = false;
    long timeoutMsec;

    static CleanerContext createCleanerContext(FileStatus status, long timeoutMsec) {
      return status != null ? new CleanerContext(status, timeoutMsec) : null;
    }

    private CleanerContext(FileStatus status, long timeoutMsec) {
      this.target = status;
      this.result = false;
      this.timeoutMsec = timeoutMsec;
    }

    synchronized void setResult(boolean res) {
      this.result = res;
      this.setFromCleaner = true;
      notify();
    }

    synchronized boolean getResult(long waitIfNotFinished) {
      long totalTimeMsec = 0;
      try {
        while (!setFromCleaner) {
          long startTimeNanos = System.nanoTime();
          wait(waitIfNotFinished);
          totalTimeMsec += TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTimeNanos,
              TimeUnit.NANOSECONDS);
          if (totalTimeMsec >= timeoutMsec) {
            LOG.warn("Spend too much time " + totalTimeMsec + " ms to delete oldwals " + target);
            return result;
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting deletion of " + target);
        return result;
      }
      return result;
    }

    FileStatus getTargetToClean() {
      return target;
    }
  }
}
