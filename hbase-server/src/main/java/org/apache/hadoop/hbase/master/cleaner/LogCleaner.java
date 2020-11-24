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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * This Chore, every time it runs, will attempt to delete the WALs and Procedure WALs in the old
 * logs folder. The WAL is only deleted if none of the cleaner delegates says otherwise.
 * @see BaseLogCleanerDelegate
 */
@InterfaceAudience.Private
public class LogCleaner extends CleanerChore<BaseLogCleanerDelegate>
  implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);

  public static final String OLD_WALS_CLEANER_THREAD_SIZE = "hbase.oldwals.cleaner.thread.size";
  public static final int DEFAULT_OLD_WALS_CLEANER_THREAD_SIZE = 2;

  public static final String OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC =
      "hbase.oldwals.cleaner.thread.timeout.msec";
  static final long DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC = 60 * 1000L;

  private final LinkedBlockingQueue<CleanerContext> pendingDelete;
  private List<Thread> oldWALsCleaner;
  private long cleanerThreadTimeoutMsec;

  /**
   * @param period the period of time to sleep between each run
   * @param stopper the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param oldLogDir the path to the archived logs
   * @param pool the thread pool used to scan directories
   */
  public LogCleaner(final int period, final Stoppable stopper, Configuration conf, FileSystem fs,
    Path oldLogDir, DirScanPool pool) {
    super("LogsCleaner", period, stopper, conf, fs, oldLogDir, HBASE_MASTER_LOGCLEANER_PLUGINS,
      pool);
    this.pendingDelete = new LinkedBlockingQueue<>();
    int size = conf.getInt(OLD_WALS_CLEANER_THREAD_SIZE, DEFAULT_OLD_WALS_CLEANER_THREAD_SIZE);
    this.oldWALsCleaner = createOldWalsCleaner(size);
    this.cleanerThreadTimeoutMsec = conf.getLong(OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC,
      DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC);
  }

  @Override
  protected boolean validate(Path file) {
    return AbstractFSWALProvider.validateWALFilename(file.getName()) ||
      MasterProcedureUtil.validateProcedureWALFilename(file.getName()) ||
      file.getName().endsWith(MasterRegionFactory.ARCHIVED_WAL_SUFFIX);
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    int newSize = conf.getInt(OLD_WALS_CLEANER_THREAD_SIZE, DEFAULT_OLD_WALS_CLEANER_THREAD_SIZE);
    if (newSize == oldWALsCleaner.size()) {
      LOG.debug("Size from configuration is the same as previous which "
          + "is {}, no need to update.", newSize);
      return;
    }
    interruptOldWALsCleaner();
    oldWALsCleaner = createOldWalsCleaner(newSize);
    cleanerThreadTimeoutMsec = conf.getLong(OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC,
        DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC);
  }

  @Override
  protected int deleteFiles(Iterable<FileStatus> filesToDelete) {
    List<CleanerContext> results = new ArrayList<>();
    for (FileStatus file : filesToDelete) {
      LOG.trace("Scheduling file {} for deletion", file);
      if (file != null) {
        results.add(new CleanerContext(file));
      }
    }
    if (results.isEmpty()) {
      return 0;
    }

    LOG.debug("Old WALs for delete: {}",
      results.stream().map(cc -> cc.target.getPath().getName()).
        collect(Collectors.joining(", ")));
    pendingDelete.addAll(results);

    int deletedFiles = 0;
    for (CleanerContext res : results) {
      LOG.trace("Awaiting the results for deletion of old WAL file: {}", res);
      deletedFiles += res.getResult(this.cleanerThreadTimeoutMsec) ? 1 : 0;
    }
    return deletedFiles;
  }

  @Override
  public synchronized void cleanup() {
    super.cleanup();
    interruptOldWALsCleaner();
  }

  int getSizeOfCleaners() {
    return oldWALsCleaner.size();
  }

  long getCleanerThreadTimeoutMsec() {
    return cleanerThreadTimeoutMsec;
  }

  private List<Thread> createOldWalsCleaner(int size) {
    LOG.info("Creating {} old WALs cleaner threads", size);

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
      LOG.trace("Interrupting thread: {}", cleaner);
      cleaner.interrupt();
    }
    oldWALsCleaner.clear();
  }

  private void deleteFile() {
    while (true) {
      try {
        final CleanerContext context = pendingDelete.take();
        Preconditions.checkNotNull(context);
        FileStatus oldWalFile = context.getTargetToClean();
        try {
          LOG.debug("Deleting {}", oldWalFile);
          boolean succeed = this.fs.delete(oldWalFile.getPath(), false);
          context.setResult(succeed);
        } catch (IOException e) {
          // fs.delete() fails.
          LOG.warn("Failed to delete old WAL file", e);
          context.setResult(false);
        }
      } catch (InterruptedException ite) {
        // It is most likely from configuration changing request
        LOG.warn("Interrupted while cleaning old WALs, will "
            + "try to clean it next round. Exiting.");
        // Restore interrupt status
        Thread.currentThread().interrupt();
        return;
      }
      LOG.trace("Exiting");
    }
  }

  @Override
  public synchronized void cancel(boolean mayInterruptIfRunning) {
    LOG.debug("Cancelling LogCleaner");
    super.cancel(mayInterruptIfRunning);
    interruptOldWALsCleaner();
  }

  private static final class CleanerContext {

    final FileStatus target;
    final AtomicBoolean result;
    final CountDownLatch remainingResults;

    private CleanerContext(FileStatus status) {
      this.target = status;
      this.result = new AtomicBoolean(false);
      this.remainingResults = new CountDownLatch(1);
    }

    void setResult(boolean res) {
      this.result.set(res);
      this.remainingResults.countDown();
    }

    boolean getResult(long waitIfNotFinished) {
      try {
        boolean completed = this.remainingResults.await(waitIfNotFinished,
            TimeUnit.MILLISECONDS);
        if (!completed) {
          LOG.warn("Spent too much time [{}ms] deleting old WAL file: {}",
              waitIfNotFinished, target);
          return false;
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while awaiting deletion of WAL file: {}", target);
        return false;
      }
      return result.get();
    }

    FileStatus getTargetToClean() {
      return target;
    }

    @Override
    public String toString() {
      return "CleanerContext [target=" + target + ", result=" + result + "]";
    }
  }
}
