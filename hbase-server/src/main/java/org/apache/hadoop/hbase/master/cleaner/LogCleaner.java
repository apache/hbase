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

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;

/**
 * This Chore, every time it runs, will attempt to delete the WALs in the old logs folder. The WAL
 * is only deleted if none of the cleaner delegates says otherwise.
 * @see BaseLogCleanerDelegate
 */
@InterfaceAudience.Private
public class LogCleaner extends CleanerChore<BaseLogCleanerDelegate> {
  private static final Log LOG = LogFactory.getLog(LogCleaner.class.getName());

  public static final String OLD_WALS_CLEANER_SIZE = "hbase.oldwals.cleaner.thread.size";
  public static final int OLD_WALS_CLEANER_DEFAULT_SIZE = 2;

  private final LinkedBlockingQueue<CleanerContext> pendingDelete;
  private List<Thread> oldWALsCleaner;

  /**
   * @param p the period of time to sleep between each run
   * @param s the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param oldLogDir the path to the archived logs
   */
  public LogCleaner(final int p, final Stoppable s, Configuration conf, FileSystem fs,
      Path oldLogDir) {
    super("LogsCleaner", p, s, conf, fs, oldLogDir, HBASE_MASTER_LOGCLEANER_PLUGINS);
    this.pendingDelete = new LinkedBlockingQueue<>();
    int size = conf.getInt(OLD_WALS_CLEANER_SIZE, OLD_WALS_CLEANER_DEFAULT_SIZE);
    this.oldWALsCleaner = createOldWalsCleaner(size);
  }

  @Override
  protected boolean validate(Path file) {
    return DefaultWALProvider.validateWALFilename(file.getName());
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    super.onConfigurationChange(conf);

    int newSize = conf.getInt(OLD_WALS_CLEANER_SIZE, OLD_WALS_CLEANER_DEFAULT_SIZE);
    if (newSize == oldWALsCleaner.size()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Size from configuration is the same as previous which is " +
            newSize + ", no need to update.");
      }
      return;
    }
    interruptOldWALsCleaner();
    oldWALsCleaner = createOldWalsCleaner(newSize);
  }

  @Override
  protected int deleteFiles(Iterable<FileStatus> filesToDelete) {
    List<CleanerContext> results = new LinkedList<>();
    for (FileStatus toDelete : filesToDelete) {
      CleanerContext context = CleanerContext.createCleanerContext(toDelete);
      if (context != null) {
        pendingDelete.add(context);
        results.add(context);
      }
    }

    int deletedFiles = 0;
    for (CleanerContext res : results) {
      deletedFiles += res.getResult(500) ? 1 : 0;
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

  private List<Thread> createOldWalsCleaner(int size) {
    LOG.info("Creating OldWALs cleaners with size=" + size);

    List<Thread> oldWALsCleaner = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      Thread cleaner = new Thread(new Runnable() {
        @Override
        public void run() {
          deleteFile();
        }
      });
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
    // At most waits 60 seconds
    static final long MAX_WAIT = 60 * 1000;

    final FileStatus target;
    volatile boolean result;
    volatile boolean setFromCleaner = false;

    static CleanerContext createCleanerContext(FileStatus status) {
      return status != null ? new CleanerContext(status) : null;
    }

    private CleanerContext(FileStatus status) {
      this.target = status;
      this.result = false;
    }

    synchronized void setResult(boolean res) {
      this.result = res;
      this.setFromCleaner = true;
      notify();
    }

    synchronized boolean getResult(long waitIfNotFinished) {
      long totalTime = 0;
      try {
        while (!setFromCleaner) {
          wait(waitIfNotFinished);
          totalTime += waitIfNotFinished;
          if (totalTime >= MAX_WAIT) {
            LOG.warn("Spend too much time to delete oldwals " + target);
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
