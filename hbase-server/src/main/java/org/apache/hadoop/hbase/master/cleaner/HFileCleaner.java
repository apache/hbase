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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;

/**
 * This Chore, every time it runs, will clear the HFiles in the hfile archive
 * folder that are deletable for each HFile cleaner in the chain.
 */
@InterfaceAudience.Private
public class HFileCleaner extends CleanerChore<BaseHFileCleanerDelegate> implements
    ConfigurationObserver {

  public static final String MASTER_HFILE_CLEANER_PLUGINS = "hbase.master.hfilecleaner.plugins";

  public HFileCleaner(final int period, final Stoppable stopper, Configuration conf, FileSystem fs,
      Path directory) {
    this(period, stopper, conf, fs, directory, null);
  }

  // Configuration key for large/small throttle point
  public final static String HFILE_DELETE_THROTTLE_THRESHOLD =
      "hbase.regionserver.thread.hfilecleaner.throttle";
  public final static int DEFAULT_HFILE_DELETE_THROTTLE_THRESHOLD = 64 * 1024 * 1024;// 64M

  // Configuration key for large queue size
  public final static String LARGE_HFILE_DELETE_QUEUE_SIZE =
      "hbase.regionserver.hfilecleaner.large.queue.size";
  public final static int DEFAULT_LARGE_HFILE_DELETE_QUEUE_SIZE = 1048576;

  // Configuration key for small queue size
  public final static String SMALL_HFILE_DELETE_QUEUE_SIZE =
      "hbase.regionserver.hfilecleaner.small.queue.size";
  public final static int DEFAULT_SMALL_HFILE_DELETE_QUEUE_SIZE = 1048576;

  private static final Log LOG = LogFactory.getLog(HFileCleaner.class);

  BlockingQueue<HFileDeleteTask> largeFileQueue;
  BlockingQueue<HFileDeleteTask> smallFileQueue;
  private int throttlePoint;
  private int largeQueueSize;
  private int smallQueueSize;
  private List<Thread> threads = new ArrayList<Thread>();
  private boolean running;

  private long deletedLargeFiles = 0L;
  private long deletedSmallFiles = 0L;

  /**
   * @param period the period of time to sleep between each run
   * @param stopper the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param directory directory to be cleaned
   * @param params params could be used in subclass of BaseHFileCleanerDelegate
   */
  public HFileCleaner(final int period, final Stoppable stopper, Configuration conf, FileSystem fs,
                      Path directory, Map<String, Object> params) {
    super("HFileCleaner", period, stopper, conf, fs,
      directory, MASTER_HFILE_CLEANER_PLUGINS, params);
    throttlePoint =
        conf.getInt(HFILE_DELETE_THROTTLE_THRESHOLD, DEFAULT_HFILE_DELETE_THROTTLE_THRESHOLD);
    largeQueueSize =
        conf.getInt(LARGE_HFILE_DELETE_QUEUE_SIZE, DEFAULT_LARGE_HFILE_DELETE_QUEUE_SIZE);
    smallQueueSize =
        conf.getInt(SMALL_HFILE_DELETE_QUEUE_SIZE, DEFAULT_SMALL_HFILE_DELETE_QUEUE_SIZE);
    largeFileQueue = new LinkedBlockingQueue<HFileCleaner.HFileDeleteTask>(largeQueueSize);
    smallFileQueue = new LinkedBlockingQueue<HFileCleaner.HFileDeleteTask>(smallQueueSize);
    startHFileDeleteThreads();
  }

  @Override
  protected boolean validate(Path file) {
    if (HFileLink.isBackReferencesDir(file) || HFileLink.isBackReferencesDir(file.getParent())) {
      return true;
    }
    return StoreFileInfo.validateStoreFileName(file.getName());
  }

  /**
   * Exposed for TESTING!
   */
  public List<BaseHFileCleanerDelegate> getDelegatesForTesting() {
    return this.cleanersChain;
  }

  @Override
  public int deleteFiles(Iterable<FileStatus> filesToDelete) {
    int deletedFiles = 0;
    List<HFileDeleteTask> tasks = new ArrayList<HFileDeleteTask>();
    // construct delete tasks and add into relative queue
    for (FileStatus file : filesToDelete) {
      HFileDeleteTask task = deleteFile(file);
      if (task != null) {
        tasks.add(task);
      }
    }
    // wait for each submitted task to finish
    for (HFileDeleteTask task : tasks) {
      if (task.getResult()) {
        deletedFiles++;
      }
    }
    return deletedFiles;
  }

  /**
   * Construct an {@link HFileDeleteTask} for each file to delete and add into the correct queue
   * @param file the file to delete
   * @return HFileDeleteTask to track progress
   */
  private HFileDeleteTask deleteFile(FileStatus file) {
    HFileDeleteTask task = new HFileDeleteTask(file);
    boolean enqueued = dispatch(task);
    return enqueued ? task : null;
  }

  private boolean dispatch(HFileDeleteTask task) {
    if (task.fileLength >= this.throttlePoint) {
      if (!this.largeFileQueue.offer(task)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Large file deletion queue is full");
        }
        return false;
      }
    } else {
      if (!this.smallFileQueue.offer(task)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Small file deletion queue is full");
        }
        return false;
      }
    }
    return true;
  }

  @Override
  public void cleanup() {
    super.cleanup();
    stopHFileDeleteThreads();
  }

  /**
   * Start threads for hfile deletion
   */
  private void startHFileDeleteThreads() {
    final String n = Thread.currentThread().getName();
    running = true;
    // start thread for large file deletion
    Thread large = new Thread() {
      @Override
      public void run() {
        consumerLoop(largeFileQueue);
      }
    };
    large.setDaemon(true);
    large.setName(n + "-HFileCleaner.large-" + System.currentTimeMillis());
    large.start();
    LOG.debug("Starting hfile cleaner for large files: " + large.getName());
    threads.add(large);

    // start thread for small file deletion
    Thread small = new Thread() {
      @Override
      public void run() {
        consumerLoop(smallFileQueue);
      }
    };
    small.setDaemon(true);
    small.setName(n + "-HFileCleaner.small-" + System.currentTimeMillis());
    small.start();
    LOG.debug("Starting hfile cleaner for small files: " + small.getName());
    threads.add(small);
  }

  protected void consumerLoop(BlockingQueue<HFileDeleteTask> queue) {
    try {
      while (running) {
        HFileDeleteTask task = null;
        try {
          task = queue.take();
        } catch (InterruptedException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Interrupted while trying to take a task from queue", e);
          }
          break;
        }
        if (task != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Removing: " + task.filePath + " from archive");
          }
          boolean succeed;
          try {
            succeed = this.fs.delete(task.filePath, false);
          } catch (IOException e) {
            LOG.warn("Failed to delete file " + task.filePath, e);
            succeed = false;
          }
          task.setResult(succeed);
          if (succeed) {
            countDeletedFiles(queue == largeFileQueue);
          }
        }
      }
    } finally {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exit thread: " + Thread.currentThread());
      }
    }
  }

  // Currently only for testing purpose
  private void countDeletedFiles(boolean isLarge) {
    if (isLarge) {
      if (deletedLargeFiles == Long.MAX_VALUE) {
        LOG.info("Deleted more than Long.MAX_VALUE large files, reset counter to 0");
        deletedLargeFiles = 0L;
      }
      deletedLargeFiles++;
    } else {
      if (deletedSmallFiles == Long.MAX_VALUE) {
        LOG.info("Deleted more than Long.MAX_VALUE small files, reset counter to 0");
        deletedSmallFiles = 0L;
      }
      deletedSmallFiles++;
    }
  }

  /**
   * Stop threads for hfile deletion
   */
  private void stopHFileDeleteThreads() {
    running = false;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping file delete threads");
    }
    for(Thread thread: threads){
      thread.interrupt();
    }
  }

  static class HFileDeleteTask {
    private static final long MAX_WAIT = 60 * 1000L;
    private static final long WAIT_UNIT = 1000L;

    boolean done = false;
    boolean result;
    final Path filePath;
    final long fileLength;

    public HFileDeleteTask(FileStatus file) {
      this.filePath = file.getPath();
      this.fileLength = file.getLen();
    }

    public synchronized void setResult(boolean result) {
      this.done = true;
      this.result = result;
      notify();
    }

    public synchronized boolean getResult() {
      long waitTime = 0;
      try {
        while (!done) {
          wait(WAIT_UNIT);
          waitTime += WAIT_UNIT;
          if (done) {
            return this.result;
          }
          if (waitTime > MAX_WAIT) {
            LOG.warn("Wait more than " + MAX_WAIT + " ms for deleting " + this.filePath
                + ", exit...");
            return false;
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for result of deleting " + filePath
            + ", will return false", e);
        return false;
      }
      return this.result;
    }
  }

  @VisibleForTesting
  public List<Thread> getCleanerThreads() {
    return threads;
  }

  @VisibleForTesting
  public long getNumOfDeletedLargeFiles() {
    return deletedLargeFiles;
  }

  @VisibleForTesting
  public long getNumOfDeletedSmallFiles() {
    return deletedSmallFiles;
  }

  @VisibleForTesting
  public long getLargeQueueSize() {
    return largeQueueSize;
  }

  @VisibleForTesting
  public long getSmallQueueSize() {
    return smallQueueSize;
  }

  @VisibleForTesting
  public long getThrottlePoint() {
    return throttlePoint;
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    StringBuilder builder = new StringBuilder();
    builder.append("Updating configuration for HFileCleaner, previous throttle point: ")
        .append(throttlePoint).append(", largeQueueSize: ").append(largeQueueSize)
        .append(", smallQueueSize: ").append(smallQueueSize);
    stopHFileDeleteThreads();
    this.throttlePoint =
        conf.getInt(HFILE_DELETE_THROTTLE_THRESHOLD, DEFAULT_HFILE_DELETE_THROTTLE_THRESHOLD);
    this.largeQueueSize =
        conf.getInt(LARGE_HFILE_DELETE_QUEUE_SIZE, DEFAULT_LARGE_HFILE_DELETE_QUEUE_SIZE);
    this.smallQueueSize =
        conf.getInt(SMALL_HFILE_DELETE_QUEUE_SIZE, DEFAULT_SMALL_HFILE_DELETE_QUEUE_SIZE);
    // record the left over tasks
    List<HFileDeleteTask> leftOverTasks = new ArrayList<>();
    for (HFileDeleteTask task : largeFileQueue) {
      leftOverTasks.add(task);
    }
    for (HFileDeleteTask task : smallFileQueue) {
      leftOverTasks.add(task);
    }
    largeFileQueue = new LinkedBlockingQueue<HFileCleaner.HFileDeleteTask>(largeQueueSize);
    smallFileQueue = new LinkedBlockingQueue<HFileCleaner.HFileDeleteTask>(smallQueueSize);
    threads.clear();
    builder.append("; new throttle point: ").append(throttlePoint).append(", largeQueueSize: ")
        .append(largeQueueSize).append(", smallQueueSize: ").append(smallQueueSize);
    LOG.debug(builder.toString());
    startHFileDeleteThreads();
    // re-dispatch the left over tasks
    for (HFileDeleteTask task : leftOverTasks) {
      dispatch(task);
    }
  }
}
