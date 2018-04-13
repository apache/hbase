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

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Predicate;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Abstract Cleaner that uses a chain of delegates to clean a directory of files
 * @param <T> Cleaner delegate class that is dynamically loaded from configuration
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
    justification="Static pool will be only updated once.")
@InterfaceAudience.Private
public abstract class CleanerChore<T extends FileCleanerDelegate> extends ScheduledChore
    implements ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(CleanerChore.class);
  private static final int AVAIL_PROCESSORS = Runtime.getRuntime().availableProcessors();

  /**
   * If it is an integer and >= 1, it would be the size;
   * if 0.0 < size <= 1.0, size would be available processors * size.
   * Pay attention that 1.0 is different from 1, former indicates it will use 100% of cores,
   * while latter will use only 1 thread for chore to scan dir.
   */
  public static final String CHORE_POOL_SIZE = "hbase.cleaner.scan.dir.concurrent.size";
  private static final String DEFAULT_CHORE_POOL_SIZE = "0.25";

  private static class DirScanPool {
    int size;
    ForkJoinPool pool;
    int cleanerLatch;
    AtomicBoolean reconfigNotification;

    DirScanPool(Configuration conf) {
      String poolSize = conf.get(CHORE_POOL_SIZE, DEFAULT_CHORE_POOL_SIZE);
      size = calculatePoolSize(poolSize);
      // poolSize may be 0 or 0.0 from a careless configuration,
      // double check to make sure.
      size = size == 0 ? calculatePoolSize(DEFAULT_CHORE_POOL_SIZE) : size;
      pool = new ForkJoinPool(size);
      LOG.info("Cleaner pool size is {}", size);
      reconfigNotification = new AtomicBoolean(false);
      cleanerLatch = 0;
    }

    /**
     * Checks if pool can be updated. If so, mark for update later.
     * @param conf configuration
     */
    synchronized void markUpdate(Configuration conf) {
      int newSize = calculatePoolSize(conf.get(CHORE_POOL_SIZE, DEFAULT_CHORE_POOL_SIZE));
      if (newSize == size) {
        LOG.trace("Size from configuration is same as previous={}, no need to update.", newSize);
        return;
      }
      size = newSize;
      // Chore is working, update it later.
      reconfigNotification.set(true);
    }

    /**
     * Update pool with new size.
     */
    synchronized void updatePool(long timeout) {
      long stopTime = System.currentTimeMillis() + timeout;
      while (cleanerLatch != 0 && timeout > 0) {
        try {
          wait(timeout);
          timeout = stopTime - System.currentTimeMillis();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      pool.shutdownNow();
      LOG.info("Update chore's pool size from {} to {}", pool.getParallelism(), size);
      pool = new ForkJoinPool(size);
    }

    synchronized void latchCountUp() {
      cleanerLatch++;
    }

    synchronized void latchCountDown() {
      cleanerLatch--;
      notifyAll();
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    synchronized void submit(ForkJoinTask task) {
      pool.submit(task);
    }
  }
  // It may be waste resources for each cleaner chore own its pool,
  // so let's make pool for all cleaner chores.
  private static volatile DirScanPool POOL;

  protected final FileSystem fs;
  private final Path oldFileDir;
  private final Configuration conf;
  protected final Map<String, Object> params;
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  protected List<T> cleanersChain;

  public static void initChorePool(Configuration conf) {
    if (POOL == null) {
      POOL = new DirScanPool(conf);
    }
  }

  public CleanerChore(String name, final int sleepPeriod, final Stoppable s, Configuration conf,
                      FileSystem fs, Path oldFileDir, String confKey) {
    this(name, sleepPeriod, s, conf, fs, oldFileDir, confKey, null);
  }

  /**
   * @param name name of the chore being run
   * @param sleepPeriod the period of time to sleep between each run
   * @param s the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param oldFileDir the path to the archived files
   * @param confKey configuration key for the classes to instantiate
   * @param params members could be used in cleaner
   */
  public CleanerChore(String name, final int sleepPeriod, final Stoppable s, Configuration conf,
      FileSystem fs, Path oldFileDir, String confKey, Map<String, Object> params) {
    super(name, s, sleepPeriod);

    Preconditions.checkNotNull(POOL, "Chore's pool isn't initialized, please call"
      + "CleanerChore.initChorePool(Configuration) before new a cleaner chore.");
    this.fs = fs;
    this.oldFileDir = oldFileDir;
    this.conf = conf;
    this.params = params;
    initCleanerChain(confKey);
  }

  /**
   * Calculate size for cleaner pool.
   * @param poolSize size from configuration
   * @return size of pool after calculation
   */
  static int calculatePoolSize(String poolSize) {
    if (poolSize.matches("[1-9][0-9]*")) {
      // If poolSize is an integer, return it directly,
      // but upmost to the number of available processors.
      int size = Math.min(Integer.parseInt(poolSize), AVAIL_PROCESSORS);
      if (size == AVAIL_PROCESSORS) {
        LOG.warn("Use full core processors to scan dir, size={}", size);
      }
      return size;
    } else if (poolSize.matches("0.[0-9]+|1.0")) {
      // if poolSize is a double, return poolSize * availableProcessors;
      // Ensure that we always return at least one.
      int computedThreads = (int) (AVAIL_PROCESSORS * Double.valueOf(poolSize));
      if (computedThreads < 1) {
        LOG.debug("Computed {} threads for CleanerChore, using 1 instead", computedThreads);
        return 1;
      }
      return computedThreads;
    } else {
      LOG.error("Unrecognized value: " + poolSize + " for " + CHORE_POOL_SIZE +
          ", use default config: " + DEFAULT_CHORE_POOL_SIZE + " instead.");
      return calculatePoolSize(DEFAULT_CHORE_POOL_SIZE);
    }
  }

  /**
   * Validate the file to see if it even belongs in the directory. If it is valid, then the file
   * will go through the cleaner delegates, but otherwise the file is just deleted.
   * @param file full {@link Path} of the file to be checked
   * @return <tt>true</tt> if the file is valid, <tt>false</tt> otherwise
   */
  protected abstract boolean validate(Path file);

  /**
   * Instantiate and initialize all the file cleaners set in the configuration
   * @param confKey key to get the file cleaner classes from the configuration
   */
  private void initCleanerChain(String confKey) {
    this.cleanersChain = new LinkedList<>();
    String[] logCleaners = conf.getStrings(confKey);
    if (logCleaners != null) {
      for (String className : logCleaners) {
        T logCleaner = newFileCleaner(className, conf);
        if (logCleaner != null) {
          LOG.debug("Initialize cleaner={}", className);
          this.cleanersChain.add(logCleaner);
        }
      }
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    POOL.markUpdate(conf);
  }

  /**
   * A utility method to create new instances of LogCleanerDelegate based on the class name of the
   * LogCleanerDelegate.
   * @param className fully qualified class name of the LogCleanerDelegate
   * @param conf used configuration
   * @return the new instance
   */
  private T newFileCleaner(String className, Configuration conf) {
    try {
      Class<? extends FileCleanerDelegate> c = Class.forName(className).asSubclass(
        FileCleanerDelegate.class);
      @SuppressWarnings("unchecked")
      T cleaner = (T) c.getDeclaredConstructor().newInstance();
      cleaner.setConf(conf);
      cleaner.init(this.params);
      return cleaner;
    } catch (Exception e) {
      LOG.warn("Can NOT create CleanerDelegate={}", className, e);
      // skipping if can't instantiate
      return null;
    }
  }

  @Override
  protected void chore() {
    if (getEnabled()) {
      try {
        POOL.latchCountUp();
        if (runCleaner()) {
          LOG.debug("Cleaned all WALs under {}", oldFileDir);
        } else {
          LOG.warn("WALs outstanding under {}", oldFileDir);
        }
      } finally {
        POOL.latchCountDown();
      }
      // After each cleaner chore, checks if received reconfigure notification while cleaning.
      // First in cleaner turns off notification, to avoid another cleaner updating pool again.
      if (POOL.reconfigNotification.compareAndSet(true, false)) {
        // This cleaner is waiting for other cleaners finishing their jobs.
        // To avoid missing next chore, only wait 0.8 * period, then shutdown.
        POOL.updatePool((long) (0.8 * getTimeUnit().toMillis(getPeriod())));
      }
    } else {
      LOG.debug("Cleaner chore disabled! Not cleaning.");
    }
  }

  private void preRunCleaner() {
    cleanersChain.forEach(FileCleanerDelegate::preClean);
  }

  public Boolean runCleaner() {
    preRunCleaner();
    CleanerTask task = new CleanerTask(this.oldFileDir, true);
    POOL.submit(task);
    return task.join();
  }

  /**
   * Sort the given list in (descending) order of the space each element takes
   * @param dirs the list to sort, element in it should be directory (not file)
   */
  private void sortByConsumedSpace(List<FileStatus> dirs) {
    if (dirs == null || dirs.size() < 2) {
      // no need to sort for empty or single directory
      return;
    }
    dirs.sort(new Comparator<FileStatus>() {
      HashMap<FileStatus, Long> directorySpaces = new HashMap<>();

      @Override
      public int compare(FileStatus f1, FileStatus f2) {
        long f1ConsumedSpace = getSpace(f1);
        long f2ConsumedSpace = getSpace(f2);
        return Long.compare(f2ConsumedSpace, f1ConsumedSpace);
      }

      private long getSpace(FileStatus f) {
        Long cached = directorySpaces.get(f);
        if (cached != null) {
          return cached;
        }
        try {
          long space =
              f.isDirectory() ? fs.getContentSummary(f.getPath()).getSpaceConsumed() : f.getLen();
          directorySpaces.put(f, space);
          return space;
        } catch (IOException e) {
          LOG.trace("Failed to get space consumed by path={}", f, e);
          return -1;
        }
      }
    });
  }

  /**
   * Run the given files through each of the cleaners to see if it should be deleted, deleting it if
   * necessary.
   * @param files List of FileStatus for the files to check (and possibly delete)
   * @return true iff successfully deleted all files
   */
  private boolean checkAndDeleteFiles(List<FileStatus> files) {
    if (files == null) {
      return true;
    }

    // first check to see if the path is valid
    List<FileStatus> validFiles = Lists.newArrayListWithCapacity(files.size());
    List<FileStatus> invalidFiles = Lists.newArrayList();
    for (FileStatus file : files) {
      if (validate(file.getPath())) {
        validFiles.add(file);
      } else {
        LOG.warn("Found a wrongly formatted file: " + file.getPath() + " - will delete it.");
        invalidFiles.add(file);
      }
    }

    Iterable<FileStatus> deletableValidFiles = validFiles;
    // check each of the cleaners for the valid files
    for (T cleaner : cleanersChain) {
      if (cleaner.isStopped() || this.getStopper().isStopped()) {
        LOG.warn("A file cleaner" + this.getName() + " is stopped, won't delete any more files in:"
            + this.oldFileDir);
        return false;
      }

      Iterable<FileStatus> filteredFiles = cleaner.getDeletableFiles(deletableValidFiles);

      // trace which cleaner is holding on to each file
      if (LOG.isTraceEnabled()) {
        ImmutableSet<FileStatus> filteredFileSet = ImmutableSet.copyOf(filteredFiles);
        for (FileStatus file : deletableValidFiles) {
          if (!filteredFileSet.contains(file)) {
            LOG.trace(file.getPath() + " is not deletable according to:" + cleaner);
          }
        }
      }

      deletableValidFiles = filteredFiles;
    }

    Iterable<FileStatus> filesToDelete = Iterables.concat(invalidFiles, deletableValidFiles);
    return deleteFiles(filesToDelete) == files.size();
  }

  /**
   * Delete the given files
   * @param filesToDelete files to delete
   * @return number of deleted files
   */
  protected int deleteFiles(Iterable<FileStatus> filesToDelete) {
    int deletedFileCount = 0;
    for (FileStatus file : filesToDelete) {
      Path filePath = file.getPath();
      LOG.trace("Removing {} from archive", filePath);
      try {
        boolean success = this.fs.delete(filePath, false);
        if (success) {
          deletedFileCount++;
        } else {
          LOG.warn("Attempted to delete:" + filePath
              + ", but couldn't. Run cleaner chain and attempt to delete on next pass.");
        }
      } catch (IOException e) {
        e = e instanceof RemoteException ?
                  ((RemoteException)e).unwrapRemoteException() : e;
        LOG.warn("Error while deleting: " + filePath, e);
      }
    }
    return deletedFileCount;
  }

  @Override
  public synchronized void cleanup() {
    for (T lc : this.cleanersChain) {
      try {
        lc.stop("Exiting");
      } catch (Throwable t) {
        LOG.warn("Stopping", t);
      }
    }
  }

  @VisibleForTesting
  int getChorePoolSize() {
    return POOL.size;
  }

  /**
   * @param enabled
   */
  public boolean setEnabled(final boolean enabled) {
    return this.enabled.getAndSet(enabled);
  }

  public boolean getEnabled() { return this.enabled.get();
  }

  private interface Action<T> {
    T act() throws IOException;
  }

  /**
   * Attemps to clean up a directory, its subdirectories, and files.
   * Return value is true if everything was deleted. false on partial / total failures.
   */
  private class CleanerTask extends RecursiveTask<Boolean> {
    private final Path dir;
    private final boolean root;

    CleanerTask(final FileStatus dir, final boolean root) {
      this(dir.getPath(), root);
    }

    CleanerTask(final Path dir, final boolean root) {
      this.dir = dir;
      this.root = root;
    }

    @Override
    protected Boolean compute() {
      LOG.debug("Cleaning under {}", dir);
      List<FileStatus> subDirs;
      List<FileStatus> files;
      try {
        // if dir doesn't exist, we'll get null back for both of these
        // which will fall through to succeeding.
        subDirs = getFilteredStatus(status -> status.isDirectory());
        files = getFilteredStatus(status -> status.isFile());
      } catch (IOException ioe) {
        LOG.warn("failed to get FileStatus for contents of '{}'", dir, ioe);
        return false;
      }

      boolean nullSubDirs = subDirs == null;
      if (nullSubDirs) {
        LOG.trace("There is no subdir under {}", dir);
      }
      if (files == null) {
        LOG.trace("There is no file under {}", dir);
      }

      int capacity = nullSubDirs ? 0 : subDirs.size();
      List<CleanerTask> tasks = Lists.newArrayListWithCapacity(capacity);
      if (!nullSubDirs) {
        sortByConsumedSpace(subDirs);
        for (FileStatus subdir : subDirs) {
          CleanerTask task = new CleanerTask(subdir, false);
          tasks.add(task);
          task.fork();
        }
      }

      boolean result = true;
      result &= deleteAction(() -> checkAndDeleteFiles(files), "files");
      result &= deleteAction(() -> getCleanResult(tasks), "subdirs");
      // if and only if files and subdirs under current dir are deleted successfully, and
      // it is not the root dir, then task will try to delete it.
      if (result && !root) {
        result &= deleteAction(() -> fs.delete(dir, false), "dir");
      }
      return result;
    }

    /**
     * Get FileStatus with filter.
     * Pay attention that FSUtils #listStatusWithStatusFilter would return null,
     * even though status is empty but not null.
     * @param function a filter function
     * @return filtered FileStatus or null if dir doesn't exist
     * @throws IOException if there's an error other than dir not existing
     */
    private List<FileStatus> getFilteredStatus(Predicate<FileStatus> function) throws IOException {
      return FSUtils.listStatusWithStatusFilter(fs, dir, status -> function.test(status));
    }

    /**
     * Perform a delete on a specified type.
     * @param deletion a delete
     * @param type possible values are 'files', 'subdirs', 'dirs'
     * @return true if it deleted successfully, false otherwise
     */
    private boolean deleteAction(Action<Boolean> deletion, String type) {
      boolean deleted;
      try {
        LOG.trace("Start deleting {} under {}", type, dir);
        deleted = deletion.act();
      } catch (PathIsNotEmptyDirectoryException exception) {
        // N.B. HDFS throws this exception when we try to delete a non-empty directory, but
        // LocalFileSystem throws a bare IOException. So some test code will get the verbose
        // message below.
        LOG.debug("Couldn't delete '{}' yet because it isn't empty. Probably transient. " +
            "exception details at TRACE.", dir);
        LOG.trace("Couldn't delete '{}' yet because it isn't empty w/exception.", dir, exception);
        deleted = false;
      } catch (IOException ioe) {
        LOG.info("Could not delete {} under {}. might be transient; we'll retry. if it keeps " +
                  "happening, use following exception when asking on mailing list.",
                  type, dir, ioe);
        deleted = false;
      }
      LOG.trace("Finish deleting {} under {}, deleted=", type, dir, deleted);
      return deleted;
    }

    /**
     * Get cleaner results of subdirs.
     * @param tasks subdirs cleaner tasks
     * @return true if all subdirs deleted successfully, false for patial/all failures
     * @throws IOException something happen during computation
     */
    private boolean getCleanResult(List<CleanerTask> tasks) throws IOException {
      boolean cleaned = true;
      try {
        for (CleanerTask task : tasks) {
          cleaned &= task.get();
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
      return cleaned;
    }
  }
}
