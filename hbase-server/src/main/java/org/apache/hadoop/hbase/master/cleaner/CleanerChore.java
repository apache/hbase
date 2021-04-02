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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Abstract Cleaner that uses a chain of delegates to clean a directory of files
 * @param <T> Cleaner delegate class that is dynamically loaded from configuration
 */
@InterfaceAudience.Private
public abstract class CleanerChore<T extends FileCleanerDelegate> extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(CleanerChore.class);
  private static final int AVAIL_PROCESSORS = Runtime.getRuntime().availableProcessors();

  /**
   * If it is an integer and >= 1, it would be the size;
   * if 0.0 < size <= 1.0, size would be available processors * size.
   * Pay attention that 1.0 is different from 1, former indicates it will use 100% of cores,
   * while latter will use only 1 thread for chore to scan dir.
   */
  public static final String CHORE_POOL_SIZE = "hbase.cleaner.scan.dir.concurrent.size";
  static final String DEFAULT_CHORE_POOL_SIZE = "0.25";

  private final DirScanPool pool;

  protected final FileSystem fs;
  private final Path oldFileDir;
  private final Configuration conf;
  protected final Map<String, Object> params;
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  protected List<T> cleanersChain;

  public CleanerChore(String name, final int sleepPeriod, final Stoppable s, Configuration conf,
    FileSystem fs, Path oldFileDir, String confKey, DirScanPool pool) {
    this(name, sleepPeriod, s, conf, fs, oldFileDir, confKey, pool, null);
  }

  /**
   * @param name name of the chore being run
   * @param sleepPeriod the period of time to sleep between each run
   * @param s the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param oldFileDir the path to the archived files
   * @param confKey configuration key for the classes to instantiate
   * @param pool the thread pool used to scan directories
   * @param params members could be used in cleaner
   */
  public CleanerChore(String name, final int sleepPeriod, final Stoppable s, Configuration conf,
    FileSystem fs, Path oldFileDir, String confKey, DirScanPool pool, Map<String, Object> params) {
    super(name, s, sleepPeriod);

    Preconditions.checkNotNull(pool, "Chore's pool can not be null");
    this.pool = pool;
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
      int computedThreads = (int) (AVAIL_PROCESSORS * Double.parseDouble(poolSize));
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
      for (String className: logCleaners) {
        className = className.trim();
        if (className.isEmpty()) {
          continue;
        }
        T logCleaner = newFileCleaner(className, conf);
        if (logCleaner != null) {
          LOG.info("Initialize cleaner={}", className);
          this.cleanersChain.add(logCleaner);
        }
      }
    }
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
        pool.latchCountUp();
        if (runCleaner()) {
          LOG.trace("Cleaned all WALs under {}", oldFileDir);
        } else {
          LOG.trace("WALs outstanding under {}", oldFileDir);
        }
      } finally {
        pool.latchCountDown();
      }
      // After each cleaner chore, checks if received reconfigure notification while cleaning.
      // First in cleaner turns off notification, to avoid another cleaner updating pool again.
      // This cleaner is waiting for other cleaners finishing their jobs.
      // To avoid missing next chore, only wait 0.8 * period, then shutdown.
      pool.tryUpdatePoolSize((long) (0.8 * getTimeUnit().toMillis(getPeriod())));
    } else {
      LOG.trace("Cleaner chore disabled! Not cleaning.");
    }
  }

  private void preRunCleaner() {
    cleanersChain.forEach(FileCleanerDelegate::preClean);
  }

  public boolean runCleaner() {
    preRunCleaner();
    try {
      CompletableFuture<Boolean> future = new CompletableFuture<>();
      pool.execute(() -> traverseAndDelete(oldFileDir, true, future));
      return future.get();
    } catch (Exception e) {
      LOG.info("Failed to traverse and delete the dir: {}", oldFileDir, e);
      return false;
    }
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
   * Check if a empty directory with no subdirs or subfiles can be deleted
   * @param dir Path of the directory
   * @return True if the directory can be deleted, otherwise false
   */
  private boolean isEmptyDirDeletable(Path dir) {
    for (T cleaner : cleanersChain) {
      if (cleaner.isStopped() || this.getStopper().isStopped()) {
        LOG.warn("A file cleaner {} is stopped, won't delete the empty directory {}",
          this.getName(), dir);
        return false;
      }
      if (!cleaner.isEmptyDirDeletable(dir)) {
        // If one of the cleaner need the empty directory, skip delete it
        return false;
      }
    }
    return true;
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

  int getChorePoolSize() {
    return pool.getSize();
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
    T act() throws Exception;
  }

  /**
   * Attempts to clean up a directory(its subdirectories, and files) in a
   * {@link java.util.concurrent.ThreadPoolExecutor} concurrently. We can get the final result by
   * calling result.get().
   */
  private void traverseAndDelete(Path dir, boolean root, CompletableFuture<Boolean> result) {
    try {
      // Step.1: List all files under the given directory.
      List<FileStatus> allPaths = Arrays.asList(fs.listStatus(dir));
      List<FileStatus> subDirs =
          allPaths.stream().filter(FileStatus::isDirectory).collect(Collectors.toList());
      List<FileStatus> files =
          allPaths.stream().filter(FileStatus::isFile).collect(Collectors.toList());

      // Step.2: Try to delete all the deletable files.
      boolean allFilesDeleted =
          files.isEmpty() || deleteAction(() -> checkAndDeleteFiles(files), "files", dir);

      // Step.3: Start to traverse and delete the sub-directories.
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();
      if (!subDirs.isEmpty()) {
        sortByConsumedSpace(subDirs);
        // Submit the request of sub-directory deletion.
        subDirs.forEach(subDir -> {
          CompletableFuture<Boolean> subFuture = new CompletableFuture<>();
          pool.execute(() -> traverseAndDelete(subDir.getPath(), false, subFuture));
          futures.add(subFuture);
        });
      }

      // Step.4: Once all sub-files & sub-directories are deleted, then can try to delete the
      // current directory asynchronously.
      FutureUtils.addListener(
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])),
        (voidObj, e) -> {
          if (e != null) {
            result.completeExceptionally(e);
            return;
          }
          try {
            boolean allSubDirsDeleted = futures.stream().allMatch(CompletableFuture::join);
            boolean deleted = allFilesDeleted && allSubDirsDeleted && isEmptyDirDeletable(dir);
            if (deleted && !root) {
              // If and only if files and sub-dirs under current dir are deleted successfully, and
              // the empty directory can be deleted, and it is not the root dir then task will
              // try to delete it.
              deleted = deleteAction(() -> fs.delete(dir, false), "dir", dir);
            }
            result.complete(deleted);
          } catch (Exception ie) {
            // Must handle the inner exception here, otherwise the result may get stuck if one
            // sub-directory get some failure.
            result.completeExceptionally(ie);
          }
        });
    } catch (Exception e) {
      LOG.debug("Failed to traverse and delete the path: {}", dir, e);
      result.completeExceptionally(e);
    }
  }

  /**
   * Perform a delete on a specified type.
   * @param deletion a delete
   * @param type possible values are 'files', 'subdirs', 'dirs'
   * @return true if it deleted successfully, false otherwise
   */
  private boolean deleteAction(Action<Boolean> deletion, String type, Path dir) {
    boolean deleted;
    try {
      LOG.trace("Start deleting {} under {}", type, dir);
      deleted = deletion.act();
    } catch (PathIsNotEmptyDirectoryException exception) {
      // N.B. HDFS throws this exception when we try to delete a non-empty directory, but
      // LocalFileSystem throws a bare IOException. So some test code will get the verbose
      // message below.
      LOG.debug("Couldn't delete '{}' yet because it isn't empty w/exception.", dir, exception);
      deleted = false;
    } catch (IOException ioe) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Could not delete {} under {}; will retry. If it keeps happening, " +
            "quote the exception when asking on mailing list.", type, dir, ioe);
      } else {
        LOG.info("Could not delete {} under {} because {}; will retry. If it  keeps happening, enable" +
            "TRACE-level logging and quote the exception when asking on mailing list.",
            type, dir, ioe.getMessage());
      }
      deleted = false;
    } catch (Exception e) {
      LOG.info("unexpected exception: ", e);
      deleted = false;
    }
    LOG.trace("Finish deleting {} under {}, deleted=", type, dir, deleted);
    return deleted;
  }
}
