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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.ipc.RemoteException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Abstract Cleaner that uses a chain of delegates to clean a directory of files
 * @param <T> Cleaner delegate class that is dynamically loaded from configuration
 */
@InterfaceAudience.Private
public abstract class CleanerChore<T extends FileCleanerDelegate> extends ScheduledChore {

  private static final Log LOG = LogFactory.getLog(CleanerChore.class.getName());
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
  protected List<T> cleanersChain;
  protected Map<String, Object> params;
  private AtomicBoolean enabled = new AtomicBoolean(true);

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
  public CleanerChore(String name, final int sleepPeriod, final Stoppable s,
    Configuration conf, FileSystem fs, Path oldFileDir, String confKey,
    DirScanPool pool, Map<String, Object> params) {
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
        LOG.warn("Use full core processors to scan dir, size=" + size);
      }
      return size;
    } else if (poolSize.matches("0.[0-9]+|1.0")) {
      // if poolSize is a double, return poolSize * availableProcessors;
      // Ensure that we always return at least one.
      int computedThreads = (int) (AVAIL_PROCESSORS * Double.valueOf(poolSize));
      if (computedThreads < 1) {
        LOG.debug("Computed " + computedThreads + " threads for CleanerChore, using 1 instead");
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
    this.cleanersChain = new LinkedList<T>();
    String[] logCleaners = conf.getStrings(confKey);
    if (logCleaners != null) {
      for (String className : logCleaners) {
        T logCleaner = newFileCleaner(className, conf);
        if (logCleaner != null) {
          LOG.debug("initialize cleaner=" + className);
          this.cleanersChain.add(logCleaner);
        }
      }
    }
  }

  /**
   * A utility method to create new instances of LogCleanerDelegate based on the class name of the
   * LogCleanerDelegate.
   * @param className fully qualified class name of the LogCleanerDelegate
   * @param conf
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
      LOG.warn("Can NOT create CleanerDelegate: " + className, e);
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
          if (LOG.isTraceEnabled()) {
            LOG.trace("Cleaned all WALs under " + oldFileDir);
          }
        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace("WALs outstanding under " + oldFileDir);
          }
        }
      } finally {
        pool.latchCountDown();
      }
      // This cleaner is waiting for other cleaners finishing their jobs.
    // To avoid missing next chore, only wait 0.8 * period, then shutdown.
    pool.tryUpdatePoolSize((long) (0.8 * getTimeUnit().toMillis(getPeriod())));
  } else {
    LOG.trace("Cleaner chore disabled! Not cleaning.");
  }
}

  public Boolean runCleaner() {
    try {
      // Step 1: List all files under the given directory.
      List<FileStatus> subDirs = new ArrayList<>();
      List<FileStatus> files = new ArrayList<>();
      for (FileStatus status : fs.listStatus(oldFileDir)) {
        if (status.isDirectory()) subDirs.add(status);
        else if (status.isFile()) files.add(status);
      }

      // Step 2: Delete all files if any
      boolean allFilesDeleted = files.isEmpty() && checkAndDeleteFiles(files);

      // Step 3: Delete all sub dirs if any
      // And here we parallel those top sub dirs deletion.
      List<Future> cleanResults = new ArrayList<>(subDirs.size());
      for (FileStatus subdir : subDirs) {
        cleanResults.add(pool.execute(new TraversalCleaner(subdir)));
      }

      // Get result back. And the top dir should be kept.
      boolean result = allFilesDeleted;
      for (Future res : cleanResults) {
        result &= ((boolean) res.get());
      }
      return result;
    } catch (Exception e) {
      LOG.info("Failed to traverse and delete paths under the dir: " + oldFileDir, e);
      return false;
    }
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
      if (cleaner.isStopped() || getStopper().isStopped()) {
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
      LOG.trace("Removing " + file + " from archive");
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
    return pool.getSize();
  }

  /**
   * @param enabled
   */
  public boolean setEnabled(final boolean enabled) {
    return this.enabled.getAndSet(enabled);
  }

  public boolean getEnabled() {
    return this.enabled.get();
  }

  /**
   * Attemps to clean up a directory, its subdirectories, and files. Return value is true if
   * everything was deleted. false on partial / total failures.
   */
  private final class TraversalCleaner implements Callable<Boolean> {
    private final Path dir;

    TraversalCleaner(final FileStatus dir) {
      this(dir.getPath());
    }

    TraversalCleaner(final Path dir) {
      this.dir = dir;
    }

    @Override
    public Boolean call() {
      LOG.trace("Cleaning under " + dir);
      try {
        return travel(dir);
      } catch (Exception e) {
        LOG.debug("Failed to traverse and delete paths under the dir: " + dir, e);
        return false;
      }
    }

    private boolean travel(Path p) throws IOException {
      // Step 1: List all files under the given directory.
      List<FileStatus> allPaths = Arrays.asList(fs.listStatus(p));
      List<FileStatus> subDirs = new ArrayList<>();
      List<FileStatus> files = new ArrayList<>();
      for (FileStatus status : allPaths) {
        if (status.isDirectory()) subDirs.add(status);
        else if (status.isFile()) files.add(status);
      }

      // Step 2: Delete all files if any
      boolean allDeleted = files.isEmpty() && checkAndDeleteFiles(files);

      // Step 3: Delete all sub dirs if any
      if (!subDirs.isEmpty()) {
        for (FileStatus status : subDirs) {
          allDeleted &= travel(status.getPath());
        }
      }

      // Step 4. Delete dir itself.
      return allDeleted && fs.delete(dir, false);
    }
  }
}
