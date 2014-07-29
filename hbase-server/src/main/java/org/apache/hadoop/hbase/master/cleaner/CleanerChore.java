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
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.ipc.RemoteException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Abstract Cleaner that uses a chain of delegates to clean a directory of files
 * @param <T> Cleaner delegate class that is dynamically loaded from configuration
 */
public abstract class CleanerChore<T extends FileCleanerDelegate> extends Chore {

  private static final Log LOG = LogFactory.getLog(CleanerChore.class.getName());

  private final FileSystem fs;
  private final Path oldFileDir;
  private final Configuration conf;
  protected List<T> cleanersChain;

  /**
   * @param name name of the chore being run
   * @param sleepPeriod the period of time to sleep between each run
   * @param s the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param oldFileDir the path to the archived files
   * @param confKey configuration key for the classes to instantiate
   */
  public CleanerChore(String name, final int sleepPeriod, final Stoppable s, Configuration conf,
      FileSystem fs, Path oldFileDir, String confKey) {
    super(name, sleepPeriod, s);
    this.fs = fs;
    this.oldFileDir = oldFileDir;
    this.conf = conf;

    initCleanerChain(confKey);
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
      T cleaner = (T) c.newInstance();
      cleaner.setConf(conf);
      return cleaner;
    } catch (Exception e) {
      LOG.warn("Can NOT create CleanerDelegate: " + className, e);
      // skipping if can't instantiate
      return null;
    }
  }

  @Override
  protected void chore() {
    try {
      FileStatus[] files = FSUtils.listStatus(this.fs, this.oldFileDir);
      checkAndDeleteEntries(files);
    } catch (IOException e) {
      e = e instanceof RemoteException ?
              ((RemoteException)e).unwrapRemoteException() : e;
      LOG.warn("Error while cleaning the logs", e);
    }
  }

  /**
   * Loop over the given directory entries, and check whether they can be deleted.
   * If an entry is itself a directory it will be recursively checked and deleted itself iff
   * all subentries are deleted (and no new subentries are added in the mean time)
   *
   * @param entries directory entries to check
   * @return true if all entries were successfully deleted
   */
  private boolean checkAndDeleteEntries(FileStatus[] entries) {
    if (entries == null) {
      return true;
    }
    boolean allEntriesDeleted = true;
    List<FileStatus> files = Lists.newArrayListWithCapacity(entries.length);
    for (FileStatus child : entries) {
      Path path = child.getPath();
      if (child.isDirectory()) {
        // for each subdirectory delete it and all entries if possible
        if (!checkAndDeleteDirectory(path)) {
          allEntriesDeleted = false;
        }
      } else {
        // collect all files to attempt to delete in one batch
        files.add(child);
      }
    }
    if (!checkAndDeleteFiles(files)) {
      allEntriesDeleted = false;
    }
    return allEntriesDeleted;
  }
  
  /**
   * Attempt to delete a directory and all files under that directory. Each child file is passed
   * through the delegates to see if it can be deleted. If the directory has no children when the
   * cleaners have finished it is deleted.
   * <p>
   * If new children files are added between checks of the directory, the directory will <b>not</b>
   * be deleted.
   * @param dir directory to check
   * @return <tt>true</tt> if the directory was deleted, <tt>false</tt> otherwise.
   */
  @VisibleForTesting boolean checkAndDeleteDirectory(Path dir) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Checking directory: " + dir);
    }

    try {
      FileStatus[] children = FSUtils.listStatus(fs, dir);
      boolean allChildrenDeleted = checkAndDeleteEntries(children);
  
      // if the directory still has children, we can't delete it, so we are done
      if (!allChildrenDeleted) return false;
    } catch (IOException e) {
        e = e instanceof RemoteException ?
                ((RemoteException)e).unwrapRemoteException() : e;
    	LOG.warn("Error while listing directory: " + dir, e);
      // couldn't list directory, so don't try to delete, and don't return success
      return false;
    }

    // otherwise, all the children (that we know about) have been deleted, so we should try to
    // delete this directory. However, don't do so recursively so we don't delete files that have
    // been added since we last checked.
    try {
      return fs.delete(dir, false);
    } catch (IOException e) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Couldn't delete directory: " + dir, e);
      }
      // couldn't delete w/o exception, so we can't return success.
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
      if (cleaner.isStopped() || this.stopper.isStopped()) {
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
    int deletedFileCount = 0;
    for (FileStatus file : filesToDelete) {
      Path filePath = file.getPath();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Removing: " + filePath + " from archive");
      }
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

    return deletedFileCount == files.size();
  }

  @Override
  public void cleanup() {
    for (T lc : this.cleanersChain) {
      try {
        lc.stop("Exiting");
      } catch (Throwable t) {
        LOG.warn("Stopping", t);
      }
    }
  }
}
