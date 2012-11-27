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
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Abstract Cleaner that uses a chain of delegates to clean a directory of files
 * @param <T> Cleaner delegate class that is dynamically loaded from configuration
 */
public abstract class CleanerChore<T extends FileCleanerDelegate> extends Chore {

  private static final Log LOG = LogFactory.getLog(CleanerChore.class.getName());

  private final FileSystem fs;
  private final Path oldFileDir;
  private final Configuration conf;
  List<T> cleanersChain;

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
   * Instanitate and initialize all the file cleaners set in the configuration
   * @param confKey key to get the file cleaner classes from the configuration
   */
  private void initCleanerChain(String confKey) {
    this.cleanersChain = new LinkedList<T>();
    String[] logCleaners = conf.getStrings(confKey);
    if (logCleaners != null) {
      for (String className : logCleaners) {
        T logCleaner = newFileCleaner(className, conf);
        if (logCleaner != null) this.cleanersChain.add(logCleaner);
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
  public T newFileCleaner(String className, Configuration conf) {
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
      FileStatus[] files = FSUtils.listStatus(this.fs, this.oldFileDir, null);
      // if the path (file or directory) doesn't exist, then we can just return
      if (files == null) return;
      // loop over the found files and see if they should be deleted
      for (FileStatus file : files) {
        try {
          if (file.isDir()) checkAndDeleteDirectory(file.getPath());
          else checkAndDelete(file.getPath());
        } catch (IOException e) {
          e = RemoteExceptionHandler.checkIOException(e);
          LOG.warn("Error while cleaning the logs", e);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to get status of:" + oldFileDir);
    }

  }

  /**
   * Attempt to delete a directory and all files under that directory. Each child file is passed
   * through the delegates to see if it can be deleted. If the directory has not children when the
   * cleaners have finished it is deleted.
   * <p>
   * If new children files are added between checks of the directory, the directory will <b>not</b>
   * be deleted.
   * @param toCheck directory to check
   * @return <tt>true</tt> if the directory was deleted, <tt>false</tt> otherwise.
   * @throws IOException if there is an unexpected filesystem error
   */
  private boolean checkAndDeleteDirectory(Path toCheck) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Checking directory: " + toCheck);
    }
    FileStatus[] children = FSUtils.listStatus(fs, toCheck, null);
    // if the directory doesn't exist, then we are done
    if (children == null) return true;

    boolean canDeleteThis = true;
    for (FileStatus child : children) {
      Path path = child.getPath();
      // attempt to delete all the files under the directory
      if (child.isDir()) {
        if (!checkAndDeleteDirectory(path)) {
          canDeleteThis = false;
        }
      }
      // otherwise we can just check the file
      else if (!checkAndDelete(path)) {
        canDeleteThis = false;
      }
    }

    // if all the children have been deleted, then we should try to delete this directory. However,
    // don't do so recursively so we don't delete files that have been added since we checked.
    return canDeleteThis ? fs.delete(toCheck, false) : false;
  }

  /**
   * Run the given file through each of the cleaners to see if it should be deleted, deleting it if
   * necessary.
   * @param filePath path of the file to check (and possibly delete)
   * @throws IOException if cann't delete a file because of a filesystem issue
   * @throws IllegalArgumentException if the file is a directory and has children
   */
  private boolean checkAndDelete(Path filePath) throws IOException, IllegalArgumentException {
    // first check to see if the path is valid
    if (!validate(filePath)) {
      LOG.warn("Found a wrongly formatted file: " + filePath.getName() + " deleting it.");
      boolean success = this.fs.delete(filePath, true);
      if (!success) LOG.warn("Attempted to delete:" + filePath
          + ", but couldn't. Run cleaner chain and attempt to delete on next pass.");

      return success;
    }
    // check each of the cleaners for the file
    for (T cleaner : cleanersChain) {
      if (cleaner.isStopped() || this.stopper.isStopped()) {
        LOG.warn("A file cleaner" + this.getName() + " is stopped, won't delete any file in:"
            + this.oldFileDir);
        return false;
      }

      if (!cleaner.isFileDeletable(filePath)) {
        // this file is not deletable, then we are done
        if (LOG.isTraceEnabled()) {
          LOG.trace(filePath + " is not deletable according to:" + cleaner);
        }
        return false;
      }
    }
    // delete this file if it passes all the cleaners
    if (LOG.isTraceEnabled()) {
      LOG.trace("Removing:" + filePath + " from archive");
    }
    boolean success = this.fs.delete(filePath, false);
    if (!success) {
      LOG.warn("Attempted to delete:" + filePath
          + ", but couldn't. Run cleaner chain and attempt to delete on next pass.");
    }
    return success;
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