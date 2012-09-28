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
  private List<T> cleanersChain;

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
          if (file.isDir()) checkDirectory(file.getPath());
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
   * Check to see if we can delete a directory (and all the children files of that directory).
   * <p>
   * A directory will not be deleted if it has children that are subsequently deleted since that
   * will require another set of lookups in the filesystem, which is semantically same as waiting
   * until the next time the chore is run, so we might as well wait.
   * @param fs {@link FileSystem} where he directory resides
   * @param toCheck directory to check
   * @throws IOException
   */
  private void checkDirectory(Path toCheck) throws IOException {
    LOG.debug("Checking directory: " + toCheck);
    FileStatus[] files = checkAndDeleteDirectory(toCheck);
    // if the directory doesn't exist, then we are done
    if (files == null) return;

    // otherwise we need to check each of the child files
    for (FileStatus file : files) {
      Path filePath = file.getPath();
      // if its a directory, then check to see if it should be deleted
      if (file.isDir()) {
        // check the subfiles to see if they can be deleted
        checkDirectory(filePath);
        continue;
      }
      // otherwise we can just check the file
      checkAndDelete(filePath);
    }

    // recheck the directory to see if we can delete it this time
    checkAndDeleteDirectory(toCheck);
  }

  /**
   * Check and delete the passed directory if the directory is empty
   * @param toCheck full path to the directory to check (and possibly delete)
   * @return <tt>null</tt> if the directory was empty (and possibly deleted) and otherwise an array
   *         of <code>FileStatus</code> for the files in the directory
   * @throws IOException
   */
  private FileStatus[] checkAndDeleteDirectory(Path toCheck) throws IOException {
    LOG.debug("Attempting to delete directory:" + toCheck);
    // if it doesn't exist, we are done
    if (!fs.exists(toCheck)) return null;
    // get the files below the directory
    FileStatus[] files = FSUtils.listStatus(fs, toCheck, null);
    // if there are no subfiles, then we can delete the directory
    if (files == null) {
      checkAndDelete(toCheck);
      return null;
    }

    // return the status of the files in the directory
    return files;
  }

  /**
   * Run the given file through each of the cleaners to see if it should be deleted, deleting it if
   * necessary.
   * @param filePath path of the file to check (and possibly delete)
   * @throws IOException if cann't delete a file because of a filesystem issue
   * @throws IllegalArgumentException if the file is a directory and has children
   */
  private void checkAndDelete(Path filePath) throws IOException, IllegalArgumentException {
    if (!validate(filePath)) {
      LOG.warn("Found a wrongly formatted file: " + filePath.getName() + " deleting it.");
      if (!this.fs.delete(filePath, true)) {
        LOG.warn("Attempted to delete:" + filePath
            + ", but couldn't. Run cleaner chain and attempt to delete on next pass.");
      }
      return;
    }
    for (T cleaner : cleanersChain) {
      if (cleaner.isStopped()) {
        LOG.warn("A file cleaner" + this.getName() + " is stopped, won't delete any file in:"
            + this.oldFileDir);
        return;
      }

      if (!cleaner.isFileDeletable(filePath)) {
        // this file is not deletable, then we are done
        LOG.debug(filePath + " is not deletable according to:" + cleaner);
        return;
      }
    }
    // delete this file if it passes all the cleaners
    LOG.debug("Removing:" + filePath + " from archive");
    if (!this.fs.delete(filePath, false)) {
      LOG.warn("Attempted to delete:" + filePath
          + ", but couldn't. Run cleaner chain and attempt to delete on next pass.");
    }
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
