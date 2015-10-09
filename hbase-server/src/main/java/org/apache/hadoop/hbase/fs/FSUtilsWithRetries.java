/**
 *
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

package org.apache.hadoop.hbase.fs;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FSUtilsWithRetries {
  private static final Log LOG = LogFactory.getLog(FSUtilsWithRetries.class);

  /**
   * In order to handle NN connectivity hiccups, one need to retry non-idempotent operation at the
   * client level.
   */
  private static final int DEFAULT_HDFS_CLIENT_RETRIES_NUMBER = 10;
  private static final int DEFAULT_BASE_SLEEP_BEFORE_RETRIES = 1000;
  private final int hdfsClientRetriesNumber;
  private final int baseSleepBeforeRetries;

  private final FileSystem fs;

  public FSUtilsWithRetries(final Configuration conf, final FileSystem fs) {
    this.fs = fs;

    this.hdfsClientRetriesNumber = conf.getInt("hdfs.client.retries.number",
      DEFAULT_HDFS_CLIENT_RETRIES_NUMBER);
    this.baseSleepBeforeRetries = conf.getInt("hdfs.client.sleep.before.retries",
      DEFAULT_BASE_SLEEP_BEFORE_RETRIES);
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  public boolean exists(Path path) throws IOException {
    IOException lastIOE = null;
    int i = 0;
    do {
      try {
        return fs.exists(path);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (fs.exists(path)) return true; // directory is present
        try {
          sleepBeforeRetry("Check existence", i+1);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in check existence", lastIOE);
  }

  /**
   * Creates a directory. Assumes the user has already checked for this directory existence.
   * @param dir
   * @return the result of fs.mkdirs(). In case underlying fs throws an IOException, it checks
   *         whether the directory exists or not, and returns true if it exists.
   * @throws IOException
   */
  public boolean createDir(Path dir) throws IOException {
    IOException lastIOE = null;
    int i = 0;
    do {
      try {
        return fs.mkdirs(dir);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (fs.exists(dir)) return true; // directory is present
        try {
          sleepBeforeRetry("Create Directory", i+1);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in createDir", lastIOE);
  }

  /**
   * Renames a directory. Assumes the user has already checked for this directory existence.
   * @param srcpath
   * @param dstPath
   * @return true if rename is successful.
   * @throws IOException
   */
  public boolean rename(Path srcpath, Path dstPath) throws IOException {
    IOException lastIOE = null;
    int i = 0;
    do {
      try {
        return fs.rename(srcpath, dstPath);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (!fs.exists(srcpath) && fs.exists(dstPath)) return true; // successful move
        // dir is not there, retry after some time.
        try {
          sleepBeforeRetry("Rename Directory", i+1);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);

    throw new IOException("Exception in rename", lastIOE);
  }

  /**
   * Deletes a directory. Assumes the user has already checked for this directory existence.
   * @param dir
   * @return true if the directory is deleted.
   * @throws IOException
   */
  public boolean deleteDir(Path dir) throws IOException {
    IOException lastIOE = null;
    int i = 0;
    do {
      try {
        return fs.delete(dir, true);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (!fs.exists(dir)) return true;
        // dir is there, retry deleting after some time.
        try {
          sleepBeforeRetry("Delete Directory", i+1);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);

    throw new IOException("Exception in DeleteDir", lastIOE);
  }

  /**
   * sleeping logic; handles the interrupt exception.
   */
  private void sleepBeforeRetry(String msg, int sleepMultiplier) throws InterruptedException {
    sleepBeforeRetry(msg, sleepMultiplier, baseSleepBeforeRetries, hdfsClientRetriesNumber);
  }

  /**
   * sleeping logic for static methods; handles the interrupt exception. Keeping a static version
   * for this to avoid re-looking for the integer values.
   */
  private static void sleepBeforeRetry(String msg, int sleepMultiplier, int baseSleepBeforeRetries,
      int hdfsClientRetriesNumber) throws InterruptedException {
    if (sleepMultiplier > hdfsClientRetriesNumber) {
      LOG.debug(msg + ", retries exhausted");
      return;
    }
    LOG.debug(msg + ", sleeping " + baseSleepBeforeRetries + " times " + sleepMultiplier);
    Thread.sleep((long)baseSleepBeforeRetries * sleepMultiplier);
  }
}
