/*
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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.regionserver.wal.HLogFileSystem;
import org.apache.hadoop.hbase.util.Threads;

/**
 * An abstraction of the underlying filesystem. This is used by other entities such as
 * {@link HLogFileSystem}, to make calls to the underlying filesystem.
 *
 */
public abstract class HBaseFileSystem {

  public static final Log LOG = LogFactory.getLog(HBaseFileSystem.class);

  /**
   * In order to handle NN connectivity hiccups, one need to retry non-idempotent operation at the
   * client level.
   */
  protected static  int hdfsClientRetriesNumber;
  private static int baseSleepBeforeRetries;
  private static final int DEFAULT_HDFS_CLIENT_RETRIES_NUMBER = 10;
  private static final int DEFAULT_BASE_SLEEP_BEFORE_RETRIES = 1000;

  
  /**
   * Deletes a file. Assumes the user has already checked for this directory existence.
   * @param dir
   * @param fs
   * @param conf
   * @return true if the directory is deleted.
   * @throws IOException
   */
  public static boolean deleteFileFromFileSystem(FileSystem fs, Configuration conf, Path dir)
      throws IOException {
    IOException lastIOE = null;
    int i = 0;
    checkAndSetRetryCounts(conf);
    do {
      try {
        return fs.delete(dir, false);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (!fs.exists(dir)) return true;
        // dir is there, retry deleting after some time.
        sleepBeforeRetry("Delete File", i + 1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in deleteFileFromFileSystem", lastIOE);
  }
  
  
  /**
   * Deletes a directory. Assumes the user has already checked for this directory existence.
   * @param dir
   * @param fs
   * @param conf
   * @return true if the directory is deleted.
   * @throws IOException
   */
  public static boolean deleteDirFromFileSystem(FileSystem fs, Configuration conf, Path dir)
      throws IOException {
    IOException lastIOE = null;
    int i = 0;
    checkAndSetRetryCounts(conf);
    do {
      try {
        return fs.delete(dir, true);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (!fs.exists(dir)) return true;
        // dir is there, retry deleting after some time.
        sleepBeforeRetry("Delete Dir", i + 1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in deleteDirFromFileSystem", lastIOE);
  }

  protected static void checkAndSetRetryCounts(Configuration conf) {
    if (hdfsClientRetriesNumber == 0) {
      hdfsClientRetriesNumber = conf.getInt("hdfs.client.retries.number",
        DEFAULT_HDFS_CLIENT_RETRIES_NUMBER);
      baseSleepBeforeRetries = conf.getInt("hdfs.client.sleep.before.retries",
        DEFAULT_BASE_SLEEP_BEFORE_RETRIES);
    }
  }
  
  /**
   * Creates a directory for a filesystem and configuration object. Assumes the user has already
   * checked for this directory existence.
   * @param fs
   * @param conf
   * @param dir
   * @return the result of fs.mkdirs(). In case underlying fs throws an IOException, it checks
   *         whether the directory exists or not, and returns true if it exists.
   * @throws IOException
   */
  public static boolean makeDirOnFileSystem(FileSystem fs, Configuration conf, Path dir)
      throws IOException {
    int i = 0;
    IOException lastIOE = null;
    checkAndSetRetryCounts(conf);
    do {
      try {
        return fs.mkdirs(dir);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (fs.exists(dir)) return true; // directory is present
        sleepBeforeRetry("Create Directory", i+1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in makeDirOnFileSystem", lastIOE);
  }
  
  /**
   * Renames a directory. Assumes the user has already checked for this directory existence.
   * @param src
   * @param fs
   * @param dst
   * @param conf
   * @return true if the directory is renamed.
   * @throws IOException
   */
  public static boolean renameDirForFileSystem(FileSystem fs, Configuration conf, Path src, Path dst)
      throws IOException {
    IOException lastIOE = null;
    int i = 0;
    checkAndSetRetryCounts(conf);
    do {
      try {
        return fs.rename(src, dst);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (!fs.exists(src) && fs.exists(dst)) return true;
        // src is there, retry renaming after some time.
        sleepBeforeRetry("Rename Directory", i + 1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in renameDirForFileSystem", lastIOE);
  }
  
/**
 * Creates a path on the file system. Checks whether the path exists already or not, and use it
 * for retrying in case underlying fs throws an exception.
 * @param fs
 * @param conf
 * @param dir
 * @param overwrite
 * @return
 * @throws IOException
 */
  public static FSDataOutputStream createPathOnFileSystem(FileSystem fs, Configuration conf, Path dir,
      boolean overwrite) throws IOException {
    int i = 0;
    boolean existsBefore = fs.exists(dir);
    IOException lastIOE = null;
    checkAndSetRetryCounts(conf);
    do {
      try {
        return fs.create(dir, overwrite);
      } catch (IOException ioe) {
        lastIOE = ioe;
        // directory is present, don't overwrite
        if (!existsBefore && fs.exists(dir)) return fs.create(dir, false);
        sleepBeforeRetry("Create Path", i + 1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in createPathOnFileSystem", lastIOE);
  }

  /**
   * Creates the specified file with the given permission.
   * @param fs
   * @param path
   * @param perm
   * @param overwrite
   * @return
   * @throws IOException 
   */
  public static FSDataOutputStream createPathWithPermsOnFileSystem(FileSystem fs,
      Configuration conf, Path path, FsPermission perm, boolean overwrite) throws IOException {
    int i = 0;
    IOException lastIOE = null;
    boolean existsBefore = fs.exists(path);
    checkAndSetRetryCounts(conf);
    do {
      try {
        return fs.create(path, perm, overwrite, fs.getConf().getInt("io.file.buffer.size", 4096),
          fs.getDefaultReplication(), fs.getDefaultBlockSize(), null);
      } catch (IOException ioe) {
        lastIOE = ioe;
        // path is present now, don't overwrite
        if (!existsBefore && fs.exists(path)) return fs.create(path, false);
        // if it existed before let's retry in case we get IOE, as we can't rely on fs.exists()
        sleepBeforeRetry("Create Path with Perms", i + 1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in createPathWithPermsOnFileSystem", lastIOE);
  }

/**
 * Creates the file. Assumes the user has already checked for this file existence.
 * @param fs
 * @param conf
 * @param dir
 * @return result true if the file is created with this call, false otherwise.
 * @throws IOException
 */
  public static boolean createNewFileOnFileSystem(FileSystem fs, Configuration conf, Path file)
      throws IOException {
    int i = 0;
    IOException lastIOE = null;
    checkAndSetRetryCounts(conf);
    do {
      try {
        return fs.createNewFile(file);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (fs.exists(file)) return true; // file exists now, return true.
        sleepBeforeRetry("Create NewFile", i + 1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in createNewFileOnFileSystem", lastIOE);
  }
  
  /**
   * sleeping logic for static methods; handles the interrupt exception. Keeping a static version
   * for this to avoid re-looking for the integer values.
   */
  protected static void sleepBeforeRetry(String msg, int sleepMultiplier) {
    if (sleepMultiplier > hdfsClientRetriesNumber) {
      LOG.warn(msg + ", retries exhausted");
      return;
    }
    LOG.info(msg + ", sleeping " + baseSleepBeforeRetries + " times " + sleepMultiplier);
    Threads.sleep(baseSleepBeforeRetries * sleepMultiplier);
  }
}
