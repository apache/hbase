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
import org.apache.hadoop.hbase.util.FSUtils;
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
  // This static block is added for performance reasons. This is to ensure we are not checking
  // in the method calls whether retry properties are set or not. Refer to HBase-8288 for more
  // context.
  static {
    setRetryCounts(HBaseConfiguration.create());
  }

  
  /**
   * Deletes a file. Assumes the user has already checked for this file's existence.
   * @param fs
   * @param file
   * @return true if the file is deleted.
   * @throws IOException
   */
  public static boolean deleteFileFromFileSystem(FileSystem fs, Path file)
      throws IOException {
    IOException lastIOE = null;
    int i = 0;
    do {
      try {
        return fs.delete(file, false);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (!fs.exists(file)) return true;
        // dir is there, retry deleting after some time.
        sleepBeforeRetry("Delete File", i + 1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in deleteFileFromFileSystem", lastIOE);
  }
  
  
  /**
   * Deletes a directory. Assumes the user has already checked for this directory's existence.
   * @param fs
   * @param dir
   * @return true if the directory is deleted.
   * @throws IOException
   */
  public static boolean deleteDirFromFileSystem(FileSystem fs, Path dir)
      throws IOException {
    IOException lastIOE = null;
    int i = 0;
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

  protected static void setRetryCounts(Configuration conf) {
    hdfsClientRetriesNumber = conf.getInt("hdfs.client.retries.number",
      DEFAULT_HDFS_CLIENT_RETRIES_NUMBER);
    baseSleepBeforeRetries = conf.getInt("hdfs.client.sleep.before.retries",
      DEFAULT_BASE_SLEEP_BEFORE_RETRIES);
  }
  
  /**
   * Creates a directory for a filesystem and configuration object. Assumes the user has already
   * checked for this directory existence.
   * @param fs
   * @param dir
   * @return the result of fs.mkdirs(). In case underlying fs throws an IOException, it checks
   *         whether the directory exists or not, and returns true if it exists.
   * @throws IOException
   */
  public static boolean makeDirOnFileSystem(FileSystem fs, Path dir)
      throws IOException {
    int i = 0;
    IOException lastIOE = null;
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
   * @param fs
   * @param src
   * @param dst
   * @return true if the directory is renamed.
   * @throws IOException
   */
  public static boolean renameDirForFileSystem(FileSystem fs, Path src, Path dst)
      throws IOException {
    IOException lastIOE = null;
    int i = 0;
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
   * for retrying in case underlying fs throws an exception. If the dir already exists and overwrite
   * flag is false, the underlying FileSystem throws an IOE. It is not retried and the IOE is
   * re-thrown to the caller.
   * @param fs
   * @param dir
   * @param overwrite
   * @return
   * @throws IOException
   */
  public static FSDataOutputStream createPathOnFileSystem(FileSystem fs, Path dir, boolean overwrite)
      throws IOException {
    int i = 0;
    boolean existsBefore = fs.exists(dir);
    IOException lastIOE = null;
    do {
      try {
        return fs.create(dir, overwrite);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (existsBefore && !overwrite) throw ioe;// a legitimate exception
        sleepBeforeRetry("Create Path", i + 1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in createPathOnFileSystem", lastIOE);
  }

  /**
   * Creates the specified file with the given permission.
   * If the dir already exists and the overwrite flag is false, underlying FileSystem throws
   * an IOE. It is not retried and the IOE is re-thrown to the caller.
   * @param fs
   * @param path
   * @param perm
   * @param overwrite
   * @return
   * @throws IOException 
   */
  public static FSDataOutputStream createPathWithPermsOnFileSystem(FileSystem fs, Path path,
      FsPermission perm, boolean overwrite) throws IOException {
    int i = 0;
    IOException lastIOE = null;
    boolean existsBefore = fs.exists(path);
    do {
      try {
        return fs.create(path, perm, overwrite, FSUtils.getDefaultBufferSize(fs),
          FSUtils.getDefaultReplication(fs, path), FSUtils.getDefaultBlockSize(fs, path), null);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (existsBefore && !overwrite) throw ioe;// a legitimate exception
        sleepBeforeRetry("Create Path with Perms", i + 1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in createPathWithPermsOnFileSystem", lastIOE);
  }

/**
 * Creates the file. Assumes the user has already checked for this file existence.
 * @param fs
 * @param dir
 * @return result true if the file is created with this call, false otherwise.
 * @throws IOException
 */
  public static boolean createNewFileOnFileSystem(FileSystem fs, Path file)
      throws IOException {
    int i = 0;
    IOException lastIOE = null;
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
