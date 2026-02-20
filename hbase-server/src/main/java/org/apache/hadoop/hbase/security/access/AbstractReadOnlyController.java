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
package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class AbstractReadOnlyController implements Coprocessor {
  private MasterServices masterServices;
  private static final Logger LOG = LoggerFactory.getLogger(AbstractReadOnlyController.class);

  protected void internalReadOnlyGuard() throws DoNotRetryIOException {
    throw new DoNotRetryIOException("Operation not allowed in Read-Only Mode");
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
  }

  public static void manageActiveClusterIdFile(boolean readOnlyEnabled, MasterFileSystem mfs) {
    FileSystem fs = mfs.getFileSystem();
    Path rootDir = mfs.getRootDir();
    Path activeClusterFile = new Path(rootDir, HConstants.ACTIVE_CLUSTER_SUFFIX_FILE_NAME);

    try {
      if (readOnlyEnabled) {
        // ENABLING READ-ONLY (false -> true), delete the active cluster file.
        LOG.debug("Global read-only mode is being ENABLED. Deleting active cluster file: {}",
          activeClusterFile);
        try {
          fs.delete(activeClusterFile, false);
          LOG.info("Successfully deleted active cluster file: {}", activeClusterFile);
        } catch (IOException e) {
          LOG.error(
            "Failed to delete active cluster file: {}. "
              + "Read-only flag will be updated, but file system state is inconsistent.",
            activeClusterFile);
        }
      } else {
        // DISABLING READ-ONLY (true -> false), create the active cluster file id file
        int wait = mfs.getConfiguration().getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
        FSUtils.setActiveClusterSuffix(fs, rootDir, mfs.getSuffixFileDataToWrite(), wait);
      }
    } catch (IOException e) {
      // We still update the flag, but log that the operation failed.
      LOG.error("Failed to perform file operation for read-only switch. "
        + "Flag will be updated, but file system state may be inconsistent.", e);
    }
  }
}
