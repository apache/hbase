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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ActiveClusterSuffix;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class AbstractReadOnlyController implements Coprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractReadOnlyController.class);

  private static final Set<TableName> writableTables =
    Set.of(TableName.META_TABLE_NAME, MasterRegionFactory.TABLE_NAME);

  public static boolean
    isWritableInReadOnlyMode(final ObserverContext<? extends RegionCoprocessorEnvironment> c) {
    return writableTables.contains(c.getEnvironment().getRegionInfo().getTable());
  }

  public static boolean isWritableInReadOnlyMode(final TableName tableName) {
    return writableTables.contains(tableName);
  }

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
        try (FSDataInputStream in = fs.open(activeClusterFile)) {
          ActiveClusterSuffix actualClusterFileData =
            ActiveClusterSuffix.parseFrom(in.readAllBytes());
          ActiveClusterSuffix expectedClusterFileData = mfs.getActiveClusterSuffix();
          if (expectedClusterFileData.equals(actualClusterFileData)) {
            fs.delete(activeClusterFile, false);
            LOG.info("Successfully deleted active cluster file: {}", activeClusterFile);
          } else {
            LOG.debug(
              "Active cluster file data does not match expected data. "
                + "Not deleting the file to avoid potential inconsistency. "
                + "Actual data: {}, Expected data: {}",
              actualClusterFileData, expectedClusterFileData);
          }
        } catch (FileNotFoundException e) {
          LOG.debug("Active cluster file does not exist at: {}. No need to delete.",
            activeClusterFile);
        } catch (IOException e) {
          LOG.error(
            "Failed to delete active cluster file: {}. "
              + "Read-only flag will be updated, but file system state is inconsistent.",
            activeClusterFile, e);
        } catch (DeserializationException e) {
          LOG.error("Failed to deserialize ActiveClusterSuffix from file {}", activeClusterFile, e);
        }
      } else {
        // DISABLING READ-ONLY (true -> false), create the active cluster file id file
        int wait = mfs.getConfiguration().getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
        if (!fs.exists(activeClusterFile)) {
          FSUtils.setClusterFile(fs, rootDir, HConstants.ACTIVE_CLUSTER_SUFFIX_FILE_NAME,
            mfs.getActiveClusterSuffix(), wait);
        } else {
          LOG.debug("Active cluster file already exists at: {}. No need to create it again.",
            activeClusterFile);
        }
      }
    } catch (IOException e) {
      // We still update the flag, but log that the operation failed.
      LOG.error("Failed to perform file operation for read-only switch. "
        + "Flag will be updated, but file system state may be inconsistent.", e);
    }
  }
}
