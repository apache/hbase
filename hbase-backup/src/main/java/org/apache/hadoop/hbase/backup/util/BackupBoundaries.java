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
package org.apache.hadoop.hbase.backup.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks time boundaries for WAL file cleanup during backup operations. Maintains the oldest
 * timestamp per RegionServer included in any backup, enabling safe determination of which WAL files
 * can be deleted without compromising backup integrity.
 */
@InterfaceAudience.Private
public class BackupBoundaries {
  private static final Logger LOG = LoggerFactory.getLogger(BackupBoundaries.class);

  // This map tracks, for every RegionServer, the least recent (= oldest / lowest timestamp)
  // inclusion in any backup. In other words, it is the timestamp boundary up to which all backup
  // roots have included the WAL in their backup.
  private final Map<Address, Long> boundaries;

  // The fallback cleanup boundary for RegionServers without explicit backup boundaries
  // (e.g., servers that joined after backups began can be checked against this boundary)
  private final long defaultBoundary;

  private BackupBoundaries(Map<Address, Long> boundaries, long defaultBoundary) {
    this.boundaries = boundaries;
    this.defaultBoundary = defaultBoundary;
  }

  public boolean isDeletable(Path walLogPath) {
    try {
      String hostname = BackupUtils.parseHostNameFromLogFile(walLogPath);

      if (hostname == null) {
        LOG.warn(
          "Cannot parse hostname from RegionServer WAL file: {}. Ignoring cleanup of this log",
          walLogPath);
        return false;
      }

      Address address = Address.fromString(hostname);
      long pathTs = AbstractFSWALProvider.getTimestamp(walLogPath.getName());

      if (!boundaries.containsKey(address)) {
        boolean isDeletable = pathTs <= defaultBoundary;
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Boundary for {} not found. isDeletable = {} based on defaultBoundary = {} and WAL ts of {}",
            walLogPath, isDeletable, defaultBoundary, pathTs);
        }
        return isDeletable;
      }

      long backupTs = boundaries.get(address);
      if (pathTs <= backupTs) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "WAL cleanup time-boundary found for server {}: {}. Ok to delete older file: {}",
            address.getHostName(), pathTs, walLogPath);
        }
        return true;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("WAL cleanup time-boundary found for server {}: {}. Keeping younger file: {}",
          address.getHostName(), backupTs, walLogPath);
      }

      return false;
    } catch (Exception e) {
      LOG.warn("Error occurred while filtering file: {}. Ignoring cleanup of this log", walLogPath,
        e);
      return false;
    }
  }

  public Map<Address, Long> getBoundaries() {
    return boundaries;
  }

  public long getDefaultBoundary() {
    return defaultBoundary;
  }

  public static BackupBoundariesBuilder builder(long tsCleanupBuffer) {
    return new BackupBoundariesBuilder(tsCleanupBuffer);
  }

  public static class BackupBoundariesBuilder {
    private final Map<Address, Long> boundaries = new HashMap<>();
    private final long tsCleanupBuffer;

    private long oldestStartTs = Long.MAX_VALUE;

    private BackupBoundariesBuilder(long tsCleanupBuffer) {
      this.tsCleanupBuffer = tsCleanupBuffer;
    }

    /**
     * Updates the boundaries based on the provided backup info.
     * @param backupInfo the most recent completed backup info for a backup root, or if there is no
     *                   such completed backup, the currently running backup.
     */
    public void update(BackupInfo backupInfo) {
      switch (backupInfo.getState()) {
        case COMPLETE:
          // If a completed backup exists in the backup root, we want to protect all logs that
          // have been created since the log-roll that happened for that backup.
          for (TableName table : backupInfo.getTableSetTimestampMap().keySet()) {
            for (Map.Entry<String, Long> entry : backupInfo.getTableSetTimestampMap().get(table)
              .entrySet()) {
              Address regionServerAddress = Address.fromString(entry.getKey());
              Long logRollTs = entry.getValue();

              Long storedTs = boundaries.get(regionServerAddress);
              if (storedTs == null || logRollTs < storedTs) {
                boundaries.put(regionServerAddress, logRollTs);
              }
            }
          }
          break;
        case RUNNING:
          // If there is NO completed backup in the backup root, there are no persisted log-roll
          // timestamps available yet. But, we still want to protect all files that have been
          // created since the start of the currently running backup.
          oldestStartTs = Math.min(oldestStartTs, backupInfo.getStartTs());
          break;
        default:
          throw new IllegalStateException("Unexpected backupInfo state: " + backupInfo.getState());
      }
    }

    public BackupBoundaries build() {
      if (boundaries.isEmpty()) {
        long defaultBoundary = oldestStartTs - tsCleanupBuffer;
        return new BackupBoundaries(Collections.emptyMap(), defaultBoundary);
      }

      long oldestRollTs = Collections.min(boundaries.values());
      long defaultBoundary = Math.min(oldestRollTs, oldestStartTs) - tsCleanupBuffer;
      return new BackupBoundaries(boundaries, defaultBoundary);
    }
  }
}
