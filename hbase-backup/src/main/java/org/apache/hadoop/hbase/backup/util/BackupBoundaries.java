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
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.wal.WAL;
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
  private static final BackupBoundaries EMPTY_BOUNDARIES =
    new BackupBoundaries(Collections.emptyMap(), Long.MAX_VALUE);

  // This map tracks, for every RegionServer, the least recent (= oldest / lowest timestamp)
  // inclusion in any backup. In other words, it is the timestamp boundary up to which all backup
  // roots have included the WAL in their backup.
  private final Map<Address, Long> boundaries;

  // The minimum WAL roll timestamp from the most recent backup of each backup root, used as a
  // fallback cleanup boundary for RegionServers without explicit backup boundaries (e.g., servers
  // that joined after backups began)
  private final long oldestStartCode;

  private BackupBoundaries(Map<Address, Long> boundaries, long oldestStartCode) {
    this.boundaries = boundaries;
    this.oldestStartCode = oldestStartCode;
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
      long pathTs = WAL.getTimestamp(walLogPath.getName());

      if (!boundaries.containsKey(address)) {
        boolean isDeletable = pathTs <= oldestStartCode;
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Boundary for {} not found. isDeletable = {} based on oldestStartCode = {} and WAL ts of {}",
            walLogPath, isDeletable, oldestStartCode, pathTs);
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

  public long getOldestStartCode() {
    return oldestStartCode;
  }

  public static BackupBoundariesBuilder builder(long tsCleanupBuffer) {
    return new BackupBoundariesBuilder(tsCleanupBuffer);
  }

  public static class BackupBoundariesBuilder {
    private final Map<Address, Long> boundaries = new HashMap<>();
    private final long tsCleanupBuffer;

    private long oldestStartCode = Long.MAX_VALUE;

    private BackupBoundariesBuilder(long tsCleanupBuffer) {
      this.tsCleanupBuffer = tsCleanupBuffer;
    }

    public BackupBoundariesBuilder addBackupTimestamps(String host, long hostLogRollTs,
      long backupStartCode) {
      Address address = Address.fromString(host);
      Long storedTs = boundaries.get(address);
      if (storedTs == null || hostLogRollTs < storedTs) {
        boundaries.put(address, hostLogRollTs);
      }

      if (oldestStartCode > backupStartCode) {
        oldestStartCode = backupStartCode;
      }

      return this;
    }

    public BackupBoundaries build() {
      if (boundaries.isEmpty()) {
        return EMPTY_BOUNDARIES;
      }

      oldestStartCode -= tsCleanupBuffer;
      return new BackupBoundaries(boundaries, oldestStartCode);
    }
  }
}
