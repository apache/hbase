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
  private static final BackupBoundaries EMPTY_BOUNDARIES =
    new BackupBoundaries(Collections.emptyMap());

  // Tracks WAL cleanup boundaries separately for each backup root to ensure WALs are only deleted
  // when ALL backup roots no longer need them. The map key is the backup ID of the most recent
  // backup from each backup root. Each BoundaryInfo contains: (1) a map of WAL timestamp
  // boundaries per RegionServer (the oldest WAL timestamp included in that backup root's most
  // recent backup), and (2) an oldestStartCode used as a fallback boundary for RegionServers not
  // explicitly tracked (e.g., servers that joined after the backup began). A WAL file can only be
  // deleted if it's older than the boundary for ALL backup roots, protecting WALs needed by any
  // backup root even when other roots have already backed up that host at a later timestamp.
  private final Map<String, BoundaryInfo> boundaries;

  private BackupBoundaries(Map<String, BoundaryInfo> boundaries) {
    this.boundaries = boundaries;
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

      for (Map.Entry<String, BoundaryInfo> entry : boundaries.entrySet()) {
        String backupId = entry.getKey();
        BoundaryInfo boundary = entry.getValue();
        DeleteStatus status = boundary.getStatus(address, pathTs);
        if (!status.isDeletable()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Backup {} preventing deletion of {} with ts of {} due to {}", backupId,
              walLogPath, pathTs, status);
          }
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      LOG.warn("Error occurred while filtering file: {}. Ignoring cleanup of this log", walLogPath,
        e);
      return false;
    }
  }

  public Map<String, BoundaryInfo> getBoundaries() {
    return boundaries;
  }

  public static BackupBoundariesBuilder builder(long tsCleanupBuffer) {
    return new BackupBoundariesBuilder(tsCleanupBuffer);
  }

  public static class BackupBoundariesBuilder {
    private final Map<String, BoundaryInfo> boundaries = new HashMap<>();
    private final long tsCleanupBuffer;

    private BackupBoundariesBuilder(long tsCleanupBuffer) {
      this.tsCleanupBuffer = tsCleanupBuffer;
    }

    public BackupBoundariesBuilder addBackupTimestamps(String backupId, String host,
      long hostLogRollTs, long backupStartCode) {
      BoundaryInfo boundary = boundaries.computeIfAbsent(backupId, ignore -> new BoundaryInfo());
      Address address = Address.fromString(host);
      boundary.add(address, hostLogRollTs, backupStartCode);
      return this;
    }

    public BackupBoundaries build() {
      if (boundaries.isEmpty()) {
        return EMPTY_BOUNDARIES;
      }

      for (BoundaryInfo boundary : boundaries.values()) {
        boundary.oldestStartCode -= tsCleanupBuffer;
      }

      return new BackupBoundaries(boundaries);
    }
  }

  public static class BoundaryInfo {
    private final Map<Address, Long> boundaries = new HashMap<>();
    private long oldestStartCode = Long.MAX_VALUE;

    public Map<Address, Long> getBoundaries() {
      return boundaries;
    }

    public long getOldestStartCode() {
      return oldestStartCode;
    }

    public void add(Address address, long hostLogRollTs, long backupStartCode) {
      Long storedTs = boundaries.get(address);
      if (storedTs == null || hostLogRollTs < storedTs) {
        boundaries.put(address, hostLogRollTs);
      }

      if (oldestStartCode > backupStartCode) {
        oldestStartCode = backupStartCode;
      }
    }

    private DeleteStatus getStatus(Address address, long hostLogRollTs) {
      Long storedTs = boundaries.get(address);

      if (storedTs == null) {
        return hostLogRollTs <= oldestStartCode
          ? DeleteStatus.OK
          : DeleteStatus.NOT_DELETABLE_START_CODE;
      }

      return hostLogRollTs <= storedTs ? DeleteStatus.OK : DeleteStatus.NOT_DELETABLE_BOUNDARY;
    }
  }

  private enum DeleteStatus {
    OK(true),
    NOT_DELETABLE_START_CODE(false),
    NOT_DELETABLE_BOUNDARY(false);

    private final boolean deletable;

    DeleteStatus(boolean deletable) {
      this.deletable = deletable;
    }

    public boolean isDeletable() {
      return deletable;
    }
  }
}
