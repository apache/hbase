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
  private static final BackupBoundaries EMPTY_BOUNDARIES =
    new BackupBoundaries(Collections.emptyMap());

  // Tracks WAL cleanup boundaries separately for each backup root to ensure WALs are only deleted
  // when ALL backup roots no longer need them. The map key is the backup ID of the most recent
  // backup from each backup root. Each BoundaryInfo contains: (1) a map of WAL timestamp
  // boundaries per RegionServer (the oldest WAL timestamp included in that backup root's most
  // recent backup), and (2) a defaultBoundary used as a fallback boundary for RegionServers not
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
        DeletionEligibility eligibility = boundary.getDeletionEligibility(address, pathTs);
        if (!eligibility.isEligible()) {
          LOG.debug("Backup {} preventing deletion of {} with ts of {} due to {}", backupId,
            walLogPath, pathTs, eligibility);
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
    private final Map<String, BoundaryInfoBuilder> boundaries = new HashMap<>();
    private final long tsCleanupBuffer;

    private BackupBoundariesBuilder(long tsCleanupBuffer) {
      this.tsCleanupBuffer = tsCleanupBuffer;
    }

    /**
     * Updates the boundaries based on the provided backup info.
     * @param backupInfo the most recent completed backup info for a backup root, or if there is no
     *                   such completed backup, the currently running backup.
     */
    public void update(BackupInfo backupInfo) {
      boundaries.computeIfAbsent(backupInfo.getBackupId(), ignore -> new BoundaryInfoBuilder())
        .update(backupInfo);
    }

    public BackupBoundaries build() {
      if (boundaries.isEmpty()) {
        return EMPTY_BOUNDARIES;
      }

      Map<String, BoundaryInfo> built = new HashMap<>();
      boundaries
        .forEach((backupId, builder) -> built.put(backupId, builder.build(tsCleanupBuffer)));
      return new BackupBoundaries(built);
    }
  }

  private static class BoundaryInfoBuilder {
    private final Map<Address, Long> boundaries = new HashMap<>();
    private long oldestStartTs = Long.MAX_VALUE;

    void update(BackupInfo backupInfo) {
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

    BoundaryInfo build(long tsCleanupBuffer) {
      // The buffer is only applied to the default boundary, intentionally. A per-host boundary is
      // that RegionServer's own log-roll timestamp and is compared against WALs from the same host,
      // so both come from one clock. The default boundary is compared against WALs of other hosts,
      // or derived from a backup's start time, so it needs a margin for clock skew across machines.
      long defaultBoundary;
      if (boundaries.isEmpty()) {
        defaultBoundary = oldestStartTs - tsCleanupBuffer;
      } else {
        long oldestRollTs = Collections.min(boundaries.values());
        defaultBoundary = Math.min(oldestRollTs, oldestStartTs) - tsCleanupBuffer;
      }
      return new BoundaryInfo(Collections.unmodifiableMap(boundaries), defaultBoundary);
    }
  }

  public static class BoundaryInfo {
    private final Map<Address, Long> boundaries;
    private final long defaultBoundary;

    private BoundaryInfo(Map<Address, Long> boundaries, long defaultBoundary) {
      this.boundaries = boundaries;
      this.defaultBoundary = defaultBoundary;
    }

    public Map<Address, Long> getBoundaries() {
      return boundaries;
    }

    public long getDefaultBoundary() {
      return defaultBoundary;
    }

    private DeletionEligibility getDeletionEligibility(Address address, long hostLogRollTs) {
      Long storedTs = boundaries.get(address);

      if (storedTs == null) {
        return hostLogRollTs <= defaultBoundary
          ? DeletionEligibility.ELIGIBLE
          : DeletionEligibility.PROTECTED_BY_DEFAULT_BOUNDARY;
      }

      return hostLogRollTs <= storedTs
        ? DeletionEligibility.ELIGIBLE
        : DeletionEligibility.PROTECTED_BY_HOST_BOUNDARY;
    }
  }

  private enum DeletionEligibility {
    ELIGIBLE(true),
    PROTECTED_BY_DEFAULT_BOUNDARY(false),
    PROTECTED_BY_HOST_BOUNDARY(false);

    private final boolean eligible;

    DeletionEligibility(boolean eligible) {
      this.eligible = eligible;
    }

    public boolean isEligible() {
      return eligible;
    }
  }
}
