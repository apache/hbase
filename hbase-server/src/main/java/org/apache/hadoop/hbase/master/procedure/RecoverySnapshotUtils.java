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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

/**
 * Utility class for recovery snapshot functionality, which automatically creates snapshots before
 * dropping tables, truncating tables, or deleting column families.
 */
@InterfaceAudience.Private
public class RecoverySnapshotUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RecoverySnapshotUtils.class);

  private RecoverySnapshotUtils() {

  }

  /**
   * Checks if recovery snapshots are enabled for destructive table actions.
   * @param env MasterProcedureEnv
   * @return true if recovery snapshot functionality is enabled, false otherwise
   */
  public static boolean isRecoveryEnabled(final MasterProcedureEnv env) {
    return env.getMasterConfiguration().getBoolean(
      HConstants.SNAPSHOT_BEFORE_DESTRUCTIVE_ACTION_ENABLED_KEY,
      HConstants.DEFAULT_SNAPSHOT_BEFORE_DESTRUCTIVE_ACTION_ENABLED)
      && env.getMasterConfiguration().getBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
  }

  /**
   * Gets the TTL that should be used for snapshots created before destructive schema actions.
   * Checks for table-level override first, then falls back to site configuration.
   * @param env             MasterProcedureEnv
   * @param tableDescriptor the table descriptor to check for table-level TTL override
   * @return TTL in seconds
   */
  public static long getRecoverySnapshotTtl(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor) {
    // Check table-level override first
    if (tableDescriptor != null) {
      String tableLevelTtl = tableDescriptor.getValue(HConstants.TABLE_RECOVERY_SNAPSHOT_TTL_KEY);
      if (tableLevelTtl != null) {
        try {
          long ttl = Long.parseLong(tableLevelTtl);
          LOG.debug("Using table-level recovery snapshot TTL {} seconds for table {}", ttl,
            tableDescriptor.getTableName());
          return ttl;
        } catch (NumberFormatException e) {
          LOG.warn("Invalid table-level recovery snapshot TTL '{}' for table {}, using default",
            tableLevelTtl, tableDescriptor.getTableName());
        }
      }
    }

    // Fall back to site configuration
    return env.getMasterConfiguration().getLong(
      HConstants.SNAPSHOT_BEFORE_DESTRUCTIVE_ACTION_TTL_KEY,
      HConstants.DEFAULT_SNAPSHOT_BEFORE_DESTRUCTIVE_ACTION_TTL);
  }

  /**
   * Generates a recovery snapshot name.
   * <p>
   * The naming convention is: <tt>auto_{table}_{timestamp}</tt>
   * @param tableName the table name
   * @return the generated snapshot name
   */
  public static String generateSnapshotName(final TableName tableName) {
    return generateSnapshotName(tableName, EnvironmentEdgeManager.currentTime());
  }

  /**
   * Generates a recovery snapshot name.
   * <p>
   * The naming convention is: <tt>auto_{table}_{timestamp}</tt>
   * @param tableName the table name
   * @param timestamp the timestamp when the snapshot was initiated
   * @return the generated snapshot name
   */
  public static String generateSnapshotName(final TableName tableName, final long timestamp) {
    return "auto_" + tableName.getNameAsString() + "_" + timestamp;
  }

  /**
   * Creates a SnapshotDescription for the recovery snapshot for a given operation.
   * @param tableName    the table name
   * @param snapshotName the snapshot name
   * @param ttl          the TTL for the snapshot in seconds (0 means no TTL)
   * @return SnapshotDescription for the recovery snapshot
   */
  public static SnapshotProtos.SnapshotDescription
    buildSnapshotDescription(final TableName tableName, final String snapshotName, final long ttl) {
    SnapshotProtos.SnapshotDescription.Builder builder =
      SnapshotProtos.SnapshotDescription.newBuilder();
    builder.setName(snapshotName);
    builder.setTable(tableName.getNameAsString());
    builder.setType(SnapshotProtos.SnapshotDescription.Type.FLUSH);
    builder.setCreationTime(EnvironmentEdgeManager.currentTime());
    if (ttl > 0) {
      builder.setTtl(ttl);
    }
    builder.setVersion(SnapshotDescriptionUtils.SNAPSHOT_LAYOUT_VERSION);
    return builder.build();
  }

  /**
   * Creates a SnapshotProcedure for soft drop functionality.
   * <p>
   * This method should be called from procedures that need to create a snapshot before performing
   * destructive operations. It will check for table-level TTL overrides.
   * @param env             MasterProcedureEnv
   * @param tableName       the table name
   * @param snapshotName    the name for the snapshot
   * @param tableDescriptor the table descriptor to check for table-level TTL override
   * @return SnapshotProcedure that can be added as a child procedure
   * @throws IOException if snapshot creation fails
   */
  public static SnapshotProcedure createSnapshotProcedure(final MasterProcedureEnv env,
    final TableName tableName, final String snapshotName, final TableDescriptor tableDescriptor)
    throws IOException {
    long ttl = getRecoverySnapshotTtl(env, tableDescriptor);
    SnapshotProtos.SnapshotDescription snapshotDesc =
      buildSnapshotDescription(tableName, snapshotName, ttl);
    return new SnapshotProcedure(env, snapshotDesc);
  }

  /**
   * Deletes a recovery snapshot during rollback scenarios.
   * <p>
   * This method should be called during procedure rollback to clean up any snapshots that were
   * created before the failure.
   * @param env          MasterProcedureEnv
   * @param snapshotName the name of the snapshot to delete
   * @param tableName    the table name (for logging)
   */
  public static void deleteRecoverySnapshot(final MasterProcedureEnv env, final String snapshotName,
    final TableName tableName) {
    try {
      LOG.debug("Deleting recovery snapshot {} for table {} during rollback", snapshotName,
        tableName);
      SnapshotManager snapshotManager = env.getMasterServices().getSnapshotManager();
      if (snapshotManager == null) {
        LOG.warn("SnapshotManager is not available, cannot delete recovery snapshot {}",
          snapshotName);
        return;
      }
      // Delete the snapshot using the snapshot manager. The SnapshotManager will handle existence
      // checks.
      SnapshotProtos.SnapshotDescription snapshotDesc =
        buildSnapshotDescription(tableName, snapshotName, 0);
      snapshotManager.deleteSnapshot(snapshotDesc);
      LOG.info("Successfully deleted recovery snapshot {} for table {} during rollback",
        snapshotName, tableName);
    } catch (SnapshotDoesNotExistException e) {
      // Expected during rollback if the snapshot was never created or already cleaned up.
      LOG.debug("Recovery snapshot {} for table {} does not exist, skipping", snapshotName,
        tableName);
    } catch (Exception e) {
      // During rollback, we don't want to fail the rollback process due to snapshot cleanup
      // issues. Log the error and continue. The snapshot can be manually cleaned up later.
      LOG.warn("Failed to delete recovery snapshot {} for table {} during rollback: {}. "
        + "Manual cleanup may be required.", snapshotName, tableName, e.getMessage());
    }
  }
}
