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
package org.apache.hadoop.hbase.backup;

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_WAL_DIR;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.LONG_OPTION_PITR_BACKUP_PATH;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.LONG_OPTION_TO_DATETIME;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_PITR_BACKUP_PATH;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_PITR_BACKUP_PATH_DESC;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TO_DATETIME;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TO_DATETIME_DESC;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Strings;

/**
 * Command-line entry point for restore operation
 */
@InterfaceAudience.Private
public class PointInTimeRestoreDriver extends AbstractRestoreDriver {
  private static final String USAGE_STRING = """
      Usage: hbase pitr [options]
        <backup_path>   Backup Path to use for Point in Time Restore
        table(s)        Comma-separated list of tables to restore
      """;

  @Override
  protected int executeRestore(boolean check, TableName[] fromTables, TableName[] toTables,
    boolean isOverwrite) {
    String walBackupDir = getConf().get(CONF_CONTINUOUS_BACKUP_WAL_DIR);
    if (Strings.isNullOrEmpty(walBackupDir)) {
      System.err.printf(
        "Point-in-Time Restore requires the WAL backup directory (%s) to replay logs after full and incremental backups. "
          + "Set this property if you need Point-in-Time Restore. Otherwise, use the normal restore process with the appropriate backup ID.%n",
        CONF_CONTINUOUS_BACKUP_WAL_DIR);
      return -1;
    }

    String[] remainArgs = cmd.getArgs();
    if (remainArgs.length != 0) {
      printToolUsage();
      return -1;
    }

    String backupRootDir = cmd.getOptionValue(OPTION_PITR_BACKUP_PATH);

    try (final Connection conn = ConnectionFactory.createConnection(conf);
      BackupAdmin client = new BackupAdminImpl(conn)) {
      // Get the replication checkpoint (last known safe point for Continuous Backup)
      long replicationCheckpoint = BackupUtils.getReplicationCheckpoint(conn);
      long endTime = replicationCheckpoint;

      if (cmd.hasOption(OPTION_TO_DATETIME)) {
        String time = cmd.getOptionValue(OPTION_TO_DATETIME);
        try {
          endTime = Long.parseLong(time);
          // Convert seconds to milliseconds if input is in seconds
          if (endTime < 10_000_000_000L) {
            endTime *= 1000;
          }
        } catch (NumberFormatException e) {
          System.out.println("ERROR: Invalid timestamp format for --to-datetime: " + time);
          printToolUsage();
          return -5;
        }
      }

      // Ensure the requested restore time does not exceed the replication checkpoint
      if (endTime > replicationCheckpoint) {
        LOG.error(
          "ERROR: Requested restore time ({}) exceeds the last known safe replication checkpoint ({}). "
            + "Please choose a time before this checkpoint to ensure data consistency.",
          endTime, replicationCheckpoint);
        return -5;
      }

      PointInTimeRestoreRequest pointInTimeRestoreRequest = new PointInTimeRestoreRequest.Builder()
        .withBackupRootDir(backupRootDir).withCheck(check).withFromTables(fromTables)
        .withToTables(toTables).withOverwrite(isOverwrite).withToDateTime(endTime).build();

      client.pointInTimeRestore(pointInTimeRestoreRequest);
    } catch (Exception e) {
      LOG.error("Error while running restore backup", e);
      return -5;
    }
    return 0;
  }

  @Override
  protected void addOptions() {
    super.addOptions();
    addOptWithArg(OPTION_TO_DATETIME, LONG_OPTION_TO_DATETIME, OPTION_TO_DATETIME_DESC);
    addOptWithArg(OPTION_PITR_BACKUP_PATH, LONG_OPTION_PITR_BACKUP_PATH,
      OPTION_PITR_BACKUP_PATH_DESC);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Path rootDir = CommonFSUtils.getRootDir(conf);
    URI defaultFs = rootDir.getFileSystem(conf).getUri();
    CommonFSUtils.setFsDefault(conf, new Path(defaultFs));
    int ret = ToolRunner.run(conf, new PointInTimeRestoreDriver(), args);
    System.exit(ret);
  }

  @Override
  protected String getUsageString() {
    return USAGE_STRING;
  }
}
