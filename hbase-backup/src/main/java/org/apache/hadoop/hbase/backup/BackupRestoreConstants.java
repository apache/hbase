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

import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * BackupRestoreConstants holds a bunch of HBase Backup and Restore constants
 */
@InterfaceAudience.Private
public interface BackupRestoreConstants {
  /*
   * Backup/Restore constants
   */
  String BACKUP_SYSTEM_TABLE_NAME_KEY = "hbase.backup.system.table.name";
  String BACKUP_SYSTEM_TABLE_NAME_DEFAULT = "backup:system";

  String BACKUP_SYSTEM_TTL_KEY = "hbase.backup.system.ttl";

  int BACKUP_SYSTEM_TTL_DEFAULT = HConstants.FOREVER;
  String BACKUP_ENABLE_KEY = "hbase.backup.enable";
  boolean BACKUP_ENABLE_DEFAULT = false;

  String BACKUP_MAX_ATTEMPTS_KEY = "hbase.backup.attempts.max";
  int DEFAULT_BACKUP_MAX_ATTEMPTS = 10;

  String BACKUP_ATTEMPTS_PAUSE_MS_KEY = "hbase.backup.attempts.pause.ms";
  int DEFAULT_BACKUP_ATTEMPTS_PAUSE_MS = 10000;

  /*
   * Drivers option list
   */
  String OPTION_OVERWRITE = "o";
  String OPTION_OVERWRITE_DESC = "Overwrite data if any of the restore target tables exists";

  String OPTION_CHECK = "c";
  String OPTION_CHECK_DESC =
    "Check restore sequence and dependencies only (does not execute the command)";

  String OPTION_SET = "s";
  String OPTION_SET_DESC = "Backup set name";
  String OPTION_SET_RESTORE_DESC = "Backup set to restore, mutually exclusive with -t (table list)";
  String OPTION_SET_BACKUP_DESC = "Backup set to backup, mutually exclusive with -t (table list)";
  String OPTION_DEBUG = "d";
  String OPTION_DEBUG_DESC = "Enable debug loggings";

  String OPTION_TABLE = "t";
  String OPTION_TABLE_DESC =
    "Table name. If specified, only backup images, which contain this table will be listed.";

  String OPTION_LIST = "l";
  String OPTION_TABLE_LIST_DESC = "Table name list, comma-separated.";
  String OPTION_BACKUP_LIST_DESC = "Backup ids list, comma-separated.";

  String OPTION_BANDWIDTH = "b";
  String OPTION_BANDWIDTH_DESC = "Bandwidth per task (MapReduce task) in MB/s";

  String OPTION_WORKERS = "w";
  String OPTION_WORKERS_DESC = "Number of parallel MapReduce tasks to execute";

  String OPTION_IGNORECHECKSUM = "i";
  String OPTION_IGNORECHECKSUM_DESC =
    "Ignore checksum verify between source snapshot and exported snapshot."
      + " Especially when the source and target file system types are different,"
      + " we should use -i option to skip checksum-checks.";

  String OPTION_RECORD_NUMBER = "n";
  String OPTION_RECORD_NUMBER_DESC = "Number of records of backup history. Default: 10";

  String OPTION_PATH = "p";
  String OPTION_PATH_DESC = "Backup destination root directory path";

  String OPTION_KEEP = "k";
  String OPTION_KEEP_DESC = "Specifies maximum age of backup (in days) to keep during bulk delete";

  String OPTION_TABLE_MAPPING = "m";
  String OPTION_TABLE_MAPPING_DESC = "A comma separated list of target tables. "
    + "If specified, each table in <tables> must have a mapping";
  String OPTION_YARN_QUEUE_NAME = "q";
  String OPTION_YARN_QUEUE_NAME_DESC = "Yarn queue name to run backup create command on";
  String OPTION_YARN_QUEUE_NAME_RESTORE_DESC = "Yarn queue name to run backup restore command on";

  String OPTION_ENABLE_CONTINUOUS_BACKUP = "cb";
  String LONG_OPTION_ENABLE_CONTINUOUS_BACKUP = "continuous-backup-enabled";
  String OPTION_ENABLE_CONTINUOUS_BACKUP_DESC =
    "Flag indicating that the full backup is part of a continuous backup process.";

  String OPTION_FORCE_DELETE = "fd";
  String LONG_OPTION_FORCE_DELETE = "force-delete";
  String OPTION_FORCE_DELETE_DESC =
    "Flag to forcefully delete the backup, even if it may be required for Point-in-Time Restore";

  String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  String BACKUP_CONFIG_STRING =
    BackupRestoreConstants.BACKUP_ENABLE_KEY + "=true\n" + "hbase.master.logcleaner.plugins="
      + "YOUR_PLUGINS,org.apache.hadoop.hbase.backup.master.BackupLogCleaner\n"
      + "hbase.procedure.master.classes=YOUR_CLASSES,"
      + "org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager\n"
      + "hbase.procedure.regionserver.classes=YOUR_CLASSES,"
      + "org.apache.hadoop.hbase.backup.regionserver.LogRollRegionServerProcedureManager\n"
      + "hbase.coprocessor.region.classes=YOUR_CLASSES,"
      + "org.apache.hadoop.hbase.backup.BackupObserver\n" + "and restart the cluster\n"
      + "For more information please see http://hbase.apache.org/book.html#backuprestore\n";
  String ENABLE_BACKUP = "Backup is not enabled. To enable backup, " + "in hbase-site.xml, set:\n "
    + BACKUP_CONFIG_STRING;

  String VERIFY_BACKUP = "To enable backup, in hbase-site.xml, set:\n " + BACKUP_CONFIG_STRING;

  /*
   * Delimiter in table name list in restore command
   */
  String TABLENAME_DELIMITER_IN_COMMAND = ",";

  String CONF_STAGING_ROOT = "snapshot.export.staging.root";

  String BACKUPID_PREFIX = "backup_";

  String CONTINUOUS_BACKUP_REPLICATION_PEER = "continuous_backup_replication_peer";

  String DEFAULT_CONTINUOUS_BACKUP_REPLICATION_ENDPOINT =
    "org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint";

  String CONF_CONTINUOUS_BACKUP_WAL_DIR = "hbase.backup.continuous.wal.dir";

  String CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS = "hbase.backup.continuous.pitr.window.days";
  long DEFAULT_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS = 30;

  enum BackupCommand {
    CREATE,
    CANCEL,
    DELETE,
    DESCRIBE,
    HISTORY,
    STATUS,
    CONVERT,
    MERGE,
    STOP,
    SHOW,
    HELP,
    PROGRESS,
    SET,
    SET_ADD,
    SET_REMOVE,
    SET_DELETE,
    SET_DESCRIBE,
    SET_LIST,
    REPAIR
  }
}
