/**
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
  public final static String BACKUP_SYSTEM_TABLE_NAME_KEY = "hbase.backup.system.table.name";
  public final static String BACKUP_SYSTEM_TABLE_NAME_DEFAULT = "backup:system";

  public final static String BACKUP_SYSTEM_TTL_KEY = "hbase.backup.system.ttl";

  public final static int BACKUP_SYSTEM_TTL_DEFAULT = HConstants.FOREVER;
  public final static String BACKUP_ENABLE_KEY = "hbase.backup.enable";
  public final static boolean BACKUP_ENABLE_DEFAULT = false;


  public static final String BACKUP_MAX_ATTEMPTS_KEY = "hbase.backup.attempts.max";
  public static final int DEFAULT_BACKUP_MAX_ATTEMPTS = 10;

  public static final String BACKUP_ATTEMPTS_PAUSE_MS_KEY = "hbase.backup.attempts.pause.ms";
  public static final int DEFAULT_BACKUP_ATTEMPTS_PAUSE_MS = 10000;

  /*
   *  Drivers option list
   */
  public static final String OPTION_OVERWRITE = "o";
  public static final String OPTION_OVERWRITE_DESC =
      "Overwrite data if any of the restore target tables exists";

  public static final String OPTION_CHECK = "c";
  public static final String OPTION_CHECK_DESC =
      "Check restore sequence and dependencies only (does not execute the command)";

  public static final String OPTION_SET = "s";
  public static final String OPTION_SET_DESC = "Backup set name";
  public static final String OPTION_SET_RESTORE_DESC =
      "Backup set to restore, mutually exclusive with -t (table list)";
  public static final String OPTION_SET_BACKUP_DESC =
      "Backup set to backup, mutually exclusive with -t (table list)";
  public static final String OPTION_DEBUG = "d";
  public static final String OPTION_DEBUG_DESC = "Enable debug loggings";

  public static final String OPTION_TABLE = "t";
  public static final String OPTION_TABLE_DESC = "Table name. If specified, only backup images,"
      + " which contain this table will be listed.";

  public static final String OPTION_TABLE_LIST = "l";
  public static final String OPTION_TABLE_LIST_DESC = "Table name list, comma-separated.";

  public static final String OPTION_BANDWIDTH = "b";
  public static final String OPTION_BANDWIDTH_DESC = "Bandwidth per task (MapReduce task) in MB/s";

  public static final String OPTION_WORKERS = "w";
  public static final String OPTION_WORKERS_DESC = "Number of parallel MapReduce tasks to execute";

  public static final String OPTION_RECORD_NUMBER = "n";
  public static final String OPTION_RECORD_NUMBER_DESC =
      "Number of records of backup history. Default: 10";

  public static final String OPTION_PATH = "p";
  public static final String OPTION_PATH_DESC = "Backup destination root directory path";

  public static final String OPTION_TABLE_MAPPING = "m";
  public static final String OPTION_TABLE_MAPPING_DESC =
      "A comma separated list of target tables. "
          + "If specified, each table in <tables> must have a mapping";
  public static final String OPTION_YARN_QUEUE_NAME = "q";
  public static final String OPTION_YARN_QUEUE_NAME_DESC = "Yarn queue name to run backup create command on";
  public static final String OPTION_YARN_QUEUE_NAME_RESTORE_DESC = "Yarn queue name to run backup restore command on";

  public final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  public static final String BACKUP_CONFIG_STRING =  BackupRestoreConstants.BACKUP_ENABLE_KEY + "=true\n"
      + "hbase.master.logcleaner.plugins="
      +"YOUR_PLUGINS,org.apache.hadoop.hbase.backup.master.BackupLogCleaner\n"
      + "hbase.procedure.master.classes=YOUR_CLASSES,"
      +"org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager\n"
      + "hbase.procedure.regionserver.classes=YOUR_CLASSES,"
      + "org.apache.hadoop.hbase.backup.regionserver.LogRollRegionServerProcedureManager\n"
      + "hbase.coprocessor.region.classes=YOUR_CLASSES,"
      + "org.apache.hadoop.hbase.backup.BackupObserver\n"
      + "and restart the cluster\n";
  public static final String ENABLE_BACKUP = "Backup is not enabled. To enable backup, "+
      "in hbase-site.xml, set:\n "
      + BACKUP_CONFIG_STRING;

  public static final String VERIFY_BACKUP = "Please make sure that backup is enabled on the cluster. To enable backup, "+
      "in hbase-site.xml, set:\n "
      + BACKUP_CONFIG_STRING;

  /*
   *  Delimiter in table name list in restore command
   */
  public static final String TABLENAME_DELIMITER_IN_COMMAND = ",";

  public static final String CONF_STAGING_ROOT = "snapshot.export.staging.root";

  public static final String BACKUPID_PREFIX = "backup_";

  public static enum BackupCommand {
    CREATE, CANCEL, DELETE, DESCRIBE, HISTORY, STATUS, CONVERT, MERGE, STOP, SHOW, HELP, PROGRESS,
    SET, SET_ADD, SET_REMOVE, SET_DELETE, SET_DESCRIBE, SET_LIST, REPAIR
  }

}
