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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * HConstants holds a bunch of HBase Backup and Restore constants
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class BackupRestoreConstants {

  // constants for znode data keys in backup znode
  public static final String BACKUP_PROGRESS = "progress";
  public static final String BACKUP_START_TIME = "startTs";
  public static final String BACKUP_INPROGRESS_PHASE = "phase";
  public static final String BACKUP_COMPLETE_TIME = "completeTs";
  public static final String BACKUP_FAIL_TIME = "failedTs";
  public static final String BACKUP_FAIL_PHASE = "failedphase";
  public static final String BACKUP_FAIL_MSG = "failedmessage";
  public static final String BACKUP_ROOT_PATH = "targetRootDir";
  public static final String BACKUP_REQUEST_TABLE_LIST = "tablelist";
  public static final String BACKUP_REQUEST_TYPE = "type";
  public static final String BACKUP_BYTES_COPIED = "bytescopied";
  public static final String BACKUP_ANCESTORS = "ancestors";
  public static final String BACKUP_EXISTINGSNAPSHOT = "snapshot";

  public static final String BACKUP_TYPE_FULL = "full";
  public static final String BACKUP_TYPE_INCR = "incremental";

  // delimiter in tablename list in restore command
  public static final String TABLENAME_DELIMITER_IN_COMMAND = ",";

  // delimiter in znode data
  public static final String ZNODE_DATA_DELIMITER = ",";

  public static final String CONF_STAGING_ROOT = "snapshot.export.staging.root";

  public static final String BACKUPID_PREFIX = "backup_";

  public static enum BACKUP_COMMAND {
    CREATE, CANCEL, DELETE, DESCRIBE, HISTORY, STATUS, CONVERT, MERGE, STOP, SHOW, HELP,
  }

  private BackupRestoreConstants() {
    // Can't be instantiated with this ctor.
  }
}
