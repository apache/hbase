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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceBackupCopyJob;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceBackupMergeJob;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceRestoreJob;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceRestoreToOriginalSplitsJob;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Factory implementation for backup/restore related jobs
 */
@InterfaceAudience.Private
public final class BackupRestoreFactory {
  public final static String HBASE_INCR_RESTORE_IMPL_CLASS = "hbase.incremental.restore.class";
  public final static String HBASE_BACKUP_COPY_IMPL_CLASS = "hbase.backup.copy.class";
  public final static String HBASE_BACKUP_MERGE_IMPL_CLASS = "hbase.backup.merge.class";

  private BackupRestoreFactory() {
    throw new AssertionError("Instantiating utility class...");
  }

  /**
   * Gets backup restore job
   * @param conf configuration
   * @return backup restore job instance
   */
  public static RestoreJob getRestoreJob(Configuration conf) {
    Class<? extends RestoreJob> defaultCls =
      conf.getBoolean(RestoreJob.KEEP_ORIGINAL_SPLITS_KEY, RestoreJob.KEEP_ORIGINAL_SPLITS_DEFAULT)
        ? MapReduceRestoreToOriginalSplitsJob.class
        : MapReduceRestoreJob.class;

    Class<? extends RestoreJob> cls =
      conf.getClass(HBASE_INCR_RESTORE_IMPL_CLASS, defaultCls, RestoreJob.class);
    RestoreJob service = ReflectionUtils.newInstance(cls, conf);
    service.setConf(conf);
    return service;
  }

  /**
   * Gets backup copy job
   * @param conf configuration
   * @return backup copy job instance
   */
  public static BackupCopyJob getBackupCopyJob(Configuration conf) {
    Class<? extends BackupCopyJob> cls = conf.getClass(HBASE_BACKUP_COPY_IMPL_CLASS,
      MapReduceBackupCopyJob.class, BackupCopyJob.class);
    BackupCopyJob service = ReflectionUtils.newInstance(cls, conf);
    service.setConf(conf);
    return service;
  }

  /**
   * Gets backup merge job
   * @param conf configuration
   * @return backup merge job instance
   */
  public static BackupMergeJob getBackupMergeJob(Configuration conf) {
    Class<? extends BackupMergeJob> cls = conf.getClass(HBASE_BACKUP_MERGE_IMPL_CLASS,
      MapReduceBackupMergeJob.class, BackupMergeJob.class);
    BackupMergeJob service = ReflectionUtils.newInstance(cls, conf);
    service.setConf(conf);
    return service;
  }
}
