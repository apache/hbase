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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceBackupCopyService;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceRestoreService;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.util.ReflectionUtils;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class BackupRestoreServiceFactory {

  public final static String HBASE_INCR_RESTORE_IMPL_CLASS = "hbase.incremental.restore.class";
  public final static String HBASE_BACKUP_COPY_IMPL_CLASS = "hbase.backup.copy.class";

  private BackupRestoreServiceFactory(){
    throw new AssertionError("Instantiating utility class...");
  }
  
  /**
   * Gets incremental restore service
   * @param conf - configuration
   * @return incremental backup service instance
   */
  public static IncrementalRestoreService getIncrementalRestoreService(Configuration conf) {
    Class<? extends IncrementalRestoreService> cls =
        conf.getClass(HBASE_INCR_RESTORE_IMPL_CLASS, MapReduceRestoreService.class,
          IncrementalRestoreService.class);
    return ReflectionUtils.newInstance(cls, conf);
  }
  
  /**
   * Gets backup copy service
   * @param conf - configuration
   * @return backup copy service
   */
  public static BackupCopyService getBackupCopyService(Configuration conf) {
    Class<? extends BackupCopyService> cls =
        conf.getClass(HBASE_BACKUP_COPY_IMPL_CLASS, MapReduceBackupCopyService.class,
          BackupCopyService.class);
    return ReflectionUtils.newInstance(cls, conf);
  }
}
