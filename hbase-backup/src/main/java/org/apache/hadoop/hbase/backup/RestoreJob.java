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

import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Restore operation job interface Concrete implementation is provided by backup provider, see
 * {@link BackupRestoreFactory}
 */

@InterfaceAudience.Private
public interface RestoreJob extends Configurable {

  String KEEP_ORIGINAL_SPLITS_KEY = "hbase.backup.restorejob.keep.original.splits";
  boolean KEEP_ORIGINAL_SPLITS_DEFAULT = false;

  /**
   * Run restore operation
   * @param dirPaths          path array of WAL log directories
   * @param fromTables        from tables
   * @param restoreRootDir    output file system
   * @param toTables          to tables
   * @param fullBackupRestore full backup restore
   * @throws IOException if running the job fails
   */
  void run(Path[] dirPaths, TableName[] fromTables, Path restoreRootDir, TableName[] toTables,
    boolean fullBackupRestore) throws IOException;
}
