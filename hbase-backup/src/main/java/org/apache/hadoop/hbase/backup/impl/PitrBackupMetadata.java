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
package org.apache.hadoop.hbase.backup.impl;

import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A unified abstraction over backup metadata used during Point-In-Time Restore (PITR).
 * <p>
 * This interface allows the PITR algorithm to operate uniformly over different types of backup
 * metadata sources, such as {@link BackupInfo} (system table) and {@link BackupImage} (custom
 * backup location), without knowing their specific implementations.
 */
@InterfaceAudience.Private
public interface PitrBackupMetadata {

  /** Returns List of table names included in the backup */
  List<TableName> getTableNames();

  /** Returns Start timestamp of the backup */
  long getStartTs();

  /** Returns Completion timestamp of the backup */
  long getCompleteTs();

  /** Returns Unique identifier for the backup */
  String getBackupId();

  /** Returns Root directory where the backup is stored */
  String getRootDir();

  /** Returns backup type */
  BackupType getType();

  /** Returns incrCommittedWalTs */
  long getIncrCommittedWalTs();
}
