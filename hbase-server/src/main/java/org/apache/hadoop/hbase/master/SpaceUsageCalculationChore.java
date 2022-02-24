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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableState.State;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  This chore is used to calculate the space usage information for archive dir, old wal dir
 *  and disabled tables.
 */
@InterfaceAudience.Private
public class SpaceUsageCalculationChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(SpaceUsageCalculationChore.class);

  private static final String SPACE_USED_CHORE_INTERVAL = "hbase.master.space.used.chore.interval";
  private static final int DEFAULT_SPACE_USED_CHORE_INTERVAL = 60 * 1000;

  private final Map<TableName, Long> disabledTableSpaceUsed = new HashMap<>();
  private volatile long archiveDirSize = 0;
  private volatile long oldWALDirSize = 0;

  private final MasterServices master;

  public SpaceUsageCalculationChore(MasterServices master) {
    super("SpaceUsageCalculationChore", master, master.getConfiguration()
      .getInt(SPACE_USED_CHORE_INTERVAL, DEFAULT_SPACE_USED_CHORE_INTERVAL));
    this.master = master;
  }

  @Override
  protected synchronized void chore() {
    disabledTableSpaceUsed.clear();
    Configuration conf = master.getConfiguration();
    FileSystem fs = master.getFileSystem();
    if (fs == null) {
      return;
    }

    try {
      Set<TableName> disabledTables =
        master.getTableStateManager().getTablesInStates(State.DISABLED);
      Path rootDir = CommonFSUtils.getRootDir(conf);
      for (TableName tableName : disabledTables) {
        Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
        disabledTableSpaceUsed.put(tableName, fs.getUsed(tableDir));
      }
      archiveDirSize = fs.getUsed(HFileArchiveUtil.getArchivePath(conf));
      oldWALDirSize = fs.getUsed(master.getMasterWalManager().getOldLogDir());
    } catch (IOException e) {
      LOG.warn("Failed calculate space usage", e);
      // if unexpected exception happens, reset the metric
      disabledTableSpaceUsed.clear();
      archiveDirSize = 0;
      oldWALDirSize = 0;
    }
  }

  public synchronized Map<TableName, Long> getDisabledTableSpaceUsage() {
    return Collections.unmodifiableMap(disabledTableSpaceUsed);
  }

  public long getArchiveDirSize() {
    return archiveDirSize;
  }

  public long getOldWALDirSize() {
    return oldWALDirSize;
  }
}
