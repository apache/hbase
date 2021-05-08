/**
 *
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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.util.StringUtils;

/**
 * Class used to push numbers about the WAL into the metrics subsystem.  This will take a
 * single function call and turn it into multiple manipulations of the hadoop metrics system.
 */
@InterfaceAudience.Private
public class MetricsWAL implements WALActionsListener {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsWAL.class);

  private final MetricsWALSource source;

  public MetricsWAL() {
    this(CompatibilitySingletonFactory.getInstance(MetricsWALSource.class));
  }

  MetricsWAL(MetricsWALSource s) {
    this.source = s;
  }

  @Override
  public void postSync(final long timeInNanos, final int handlerSyncs) {
    source.incrementSyncTime(timeInNanos/1000000L);
  }

  @Override
  public void postAppend(final long size, final long time, final WALKey logkey,
      final WALEdit logEdit) throws IOException {
    TableName tableName = logkey.getTableName();
    source.incrementAppendCount(tableName);
    source.incrementAppendTime(time);
    source.incrementAppendSize(tableName, size);
    source.incrementWrittenBytes(size);

    if (time > 1000) {
      source.incrementSlowAppendCount();
      LOG.warn(String.format("%s took %d ms appending an edit to wal; len~=%s",
          Thread.currentThread().getName(),
          time,
          StringUtils.humanReadableInt(size)));
    }
  }

  @Override
  public void logRollRequested(WALActionsListener.RollRequestReason reason) {
    source.incrementLogRollRequested();
    switch (reason) {
      case ERROR:
        source.incrementErrorLogRoll();
        break;
      case LOW_REPLICATION:
        source.incrementLowReplicationLogRoll();
        break;
      case SIZE:
        source.incrementSizeLogRoll();
        break;
      case SLOW_SYNC:
        source.incrementSlowSyncLogRoll();
        break;
      default:
        break;
    }
  }

  @Override
  public void postLogRoll(Path oldPath, Path newPath) {
    // oldPath can be null if this is the first time we created a wal
    // Also newPath can be equal to oldPath if AbstractFSWAL#replaceWriter fails
    if (newPath != oldPath) {
      source.incrementSuccessfulLogRolls();
    }
  }
}
