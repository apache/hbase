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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to receive new wals.
 */
@InterfaceAudience.Private
class ReplicationSourceWALActionListener implements WALActionsListener {

  private final Configuration conf;

  private final ReplicationSourceManager manager;

  public ReplicationSourceWALActionListener(Configuration conf, ReplicationSourceManager manager) {
    this.conf = conf;
    this.manager = manager;
  }

  @Override
  public void postLogRoll(Path oldPath, Path newPath) throws IOException {
    manager.postLogRoll(newPath);
  }

  @Override
  public void postAppend(final long size, final long time, final WALKey logkey,
    final WALEdit logEdit) throws IOException {
    if (conf.getBoolean("hbase.replication.walAppendsize.metric.enabled", true)) {
      manager.postAppend(size, time, logkey, logEdit);
    }
  }

  @Override
  public void visitLogEntryBeforeWrite(RegionInfo info, WALKey logKey, WALEdit logEdit) {
    scopeWALEdits(logKey, logEdit, conf);
  }

  /**
   * Utility method used to set the correct scopes on each log key. Doesn't set a scope on keys from
   * compaction WAL edits and if the scope is local.
   * @param logKey  Key that may get scoped according to its edits
   * @param logEdit Edits used to lookup the scopes
   */
  static void scopeWALEdits(WALKey logKey, WALEdit logEdit, Configuration conf) {
    // For bulk load replication we need meta family to know the file we want to replicate.
    if (ReplicationUtils.isReplicationForBulkLoadDataEnabled(conf)) {
      return;
    }
    // Allow replication marker row to pass through.
    if (WALEdit.isReplicationMarkerEdit(logEdit)) {
      return;
    }
    // For replay, or if all the cells are markers, do not need to store replication scope.
    if (
      logEdit.isReplay() || logEdit.getCells().stream().allMatch(c -> WALEdit.isMetaEditFamily(c))
    ) {
      ((WALKeyImpl) logKey).clearReplicationScope();
    }
  }
}
