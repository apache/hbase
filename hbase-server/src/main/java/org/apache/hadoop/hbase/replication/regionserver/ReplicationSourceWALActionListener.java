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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

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
  public void preLogRoll(Path oldPath, Path newPath) throws IOException {
    manager.preLogRoll(newPath);
  }

  @Override
  public void postLogRoll(Path oldPath, Path newPath) throws IOException {
    manager.postLogRoll(newPath);
  }

  @Override
  public void visitLogEntryBeforeWrite(WALKey logKey, WALEdit logEdit) throws IOException {
    scopeWALEdits(logKey, logEdit, conf);
  }

  /**
   * Utility method used to set the correct scopes on each log key. Doesn't set a scope on keys from
   * compaction WAL edits and if the scope is local.
   * @param logKey Key that may get scoped according to its edits
   * @param logEdit Edits used to lookup the scopes
   * @throws IOException If failed to parse the WALEdit
   */
  @VisibleForTesting
  static void scopeWALEdits(WALKey logKey, WALEdit logEdit, Configuration conf) throws IOException {
    boolean replicationForBulkLoadEnabled =
        ReplicationUtils.isReplicationForBulkLoadDataEnabled(conf);
    boolean foundOtherEdits = false;
    for (Cell cell : logEdit.getCells()) {
      if (!CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
        foundOtherEdits = true;
        break;
      }
    }

    if (!foundOtherEdits && logEdit.getCells().size() > 0) {
      WALProtos.RegionEventDescriptor maybeEvent =
          WALEdit.getRegionEventDescriptor(logEdit.getCells().get(0));
      if (maybeEvent != null &&
        (maybeEvent.getEventType() == WALProtos.RegionEventDescriptor.EventType.REGION_CLOSE)) {
        // In serially replication, we use scopes when reading close marker.
        foundOtherEdits = true;
      }
    }
    if ((!replicationForBulkLoadEnabled && !foundOtherEdits) || logEdit.isReplay()) {
      ((WALKeyImpl) logKey).serializeReplicationScope(false);
    }
  }
}
