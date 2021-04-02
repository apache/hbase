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

import java.util.Collections;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * ReplicationSource that reads catalog WAL files -- e.g. hbase:meta WAL files -- and lets through
 * all WALEdits from these WALs. This ReplicationSource is NOT created via
 * {@link ReplicationSourceFactory}.
 */
@InterfaceAudience.Private
class CatalogReplicationSource extends ReplicationSource {
  CatalogReplicationSource() {
    // Filters in hbase:meta WAL files and allows all edits, including 'meta' edits (these are
    // filtered out in the 'super' class default implementation).
    super(p -> AbstractFSWALProvider.isMetaFile(p), Collections.emptyList());
  }

  @Override
  public void logPositionAndCleanOldLogs(WALEntryBatch entryBatch) {
    // Noop. This CatalogReplicationSource implementation does not persist state to backing storage
    // nor does it keep its WALs in a general map up in ReplicationSourceManager --
    // CatalogReplicationSource is used by the Catalog Read Replica feature which resets everytime
    // the WAL source process crashes. Skip calling through to the default implementation.
    // See "4.1 Skip maintaining zookeeper replication queue (offsets/WALs)" in the
    // design doc attached to HBASE-18070 'Enable memstore replication for meta replica for detail'
    // for background on why no need to keep WAL state.
  }
}
