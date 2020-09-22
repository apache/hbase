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
 * ReplicationSource that reads hbase:meta WAL files and lets through all WALEdits from these WALs.
 * NOT created via {@link ReplicationSourceFactory} -- specialized.
 */
@InterfaceAudience.Private
class HBaseMetaReplicationSource extends ReplicationSource {
  HBaseMetaReplicationSource() {
    // Filters in hbase:meta WAL files and allows all edits, including 'meta' edits (these are
    // filtered out in the 'super' class default implementation).
    super(p -> AbstractFSWALProvider.isMetaFile(p), Collections.emptyList());
  }

  @Override
  public void logPositionAndCleanOldLogs(WALEntryBatch entryBatch) {
    // Noop. This implementation does not persist state to backing storage nor does it keep its
    // WALs in a general map up in ReplicationSourceManager from it has to help maintain.
  }
}
