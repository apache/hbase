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
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * ReplicationSource that reads hbase:meta WAL files and lets through all WALEdits from these WALs.
 * It only considers meta WALs -- user-space WALs are filtered out -- and it lets through ALL edits
 * (default implementation filters OUT hbase:meta edits). Pass the hbase:meta WALProvider, NOT a
 * user-space WALProvider, else this source will produce no edits. This implementation does NOT
 * make use of backing storage intentionally; use if do not want to make use of the queue-recovery
 * feature on server crash.
 */
@InterfaceAudience.Private
class HBaseMetaNoQueueStoreReplicationSource extends ReplicationSource {
  /**
   * @param walProvider Pass the hbase:meta WALProvider!
   */
  HBaseMetaNoQueueStoreReplicationSource(WALProvider walProvider) {
    // Filters in hbase:meta WAL files and allows all edits, including 'meta' edits (these are
    // filtered out in the 'super' class default implementation).
    super(walProvider, p -> AbstractFSWALProvider.isMetaFile(p), Collections.emptyList());
  }

  @Override public boolean isQueuePersisted() {
    // Do NOT persist queues to the replication store when doing hbase:meta replication.
    return false;
  }
}
