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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * WAL reader for a serial replication peer.
 */
@InterfaceAudience.Private
public class SerialReplicationSourceWALReader extends ReplicationSourceWALReader {

  // used to store the first cell in an entry before filtering. This is because that if serial
  // replication is enabled, we may find out that an entry can not be pushed after filtering. And
  // when we try the next time, the cells maybe null since the entry has already been filtered,
  // especially for region event wal entries. And this can also used to determine whether we can
  // skip filtering.
  private Cell firstCellInEntryBeforeFiltering;

  private final SerialReplicationChecker checker;

  public SerialReplicationSourceWALReader(FileSystem fs, Configuration conf,
      ReplicationSourceLogQueue logQueue, long startPosition, WALEntryFilter filter,
      ReplicationSource source, String walGroupId) {
    super(fs, conf, logQueue, startPosition, filter, source, walGroupId);
    checker = new SerialReplicationChecker(conf, source);
  }

  @Override
  protected void readWALEntries(WALEntryStream entryStream, WALEntryBatch batch)
    throws IOException, InterruptedException {
    Path currentPath = entryStream.getCurrentPath();
    long positionBefore = entryStream.getPosition();
    for (;;) {
      Entry entry = entryStream.peek();
      boolean doFiltering = true;
      if (firstCellInEntryBeforeFiltering == null) {
        assert !entry.getEdit().isEmpty() : "should not write empty edits";
        // Used to locate the region record in meta table. In WAL we only have the table name and
        // encoded region name which can not be mapping to region name without scanning all the
        // records for a table, so we need a start key, just like what we have done at client side
        // when locating a region. For the markers, we will use the start key of the region as the
        // row key for the edit. And we need to do this before filtering since all the cells may
        // be filtered out, especially that for the markers.
        firstCellInEntryBeforeFiltering = entry.getEdit().getCells().get(0);
      } else {
        // if this is not null then we know that the entry has already been filtered.
        doFiltering = false;
      }

      if (doFiltering) {
        entry = filterEntry(entry);
      }
      if (entry != null) {
        if (!checker.canPush(entry, firstCellInEntryBeforeFiltering)) {
          if (batch.getLastWalPosition() > positionBefore) {
            // we have something that can push, break
            break;
          } else {
            checker.waitUntilCanPush(entry, firstCellInEntryBeforeFiltering);
          }
        }
        // arrive here means we can push the entry, record the last sequence id
        batch.setLastSeqId(Bytes.toString(entry.getKey().getEncodedRegionName()),
          entry.getKey().getSequenceId());
        // actually remove the entry.
        removeEntryFromStream(entryStream, batch);
        if (addEntryToBatch(batch, entry)) {
          break;
        }
      } else {
        // actually remove the entry.
        removeEntryFromStream(entryStream, batch);
      }
      boolean hasNext = entryStream.hasNext();
      // always return if we have switched to a new file.
      if (switched(entryStream, currentPath)) {
        batch.setEndOfFile(true);
        break;
      }
      if (!hasNext) {
        break;
      }
    }
  }

  private void removeEntryFromStream(WALEntryStream entryStream, WALEntryBatch batch)
      throws IOException {
    entryStream.next();
    firstCellInEntryBeforeFiltering = null;
    batch.setLastWalPosition(entryStream.getPosition());
  }
}
