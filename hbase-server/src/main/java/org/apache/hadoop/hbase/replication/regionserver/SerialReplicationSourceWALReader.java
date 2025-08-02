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
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.replication.ScopeWALEntryFilter;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

/**
 * WAL reader for a serial replication peer.
 */
@InterfaceAudience.Private
public class SerialReplicationSourceWALReader extends ReplicationSourceWALReader {

  private static final Logger LOG = LoggerFactory.getLogger(SerialReplicationSourceWALReader.class);

  // used to store the first cell in an entry before filtering. This is because that if serial
  // replication is enabled, we may find out that an entry can not be pushed after filtering. And
  // when we try the next time, the cells maybe null since the entry has already been filtered,
  // especially for region event wal entries. And this can also used to determine whether we can
  // skip filtering.
  private Cell firstCellInEntryBeforeFiltering;

  private String encodedRegionName;

  private long sequenceId = HConstants.NO_SEQNUM;

  private final SerialReplicationChecker checker;

  public SerialReplicationSourceWALReader(FileSystem fs, Configuration conf,
    ReplicationSourceLogQueue logQueue, long startPosition, WALEntryFilter filter,
    ReplicationSource source, String walGroupId) {
    super(fs, conf, logQueue, startPosition, filter, source, walGroupId);
    checker = new SerialReplicationChecker(conf, source);
  }

  // Under some special cases, we may filter out some entries but we still need to record the last
  // pushed sequence id for these entries. For example, when we setup a bidirection replication A
  // <-> B, if we write to both cluster A and cluster B, cluster A will not replicate the entries
  // which are replicated from cluster B, which means we may have holes in the replication sequence
  // ids. So if the region is closed abnormally, i.e, we do not have a close event for the region,
  // and before the closeing, we have some entries from cluster B, then the replication from cluster
  // A to cluster B will be stuck if we do not record the last pushed sequence id of these entries
  // because we will find out that the previous sequence id range will never finish. So we need to
  // record the sequence id for these entries so the last pushed sequence id can reach the region
  // barrier.
  // So here we need to determine whether the entry has something need to replicated only through
  // the replication scope, if so, we need to record the last pushed sequence id, no matter whether
  // it will be filtered out later.
  // Please see HBASE-29463
  private boolean hasGlobalScope(Entry entry) {
    NavigableMap<byte[], Integer> scopes = entry.getKey().getReplicationScopes();
    if (MapUtils.isEmpty(scopes)) {
      return false;
    }
    for (byte[] family : entry.getEdit().getFamilies()) {
      if (ScopeWALEntryFilter.hasGlobalScope(scopes, family)) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected void readWALEntries(WALEntryStream entryStream, WALEntryBatch batch)
    throws InterruptedException {
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
        if (hasGlobalScope(entry)) {
          // this is for recording last pushed sequence id, see the above comments for
          // hasGlobalScope
          encodedRegionName = Bytes.toString(entry.getKey().getEncodedRegionName());
          sequenceId = entry.getKey().getSequenceId();
        }
      } else {
        // if this is not null then we know that the entry has already been filtered.
        doFiltering = false;
      }

      if (doFiltering) {
        entry = filterEntry(entry);
      }
      if (entry != null) {
        int sleepMultiplier = 1;
        try {
          if (!checker.canPush(entry, firstCellInEntryBeforeFiltering)) {
            if (batch.getLastWalPosition() > positionBefore) {
              // we have something that can push, break
              break;
            } else {
              checker.waitUntilCanPush(entry, firstCellInEntryBeforeFiltering);
            }
          }
        } catch (IOException e) {
          LOG.warn("failed to check whether we can push the WAL entries", e);
          if (batch.getLastWalPosition() > positionBefore) {
            // we have something that can push, break
            break;
          }
          sleepMultiplier = sleep(sleepMultiplier);
        }
        // actually remove the entry.
        removeEntryFromStream(entryStream, batch);
        if (addEntryToBatch(batch, entry)) {
          break;
        }
      } else {
        // actually remove the entry.
        removeEntryFromStream(entryStream, batch);
      }
      WALEntryStream.HasNext hasNext = entryStream.hasNext();
      // always return if we have switched to a new file.
      if (switched(entryStream, currentPath)) {
        batch.setEndOfFile(true);
        break;
      }
      if (hasNext != WALEntryStream.HasNext.YES) {
        // For hasNext other than YES, it is OK to just retry.
        // As for RETRY and RETRY_IMMEDIATELY, the correct action is to retry, and for NO, it will
        // return NO again when you call the method next time, so it is OK to just return here and
        // let the loop in the upper layer to call hasNext again.
        break;
      }
    }
  }

  private void removeEntryFromStream(WALEntryStream entryStream, WALEntryBatch batch) {
    entryStream.next();
    batch.setLastWalPosition(entryStream.getPosition());
    // record last pushed sequence id if needed
    if (encodedRegionName != null) {
      batch.setLastSeqId(encodedRegionName, sequenceId);
      encodedRegionName = null;
    }
    firstCellInEntryBeforeFiltering = null;
  }
}
