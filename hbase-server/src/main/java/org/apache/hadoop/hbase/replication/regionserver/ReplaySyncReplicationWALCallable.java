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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCellScanner;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.hadoop.hbase.protobuf.ReplicationProtobufUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALHeaderEOFException;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALStreamReader;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReplaySyncReplicationWALParameter;

/**
 * This callable executed at RS side to replay sync replication wal.
 */
@InterfaceAudience.Private
public class ReplaySyncReplicationWALCallable extends BaseRSProcedureCallable {

  private static final Logger LOG = LoggerFactory.getLogger(ReplaySyncReplicationWALCallable.class);

  private static final String REPLAY_SYNC_REPLICATION_WAL_BATCH_SIZE =
    "hbase.replay.sync.replication.wal.batch.size";

  private static final long DEFAULT_REPLAY_SYNC_REPLICATION_WAL_BATCH_SIZE = 8 * 1024 * 1024;

  private String peerId;

  private List<String> wals = new ArrayList<>();

  private long batchSize;

  private final KeyLocker<String> peersLock = new KeyLocker<>();

  @Override
  protected byte[] doCall() throws Exception {
    LOG.info("Received a replay sync replication wals {} event, peerId={}", wals, peerId);
    if (rs.getReplicationSinkService() != null) {
      Lock peerLock = peersLock.acquireLock(wals.get(0));
      try {
        for (String wal : wals) {
          replayWAL(wal);
        }
      } finally {
        peerLock.unlock();
      }
    }
    return null;
  }

  @Override
  protected void initParameter(byte[] parameter) throws InvalidProtocolBufferException {
    ReplaySyncReplicationWALParameter param =
      ReplaySyncReplicationWALParameter.parseFrom(parameter);
    this.peerId = param.getPeerId();
    param.getWalList().forEach(this.wals::add);
    this.batchSize = rs.getConfiguration().getLong(REPLAY_SYNC_REPLICATION_WAL_BATCH_SIZE,
      DEFAULT_REPLAY_SYNC_REPLICATION_WAL_BATCH_SIZE);
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_REPLAY_SYNC_REPLICATION_WAL;
  }

  private void replayWAL(String wal) throws IOException {
    WALStreamReader reader = getReader(wal);
    if (reader == null) {
      return;
    }
    try {
      List<Entry> entries = readWALEntries(reader, wal);
      while (!entries.isEmpty()) {
        Pair<AdminProtos.ReplicateWALEntryRequest, ExtendedCellScanner> pair =
          ReplicationProtobufUtil
            .buildReplicateWALEntryRequest(entries.toArray(new Entry[entries.size()]));
        ReplicateWALEntryRequest request = pair.getFirst();
        rs.getReplicationSinkService().replicateLogEntries(request.getEntryList(), pair.getSecond(),
          request.getReplicationClusterId(), request.getSourceBaseNamespaceDirPath(),
          request.getSourceHFileArchiveDirPath());
        // Read next entries.
        entries = readWALEntries(reader, wal);
      }
    } finally {
      reader.close();
    }
  }

  private WALStreamReader getReader(String wal) throws IOException {
    Path path = new Path(rs.getWALRootDir(), wal);
    try {
      RecoverLeaseFSUtils.recoverFileLease(rs.getWALFileSystem(), path, rs.getConfiguration());
      return WALFactory.createStreamReader(rs.getWALFileSystem(), path, rs.getConfiguration());
    } catch (WALHeaderEOFException e) {
      LOG.warn("EOF while opening WAL reader for {}", path, e);
      return null;
    }
  }

  // return whether we should include this entry.
  private boolean filter(Entry entry) {
    WALEdit edit = entry.getEdit();
    WALUtil.filterCells(edit, c -> CellUtil.matchingFamily(c, WALEdit.METAFAMILY) ? null : c);
    return !edit.isEmpty();
  }

  private List<Entry> readWALEntries(WALStreamReader reader, String wal) throws IOException {
    List<Entry> entries = new ArrayList<>();
    if (reader == null) {
      return entries;
    }
    long size = 0;
    for (;;) {
      Entry entry;
      try {
        entry = reader.next();
      } catch (EOFException e) {
        LOG.info("EOF while reading WAL entries from {}: {}, continuing", wal, e.toString());
        break;
      }
      if (entry == null) {
        break;
      }
      if (filter(entry)) {
        entries.add(entry);
        size += entry.getEdit().heapSize();
        if (size > batchSize) {
          break;
        }
      }
    }
    return entries;
  }
}
