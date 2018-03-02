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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.RSProcedureCallable;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
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
public class ReplaySyncReplicationWALCallable implements RSProcedureCallable {

  private static final Logger LOG = LoggerFactory.getLogger(ReplaySyncReplicationWALCallable.class);

  private static final String REPLAY_SYNC_REPLICATION_WAL_BATCH_SIZE =
      "hbase.replay.sync.replication.wal.batch.size";

  private static final long DEFAULT_REPLAY_SYNC_REPLICATION_WAL_BATCH_SIZE = 8 * 1024 * 1024;

  private HRegionServer rs;

  private FileSystem fs;

  private Configuration conf;

  private String peerId;

  private String wal;

  private Exception initError;

  private long batchSize;

  @Override
  public Void call() throws Exception {
    if (initError != null) {
      throw initError;
    }
    LOG.info("Received a replay sync replication wal {} event, peerId={}", wal, peerId);
    if (rs.getReplicationSinkService() != null) {
      try (Reader reader = getReader()) {
        List<Entry> entries = readWALEntries(reader);
        while (!entries.isEmpty()) {
          Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner> pair = ReplicationProtbufUtil
              .buildReplicateWALEntryRequest(entries.toArray(new Entry[entries.size()]));
          ReplicateWALEntryRequest request = pair.getFirst();
          rs.getReplicationSinkService().replicateLogEntries(request.getEntryList(),
            pair.getSecond(), request.getReplicationClusterId(),
            request.getSourceBaseNamespaceDirPath(), request.getSourceHFileArchiveDirPath());
          // Read next entries.
          entries = readWALEntries(reader);
        }
      }
    }
    return null;
  }

  @Override
  public void init(byte[] parameter, HRegionServer rs) {
    this.rs = rs;
    this.fs = rs.getWALFileSystem();
    this.conf = rs.getConfiguration();
    try {
      ReplaySyncReplicationWALParameter param =
          ReplaySyncReplicationWALParameter.parseFrom(parameter);
      this.peerId = param.getPeerId();
      this.wal = param.getWal();
      this.batchSize = rs.getConfiguration().getLong(REPLAY_SYNC_REPLICATION_WAL_BATCH_SIZE,
        DEFAULT_REPLAY_SYNC_REPLICATION_WAL_BATCH_SIZE);
    } catch (InvalidProtocolBufferException e) {
      initError = e;
    }
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_REPLAY_SYNC_REPLICATION_WAL;
  }

  private Reader getReader() throws IOException {
    Path path = new Path(rs.getWALRootDir(), wal);
    long length = rs.getWALFileSystem().getFileStatus(path).getLen();
    try {
      FSUtils.getInstance(fs, conf).recoverFileLease(fs, path, conf);
      return WALFactory.createReader(rs.getWALFileSystem(), path, rs.getConfiguration());
    } catch (EOFException e) {
      if (length <= 0) {
        LOG.warn("File is empty. Could not open {} for reading because {}", path, e);
        return null;
      }
      throw e;
    }
  }

  // return whether we should include this entry.
  private boolean filter(Entry entry) {
    WALEdit edit = entry.getEdit();
    WALUtil.filterCells(edit, c -> CellUtil.matchingFamily(c, WALEdit.METAFAMILY) ? null : c);
    return !edit.isEmpty();
  }

  private List<Entry> readWALEntries(Reader reader) throws IOException {
    List<Entry> entries = new ArrayList<>();
    if (reader == null) {
      return entries;
    }
    long size = 0;
    for (;;) {
      Entry entry = reader.next();
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
