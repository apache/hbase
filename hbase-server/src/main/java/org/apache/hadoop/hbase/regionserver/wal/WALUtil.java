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
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;

import com.google.protobuf.TextFormat;

/**
 * Helper methods to ease Region Server integration with the write ahead log.
 * Note that methods in this class specifically should not require access to anything
 * other than the API found in {@link WAL}.
 */
@InterfaceAudience.Private
public class WALUtil {
  private static final Log LOG = LogFactory.getLog(WALUtil.class);

  /**
   * Write the marker that a compaction has succeeded and is about to be committed.
   * This provides info to the HMaster to allow it to recover the compaction if
   * this regionserver dies in the middle (This part is not yet implemented). It also prevents
   * the compaction from finishing if this regionserver has already lost its lease on the log.
   * @param sequenceId Used by WAL to get sequence Id for the waledit.
   */
  public static void writeCompactionMarker(WAL log, HTableDescriptor htd, HRegionInfo info,
      final CompactionDescriptor c, AtomicLong sequenceId) throws IOException {
    TableName tn = TableName.valueOf(c.getTableName().toByteArray());
    // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
    WALKey key = new HLogKey(info.getEncodedNameAsBytes(), tn);
    log.append(htd, info, key, WALEdit.createCompaction(info, c), sequenceId, false, null);
    log.sync();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended compaction marker " + TextFormat.shortDebugString(c));
    }
  }

  /**
   * Write a flush marker indicating a start / abort or a complete of a region flush
   */
  public static long writeFlushMarker(WAL log, HTableDescriptor htd, HRegionInfo info,
      final FlushDescriptor f, AtomicLong sequenceId, boolean sync) throws IOException {
    TableName tn = TableName.valueOf(f.getTableName().toByteArray());
    // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
    WALKey key = new HLogKey(info.getEncodedNameAsBytes(), tn);
    long trx = log.append(htd, info, key, WALEdit.createFlushWALEdit(info, f), sequenceId, false,
        null);
    if (sync) log.sync(trx);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended flush marker " + TextFormat.shortDebugString(f));
    }
    return trx;
  }

  /**
   * Write a region open marker indicating that the region is opened
   */
  public static long writeRegionEventMarker(WAL log, HTableDescriptor htd, HRegionInfo info,
      final RegionEventDescriptor r, AtomicLong sequenceId) throws IOException {
    TableName tn = TableName.valueOf(r.getTableName().toByteArray());
    // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
    WALKey key = new HLogKey(info.getEncodedNameAsBytes(), tn);
    long trx = log.append(htd, info, key, WALEdit.createRegionEventWALEdit(info, r),
      sequenceId, false, null);
    log.sync(trx);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended region event marker " + TextFormat.shortDebugString(r));
    }
    return trx;
  }

  /**
   * Write a log marker that a bulk load has succeeded and is about to be committed.
   *
   * @param wal        The log to write into.
   * @param htd        A description of the table that we are bulk loading into.
   * @param info       A description of the region in the table that we are bulk loading into.
   * @param descriptor A protocol buffers based description of the client's bulk loading request
   * @param sequenceId The current sequenceId in the log at the time when we were to write the
   *                   bulk load marker.
   * @return txid of this transaction or if nothing to do, the last txid
   * @throws IOException We will throw an IOException if we can not append to the HLog.
   */
  public static long writeBulkLoadMarkerAndSync(final WAL wal,
                                                final HTableDescriptor htd,
                                                final HRegionInfo info,
                                                final WALProtos.BulkLoadDescriptor descriptor,
                                                final AtomicLong sequenceId) throws IOException {
    TableName tn = info.getTable();
    WALKey key = new HLogKey(info.getEncodedNameAsBytes(), tn);

    // Add it to the log but the false specifies that we don't need to add it to the memstore
    long trx = wal.append(htd,
            info,
            key,
            WALEdit.createBulkLoadEvent(info, descriptor),
            sequenceId,
            false,
            new ArrayList<Cell>());
    wal.sync(trx);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended Bulk Load marker " + TextFormat.shortDebugString(descriptor));
    }
    return trx;
  }
  
}
