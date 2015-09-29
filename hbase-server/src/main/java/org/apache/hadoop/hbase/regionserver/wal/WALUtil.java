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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
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

  private WALUtil() {
    // Shut down construction of this class.
  }

  /**
   * Write the marker that a compaction has succeeded and is about to be committed.
   * This provides info to the HMaster to allow it to recover the compaction if
   * this regionserver dies in the middle (This part is not yet implemented). It also prevents
   * the compaction from finishing if this regionserver has already lost its lease on the log.
   * @param mvcc Used by WAL to get sequence Id for the waledit.
   */
  public static long writeCompactionMarker(WAL wal, HTableDescriptor htd, HRegionInfo hri,
      final CompactionDescriptor c, MultiVersionConcurrencyControl mvcc)
  throws IOException {
    long trx = writeMarker(wal, htd, hri, WALEdit.createCompaction(hri, c), mvcc, true);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended compaction marker " + TextFormat.shortDebugString(c));
    }
    return trx;
  }

  /**
   * Write a flush marker indicating a start / abort or a complete of a region flush
   */
  public static long writeFlushMarker(WAL wal, HTableDescriptor htd, HRegionInfo hri,
      final FlushDescriptor f, boolean sync, MultiVersionConcurrencyControl mvcc)
  throws IOException {
    long trx = writeMarker(wal, htd, hri, WALEdit.createFlushWALEdit(hri, f), mvcc, sync);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended flush marker " + TextFormat.shortDebugString(f));
    }
    return trx;
  }

  /**
   * Write a region open marker indicating that the region is opened
   */
  public static long writeRegionEventMarker(WAL wal, HTableDescriptor htd, HRegionInfo hri,
      final RegionEventDescriptor r, final MultiVersionConcurrencyControl mvcc)
  throws IOException {
    long trx = writeMarker(wal, htd, hri, WALEdit.createRegionEventWALEdit(hri, r), mvcc, true);
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
   * @param hri       A description of the region in the table that we are bulk loading into.
   * @param desc A protocol buffers based description of the client's bulk loading request
   * @return txid of this transaction or if nothing to do, the last txid
   * @throws IOException We will throw an IOException if we can not append to the HLog.
   */
  public static long writeBulkLoadMarkerAndSync(final WAL wal, final HTableDescriptor htd,
      final HRegionInfo hri, final WALProtos.BulkLoadDescriptor desc,
      final MultiVersionConcurrencyControl mvcc)
  throws IOException {
    long trx = writeMarker(wal, htd, hri, WALEdit.createBulkLoadEvent(hri, desc), mvcc, true);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended Bulk Load marker " + TextFormat.shortDebugString(desc));
    }
    return trx;
  }

  private static long writeMarker(final WAL wal, final HTableDescriptor htd, final HRegionInfo hri,
      final WALEdit edit, final MultiVersionConcurrencyControl mvcc, final boolean sync)
  throws IOException {
    // TODO: Pass in current time to use?
    WALKey key =
      new HLogKey(hri.getEncodedNameAsBytes(), hri.getTable(), System.currentTimeMillis(), mvcc);
    // Add it to the log but the false specifies that we don't need to add it to the memstore
    long trx = MultiVersionConcurrencyControl.NONE;
    try {
      trx = wal.append(htd, hri, key, edit, false);
      if (sync) wal.sync(trx);
    } finally {
      // If you get hung here, is it a real WAL or a mocked WAL? If the latter, you need to
      // trip the latch that is inside in getWriteEntry up in your mock. See down in the append
      // called from onEvent in FSHLog.
      MultiVersionConcurrencyControl.WriteEntry we = key.getWriteEntry();
      if (mvcc != null && we != null) mvcc.complete(we);
    }
    return trx;
  }
}