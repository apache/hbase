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
import java.util.NavigableMap;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;

/**
 * Helper methods to ease Region Server integration with the Write Ahead Log (WAL).
 * Note that methods in this class specifically should not require access to anything
 * other than the API found in {@link WAL}. For internal use only.
 */
@InterfaceAudience.Private
public class WALUtil {
  private static final Logger LOG = LoggerFactory.getLogger(WALUtil.class);

  private WALUtil() {
    // Shut down construction of this class.
  }

  /**
   * Write the marker that a compaction has succeeded and is about to be committed.
   * This provides info to the HMaster to allow it to recover the compaction if this regionserver
   * dies in the middle. It also prevents the compaction from finishing if this regionserver has
   * already lost its lease on the log.
   *
   * <p>This write is for internal use only. Not for external client consumption.
   * @param mvcc Used by WAL to get sequence Id for the waledit.
   */
  public static WALKeyImpl writeCompactionMarker(WAL wal,
      NavigableMap<byte[], Integer> replicationScope, RegionInfo hri, final CompactionDescriptor c,
      MultiVersionConcurrencyControl mvcc)
  throws IOException {
    WALKeyImpl walKey =
        writeMarker(wal, replicationScope, hri, WALEdit.createCompaction(hri, c), mvcc);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended compaction marker " + TextFormat.shortDebugString(c));
    }
    return walKey;
  }

  /**
   * Write a flush marker indicating a start / abort or a complete of a region flush
   *
   * <p>This write is for internal use only. Not for external client consumption.
   */
  public static WALKeyImpl writeFlushMarker(WAL wal, NavigableMap<byte[], Integer> replicationScope,
      RegionInfo hri, final FlushDescriptor f, boolean sync, MultiVersionConcurrencyControl mvcc)
          throws IOException {
    WALKeyImpl walKey = doFullAppendTransaction(wal, replicationScope, hri,
        WALEdit.createFlushWALEdit(hri, f), mvcc, sync);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended flush marker " + TextFormat.shortDebugString(f));
    }
    return walKey;
  }

  /**
   * Write a region open marker indicating that the region is opened.
   * This write is for internal use only. Not for external client consumption.
   */
  public static WALKeyImpl writeRegionEventMarker(WAL wal,
      NavigableMap<byte[], Integer> replicationScope, RegionInfo hri,
      final RegionEventDescriptor r, final MultiVersionConcurrencyControl mvcc)
  throws IOException {
    WALKeyImpl walKey = writeMarker(wal, replicationScope, hri,
        WALEdit.createRegionEventWALEdit(hri, r), mvcc);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended region event marker " + TextFormat.shortDebugString(r));
    }
    return walKey;
  }

  /**
   * Write a log marker that a bulk load has succeeded and is about to be committed.
   * This write is for internal use only. Not for external client consumption.
   * @param wal The log to write into.
   * @param replicationScope The replication scope of the families in the HRegion
   * @param hri A description of the region in the table that we are bulk loading into.
   * @param desc A protocol buffers based description of the client's bulk loading request
   * @return walKey with sequenceid filled out for this bulk load marker
   * @throws IOException We will throw an IOException if we can not append to the HLog.
   */
  public static WALKeyImpl writeBulkLoadMarkerAndSync(final WAL wal,
      final NavigableMap<byte[], Integer> replicationScope, final RegionInfo hri,
      final WALProtos.BulkLoadDescriptor desc, final MultiVersionConcurrencyControl mvcc)
          throws IOException {
    WALKeyImpl walKey =
        writeMarker(wal, replicationScope, hri, WALEdit.createBulkLoadEvent(hri, desc), mvcc);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended Bulk Load marker " + TextFormat.shortDebugString(desc));
    }
    return walKey;
  }

  private static WALKeyImpl writeMarker(final WAL wal,
      final NavigableMap<byte[], Integer> replicationScope, final RegionInfo hri,
      final WALEdit edit, final MultiVersionConcurrencyControl mvcc)
  throws IOException {
    // If sync == true in below, then timeout is not used; safe to pass UNSPECIFIED_TIMEOUT
    return doFullAppendTransaction(wal, replicationScope, hri, edit, mvcc, true);
  }

  /**
   * A 'full' WAL transaction involves starting an mvcc transaction followed by an append,
   * an optional sync, and then a call to complete the mvcc transaction. This method does it all.
   * Good for case of adding a single edit or marker to the WAL.
   *
   * <p>This write is for internal use only. Not for external client consumption.
   * @return WALKeyImpl that was added to the WAL.
   */
  public static WALKeyImpl doFullAppendTransaction(final WAL wal,
      final NavigableMap<byte[], Integer> replicationScope, final RegionInfo hri,
      final WALEdit edit, final MultiVersionConcurrencyControl mvcc, final boolean sync)
  throws IOException {
    // TODO: Pass in current time to use?
    WALKeyImpl walKey = new WALKeyImpl(hri.getEncodedNameAsBytes(), hri.getTable(),
        System.currentTimeMillis(), mvcc, replicationScope);
    long trx = MultiVersionConcurrencyControl.NONE;
    try {
      trx = wal.append(hri, walKey, edit, false);
      if (sync) {
        wal.sync(trx);
      }
      // Call complete only here because these are markers only. They are not for clients to read.
      mvcc.complete(walKey.getWriteEntry());
    } catch (IOException ioe) {
      if (walKey.getWriteEntry() != null) {
        mvcc.complete(walKey.getWriteEntry());
      }
      throw ioe;
    }
    return walKey;
  }

  /**
   * Blocksize returned here is 2x the default HDFS blocksize unless explicitly set in
   * Configuration. Works in tandem with hbase.regionserver.logroll.multiplier. See comment in
   * AbstractFSWAL in Constructor where we set blocksize and logrollsize for why.
   * @return Blocksize to use writing WALs.
   */
  public static long getWALBlockSize(Configuration conf, FileSystem fs, Path dir)
      throws IOException {
    return conf.getLong("hbase.regionserver.hlog.blocksize",
        CommonFSUtils.getDefaultBlockSize(fs, dir) * 2);
  }

  public static void filterCells(WALEdit edit, Function<Cell, Cell> mapper) {
    ArrayList<Cell> cells = edit.getCells();
    int size = cells.size();
    int newSize = 0;
    for (int i = 0; i < size; i++) {
      Cell cell = mapper.apply(cells.get(i));
      if (cell != null) {
        cells.set(newSize, cell);
        newSize++;
      }
    }
    for (int i = size - 1; i >= newSize; i--) {
      cells.remove(i);
    }
    if (newSize < size / 2) {
      cells.trimToSize();
    }
  }
}
