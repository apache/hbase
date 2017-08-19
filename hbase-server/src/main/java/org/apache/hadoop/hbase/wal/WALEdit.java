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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;


/**
 * WALEdit: Used in HBase's transaction log (WAL) to represent
 * the collection of edits (KeyValue objects) corresponding to a
 * single transaction.
 *
 * All the edits for a given transaction are written out as a single record, in PB format followed
 * by Cells written via the WALCellEncoder.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.REPLICATION,
    HBaseInterfaceAudience.COPROC })
public class WALEdit implements HeapSize {
  private static final Log LOG = LogFactory.getLog(WALEdit.class);

  // TODO: Get rid of this; see HBASE-8457
  public static final byte [] METAFAMILY = Bytes.toBytes("METAFAMILY");
  @VisibleForTesting
  public static final byte [] METAROW = Bytes.toBytes("METAROW");
  @VisibleForTesting
  public static final byte[] COMPACTION = Bytes.toBytes("HBASE::COMPACTION");
  @VisibleForTesting
  public static final byte [] FLUSH = Bytes.toBytes("HBASE::FLUSH");
  @VisibleForTesting
  public static final byte [] REGION_EVENT = Bytes.toBytes("HBASE::REGION_EVENT");
  @VisibleForTesting
  public static final byte [] BULK_LOAD = Bytes.toBytes("HBASE::BULK_LOAD");

  private final boolean isReplay;

  private ArrayList<Cell> cells = null;

  public WALEdit() {
    this(false);
  }

  public WALEdit(boolean isReplay) {
    this(1, isReplay);
  }

  public WALEdit(int cellCount) {
    this(cellCount, false);
  }

  public WALEdit(int cellCount, boolean isReplay) {
    this.isReplay = isReplay;
    cells = new ArrayList<>(cellCount);
  }

  /**
   * @param f
   * @return True is <code>f</code> is {@link #METAFAMILY}
   */
  public static boolean isMetaEditFamily(final byte [] f) {
    return Bytes.equals(METAFAMILY, f);
  }

  public static boolean isMetaEditFamily(Cell cell) {
    return CellUtil.matchingFamily(cell, METAFAMILY);
  }

  public boolean isMetaEdit() {
    for (Cell cell: cells) {
      if (!isMetaEditFamily(cell)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return True when current WALEdit is created by log replay. Replication skips WALEdits from
   *         replay.
   */
  public boolean isReplay() {
    return this.isReplay;
  }

  public WALEdit add(Cell cell) {
    this.cells.add(cell);
    return this;
  }

  public boolean isEmpty() {
    return cells.isEmpty();
  }

  public int size() {
    return cells.size();
  }

  public ArrayList<Cell> getCells() {
    return cells;
  }

  /**
   * This is not thread safe.
   * This will change the WALEdit and shouldn't be used unless you are sure that nothing
   * else depends on the contents being immutable.
   *
   * @param cells the list of cells that this WALEdit now contains.
   */
  @InterfaceAudience.Private
  public void setCells(ArrayList<Cell> cells) {
    this.cells = cells;
  }

  /**
   * Reads WALEdit from cells.
   * @param cellDecoder Cell decoder.
   * @param expectedCount Expected cell count.
   * @return Number of KVs read.
   */
  public int readFromCells(Codec.Decoder cellDecoder, int expectedCount) throws IOException {
    cells.clear();
    cells.ensureCapacity(expectedCount);
    while (cells.size() < expectedCount && cellDecoder.advance()) {
      cells.add(cellDecoder.current());
    }
    return cells.size();
  }

  @Override
  public long heapSize() {
    long ret = ClassSize.ARRAYLIST;
    for (Cell cell : cells) {
      ret += CellUtil.estimatedHeapSizeOf(cell);
    }
    return ret;
  }

  public long estimatedSerializedSizeOf() {
    long ret = 0;
    for (Cell cell: cells) {
      ret += CellUtil.estimatedSerializedSizeOf(cell);
    }
    return ret;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("[#edits: " + cells.size() + " = <");
    for (Cell cell : cells) {
      sb.append(cell);
      sb.append("; ");
    }
    sb.append(">]");
    return sb.toString();
  }

  public static WALEdit createFlushWALEdit(HRegionInfo hri, FlushDescriptor f) {
    KeyValue kv = new KeyValue(getRowForRegion(hri), METAFAMILY, FLUSH,
      EnvironmentEdgeManager.currentTime(), f.toByteArray());
    return new WALEdit().add(kv);
  }

  public static FlushDescriptor getFlushDescriptor(Cell cell) throws IOException {
    if (CellUtil.matchingColumn(cell, METAFAMILY, FLUSH)) {
      return FlushDescriptor.parseFrom(CellUtil.cloneValue(cell));
    }
    return null;
  }

  public static WALEdit createRegionEventWALEdit(HRegionInfo hri,
      RegionEventDescriptor regionEventDesc) {
    KeyValue kv = new KeyValue(getRowForRegion(hri), METAFAMILY, REGION_EVENT,
      EnvironmentEdgeManager.currentTime(), regionEventDesc.toByteArray());
    return new WALEdit().add(kv);
  }

  public static RegionEventDescriptor getRegionEventDescriptor(Cell cell) throws IOException {
    if (CellUtil.matchingColumn(cell, METAFAMILY, REGION_EVENT)) {
      return RegionEventDescriptor.parseFrom(CellUtil.cloneValue(cell));
    }
    return null;
  }

  /**
   * Create a compaction WALEdit
   * @param c
   * @return A WALEdit that has <code>c</code> serialized as its value
   */
  public static WALEdit createCompaction(final HRegionInfo hri, final CompactionDescriptor c) {
    byte [] pbbytes = c.toByteArray();
    KeyValue kv = new KeyValue(getRowForRegion(hri), METAFAMILY, COMPACTION,
      EnvironmentEdgeManager.currentTime(), pbbytes);
    return new WALEdit().add(kv); //replication scope null so that this won't be replicated
  }

  public static byte[] getRowForRegion(HRegionInfo hri) {
    byte[] startKey = hri.getStartKey();
    if (startKey.length == 0) {
      // empty row key is not allowed in mutations because it is both the start key and the end key
      // we return the smallest byte[] that is bigger (in lex comparison) than byte[0].
      return new byte[] {0};
    }
    return startKey;
  }

  /**
   * Deserialized and returns a CompactionDescriptor is the KeyValue contains one.
   * @param kv the key value
   * @return deserialized CompactionDescriptor or null.
   */
  public static CompactionDescriptor getCompaction(Cell kv) throws IOException {
    if (isCompactionMarker(kv)) {
      return CompactionDescriptor.parseFrom(CellUtil.cloneValue(kv));
    }
    return null;
  }

  /**
   * Returns true if the given cell is a serialized {@link CompactionDescriptor}
   *
   * @see #getCompaction(Cell)
   */
  public static boolean isCompactionMarker(Cell cell) {
    return CellUtil.matchingColumn(cell, METAFAMILY, COMPACTION);
  }

  /**
   * Create a bulk loader WALEdit
   *
   * @param hri                The HRegionInfo for the region in which we are bulk loading
   * @param bulkLoadDescriptor The descriptor for the Bulk Loader
   * @return The WALEdit for the BulkLoad
   */
  public static WALEdit createBulkLoadEvent(HRegionInfo hri,
                                            WALProtos.BulkLoadDescriptor bulkLoadDescriptor) {
    KeyValue kv = new KeyValue(getRowForRegion(hri),
        METAFAMILY,
        BULK_LOAD,
        EnvironmentEdgeManager.currentTime(),
        bulkLoadDescriptor.toByteArray());
    return new WALEdit().add(kv);
  }

  /**
   * Deserialized and returns a BulkLoadDescriptor from the passed in Cell
   * @param cell the key value
   * @return deserialized BulkLoadDescriptor or null.
   */
  public static WALProtos.BulkLoadDescriptor getBulkLoadDescriptor(Cell cell) throws IOException {
    if (CellUtil.matchingColumn(cell, METAFAMILY, BULK_LOAD)) {
      return WALProtos.BulkLoadDescriptor.parseFrom(CellUtil.cloneValue(cell));
    }
    return null;
  }
}
