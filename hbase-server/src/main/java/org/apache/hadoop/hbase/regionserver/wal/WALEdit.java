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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.io.Writable;

import com.google.common.annotations.VisibleForTesting;


/**
 * WALEdit: Used in HBase's transaction log (WAL) to represent
 * the collection of edits (KeyValue objects) corresponding to a
 * single transaction. The class implements "Writable" interface
 * for serializing/deserializing a set of KeyValue items.
 *
 * Previously, if a transaction contains 3 edits to c1, c2, c3 for a row R,
 * the WAL would have three log entries as follows:
 *
 *    &lt;logseq1-for-edit1&gt;:&lt;eyValue-for-edit-c1&gt;
 *    &lt;logseq2-for-edit2&gt;:&lt;KeyValue-for-edit-c2&gt;
 *    &lt;logseq3-for-edit3&gt;:&lt;KeyValue-for-edit-c3&gt;
 *
 * This presents problems because row level atomicity of transactions
 * was not guaranteed. If we crash after few of the above appends make
 * it, then recovery will restore a partial transaction.
 *
 * In the new world, all the edits for a given transaction are written
 * out as a single record, for example:
 *
 *   &lt;logseq#-for-entire-txn&gt;:&lt;WALEdit-for-entire-txn&gt;
 *
 * where, the WALEdit is serialized as:
 *   &lt;-1, # of edits, &lt;KeyValue&gt;, &lt;KeyValue&gt;, ... &gt;
 * For example:
 *   &lt;-1, 3, &lt;KV-for-edit-c1&gt;, &lt;KV-for-edit-c2&gt;, &lt;KV-for-edit-c3&gt;&gt;
 *
 * The -1 marker is just a special way of being backward compatible with
 * an old WAL which would have contained a single &lt;KeyValue&gt;.
 *
 * The deserializer for WALEdit backward compatibly detects if the record
 * is an old style KeyValue or the new style WALEdit.
 *
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.REPLICATION,
    HBaseInterfaceAudience.COPROC })
public class WALEdit implements Writable, HeapSize {
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

  private final int VERSION_2 = -1;
  private final boolean isReplay;

  private ArrayList<Cell> cells = new ArrayList<Cell>(1);

  public static final WALEdit EMPTY_WALEDIT = new WALEdit();

  // Only here for legacy writable deserialization
  /**
   * @deprecated Legacy
   */
  @Deprecated
  private NavigableMap<byte[], Integer> scopes;

  private CompressionContext compressionContext;

  public WALEdit() {
    this(false);
  }

  public WALEdit(boolean isReplay) {
    this.isReplay = isReplay;
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

  public void setCompressionContext(final CompressionContext compressionContext) {
    this.compressionContext = compressionContext;
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

  public NavigableMap<byte[], Integer> getAndRemoveScopes() {
    NavigableMap<byte[], Integer> result = scopes;
    scopes = null;
    return result;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    cells.clear();
    if (scopes != null) {
      scopes.clear();
    }
    int versionOrLength = in.readInt();
    // TODO: Change version when we protobuf.  Also, change way we serialize KV!  Pb it too.
    if (versionOrLength == VERSION_2) {
      // this is new style WAL entry containing multiple KeyValues.
      int numEdits = in.readInt();
      for (int idx = 0; idx < numEdits; idx++) {
        if (compressionContext != null) {
          this.add(KeyValueCompression.readKV(in, compressionContext));
        } else {
          this.add(KeyValue.create(in));
        }
      }
      int numFamilies = in.readInt();
      if (numFamilies > 0) {
        if (scopes == null) {
          scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
        }
        for (int i = 0; i < numFamilies; i++) {
          byte[] fam = Bytes.readByteArray(in);
          int scope = in.readInt();
          scopes.put(fam, scope);
        }
      }
    } else {
      // this is an old style WAL entry. The int that we just
      // read is actually the length of a single KeyValue
      this.add(KeyValue.create(versionOrLength, in));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    LOG.warn("WALEdit is being serialized to writable - only expected in test code");
    out.writeInt(VERSION_2);
    out.writeInt(cells.size());
    // We interleave the two lists for code simplicity
    for (Cell cell : cells) {
      // This is not used in any of the core code flows so it is just fine to convert to KV
      KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
      if (compressionContext != null) {
        KeyValueCompression.writeKV(out, kv, compressionContext);
      } else{
        KeyValue.write(kv, out);
      }
    }
    if (scopes == null) {
      out.writeInt(0);
    } else {
      out.writeInt(scopes.size());
      for (byte[] key : scopes.keySet()) {
        Bytes.writeByteArray(out, key);
        out.writeInt(scopes.get(key));
      }
    }
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
    if (scopes != null) {
      ret += ClassSize.TREEMAP;
      ret += ClassSize.align(scopes.size() * ClassSize.MAP_ENTRY);
      // TODO this isn't quite right, need help here
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
    if (scopes != null) {
      sb.append(" scopes: " + scopes.toString());
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
      return FlushDescriptor.parseFrom(cell.getValue());
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
      return RegionEventDescriptor.parseFrom(cell.getValue());
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
    if (CellUtil.matchingColumn(kv, METAFAMILY, COMPACTION)) {
      return CompactionDescriptor.parseFrom(kv.getValue());
    }
    return null;
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
      return WALProtos.BulkLoadDescriptor.parseFrom(cell.getValue());
    }
    return null;
  }
}
