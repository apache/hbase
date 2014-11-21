/**
 * Copyright 2009 The Apache Software Foundation
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

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.ipc.ByteBufferOutputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * WALEdit: Used in HBase's transaction log (WAL) to represent
 * the collection of edits (KeyValue objects) corresponding to a
 * single transaction. The class implements "Writable" interface
 * for serializing/deserializing a set of KeyValue items.
 *
 * Previously, if a transaction contains 3 edits to c1, c2, c3 for a row R,
 * the HLog would have three log entries as follows:
 *
 *    <logseq1-for-edit1>:<KeyValue-for-edit-c1>
 *    <logseq2-for-edit2>:<KeyValue-for-edit-c2>
 *    <logseq3-for-edit3>:<KeyValue-for-edit-c3>
 *
 * This presents problems because row level atomicity of transactions
 * was not guaranteed. If we crash after few of the above appends make
 * it, then recovery will restore a partial transaction.
 *
 * In the new world, all the edits for a given transaction are written
 * out as a single record, for example:
 *
 *   <logseq#-for-entire-txn>:<WALEdit-for-entire-txn>
 *
 * where, the WALEdit is serialized as:
 *   <-1, # of edits, <KeyValue>, <KeyValue>, ... >
 * For example:
 *   <-1, 3, <Keyvalue-for-edit-c1>, <KeyValue-for-edit-c2>, <KeyValue-for-edit-c3>>
 *
 * The -1 marker is just a special way of being backward compatible with
 * an old HLog which would have contained a single <KeyValue>.
 *
 * The deserializer for WALEdit backward compatibly detects if the record
 * is an old style KeyValue or the new style WALEdit.
 *
 */

@ThriftStruct
public final class WALEdit implements Writable, HeapSize {
  public enum PayloadHeaderField {
    MAGIC(0, Bytes.SIZEOF_BYTE),
    TYPE(1, Bytes.SIZEOF_BYTE),
    VERSION(2, Bytes.SIZEOF_BYTE),
    TIMESTAMP(3, Bytes.SIZEOF_LONG),
    COMPRESSION_CODEC(11, Bytes.SIZEOF_BYTE),
    UNCOMPRESSED_LENGTH(12, Bytes.SIZEOF_INT),
    NUM_WALEDITS(16, Bytes.SIZEOF_INT);

    final int offset;
    final int length;

    private PayloadHeaderField(final int offset, final int length) {
      this.offset = offset;
      this.length = length;
    }
  }

  public final static int PAYLOAD_HEADER_SIZE =
          PayloadHeaderField.NUM_WALEDITS.offset +
                  PayloadHeaderField.NUM_WALEDITS.length;

  private final int VERSION_2 = -1;

  private final List<KeyValue> kvs;

  private NavigableMap<byte[], Integer> scopes;
  
  private long length = 0;

  private SettableFuture<Long> commitFuture;

  @ThriftConstructor
  public WALEdit(
      @ThriftField(1) final List<KeyValue> kvs) {
    this.kvs = kvs;
    for (KeyValue k : kvs) {
      length += k.getLength();
    }
  }

  public WALEdit() {
    kvs = new ArrayList<KeyValue>();
  }

  public SettableFuture<Long> getCommitFuture() {
    return commitFuture;
  }

  public void add(KeyValue kv) {
    this.kvs.add(kv);
    length += kv.getLength();
  }

  public boolean isEmpty() {
    return kvs.isEmpty();
  }
  
  public long getTotalKeyValueLength() {
    return length;
  }

  public int size() {
    return kvs.size();
  }

  @ThriftField(1)
  public List<KeyValue> getKeyValues() {
    return kvs;
  }

  public NavigableMap<byte[], Integer> getScopes() {
    return scopes;
  }

  public void setScopes (NavigableMap<byte[], Integer> scopes) {
    // We currently process the map outside of WALEdit,
    // TODO revisit when replication is part of core
    this.scopes = scopes;
  }

  public void readFields(DataInput in) throws IOException {
    kvs.clear();
    if (scopes != null) {
      scopes.clear();
    }
    int versionOrLength = in.readInt();
    if (versionOrLength == VERSION_2) {
      // this is new style HLog entry containing multiple KeyValues.
      int numEdits = in.readInt();
      for (int idx = 0; idx < numEdits; idx++) {
        KeyValue kv = new KeyValue();
        kv.readFields(in);
        this.add(kv);
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
      // this is an old style HLog entry. The int that we just
      // read is actually the length of a single KeyValue.
      KeyValue kv = new KeyValue();
      kv.readFields(versionOrLength, in);
      this.add(kv);
    }

  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(VERSION_2);
    out.writeInt(kvs.size());
    // We interleave the two lists for code simplicity
    for (KeyValue kv : kvs) {
      kv.write(out);
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

  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("[#edits: " + kvs.size() + " = <");
    for (KeyValue kv : kvs) {
      sb.append(kv.toString());
      sb.append("; ");
    }
    if (scopes != null) {
      sb.append(" scopes: " + scopes.toString());
    }
    sb.append(">]");
    return sb.toString();
  }

  @Override
  public long heapSize() {
    long ret = 0;
    for (KeyValue kv : kvs) {
      ret += kv.heapSize();
    }
    if (scopes != null) {
      ret += ClassSize.TREEMAP;
      ret += ClassSize.align(scopes.size() * ClassSize.MAP_ENTRY);
    }
    return ret;
  }

  /**
   * Serialize the given list of WALEdits to an OutputStream.
   * @param edits the WALEdits to be serialized
   * @param os the {@link DataOutputStream} to write to
   * @throws IOException if the output could not be written to the stream
   */
  private static void serializeWALEdits(final List<WALEdit> edits,
          final DataOutputStream os) throws IOException {
    for (final WALEdit e : edits) {
      os.writeInt(e.getKeyValues().size());
      for (final KeyValue k : e.getKeyValues()) {
        os.writeInt(k.getLength());
        os.write(k.getBuffer(), k.getOffset(), k.getLength());
      }
    }
  }

  /**
   * Serialize the given list of WALEdits edits to a {@link ByteBuffer},
   * optionally compressing the WALEdit data using the given compression codec.
   *
   * @param edits the list of WALEdits
   * @return a {@link ByteBuffer} containing a serialized representation of the
   *          WALEdits.
   * @throws java.io.IOException if the WALEdits could not be serialized
   */
  public static ByteBuffer serializeToByteBuffer(final List<WALEdit> edits,
          long timestamp, Compression.Algorithm codec) throws IOException {
    Preconditions.checkNotNull(codec);
    if (edits == null) {
      return null;
    }

    int totalPayloadSize = getTotalPayloadSize(edits);
    ByteBufferOutputStream buffer = new ByteBufferOutputStream(
            totalPayloadSize);
    try (DataOutputStream os = new DataOutputStream(buffer)) {
      // Write the magic value
      os.write(HConstants.CONSENSUS_PAYLOAD_MAGIC_VALUE);

      // Write that the payload is WALEdit
      os.write(HConstants.BATCHED_WALEDIT_TYPE);

      // Write the version of WALEdit
      os.write(HConstants.BATCHED_WALEDIT_VERSION);

      // Write the timestamp
      os.writeLong(timestamp);

      // Write compression algorithm
      os.write((byte) codec.ordinal());

      // Write uncompressed size of the list of WALEdits
      os.writeInt(totalPayloadSize - PAYLOAD_HEADER_SIZE);

      // Write the number of WALEdits in the list
      os.writeInt(edits.size());
    }

    // Turn on compression if requested when serializing the list of WALEdits.
    boolean compressed = !codec.equals(Compression.Algorithm.NONE);
    Compressor compressor = codec.getCompressor();
    try (DataOutputStream os = new DataOutputStream(compressed ?
                    codec.createCompressionStream(buffer, compressor,
                            totalPayloadSize - PAYLOAD_HEADER_SIZE) : buffer)) {
      serializeWALEdits(edits, os);
    } finally {
      codec.returnCompressor(compressor);
    }

    // Flip and return the byte buffer.
    return buffer.getByteBuffer();
  }

  public static int getWALEditsSize(final List<WALEdit> edits) {
    int size = 0;
    for (final WALEdit e : edits) {
      size += Bytes.SIZEOF_INT + e.getKeyValues().size() * Bytes.SIZEOF_INT
              + e.getTotalKeyValueLength();
    }
    return size;
  }

  public static int getTotalPayloadSize(final List<WALEdit> edits) {
    return PAYLOAD_HEADER_SIZE + getWALEditsSize(edits);
  }

  public static boolean isBatchedWALEdit(final ByteBuffer data) {
    // Read the Magic Value
    if (data.get(data.position() + PayloadHeaderField.MAGIC.offset) !=
            HConstants.CONSENSUS_PAYLOAD_MAGIC_VALUE) {
      return false;
    }

    // Read the payload type
    if (data.get(data.position() + PayloadHeaderField.TYPE.offset) !=
            HConstants.BATCHED_WALEDIT_TYPE) {
      return false;
    }
    return true;
  }

  /**
   * Get the timestamp of the batched WALEdit. This method assumes the given
   * ByteBuffer contains a valid batched WALEdits which can be verified using
   * {@link #isBatchedWALEdit}.
   */
  public static long getTimestamp(final ByteBuffer data) {
    return data.getLong(data.position() + PayloadHeaderField.TIMESTAMP.offset);
  }

  /**
   * Get the compression codec used to compress the serialized WALEdits
   * contained in the given {@link ByteBuffer}. This method assumes the position
   * of the buffer to point to the does not change the
   * position value of the buffer and assumes the caller has performed a version
   * check on the buffer to ensure the
   *
   * @param data a {@link java.nio.ByteBuffer} containing a serialized list of
   *             WALEdits
   * @return the compression codec or the NONE codec if the WALEdit was written
   *              with a version which does not support compression
   */
  public static Compression.Algorithm getCompressionCodec(
          final ByteBuffer data) {
    byte codecValue = data.get(data.position() +
            PayloadHeaderField.COMPRESSION_CODEC.offset);
    Compression.Algorithm[] codecs = Compression.Algorithm.values();
    if (codecValue >= 0 && codecValue < codecs.length) {
      return codecs[codecValue];
    }
    return Compression.Algorithm.NONE;
  }

  /**
   * Wrap the array backing the given ByteBuffer with a ByteArrayInputStream.
   * Since this InputStream works on the underlying array the state of the given
   * ByteBuffer is guaranteed to remain unchanged.
   *
   * @param buffer an array backed {@link ByteBuffer}
   * @param position the position in the buffer from where to start the stream
   * @param length length of the input stream
   * @return an {@link java.io.InputStream} wrapping the underlying array of
   *          the given {@link ByteBuffer}
   */
  private static ByteArrayInputStream getByteArrayInputStream(
          final ByteBuffer buffer, final int position, final int length) {
    Preconditions.checkArgument(buffer.hasArray(),
            "An array backed buffer is required");
    Preconditions.checkArgument(position >= buffer.position(),
            "Position can not be behind buffer.position()");
    Preconditions.checkArgument(
            position - buffer.position() + length <= buffer.remaining(),
            "Length can not be past the remainder of the buffer");
    return new ByteArrayInputStream(buffer.array(),
            buffer.arrayOffset() + position, length);
  }

  /**
   * Read a list of serialized WALEdits from the given
   * {@link DataInputStream}, instantiating them backed by the given
   * {@link ByteBuffer}.
   *
   * @param numEdits the number of WALEdits expected in the stream
   * @param is the {@link InputStream} containing serialized WALEdits
   * @param buffer the {@link ByteBuffer} to be used to back the KVs
   * @param offset the offset in the buffer from where to copy the KVs
   * @param copyToBuffer copy from the stream to the buffer if true, assume the
   *                     steam data is already in the buffer otherwise
   * @return a list of WALEdits
   * @throws IOException if an exception occurs while reading from the stream
   */
  private static List<WALEdit> deserializeWALEdits(final int numEdits,
          final DataInputStream is, final ByteBuffer buffer, final int offset,
          final boolean copyToBuffer) throws IOException {
    List<WALEdit> edits = new ArrayList<>(numEdits);
    byte[] array = buffer.array();
    int cursor = buffer.arrayOffset() + offset;

    for (int editIdx = 0; editIdx < numEdits; ++editIdx) {
      WALEdit edit = new WALEdit();
      int numKVs = is.readInt();
      cursor += Bytes.SIZEOF_INT;

      for (int kvIdx = 0; kvIdx < numKVs; ++kvIdx) {
        int kvLen = is.readInt();
        cursor += Bytes.SIZEOF_INT;

        if (copyToBuffer) {
          // If the buffer does not contain the data yet (which would be the
          // case if it is compressed), copy from the InputStream to the buffer.
          is.read(array, cursor, kvLen);
        } else {
          // Do not copy to the buffer and advance the stream cursor.
          is.skipBytes(kvLen);
        }

        // Instantiate the KV backed by the ByteBuffer
        edit.add(new KeyValue(array, cursor, kvLen));
        // Move the ByteBuffer write cursor
        cursor += kvLen;
      }

      edits.add(edit);
    }

    return edits;
  }

  /**
   * De-serializes a ByteBuffer to list of WALEdits. If the serialized WALEdits
   * are not compressed, the resulting list of KVs will be backed by the array
   * backing the ByteBuffer instead of allocating fresh buffers. As a
   * consequence of this method assumes the state of the ByteBuffer is never
   * modified.
   *
   * @param data a {@link ByteBuffer} containing serialized WALEdits
   * @return a list of WALEdits
   * @throws java.io.IOException if the WALEdits could not be deserialized
   */
  public static List<WALEdit> deserializeFromByteBuffer(final ByteBuffer data)
          throws IOException {
    if (!isBatchedWALEdit(data)) {
      return null;
    }

    int firstBytePosition = data.position();
    int bufferLength = data.remaining();

    // The check above already read the magic value and type fields, so move on
    // to the version field.
    byte version = data.get(firstBytePosition +
            PayloadHeaderField.VERSION.offset);
    if (version != HConstants.BATCHED_WALEDIT_VERSION) {
      return null;
    }

    // Get the compression codec and uncompressed size of the list of WALEdits.
    // Use the remainder of the current buffer as a hint.
    Compression.Algorithm codec = getCompressionCodec(data);

    int uncompressedEditsLen = data.getInt(firstBytePosition +
            PayloadHeaderField.UNCOMPRESSED_LENGTH.offset);
    int numEdits = data.getInt(firstBytePosition +
            PayloadHeaderField.NUM_WALEDITS.offset);

    if (numEdits == 0) {
      return Collections.emptyList();
    }

    // Wrap the remainder of the given ByteBuffer with a DataInputStream and
    // de-serialize the list of WALEdits.
    //
    // If the WALEdits are compressed, wrap the InputStream by a decompression
    // stream and allocate a new buffer to store the uncompressed data.
    int cursor = firstBytePosition + PAYLOAD_HEADER_SIZE;
    InputStream is = getByteArrayInputStream(data, cursor,
            bufferLength - PAYLOAD_HEADER_SIZE);
    ByteBuffer deserializedData = data;

    final boolean compressed = !codec.equals(Compression.Algorithm.NONE);
    Decompressor decompressor = codec.getDecompressor();
    try {
      if (compressed) {
        int compressedEditsLen = bufferLength - PAYLOAD_HEADER_SIZE;
        is = codec.createDecompressionStream(is, decompressor,
                compressedEditsLen);
        // Allocate a new ByteBuffer for the uncompressed data.
        deserializedData = ByteBuffer.allocate(uncompressedEditsLen);
        cursor = 0;
      }

      try (DataInputStream dis = new DataInputStream(is)) {
        return deserializeWALEdits(numEdits, dis, deserializedData, cursor,
                compressed);
      }
    } finally {
      codec.returnDecompressor(decompressor);
    }
  }
}
