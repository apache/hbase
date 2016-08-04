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

package org.apache.hadoop.hbase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IterableUtils;
import org.apache.hadoop.hbase.util.SimpleByteRange;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * static convenience methods for dealing with KeyValues and collections of KeyValues
 */
@InterfaceAudience.Private
public class KeyValueUtil {

  /**************** length *********************/

  public static int length(final Cell cell) {
    return (int) (KeyValue.getKeyValueDataStructureSize(cell.getRowLength(),
        cell.getFamilyLength(), cell.getQualifierLength(), cell.getValueLength(),
        cell.getTagsLengthUnsigned()));
  }

  public static int keyLength(final Cell cell) {
    return (int)KeyValue.getKeyDataStructureSize(cell.getRowLength(), cell.getFamilyLength(),
      cell.getQualifierLength());
  }

  public static int lengthWithMvccVersion(final KeyValue kv, final boolean includeMvccVersion) {
    int length = kv.getLength();
    if (includeMvccVersion) {
      length += WritableUtils.getVIntSize(kv.getMvccVersion());
    }
    return length;
  }

  public static int totalLengthWithMvccVersion(final Iterable<? extends KeyValue> kvs,
      final boolean includeMvccVersion) {
    int length = 0;
    for (KeyValue kv : IterableUtils.nullSafe(kvs)) {
      length += lengthWithMvccVersion(kv, includeMvccVersion);
    }
    return length;
  }


  /**************** copy key only *********************/

  public static KeyValue copyToNewKeyValue(final Cell cell) {
    byte[] bytes = copyToNewByteArray(cell);
    KeyValue kvCell = new KeyValue(bytes, 0, bytes.length);
    kvCell.setMvccVersion(cell.getMvccVersion());
    return kvCell;
  }

  public static ByteBuffer copyKeyToNewByteBuffer(final Cell cell) {
    byte[] bytes = new byte[keyLength(cell)];
    appendKeyToByteArrayWithoutValue(cell, bytes, 0);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return buffer;
  }

  public static byte[] copyToNewByteArray(final Cell cell) {
    int v1Length = length(cell);
    byte[] backingBytes = new byte[v1Length];
    appendToByteArray(cell, backingBytes, 0);
    return backingBytes;
  }

  protected static int appendKeyToByteArrayWithoutValue(final Cell cell, final byte[] output,
      final int offset) {
    int nextOffset = offset;
    nextOffset = Bytes.putShort(output, nextOffset, cell.getRowLength());
    nextOffset = CellUtil.copyRowTo(cell, output, nextOffset);
    nextOffset = Bytes.putByte(output, nextOffset, cell.getFamilyLength());
    nextOffset = CellUtil.copyFamilyTo(cell, output, nextOffset);
    nextOffset = CellUtil.copyQualifierTo(cell, output, nextOffset);
    nextOffset = Bytes.putLong(output, nextOffset, cell.getTimestamp());
    nextOffset = Bytes.putByte(output, nextOffset, cell.getTypeByte());
    return nextOffset;
  }


  /**************** copy key and value *********************/

  public static int appendToByteArray(final Cell cell, final byte[] output, final int offset) {
    int pos = offset;
    pos = Bytes.putInt(output, pos, keyLength(cell));
    pos = Bytes.putInt(output, pos, cell.getValueLength());
    pos = appendKeyToByteArrayWithoutValue(cell, output, pos);
    pos = CellUtil.copyValueTo(cell, output, pos);
    if ((cell.getTagsLengthUnsigned() > 0)) {
      pos = Bytes.putAsShort(output, pos, cell.getTagsLengthUnsigned());
      pos = CellUtil.copyTagTo(cell, output, pos);
    }
    return pos;
  }

  public static ByteBuffer copyToNewByteBuffer(final Cell cell) {
    byte[] bytes = new byte[length(cell)];
    appendToByteArray(cell, bytes, 0);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return buffer;
  }

  public static void appendToByteBuffer(final ByteBuffer bb, final KeyValue kv,
      final boolean includeMvccVersion) {
    // keep pushing the limit out. assume enough capacity
    bb.limit(bb.position() + kv.getLength());
    bb.put(kv.getBuffer(), kv.getOffset(), kv.getLength());
    if (includeMvccVersion) {
      int numMvccVersionBytes = WritableUtils.getVIntSize(kv.getMvccVersion());
      ByteBufferUtils.extendLimit(bb, numMvccVersionBytes);
      ByteBufferUtils.writeVLong(bb, kv.getMvccVersion());
    }
  }


  /**************** iterating *******************************/

  /**
   * Creates a new KeyValue object positioned in the supplied ByteBuffer and sets the ByteBuffer's
   * position to the start of the next KeyValue. Does not allocate a new array or copy data.
   * @param bb
   * @param includesMvccVersion
   * @param includesTags
   */
  public static KeyValue nextShallowCopy(final ByteBuffer bb, final boolean includesMvccVersion,
      boolean includesTags) {
    if (bb.isDirect()) {
      throw new IllegalArgumentException("only supports heap buffers");
    }
    if (bb.remaining() < 1) {
      return null;
    }
    KeyValue keyValue = null;
    int underlyingArrayOffset = bb.arrayOffset() + bb.position();
    int keyLength = bb.getInt();
    int valueLength = bb.getInt();
    ByteBufferUtils.skip(bb, keyLength + valueLength);
    int tagsLength = 0;
    if (includesTags) {
      // Read short as unsigned, high byte first
      tagsLength = ((bb.get() & 0xff) << 8) ^ (bb.get() & 0xff);
      ByteBufferUtils.skip(bb, tagsLength);
    }
    int kvLength = (int) KeyValue.getKeyValueDataStructureSize(keyLength, valueLength, tagsLength);
    keyValue = new KeyValue(bb.array(), underlyingArrayOffset, kvLength);
    if (includesMvccVersion) {
      long mvccVersion = ByteBufferUtils.readVLong(bb);
      keyValue.setMvccVersion(mvccVersion);
    }
    return keyValue;
  }


  /*************** next/previous **********************************/

  /*************** next/previous **********************************/

  /**
   * Append single byte 0x00 to the end of the input row key
   */
  public static KeyValue createFirstKeyInNextRow(final Cell in){
    byte[] nextRow = new byte[in.getRowLength() + 1];
    System.arraycopy(in.getRowArray(), in.getRowOffset(), nextRow, 0, in.getRowLength());
    nextRow[nextRow.length - 1] = 0;//maybe not necessary
    return createFirstOnRow(nextRow);
  }

  /**
   * Increment the row bytes and clear the other fields
   */
  public static KeyValue createFirstKeyInIncrementedRow(final Cell in){
    byte[] thisRow = new SimpleByteRange(in.getRowArray(), in.getRowOffset(),
        in.getRowLength()).deepCopyToNewArray();
    byte[] nextRow = Bytes.unsignedCopyAndIncrement(thisRow);
    return createFirstOnRow(nextRow);
  }

  /**
   * Decrement the timestamp.  For tests (currently wasteful)
   *
   * Remember timestamps are sorted reverse chronologically.
   * @param in
   * @return previous key
   */
  public static KeyValue previousKey(final KeyValue in) {
    return createFirstOnRow(CellUtil.cloneRow(in), CellUtil.cloneFamily(in),
      CellUtil.cloneQualifier(in), in.getTimestamp() - 1);
  }


  /**
   * Create a KeyValue for the specified row, family and qualifier that would be
   * larger than or equal to all other possible KeyValues that have the same
   * row, family, qualifier. Used for reseeking. Should NEVER be returned to a client.
   *
   * @param row
   *          row key
   * @param roffset
   *         row offset
   * @param rlength
   *         row length
   * @param family
   *         family name
   * @param foffset
   *         family offset
   * @param flength
   *         family length
   * @param qualifier
   *        column qualifier
   * @param qoffset
   *        qualifier offset
   * @param qlength
   *        qualifier length
   * @return Last possible key on passed row, family, qualifier.
   */
  public static KeyValue createLastOnRow(final byte[] row, final int roffset, final int rlength,
      final byte[] family, final int foffset, final int flength, final byte[] qualifier,
      final int qoffset, final int qlength) {
    return new KeyValue(row, roffset, rlength, family, foffset, flength, qualifier, qoffset,
        qlength, HConstants.OLDEST_TIMESTAMP, Type.Minimum, null, 0, 0);
  }
  
  /**
   * Creates a keyValue for the specified keyvalue larger than or equal to all other possible
   * KeyValues that have the same row, family, qualifer.  Used for reseeking
   * @param kv
   * @return KeyValue
   */
  public static KeyValue createLastOnRow(Cell kv) {
    return createLastOnRow(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), null, 0, 0,
        null, 0, 0);
  }

  /**
   * Similar to
   * {@link #createLastOnRow(byte[], int, int, byte[], int, int, byte[], int, int)}
   * but creates the last key on the row/column of this KV (the value part of
   * the returned KV is always empty). Used in creating "fake keys" for the
   * multi-column Bloom filter optimization to skip the row/column we already
   * know is not in the file. Not to be returned to clients.
   * 
   * @param kv - cell
   * @return the last key on the row/column of the given key-value pair
   */
  public static KeyValue createLastOnRowCol(Cell kv) {
    return new KeyValue(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
        kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(),
        kv.getQualifierOffset(), kv.getQualifierLength(), HConstants.OLDEST_TIMESTAMP,
        Type.Minimum, null, 0, 0);
  }

  /**
   * Creates the first KV with the row/family/qualifier of this KV and the given
   * timestamp. Uses the "maximum" KV type that guarantees that the new KV is
   * the lowest possible for this combination of row, family, qualifier, and
   * timestamp. This KV's own timestamp is ignored. While this function copies
   * the value from this KV, it is normally used on key-only KVs.
   * 
   * @param kv - cell
   * @param ts
   */
  public static KeyValue createFirstOnRowColTS(Cell kv, long ts) {
    return new KeyValue(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
        kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(),
        kv.getQualifierOffset(), kv.getQualifierLength(), ts, Type.Maximum, kv.getValueArray(),
        kv.getValueOffset(), kv.getValueLength());
  }
  
  /**
   * Create a KeyValue that is smaller than all other possible KeyValues
   * for the given row. That is any (valid) KeyValue on 'row' would sort
   * _after_ the result.
   *
   * @param row - row key (arbitrary byte array)
   * @return First possible KeyValue on passed <code>row</code>
   */
  public static KeyValue createFirstOnRow(final byte [] row, int roffset, short rlength) {
    return new KeyValue(row, roffset, rlength,
        null, 0, 0, null, 0, 0, HConstants.LATEST_TIMESTAMP, Type.Maximum, null, 0, 0);
  }
  

  /**
   * Creates a KeyValue that is last on the specified row id. That is,
   * every other possible KeyValue for the given row would compareTo()
   * less than the result of this call.
   * @param row row key
   * @return Last possible KeyValue on passed <code>row</code>
   */
  public static KeyValue createLastOnRow(final byte[] row) {
    return new KeyValue(row, null, null, HConstants.LATEST_TIMESTAMP, Type.Minimum);
  }

  /**
   * Create a KeyValue that is smaller than all other possible KeyValues
   * for the given row. That is any (valid) KeyValue on 'row' would sort
   * _after_ the result.
   *
   * @param row - row key (arbitrary byte array)
   * @return First possible KeyValue on passed <code>row</code>
   */
  public static KeyValue createFirstOnRow(final byte [] row) {
    return createFirstOnRow(row, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Creates a KeyValue that is smaller than all other KeyValues that
   * are older than the passed timestamp.
   * @param row - row key (arbitrary byte array)
   * @param ts - timestamp
   * @return First possible key on passed <code>row</code> and timestamp.
   */
  public static KeyValue createFirstOnRow(final byte [] row,
      final long ts) {
    return new KeyValue(row, null, null, ts, Type.Maximum);
  }

  /**
   * Create a KeyValue for the specified row, family and qualifier that would be
   * smaller than all other possible KeyValues that have the same row,family,qualifier.
   * Used for seeking.
   * @param row - row key (arbitrary byte array)
   * @param family - family name
   * @param qualifier - column qualifier
   * @return First possible key on passed <code>row</code>, and column.
   */
  public static KeyValue createFirstOnRow(final byte [] row, final byte [] family,
      final byte [] qualifier) {
    return new KeyValue(row, family, qualifier, HConstants.LATEST_TIMESTAMP, Type.Maximum);
  }

  /**
   * Create a Delete Family KeyValue for the specified row and family that would
   * be smaller than all other possible Delete Family KeyValues that have the
   * same row and family.
   * Used for seeking.
   * @param row - row key (arbitrary byte array)
   * @param family - family name
   * @return First Delete Family possible key on passed <code>row</code>.
   */
  public static KeyValue createFirstDeleteFamilyOnRow(final byte [] row,
      final byte [] family) {
    return new KeyValue(row, family, null, HConstants.LATEST_TIMESTAMP,
        Type.DeleteFamily);
  }

  /**
   * @param row - row key (arbitrary byte array)
   * @param f - family name
   * @param q - column qualifier
   * @param ts - timestamp
   * @return First possible key on passed <code>row</code>, column and timestamp
   */
  public static KeyValue createFirstOnRow(final byte [] row, final byte [] f,
      final byte [] q, final long ts) {
    return new KeyValue(row, f, q, ts, Type.Maximum);
  }

  /**
   * Create a KeyValue for the specified row, family and qualifier that would be
   * smaller than all other possible KeyValues that have the same row,
   * family, qualifier.
   * Used for seeking.
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @return First possible key on passed Row, Family, Qualifier.
   */
  public static KeyValue createFirstOnRow(final byte [] row,
      final int roffset, final int rlength, final byte [] family,
      final int foffset, final int flength, final byte [] qualifier,
      final int qoffset, final int qlength) {
    return new KeyValue(row, roffset, rlength, family,
        foffset, flength, qualifier, qoffset, qlength,
        HConstants.LATEST_TIMESTAMP, Type.Maximum, null, 0, 0);
  }

  /**
   * Create a KeyValue for the specified row, family and qualifier that would be
   * smaller than all other possible KeyValues that have the same row,
   * family, qualifier.
   * Used for seeking.
   *
   * @param buffer the buffer to use for the new <code>KeyValue</code> object
   * @param row the value key
   * @param family family name
   * @param qualifier column qualifier
   *
   * @return First possible key on passed Row, Family, Qualifier.
   *
   * @throws IllegalArgumentException The resulting <code>KeyValue</code> object would be larger
   * than the provided buffer or than <code>Integer.MAX_VALUE</code>
   */
  public static KeyValue createFirstOnRow(byte [] buffer, final byte [] row,
      final byte [] family, final byte [] qualifier)
          throws IllegalArgumentException {
    return createFirstOnRow(buffer, 0, row, 0, row.length,
        family, 0, family.length,
        qualifier, 0, qualifier.length);
  }

  /**
   * Create a KeyValue for the specified row, family and qualifier that would be
   * smaller than all other possible KeyValues that have the same row,
   * family, qualifier.
   * Used for seeking.
   *
   * @param buffer the buffer to use for the new <code>KeyValue</code> object
   * @param boffset buffer offset
   * @param row the value key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return First possible key on passed Row, Family, Qualifier.
   *
   * @throws IllegalArgumentException The resulting <code>KeyValue</code> object would be larger
   * than the provided buffer or than <code>Integer.MAX_VALUE</code>
   */
  public static KeyValue createFirstOnRow(byte[] buffer, final int boffset, final byte[] row,
      final int roffset, final int rlength, final byte[] family, final int foffset,
      final int flength, final byte[] qualifier, final int qoffset, final int qlength)
      throws IllegalArgumentException {

    long lLength = KeyValue.getKeyValueDataStructureSize(rlength, flength, qlength, 0);

    if (lLength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("KeyValue length " + lLength + " > " + Integer.MAX_VALUE);
    }
    int iLength = (int) lLength;
    if (buffer.length - boffset < iLength) {
      throw new IllegalArgumentException("Buffer size " + (buffer.length - boffset) + " < "
          + iLength);
    }

    int len = KeyValue.writeByteArray(buffer, boffset, row, roffset, rlength, family, foffset,
        flength, qualifier, qoffset, qlength, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum,
        null, 0, 0, null);
    return new KeyValue(buffer, boffset, len);
  }

  /**
   * Creates the first KV with the row/family/qualifier of this KV and the
   * given timestamp. Uses the "maximum" KV type that guarantees that the new
   * KV is the lowest possible for this combination of row, family, qualifier,
   * and timestamp. This KV's own timestamp is ignored. While this function
   * copies the value from this KV, it is normally used on key-only KVs.
   */
  public static KeyValue createFirstOnRowColTS(KeyValue kv, long ts) {
    return new KeyValue(
        kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
        kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
        kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
        ts, Type.Maximum, kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
  }

  /*************** misc **********************************/
  /**
   * @param cell
   * @return <code>cell<code> if it is an instance of {@link KeyValue} else we will return a
   * new {@link KeyValue} instance made from <code>cell</code>
   */
  public static KeyValue ensureKeyValue(final Cell cell) {
    if (cell == null) return null;
    return cell instanceof KeyValue? (KeyValue)cell: copyToNewKeyValue(cell);
  }

  public static List<KeyValue> ensureKeyValues(List<Cell> cells) {
    List<KeyValue> lazyList = Lists.transform(cells, new Function<Cell, KeyValue>() {
      @Override
      public KeyValue apply(Cell arg0) {
        return KeyValueUtil.ensureKeyValue(arg0);
      }
    });
    return new ArrayList<KeyValue>(lazyList);
  }
}
