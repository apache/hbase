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

  /**
   * Append single byte 0x00 to the end of the input row key
   */
  public static KeyValue createFirstKeyInNextRow(final Cell in){
    byte[] nextRow = new byte[in.getRowLength() + 1];
    System.arraycopy(in.getRowArray(), in.getRowOffset(), nextRow, 0, in.getRowLength());
    nextRow[nextRow.length - 1] = 0;//maybe not necessary
    return KeyValue.createFirstOnRow(nextRow);
  }

  /**
   * Increment the row bytes and clear the other fields
   */
  public static KeyValue createFirstKeyInIncrementedRow(final Cell in){
    byte[] thisRow = new SimpleByteRange(in.getRowArray(), in.getRowOffset(), in.getRowLength())
        .deepCopyToNewArray();
    byte[] nextRow = Bytes.unsignedCopyAndIncrement(thisRow);
    return KeyValue.createFirstOnRow(nextRow);
  }

  /**
   * Decrement the timestamp.  For tests (currently wasteful)
   *
   * Remember timestamps are sorted reverse chronologically.
   * @param in
   * @return previous key
   */
  public static KeyValue previousKey(final KeyValue in) {
    return KeyValue.createFirstOnRow(CellUtil.cloneRow(in), CellUtil.cloneFamily(in),
      CellUtil.cloneQualifier(in), in.getTimestamp() - 1);
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
