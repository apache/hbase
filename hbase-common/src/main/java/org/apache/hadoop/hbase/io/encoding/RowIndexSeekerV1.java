/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder.EncodedSeeker;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;
import org.apache.hadoop.io.WritableUtils;

@InterfaceAudience.Private
public class RowIndexSeekerV1 implements EncodedSeeker {

  private HFileBlockDecodingContext decodingCtx;
  private final KVComparator comparator;

  private ByteBuffer currentBuffer;
  private SeekerState current = new SeekerState(); // always valid
  private SeekerState previous = new SeekerState(); // may not be valid

  private int rowNumber;
  private ByteBuffer rowOffsets = null;

  public RowIndexSeekerV1(KVComparator comparator,
      HFileBlockDecodingContext decodingCtx) {
    this.comparator = comparator;
    this.decodingCtx = decodingCtx;
  }

  @Override
  public void setCurrentBuffer(ByteBuffer buffer) {
    int onDiskSize = Bytes.toIntUnsafe(buffer.array(), buffer.arrayOffset()
        + buffer.limit() - Bytes.SIZEOF_INT);
    // int onDiskSize = buffer.getInt(buffer.limit() - Bytes.SIZEOF_INT);

    // Data part
    ByteBuffer dup = buffer.duplicate();
    dup.position(buffer.position());
    dup.limit(buffer.position() + onDiskSize);
    currentBuffer = dup.slice();
    current.currentBuffer = currentBuffer;
    ByteBufferUtils.skip(buffer, onDiskSize);

    // Row offset
    rowNumber = buffer.getInt();
    // equals Bytes.SIZEOF_INT * rowNumber
    int totalRowOffsetsLength = rowNumber << 2;
    ByteBuffer rowDup = buffer.duplicate();
    rowDup.position(buffer.position());
    rowDup.limit(buffer.position() + totalRowOffsetsLength);
    rowOffsets = rowDup.slice();

    decodeFirst();
  }

  @Override
  public ByteBuffer getKeyDeepCopy() {
    ByteBuffer keyBuffer = ByteBuffer.allocate(current.keyLength);
    keyBuffer.put(current.keyBuffer.getBytes(), current.keyBuffer.getOffset(),
        current.keyLength);
    keyBuffer.rewind();
    return keyBuffer;
  }

  @Override
  public ByteBuffer getValueShallowCopy() {
    ByteBuffer dup = currentBuffer.duplicate();
    dup.position(current.valueOffset);
    dup.limit(current.valueOffset + current.valueLength);
    return dup.slice();
  }

  ByteBuffer getKeyValueBuffer() {
    ByteBuffer kvBuffer = createKVBuffer();
    kvBuffer.putInt(current.keyLength);
    kvBuffer.putInt(current.valueLength);
    kvBuffer.put(current.keyBuffer.getBytes(), current.keyBuffer.getOffset(),
        current.keyLength);
    ByteBufferUtils.copyFromBufferToBuffer(kvBuffer, currentBuffer,
        current.valueOffset, current.valueLength);
    if (current.tagsLength > 0) {
      // Put short as unsigned
      kvBuffer.put((byte) (current.tagsLength >> 8 & 0xff));
      kvBuffer.put((byte) (current.tagsLength & 0xff));
      if (current.tagsOffset != -1) {
        ByteBufferUtils.copyFromBufferToBuffer(kvBuffer, currentBuffer,
            current.tagsOffset, current.tagsLength);
      }
    }
    if (includesMvcc()) {
      ByteBufferUtils.writeVLong(kvBuffer, current.getSequenceId());
    }
    kvBuffer.rewind();
    return kvBuffer;
  }

  protected ByteBuffer createKVBuffer() {
    int kvBufSize = (int) KeyValue.getKeyValueDataStructureSize(
        current.keyLength, current.valueLength, current.tagsLength);
    if (includesMvcc()) {
      kvBufSize += WritableUtils.getVIntSize(current.getSequenceId());
    }
    ByteBuffer kvBuffer = ByteBuffer.allocate(kvBufSize);
    return kvBuffer;
  }

  @Override
  public Cell getKeyValue() {
    return current.toCell();
  }

  @Override
  public void rewind() {
    currentBuffer.rewind();
    decodeFirst();
  }

  @Override
  public boolean next() {
    if (!currentBuffer.hasRemaining()) {
      return false;
    }
    decodeNext();
    previous.invalidate();
    return true;
  }

  @Override
  public int seekToKeyInBlock(byte[] key, int offset, int length,
      boolean seekBefore) {
    return seekToKeyInBlock(new KeyValue.KeyOnlyKeyValue(key, offset, length),
        seekBefore);
  }

  private int binarySearch(Cell seekCell, boolean seekBefore) {
    int low = 0;
    int high = rowNumber - 1;
    int mid = (low + high) >>> 1;
    int comp = 0;
    SimpleMutableByteRange row = new SimpleMutableByteRange();
    while (low <= high) {
      mid = (low + high) >>> 1;
      getRow(mid, row);
      comp = comparator.compareRows(row.getBytes(), row.getOffset(),
          row.getLength(), seekCell.getRowArray(), seekCell.getRowOffset(),
          seekCell.getRowLength());
      if (comp < 0) {
        low = mid + 1;
      } else if (comp > 0) {
        high = mid - 1;
      } else {
        // key found
        if (seekBefore) {
          return mid - 1;
        } else {
          return mid;
        }
      }
    }
    // key not found.
    if (comp > 0) {
      return mid - 1;
    } else {
      return mid;
    }
  }

  private void getRow(int index, SimpleMutableByteRange row) {
    int offset = Bytes.toIntUnsafe(rowOffsets.array(), rowOffsets.arrayOffset()
        + (index << 2)); // index * Bytes.SIZEOF_INT
    int position = currentBuffer.arrayOffset() + offset + Bytes.SIZEOF_LONG;
    short rowLen = Bytes.toShortUnsafe(currentBuffer.array(), position);
    row.set(currentBuffer.array(), position + Bytes.SIZEOF_SHORT, rowLen);
  }

  @Override
  public int seekToKeyInBlock(Cell seekCell, boolean seekBefore) {
    previous.invalidate();
    int index = binarySearch(seekCell, seekBefore);
    if (index < 0) {
      return HConstants.INDEX_KEY_MAGIC; // using optimized index key
    } else {
      int offset = Bytes.toIntUnsafe(rowOffsets.array(),
          rowOffsets.arrayOffset() + (index << 2));
      if (offset != 0) {
        decodeAtPosition(offset);
      }
    }
    do {
      int comp;
      comp = comparator.compareOnlyKeyPortion(seekCell, current.currentKey);
      if (comp == 0) { // exact match
        if (seekBefore) {
          if (!previous.isValid()) {
            // The caller (seekBefore) has to ensure that we are not at the
            // first key in the block.
            throw new IllegalStateException("Cannot seekBefore if "
                + "positioned at the first key in the block: key="
                + Bytes.toStringBinary(seekCell.getRowArray()));
          }
          moveToPrevious();
          return 1;
        }
        return 0;
      }

      if (comp < 0) { // already too large, check previous
        if (previous.isValid()) {
          moveToPrevious();
        } else {
          return HConstants.INDEX_KEY_MAGIC; // using optimized index key
        }
        return 1;
      }

      // move to next, if more data is available
      if (currentBuffer.hasRemaining()) {
        previous.copyFromNext(current);
        decodeNext();
      } else {
        break;
      }
    } while (true);

    // we hit the end of the block, not an exact match
    return 1;
  }

  private void moveToPrevious() {
    if (!previous.isValid()) {
      throw new IllegalStateException(
          "Can move back only once and not in first key in the block.");
    }

    SeekerState tmp = previous;
    previous = current;
    current = tmp;

    // move after last key value
    currentBuffer.position(current.nextKvOffset);
    previous.invalidate();
  }

  @Override
  public int compareKey(KVComparator comparator, byte[] key, int offset,
      int length) {
    return comparator.compareFlatKey(key, offset, length,
        current.keyBuffer.getBytes(), current.keyBuffer.getOffset(),
        current.keyBuffer.getLength());
  }

  @Override
  public int compareKey(KVComparator comparator, Cell key) {
    return comparator.compareOnlyKeyPortion(key, new KeyValue.KeyOnlyKeyValue(
        current.keyBuffer.getBytes(), current.keyBuffer.getOffset(),
        current.keyBuffer.getLength()));
  }

  protected void decodeFirst() {
    decodeNext();
    previous.invalidate();
  }

  protected void decodeAtPosition(int position) {
    currentBuffer.position(position);
    decodeNext();
    previous.invalidate();
  }

  protected void decodeNext() {
    current.startOffset = currentBuffer.position();
    int p = currentBuffer.position() + currentBuffer.arrayOffset();
    long ll = Bytes.toLong(currentBuffer.array(), p);
    // Read top half as an int of key length and bottom int as value length
    current.keyLength = (int) (ll >> Integer.SIZE);
    current.valueLength = (int) (Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
    ByteBufferUtils.skip(currentBuffer, Bytes.SIZEOF_LONG);
    // key part
    current.keyBuffer.set(currentBuffer.array(), currentBuffer.arrayOffset()
        + currentBuffer.position(), current.keyLength);
    ByteBufferUtils.skip(currentBuffer, current.keyLength);
    // value part
    current.valueOffset = currentBuffer.position();
    ByteBufferUtils.skip(currentBuffer, current.valueLength);
    if (includesTags()) {
      decodeTags();
    }
    if (includesMvcc()) {
      current.memstoreTS = ByteBufferUtils.readVLong(currentBuffer);
    } else {
      current.memstoreTS = 0;
    }
    current.nextKvOffset = currentBuffer.position();
    current.setKey(current.keyBuffer.getBytes(), current.keyBuffer.getOffset(),
        current.keyBuffer.getLength());
  }

  protected boolean includesMvcc() {
    return this.decodingCtx.getHFileContext().isIncludesMvcc();
  }

  protected boolean includesTags() {
    return this.decodingCtx.getHFileContext().isIncludesTags();
  }

  protected void decodeTags() {
    current.tagsLength = currentBuffer.getShort();
    current.tagsOffset = currentBuffer.position();
    ByteBufferUtils.skip(currentBuffer, current.tagsLength);
  }

  protected class SeekerState {
    /**
     * The size of a (key length, value length) tuple that prefixes each entry
     * in a data block.
     */
    public final static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;

    protected ByteBuffer currentBuffer;
    protected int startOffset = -1;
    protected int valueOffset = -1;
    protected int keyLength;
    protected int valueLength;
    protected int tagsLength = 0;
    protected int tagsOffset = -1;

    protected SimpleMutableByteRange keyBuffer = new SimpleMutableByteRange();
    protected long memstoreTS;
    protected int nextKvOffset;
    protected KeyValue.KeyOnlyKeyValue currentKey = new KeyValue.KeyOnlyKeyValue();

    protected boolean isValid() {
      return valueOffset != -1;
    }

    protected void invalidate() {
      valueOffset = -1;
      currentKey = new KeyValue.KeyOnlyKeyValue();
      currentBuffer = null;
    }

    protected void setKey(byte[] key, int offset, int length) {
      currentKey.setKey(key, offset, length);
    }

    protected long getSequenceId() {
      return memstoreTS;
    }

    /**
     * Copy the state from the next one into this instance (the previous state
     * placeholder). Used to save the previous state when we are advancing the
     * seeker to the next key/value.
     */
    protected void copyFromNext(SeekerState nextState) {
      keyBuffer.set(nextState.keyBuffer.getBytes(),
          nextState.keyBuffer.getOffset(), nextState.keyBuffer.getLength());
      currentKey.setKey(nextState.keyBuffer.getBytes(),
          nextState.keyBuffer.getOffset(), nextState.keyBuffer.getLength());

      startOffset = nextState.startOffset;
      valueOffset = nextState.valueOffset;
      keyLength = nextState.keyLength;
      valueLength = nextState.valueLength;
      nextKvOffset = nextState.nextKvOffset;
      memstoreTS = nextState.memstoreTS;
      currentBuffer = nextState.currentBuffer;
      tagsOffset = nextState.tagsOffset;
      tagsLength = nextState.tagsLength;
    }

    @Override
    public String toString() {
      return CellUtil.getCellKeyAsString(toCell());
    }

    protected int getCellBufSize() {
      int kvBufSize = KEY_VALUE_LEN_SIZE + keyLength + valueLength;
      if (includesTags()) {
        kvBufSize += Bytes.SIZEOF_SHORT + tagsLength;
      }
      return kvBufSize;
    }

    protected Cell formNoTagsKeyValue() {
      NoTagsKeyValue ret = new NoTagsKeyValue(currentBuffer.array(),
          currentBuffer.arrayOffset() + startOffset, getCellBufSize());
      if (includesMvcc()) {
        ret.setSequenceId(memstoreTS);
      }
      return ret;
    }

    public Cell toCell() {
      if (tagsOffset > 0) {
        KeyValue ret = new KeyValue(currentBuffer.array(),
            currentBuffer.arrayOffset() + startOffset, getCellBufSize());
        if (includesMvcc()) {
          ret.setSequenceId(memstoreTS);
        }
        return ret;
      } else {
        return formNoTagsKeyValue();
      }
    }
  }

}
