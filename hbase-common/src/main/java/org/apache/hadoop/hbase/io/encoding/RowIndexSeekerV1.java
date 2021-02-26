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
import org.apache.hadoop.hbase.ByteBufferKeyOnlyKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.SizeCachedByteBufferKeyValue;
import org.apache.hadoop.hbase.SizeCachedKeyValue;
import org.apache.hadoop.hbase.SizeCachedNoTagsByteBufferKeyValue;
import org.apache.hadoop.hbase.SizeCachedNoTagsKeyValue;
import org.apache.hadoop.hbase.io.encoding.AbstractDataBlockEncoder.AbstractEncodedSeeker;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RowIndexSeekerV1 extends AbstractEncodedSeeker {

  // A temp pair object which will be reused by ByteBuff#asSubByteBuffer calls. This avoids too
  // many object creations.
  protected final ObjectIntPair<ByteBuffer> tmpPair = new ObjectIntPair<>();

  private ByteBuff currentBuffer;
  private SeekerState current = new SeekerState(); // always valid
  private SeekerState previous = new SeekerState(); // may not be valid

  private int rowNumber;
  private ByteBuff rowOffsets = null;
  private final CellComparator cellComparator;

  public RowIndexSeekerV1(HFileBlockDecodingContext decodingCtx) {
    super(decodingCtx);
    this.cellComparator = decodingCtx.getHFileContext().getCellComparator();
  }

  @Override
  public void setCurrentBuffer(ByteBuff buffer) {
    int onDiskSize = buffer.getInt(buffer.limit() - Bytes.SIZEOF_INT);

    // Data part
    ByteBuff dup = buffer.duplicate();
    dup.position(buffer.position());
    dup.limit(buffer.position() + onDiskSize);
    currentBuffer = dup.slice();
    current.currentBuffer = currentBuffer;
    buffer.skip(onDiskSize);

    // Row offset
    rowNumber = buffer.getInt();
    int totalRowOffsetsLength = Bytes.SIZEOF_INT * rowNumber;
    ByteBuff rowDup = buffer.duplicate();
    rowDup.position(buffer.position());
    rowDup.limit(buffer.position() + totalRowOffsetsLength);
    rowOffsets = rowDup.slice();

    decodeFirst();
  }

  @Override
  public Cell getKey() {
    if (current.keyBuffer.hasArray()) {
      return new KeyValue.KeyOnlyKeyValue(current.keyBuffer.array(),
          current.keyBuffer.arrayOffset() + current.keyBuffer.position(),
          current.keyLength);
    } else {
      byte[] key = new byte[current.keyLength];
      ByteBufferUtils.copyFromBufferToArray(key, current.keyBuffer,
          current.keyBuffer.position(), 0, current.keyLength);
      return new KeyValue.KeyOnlyKeyValue(key, 0, current.keyLength);
    }
  }

  @Override
  public ByteBuffer getValueShallowCopy() {
    currentBuffer.asSubByteBuffer(current.valueOffset, current.valueLength,
        tmpPair);
    ByteBuffer dup = tmpPair.getFirst().duplicate();
    dup.position(tmpPair.getSecond());
    dup.limit(tmpPair.getSecond() + current.valueLength);
    return dup.slice();
  }

  @Override
  public Cell getCell() {
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

  private int binarySearch(Cell seekCell, boolean seekBefore) {
    int low = 0;
    int high = rowNumber - 1;
    int mid = low + ((high - low) >> 1);
    int comp = 0;
    while (low <= high) {
      mid = low + ((high - low) >> 1);
      comp = this.cellComparator.compareRows(getRow(mid), seekCell);
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

  private ByteBuffer getRow(int index) {
    int offset = rowOffsets.getIntAfterPosition(index * Bytes.SIZEOF_INT);
    ByteBuff block = currentBuffer.duplicate();
    block.position(offset + Bytes.SIZEOF_LONG);
    short rowLen = block.getShort();
    block.asSubByteBuffer(block.position(), rowLen, tmpPair);
    ByteBuffer row = tmpPair.getFirst();
    row.position(tmpPair.getSecond()).limit(tmpPair.getSecond() + rowLen);
    return row;
  }

  @Override
  public int seekToKeyInBlock(Cell seekCell, boolean seekBefore) {
    previous.invalidate();
    int index = binarySearch(seekCell, seekBefore);
    if (index < 0) {
      return HConstants.INDEX_KEY_MAGIC; // using optimized index key
    } else {
      int offset = rowOffsets.getIntAfterPosition(index * Bytes.SIZEOF_INT);
      if (offset != 0) {
        decodeAtPosition(offset);
      }
    }
    do {
      int comp =
        PrivateCellUtil.compareKeyIgnoresMvcc(this.cellComparator, seekCell, current.currentKey);
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
      throw new IllegalStateException("Can move back only once and not in first key in the block.");
    }

    SeekerState tmp = previous;
    previous = current;
    current = tmp;

    // move after last key value
    currentBuffer.position(current.nextKvOffset);
    previous.invalidate();
  }

  @Override
  public int compareKey(CellComparator comparator, Cell key) {
    return PrivateCellUtil.compareKeyIgnoresMvcc(comparator, key, current.currentKey);
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
    long ll = currentBuffer.getLongAfterPosition(0);
    // Read top half as an int of key length and bottom int as value length
    current.keyLength = (int) (ll >> Integer.SIZE);
    current.valueLength = (int) (Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
    currentBuffer.skip(Bytes.SIZEOF_LONG);
    // key part
    currentBuffer.asSubByteBuffer(currentBuffer.position(), current.keyLength,
        tmpPair);
    ByteBuffer key = tmpPair.getFirst().duplicate();
    key.position(tmpPair.getSecond()).limit(
        tmpPair.getSecond() + current.keyLength);
    current.keyBuffer = key;
    currentBuffer.skip(current.keyLength);
    // value part
    current.valueOffset = currentBuffer.position();
    currentBuffer.skip(current.valueLength);
    if (includesTags()) {
      decodeTags();
    }
    if (includesMvcc()) {
      current.memstoreTS = ByteBufferUtils.readVLong(currentBuffer);
    } else {
      current.memstoreTS = 0;
    }
    current.nextKvOffset = currentBuffer.position();
    current.currentKey.setKey(current.keyBuffer, tmpPair.getSecond(),
        current.keyLength);
  }

  protected void decodeTags() {
    current.tagsLength = currentBuffer.getShortAfterPosition(0);
    currentBuffer.skip(Bytes.SIZEOF_SHORT);
    current.tagsOffset = currentBuffer.position();
    currentBuffer.skip(current.tagsLength);
  }

  private class SeekerState {
    /**
     * The size of a (key length, value length) tuple that prefixes each entry
     * in a data block.
     */
    public final static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;

    protected ByteBuff currentBuffer;
    protected int startOffset = -1;
    protected int valueOffset = -1;
    protected int keyLength;
    protected int valueLength;
    protected int tagsLength = 0;
    protected int tagsOffset = -1;

    protected ByteBuffer keyBuffer = null;
    protected long memstoreTS;
    protected int nextKvOffset;
    // buffer backed keyonlyKV
    private ByteBufferKeyOnlyKeyValue currentKey = new ByteBufferKeyOnlyKeyValue();

    protected boolean isValid() {
      return valueOffset != -1;
    }

    protected void invalidate() {
      valueOffset = -1;
      currentKey = new ByteBufferKeyOnlyKeyValue();
      currentBuffer = null;
    }

    /**
     * Copy the state from the next one into this instance (the previous state placeholder). Used to
     * save the previous state when we are advancing the seeker to the next key/value.
     */
    protected void copyFromNext(SeekerState nextState) {
      keyBuffer = nextState.keyBuffer;
      currentKey.setKey(nextState.keyBuffer,
          nextState.currentKey.getRowPosition() - Bytes.SIZEOF_SHORT,
          nextState.keyLength);

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
      if (includesTags() && tagsLength > 0) {
        kvBufSize += Bytes.SIZEOF_SHORT + tagsLength;
      }
      return kvBufSize;
    }

    public Cell toCell() {
      Cell ret;
      int cellBufSize = getCellBufSize();
      long seqId = 0L;
      if (includesMvcc()) {
        seqId = memstoreTS;
      }
      if (currentBuffer.hasArray()) {
        // TODO : reduce the varieties of KV here. Check if based on a boolean
        // we can handle the 'no tags' case.
        if (tagsLength > 0) {
          // TODO : getRow len here.
          ret = new SizeCachedKeyValue(currentBuffer.array(),
              currentBuffer.arrayOffset() + startOffset, cellBufSize, seqId, keyLength);
        } else {
          ret = new SizeCachedNoTagsKeyValue(currentBuffer.array(),
              currentBuffer.arrayOffset() + startOffset, cellBufSize, seqId, keyLength);
        }
      } else {
        currentBuffer.asSubByteBuffer(startOffset, cellBufSize, tmpPair);
        ByteBuffer buf = tmpPair.getFirst();
        if (buf.isDirect()) {
          // TODO : getRow len here.
          ret = tagsLength > 0
              ? new SizeCachedByteBufferKeyValue(buf, tmpPair.getSecond(), cellBufSize, seqId,
                  keyLength)
              : new SizeCachedNoTagsByteBufferKeyValue(buf, tmpPair.getSecond(), cellBufSize, seqId,
                  keyLength);
        } else {
          if (tagsLength > 0) {
            ret = new SizeCachedKeyValue(buf.array(), buf.arrayOffset()
                + tmpPair.getSecond(), cellBufSize, seqId, keyLength);
          } else {
            ret = new SizeCachedNoTagsKeyValue(buf.array(), buf.arrayOffset()
                + tmpPair.getSecond(), cellBufSize, seqId, keyLength);
          }
        }
      }
      return ret;
    }
  }
}
