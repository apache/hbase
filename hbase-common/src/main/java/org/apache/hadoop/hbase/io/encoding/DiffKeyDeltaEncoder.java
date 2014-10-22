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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Compress using:
 * - store size of common prefix
 * - save column family once, it is same within HFile
 * - use integer compression for key, value and prefix (7-bit encoding)
 * - use bits to avoid duplication key length, value length
 *   and type if it same as previous
 * - store in 3 bits length of timestamp field
 * - allow diff in timestamp instead of actual value
 *
 * Format:
 * - 1 byte:    flag
 * - 1-5 bytes: key length (only if FLAG_SAME_KEY_LENGTH is not set in flag)
 * - 1-5 bytes: value length (only if FLAG_SAME_VALUE_LENGTH is not set in flag)
 * - 1-5 bytes: prefix length
 * - ... bytes: rest of the row (if prefix length is small enough)
 * - ... bytes: qualifier (or suffix depending on prefix length)
 * - 1-8 bytes: timestamp or diff
 * - 1 byte:    type (only if FLAG_SAME_TYPE is not set in the flag)
 * - ... bytes: value
 */
@InterfaceAudience.Private
public class DiffKeyDeltaEncoder extends BufferedDataBlockEncoder {
  static final int FLAG_SAME_KEY_LENGTH = 1;
  static final int FLAG_SAME_VALUE_LENGTH = 1 << 1;
  static final int FLAG_SAME_TYPE = 1 << 2;
  static final int FLAG_TIMESTAMP_IS_DIFF = 1 << 3;
  static final int MASK_TIMESTAMP_LENGTH = (1 << 4) | (1 << 5) | (1 << 6);
  static final int SHIFT_TIMESTAMP_LENGTH = 4;
  static final int FLAG_TIMESTAMP_SIGN = 1 << 7;

  protected static class DiffCompressionState extends CompressionState {
    long timestamp;
    byte[] familyNameWithSize;

    @Override
    protected void readTimestamp(ByteBuffer in) {
      timestamp = in.getLong();
    }

    @Override
    void copyFrom(CompressionState state) {
      super.copyFrom(state);
      DiffCompressionState state2 = (DiffCompressionState) state;
      timestamp = state2.timestamp;
    }
  }

  private void uncompressSingleKeyValue(DataInputStream source,
      ByteBuffer buffer,
      DiffCompressionState state)
          throws IOException, EncoderBufferTooSmallException {
    // read the column family at the beginning
    if (state.isFirst()) {
      state.familyLength = source.readByte();
      state.familyNameWithSize =
          new byte[(state.familyLength & 0xff) + KeyValue.FAMILY_LENGTH_SIZE];
      state.familyNameWithSize[0] = state.familyLength;
      int read = source.read(state.familyNameWithSize, KeyValue.FAMILY_LENGTH_SIZE,
          state.familyLength);
      assert read == state.familyLength;
    }

    // read flag
    byte flag = source.readByte();

    // read key/value/common lengths
    int keyLength;
    int valueLength;
    if ((flag & FLAG_SAME_KEY_LENGTH) != 0) {
      keyLength = state.keyLength;
    } else {
      keyLength = ByteBufferUtils.readCompressedInt(source);
    }
    if ((flag & FLAG_SAME_VALUE_LENGTH) != 0) {
      valueLength = state.valueLength;
    } else {
      valueLength = ByteBufferUtils.readCompressedInt(source);
    }
    int commonPrefix = ByteBufferUtils.readCompressedInt(source);

    // create KeyValue buffer and fill it prefix
    int keyOffset = buffer.position();
    ensureSpace(buffer, keyLength + valueLength + KeyValue.ROW_OFFSET);
    buffer.putInt(keyLength);
    buffer.putInt(valueLength);

    // copy common from previous key
    if (commonPrefix > 0) {
      ByteBufferUtils.copyFromBufferToBuffer(buffer, buffer, state.prevOffset
          + KeyValue.ROW_OFFSET, commonPrefix);
    }

    // copy the rest of the key from the buffer
    int keyRestLength;
    if (state.isFirst() || commonPrefix <
        state.rowLength + KeyValue.ROW_LENGTH_SIZE) {
      // omit the family part of the key, it is always the same
      short rowLength;
      int rowRestLength;

      // check length of row
      if (commonPrefix < KeyValue.ROW_LENGTH_SIZE) {
        // not yet copied, do it now
        ByteBufferUtils.copyFromStreamToBuffer(buffer, source,
            KeyValue.ROW_LENGTH_SIZE - commonPrefix);
        ByteBufferUtils.skip(buffer, -KeyValue.ROW_LENGTH_SIZE);
        rowLength = buffer.getShort();
        rowRestLength = rowLength;
      } else {
        // already in buffer, just read it
        rowLength = buffer.getShort(keyOffset + KeyValue.ROW_OFFSET);
        rowRestLength = rowLength + KeyValue.ROW_LENGTH_SIZE - commonPrefix;
      }

      // copy the rest of row
      ByteBufferUtils.copyFromStreamToBuffer(buffer, source, rowRestLength);
      state.rowLength = rowLength;

      // copy the column family
      buffer.put(state.familyNameWithSize);

      keyRestLength = keyLength - rowLength -
          state.familyNameWithSize.length -
          (KeyValue.ROW_LENGTH_SIZE + KeyValue.TIMESTAMP_TYPE_SIZE);
    } else {
      // prevRowWithSizeLength is the same as on previous row
      keyRestLength = keyLength - commonPrefix - KeyValue.TIMESTAMP_TYPE_SIZE;
    }
    // copy the rest of the key, after column family -> column qualifier
    ByteBufferUtils.copyFromStreamToBuffer(buffer, source, keyRestLength);

    // handle timestamp
    int timestampFitsInBytes =
        ((flag & MASK_TIMESTAMP_LENGTH) >>> SHIFT_TIMESTAMP_LENGTH) + 1;
    long timestamp = ByteBufferUtils.readLong(source, timestampFitsInBytes);
    if ((flag & FLAG_TIMESTAMP_SIGN) != 0) {
      timestamp = -timestamp;
    }
    if ((flag & FLAG_TIMESTAMP_IS_DIFF) != 0) {
      timestamp = state.timestamp - timestamp;
    }
    buffer.putLong(timestamp);

    // copy the type field
    byte type;
    if ((flag & FLAG_SAME_TYPE) != 0) {
      type = state.type;
    } else {
      type = source.readByte();
    }
    buffer.put(type);

    // copy value part
    ByteBufferUtils.copyFromStreamToBuffer(buffer, source, valueLength);

    state.keyLength = keyLength;
    state.valueLength = valueLength;
    state.prevOffset = keyOffset;
    state.timestamp = timestamp;
    state.type = type;
    // state.qualifier is unused
  }

  @Override
  public int internalEncode(Cell cell, HFileBlockDefaultEncodingContext encodingContext,
      DataOutputStream out) throws IOException {
    EncodingState state = encodingContext.getEncodingState();
    int size = compressSingleKeyValue(out, cell, state.prevCell);
    size += afterEncodingKeyValue(cell, out, encodingContext);
    state.prevCell = cell;
    return size;
  }

  private int compressSingleKeyValue(DataOutputStream out, Cell cell, Cell prevCell)
      throws IOException {
    byte flag = 0;
    int kLength = KeyValueUtil.keyLength(cell);
    int vLength = cell.getValueLength();

    long timestamp;
    long diffTimestamp = 0;
    int diffTimestampFitsInBytes = 0;
    int timestampFitsInBytes;
    int commonPrefix = 0;

    if (prevCell == null) {
      timestamp = cell.getTimestamp();
      if (timestamp < 0) {
        flag |= FLAG_TIMESTAMP_SIGN;
        timestamp = -timestamp;
      }
      timestampFitsInBytes = ByteBufferUtils.longFitsIn(timestamp);
      flag |= (timestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH;
      // put column family
      byte familyLength = cell.getFamilyLength();
      out.write(familyLength);
      out.write(cell.getFamilyArray(), cell.getFamilyOffset(), familyLength);
    } else {
      // Finding common prefix
      int preKeyLength = KeyValueUtil.keyLength(prevCell);
      commonPrefix = CellUtil.findCommonPrefixInFlatKey(cell, prevCell, true, false);
      if (kLength == preKeyLength) {
        flag |= FLAG_SAME_KEY_LENGTH;
      }
      if (vLength == prevCell.getValueLength()) {
        flag |= FLAG_SAME_VALUE_LENGTH;
      }
      if (cell.getTypeByte() == prevCell.getTypeByte()) {
        flag |= FLAG_SAME_TYPE;
      }
      // don't compress timestamp and type using prefix encode timestamp
      timestamp = cell.getTimestamp();
      diffTimestamp = prevCell.getTimestamp() - timestamp;
      boolean negativeTimestamp = timestamp < 0;
      if (negativeTimestamp) {
        timestamp = -timestamp;
      }
      timestampFitsInBytes = ByteBufferUtils.longFitsIn(timestamp);
      boolean minusDiffTimestamp = diffTimestamp < 0;
      if (minusDiffTimestamp) {
        diffTimestamp = -diffTimestamp;
      }
      diffTimestampFitsInBytes = ByteBufferUtils.longFitsIn(diffTimestamp);
      if (diffTimestampFitsInBytes < timestampFitsInBytes) {
        flag |= (diffTimestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH;
        flag |= FLAG_TIMESTAMP_IS_DIFF;
        if (minusDiffTimestamp) {
          flag |= FLAG_TIMESTAMP_SIGN;
        }
      } else {
        flag |= (timestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH;
        if (negativeTimestamp) {
          flag |= FLAG_TIMESTAMP_SIGN;
        }
      }
    }
    out.write(flag);
    if ((flag & FLAG_SAME_KEY_LENGTH) == 0) {
      ByteBufferUtils.putCompressedInt(out, kLength);
    }
    if ((flag & FLAG_SAME_VALUE_LENGTH) == 0) {
      ByteBufferUtils.putCompressedInt(out, vLength);
    }
    ByteBufferUtils.putCompressedInt(out, commonPrefix);
    short rLen = cell.getRowLength();
    if (commonPrefix < rLen + KeyValue.ROW_LENGTH_SIZE) {
      // Previous and current rows are different. Copy the differing part of
      // the row, skip the column family, and copy the qualifier.
      CellUtil.writeRowKeyExcludingCommon(cell, rLen, commonPrefix, out);
      out.write(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    } else {
      // The common part includes the whole row. As the column family is the
      // same across the whole file, it will automatically be included in the
      // common prefix, so we need not special-case it here.
      // What we write here is the non common part of the qualifier
      int commonQualPrefix = commonPrefix - (rLen + KeyValue.ROW_LENGTH_SIZE)
          - (cell.getFamilyLength() + KeyValue.FAMILY_LENGTH_SIZE);
      out.write(cell.getQualifierArray(), cell.getQualifierOffset() + commonQualPrefix,
          cell.getQualifierLength() - commonQualPrefix);
    }
    if ((flag & FLAG_TIMESTAMP_IS_DIFF) == 0) {
      ByteBufferUtils.putLong(out, timestamp, timestampFitsInBytes);
    } else {
      ByteBufferUtils.putLong(out, diffTimestamp, diffTimestampFitsInBytes);
    }

    if ((flag & FLAG_SAME_TYPE) == 0) {
      out.write(cell.getTypeByte());
    }
    out.write(cell.getValueArray(), cell.getValueOffset(), vLength);
    return kLength + vLength + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE;
  }

  @Override
  public ByteBuffer getFirstKeyInBlock(ByteBuffer block) {
    block.mark();
    block.position(Bytes.SIZEOF_INT);
    byte familyLength = block.get();
    ByteBufferUtils.skip(block, familyLength);
    byte flag = block.get();
    int keyLength = ByteBufferUtils.readCompressedInt(block);
    ByteBufferUtils.readCompressedInt(block); // valueLength
    ByteBufferUtils.readCompressedInt(block); // commonLength
    ByteBuffer result = ByteBuffer.allocate(keyLength);

    // copy row
    assert !(result.isDirect());
    int pos = result.arrayOffset();
    block.get(result.array(), pos, Bytes.SIZEOF_SHORT);
    pos += Bytes.SIZEOF_SHORT;
    short rowLength = result.getShort();
    block.get(result.array(), pos, rowLength);
    pos += rowLength;

    // copy family
    int savePosition = block.position();
    block.position(Bytes.SIZEOF_INT);
    block.get(result.array(), pos, familyLength + Bytes.SIZEOF_BYTE);
    pos += familyLength + Bytes.SIZEOF_BYTE;

    // copy qualifier
    block.position(savePosition);
    int qualifierLength =
        keyLength - pos + result.arrayOffset() - KeyValue.TIMESTAMP_TYPE_SIZE;
    block.get(result.array(), pos, qualifierLength);
    pos += qualifierLength;

    // copy the timestamp and type
    int timestampFitInBytes =
        ((flag & MASK_TIMESTAMP_LENGTH) >>> SHIFT_TIMESTAMP_LENGTH) + 1;
    long timestamp = ByteBufferUtils.readLong(block, timestampFitInBytes);
    if ((flag & FLAG_TIMESTAMP_SIGN) != 0) {
      timestamp = -timestamp;
    }
    result.putLong(pos, timestamp);
    pos += Bytes.SIZEOF_LONG;
    block.get(result.array(), pos, Bytes.SIZEOF_BYTE);

    block.reset();
    return result;
  }

  @Override
  public String toString() {
    return DiffKeyDeltaEncoder.class.getSimpleName();
  }

  protected static class DiffSeekerState extends SeekerState {
    private int rowLengthWithSize;
    private long timestamp;

    @Override
    protected void copyFromNext(SeekerState that) {
      super.copyFromNext(that);
      DiffSeekerState other = (DiffSeekerState) that;
      rowLengthWithSize = other.rowLengthWithSize;
      timestamp = other.timestamp;
    }
  }

  @Override
  public EncodedSeeker createSeeker(KVComparator comparator,
      HFileBlockDecodingContext decodingCtx) {
    return new BufferedEncodedSeeker<DiffSeekerState>(comparator, decodingCtx) {
      private byte[] familyNameWithSize;
      private static final int TIMESTAMP_WITH_TYPE_LENGTH =
          Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE;

      private void decode(boolean isFirst) {
        byte flag = currentBuffer.get();
        byte type = 0;
        if ((flag & FLAG_SAME_KEY_LENGTH) == 0) {
          if (!isFirst) {
            type = current.keyBuffer[current.keyLength - Bytes.SIZEOF_BYTE];
          }
          current.keyLength = ByteBufferUtils.readCompressedInt(currentBuffer);
        }
        if ((flag & FLAG_SAME_VALUE_LENGTH) == 0) {
          current.valueLength =
              ByteBufferUtils.readCompressedInt(currentBuffer);
        }
        current.lastCommonPrefix =
            ByteBufferUtils.readCompressedInt(currentBuffer);

        current.ensureSpaceForKey();

        if (current.lastCommonPrefix < Bytes.SIZEOF_SHORT) {
          // length of row is different, copy everything except family

          // copy the row size
          currentBuffer.get(current.keyBuffer, current.lastCommonPrefix,
              Bytes.SIZEOF_SHORT - current.lastCommonPrefix);
          current.rowLengthWithSize = Bytes.toShort(current.keyBuffer, 0) +
              Bytes.SIZEOF_SHORT;

          // copy the rest of row
          currentBuffer.get(current.keyBuffer, Bytes.SIZEOF_SHORT,
              current.rowLengthWithSize - Bytes.SIZEOF_SHORT);

          // copy the column family
          System.arraycopy(familyNameWithSize, 0, current.keyBuffer,
              current.rowLengthWithSize, familyNameWithSize.length);

          // copy the qualifier
          currentBuffer.get(current.keyBuffer,
              current.rowLengthWithSize + familyNameWithSize.length,
              current.keyLength - current.rowLengthWithSize -
              familyNameWithSize.length - TIMESTAMP_WITH_TYPE_LENGTH);
        } else if (current.lastCommonPrefix < current.rowLengthWithSize) {
          // we have to copy part of row and qualifier,
          // but column family is in right place

          // before column family (rest of row)
          currentBuffer.get(current.keyBuffer, current.lastCommonPrefix,
              current.rowLengthWithSize - current.lastCommonPrefix);

          // after column family (qualifier)
          currentBuffer.get(current.keyBuffer,
              current.rowLengthWithSize + familyNameWithSize.length,
              current.keyLength - current.rowLengthWithSize -
              familyNameWithSize.length - TIMESTAMP_WITH_TYPE_LENGTH);
        } else {
          // copy just the ending
          currentBuffer.get(current.keyBuffer, current.lastCommonPrefix,
              current.keyLength - TIMESTAMP_WITH_TYPE_LENGTH -
              current.lastCommonPrefix);
        }

        // timestamp
        int pos = current.keyLength - TIMESTAMP_WITH_TYPE_LENGTH;
        int timestampFitInBytes = 1 +
            ((flag & MASK_TIMESTAMP_LENGTH) >>> SHIFT_TIMESTAMP_LENGTH);
        long timestampOrDiff =
            ByteBufferUtils.readLong(currentBuffer, timestampFitInBytes);
        if ((flag & FLAG_TIMESTAMP_SIGN) != 0) {
          timestampOrDiff = -timestampOrDiff;
        }
        if ((flag & FLAG_TIMESTAMP_IS_DIFF) == 0) { // it is timestamp
          current.timestamp = timestampOrDiff;
        } else { // it is diff
          current.timestamp = current.timestamp - timestampOrDiff;
        }
        Bytes.putLong(current.keyBuffer, pos, current.timestamp);
        pos += Bytes.SIZEOF_LONG;

        // type
        if ((flag & FLAG_SAME_TYPE) == 0) {
          currentBuffer.get(current.keyBuffer, pos, Bytes.SIZEOF_BYTE);
        } else if ((flag & FLAG_SAME_KEY_LENGTH) == 0) {
          current.keyBuffer[pos] = type;
        }

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
      }

      @Override
      protected void decodeFirst() {
        ByteBufferUtils.skip(currentBuffer, Bytes.SIZEOF_INT);

        // read column family
        byte familyNameLength = currentBuffer.get();
        familyNameWithSize = new byte[familyNameLength + Bytes.SIZEOF_BYTE];
        familyNameWithSize[0] = familyNameLength;
        currentBuffer.get(familyNameWithSize, Bytes.SIZEOF_BYTE,
            familyNameLength);
        decode(true);
      }

      @Override
      protected void decodeNext() {
        decode(false);
      }

      @Override
      protected DiffSeekerState createSeekerState() {
        return new DiffSeekerState();
      }
    };
  }

  @Override
  protected ByteBuffer internalDecodeKeyValues(DataInputStream source, int allocateHeaderLength,
      int skipLastBytes, HFileBlockDefaultDecodingContext decodingCtx) throws IOException {
    int decompressedSize = source.readInt();
    ByteBuffer buffer = ByteBuffer.allocate(decompressedSize +
        allocateHeaderLength);
    buffer.position(allocateHeaderLength);
    DiffCompressionState state = new DiffCompressionState();
    while (source.available() > skipLastBytes) {
      uncompressSingleKeyValue(source, buffer, state);
      afterDecodingKeyValue(source, buffer, decodingCtx);
    }

    if (source.available() != skipLastBytes) {
      throw new IllegalStateException("Read too much bytes.");
    }

    return buffer;
  }
}
