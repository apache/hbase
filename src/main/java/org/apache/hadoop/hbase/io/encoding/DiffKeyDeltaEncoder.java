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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;

/**
 * Compress using:
 * - store the size of common prefix
 * - save column family once, it is same within HFile
 * - use integer compression for key, value and prefix lengths (7-bit encoding)
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
public class DiffKeyDeltaEncoder extends BufferedDataBlockEncoder {
  static final int FLAG_SAME_KEY_LENGTH = 1;
  static final int FLAG_SAME_VALUE_LENGTH = 1 << 1;
  static final int FLAG_SAME_TYPE = 1 << 2;
  static final int FLAG_TIMESTAMP_IS_DIFF = 1 << 3;
  static final int MASK_TIMESTAMP_LENGTH = (1 << 4) | (1 << 5) | (1 << 6);
  static final int SHIFT_TIMESTAMP_LENGTH = 4;
  static final int FLAG_TIMESTAMP_SIGN = 1 << 7;

  protected static class DiffEncodingState extends EncodingState {
    long timestamp;
    byte[] familyNameWithSize;

    @Override
    protected void readTimestamp(ByteBuffer in) {
      timestamp = in.getLong();
    }

    @Override
    void copyFrom(EncodingState state) {
      super.copyFrom(state);
      DiffEncodingState state2 = (DiffEncodingState) state;
      timestamp = state2.timestamp;
    }
  }

  private void uncompressSingleKeyValue(DataInputStream source,
      ByteBuffer buffer, DiffEncodingState state)
      throws IOException, EncoderBufferTooSmallException {
    // read the column family at the beginning
    if (state.isFirst()) {
      state.familyLength = source.readByte();
      state.familyNameWithSize =
          new byte[(state.familyLength & 0xff) + KeyValue.FAMILY_LENGTH_SIZE];
      state.familyNameWithSize[0] = state.familyLength;
      source.read(state.familyNameWithSize, KeyValue.FAMILY_LENGTH_SIZE,
          state.familyLength);
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

    int keyOffset = buffer.position();
    ByteBufferUtils.ensureSpace(buffer, keyLength + valueLength + KeyValue.ROW_OFFSET);
    buffer.putInt(keyLength);
    buffer.putInt(valueLength);

    // copy common from previous key
    if (commonPrefix > 0) {
      ByteBufferUtils.copyFromBufferToBuffer(buffer, buffer, state.keyOffset
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

    state.keyOffset = keyOffset;
    state.timestamp = timestamp;
    state.type = type;
    state.keyLength = keyLength;
    state.valueLength = valueLength;
  }

  @Override
  public ByteBuffer decodeKeyValues(DataInputStream source,
      int allocHeaderLength, boolean includesMemstoreTS, int totalEncodedSize)
      throws IOException {
    int skipLastBytes = source.available() - totalEncodedSize;
    Preconditions.checkState(skipLastBytes >= 0, "Requested to skip a negative number of bytes");
    int decompressedSize = source.readInt();
    ByteBuffer buffer = ByteBuffer.allocate(decompressedSize +
        allocHeaderLength);
    buffer.position(allocHeaderLength);
    DiffEncodingState state = new DiffEncodingState();
    while (source.available() > skipLastBytes) {
      uncompressSingleKeyValue(source, buffer, state);
      afterDecodingKeyValue(source, buffer, includesMemstoreTS);
    }

    if (source.available() != skipLastBytes) {
      throw new IOException("Read too many bytes");
    }

    return buffer;
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
  public DiffKeyDeltaEncoderWriter createWriter(DataOutputStream out,
      boolean includesMemstoreTS) throws IOException {
    return new DiffKeyDeltaEncoderWriter(out, includesMemstoreTS);
  }

  /**
   * A writer that incrementally performs Fast Diff Delta Encoding
   */
  private static class DiffKeyDeltaEncoderWriter
      extends BufferedEncodedWriter<DiffEncodingState> {

    public DiffKeyDeltaEncoderWriter(DataOutputStream out,
        boolean includesMemstoreTS) throws IOException {
      super(out, includesMemstoreTS);
    }

    @Override
    DiffEncodingState createState() {
      return new DiffEncodingState();
    }

    @Override
    public void updateInitial(final byte[] key,
        final int keyOffset, final int keyLength, final byte[] value,
        final int valueOffset, final int valueLength) throws IOException {
      ByteBuffer keyBuffer = ByteBuffer.wrap(key, keyOffset, keyLength);
      long timestamp;
      long diffTimestamp = 0;
      int diffTimestampFitsInBytes = 0;
      byte flag = 0;
      int commonPrefix;
      int timestampFitsInBytes;

      if (this.prevState == null) {
        currentState.readKey(keyBuffer, keyLength, valueLength);
        timestamp = currentState.timestamp;
        if (timestamp < 0) {
          flag |= FLAG_TIMESTAMP_SIGN;
          timestamp = -timestamp;
        }
        timestampFitsInBytes = ByteBufferUtils.longFitsIn(timestamp);

        flag |= (timestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH;
        commonPrefix = 0;

        // put column family
        this.out.write(key, keyOffset + currentState.rowLength
            + KeyValue.ROW_LENGTH_SIZE, currentState.familyLength
            + KeyValue.FAMILY_LENGTH_SIZE);
      } else {
        // find a common prefix and skip it
        // don't compress timestamp and type using this prefix
        commonPrefix = getCommonPrefixLength(key, keyOffset, keyLength -
            KeyValue.TIMESTAMP_TYPE_SIZE, this.prevState.key,
            this.prevState.keyOffset, this.prevState.keyLength -
            KeyValue.TIMESTAMP_TYPE_SIZE);

        currentState.readKey(keyBuffer, keyLength, valueLength,
            commonPrefix, this.prevState);
        timestamp = currentState.timestamp;
        boolean negativeTimestamp = timestamp < 0;
        if (negativeTimestamp) {
          timestamp = -timestamp;
        }
        timestampFitsInBytes = ByteBufferUtils.longFitsIn(timestamp);

        if (keyLength == this.prevState.keyLength) {
          flag |= FLAG_SAME_KEY_LENGTH;
        }
        if (valueLength == this.prevState.valueLength) {
          flag |= FLAG_SAME_VALUE_LENGTH;
        }
        if (currentState.type == this.prevState.type) {
          flag |= FLAG_SAME_TYPE;
        }

        // encode timestamp
        diffTimestamp = this.prevState.timestamp - currentState.timestamp;
        boolean negativeDiffTimestamp = diffTimestamp < 0;
        if (negativeDiffTimestamp) {
          diffTimestamp = -diffTimestamp;
        }
        diffTimestampFitsInBytes = ByteBufferUtils.longFitsIn(diffTimestamp);
        if (diffTimestampFitsInBytes < timestampFitsInBytes) {
          flag |= (diffTimestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH;
          flag |= FLAG_TIMESTAMP_IS_DIFF;
          if (negativeDiffTimestamp) {
            flag |= FLAG_TIMESTAMP_SIGN;
          }
        } else {
          flag |= (timestampFitsInBytes - 1) << SHIFT_TIMESTAMP_LENGTH;
          if (negativeTimestamp) {
            flag |= FLAG_TIMESTAMP_SIGN;
          }
        }
      }

      this.out.write(flag);

      if ((flag & FLAG_SAME_KEY_LENGTH) == 0) {
        ByteBufferUtils.putCompressedInt(this.out, keyLength);
      }
      if ((flag & FLAG_SAME_VALUE_LENGTH) == 0) {
        ByteBufferUtils.putCompressedInt(this.out, valueLength);
      }

      ByteBufferUtils.putCompressedInt(this.out, commonPrefix);

      if ((this.prevState == null) ||
          commonPrefix < currentState.rowLength + KeyValue.ROW_LENGTH_SIZE) {
        int restRowLength =
            currentState.rowLength + KeyValue.ROW_LENGTH_SIZE - commonPrefix;
        this.out.write(key, keyOffset + commonPrefix, restRowLength);
        this.out.write(key, keyOffset + commonPrefix + restRowLength +
            currentState.familyLength + KeyValue.FAMILY_LENGTH_SIZE,
            currentState.qualifierLength);
      } else {
        this.out.write(key, keyOffset + commonPrefix, keyLength -
            commonPrefix - KeyValue.TIMESTAMP_TYPE_SIZE);
      }

      if ((flag & FLAG_TIMESTAMP_IS_DIFF) == 0) {
        ByteBufferUtils.putLong(this.out, timestamp, timestampFitsInBytes);
      } else {
        ByteBufferUtils.putLong(this.out, diffTimestamp,
            diffTimestampFitsInBytes);
      }

      if ((flag & FLAG_SAME_TYPE) == 0) {
        this.out.write(currentState.type);
      }

      this.out.write(value, valueOffset, valueLength);

      this.currentState.key = key;
      this.currentState.keyOffset = keyOffset;
    }
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
  public EncodedSeeker createSeeker(RawComparator<byte[]> comparator,
      final boolean includesMemstoreTS) {
    return new BufferedEncodedSeeker<DiffSeekerState>(comparator) {
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

        if (includesMemstoreTS) {
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
}
