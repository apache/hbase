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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Encoder similar to {@link DiffKeyDeltaEncoder} but supposedly faster.
 *
 * Compress using:
 * - store size of common prefix
 * - save column family once in the first KeyValue
 * - use integer compression for key, value and prefix (7-bit encoding)
 * - use bits to avoid duplication key length, value length
 *   and type if it same as previous
 * - store in 3 bits length of prefix timestamp
 *    with previous KeyValue's timestamp
 * - one bit which allow to omit value if it is the same
 *
 * Format:
 * - 1 byte:    flag
 * - 1-5 bytes: key length (only if FLAG_SAME_KEY_LENGTH is not set in flag)
 * - 1-5 bytes: value length (only if FLAG_SAME_VALUE_LENGTH is not set in flag)
 * - 1-5 bytes: prefix length
 * - ... bytes: rest of the row (if prefix length is small enough)
 * - ... bytes: qualifier (or suffix depending on prefix length)
 * - 1-8 bytes: timestamp suffix
 * - 1 byte:    type (only if FLAG_SAME_TYPE is not set in the flag)
 * - ... bytes: value (only if FLAG_SAME_VALUE is not set in the flag)
 *
 */
@InterfaceAudience.Private
public class FastDiffDeltaEncoder extends BufferedDataBlockEncoder {
  final int MASK_TIMESTAMP_LENGTH = (1 << 0) | (1 << 1) | (1 << 2);
  final int SHIFT_TIMESTAMP_LENGTH = 0;
  final int FLAG_SAME_KEY_LENGTH = 1 << 3;
  final int FLAG_SAME_VALUE_LENGTH = 1 << 4;
  final int FLAG_SAME_TYPE = 1 << 5;
  final int FLAG_SAME_VALUE = 1 << 6;

  private static class FastDiffCompressionState extends CompressionState {
    byte[] timestamp = new byte[KeyValue.TIMESTAMP_SIZE];
    int prevTimestampOffset;

    @Override
    protected void readTimestamp(ByteBuffer in) {
      in.get(timestamp);
    }

    @Override
    void copyFrom(CompressionState state) {
      super.copyFrom(state);
      FastDiffCompressionState state2 = (FastDiffCompressionState) state;
      System.arraycopy(state2.timestamp, 0, timestamp, 0,
          KeyValue.TIMESTAMP_SIZE);
      prevTimestampOffset = state2.prevTimestampOffset;
    }

    /**
     * Copies the first key/value from the given stream, and initializes
     * decompression state based on it. Assumes that we have already read key
     * and value lengths. Does not set {@link #qualifierLength} (not used by
     * decompression) or {@link #prevOffset} (set by the calle afterwards).
     */
    private void decompressFirstKV(ByteBuffer out, DataInputStream in)
        throws IOException {
      int kvPos = out.position();
      out.putInt(keyLength);
      out.putInt(valueLength);
      prevTimestampOffset = out.position() + keyLength -
          KeyValue.TIMESTAMP_TYPE_SIZE;
      ByteBufferUtils.copyFromStreamToBuffer(out, in, keyLength + valueLength);
      rowLength = out.getShort(kvPos + KeyValue.ROW_OFFSET);
      familyLength = out.get(kvPos + KeyValue.ROW_OFFSET +
          KeyValue.ROW_LENGTH_SIZE + rowLength);
      type = out.get(prevTimestampOffset + KeyValue.TIMESTAMP_SIZE);
    }

  }

  private void compressSingleKeyValue(
        FastDiffCompressionState previousState,
        FastDiffCompressionState currentState,
        OutputStream out, ByteBuffer in) throws IOException {
    currentState.prevOffset = in.position();
    int keyLength = in.getInt();
    int valueOffset =
        currentState.prevOffset + keyLength + KeyValue.ROW_OFFSET;
    int valueLength = in.getInt();
    byte flag = 0;

    if (previousState.isFirst()) {
      // copy the key, there is no common prefix with none
      out.write(flag);
      ByteBufferUtils.putCompressedInt(out, keyLength);
      ByteBufferUtils.putCompressedInt(out, valueLength);
      ByteBufferUtils.putCompressedInt(out, 0);

      currentState.readKey(in, keyLength, valueLength);

      ByteBufferUtils.moveBufferToStream(out, in, keyLength + valueLength);
    } else {
      // find a common prefix and skip it
      int commonPrefix = ByteBufferUtils.findCommonPrefix(in, in.position(),
          previousState.prevOffset + KeyValue.ROW_OFFSET,
          Math.min(keyLength, previousState.keyLength) -
          KeyValue.TIMESTAMP_TYPE_SIZE);

      currentState.readKey(in, keyLength, valueLength,
          commonPrefix, previousState);

      if (keyLength == previousState.keyLength) {
        flag |= FLAG_SAME_KEY_LENGTH;
      }
      if (valueLength == previousState.valueLength) {
        flag |= FLAG_SAME_VALUE_LENGTH;
      }
      if (currentState.type == previousState.type) {
        flag |= FLAG_SAME_TYPE;
      }

      int commonTimestampPrefix = findCommonTimestampPrefix(
          currentState, previousState);
      flag |= commonTimestampPrefix << SHIFT_TIMESTAMP_LENGTH;

      // Check if current and previous values are the same. Compare value
      // length first as an optimization.
      if (valueLength == previousState.valueLength) {
        int previousValueOffset = previousState.prevOffset
            + previousState.keyLength + KeyValue.ROW_OFFSET;
        if (ByteBufferUtils.arePartsEqual(in,
                previousValueOffset, previousState.valueLength,
                valueOffset, valueLength)) {
          flag |= FLAG_SAME_VALUE;
        }
      }

      out.write(flag);
      if ((flag & FLAG_SAME_KEY_LENGTH) == 0) {
        ByteBufferUtils.putCompressedInt(out, keyLength);
      }
      if ((flag & FLAG_SAME_VALUE_LENGTH) == 0) {
        ByteBufferUtils.putCompressedInt(out, valueLength);
      }
      ByteBufferUtils.putCompressedInt(out, commonPrefix);

      ByteBufferUtils.skip(in, commonPrefix);
      if (commonPrefix < currentState.rowLength + KeyValue.ROW_LENGTH_SIZE) {
        // Previous and current rows are different. Copy the differing part of
        // the row, skip the column family, and copy the qualifier.
        ByteBufferUtils.moveBufferToStream(out, in,
            currentState.rowLength + KeyValue.ROW_LENGTH_SIZE - commonPrefix);
        ByteBufferUtils.skip(in, currentState.familyLength +
            KeyValue.FAMILY_LENGTH_SIZE);
        ByteBufferUtils.moveBufferToStream(out, in,
            currentState.qualifierLength);
      } else {
        // The common part includes the whole row. As the column family is the
        // same across the whole file, it will automatically be included in the
        // common prefix, so we need not special-case it here.
        int restKeyLength = keyLength - commonPrefix -
            KeyValue.TIMESTAMP_TYPE_SIZE;
        ByteBufferUtils.moveBufferToStream(out, in, restKeyLength);
      }
      ByteBufferUtils.skip(in, commonTimestampPrefix);
      ByteBufferUtils.moveBufferToStream(out, in,
          KeyValue.TIMESTAMP_SIZE - commonTimestampPrefix);

      // Write the type if it is not the same as before.
      if ((flag & FLAG_SAME_TYPE) == 0) {
        out.write(currentState.type);
      }

      // Write the value if it is not the same as before.
      if ((flag & FLAG_SAME_VALUE) == 0) {
        ByteBufferUtils.copyBufferToStream(out, in, valueOffset, valueLength);
      }

      // Skip key type and value in the input buffer.
      ByteBufferUtils.skip(in, KeyValue.TYPE_SIZE + currentState.valueLength);
    }
  }

  private int findCommonTimestampPrefix(FastDiffCompressionState left,
      FastDiffCompressionState right) {
    int prefixTimestamp = 0;
    while (prefixTimestamp < (KeyValue.TIMESTAMP_SIZE - 1) &&
        left.timestamp[prefixTimestamp]
            == right.timestamp[prefixTimestamp]) {
      prefixTimestamp++;
    }
    return prefixTimestamp; // has to be at most 7 bytes
  }

  private void uncompressSingleKeyValue(DataInputStream source,
      ByteBuffer out, FastDiffCompressionState state)
          throws IOException, EncoderBufferTooSmallException {
    byte flag = source.readByte();
    int prevKeyLength = state.keyLength;

    if ((flag & FLAG_SAME_KEY_LENGTH) == 0) {
      state.keyLength = ByteBufferUtils.readCompressedInt(source);
    }
    if ((flag & FLAG_SAME_VALUE_LENGTH) == 0) {
      state.valueLength = ByteBufferUtils.readCompressedInt(source);
    }
    int commonLength = ByteBufferUtils.readCompressedInt(source);

    ByteBufferUtils.ensureSpace(out, state.keyLength + state.valueLength +
        KeyValue.ROW_OFFSET);

    int kvPos = out.position();

    if (!state.isFirst()) {
      // copy the prefix
      int common;
      int prevOffset;

      if ((flag & FLAG_SAME_VALUE_LENGTH) == 0) {
        out.putInt(state.keyLength);
        out.putInt(state.valueLength);
        prevOffset = state.prevOffset + KeyValue.ROW_OFFSET;
        common = commonLength;
      } else {
        if ((flag & FLAG_SAME_KEY_LENGTH) != 0) {
          prevOffset = state.prevOffset;
          common = commonLength + KeyValue.ROW_OFFSET;
        } else {
          out.putInt(state.keyLength);
          prevOffset = state.prevOffset + KeyValue.KEY_LENGTH_SIZE;
          common = commonLength + KeyValue.KEY_LENGTH_SIZE;
        }
      }

      ByteBufferUtils.copyFromBufferToBuffer(out, out, prevOffset, common);

      // copy the rest of the key from the buffer
      int keyRestLength;
      if (commonLength < state.rowLength + KeyValue.ROW_LENGTH_SIZE) {
        // omit the family part of the key, it is always the same
        int rowWithSizeLength;
        int rowRestLength;

        // check length of row
        if (commonLength < KeyValue.ROW_LENGTH_SIZE) {
          // not yet copied, do it now
          ByteBufferUtils.copyFromStreamToBuffer(out, source,
              KeyValue.ROW_LENGTH_SIZE - commonLength);

          rowWithSizeLength = out.getShort(out.position() -
              KeyValue.ROW_LENGTH_SIZE) + KeyValue.ROW_LENGTH_SIZE;
          rowRestLength = rowWithSizeLength - KeyValue.ROW_LENGTH_SIZE;
        } else {
          // already in kvBuffer, just read it
          rowWithSizeLength = out.getShort(kvPos + KeyValue.ROW_OFFSET) +
              KeyValue.ROW_LENGTH_SIZE;
          rowRestLength = rowWithSizeLength - commonLength;
        }

        // copy the rest of row
        ByteBufferUtils.copyFromStreamToBuffer(out, source, rowRestLength);

        // copy the column family
        ByteBufferUtils.copyFromBufferToBuffer(out, out,
            state.prevOffset + KeyValue.ROW_OFFSET + KeyValue.ROW_LENGTH_SIZE
                + state.rowLength, state.familyLength
                + KeyValue.FAMILY_LENGTH_SIZE);
        state.rowLength = (short) (rowWithSizeLength -
            KeyValue.ROW_LENGTH_SIZE);

        keyRestLength = state.keyLength - rowWithSizeLength -
            state.familyLength -
            (KeyValue.FAMILY_LENGTH_SIZE + KeyValue.TIMESTAMP_TYPE_SIZE);
      } else {
        // prevRowWithSizeLength is the same as on previous row
        keyRestLength = state.keyLength - commonLength -
            KeyValue.TIMESTAMP_TYPE_SIZE;
      }
      // copy the rest of the key, after column family == column qualifier
      ByteBufferUtils.copyFromStreamToBuffer(out, source, keyRestLength);

      // copy timestamp
      int prefixTimestamp =
          (flag & MASK_TIMESTAMP_LENGTH) >>> SHIFT_TIMESTAMP_LENGTH;
      ByteBufferUtils.copyFromBufferToBuffer(out, out,
          state.prevTimestampOffset, prefixTimestamp);
      state.prevTimestampOffset = out.position() - prefixTimestamp;
      ByteBufferUtils.copyFromStreamToBuffer(out, source,
          KeyValue.TIMESTAMP_SIZE - prefixTimestamp);

      // copy the type and value
      if ((flag & FLAG_SAME_TYPE) != 0) {
        out.put(state.type);
        if ((flag & FLAG_SAME_VALUE) != 0) {
          ByteBufferUtils.copyFromBufferToBuffer(out, out, state.prevOffset +
              KeyValue.ROW_OFFSET + prevKeyLength, state.valueLength);
        } else {
          ByteBufferUtils.copyFromStreamToBuffer(out, source,
              state.valueLength);
        }
      } else {
        if ((flag & FLAG_SAME_VALUE) != 0) {
          ByteBufferUtils.copyFromStreamToBuffer(out, source,
              KeyValue.TYPE_SIZE);
          ByteBufferUtils.copyFromBufferToBuffer(out, out, state.prevOffset +
              KeyValue.ROW_OFFSET + prevKeyLength, state.valueLength);
        } else {
          ByteBufferUtils.copyFromStreamToBuffer(out, source,
              state.valueLength + KeyValue.TYPE_SIZE);
        }
        state.type = out.get(state.prevTimestampOffset +
            KeyValue.TIMESTAMP_SIZE);
      }
    } else { // this is the first element
      state.decompressFirstKV(out, source);
    }

    state.prevOffset = kvPos;
  }

  @Override
  public void compressKeyValues(DataOutputStream out,
      ByteBuffer in, boolean includesMemstoreTS) throws IOException {
    in.rewind();
    ByteBufferUtils.putInt(out, in.limit());
    FastDiffCompressionState previousState = new FastDiffCompressionState();
    FastDiffCompressionState currentState = new FastDiffCompressionState();
    while (in.hasRemaining()) {
      compressSingleKeyValue(previousState, currentState,
          out, in);
      afterEncodingKeyValue(in, out, includesMemstoreTS);

      // swap previousState <-> currentState
      FastDiffCompressionState tmp = previousState;
      previousState = currentState;
      currentState = tmp;
    }
  }

  @Override
  public ByteBuffer uncompressKeyValues(DataInputStream source,
      int allocHeaderLength, int skipLastBytes, boolean includesMemstoreTS)
          throws IOException {
    int decompressedSize = source.readInt();
    ByteBuffer buffer = ByteBuffer.allocate(decompressedSize +
        allocHeaderLength);
    buffer.position(allocHeaderLength);
    FastDiffCompressionState state = new FastDiffCompressionState();
    while (source.available() > skipLastBytes) {
      uncompressSingleKeyValue(source, buffer, state);
      afterDecodingKeyValue(source, buffer, includesMemstoreTS);
    }

    if (source.available() != skipLastBytes) {
      throw new IllegalStateException("Read too much bytes.");
    }

    return buffer;
  }

  @Override
  public ByteBuffer getFirstKeyInBlock(ByteBuffer block) {
    block.mark();
    block.position(Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE);
    int keyLength = ByteBufferUtils.readCompressedInt(block);
    ByteBufferUtils.readCompressedInt(block); // valueLength
    ByteBufferUtils.readCompressedInt(block); // commonLength
    int pos = block.position();
    block.reset();
    return ByteBuffer.wrap(block.array(), block.arrayOffset() + pos, keyLength).slice();
  }

  @Override
  public String toString() {
    return FastDiffDeltaEncoder.class.getSimpleName();
  }

  protected static class FastDiffSeekerState extends SeekerState {
    private byte[] prevTimestampAndType =
        new byte[KeyValue.TIMESTAMP_TYPE_SIZE];
    private int rowLengthWithSize;
    private int familyLengthWithSize;

    @Override
    protected void copyFromNext(SeekerState that) {
      super.copyFromNext(that);
      FastDiffSeekerState other = (FastDiffSeekerState) that;
      System.arraycopy(other.prevTimestampAndType, 0,
          prevTimestampAndType, 0,
          KeyValue.TIMESTAMP_TYPE_SIZE);
      rowLengthWithSize = other.rowLengthWithSize;
      familyLengthWithSize = other.familyLengthWithSize;
    }
  }

  @Override
  public EncodedSeeker createSeeker(RawComparator<byte[]> comparator,
      final boolean includesMemstoreTS) {
    return new BufferedEncodedSeeker<FastDiffSeekerState>(comparator) {
      private void decode(boolean isFirst) {
        byte flag = currentBuffer.get();
        if ((flag & FLAG_SAME_KEY_LENGTH) == 0) {
          if (!isFirst) {
            System.arraycopy(current.keyBuffer,
                current.keyLength - current.prevTimestampAndType.length,
                current.prevTimestampAndType, 0,
                current.prevTimestampAndType.length);
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

        if (isFirst) {
          // copy everything
          currentBuffer.get(current.keyBuffer, current.lastCommonPrefix,
              current.keyLength - current.prevTimestampAndType.length);
          current.rowLengthWithSize = Bytes.toShort(current.keyBuffer, 0) +
              Bytes.SIZEOF_SHORT;
          current.familyLengthWithSize =
              current.keyBuffer[current.rowLengthWithSize] + Bytes.SIZEOF_BYTE;
        } else if (current.lastCommonPrefix < Bytes.SIZEOF_SHORT) {
          // length of row is different, copy everything except family

          // copy the row size
          int oldRowLengthWithSize = current.rowLengthWithSize;
          currentBuffer.get(current.keyBuffer, current.lastCommonPrefix,
              Bytes.SIZEOF_SHORT - current.lastCommonPrefix);
          current.rowLengthWithSize = Bytes.toShort(current.keyBuffer, 0) +
              Bytes.SIZEOF_SHORT;

          // move the column family
          System.arraycopy(current.keyBuffer, oldRowLengthWithSize,
              current.keyBuffer, current.rowLengthWithSize,
              current.familyLengthWithSize);

          // copy the rest of row
          currentBuffer.get(current.keyBuffer, Bytes.SIZEOF_SHORT,
              current.rowLengthWithSize - Bytes.SIZEOF_SHORT);

          // copy the qualifier
          currentBuffer.get(current.keyBuffer, current.rowLengthWithSize
              + current.familyLengthWithSize, current.keyLength
              - current.rowLengthWithSize - current.familyLengthWithSize
              - current.prevTimestampAndType.length);
        } else if (current.lastCommonPrefix < current.rowLengthWithSize) {
          // We have to copy part of row and qualifier, but the column family
          // is in the right place.

          // before column family (rest of row)
          currentBuffer.get(current.keyBuffer, current.lastCommonPrefix,
              current.rowLengthWithSize - current.lastCommonPrefix);

          // after column family (qualifier)
          currentBuffer.get(current.keyBuffer, current.rowLengthWithSize
              + current.familyLengthWithSize, current.keyLength
              - current.rowLengthWithSize - current.familyLengthWithSize
              - current.prevTimestampAndType.length);
        } else {
          // copy just the ending
          currentBuffer.get(current.keyBuffer, current.lastCommonPrefix,
              current.keyLength - current.prevTimestampAndType.length
                  - current.lastCommonPrefix);
        }

        // timestamp
        int pos = current.keyLength - current.prevTimestampAndType.length;
        int commonTimestampPrefix = (flag & MASK_TIMESTAMP_LENGTH) >>>
          SHIFT_TIMESTAMP_LENGTH;
        if ((flag & FLAG_SAME_KEY_LENGTH) == 0) {
          System.arraycopy(current.prevTimestampAndType, 0, current.keyBuffer,
              pos, commonTimestampPrefix);
        }
        pos += commonTimestampPrefix;
        currentBuffer.get(current.keyBuffer, pos,
            Bytes.SIZEOF_LONG - commonTimestampPrefix);
        pos += Bytes.SIZEOF_LONG - commonTimestampPrefix;

        // type
        if ((flag & FLAG_SAME_TYPE) == 0) {
          currentBuffer.get(current.keyBuffer, pos, Bytes.SIZEOF_BYTE);
        } else if ((flag & FLAG_SAME_KEY_LENGTH) == 0) {
          current.keyBuffer[pos] =
              current.prevTimestampAndType[Bytes.SIZEOF_LONG];
        }

        // handle value
        if ((flag & FLAG_SAME_VALUE) == 0) {
          current.valueOffset = currentBuffer.position();
          ByteBufferUtils.skip(currentBuffer, current.valueLength);
        }

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
        decode(true);
      }

      @Override
      protected void decodeNext() {
        decode(false);
      }

      @Override
      protected FastDiffSeekerState createSeekerState() {
        return new FastDiffSeekerState();
      }
    };
  }
}
