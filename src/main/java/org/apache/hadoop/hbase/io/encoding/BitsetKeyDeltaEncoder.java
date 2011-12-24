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

/**
 * Compress using bit fields to avoid repetitions of certain fields.
 * Also compress the value and key size.
 *
 * Format:
 * <ul>
 * <li> 1 byte:    flag </li>
 * <li> 1-4 bytes: key length </li>
 * <li> 1-4 bytes: value length </li>
 * <li> Key parts which are new (number of bytes varies) </li>
 * <li> Value (number of bytes varies) </li>
 * </ul>
 *
 * In worst case compressed KeyValue will be one byte longer than original.
 */
public class BitsetKeyDeltaEncoder extends BufferedDataBlockEncoder {

  /* Constants used in flag byte */
  static final int SAME_ROW_FLAG = 1;
  static final int SAME_FAMILY_FLAG = 1 << 1;
  static final int SAME_QUALIFIER_FLAG = 1 << 2;
  static final int SAME_TYPE_FLAG = 1 << 3;
  static final int VALUE_SIZE_MASK = (1 << 4) | (1 << 5);
  static final int VALUE_SIZE_SHIFT = 4;
  static final int KEY_SIZE_MASK = (1 << 6) | (1 << 7);
  static final int KEY_SIZE_SHIFT = 6;

  @Override
  public void compressKeyValues(DataOutputStream out,
      ByteBuffer in, boolean includesMemstoreTS)
      throws IOException {
    in.rewind();
    ByteBufferUtils.putInt(out, in.limit());
    CompressionState state = new CompressionState();
    while (in.hasRemaining()) {
      compressSingleKeyValue(state, out, in);
      afterEncodingKeyValue(in, out, includesMemstoreTS);
    }
  }

  @Override
  public ByteBuffer uncompressKeyValues(DataInputStream in,
      int allocHeaderLength, int skipLastBytes, boolean includesMemstoreTS)
          throws IOException {
    int decompressedSize = in.readInt();
    ByteBuffer buffer = ByteBuffer.allocate(decompressedSize +
        allocHeaderLength);
    buffer.position(allocHeaderLength);
    CompressionState state = new CompressionState();
    while (in.available() > skipLastBytes) {
      uncompressSingleKeyValue(in, buffer, state);
      afterDecodingKeyValue(in, buffer, includesMemstoreTS);
    }

    if (in.available() != skipLastBytes) {
      throw new IllegalStateException("Read too much bytes.");
    }

    return buffer;
  }

  private void uncompressSingleKeyValue(DataInputStream in,
      ByteBuffer buffer,
      CompressionState state)
          throws IOException, EncoderBufferTooSmallException {
    byte flag = in.readByte();

    // Read key length
    int keyLengthFitInBytes = 1 + ((flag & KEY_SIZE_MASK) >>> KEY_SIZE_SHIFT);
    int keyLength = ByteBufferUtils.readCompressedInt(in, keyLengthFitInBytes);

    // Read value length
    int valueLengthFitInBytes = 1 +
        ((flag & VALUE_SIZE_MASK) >>> VALUE_SIZE_SHIFT);
    int valueLength =
        ByteBufferUtils.readCompressedInt(in, valueLengthFitInBytes);

    // Create buffer blob and put length and size there.
    ByteBufferUtils.ensureSpace(buffer, keyLength + valueLength
        + KeyValue.ROW_OFFSET);
    buffer.putInt(keyLength);
    buffer.putInt(valueLength);
    int prevElementOffset = state.prevOffset + KeyValue.ROW_OFFSET;
    int prevRowOffset = prevElementOffset;
    prevElementOffset += state.rowLength + KeyValue.ROW_LENGTH_SIZE;

    // Read row
    if (state.isFirst() || (flag & SAME_ROW_FLAG) == 0) {
      state.rowLength = in.readShort();
      buffer.putShort(state.rowLength);
      ByteBufferUtils.copyFromStream(in, buffer, state.rowLength);
    } else {
      ByteBufferUtils.copyFromBuffer(buffer, buffer, prevRowOffset,
          state.rowLength + KeyValue.ROW_LENGTH_SIZE);
    }


    // Read family
    int prevFamilyOffset = prevElementOffset;
    prevElementOffset += state.familyLength + KeyValue.FAMILY_LENGTH_SIZE;

    if (state.isFirst() || (flag & SAME_FAMILY_FLAG) == 0) {
      state.familyLength = in.readByte();
      buffer.put(state.familyLength);
      ByteBufferUtils.copyFromStream(in, buffer, state.familyLength);
    } else {
      ByteBufferUtils.copyFromBuffer(buffer, buffer, prevFamilyOffset,
          state.familyLength + KeyValue.FAMILY_LENGTH_SIZE);
    }

    // Read qualifier
    if (state.isFirst() || (flag & SAME_QUALIFIER_FLAG) == 0) {
      state.qualifierLength = keyLength - state.rowLength - state.familyLength
          - KeyValue.KEY_INFRASTRUCTURE_SIZE;
      ByteBufferUtils.copyFromStream(in, buffer, state.qualifierLength);
    } else {
      ByteBufferUtils.copyFromBuffer(buffer, buffer,
          prevElementOffset, state.qualifierLength);
    }

    // Read timestamp
    ByteBufferUtils.copyFromStream(in, buffer, KeyValue.TIMESTAMP_SIZE);

    // Read type
    if (state.isFirst() || (flag & SAME_TYPE_FLAG) == 0) {
      state.type = in.readByte();
    }
    buffer.put(state.type);

    // Read value
    state.prevOffset = buffer.position() - keyLength - KeyValue.ROW_OFFSET;
    ByteBufferUtils.copyFromStream(in, buffer, valueLength);
  }

  private void compressSingleKeyValue(CompressionState state,
      OutputStream out, ByteBuffer in) throws IOException {
    int kvPos = in.position();
    int keyLength = in.getInt();
    int valueLength = in.getInt();

    byte flags = 0;

    // Key length
    int keyLengthFitsInBytes = ByteBufferUtils.intFitsIn(keyLength);
    flags |= (keyLengthFitsInBytes - 1) << KEY_SIZE_SHIFT;

    // Value length
    int valueLengthFitsInBytes = ByteBufferUtils.intFitsIn(valueLength);
    flags |= (valueLengthFitsInBytes - 1) << VALUE_SIZE_SHIFT;

    if (state.isFirst()) {
      ByteBufferUtils.copyToStream(out, flags);

      writeKeyValueCompressedLengths(out, in,
          keyLengthFitsInBytes, valueLengthFitsInBytes);

      state.readKey(in, keyLength, valueLength);
      ByteBufferUtils.copyToStream(out, in, keyLength);
    } else {
      in.mark(); // beginning of the key
      int prevElementOffset = state.prevOffset + KeyValue.ROW_OFFSET +
          KeyValue.ROW_LENGTH_SIZE;

      // Row same
      short rowLength = in.getShort();
      int prevRowOffset = prevElementOffset;
      prevElementOffset += state.rowLength + KeyValue.FAMILY_LENGTH_SIZE;

      if (ByteBufferUtils.arePartsEqual(in, in.position(), rowLength,
          prevRowOffset, state.rowLength)) {
        flags |= SAME_ROW_FLAG;
      } else {
        state.rowLength = rowLength;
      }
      ByteBufferUtils.skip(in, rowLength);

      // Family same
      byte familyLength = in.get();
      int prevFamilyOffset = prevElementOffset;
      prevElementOffset += state.familyLength;

      if (ByteBufferUtils.arePartsEqual(in,
          in.position(), familyLength,
          prevFamilyOffset, state.familyLength)) {
        flags |= SAME_FAMILY_FLAG;
      } else {
        state.familyLength = familyLength;
      }
      ByteBufferUtils.skip(in, familyLength);

      // Qualifier same
      int qualifierLength = keyLength - rowLength - familyLength -
          KeyValue.KEY_INFRASTRUCTURE_SIZE;
      int prevQualifierOffset = prevElementOffset;
      if (ByteBufferUtils.arePartsEqual(in, in.position(), qualifierLength,
          prevQualifierOffset, state.qualifierLength)) {
        flags |= SAME_QUALIFIER_FLAG;
      } else {
        state.qualifierLength = qualifierLength;
      }
      ByteBufferUtils.skip(in, qualifierLength + KeyValue.TIMESTAMP_SIZE);

      // Type same
      byte type = in.get();
      if (type == state.type) {
        flags |= SAME_TYPE_FLAG;
      } else {
        state.type = type;
      }

      // write it
      ByteBufferUtils.copyToStream(out, flags);
      in.reset(); // return to beginning of the key

      writeKeyValueCompressedLengths(out, in,
          keyLengthFitsInBytes, valueLengthFitsInBytes);

      if ((flags & SAME_ROW_FLAG) == 0) {
        ByteBufferUtils.copyToStream(out, in, rowLength
            + KeyValue.ROW_LENGTH_SIZE);
      } else {
        ByteBufferUtils.skip(in, rowLength + KeyValue.ROW_LENGTH_SIZE);
      }

      if ((flags & SAME_FAMILY_FLAG) == 0) {
        ByteBufferUtils.copyToStream(out, in, familyLength
            + KeyValue.FAMILY_LENGTH_SIZE);
      } else {
        ByteBufferUtils.skip(in, familyLength + KeyValue.FAMILY_LENGTH_SIZE);
      }

      if ((flags & SAME_QUALIFIER_FLAG) == 0) {
        ByteBufferUtils.copyToStream(out, in, qualifierLength);
      } else {
        ByteBufferUtils.skip(in, qualifierLength);
      }

      // Timestamp is always different
      ByteBufferUtils.copyToStream(out, in, KeyValue.TIMESTAMP_SIZE);

      if ((flags & SAME_TYPE_FLAG) == 0) {
        ByteBufferUtils.copyToStream(out, type);
      }
      ByteBufferUtils.skip(in, KeyValue.TYPE_SIZE);
    }

    // Copy value
    state.prevOffset = kvPos;
    ByteBufferUtils.copyToStream(out, in, valueLength);
  }

  private void writeKeyValueCompressedLengths(OutputStream out,
      ByteBuffer in, int keyLengthFitsInBytes,
      int valueLengthFitsInBytes) throws IOException {
    int off = in.position() - KeyValue.ROW_OFFSET;
    ByteBufferUtils.copyToStream(out, in, off + (4 - keyLengthFitsInBytes),
        keyLengthFitsInBytes);
    off += KeyValue.KEY_LENGTH_SIZE;
    ByteBufferUtils.copyToStream(out, in, off + (4 - valueLengthFitsInBytes),
        valueLengthFitsInBytes);
  }

  @Override
  public ByteBuffer getFirstKeyInBlock(ByteBuffer block) {
    block.mark();
    block.position(Bytes.SIZEOF_INT);
    byte flag = block.get();
    int keyLength = ByteBufferUtils.readCompressedInt(
        block, 1 + ((flag & KEY_SIZE_MASK) >>> KEY_SIZE_SHIFT));

    // valueLength
    ByteBufferUtils.readCompressedInt(
        block, 1 + ((flag & VALUE_SIZE_MASK) >>> VALUE_SIZE_SHIFT));
    int pos = block.position();
    block.reset();
    return ByteBuffer.wrap(block.array(), pos, keyLength).slice();
  }

  @Override
  public String toString() {
    return BitsetKeyDeltaEncoder.class.getSimpleName();
  }

  @Override
  public EncodedSeeker createSeeker(RawComparator<byte[]> comparator,
      final boolean includesMemstoreTS) {
    return new BufferedEncodedSeeker(comparator) {
      private int familyLengthWithSize;
      private int rowLengthWithSize;

      private void decode() {
        byte type = 0;

        // Read key and value length
        byte flag = currentBuffer.get();
        int oldKeyLength = current.keyLength;
        int keyLengthFitInBytes = 1 + ((flag & KEY_SIZE_MASK) >>> KEY_SIZE_SHIFT);
        current.keyLength =
            ByteBufferUtils.readCompressedInt(currentBuffer,
                keyLengthFitInBytes);

        // Read value length
        int valueLengthFitInBytes = 1 +
            ((flag & VALUE_SIZE_MASK) >>> VALUE_SIZE_SHIFT);
        current.valueLength =
            ByteBufferUtils.readCompressedInt(currentBuffer,
                valueLengthFitInBytes);

        if (oldKeyLength != current.keyLength && (flag & SAME_TYPE_FLAG) != 0) {
          type = current.keyBuffer[oldKeyLength -1];
        }

        current.lastCommonPrefix = 0;
        switch (flag &
            (SAME_ROW_FLAG | SAME_FAMILY_FLAG | SAME_QUALIFIER_FLAG)) {
            case SAME_ROW_FLAG | SAME_FAMILY_FLAG | SAME_QUALIFIER_FLAG:
              current.lastCommonPrefix = current.keyLength -
                  familyLengthWithSize - rowLengthWithSize - // will be added
                  KeyValue.TIMESTAMP_TYPE_SIZE;
            //$FALL-THROUGH$
            case SAME_ROW_FLAG | SAME_FAMILY_FLAG:
              current.lastCommonPrefix +=
                  familyLengthWithSize + rowLengthWithSize;
            //$FALL-THROUGH$
            case 0: // fall through
              currentBuffer.get(current.keyBuffer, current.lastCommonPrefix,
                  current.keyLength - current.lastCommonPrefix -
                  Bytes.SIZEOF_BYTE);
              break;

            case SAME_FAMILY_FLAG:
            //$FALL-THROUGH$
            case SAME_FAMILY_FLAG | SAME_QUALIFIER_FLAG:

              // find size of new row
              currentBuffer.get(current.keyBuffer, 0, Bytes.SIZEOF_SHORT);
              int oldRowLengthWithSize = rowLengthWithSize;
              rowLengthWithSize = Bytes.toShort(current.keyBuffer) +
                  Bytes.SIZEOF_SHORT;

              // move the column family and qualifier
              int moveLength;
              if ((flag & SAME_QUALIFIER_FLAG) == 0) {
                moveLength = familyLengthWithSize;
              } else {
                moveLength = current.keyLength - rowLengthWithSize -
                    KeyValue.TIMESTAMP_TYPE_SIZE;
              }
              System.arraycopy(current.keyBuffer, oldRowLengthWithSize,
                  current.keyBuffer, rowLengthWithSize, moveLength);

              // copy row
              currentBuffer.get(current.keyBuffer, Bytes.SIZEOF_SHORT,
                  rowLengthWithSize - Bytes.SIZEOF_SHORT);

              // copy qualifier and timestamp
              if ((flag & SAME_QUALIFIER_FLAG) == 0) {
                currentBuffer.get(current.keyBuffer,
                    rowLengthWithSize + familyLengthWithSize,
                    current.keyLength - rowLengthWithSize -
                    familyLengthWithSize - Bytes.SIZEOF_BYTE);
              } else {
                currentBuffer.get(current.keyBuffer,
                    current.keyLength - KeyValue.TIMESTAMP_TYPE_SIZE,
                    Bytes.SIZEOF_LONG);
              }
              break;

            case SAME_QUALIFIER_FLAG:
            //$FALL-THROUGH$
            case SAME_QUALIFIER_FLAG | SAME_ROW_FLAG:
            //$FALL-THROUGH$
            case SAME_ROW_FLAG:
            //$FALL-THROUGH$
            default:
              throw new RuntimeException("Unexpected flag!");
        }

        // we need to save length for the first key
        if ((flag & SAME_ROW_FLAG) == 0) {
          rowLengthWithSize = Bytes.toShort(current.keyBuffer) +
              Bytes.SIZEOF_SHORT;
          familyLengthWithSize = current.keyBuffer[rowLengthWithSize] +
              Bytes.SIZEOF_BYTE;
        } else if ((flag & SAME_FAMILY_FLAG) != 0) {
          familyLengthWithSize = current.keyBuffer[rowLengthWithSize] +
              Bytes.SIZEOF_BYTE;
        }

        // type
        if ((flag & SAME_TYPE_FLAG) == 0) {
          currentBuffer.get(current.keyBuffer,
              current.keyLength - Bytes.SIZEOF_BYTE, Bytes.SIZEOF_BYTE);
        } else if (oldKeyLength != current.keyLength) {
          current.keyBuffer[current.keyLength - Bytes.SIZEOF_BYTE] = type;
        }

        // value
        current.valueOffset = currentBuffer.position();
        ByteBufferUtils.skip(currentBuffer, current.valueLength);

        if (includesMemstoreTS) {
          current.memstoreTS = ByteBufferUtils.readVLong(currentBuffer);
        } else {
          current.memstoreTS = 0;
        }
      }

      @Override
      protected void decodeNext() {
        decode();
      }

      @Override
      protected void decodeFirst() {
        ByteBufferUtils.skip(currentBuffer, Bytes.SIZEOF_INT);
        decode();
      }
    };
  }
}
