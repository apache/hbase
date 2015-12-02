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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Compress key by storing size of common prefix with previous KeyValue
 * and storing raw size of rest.
 *
 * Format:
 * 1-5 bytes: compressed key length minus prefix (7-bit encoding)
 * 1-5 bytes: compressed value length (7-bit encoding)
 * 1-3 bytes: compressed length of common key prefix
 * ... bytes: rest of key (including timestamp)
 * ... bytes: value
 *
 * In a worst case compressed KeyValue will be three bytes longer than original.
 *
 */
@InterfaceAudience.Private
public class PrefixKeyDeltaEncoder extends BufferedDataBlockEncoder {

  @Override
  public int internalEncode(Cell cell, HFileBlockDefaultEncodingContext encodingContext,
      DataOutputStream out) throws IOException {
    int klength = KeyValueUtil.keyLength(cell);
    int vlength = cell.getValueLength();
    EncodingState state = encodingContext.getEncodingState();
    if (state.prevCell == null) {
      // copy the key, there is no common prefix with none
      ByteBufferUtils.putCompressedInt(out, klength);
      ByteBufferUtils.putCompressedInt(out, vlength);
      ByteBufferUtils.putCompressedInt(out, 0);
      CellUtil.writeFlatKey(cell, out);
    } else {
      // find a common prefix and skip it
      int common = CellUtil.findCommonPrefixInFlatKey(cell, state.prevCell, true, true);
      ByteBufferUtils.putCompressedInt(out, klength - common);
      ByteBufferUtils.putCompressedInt(out, vlength);
      ByteBufferUtils.putCompressedInt(out, common);
      writeKeyExcludingCommon(cell, common, out);
    }
    // Write the value part
    CellUtil.writeValue(out, cell, vlength);
    int size = klength + vlength + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE;
    size += afterEncodingKeyValue(cell, out, encodingContext);
    state.prevCell = cell;
    return size;
  }

  private void writeKeyExcludingCommon(Cell cell, int commonPrefix, DataOutputStream out)
      throws IOException {
    short rLen = cell.getRowLength();
    if (commonPrefix < rLen + KeyValue.ROW_LENGTH_SIZE) {
      // Previous and current rows are different. Need to write the differing part followed by
      // cf,q,ts and type
      CellUtil.writeRowKeyExcludingCommon(cell, rLen, commonPrefix, out);
      byte fLen = cell.getFamilyLength();
      out.writeByte(fLen);
      CellUtil.writeFamily(out, cell, fLen);
      CellUtil.writeQualifier(out, cell, cell.getQualifierLength());
      out.writeLong(cell.getTimestamp());
      out.writeByte(cell.getTypeByte());
    } else {
      // The full row key part is common. CF part will be common for sure as we deal with Cells in
      // same family. Just need write the differing part in q, ts and type
      commonPrefix = commonPrefix - (rLen + KeyValue.ROW_LENGTH_SIZE)
          - (cell.getFamilyLength() + KeyValue.FAMILY_LENGTH_SIZE);
      int qLen = cell.getQualifierLength();
      int commonQualPrefix = Math.min(commonPrefix, qLen);
      int qualPartLenToWrite = qLen - commonQualPrefix;
      if (qualPartLenToWrite > 0) {
        CellUtil.writeQualifierSkippingBytes(out, cell, qLen, commonQualPrefix);
      }
      commonPrefix -= commonQualPrefix;
      // Common part in TS also?
      if (commonPrefix > 0) {
        int commonTimestampPrefix = Math.min(commonPrefix, KeyValue.TIMESTAMP_SIZE);
        if (commonTimestampPrefix < KeyValue.TIMESTAMP_SIZE) {
          byte[] curTsBuf = Bytes.toBytes(cell.getTimestamp());
          out.write(curTsBuf, commonTimestampPrefix, KeyValue.TIMESTAMP_SIZE
              - commonTimestampPrefix);
        }
        commonPrefix -= commonTimestampPrefix;
        if (commonPrefix == 0) {
          out.writeByte(cell.getTypeByte());
        }
      } else {
        out.writeLong(cell.getTimestamp());
        out.writeByte(cell.getTypeByte());
      }
    }
  }

  @Override
  protected ByteBuffer internalDecodeKeyValues(DataInputStream source, int allocateHeaderLength,
      int skipLastBytes, HFileBlockDefaultDecodingContext decodingCtx) throws IOException {
    int decompressedSize = source.readInt();
    ByteBuffer buffer = ByteBuffer.allocate(decompressedSize +
        allocateHeaderLength);
    buffer.position(allocateHeaderLength);
    int prevKeyOffset = 0;

    while (source.available() > skipLastBytes) {
      prevKeyOffset = decodeKeyValue(source, buffer, prevKeyOffset);
      afterDecodingKeyValue(source, buffer, decodingCtx);
    }

    if (source.available() != skipLastBytes) {
      throw new IllegalStateException("Read too many bytes.");
    }

    buffer.limit(buffer.position());
    return buffer;
  }

  private int decodeKeyValue(DataInputStream source, ByteBuffer buffer,
      int prevKeyOffset)
          throws IOException, EncoderBufferTooSmallException {
    int keyLength = ByteBufferUtils.readCompressedInt(source);
    int valueLength = ByteBufferUtils.readCompressedInt(source);
    int commonLength = ByteBufferUtils.readCompressedInt(source);
    int keyOffset;
    keyLength += commonLength;

    ensureSpace(buffer, keyLength + valueLength + KeyValue.ROW_OFFSET);

    buffer.putInt(keyLength);
    buffer.putInt(valueLength);

    // copy the prefix
    if (commonLength > 0) {
      keyOffset = buffer.position();
      ByteBufferUtils.copyFromBufferToBuffer(buffer, buffer, prevKeyOffset,
          commonLength);
    } else {
      keyOffset = buffer.position();
    }

    // copy rest of the key and value
    int len = keyLength - commonLength + valueLength;
    ByteBufferUtils.copyFromStreamToBuffer(buffer, source, len);
    return keyOffset;
  }

  @Override
  public Cell getFirstKeyCellInBlock(ByteBuff block) {
    block.mark();
    block.position(Bytes.SIZEOF_INT);
    int keyLength = ByteBuff.readCompressedInt(block);
    // TODO : See if we can avoid these reads as the read values are not getting used
    ByteBuff.readCompressedInt(block);
    int commonLength = ByteBuff.readCompressedInt(block);
    if (commonLength != 0) {
      throw new AssertionError("Nonzero common length in the first key in "
          + "block: " + commonLength);
    }
    ByteBuffer key = block.asSubByteBuffer(keyLength).duplicate();
    block.reset();
    return createFirstKeyCell(key, keyLength);
  }

  @Override
  public String toString() {
    return PrefixKeyDeltaEncoder.class.getSimpleName();
  }

  @Override
  public EncodedSeeker createSeeker(CellComparator comparator,
      final HFileBlockDecodingContext decodingCtx) {
    return new BufferedEncodedSeeker<SeekerState>(comparator, decodingCtx) {
      @Override
      protected void decodeNext() {
        current.keyLength = ByteBuff.readCompressedInt(currentBuffer);
        current.valueLength = ByteBuff.readCompressedInt(currentBuffer);
        current.lastCommonPrefix = ByteBuff.readCompressedInt(currentBuffer);
        current.keyLength += current.lastCommonPrefix;
        current.ensureSpaceForKey();
        currentBuffer.get(current.keyBuffer, current.lastCommonPrefix,
            current.keyLength - current.lastCommonPrefix);
        current.valueOffset = currentBuffer.position();
        currentBuffer.skip(current.valueLength);
        if (includesTags()) {
          decodeTags();
        }
        if (includesMvcc()) {
          current.memstoreTS = ByteBuff.readVLong(currentBuffer);
        } else {
          current.memstoreTS = 0;
        }
        current.nextKvOffset = currentBuffer.position();
      }

      @Override
      protected void decodeFirst() {
        currentBuffer.skip(Bytes.SIZEOF_INT);
        decodeNext();
      }
    };
  }
}
