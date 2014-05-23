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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
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
  public int internalEncode(KeyValue kv, HFileBlockDefaultEncodingContext encodingContext,
      DataOutputStream out) throws IOException {
    byte[] kvBuf = kv.getBuffer();
    int klength = kv.getKeyLength();
    int vlength = kv.getValueLength();
    EncodingState state = encodingContext.getEncodingState();
    if (state.prevKv == null) {
      // copy the key, there is no common prefix with none
      ByteBufferUtils.putCompressedInt(out, klength);
      ByteBufferUtils.putCompressedInt(out, vlength);
      ByteBufferUtils.putCompressedInt(out, 0);
      out.write(kvBuf, kv.getKeyOffset(), klength + vlength);
    } else {
      // find a common prefix and skip it
      int common = ByteBufferUtils.findCommonPrefix(state.prevKv.getBuffer(),
          state.prevKv.getKeyOffset(), state.prevKv.getKeyLength(), kvBuf, kv.getKeyOffset(),
          kv.getKeyLength());
      ByteBufferUtils.putCompressedInt(out, klength - common);
      ByteBufferUtils.putCompressedInt(out, vlength);
      ByteBufferUtils.putCompressedInt(out, common);
      out.write(kvBuf, kv.getKeyOffset() + common, klength - common + vlength);
    }
    int size = klength + vlength + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE;
    size += afterEncodingKeyValue(kv, out, encodingContext);
    state.prevKv = kv;
    return size;
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
  public ByteBuffer getFirstKeyInBlock(ByteBuffer block) {
    block.mark();
    block.position(Bytes.SIZEOF_INT);
    int keyLength = ByteBufferUtils.readCompressedInt(block);
    ByteBufferUtils.readCompressedInt(block);
    int commonLength = ByteBufferUtils.readCompressedInt(block);
    if (commonLength != 0) {
      throw new AssertionError("Nonzero common length in the first key in "
          + "block: " + commonLength);
    }
    int pos = block.position();
    block.reset();
    return ByteBuffer.wrap(block.array(), block.arrayOffset() + pos, keyLength)
        .slice();
  }

  @Override
  public String toString() {
    return PrefixKeyDeltaEncoder.class.getSimpleName();
  }

  @Override
  public EncodedSeeker createSeeker(KVComparator comparator,
      final HFileBlockDecodingContext decodingCtx) {
    return new BufferedEncodedSeeker<SeekerState>(comparator, decodingCtx) {
      @Override
      protected void decodeNext() {
        current.keyLength = ByteBufferUtils.readCompressedInt(currentBuffer);
        current.valueLength = ByteBufferUtils.readCompressedInt(currentBuffer);
        current.lastCommonPrefix =
            ByteBufferUtils.readCompressedInt(currentBuffer);
        current.keyLength += current.lastCommonPrefix;
        current.ensureSpaceForKey();
        currentBuffer.get(current.keyBuffer, current.lastCommonPrefix,
            current.keyLength - current.lastCommonPrefix);
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
        decodeNext();
      }
    };
  }
}
