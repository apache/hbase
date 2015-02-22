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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

/**
 * Just copy data, do not do any kind of compression. Use for comparison and
 * benchmarking.
 */
@InterfaceAudience.Private
public class CopyKeyDataBlockEncoder extends BufferedDataBlockEncoder {

  @Override
  public int internalEncode(Cell cell, HFileBlockDefaultEncodingContext encodingContext,
      DataOutputStream out) throws IOException {
    int klength = KeyValueUtil.keyLength(cell);
    int vlength = cell.getValueLength();

    out.writeInt(klength);
    out.writeInt(vlength);
    CellUtil.writeFlatKey(cell, out);
    out.write(cell.getValueArray(), cell.getValueOffset(), vlength);
    int size = klength + vlength + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE;
    // Write the additional tag into the stream
    if (encodingContext.getHFileContext().isIncludesTags()) {
      int tagsLength = cell.getTagsLength();
      out.writeShort(tagsLength);
      if (tagsLength > 0) {
        out.write(cell.getTagsArray(), cell.getTagsOffset(), tagsLength);
      }
      size += tagsLength + KeyValue.TAGS_LENGTH_SIZE;
    }
    if (encodingContext.getHFileContext().isIncludesMvcc()) {
      WritableUtils.writeVLong(out, cell.getSequenceId());
      size += WritableUtils.getVIntSize(cell.getSequenceId());
    }
    return size;
  }

  @Override
  public ByteBuffer getFirstKeyInBlock(ByteBuffer block) {
    int keyLength = block.getInt(Bytes.SIZEOF_INT);
    ByteBuffer dup = block.duplicate();
    int pos = 3 * Bytes.SIZEOF_INT;
    dup.position(pos);
    dup.limit(pos + keyLength);
    return dup.slice();
  }

  @Override
  public String toString() {
    return CopyKeyDataBlockEncoder.class.getSimpleName();
  }

  @Override
  public EncodedSeeker createSeeker(KVComparator comparator,
      final HFileBlockDecodingContext decodingCtx) {
    return new BufferedEncodedSeeker<SeekerState>(comparator, decodingCtx) {
      @Override
      protected void decodeNext() {
        current.keyLength = currentBuffer.getInt();
        current.valueLength = currentBuffer.getInt();
        current.ensureSpaceForKey();
        currentBuffer.get(current.keyBuffer, 0, current.keyLength);
        current.valueOffset = currentBuffer.position();
        ByteBufferUtils.skip(currentBuffer, current.valueLength);
        if (includesTags()) {
          // Read short as unsigned, high byte first
          current.tagsLength = ((currentBuffer.get() & 0xff) << 8) ^ (currentBuffer.get() & 0xff);
          ByteBufferUtils.skip(currentBuffer, current.tagsLength);
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
        current.lastCommonPrefix = 0;
        decodeNext();
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
    ByteBufferUtils.copyFromStreamToBuffer(buffer, source, decompressedSize);

    return buffer;
  }
}
