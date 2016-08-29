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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Store cells following every row's start offset, so we can binary search to a row's cells.
 *
 * Format:
 * flat cells
 * integer: number of rows
 * integer: row0's offset
 * integer: row1's offset
 * ....
 * integer: dataSize
 *
*/
@InterfaceAudience.Private
public class RowIndexCodecV1 implements DataBlockEncoder {

  private static class RowIndexEncodingState extends EncodingState {
    RowIndexEncoderV1 encoder = null;
  }

  @Override
  public void startBlockEncoding(HFileBlockEncodingContext blkEncodingCtx,
      DataOutputStream out) throws IOException {
    if (blkEncodingCtx.getClass() != HFileBlockDefaultEncodingContext.class) {
      throw new IOException(this.getClass().getName() + " only accepts "
          + HFileBlockDefaultEncodingContext.class.getName() + " as the "
          + "encoding context.");
    }

    HFileBlockDefaultEncodingContext encodingCtx = (HFileBlockDefaultEncodingContext) blkEncodingCtx;
    encodingCtx.prepareEncoding(out);

    RowIndexEncoderV1 encoder = new RowIndexEncoderV1(out, encodingCtx);
    RowIndexEncodingState state = new RowIndexEncodingState();
    state.encoder = encoder;
    blkEncodingCtx.setEncodingState(state);
  }

  @Override
  public int encode(Cell cell, HFileBlockEncodingContext encodingCtx,
      DataOutputStream out) throws IOException {
    RowIndexEncodingState state = (RowIndexEncodingState) encodingCtx
        .getEncodingState();
    RowIndexEncoderV1 encoder = state.encoder;
    return encoder.write(cell);
  }

  @Override
  public void endBlockEncoding(HFileBlockEncodingContext encodingCtx,
      DataOutputStream out, byte[] uncompressedBytesWithHeader)
      throws IOException {
    RowIndexEncodingState state = (RowIndexEncodingState) encodingCtx
        .getEncodingState();
    RowIndexEncoderV1 encoder = state.encoder;
    encoder.flush();
    if (encodingCtx.getDataBlockEncoding() != DataBlockEncoding.NONE) {
      encodingCtx.postEncoding(BlockType.ENCODED_DATA);
    } else {
      encodingCtx.postEncoding(BlockType.DATA);
    }
  }

  @Override
  public ByteBuffer decodeKeyValues(DataInputStream source,
      HFileBlockDecodingContext decodingCtx) throws IOException {
    if (!decodingCtx.getHFileContext().isIncludesTags()) {
      ByteBuffer sourceAsBuffer = ByteBufferUtils
          .drainInputStreamToBuffer(source);// waste
      sourceAsBuffer.mark();
      sourceAsBuffer.position(sourceAsBuffer.limit() - Bytes.SIZEOF_INT);
      int onDiskSize = sourceAsBuffer.getInt();
      sourceAsBuffer.reset();
      ByteBuffer dup = sourceAsBuffer.duplicate();
      dup.position(sourceAsBuffer.position());
      dup.limit(sourceAsBuffer.position() + onDiskSize);
      return dup.slice();
    } else {
      ByteBuffer sourceAsBuffer = ByteBufferUtils
          .drainInputStreamToBuffer(source);// waste
      sourceAsBuffer.mark();
      RowIndexSeekerV1 seeker = new RowIndexSeekerV1(KeyValue.COMPARATOR,
          decodingCtx);
      seeker.setCurrentBuffer(sourceAsBuffer);
      List<ByteBuffer> kvs = new ArrayList<ByteBuffer>();
      kvs.add(seeker.getKeyValueBuffer());
      while (seeker.next()) {
        kvs.add(seeker.getKeyValueBuffer());
      }
      int totalLength = 0;
      for (ByteBuffer buf : kvs) {
        totalLength += buf.remaining();
      }
      byte[] keyValueBytes = new byte[totalLength];
      ByteBuffer result = ByteBuffer.wrap(keyValueBytes);
      for (ByteBuffer buf : kvs) {
        result.put(buf);
      }
      return result;
    }
  }

  @Override
  public ByteBuffer getFirstKeyInBlock(ByteBuffer block) {
    block.mark();
    int keyLength = block.getInt();
    block.getInt();
    int pos = block.position();
    block.reset();
    ByteBuffer dup = block.duplicate();
    dup.position(pos);
    dup.limit(pos + keyLength);
    return dup.slice();
  }

  @Override
  public EncodedSeeker createSeeker(KVComparator comparator,
      HFileBlockDecodingContext decodingCtx) {
    return new RowIndexSeekerV1(comparator, decodingCtx);
  }

  @Override
  public HFileBlockEncodingContext newDataBlockEncodingContext(
      DataBlockEncoding encoding, byte[] header, HFileContext meta) {
    return new HFileBlockDefaultEncodingContext(encoding, header, meta);
  }

  @Override
  public HFileBlockDecodingContext newDataBlockDecodingContext(HFileContext meta) {
    return new HFileBlockDefaultDecodingContext(meta);
  }

}
