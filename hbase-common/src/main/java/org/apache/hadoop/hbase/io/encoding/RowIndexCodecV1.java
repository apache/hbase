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
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.yetus.audience.InterfaceAudience;

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
public class RowIndexCodecV1 extends AbstractDataBlockEncoder {

  private static class RowIndexEncodingState extends EncodingState {
    RowIndexEncoderV1 encoder = null;

    @Override
    public void beforeShipped() {
      if (encoder != null) {
        encoder.beforeShipped();
      }
    }
  }

  @Override
  public void startBlockEncoding(HFileBlockEncodingContext blkEncodingCtx,
      DataOutputStream out) throws IOException {
    if (blkEncodingCtx.getClass() != HFileBlockDefaultEncodingContext.class) {
      throw new IOException(this.getClass().getName() + " only accepts "
          + HFileBlockDefaultEncodingContext.class.getName() + " as the "
          + "encoding context.");
    }

    HFileBlockDefaultEncodingContext encodingCtx =
      (HFileBlockDefaultEncodingContext) blkEncodingCtx;
    encodingCtx.prepareEncoding(out);

    RowIndexEncoderV1 encoder = new RowIndexEncoderV1(out, encodingCtx);
    RowIndexEncodingState state = new RowIndexEncodingState();
    state.encoder = encoder;
    blkEncodingCtx.setEncodingState(state);
  }

  @Override
  public void encode(Cell cell, HFileBlockEncodingContext encodingCtx,
      DataOutputStream out) throws IOException {
    RowIndexEncodingState state = (RowIndexEncodingState) encodingCtx
        .getEncodingState();
    RowIndexEncoderV1 encoder = state.encoder;
    encoder.write(cell);
  }

  @Override
  public void endBlockEncoding(HFileBlockEncodingContext encodingCtx,
      DataOutputStream out, byte[] uncompressedBytesWithHeader)
      throws IOException {
    RowIndexEncodingState state = (RowIndexEncodingState) encodingCtx
        .getEncodingState();
    RowIndexEncoderV1 encoder = state.encoder;
    encoder.flush();
    postEncoding(encodingCtx);
  }

  @Override
  public ByteBuffer decodeKeyValues(DataInputStream source,
      HFileBlockDecodingContext decodingCtx) throws IOException {
    ByteBuffer sourceAsBuffer = ByteBufferUtils
        .drainInputStreamToBuffer(source);// waste
    sourceAsBuffer.mark();
    if (!decodingCtx.getHFileContext().isIncludesTags()) {
      sourceAsBuffer.position(sourceAsBuffer.limit() - Bytes.SIZEOF_INT);
      int onDiskSize = sourceAsBuffer.getInt();
      sourceAsBuffer.reset();
      ByteBuffer dup = sourceAsBuffer.duplicate();
      dup.position(sourceAsBuffer.position());
      dup.limit(sourceAsBuffer.position() + onDiskSize);
      return dup.slice();
    } else {
      RowIndexSeekerV1 seeker = new RowIndexSeekerV1(decodingCtx);
      seeker.setCurrentBuffer(new SingleByteBuff(sourceAsBuffer));
      List<Cell> kvs = new ArrayList<>();
      kvs.add(seeker.getCell());
      while (seeker.next()) {
        kvs.add(seeker.getCell());
      }
      boolean includesMvcc = decodingCtx.getHFileContext().isIncludesMvcc();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);
      for (Cell cell : kvs) {
        KeyValue currentCell = KeyValueUtil.copyToNewKeyValue(cell);
        out.write(currentCell.getBuffer(), currentCell.getOffset(),
            currentCell.getLength());
        if (includesMvcc) {
          WritableUtils.writeVLong(out, cell.getSequenceId());
        }
      }
      out.flush();
      return ByteBuffer.wrap(baos.getBuffer(), 0, baos.size());
    }
  }

  @Override
  public Cell getFirstKeyCellInBlock(ByteBuff block) {
    block.mark();
    int keyLength = block.getInt();
    block.getInt();
    ByteBuffer key = block.asSubByteBuffer(keyLength).duplicate();
    block.reset();
    return createFirstKeyCell(key, keyLength);
  }

  @Override
  public EncodedSeeker createSeeker(HFileBlockDecodingContext decodingCtx) {
    return new RowIndexSeekerV1(decodingCtx);
  }
}
