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
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Just copy data, do not do any kind of compression. Use for comparison and
 * benchmarking.
 */
@InterfaceAudience.Private
public class CopyKeyDataBlockEncoder extends BufferedDataBlockEncoder {

  private static class CopyKeyEncodingState extends EncodingState {
    NoneEncoder encoder = null;
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

    NoneEncoder encoder = new NoneEncoder(out, encodingCtx);
    CopyKeyEncodingState state = new CopyKeyEncodingState();
    state.encoder = encoder;
    blkEncodingCtx.setEncodingState(state);
  }

  @Override
  public int internalEncode(Cell cell,
      HFileBlockDefaultEncodingContext encodingContext, DataOutputStream out)
      throws IOException {
    CopyKeyEncodingState state = (CopyKeyEncodingState) encodingContext
        .getEncodingState();
    NoneEncoder encoder = state.encoder;
    return encoder.write(cell);
  }

  @Override
  public Cell getFirstKeyCellInBlock(ByteBuff block) {
    int keyLength = block.getIntAfterPosition(Bytes.SIZEOF_INT);
    int pos = 3 * Bytes.SIZEOF_INT;
    ByteBuffer key = block.asSubByteBuffer(pos + keyLength).duplicate();
    return createFirstKeyCell(key, keyLength);
  }

  @Override
  public String toString() {
    return CopyKeyDataBlockEncoder.class.getSimpleName();
  }

  @Override
  public EncodedSeeker createSeeker(final HFileBlockDecodingContext decodingCtx) {
    return new SeekerStateBufferedEncodedSeeker(decodingCtx);
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

  private static class SeekerStateBufferedEncodedSeeker
      extends BufferedEncodedSeeker<SeekerState> {

    private SeekerStateBufferedEncodedSeeker(HFileBlockDecodingContext decodingCtx) {
      super(decodingCtx);
    }

    @Override
    protected void decodeNext() {
      current.keyLength = currentBuffer.getInt();
      current.valueLength = currentBuffer.getInt();
      current.ensureSpaceForKey();
      currentBuffer.get(current.keyBuffer, 0, current.keyLength);
      current.valueOffset = currentBuffer.position();
      currentBuffer.skip(current.valueLength);
      if (includesTags()) {
        // Read short as unsigned, high byte first
        current.tagsLength = ((currentBuffer.get() & 0xff) << 8) ^ (currentBuffer.get() & 0xff);
        currentBuffer.skip(current.tagsLength);
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
      currentBuffer.skip(Bytes.SIZEOF_INT);
      current.lastCommonPrefix = 0;
      decodeNext();
    }
  }

}
