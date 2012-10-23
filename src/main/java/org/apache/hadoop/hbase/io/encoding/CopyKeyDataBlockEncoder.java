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

import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;

/**
 * This "encoder" implementation is used for the case when encoding is turned
 * off. Unencoded block headers are exactly the same as they were before the
 * data block encoding feature was introduced (no special fields for unencoded
 * size or encoding type).
 */
public class CopyKeyDataBlockEncoder extends BufferedDataBlockEncoder {
  @Override
  public void encodeKeyValues(DataOutputStream out,
      ByteBuffer in, boolean includesMemstoreTS) throws IOException {
    in.rewind();
    ByteBufferUtils.moveBufferToStream(out, in, in.remaining());
  }

  @Override
  public int getUnencodedSize(ByteBuffer bufferWithoutHeader) {
    return bufferWithoutHeader.capacity();
  }

  @Override
  public ByteBuffer decodeKeyValues(DataInputStream source,
      int preserveHeaderLength, boolean includesMemstoreTS, int totalEncodedSize)
      throws IOException {
    // Encoded size and decoded size are the same here
    ByteBuffer buffer = ByteBuffer.allocate(totalEncodedSize +
        preserveHeaderLength);
    buffer.position(preserveHeaderLength);
    ByteBufferUtils.copyFromStreamToBuffer(buffer, source, totalEncodedSize);

    return buffer;
  }

  @Override
  public ByteBuffer getFirstKeyInBlock(ByteBuffer block) {
    int keyLength = block.getInt(0);
    return ByteBuffer.wrap(block.array(),
        block.arrayOffset() + 2 * Bytes.SIZEOF_INT, keyLength).slice();
  }

  @Override
  public String toString() {
    return CopyKeyDataBlockEncoder.class.getSimpleName();
  }

  @Override
  public UnencodedWriter createWriter(DataOutputStream out,
      boolean includesMemstoreTS) throws IOException {
    return new UnencodedWriter(out, includesMemstoreTS);
  }

  static class UnencodedWriter extends BufferedEncodedWriter<EncodingState> {
    public UnencodedWriter(DataOutputStream out, boolean includesMemstoreTS)
        throws IOException {
      super(out, includesMemstoreTS);
    }

    @Override
    public void updateInitial(final byte[] key,
        final int keyOffset, final int keyLength, final byte[] value,
        final int valueOffset, final int valueLength) throws IOException {
      // Write out the unencoded data
      out.writeInt(keyLength);
      out.writeInt(valueLength);
      this.out.write(key, keyOffset, keyLength);
      this.out.write(value, valueOffset, valueLength);
    }

    @Override
    public void reserveMetadataSpace() {
      // We are not storing unencoded block length for unencoded blocks.
    }

    @Override
    public boolean finishEncoding(byte[] data, int offset, int length) throws IOException {
      return false;  // no encoding
    }

    @Override
    EncodingState createState() {
      return null;
    }
  }

  @Override
  public EncodedSeeker createSeeker(RawComparator<byte[]> comparator,
      final boolean includesMemstoreTS) {
    return new BufferedEncodedSeeker<SeekerState>(comparator) {
      @Override
      protected void decodeNext() {
        current.keyLength = currentBuffer.getInt();
        current.valueLength = currentBuffer.getInt();
        current.ensureSpaceForKey();
        currentBuffer.get(current.keyBuffer, 0, current.keyLength);
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
        current.lastCommonPrefix = 0;
        decodeNext();
      }
    };
  }

  @Override
  boolean shouldWriteUnencodedLength() {
    // We don't store unencoded length for unencoded blocks.
    return false;
  }
}
