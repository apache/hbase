/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.OffheapKeyValue;
import org.apache.hadoop.hbase.ShareableMemory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Codec that does KeyValue version 1 serialization.
 * 
 * <p>Encodes Cell as serialized in KeyValue with total length prefix.
 * This is how KVs were serialized in Puts, Deletes and Results pre-0.96.  Its what would
 * happen if you called the Writable#write KeyValue implementation.  This encoder will fail
 * if the passed Cell is not an old-school pre-0.96 KeyValue.  Does not copy bytes writing.
 * It just writes them direct to the passed stream.
 *
 * <p>If you wrote two KeyValues to this encoder, it would look like this in the stream:
 * <pre>
 * length-of-KeyValue1 // A java int with the length of KeyValue1 backing array
 * KeyValue1 backing array filled with a KeyValue serialized in its particular format
 * length-of-KeyValue2
 * KeyValue2 backing array
 * </pre>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class KeyValueCodec implements Codec {
  public static class KeyValueEncoder extends BaseEncoder {
    public KeyValueEncoder(final OutputStream out) {
      super(out);
    }

    @Override
    public void write(Cell cell) throws IOException {
      checkFlushed();
      // Do not write tags over RPC
      ByteBufferUtils.putInt(this.out, KeyValueUtil.getSerializedSize(cell, false));
      KeyValueUtil.oswrite(cell, out, false);
    }
  }

  public static class KeyValueDecoder extends BaseDecoder {
    public KeyValueDecoder(final InputStream in) {
      super(in);
    }

    protected Cell parseCell() throws IOException {
      // No tags here
      return KeyValueUtil.iscreate(in, false);
    }
  }

  public static class ByteBuffKeyValueDecoder implements Codec.Decoder {

    protected final ByteBuff buf;
    protected Cell current = null;

    public ByteBuffKeyValueDecoder(ByteBuff buf) {
      this.buf = buf;
    }

    @Override
    public boolean advance() throws IOException {
      if (!this.buf.hasRemaining()) {
        return false;
      }
      int len = buf.getInt();
      ByteBuffer bb = buf.asSubByteBuffer(len);
      if (bb.isDirect()) {
        this.current = createCell(bb, bb.position(), len);
      } else {
        this.current = createCell(bb.array(), bb.arrayOffset() + bb.position(), len);
      }
      buf.skip(len);
      return true;
    }

    @Override
    public Cell current() {
      return this.current;
    }

    protected Cell createCell(byte[] buf, int offset, int len) {
      return new ShareableMemoryNoTagsKeyValue(buf, offset, len);
    }

    protected Cell createCell(ByteBuffer bb, int pos, int len) {
      // We know there is not going to be any tags.
      return new ShareableMemoryOffheapKeyValue(bb, pos, len, false, 0);
    }

    static class ShareableMemoryKeyValue extends KeyValue implements ShareableMemory {
      public ShareableMemoryKeyValue(byte[] bytes, int offset, int length) {
        super(bytes, offset, length);
      }

      @Override
      public Cell cloneToCell() {
        byte[] copy = Bytes.copy(this.bytes, this.offset, this.length);
        KeyValue kv = new KeyValue(copy, 0, copy.length);
        kv.setSequenceId(this.getSequenceId());
        return kv;
      }
    }

    static class ShareableMemoryNoTagsKeyValue extends NoTagsKeyValue implements ShareableMemory {
      public ShareableMemoryNoTagsKeyValue(byte[] bytes, int offset, int length) {
        super(bytes, offset, length);
      }

      @Override
      public Cell cloneToCell() {
        byte[] copy = Bytes.copy(this.bytes, this.offset, this.length);
        KeyValue kv = new NoTagsKeyValue(copy, 0, copy.length);
        kv.setSequenceId(this.getSequenceId());
        return kv;
      }
    }

    static class ShareableMemoryOffheapKeyValue extends OffheapKeyValue implements ShareableMemory {
      public ShareableMemoryOffheapKeyValue(ByteBuffer buf, int offset, int length) {
        super(buf, offset, length);
      }

      public ShareableMemoryOffheapKeyValue(ByteBuffer buf, int offset, int length, boolean hasTags,
          long seqId) {
        super(buf, offset, length, hasTags, seqId);
      }

      @Override
      public Cell cloneToCell() {
        byte[] copy = new byte[this.length];
        ByteBufferUtils.copyFromBufferToArray(copy, this.buf, this.offset, 0, this.length);
        KeyValue kv;
        if (this.hasTags) {
          kv = new KeyValue(copy, 0, copy.length);
        } else {
          kv = new NoTagsKeyValue(copy, 0, copy.length);
        }
        kv.setSequenceId(this.getSequenceId());
        return kv;
      }
    }
  }

  /**
   * Implementation depends on {@link InputStream#available()}
   */
  @Override
  public Decoder getDecoder(final InputStream is) {
    return new KeyValueDecoder(is);
  }

  @Override
  public Decoder getDecoder(ByteBuff buf) {
    return new ByteBuffKeyValueDecoder(buf);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return new KeyValueEncoder(os);
  }
}
