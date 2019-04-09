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

import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Codec that does KeyValue version 1 serialization with serializing tags also.
 *
 * <p>
 * Encodes Cell as serialized in KeyValue with total length prefix. This
 * is how KVs were serialized in Puts, Deletes and Results pre-0.96. Its what would happen if you
 * called the Writable#write KeyValue implementation. This encoder will fail if the passed Cell is
 * not an old-school pre-0.96 KeyValue. Does not copy bytes writing. It just writes them direct to
 * the passed stream.
 *
 * <p>
 * If you wrote two KeyValues to this encoder, it would look like this in the stream:
 *
 * <pre>
 * length-of-KeyValue1 // A java int with the length of KeyValue1 backing array
 * KeyValue1 backing array filled with a KeyValue serialized in its particular format
 * length-of-KeyValue2
 * KeyValue2 backing array
 * </pre>
 *
 * Note: The only difference of this with KeyValueCodec is the latter ignores tags in Cells.
 * <b>Use this Codec only at server side.</b>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class KeyValueCodecWithTags implements Codec {
  public static class KeyValueEncoder extends BaseEncoder {
    public KeyValueEncoder(final OutputStream out) {
      super(out);
    }

    @Override
    public void write(Cell cell) throws IOException {
      checkFlushed();
      // Write tags
      ByteBufferUtils.putInt(this.out, KeyValueUtil.getSerializedSize(cell, true));
      KeyValueUtil.oswrite(cell, out, true);
    }
  }

  public static class KeyValueDecoder extends BaseDecoder {
    public KeyValueDecoder(final InputStream in) {
      super(in);
    }

    @Override
    protected Cell parseCell() throws IOException {
      // create KeyValue with tags
      return KeyValueUtil.createKeyValueFromInputStream(in, true);
    }
  }

  public static class ByteBuffKeyValueDecoder extends KeyValueCodec.ByteBuffKeyValueDecoder {

    public ByteBuffKeyValueDecoder(ByteBuff buf) {
      super(buf);
    }

    @Override
    protected Cell createCell(byte[] buf, int offset, int len) {
      return new KeyValue(buf, offset, len);
    }

    @Override
    protected Cell createCell(ByteBuffer bb, int pos, int len) {
      return new ByteBufferKeyValue(bb, pos, len);
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
  public Encoder getEncoder(OutputStream os) {
    return new KeyValueEncoder(os);
  }

  @Override
  public Decoder getDecoder(ByteBuff buf) {
    return new ByteBuffKeyValueDecoder(buf);
  }
}
