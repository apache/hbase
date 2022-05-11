/*
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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.io.ByteBuffInputStream;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Basic Cell codec that just writes out all the individual elements of a Cell including the tags.
 * Uses ints delimiting all lengths. Profligate. Needs tune up. <b>Use this Codec only at server
 * side.</b>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class CellCodecWithTags implements Codec {
  static class CellEncoder extends BaseEncoder {
    CellEncoder(final OutputStream out) {
      super(out);
    }

    @Override
    public void write(Cell cell) throws IOException {
      checkFlushed();
      // Row
      write(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
      // Column family
      write(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
      // Qualifier
      write(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
      // Version
      this.out.write(Bytes.toBytes(cell.getTimestamp()));
      // Type
      this.out.write(cell.getTypeByte());
      // Value
      write(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
      // Tags
      write(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
      // MvccVersion
      this.out.write(Bytes.toBytes(cell.getSequenceId()));
    }

    /**
     * Write int length followed by array bytes. nnnn
     */
    private void write(final byte[] bytes, final int offset, final int length) throws IOException {
      this.out.write(Bytes.toBytes(length));
      this.out.write(bytes, offset, length);
    }
  }

  static class CellDecoder extends BaseDecoder {
    private final ExtendedCellBuilder cellBuilder =
      ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);

    public CellDecoder(final InputStream in) {
      super(in);
    }

    @Override
    protected Cell parseCell() throws IOException {
      byte[] row = readByteArray(this.in);
      byte[] family = readByteArray(in);
      byte[] qualifier = readByteArray(in);
      byte[] longArray = new byte[Bytes.SIZEOF_LONG];
      IOUtils.readFully(this.in, longArray);
      long timestamp = Bytes.toLong(longArray);
      byte type = (byte) this.in.read();
      byte[] value = readByteArray(in);
      byte[] tags = readByteArray(in);
      // Read memstore version
      byte[] memstoreTSArray = new byte[Bytes.SIZEOF_LONG];
      IOUtils.readFully(this.in, memstoreTSArray);
      long memstoreTS = Bytes.toLong(memstoreTSArray);
      return cellBuilder.clear().setRow(row).setFamily(family).setQualifier(qualifier)
        .setTimestamp(timestamp).setType(type).setValue(value).setSequenceId(memstoreTS)
        .setTags(tags).build();
    }

    /**
     * @return Byte array read from the stream. n
     */
    private byte[] readByteArray(final InputStream in) throws IOException {
      byte[] intArray = new byte[Bytes.SIZEOF_INT];
      IOUtils.readFully(in, intArray);
      int length = Bytes.toInt(intArray);
      byte[] bytes = new byte[length];
      IOUtils.readFully(in, bytes);
      return bytes;
    }
  }

  @Override
  public Decoder getDecoder(InputStream is) {
    return new CellDecoder(is);
  }

  @Override
  public Decoder getDecoder(ByteBuff buf) {
    return getDecoder(new ByteBuffInputStream(buf));
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return new CellEncoder(os);
  }
}
