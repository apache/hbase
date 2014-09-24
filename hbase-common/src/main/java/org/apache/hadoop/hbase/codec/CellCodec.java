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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Basic Cell codec that just writes out all the individual elements of a Cell.  Uses ints
 * delimiting all lengths. Profligate. Needs tune up.
 * Note: This will not write tags of a Cell.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class CellCodec implements Codec {
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
      // MvccVersion
      this.out.write(Bytes.toBytes(cell.getMvccVersion()));
    }

    /**
     * Write int length followed by array bytes.
     * @param bytes
     * @param offset
     * @param length
     * @throws IOException
     */
    private void write(final byte [] bytes, final int offset, final int length)
    throws IOException {
      this.out.write(Bytes.toBytes(length));
      this.out.write(bytes, offset, length);
    }
  }

  static class CellDecoder extends BaseDecoder {
    public CellDecoder(final InputStream in) {
      super(in);
    }

    protected Cell parseCell() throws IOException {
      byte [] row = readByteArray(this.in);
      byte [] family = readByteArray(in);
      byte [] qualifier = readByteArray(in);
      byte [] longArray = new byte[Bytes.SIZEOF_LONG];
      IOUtils.readFully(this.in, longArray);
      long timestamp = Bytes.toLong(longArray);
      byte type = (byte) this.in.read();
      byte[] value = readByteArray(in);
      // Read memstore version
      byte[] memstoreTSArray = new byte[Bytes.SIZEOF_LONG];
      IOUtils.readFully(this.in, memstoreTSArray);
      long memstoreTS = Bytes.toLong(memstoreTSArray);
      return CellUtil.createCell(row, family, qualifier, timestamp, type, value, memstoreTS);
    }

    /**
     * @return Byte array read from the stream.
     * @throws IOException
     */
    private byte [] readByteArray(final InputStream in) throws IOException {
      byte [] intArray = new byte[Bytes.SIZEOF_INT];
      IOUtils.readFully(in, intArray);
      int length = Bytes.toInt(intArray);
      byte [] bytes = new byte [length];
      IOUtils.readFully(in, bytes);
      return bytes;
    }
  }

  @Override
  public Decoder getDecoder(InputStream is) {
    return new CellDecoder(is);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return new CellEncoder(os);
  }
}
