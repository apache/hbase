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

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;

/**
 * Codec that just writes out Cell as a protobuf Cell Message.  Does not write the mvcc stamp.
 * Use a different codec if you want that in the stream.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class MessageCodec implements Codec {
  static class MessageEncoder extends BaseEncoder {
    MessageEncoder(final OutputStream out) {
      super(out);
    }

    @Override
    public void write(Cell cell) throws IOException {
      checkFlushed();
      CellProtos.Cell.Builder builder = CellProtos.Cell.newBuilder();
      // This copies bytes from Cell to ByteString.  I don't see anyway around the copy.
      // ByteString is final.
      builder.setRow(ByteStringer.wrap(cell.getRowArray(), cell.getRowOffset(),
          cell.getRowLength()));
      builder.setFamily(ByteStringer.wrap(cell.getFamilyArray(), cell.getFamilyOffset(),
          cell.getFamilyLength()));
      builder.setQualifier(ByteStringer.wrap(cell.getQualifierArray(),
          cell.getQualifierOffset(), cell.getQualifierLength()));
      builder.setTimestamp(cell.getTimestamp());
      builder.setCellType(CellProtos.CellType.valueOf(cell.getTypeByte()));
      builder.setValue(ByteStringer.wrap(cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength()));
      CellProtos.Cell pbcell = builder.build();
      pbcell.writeDelimitedTo(this.out);
    }
  }

  static class MessageDecoder extends BaseDecoder {
    MessageDecoder(final InputStream in) {
      super(in);
    }

    protected Cell parseCell() throws IOException {
      CellProtos.Cell pbcell = CellProtos.Cell.parseDelimitedFrom(this.in);
      return CellUtil.createCell(pbcell.getRow().toByteArray(),
        pbcell.getFamily().toByteArray(), pbcell.getQualifier().toByteArray(),
        pbcell.getTimestamp(), (byte)pbcell.getCellType().getNumber(),
        pbcell.getValue().toByteArray());
    }
  }

  @Override
  public Decoder getDecoder(InputStream is) {
    return new MessageDecoder(is);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return new MessageEncoder(os);
  }
}
