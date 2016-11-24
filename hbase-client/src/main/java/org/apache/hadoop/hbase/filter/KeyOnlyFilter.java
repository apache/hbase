/*
 *
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
package org.apache.hadoop.hbase.filter;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hadoop.hbase.ByteBufferCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException;

/**
 * A filter that will only return the key component of each KV (the value will
 * be rewritten as empty).
 * <p>
 * This filter can be used to grab all of the keys without having to also grab
 * the values.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyOnlyFilter extends FilterBase {

  boolean lenAsVal;
  public KeyOnlyFilter() { this(false); }
  public KeyOnlyFilter(boolean lenAsVal) { this.lenAsVal = lenAsVal; }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
    return false;
  }

  @Override
  public Cell transformCell(Cell cell) {
    return createKeyOnlyCell(cell);
  }

  private Cell createKeyOnlyCell(Cell c) {
    if (c instanceof ByteBufferCell) {
      return new KeyOnlyByteBufferCell((ByteBufferCell) c, lenAsVal);
    } else {
      return new KeyOnlyCell(c, lenAsVal);
    }
  }

  @Override
  public ReturnCode filterKeyValue(Cell ignored) throws IOException {
    return ReturnCode.INCLUDE;
  }
  
  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument((filterArguments.size() == 0 || filterArguments.size() == 1),
                                "Expected: 0 or 1 but got: %s", filterArguments.size());
    KeyOnlyFilter filter = new KeyOnlyFilter();
    if (filterArguments.size() == 1) {
      filter.lenAsVal = ParseFilter.convertByteArrayToBoolean(filterArguments.get(0));
    }
    return filter;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    FilterProtos.KeyOnlyFilter.Builder builder =
      FilterProtos.KeyOnlyFilter.newBuilder();
    builder.setLenAsVal(this.lenAsVal);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link KeyOnlyFilter} instance
   * @return An instance of {@link KeyOnlyFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static KeyOnlyFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.KeyOnlyFilter proto;
    try {
      proto = FilterProtos.KeyOnlyFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new KeyOnlyFilter(proto.getLenAsVal());
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof KeyOnlyFilter)) return false;

    KeyOnlyFilter other = (KeyOnlyFilter)o;
    return this.lenAsVal == other.lenAsVal;
  }

  static class KeyOnlyCell implements Cell {
    private Cell cell;
    private boolean lenAsVal;

    public KeyOnlyCell(Cell c, boolean lenAsVal) {
      this.cell = c;
      this.lenAsVal = lenAsVal;
    }

    @Override
    public byte[] getRowArray() {
      return cell.getRowArray();
    }

    @Override
    public int getRowOffset() {
      return cell.getRowOffset();
    }

    @Override
    public short getRowLength() {
      return cell.getRowLength();
    }

    @Override
    public byte[] getFamilyArray() {
      return cell.getFamilyArray();
    }

    @Override
    public int getFamilyOffset() {
      return cell.getFamilyOffset();
    }

    @Override
    public byte getFamilyLength() {
      return cell.getFamilyLength();
    }

    @Override
    public byte[] getQualifierArray() {
      return cell.getQualifierArray();
    }

    @Override
    public int getQualifierOffset() {
      return cell.getQualifierOffset();
    }

    @Override
    public int getQualifierLength() {
      return cell.getQualifierLength();
    }

    @Override
    public long getTimestamp() {
      return cell.getTimestamp();
    }

    @Override
    public byte getTypeByte() {
      return cell.getTypeByte();
    }

    @Override
    public long getSequenceId() {
      return 0;
    }

    @Override
    public byte[] getValueArray() {
      if (lenAsVal) {
        return Bytes.toBytes(cell.getValueLength());
      } else {
        return HConstants.EMPTY_BYTE_ARRAY;
      }
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      if (lenAsVal) {
        return Bytes.SIZEOF_INT;
      } else {
        return 0;
      }
    }

    @Override
    public byte[] getTagsArray() {
      return HConstants.EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      return 0;
    }
  }

  static class KeyOnlyByteBufferCell extends ByteBufferCell {
    private ByteBufferCell cell;
    private boolean lenAsVal;

    public KeyOnlyByteBufferCell(ByteBufferCell c, boolean lenAsVal) {
      this.cell = c;
      this.lenAsVal = lenAsVal;
    }

    @Override
    public byte[] getRowArray() {
      return cell.getRowArray();
    }

    @Override
    public int getRowOffset() {
      return cell.getRowOffset();
    }

    @Override
    public short getRowLength() {
      return cell.getRowLength();
    }

    @Override
    public byte[] getFamilyArray() {
      return cell.getFamilyArray();
    }

    @Override
    public int getFamilyOffset() {
      return cell.getFamilyOffset();
    }

    @Override
    public byte getFamilyLength() {
      return cell.getFamilyLength();
    }

    @Override
    public byte[] getQualifierArray() {
      return cell.getQualifierArray();
    }

    @Override
    public int getQualifierOffset() {
      return cell.getQualifierOffset();
    }

    @Override
    public int getQualifierLength() {
      return cell.getQualifierLength();
    }

    @Override
    public long getTimestamp() {
      return cell.getTimestamp();
    }

    @Override
    public byte getTypeByte() {
      return cell.getTypeByte();
    }

    @Override
    public long getSequenceId() {
      return 0;
    }

    @Override
    public byte[] getValueArray() {
      if (lenAsVal) {
        return Bytes.toBytes(cell.getValueLength());
      } else {
        return HConstants.EMPTY_BYTE_ARRAY;
      }
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      if (lenAsVal) {
        return Bytes.SIZEOF_INT;
      } else {
        return 0;
      }
    }

    @Override
    public byte[] getTagsArray() {
      return HConstants.EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      return 0;
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return cell.getRowByteBuffer();
    }

    @Override
    public int getRowPosition() {
      return cell.getRowPosition();
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return cell.getFamilyByteBuffer();
    }

    @Override
    public int getFamilyPosition() {
      return cell.getFamilyPosition();
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return cell.getQualifierByteBuffer();
    }

    @Override
    public int getQualifierPosition() {
      return cell.getQualifierPosition();
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      if (lenAsVal) {
        return ByteBuffer.wrap(Bytes.toBytes(cell.getValueLength()));
      } else {
        return HConstants.EMPTY_BYTE_BUFFER;
      }
    }

    @Override
    public int getValuePosition() {
      return 0;
    }

    @Override
    public ByteBuffer getTagsByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getTagsPosition() {
      return 0;
    }
  }

}
