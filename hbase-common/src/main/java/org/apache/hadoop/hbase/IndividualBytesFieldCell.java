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

package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.ByteBufferUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

@InterfaceAudience.Private
public class IndividualBytesFieldCell implements ExtendedCell {

  private static final long FIXED_OVERHEAD = ClassSize.align(  // do alignment(padding gap)
        ClassSize.OBJECT              // object header
      + KeyValue.TIMESTAMP_TYPE_SIZE  // timestamp and type
      + Bytes.SIZEOF_LONG             // sequence id
      + 5 * ClassSize.REFERENCE);     // references to all byte arrays: row, family, qualifier, value, tags

  // The following fields are backed by individual byte arrays
  private byte[] row;
  private byte[] family;
  private byte[] qualifier;
  private byte[] value;
  private byte[] tags;  // A byte array, rather than an array of org.apache.hadoop.hbase.Tag

  // Other fields
  private long   timestamp;
  private byte   type;  // A byte, rather than org.apache.hadoop.hbase.KeyValue.Type
  private long   seqId;

  public IndividualBytesFieldCell(byte[] row, byte[] family, byte[] qualifier,
                                  long timestamp, KeyValue.Type type,  byte[] value) {
    this(row, family, qualifier, timestamp, type, 0L /* sequence id */, value, null /* tags */);
  }

  public IndividualBytesFieldCell(byte[] row, byte[] family, byte[] qualifier,
                                  long timestamp, KeyValue.Type type, long seqId, byte[] value, byte[] tags) {

    // Check row, family, qualifier and value
    KeyValue.checkParameters(row, (row == null) ? 0 : row.length,           // row and row length
                             family, (family == null) ? 0 : family.length,  // family and family length
                             (qualifier == null) ? 0 : qualifier.length,    // qualifier length
                             (value == null) ? 0 : value.length);           // value length

    // Check timestamp
    if (timestamp < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
    }

    // Check tags
    TagUtil.checkForTagsLength((tags == null) ? 0 : tags.length);

    // No local copy is made, but reference to the input directly
    this.row       = row;
    this.family    = family;
    this.qualifier = qualifier;
    this.value     = value;
    this.tags      = tags;

    // Set others
    this.timestamp = timestamp;
    this.type      = type.getCode();
    this.seqId     = seqId;
  }

  @Override
  public int write(OutputStream out, boolean withTags) throws IOException {
    // Key length and then value length
    ByteBufferUtils.putInt(out, KeyValueUtil.keyLength(this));
    ByteBufferUtils.putInt(out, getValueLength());

    // Key
    CellUtil.writeFlatKey(this, out);

    // Value
    out.write(getValueArray());

    // Tags length and tags byte array
    if (withTags && getTagsLength() > 0) {
      // Tags length
      out.write((byte)(0xff & (tags.length >> 8)));
      out.write((byte)(0xff & tags.length));

      // Tags byte array
      out.write(tags);
    }

    return getSerializedSize(withTags);
  }

  @Override
  public void write(ByteBuffer buf, int offset) {
    KeyValueUtil.appendTo(this, buf, offset, true);
  }

  @Override
  public int getSerializedSize(boolean withTags) {
    return KeyValueUtil.length(getRowLength(), getFamilyLength(), getQualifierLength(),
                               getValueLength(), getTagsLength(), withTags);
  }

  @Override
  public long heapOverhead() {
    return   FIXED_OVERHEAD
           + ClassSize.ARRAY                               // row      , can not be null
           + ((family    == null) ? 0 : ClassSize.ARRAY)   // family   , can be null
           + ((qualifier == null) ? 0 : ClassSize.ARRAY)   // qualifier, can be null
           + ((value     == null) ? 0 : ClassSize.ARRAY)   // value    , can be null
           + ((tags      == null) ? 0 : ClassSize.ARRAY);  // tags     , can be null
  }

  @Override
  public Cell deepClone() {
    // When being added to the memstore, deepClone() is called and KeyValue has less heap overhead.
    return new KeyValue(this);
  }

  /**
   * Implement Cell interface
   */
  // 1) Row
  @Override
  public byte[] getRowArray() {
    // If row is null, the constructor will reject it, by {@link KeyValue#checkParameters()},
    // so it is safe to return row without checking.
    return row;
  }

  @Override
  public int getRowOffset() {
        return 0;
    }

  @Override
  public short getRowLength() {
    // If row is null or row.length is invalid, the constructor will reject it, by {@link KeyValue#checkParameters()},
    // so it is safe to call row.length and make the type conversion.
    return (short)(row.length);
  }

  // 2) Family
  @Override
  public byte[] getFamilyArray() {
    // Family could be null
    return (family == null) ? HConstants.EMPTY_BYTE_ARRAY : family;
  }

  @Override
  public int getFamilyOffset() {
    return 0;
  }

  @Override
  public byte getFamilyLength() {
    // If family.length is invalid, the constructor will reject it, by {@link KeyValue#checkParameters()},
    // so it is safe to make the type conversion.
    // But need to consider the condition when family is null.
    return (family == null) ? 0 : (byte)(family.length);
  }

  // 3) Qualifier
  @Override
  public byte[] getQualifierArray() {
    // Qualifier could be null
    return (qualifier == null) ? HConstants.EMPTY_BYTE_ARRAY : qualifier;
  }

  @Override
  public int getQualifierOffset() {
    return 0;
  }

  @Override
  public int getQualifierLength() {
    // Qualifier could be null
    return (qualifier == null) ? 0 : qualifier.length;
  }

  // 4) Timestamp
  @Override
  public long getTimestamp() {
    return timestamp;
  }

  //5) Type
  @Override
  public byte getTypeByte() {
    return type;
  }

  //6) Sequence id
  @Override
  public long getSequenceId() {
    return seqId;
  }

  //7) Value
  @Override
  public byte[] getValueArray() {
    // Value could be null
    return (value == null) ? HConstants.EMPTY_BYTE_ARRAY : value;
  }

  @Override
  public int getValueOffset() {
    return 0;
  }

  @Override
  public int getValueLength() {
    // Value could be null
    return (value == null) ? 0 : value.length;
  }

  // 8) Tags
  @Override
  public byte[] getTagsArray() {
    // Tags can could null
    return (tags == null) ? HConstants.EMPTY_BYTE_ARRAY : tags;
  }

  @Override
  public int getTagsOffset() {
    return 0;
  }

  @Override
  public int getTagsLength() {
    // Tags could be null
    return (tags == null) ? 0 : tags.length;
  }

  /**
   * Implement HeapSize interface
   */
  @Override
  public long heapSize() {
    // Size of array headers are already included into overhead, so do not need to include it for each byte array
    return   heapOverhead()                         // overhead, with array headers included
           + ClassSize.align(getRowLength())        // row
           + ClassSize.align(getFamilyLength())     // family
           + ClassSize.align(getQualifierLength())  // qualifier
           + ClassSize.align(getValueLength())      // value
           + ClassSize.align(getTagsLength());      // tags
  }

  /**
   * Implement Cloneable interface
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();  // only a shadow copy
  }

  /**
   * Implement SettableSequenceId interface
   */
  @Override
  public void setSequenceId(long seqId) {
    if (seqId < 0) {
      throw new IllegalArgumentException("Sequence Id cannot be negative. ts=" + seqId);
    }
    this.seqId = seqId;
  }

  /**
   * Implement SettableTimestamp interface
   */
  @Override
  public void setTimestamp(long ts) {
    if (ts < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + ts);
    }
    this.timestamp = ts;
  }

  @Override
  public void setTimestamp(byte[] ts, int tsOffset) {
    setTimestamp(Bytes.toLong(ts, tsOffset));
  }
}
