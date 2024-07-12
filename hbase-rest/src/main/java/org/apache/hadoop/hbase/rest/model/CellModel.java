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
package org.apache.hadoop.hbase.rest.model;

import static org.apache.hadoop.hbase.KeyValue.COLUMN_FAMILY_DELIMITER;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.Serializable;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.hadoop.hbase.rest.RestUtil;
import org.apache.hadoop.hbase.rest.protobuf.generated.CellMessage.Cell;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * Representation of a cell. A cell is a single value associated a column and optional qualifier,
 * and either the timestamp when it was stored or the user- provided timestamp if one was explicitly
 * supplied.
 *
 * <pre>
 * &lt;complexType name="Cell"&gt;
 *   &lt;sequence&gt;
 *     &lt;element name="value" maxOccurs="1" minOccurs="1"&gt;
 *       &lt;simpleType&gt;
 *         &lt;restriction base="base64Binary"/&gt;
 *       &lt;/simpleType&gt;
 *     &lt;/element&gt;
 *   &lt;/sequence&gt;
 *   &lt;attribute name="column" type="base64Binary" /&gt;
 *   &lt;attribute name="timestamp" type="int" /&gt;
 * &lt;/complexType&gt;
 * </pre>
 */
@XmlRootElement(name = "Cell")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Private
public class CellModel implements ProtobufMessageHandler, Serializable {
  private static final long serialVersionUID = 1L;
  public static final int MAGIC_LENGTH = -1;

  @JsonProperty("column")
  @XmlAttribute
  private byte[] column;

  @JsonProperty("timestamp")
  @XmlAttribute
  private long timestamp = HConstants.LATEST_TIMESTAMP;

  // If valueLength = -1, this represents the cell's value.
  // If valueLength <> 1, this represents an array containing the cell's value as determined by
  // offset and length.
  private byte[] value;

  @JsonIgnore
  private int valueOffset;

  @JsonIgnore
  private int valueLength = MAGIC_LENGTH;

  /**
   * Default constructor
   */
  public CellModel() {
  }

  /**
   * Constructor
   */
  public CellModel(byte[] column, byte[] value) {
    this(column, HConstants.LATEST_TIMESTAMP, value);
  }

  /**
   * Constructor
   */
  public CellModel(byte[] column, byte[] qualifier, byte[] value) {
    this(column, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /**
   * Constructor from KeyValue This avoids copying the value from the cell, and tries to optimize
   * generating the column value.
   */
  public CellModel(org.apache.hadoop.hbase.Cell cell) {
    this.column = makeColumn(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
      cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    this.timestamp = cell.getTimestamp();
    this.value = cell.getValueArray();
    this.valueOffset = cell.getValueOffset();
    this.valueLength = cell.getValueLength();
  }

  /**
   * Constructor
   */
  public CellModel(byte[] column, long timestamp, byte[] value) {
    this.column = column;
    this.timestamp = timestamp;
    setValue(value);
  }

  /**
   * Constructor
   */
  public CellModel(byte[] family, byte[] qualifier, long timestamp, byte[] value) {
    this.column = CellUtil.makeColumn(family, qualifier);
    this.timestamp = timestamp;
    setValue(value);
  }

  /** Returns the column */
  public byte[] getColumn() {
    return column;
  }

  /**
   * @param column the column to set
   */
  public void setColumn(byte[] column) {
    this.column = column;
  }

  /** Returns true if the timestamp property has been specified by the user */
  public boolean hasUserTimestamp() {
    return timestamp != HConstants.LATEST_TIMESTAMP;
  }

  /** Returns the timestamp */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @param timestamp the timestamp to set
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /** Returns the value */
  @JsonProperty("$")
  @XmlValue
  public byte[] getValue() {
    if (valueLength == MAGIC_LENGTH) {
      return value;
    } else {
      byte[] retValue = new byte[valueLength];
      System.arraycopy(value, valueOffset, retValue, 0, valueLength);
      return retValue;
    }
  }

  /** Returns the backing array for value (may be the same as value) */
  public byte[] getValueArray() {
    return value;
  }

  /**
   * @param value the value to set
   */
  @JsonProperty("$")
  public void setValue(byte[] value) {
    this.value = value;
    this.valueLength = MAGIC_LENGTH;
  }

  public int getValueOffset() {
    return valueOffset;
  }

  public int getValueLength() {
    return valueLength;
  }

  @Override
  public Message messageFromObject() {
    Cell.Builder builder = Cell.newBuilder();
    builder.setColumn(UnsafeByteOperations.unsafeWrap(getColumn()));
    if (valueLength == MAGIC_LENGTH) {
      builder.setData(UnsafeByteOperations.unsafeWrap(getValue()));
    } else {
      builder.setData(UnsafeByteOperations.unsafeWrap(value, valueOffset, valueLength));
    }
    if (hasUserTimestamp()) {
      builder.setTimestamp(getTimestamp());
    }
    return builder.build();
  }

  @Override
  public ProtobufMessageHandler getObjectFromMessage(CodedInputStream cis) throws IOException {
    Cell.Builder builder = Cell.newBuilder();
    RestUtil.mergeFrom(builder, cis);
    setColumn(builder.getColumn().toByteArray());
    setValue(builder.getData().toByteArray());
    if (builder.hasTimestamp()) {
      setTimestamp(builder.getTimestamp());
    }
    return this;
  }

  /**
   * Makes a column in family:qualifier form from separate byte arrays with offset and length.
   * <p>
   * Not recommended for usage as this is old-style API.
   * @return family:qualifier
   */
  public static byte[] makeColumn(byte[] family, int familyOffset, int familyLength,
    byte[] qualifier, int qualifierOffset, int qualifierLength) {
    byte[] column = new byte[familyLength + qualifierLength + 1];
    System.arraycopy(family, familyOffset, column, 0, familyLength);
    column[familyLength] = COLUMN_FAMILY_DELIMITER;
    System.arraycopy(qualifier, qualifierOffset, column, familyLength + 1, qualifierLength);
    return column;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    CellModel cellModel = (CellModel) obj;
    return new EqualsBuilder().append(column, cellModel.column)
      .append(timestamp, cellModel.timestamp).append(getValue(), cellModel.getValue()).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(column).append(timestamp).append(getValue()).toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("column", column).append("timestamp", timestamp)
      .append("value", getValue()).toString();
  }
}
