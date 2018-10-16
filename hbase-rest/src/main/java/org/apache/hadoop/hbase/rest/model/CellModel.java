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

package org.apache.hadoop.hbase.rest.model;

import java.io.IOException;
import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.rest.ProtobufMessageHandler;
import org.apache.hadoop.hbase.rest.protobuf.generated.CellMessage.Cell;

/**
 * Representation of a cell. A cell is a single value associated a column and
 * optional qualifier, and either the timestamp when it was stored or the user-
 * provided timestamp if one was explicitly supplied.
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
@XmlRootElement(name="Cell")
@XmlAccessorType(XmlAccessType.FIELD)
@InterfaceAudience.Private
public class CellModel implements ProtobufMessageHandler, Serializable {
  private static final long serialVersionUID = 1L;

  @JsonProperty("column")
  @XmlAttribute
  private byte[] column;

  @JsonProperty("timestamp")
  @XmlAttribute
  private long timestamp = HConstants.LATEST_TIMESTAMP;

  @JsonProperty("$")
  @XmlValue
  private byte[] value;

  /**
   * Default constructor
   */
  public CellModel() {}

  /**
   * Constructor
   * @param column
   * @param value
   */
  public CellModel(byte[] column, byte[] value) {
    this(column, HConstants.LATEST_TIMESTAMP, value);
  }

  /**
   * Constructor
   * @param column
   * @param qualifier
   * @param value
   */
  public CellModel(byte[] column, byte[] qualifier, byte[] value) {
    this(column, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /**
   * Constructor from KeyValue
   * @param cell
   */
  public CellModel(org.apache.hadoop.hbase.Cell cell) {
    this(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), cell.getTimestamp(), CellUtil
        .cloneValue(cell));
  }

  /**
   * Constructor
   * @param column
   * @param timestamp
   * @param value
   */
  public CellModel(byte[] column, long timestamp, byte[] value) {
    this.column = column;
    this.timestamp = timestamp;
    this.value = value;
  }

  /**
   * Constructor
   * @param column
   * @param qualifier
   * @param timestamp
   * @param value
   */
  public CellModel(byte[] column, byte[] qualifier, long timestamp,
      byte[] value) {
    this.column = CellUtil.makeColumn(column, qualifier);
    this.timestamp = timestamp;
    this.value = value;
  }

  /**
   * @return the column
   */
  public byte[] getColumn() {
    return column;
  }

  /**
   * @param column the column to set
   */
  public void setColumn(byte[] column) {
    this.column = column;
  }

  /**
   * @return true if the timestamp property has been specified by the
   * user
   */
  public boolean hasUserTimestamp() {
    return timestamp != HConstants.LATEST_TIMESTAMP;
  }

  /**
   * @return the timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @param timestamp the timestamp to set
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * @return the value
   */
  public byte[] getValue() {
    return value;
  }

  /**
   * @param value the value to set
   */
  public void setValue(byte[] value) {
    this.value = value;
  }

  @Override
  public byte[] createProtobufOutput() {
    Cell.Builder builder = Cell.newBuilder();
    builder.setColumn(ByteStringer.wrap(getColumn()));
    builder.setData(ByteStringer.wrap(getValue()));
    if (hasUserTimestamp()) {
      builder.setTimestamp(getTimestamp());
    }
    return builder.build().toByteArray();
  }

  @Override
  public ProtobufMessageHandler getObjectFromMessage(byte[] message)
      throws IOException {
    Cell.Builder builder = Cell.newBuilder();
    ProtobufUtil.mergeFrom(builder, message);
    setColumn(builder.getColumn().toByteArray());
    setValue(builder.getData().toByteArray());
    if (builder.hasTimestamp()) {
      setTimestamp(builder.getTimestamp());
    }
    return this;
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
    return new EqualsBuilder().
        append(column, cellModel.column).
        append(timestamp, cellModel.timestamp).
        append(value, cellModel.value).
        isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(column).
        append(timestamp).
        append(value).
        toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).
        append("column", column).
        append("timestamp", timestamp).
        append("value", value).
        toString();
  }
}
