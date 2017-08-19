/**
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
package org.apache.hadoop.hbase.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputSplit;

/**
 * A table split corresponds to a key range [low, high)
 */
@InterfaceAudience.Public
public class TableSplit implements InputSplit, Comparable<TableSplit> {
  private TableName m_tableName;
  private byte [] m_startRow;
  private byte [] m_endRow;
  private String m_regionLocation;

  /** default constructor */
  public TableSplit() {
    this((TableName)null, HConstants.EMPTY_BYTE_ARRAY,
      HConstants.EMPTY_BYTE_ARRAY, "");
  }

  /**
   * Constructor
   * @param tableName
   * @param startRow
   * @param endRow
   * @param location
   */
  public TableSplit(TableName tableName, byte [] startRow, byte [] endRow,
      final String location) {
    this.m_tableName = tableName;
    this.m_startRow = startRow;
    this.m_endRow = endRow;
    this.m_regionLocation = location;
  }

  public TableSplit(byte [] tableName, byte [] startRow, byte [] endRow,
      final String location) {
    this(TableName.valueOf(tableName), startRow, endRow,
      location);
  }

  /** @return table name */
  public TableName getTable() {
    return this.m_tableName;
  }

  /** @return table name */
   public byte [] getTableName() {
     return this.m_tableName.getName();
   }

  /** @return starting row key */
  public byte [] getStartRow() {
    return this.m_startRow;
  }

  /** @return end row key */
  public byte [] getEndRow() {
    return this.m_endRow;
  }

  /** @return the region's hostname */
  public String getRegionLocation() {
    return this.m_regionLocation;
  }

  public String[] getLocations() {
    return new String[] {this.m_regionLocation};
  }

  public long getLength() {
    // Not clear how to obtain this... seems to be used only for sorting splits
    return 0;
  }

  public void readFields(DataInput in) throws IOException {
    this.m_tableName = TableName.valueOf(Bytes.readByteArray(in));
    this.m_startRow = Bytes.readByteArray(in);
    this.m_endRow = Bytes.readByteArray(in);
    this.m_regionLocation = Bytes.toString(Bytes.readByteArray(in));
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.m_tableName.getName());
    Bytes.writeByteArray(out, this.m_startRow);
    Bytes.writeByteArray(out, this.m_endRow);
    Bytes.writeByteArray(out, Bytes.toBytes(this.m_regionLocation));
  }

  @Override
  public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("HBase table split(");
      sb.append("table name: ").append(m_tableName);
      sb.append(", start row: ").append(Bytes.toStringBinary(m_startRow));
      sb.append(", end row: ").append(Bytes.toStringBinary(m_endRow));
      sb.append(", region location: ").append(m_regionLocation);
      sb.append(")");
      return sb.toString();
  }

  @Override
  public int compareTo(TableSplit o) {
    return Bytes.compareTo(getStartRow(), o.getStartRow());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof TableSplit)) {
      return false;
    }
    TableSplit other = (TableSplit)o;
    return m_tableName.equals(other.m_tableName) &&
      Bytes.equals(m_startRow, other.m_startRow) &&
      Bytes.equals(m_endRow, other.m_endRow) &&
      m_regionLocation.equals(other.m_regionLocation);
  }

  @Override
  public int hashCode() {
    int result = m_tableName != null ? m_tableName.hashCode() : 0;
    result = 31 * result + Arrays.hashCode(m_startRow);
    result = 31 * result + Arrays.hashCode(m_endRow);
    result = 31 * result + (m_regionLocation != null ? m_regionLocation.hashCode() : 0);
    return result;
  }
}
