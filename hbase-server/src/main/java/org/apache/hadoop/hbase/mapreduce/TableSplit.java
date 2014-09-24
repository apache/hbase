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
package org.apache.hadoop.hbase.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * A table split corresponds to a key range (low, high) and an optional scanner.
 * All references to row below refer to the key of the row.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TableSplit extends InputSplit
implements Writable, Comparable<TableSplit> {
  public static final Log LOG = LogFactory.getLog(TableSplit.class);
  
  // should be < 0 (@see #readFields(DataInput))
  // version 1 supports Scan data member
  enum Version {
    UNVERSIONED(0),
    // Initial number we put on TableSplit when we introduced versioning.
    INITIAL(-1);

    final int code;
    static final Version[] byCode;
    static {
      byCode = Version.values();
      for (int i = 0; i < byCode.length; i++) {
        if (byCode[i].code != -1 * i) {
          throw new AssertionError("Values in this enum should be descending by one");
        }
      }
    }

    Version(int code) {
      this.code = code;
    }

    boolean atLeast(Version other) {
      return code <= other.code;
    }

    static Version fromCode(int code) {
      return byCode[code * -1];
    }
  }
  
  private static final Version VERSION = Version.INITIAL;
  private TableName tableName;
  private byte [] startRow;
  private byte [] endRow;
  private String regionLocation;
  private String scan = ""; // stores the serialized form of the Scan
  private long length; // Contains estimation of region size in bytes

  /** Default constructor. */
  public TableSplit() {
    this((TableName)null, null, HConstants.EMPTY_BYTE_ARRAY,
      HConstants.EMPTY_BYTE_ARRAY, "");
  }

  /**
   * @deprecated Since 0.96.0; use {@link TableSplit#TableSplit(TableName, byte[], byte[], String)}
   */
  @Deprecated
  public TableSplit(final byte [] tableName, Scan scan, byte [] startRow, byte [] endRow,
      final String location) {
    this(TableName.valueOf(tableName), scan, startRow, endRow, location);
  }

  /**
   * Creates a new instance while assigning all variables.
   * Length of region is set to 0
   *
   * @param tableName  The name of the current table.
   * @param scan The scan associated with this split.
   * @param startRow  The start row of the split.
   * @param endRow  The end row of the split.
   * @param location  The location of the region.
   */
  public TableSplit(TableName tableName, Scan scan, byte [] startRow, byte [] endRow,
                    final String location) {
    this(tableName, scan, startRow, endRow, location, 0L);
  }

  /**
   * Creates a new instance while assigning all variables.
   *
   * @param tableName  The name of the current table.
   * @param scan The scan associated with this split.
   * @param startRow  The start row of the split.
   * @param endRow  The end row of the split.
   * @param location  The location of the region.
   */
  public TableSplit(TableName tableName, Scan scan, byte [] startRow, byte [] endRow,
      final String location, long length) {
    this.tableName = tableName;
    try {
      this.scan =
        (null == scan) ? "" : TableMapReduceUtil.convertScanToString(scan);
    } catch (IOException e) {
      LOG.warn("Failed to convert Scan to String", e);
    }
    this.startRow = startRow;
    this.endRow = endRow;
    this.regionLocation = location;
    this.length = length;
  }

  /**
   * @deprecated Since 0.96.0; use {@link TableSplit#TableSplit(TableName, byte[], byte[], String)}
   */
  @Deprecated
  public TableSplit(final byte [] tableName, byte[] startRow, byte[] endRow,
      final String location) {
    this(TableName.valueOf(tableName), startRow, endRow, location);
  }

  /**
   * Creates a new instance without a scanner.
   *
   * @param tableName The name of the current table.
   * @param startRow The start row of the split.
   * @param endRow The end row of the split.
   * @param location The location of the region.
   */
  public TableSplit(TableName tableName, byte[] startRow, byte[] endRow,
      final String location) {
    this(tableName, null, startRow, endRow, location);
  }

  /**
   * Creates a new instance without a scanner.
   *
   * @param tableName The name of the current table.
   * @param startRow The start row of the split.
   * @param endRow The end row of the split.
   * @param location The location of the region.
   * @param length Size of region in bytes
   */
  public TableSplit(TableName tableName, byte[] startRow, byte[] endRow,
                    final String location, long length) {
    this(tableName, null, startRow, endRow, location, length);
  }

  /**
   * Returns a Scan object from the stored string representation.
   *
   * @return Returns a Scan object based on the stored scanner.
   * @throws IOException
   */
  public Scan getScan() throws IOException {
    return TableMapReduceUtil.convertStringToScan(this.scan);
  }

  /**
   * Returns the table name converted to a byte array.
   * @see #getTable()
   * @return The table name.
   */
  public byte [] getTableName() {
    return tableName.getName();
  }

  /**
   * Returns the table name.
   *
   * @return The table name.
   */
  public TableName getTable() {
    // It is ugly that usually to get a TableName, the method is called getTableName.  We can't do
    // that in here though because there was an existing getTableName in place already since
    // deprecated.
    return tableName;
  }

  /**
   * Returns the start row.
   *
   * @return The start row.
   */
  public byte [] getStartRow() {
    return startRow;
  }

  /**
   * Returns the end row.
   *
   * @return The end row.
   */
  public byte [] getEndRow() {
    return endRow;
  }

  /**
   * Returns the region location.
   *
   * @return The region's location.
   */
  public String getRegionLocation() {
    return regionLocation;
  }

  /**
   * Returns the region's location as an array.
   *
   * @return The array containing the region location.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
   */
  @Override
  public String[] getLocations() {
    return new String[] {regionLocation};
  }

  /**
   * Returns the length of the split.
   *
   * @return The length of the split.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
   */
  @Override
  public long getLength() {
    return length;
  }

  /**
   * Reads the values of each field.
   *
   * @param in  The input to read from.
   * @throws IOException When reading the input fails.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    Version version = Version.UNVERSIONED;
    // TableSplit was not versioned in the beginning.
    // In order to introduce it now, we make use of the fact
    // that tableName was written with Bytes.writeByteArray,
    // which encodes the array length as a vint which is >= 0.
    // Hence if the vint is >= 0 we have an old version and the vint
    // encodes the length of tableName.
    // If < 0 we just read the version and the next vint is the length.
    // @see Bytes#readByteArray(DataInput)
    int len = WritableUtils.readVInt(in);
    if (len < 0) {
      // what we just read was the version
      version = Version.fromCode(len);
      len = WritableUtils.readVInt(in);
    }
    byte[] tableNameBytes = new byte[len];
    in.readFully(tableNameBytes);
    tableName = TableName.valueOf(tableNameBytes);
    startRow = Bytes.readByteArray(in);
    endRow = Bytes.readByteArray(in);
    regionLocation = Bytes.toString(Bytes.readByteArray(in));
    if (version.atLeast(Version.INITIAL)) {
      scan = Bytes.toString(Bytes.readByteArray(in));
    }
    length = WritableUtils.readVLong(in);
  }

  /**
   * Writes the field values to the output.
   *
   * @param out  The output to write to.
   * @throws IOException When writing the values to the output fails.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, VERSION.code);
    Bytes.writeByteArray(out, tableName.getName());
    Bytes.writeByteArray(out, startRow);
    Bytes.writeByteArray(out, endRow);
    Bytes.writeByteArray(out, Bytes.toBytes(regionLocation));
    Bytes.writeByteArray(out, Bytes.toBytes(scan));
    WritableUtils.writeVLong(out, length);
  }

  /**
   * Returns the details about this instance as a string.
   *
   * @return The values of this instance as a string.
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("HBase table split(");
    sb.append("table name: ").append(tableName);
    sb.append(", scan: ").append(scan);
    sb.append(", start row: ").append(Bytes.toStringBinary(startRow));
    sb.append(", end row: ").append(Bytes.toStringBinary(endRow));
    sb.append(", region location: ").append(regionLocation);
    sb.append(")");
    return sb.toString();
  }

  /**
   * Compares this split against the given one.
   *
   * @param split  The split to compare to.
   * @return The result of the comparison.
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(TableSplit split) {
    // If The table name of the two splits is the same then compare start row
    // otherwise compare based on table names
    int tableNameComparison =
        getTable().compareTo(split.getTable());
    return tableNameComparison != 0 ? tableNameComparison : Bytes.compareTo(
        getStartRow(), split.getStartRow());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof TableSplit)) {
      return false;
    }
    return tableName.equals(((TableSplit)o).tableName) &&
      Bytes.equals(startRow, ((TableSplit)o).startRow) &&
      Bytes.equals(endRow, ((TableSplit)o).endRow) &&
      regionLocation.equals(((TableSplit)o).regionLocation);
  }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + (scan != null ? scan.hashCode() : 0);
        result = 31 * result + (startRow != null ? Arrays.hashCode(startRow) : 0);
        result = 31 * result + (endRow != null ? Arrays.hashCode(endRow) : 0);
        result = 31 * result + (regionLocation != null ? regionLocation.hashCode() : 0);
        return result;
    }
}
