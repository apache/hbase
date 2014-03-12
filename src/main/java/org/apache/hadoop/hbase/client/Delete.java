/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Used to perform Delete operations on a single row.
 * <p>
 * To delete an entire row, instantiate a Delete object with the row
 * to delete.  To further define the scope of what to delete, perform
 * additional methods as outlined below.
 * <p>
 * To delete specific families, execute {@link #deleteFamily(byte[]) deleteFamily}
 * for each family to delete.
 * <p>
 * To delete multiple versions of specific columns, execute
 * {@link #deleteColumns(byte[], byte[]) deleteColumns}
 * for each column to delete.
 * <p>
 * To delete specific versions of specific columns, execute
 * {@link #deleteColumn(byte[], byte[], long) deleteColumn}
 * for each column version to delete.
 * <p>
 * Specifying timestamps, deleteFamily and deleteColumns will delete all
 * versions with a timestamp less than or equal to that passed.  If no
 * timestamp is specified, an entry is added with a timestamp of 'now'
 * where 'now' is the servers's System.currentTimeMillis().
 * Specifying a timestamp to the deleteColumn method will
 * delete versions only with a timestamp equal to that specified.
 * If no timestamp is passed to deleteColumn, internally, it figures the
 * most recent cell's timestamp and adds a delete at that timestamp; i.e.
 * it deletes the most recently added cell.
 * <p>The timestamp passed to the constructor is used ONLY for delete of
 * rows.  For anything less -- a deleteColumn, deleteColumns or
 * deleteFamily -- then you need to use the method overrides that take a
 * timestamp.  The constructor timestamp is not referenced.
 */
@ThriftStruct
public class Delete extends Mutation
  implements Writable, Comparable<Row> {
  private static final byte DELETE_VERSION = (byte)3;

  private static final int ADDED_WRITE_TO_WAL_VERSION = 3;
  private static final int ADDED_ATTRIBUTES_VERSION = 2;
  private static final Delete dummyDelete = new Delete();

  /** Constructor for Writable.  DO NOT USE */
  public Delete() {
    this((byte[])null);
  }

  /**
   * Create a Delete operation for the specified row.
   * <p>
   * If no further operations are done, this will delete everything
   * associated with the specified row (all versions of all columns in all
   * families).
   * @param row row key
   */
  public Delete(byte[] row) {
    this(row, HConstants.LATEST_TIMESTAMP, null);
  }

  /**
   * Create a Delete operation for the specified row and timestamp, using
   * an optional row lock.<p>
   *
   * If no further operations are done, this will delete all columns in all
   * families of the specified row with a timestamp less than or equal to the
   * specified timestamp.<p>
   *
   * This timestamp is ONLY used for a delete row operation.  If specifying
   * families or columns, you must specify each timestamp individually.
   * @param row row key
   * @param timestamp maximum version timestamp (only for delete row)
   * @param rowLock previously acquired row lock, or null
   */
  public Delete(byte[] row, long timestamp, RowLock rowLock) {
    this.row = row;
    this.ts = timestamp;
    if (rowLock != null) {
      this.lockId = rowLock.getLockId();
    }
  }

  /**
   * @param d Delete to clone.
   */
  public Delete(final Delete d) {
    this.row = d.getRow();
    this.ts = d.getTimeStamp();
    this.lockId = d.getLockId();
    this.familyMap.putAll(d.getFamilyMap());
    this.writeToWAL = d.writeToWAL;
  }

  /**
   * Delete all versions of all columns of the specified family.
   * <p>
   * Overrides previous calls to deleteColumn and deleteColumns for the
   * specified family.
   * @param family family name
   * @return this for invocation chaining
   */
  public Delete deleteFamily(byte[] family) {
    this.deleteFamily(family, HConstants.LATEST_TIMESTAMP);
    return this;
  }

  @ThriftConstructor
  public Delete(@ThriftField(1) final byte[] row,
      @ThriftField(2) final long timeStamp,
      @ThriftField(3) final Map<byte[], List<KeyValue>> familyMapSerial,
      @ThriftField(4) final long lockId,
      @ThriftField(5) final boolean writeToWAL) {
    this(row, timeStamp, null);
    this.lockId = lockId;
    this.writeToWAL = writeToWAL;
    // Need to do this, since familyMapSerial might be a HashMap whereas, we
    // need a TreeMap that uses the Bytes.BYTES_COMPARATOR, as specified in
    // the Mutation class.
    this.familyMap.putAll(familyMapSerial);
  }

  @Override
  @ThriftField(1)
  public byte[] getRow() {
    return this.row;
  }

  @Override
  @ThriftField(2)
  public long getTimeStamp() {
    return this.ts;
  }

  /**
   * Method for retrieving the delete's familyMap
   * @return familyMap
   */
  @Override
  @ThriftField(3)
  public Map<byte[], List<KeyValue>> getFamilyMap() {
    return this.familyMap;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @ThriftField(4)
  public long getLockId() {
    return this.lockId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @ThriftField(5)
  public boolean getWriteToWAL() {
    return this.writeToWAL;
  }

  /**
   * Static factory constructor which can create a dummy put.
   */
  static Delete createDummyDelete() {
    return Delete.dummyDelete;
  }

  /**
   * Delete all columns of the specified family with a timestamp less than
   * or equal to the specified timestamp.
   * <p>
   * Overrides previous calls to deleteColumn and deleteColumns for the
   * specified family.
   * @param family family name
   * @param timestamp maximum version timestamp
   * @return this for invocation chaining
   */
  public Delete deleteFamily(byte[] family, long timestamp) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>();
    } else if(!list.isEmpty()) {
      list.clear();
    }
    list.add(new KeyValue(row, family, null, timestamp, KeyValue.Type.DeleteFamily));
    familyMap.put(family, list);
    return this;
  }

  public boolean isDummy() {
    return this.row == null || this.row.length == 0;
  }

  /**
   * Delete all versions of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   * @return this for invocation chaining
   */
  public Delete deleteColumns(byte[] family, byte[] qualifier) {
    this.deleteColumns(family, qualifier, HConstants.LATEST_TIMESTAMP);
    return this;
  }

  /**
   * Delete all versions of the specified column with a timestamp less than
   * or equal to the specified timestamp.
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp maximum version timestamp
   * @return this for invocation chaining
   */
  public Delete deleteColumns(byte[] family, byte[] qualifier, long timestamp) {
    List<KeyValue> list = familyMap.get(family);
    if (list == null) {
      list = new ArrayList<KeyValue>();
    }
    list.add(new KeyValue(this.row, family, qualifier, timestamp,
      KeyValue.Type.DeleteColumn));
    familyMap.put(family, list);
    return this;
  }

  /**
   * Delete the latest version of the specified column.
   * This is an expensive call in that on the server-side, it first does a
   * get to find the latest versions timestamp.  Then it adds a delete using
   * the fetched cells timestamp.
   * @param family family name
   * @param qualifier column qualifier
   * @return this for invocation chaining
   */
  public Delete deleteColumn(byte[] family, byte[] qualifier) {
    this.deleteColumn(family, qualifier, HConstants.LATEST_TIMESTAMP);
    return this;
  }

  /**
   * Delete the specified version of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @return this for invocation chaining
   */
  public Delete deleteColumn(byte[] family, byte[] qualifier, long timestamp) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>();
    }
    list.add(new KeyValue(
        this.row, family, qualifier, timestamp, KeyValue.Type.Delete));
    familyMap.put(family, list);
    return this;
  }

  //Writable
  @Override
  public void readFields(final DataInput in) throws IOException {
    int version = in.readByte();
    if (version > DELETE_VERSION) {
      throw new IOException("version not supported");
    }
    this.row = Bytes.readByteArray(in);
    this.ts = in.readLong();
    this.lockId = in.readLong();
    if (version >= ADDED_WRITE_TO_WAL_VERSION) {
      this.writeToWAL = in.readBoolean();
    }
    this.familyMap.clear();
    int numFamilies = in.readInt();
    for(int i=0;i<numFamilies;i++) {
      byte[] family = Bytes.readByteArray(in);
      int numColumns = in.readInt();
      List<KeyValue> list = new ArrayList<KeyValue>(numColumns);
      for(int j=0;j<numColumns;j++) {
        KeyValue kv = new KeyValue();
        kv.readFields(in);
        list.add(kv);
      }
      this.familyMap.put(family, list);
    }
    if (version >= ADDED_ATTRIBUTES_VERSION) {
      readAttributes(in);
    }
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    int version = 1;
    if (!getAttributesMap().isEmpty()) {
      version = Math.max(version, ADDED_ATTRIBUTES_VERSION);
    }
    if (!writeToWAL) {
      version = Math.max(version, ADDED_WRITE_TO_WAL_VERSION);
    }

    out.writeByte(version);
    Bytes.writeByteArray(out, this.row);
    out.writeLong(this.ts);
    out.writeLong(this.lockId);
    if (version >= ADDED_WRITE_TO_WAL_VERSION) {
      out.writeBoolean(writeToWAL);
    }
    out.writeInt(familyMap.size());
    for(Map.Entry<byte[], List<KeyValue>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      List<KeyValue> list = entry.getValue();
      out.writeInt(list.size());
      for(KeyValue kv : list) {
        kv.write(out);
      }
    }
    if (version >= ADDED_ATTRIBUTES_VERSION) {
      writeAttributes(out);
    }
  }

  /**
   * Delete all versions of the specified column, given in
   * <code>family:qualifier</code> notation, and with a timestamp less than
   * or equal to the specified timestamp.
   * @param column colon-delimited family and qualifier
   * @param timestamp maximum version timestamp
   * @deprecated use {@link #deleteColumn(byte[], byte[], long)} instead
   * @return this for invocation chaining
   */
  @Deprecated
  public Delete deleteColumns(byte[] column, long timestamp) {
    byte[][] parts = KeyValue.parseColumn(column);
    this.deleteColumns(parts[0], parts[1], timestamp);
    return this;
  }

  /**
   * Delete the latest version of the specified column, given in
   * <code>family:qualifier</code> notation.
   * @param column colon-delimited family and qualifier
   * @deprecated use {@link #deleteColumn(byte[], byte[])} instead
   * @return this for invocation chaining
   */
  @Deprecated
  public Delete deleteColumn(byte[] column) {
    byte[][] parts = KeyValue.parseColumn(column);
    this.deleteColumn(parts[0], parts[1], HConstants.LATEST_TIMESTAMP);
    return this;
  }


  /**
   * Compile the column family (i.e. schema) information
   * into a Map. Useful for parsing and aggregation by debugging,
   * logging, and administration tools.
   * @return Map
   */
  @Override
  public Map<String, Object> getFingerprint() {
    Map<String, Object> map = super.getFingerprint();
    map.put("operation", "Delete");
    return map;
  }

  /**
   * Modify this delete object to delete from all column families in the row at the given
   * timestamp or older. If column families have been added already, 
   * an {@link IllegalArgumentException} is thrown.
   * @param timestamp will delete data at this timestamp or older 
   */
  public void deleteRow(long timestamp) {
    Preconditions.checkArgument(familyMap.isEmpty(),
      "Cannot delete entire row, column families already specified");
    ts = timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(row, ts, lockId, familyMap);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Delete other = (Delete) obj;
    if (ts != other.getTimeStamp()) {
      return false;
    }
    if (writeToWAL != other.getWriteToWAL()) {
      return false;
    }
    if (lockId != other.getLockId()) {
      return false;
    }
    if (!Arrays.equals(other.row, this.getRow())) {
      return false;
    }
    if (familyMap == null && other.getFamilyMap() != null) {
      return false;
    } else if (familyMap != null && other.getFamilyMap() == null) {
      return false;
    }
    return familyMap.size() == other.getFamilyMap().size() &&
      familyMap.entrySet().containsAll(other.getFamilyMap().entrySet());
  }

  public static class Builder {
    private byte[] row;
    private long timeStamp;
    private Map<byte[], List<KeyValue>> familyMap;
    private long lockId;
    private boolean writeToWAL;

    public Builder() {
    }

    public Builder setRow(byte[] row) {
      this.row = row;
      return this;
    }

    public Builder setTimeStamp(long ts) {
      this.timeStamp = ts;
      return this;
    }

    public Builder setFamilyMap(Map<byte[], List<KeyValue>> familyMap) {
      this.familyMap = familyMap;
      return this;
    }

    public Builder setLockId(long lockId) {
      this.lockId = lockId;
      return this;
    }

    public Builder setWriteToWAL(boolean writeToWAL) {
      this.writeToWAL = writeToWAL;
      return this;
    }

    public Delete create() {
      return new Delete(row, timeStamp, familyMap, lockId, writeToWAL);
    }
  }

}
