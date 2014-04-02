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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.WritableWithSize;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

/**
 * Single row result of a {@link Get} or {@link Scan} query.<p>
 *
 * Convenience methods are available that return various {@link Map}
 * structures and values directly.<p>
 *
 * To get a complete mapping of all cells in the Result, which can include
 * multiple families and multiple versions, use {@link #getMap()}.<p>
 *
 * To get a mapping of each family to its columns (qualifiers and values),
 * including only the latest version of each, use {@link #getNoVersionMap()}.
 *
 * To get a mapping of qualifiers to latest values for an individual family use
 * {@link #getFamilyMap(byte[])}.<p>
 *
 * To get the latest value for a specific family and qualifier use {@link #getValue(byte[], byte[])}.
 *
 * A Result is backed by an array of {@link KeyValue} objects, each representing
 * an HBase cell defined by the row, family, qualifier, timestamp, and value.<p>
 *
 * The underlying {@link KeyValue} objects can be accessed through the methods
 * {@link #sorted()} and {@link #list()}.  Each KeyValue can then be accessed
 * through {@link KeyValue#getRow()}, {@link KeyValue#getFamily()}, {@link KeyValue#getQualifier()},
 * {@link KeyValue#getTimestamp()}, and {@link KeyValue#getValue()}.
 */
@ThriftStruct
public class Result implements Writable, WritableWithSize {
  private static final byte RESULT_VERSION = (byte)1;

  private KeyValue[] kvs = null;
  private NavigableMap<byte[],
     NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = null;
  // We're not using java serialization.  Transient here is just a marker to say
  // that this is where we cache row if we're ever asked for it.
  private transient byte[] row = null;
  private ImmutableBytesWritable bytes = null;

  /**
   * Constructor used for Writable.
   */
  public Result() {
    this.kvs = EMPTY_KEY_VALUE_ARRAY;
  }

  /**
   * Instantiate a Result with the specified array of KeyValues.
   * @param kvs array of KeyValues
   */
  public Result(KeyValue[] kvs) {
    if(kvs != null && kvs.length > 0) {
      this.kvs = kvs;
    }
  }

  private static final KeyValue[] EMPTY_KEY_VALUE_ARRAY = new KeyValue[0];
  public static final Result SENTINEL_RESULT = new Result();
  public static final Result[] SENTINEL_RESULT_ARRAY =
      new Result[] { SENTINEL_RESULT };

  /**
   * Thrift constructor
   * Instantiate a Result with the specified List of KeyValues.
   * @param kvs List of KeyValues
   */
  @ThriftConstructor
  public Result(@ThriftField(1) List<KeyValue> kvs) {
    if(kvs != null) {
      this.kvs = kvs.toArray(new KeyValue[kvs.size()]);
    }
  }

  /**
   * Instantiate a Result from the specified raw binary format.
   * @param bytes raw binary format of Result
   */
  public Result(ImmutableBytesWritable bytes) {
    this.bytes = bytes;
  }

  /**
   * Method for retrieving the row that this result is for
   * @return row
   */
  public synchronized byte[] getRow() {
    if (this.row == null) {
      if(this.kvs == null) {
        readFields();
      }
      this.row = this.kvs.length == 0? null: this.kvs[0].getRow();
    }
    return this.row;
  }

  /**
   * Return the unsorted array of KeyValues backing this Result instance.
   * @return unsorted array of KeyValues
   */
  public KeyValue[] raw() {
    if(this.kvs == null) {
      readFields();
    }
    return kvs;
  }

  /**
   * Create a sorted list of the KeyValue's in this result.
   *
   * @return The sorted list of KeyValue's.
   */
  public List<KeyValue> list() {
    if(this.kvs == null) {
      readFields();
    }
    return isEmpty()? null: Arrays.asList(sorted());
  }

  /**
   * Returns a sorted array of KeyValues in this Result.
   * <p>
   * Note: Sorting is done in place, so the backing array will be sorted
   * after calling this method.
   * @return sorted array of KeyValues
   */
  public KeyValue[] sorted() {
    if (isEmpty()) {
      return null;
    }
    Arrays.sort(kvs, KeyValue.COMPARATOR);
    return kvs;
  }

  /**
   * Map of families to all versions of its qualifiers and values.
   * <p>
   * Returns a three level Map of the form:
   * <code>Map<family,Map&lt;qualifier,Map&lt;timestamp,value>>></code>
   * <p>
   * Note: All other map returning methods make use of this map internally.
   * @return map from families to qualifiers to versions
   */
  public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
    if(this.familyMap != null) {
      return this.familyMap;
    }
    if(isEmpty()) {
      return null;
    }
    this.familyMap =
      new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
      (Bytes.BYTES_COMPARATOR);
    for(KeyValue kv : this.kvs) {
      SplitKeyValue splitKV = kv.split();
      byte[] family = splitKV.getFamily();
      NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap =
        familyMap.get(family);
      if(columnMap == null) {
        columnMap = new TreeMap<byte[], NavigableMap<Long, byte[]>>
          (Bytes.BYTES_COMPARATOR);
        familyMap.put(family, columnMap);
      }
      byte[] qualifier = splitKV.getQualifier();
      NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
      if(versionMap == null) {
        versionMap = new TreeMap<Long, byte[]>(new Comparator<Long>() {
          @Override
          public int compare(Long l1, Long l2) {
            return l2.compareTo(l1);
          }
        });
        columnMap.put(qualifier, versionMap);
      }
      Long timestamp = Bytes.toLong(splitKV.getTimestamp());
      byte[] value = splitKV.getValue();
      versionMap.put(timestamp, value);
    }
    return this.familyMap;
  }

  /**
   * Searches for the latest version of a value with specified family and
   * qualifier.
   *
   * This method calls getValue if familyMap has been established.
   * Otherwise, it performs a linear search over the whole kv-list at that time.
   *
   * @param family
   *          the family name.
   * @param qualifier
   *          the qualifier name.
   * @return the value if found, nil if not.
   */
  public byte[] searchValue(byte[] family, byte[] qualifier) {
    if (this.familyMap != null) {
      return this.getValue(family, qualifier);
    }
    byte[] res = null;
    long timeStamp = -1;
    for (KeyValue kv : this.kvs) {

      if (kv.matchingFamily(family) && kv.matchingQualifier(qualifier)) {
        long kvTimestamp = kv.getTimestamp();
        if (kvTimestamp > timeStamp) {
          timeStamp = kvTimestamp;
          res = kv.getValue();
        }
      }
    }
    return res;
  }

  /**
   * Map of families to their most recent qualifiers and values.
   * <p>
   * Returns a two level Map of the form:
   * <code>Map<family,Map&lt;qualifier,value>></code>
   * <p>
   * The most recent version of each qualifier will be used.
   *
   * @return map from families to qualifiers and value
   */
  public NavigableMap<byte[], NavigableMap<byte[], byte[]>> getNoVersionMap() {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> returnMap =
      new TreeMap<byte[], NavigableMap<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
    for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
      familyEntry : familyMap.entrySet()) {
      NavigableMap<byte[], byte[]> qualifierMap =
        new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      for(Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry :
        familyEntry.getValue().entrySet()) {
        byte[] value =
          qualifierEntry.getValue().get(qualifierEntry.getValue().firstKey());
        qualifierMap.put(qualifierEntry.getKey(), value);
      }
      returnMap.put(familyEntry.getKey(), qualifierMap);
    }
    return returnMap;
  }

  /**
   * Map of qualifiers to values.
   * <p>
   * Returns a Map of the form: <code>Map&lt;qualifier,value></code>
   * @param family column family to get
   * @return map of qualifiers to values
   */
  public NavigableMap<byte[], byte[]> getFamilyMap(byte[] family) {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte[], byte[]> returnMap =
      new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap =
      familyMap.get(family);
    if(qualifierMap == null) {
      return returnMap;
    }
    for(Map.Entry<byte[], NavigableMap<Long, byte[]>> entry :
      qualifierMap.entrySet()) {
      byte[] value =
        entry.getValue().get(entry.getValue().firstKey());
      returnMap.put(entry.getKey(), value);
    }
    return returnMap;
  }

  protected int binarySearch(final KeyValue[] kvs, final byte[] family,
      final byte[] qualifier) {
    KeyValue searchTerm =
        KeyValue.createFirstOnRow(kvs[0].getRow(),
            family, qualifier);

    // pos === ( -(insertion point) - 1)
    int pos = Arrays.binarySearch(kvs, searchTerm, KeyValue.COMPARATOR);
    // never will exact match
    if (pos < 0) {
      pos = (pos + 1) * -1;
      // pos is now insertion point
    }
    if (pos == kvs.length) {
      return -1; // doesn't exist
    }
    return pos;
  }

  /**
   * The KeyValue for the most recent for a given column. If the column does
   * not exist in the result set - if it wasn't selected in the query (Get/Scan)
   * or just does not exist in the row the return value is null.
   *
   * @param family
   * @param qualifier
   * @return KeyValue for the column or null
   */
  public KeyValue getColumnLatest(byte[] family, byte[] qualifier) {
    KeyValue [] kvs = raw(); // side effect possibly.
    if (kvs == null || kvs.length == 0) {
      return null;
    }
    int pos = binarySearch(kvs, family, qualifier);
    if (pos == -1) {
      return null;
    }
    KeyValue kv = kvs[pos];
    if (kv.matchingColumn(family, qualifier)) {
      return kv;
    }
    return null;
  }

  /**
   * Get the latest version of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   * @return value of latest version of column, null if none found
   */
  public byte[] getValue(byte[] family, byte[] qualifier) {
    Map.Entry<Long,byte[]> entry = getKeyValue(family, qualifier);
    return entry == null? null: entry.getValue();
  }

  /** 
   * Get the latest time stamp. 
   * @param family family name  
   * @param qualifier column qualifier  
   * @return the latest time stamp  
   */ 
  public long getLastestTimeStamp(byte[] family, byte[] qualifier) {
    Map.Entry<Long,byte[]> entry = getKeyValue(family, qualifier);  
    return entry == null? Long.MIN_VALUE: entry.getKey(); 
  }
  
  private Map.Entry<Long,byte[]> getKeyValue(byte[] family, byte[] qualifier) {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap =
      familyMap.get(family);
    if(qualifierMap == null) {
      return null;
    }
    NavigableMap<Long, byte[]> versionMap =
      getVersionMap(qualifierMap, qualifier);
    if(versionMap == null) {
      return null;
    }
    return versionMap.firstEntry();
  }

  private NavigableMap<Long, byte[]> getVersionMap(
      NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap, byte[] qualifier) {
    return qualifier != null?
      qualifierMap.get(qualifier): qualifierMap.get(new byte[0]);
  }

  /**
   * Checks for existence of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   * @return true if at least one value exists in the result, false if not
   */
  public boolean containsColumn(byte[] family, byte[] qualifier) {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return false;
    }
    NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap =
      familyMap.get(family);
    if(qualifierMap == null) {
      return false;
    }
    NavigableMap<Long, byte[]> versionMap = getVersionMap(qualifierMap, qualifier);
    return versionMap != null;
  }

  /**
   * Returns the value of the first column in the Result.
   * @return value of the first column
   */
  public byte[] value() {
    if (isEmpty()) {
      return null;
    }
    return kvs[0].getValue();
  }

  /**
   * Returns the raw binary encoding of this Result.<p>
   *
   * Please note, there may be an offset into the underlying byte array of the
   * returned ImmutableBytesWritable.  Be sure to use both
   * {@link ImmutableBytesWritable#get()} and {@link ImmutableBytesWritable#getOffset()}
   * @return pointer to raw binary of Result
   */
  public ImmutableBytesWritable getBytes() {
    if (this.bytes == null) {
      Result[] res = new Result[1];
      res[0] = this;
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(stream);
      try {
        writeArray(out, res);
        DataInput in = new DataInputStream(
            new ByteArrayInputStream(stream.toByteArray()));
        this.bytes = readArray(in)[0].bytes;
      } catch (IOException e) {
        return null;
      }
    }
    return this.bytes;
  }

  /**
   * Check if the underlying KeyValue [] is empty or not
   * @return true if empty
   */
  public boolean isEmpty() {
    if(this.kvs == null) {
      readFields();
    }
    return this.kvs == null || this.kvs.length == 0;
  }

  /**
   * Returns if this is a sentinel result, a place-holder for a null result.
   * Should be used in in the Thrift channel only i.e. HBaseToThriftAdapter.
   * This function does not handle the lazy loading using this.bytes which
   * happens on the HadoopRPC side.
   * @return
   */
  public boolean isSentinelResult() {
    return this.kvs == null || this.kvs.length == 0;
  }

  /**
   * @return the size of the underlying KeyValue []
   */
  public int size() {
    if(this.kvs == null) {
      readFields();
    }
    return this.kvs == null? 0: this.kvs.length;
  }

  /**
   * @return String
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("keyvalues=");
    if(isEmpty()) {
      sb.append("NONE");
      return sb.toString();
    }
    sb.append("{");
    boolean moreThanOne = false;
    for(KeyValue kv : this.kvs) {
      if(moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append(kv.toString());
    }
    sb.append("}");
    return sb.toString();
  }

  //Writable
  @Override
  public void readFields(final DataInput in)
  throws IOException {
    familyMap = null;
    row = null;
    kvs = null;
    int totalBuffer = in.readInt();
    if(totalBuffer == 0) {
      bytes = null;
      return;
    }
    byte[] raw = new byte[totalBuffer];
    HBaseClient.readFromSocket(in, raw, 0, totalBuffer);
    bytes = new ImmutableBytesWritable(raw, 0, totalBuffer);
  }

  //Create KeyValue[] when needed
  private void readFields() {
    if (bytes == null) {
      this.kvs = new KeyValue[0];
      return;
    }
    byte[] buf = bytes.get();
    int offset = bytes.getOffset();
    int finalOffset = bytes.getSize() + offset;
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    while(offset < finalOffset) {
      int keyLength = Bytes.toInt(buf, offset);
      offset += Bytes.SIZEOF_INT;
      kvs.add(new KeyValue(buf, offset, keyLength));
      offset += keyLength;
    }
    this.kvs = kvs.toArray(new KeyValue[kvs.size()]);
  }

  @Override
  public long getWritableSize() {
    if (isEmpty())
      return Bytes.SIZEOF_INT; // int size = 0

    long size = Bytes.SIZEOF_INT; // totalLen

    for (KeyValue kv : kvs) {
      size += kv.getLength();
      size += Bytes.SIZEOF_INT; // kv.getLength
    }

    return size;
  }

  @Override
  public void write(final DataOutput out)
  throws IOException {
    if(isEmpty()) {
      out.writeInt(0);
    } else {
      int totalLen = 0;
      for(KeyValue kv : kvs) {
        totalLen += kv.getLength() + Bytes.SIZEOF_INT;
      }
      out.writeInt(totalLen);
      for(KeyValue kv : kvs) {
        out.writeInt(kv.getLength());
        out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
      }
    }
  }

  public static long getWriteArraySize(Result [] results) {
    long size = Bytes.SIZEOF_BYTE; // RESULT_VERSION
    if (results == null || results.length == 0) {
      size += Bytes.SIZEOF_INT;
      return size;
    }

    size += Bytes.SIZEOF_INT; // results.length
    size += Bytes.SIZEOF_INT; // bufLen
    for (Result result : results) {
      size += Bytes.SIZEOF_INT; // either 0 or result.size()
      if (result == null || result.isEmpty())
        continue;

      for (KeyValue kv : result.raw()) {
        size += Bytes.SIZEOF_INT; // kv.getLength();
        size += kv.getLength();
      }
    }

    return size;
  }

  public static void writeArray(final DataOutput out, Result [] results)
  throws IOException {
    // Write version when writing array form.
    // This assumes that results are sent to the client as Result[], so we
    // have an opportunity to handle version differences without affecting
    // efficiency.
    out.writeByte(RESULT_VERSION);
    if(results == null || results.length == 0) {
      out.writeInt(0);
      return;
    }
    out.writeInt(results.length);
    int bufLen = 0;
    for(Result result : results) {
      bufLen += Bytes.SIZEOF_INT;
      if(result == null || result.isEmpty()) {
        continue;
      }
      for(KeyValue key : result.raw()) {
        bufLen += key.getLength() + Bytes.SIZEOF_INT;
      }
    }
    out.writeInt(bufLen);
    for(Result result : results) {
      if(result == null || result.isEmpty()) {
        out.writeInt(0);
        continue;
      }
      out.writeInt(result.size());
      for(KeyValue kv : result.raw()) {
        out.writeInt(kv.getLength());
        out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
      }
    }
  }

  public static Result [] readArray(final DataInput in)
  throws IOException {
    // Read version for array form.
    // This assumes that results are sent to the client as Result[], so we
    // have an opportunity to handle version differences without affecting
    // efficiency.
    int version = in.readByte();
    if (version > RESULT_VERSION) {
      throw new IOException("version not supported");
    }
    int numResults = in.readInt();
    if(numResults == 0) {
      return new Result[0];
    }
    Result [] results = new Result[numResults];
    int bufSize = in.readInt();
    byte[] buf = new byte[bufSize];
    int offset = 0;
    for(int i=0;i<numResults;i++) {
      int numKeys = in.readInt();
      offset += Bytes.SIZEOF_INT;
      if(numKeys == 0) {
        results[i] = new Result((ImmutableBytesWritable)null);
        continue;
      }
      int initialOffset = offset;
      for(int j=0;j<numKeys;j++) {
        int keyLen = in.readInt();
        Bytes.putInt(buf, offset, keyLen);
        offset += Bytes.SIZEOF_INT;
        in.readFully(buf, offset, keyLen);
        offset += keyLen;
      }
      int totalLength = offset - initialOffset;
      results[i] = new Result(new ImmutableBytesWritable(buf, initialOffset,
          totalLength));
    }
    return results;
  }

  /**
   * Returns a copy of the kv-list.
   * @return a copy of the kv-list.
   */
  @ThriftField(1)
  public List<KeyValue> getKvs() {
    List<KeyValue> listToReturn = new ArrayList<KeyValue>(kvs.length);
    Collections.addAll(listToReturn, kvs);
    return listToReturn;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(kvs);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Result other = (Result) obj;
    if (bytes == null) {
      if (other.bytes != null)
        return false;
    }
    if (!Arrays.equals(kvs, other.kvs))
      return false;
    return true;
  }

  public int getBytesSize() {
    int ret = 0;
    for (KeyValue kv : this.kvs) {
      ret += kv.getLength();
    }
    return ret;
  }


}
