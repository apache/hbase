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
package org.apache.hadoop.hbase.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Performs Append operations on a single row.
 * <p>
 * Note that this operation does not appear atomic to readers. Appends are done
 * under a single row lock, so write operations to a row are synchronized, but
 * readers do not take row locks so get and scan operations can see this
 * operation partially completed.
 * <p>
 * To append to a set of columns of a row, instantiate an Append object with the
 * row to append to. At least one column to append must be specified using the
 * {@link #add(byte[], byte[], byte[])} method.
 */
public class Append extends Mutation implements Writable {
  // TODO: refactor to derive from Put?
  private static final String RETURN_RESULTS = "_rr_";
  private static final byte APPEND_VERSION = (byte)1;

  /**
   * @param returnResults
   *          True (default) if the append operation should return the results.
   *          A client that is not interested in the result can save network
   *          bandwidth setting this to false.
   */
  public void setReturnResults(boolean returnResults) {
    setAttribute(RETURN_RESULTS, Bytes.toBytes(returnResults));
  }

  /**
   * @return current setting for returnResults
   */
  public boolean isReturnResults() {
    byte[] v = getAttribute(RETURN_RESULTS);
    return v == null ? true : Bytes.toBoolean(v);
  }

  /** Constructor for Writable.  DO NOT USE */
  public Append() {}

  /**
   * Create a Append operation for the specified row.
   * <p>
   * At least one column must be appended to.
   * @param row row key
   */
  public Append(byte[] row) {
    this.row = Arrays.copyOf(row, row.length);
  }

  /**
   * Add the specified column and value to this Append operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param value value to append to specified column
   * @return this
   */
  public Append add(byte [] family, byte [] qualifier, byte [] value) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>();
    }
    list.add(new KeyValue(
        this.row, family, qualifier, this.ts, KeyValue.Type.Put, value));
    familyMap.put(family, list);
    return this;
  }

  @Override
  public void readFields(final DataInput in)
  throws IOException {
    int version = in.readByte();
    if (version > APPEND_VERSION) {
      throw new IOException("version not supported: "+version);
    }
    this.row = Bytes.readByteArray(in);
    this.ts = in.readLong();
    this.lockId = in.readLong();
    this.writeToWAL = in.readBoolean();
    int numFamilies = in.readInt();
    if (!this.familyMap.isEmpty()) this.familyMap.clear();
    for(int i=0;i<numFamilies;i++) {
      byte [] family = Bytes.readByteArray(in);
      int numKeys = in.readInt();
      List<KeyValue> keys = new ArrayList<KeyValue>(numKeys);
      int totalLen = in.readInt();
      byte [] buf = new byte[totalLen];
      int offset = 0;
      for (int j = 0; j < numKeys; j++) {
        int keyLength = in.readInt();
        in.readFully(buf, offset, keyLength);
        keys.add(new KeyValue(buf, offset, keyLength));
        offset += keyLength;
      }
      this.familyMap.put(family, keys);
    }
    readAttributes(in);
  }

  @Override
  public void write(final DataOutput out)
  throws IOException {
    out.writeByte(APPEND_VERSION);
    Bytes.writeByteArray(out, this.row);
    out.writeLong(this.ts);
    out.writeLong(this.lockId);
    out.writeBoolean(this.writeToWAL);
    out.writeInt(familyMap.size());
    for (Map.Entry<byte [], List<KeyValue>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      List<KeyValue> keys = entry.getValue();
      out.writeInt(keys.size());
      int totalLen = 0;
      for(KeyValue kv : keys) {
        totalLen += kv.getLength();
      }
      out.writeInt(totalLen);
      for(KeyValue kv : keys) {
        out.writeInt(kv.getLength());
        out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
      }
    }
    writeAttributes(out);
  }
}
