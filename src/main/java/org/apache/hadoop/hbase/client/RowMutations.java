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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Performs multiple mutations atomically on a single row.
 * Currently {@link Put} and {@link Delete} are supported.
 *
 * The mutations are performed in the order in which they
 * were added.
 */
public class RowMutations extends Operation implements Row {
  private List<Mutation> mutations = new ArrayList<Mutation>();
  protected byte [] row;
  private static final byte VERSION = (byte)1;

  /** Constructor for Writable. DO NOT USE */
  public RowMutations() {}

  /**
   * Create an atomic mutation for the specified row.
   * @param row row key
   */
  public RowMutations(byte [] row) {
    if(row == null || row.length > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("Row key is invalid");
    }
    this.row = Arrays.copyOf(row, row.length);
  }

  /**
   * Add a {@link Put} operation to the list of mutations
   * @param p The {@link Put} to add
   * @throws IOException
   */
  public void add(Put p) throws IOException {
    internalAdd(p);
  }

  /**
   * Add a {@link Delete} operation to the list of mutations
   * @param d The {@link Delete} to add
   * @throws IOException
   */
  public void add(Delete d) throws IOException {
    internalAdd(d);
  }

  private void internalAdd(Mutation m) throws IOException {
    int res = Bytes.compareTo(this.row, m.getRow());
    if(res != 0) {
      throw new IOException("The row in the recently added Put/Delete " +
          Bytes.toStringBinary(m.getRow()) + " doesn't match the original one " +
          Bytes.toStringBinary(this.row));
    }
    mutations.add(m);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    int version = in.readByte();
    if (version > VERSION) {
      throw new IOException("version not supported");
    }
    this.row = Bytes.readByteArray(in);
    int numMutations = in.readInt();
    mutations.clear();
    for(int i=0; i<numMutations; i++) {
      mutations.add((Mutation) HbaseObjectWritable.readObject(in, null));
    }
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeByte(VERSION);
    Bytes.writeByteArray(out, this.row);
    out.writeInt(mutations.size());
    for (Mutation m : mutations) {
      HbaseObjectWritable.writeObject(out, m, m.getClass(), null);
    }
  }

  @Override
  public int compareTo(Row i) {
    return Bytes.compareTo(this.getRow(), i.getRow());
  }

  @Override
  public byte[] getRow() {
    return row;
  }

  /**
   * @return An unmodifiable list of the current mutations.
   */
  public List<Mutation> getMutations() {
    return Collections.unmodifiableList(mutations);
  }
  
  /**
   * Compile the column family (i.e. schema) information
   * into a Map. Useful for parsing and aggregation by debugging,
   * logging, and administration tools.
   * 
   * e.g: {num-delete=1, num-put=1, row=testRow}
   * @return Map
   */
  @Override
  public Map<String, Object> getFingerprint() {
    Map<String, Object> map = new HashMap<String, Object>();
    // ideally, we would also include table information, but that information
    // is not stored in each Operation instance.
    map.put("row", Bytes.toStringBinary(this.row));
    int deleteCnt = 0, putCnt = 0;
    for (Mutation mod: this.mutations) {
      if (mod instanceof Put) {
        putCnt++;
      }
      else {
        deleteCnt++;
      }
    }
    map.put("num-put", putCnt);
    map.put("num-delete", deleteCnt);
    return map;
  }

  /**
   * Compile the details beyond the scope of getFingerprint (row, columns,
   * timestamps, etc.) into a Map along with the fingerprinted information.
   * Useful for debugging, logging, and administration tools.
   * @param maxCols a limit on the number of columns output prior to truncation
   * e.g:
   *       {mutations=
   *             [Delete:{"operation":"Delete","totalColumns":1,"families":{...},"row":"testRow"},
   *              Put:{"operation":"Put","totalColumns":1,"families":{...},"row":"testRow"}],
   *        "num-delete"=1, 
   *        "num-put"=1,
   *        "row"="testRow"}
   * @return Map
   */
  @Override
  public Map<String, Object> toMap(int maxCols) {
    // we start with the fingerprint map and build on top of it.
    Map<String, Object> map = getFingerprint();
    // replace the fingerprint's simple list of families with a 
    // map from column families to lists of qualifiers and kv details
    List<Map<String, Object>> mutationDetails =
      new ArrayList<Map<String, Object>>();
    map.put("row", Bytes.toStringBinary(this.row));
    map.put("mutations", mutationDetails);
    // iterate through all the mutations add the map for the mutation
    int count = 0;
    for (Mutation mod: this.mutations) {
     mutationDetails.add(mod.toMap(maxCols));
     if (++count > maxCols)
       break;
    }
    return map;
  }

}
