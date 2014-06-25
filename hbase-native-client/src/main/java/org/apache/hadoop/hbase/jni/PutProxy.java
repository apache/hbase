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
 *
 */
package org.apache.hadoop.hbase.jni;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

public class PutProxy extends MutationProxy {
  public PutProxy(byte[] row) {
    this.row_ = row;
  }

  @Override
  public Mutation toHBaseMutation() {
    final Put put = new Put(row_);
    put.setDurability(durability_);
    for (byte[] family : familyMap.keySet()) {
      final List<KeyValue> kvList = familyMap.get(family);
      final int numKVs = kvList.size();
      for (int i = 0; i < numKVs; i++) {
        KeyValue kv = kvList.get(i);
        put.add(kv.family(), kv.qualifier(), kv.timestamp(), kv.value());
      }
    }
    return put;
  }

  @Override
  public void send(final HBaseClient client,
      final MutationCallbackHandler<Object, Object> cbh) {
    final Map<byte[], List<KeyValue>> familyMap = getFamilyMap();
    final int numFamilies = familyMap.size();
    final byte[][] families = new byte[numFamilies][];
    final byte[][][] qualifiers = new byte[numFamilies][][];
    final byte[][][] values = new byte[numFamilies][][];
    final long[][] timestamps = new long[numFamilies][];
    int idx = 0;
    for (byte[] family : familyMap.keySet()) {
      families[idx] = family;
      final List<KeyValue> kvList = familyMap.get(family);
      final int numKVs = kvList.size();
      qualifiers[idx] = new byte[numKVs][];
      values[idx] = new byte[numKVs][];
      timestamps[idx] = new long[numKVs];
      for (int i = 0; i < numKVs; i++) {
        KeyValue kv = kvList.get(i);
        qualifiers[idx][i] = kv.qualifier();
        values[idx][i] = kv.value();
        timestamps[idx][i] = kv.timestamp();
      }
      idx++;
    }
    final PutRequest put = new PutRequest(getTable(),
        getRow(), families, qualifiers, values, timestamps);
    // set attributes
    put.setDurable(getDurability() != Durability.SKIP_WAL);
    put.setBufferable(isBufferable());
    // hand over and attach callback
    client.put(put).addBoth(cbh);
  }
}
