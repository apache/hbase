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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

public abstract class MutationProxy extends RowProxy {
  protected Durability durability_ = Durability.USE_DEFAULT;

  protected boolean bufferable_ = false;

  protected Map<byte [], List<KeyValue>> familyMap =
      new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);

  public MutationProxy addColumn(final byte[] family,
      final byte[] qualifier, final byte[] value) {
    return addColumn(family, qualifier, KeyValue.TIMESTAMP_NOW, value);
  }

  public MutationProxy addColumn(final byte[] family,
      final byte[] qualifier, final long ts, final byte [] value) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>(0);
      familyMap.put(family, list);
    }
    list.add(new KeyValue(row_, family,
        (qualifier == null ? HBaseClient.EMPTY_ARRAY : qualifier),
        ts, (value == null ? HBaseClient.EMPTY_ARRAY : value)));
    return this;
  }

  public Durability getDurability() {
    return durability_;
  }

  public void setDurability(final int durability) {
    this.durability_ = Durability.values()[durability];
  }

  public boolean isBufferable() {
    return bufferable_;
  }

  public void setBufferable(boolean bufferable) {
    this.bufferable_ = bufferable;
  }

  public Map<byte[], List<KeyValue>> getFamilyMap() {
    return familyMap;
  }

  public abstract Mutation toHBaseMutation();

  public abstract void send(final HBaseClient client,
      final MutationCallbackHandler<Object, Object> cbh);
}
