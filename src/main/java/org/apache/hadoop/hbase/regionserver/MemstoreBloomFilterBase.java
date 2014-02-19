/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Hash;

public class MemstoreBloomFilterBase implements MemstoreBloomFilter{
  protected final ConcurrentHashMap<Integer, Boolean> map;
  final Hash hash;

  MemstoreBloomFilterBase(Configuration conf) {
    map = new ConcurrentHashMap<Integer, Boolean>();
    hash = Hash.getInstance(conf);
  }

  private int hashKey(byte[] buffer, int offset, int length) {
    int h = hash.hash(buffer, offset, length, 0);
    h = hash.hash(buffer, offset, length, h);
    return h;
  }

  protected void addToMap(byte[] buffer, int offset, int length) {
    map.put(hashKey(buffer, offset, length), true);
  }

  protected boolean containsInMap(byte[] buffer, int offset, int length) {
    return map.containsKey(hashKey(buffer, offset, length));
  }

  @Override
  public void add(KeyValue kv) {
    // Empty add method. Sub classes should implement the respective add method.
  }

  @Override
  public boolean contains(KeyValue kv) {
    // Sub classes should implement the respective add method.
    return true;
  }
}
