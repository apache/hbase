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

import org.apache.hadoop.hbase.KeyValue;

/**
 * An interface which provides the interface to be followed by the set of
 * in memory Bloom filters backed by the memstore.
 *
 * In general the in memory bloom filters for the memstore would be containing
 * two bloom filters where one is backed by the current working memstore and the
 * other is backed by the snapshot memstore.
 */
public interface MemstoreBloomFilter {

  /**
   * Adds the key value to the bloom filter. Specific implementations can
   * override the way the key value is inserted into the bloom filter.
   * @param kv
   */
  public void add(KeyValue kv);

  /**
   * Returns whether the given kv is present in the current active bloom filter.
   * @param kv
   * @return
   */
  public boolean contains(KeyValue kv);
}
