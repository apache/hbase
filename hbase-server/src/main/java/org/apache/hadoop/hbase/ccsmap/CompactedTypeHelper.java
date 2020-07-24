/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.ccsmap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public interface CompactedTypeHelper<K, V> {

  /**
   * Get estimated size of compacted k-v
   */
  int getCompactedSize(K key, V value);

  /**
   * Compact the key-value into data.
   * ONLY the area from data[offset] to data[offset+len-1] can be used for this key&value
   * If you write out the area, the result is unpredictable
   */
  void compact(K key, V value, byte[] data, int offset, int len);

  /**
   * Deserialize the key&value from data.
   * ONLY the area from data[offset] to data[offset+len-1] can be used for this key&value.
   * If you write out of the range, result will be unpredictable
   */
  KVPair<K, V> decomposite(byte[] data, int offset, int len);

  /**
   * Compare kv1 at data1[offset1, offset1+len1) with kv2 at data2[offset2, offset2+len2)
   * if key1 == key2:
   *     return 0
   * if key1 > key2:
   *     return >0
   * if key1 < key2:
   *     return <0
   * @param data1  kv1's data fragment
   * @param offset1 kv1's data offset in the fragment
   * @param len1  kv1's data length in the fragment
   * @param data2 kv2's data fragment
   * @param offset2 v2's data offset in the fragment
   * @param len2 kv2's data length in the fragment
   * @return
   */
  int compare(byte[] data1, int offset1, int len1, byte[] data2, int offset2, int len2);
  int compare(K key, byte[] data, int offset, int len);
  int compare(K key1, K key2);

}
