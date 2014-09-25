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

package org.apache.hadoop.hbase.io.hfile.slab;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;

/**
 * Interface for objects that want to know when actions occur in a SingleSizeCache.
 *
 * @deprecated As of 1.0, replaced by {@link org.apache.hadoop.hbase.io.hfile.bucket.BucketCache}.
 */
@InterfaceAudience.Private
@Deprecated
interface SlabItemActionWatcher {

  /**
   * This is called as a callback when an item is removed from a SingleSizeCache.
   *
   * @param key the key of the item being evicted
   * @param notifier the object notifying the SlabCache of the eviction.
   */
  void onEviction(BlockCacheKey key, SingleSizeCache notifier);
  
  /**
   * This is called as a callback when an item is inserted into a SingleSizeCache.
   *
   * @param key the key of the item being added
   * @param notifier the object notifying the SlabCache of the insertion..
   */
  void onInsertion(BlockCacheKey key, SingleSizeCache notifier);
}
