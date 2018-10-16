/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An interface for concurrently storing and updating the size of a Region.
 */
@InterfaceAudience.Private
public interface RegionSizeStore extends Iterable<Entry<RegionInfo,RegionSize>>, HeapSize {

  /**
   * Returns the size for the give region if one exists. If no size exists, {@code null} is
   * returned.
   *
   * @param regionInfo The region whose size is being fetched.
   * @return The size in bytes of the region or null if no size is stored.
   */
  RegionSize getRegionSize(RegionInfo regionInfo);

  /**
   * Atomically sets the given {@code size} for a region.
   *
   * @param regionInfo An identifier for a region.
   * @param size The size in bytes of the region.
   */
  void put(RegionInfo regionInfo, long size);

  /**
   * Atomically alter the size of a region.
   *
   * @param regionInfo The region to update.
   * @param delta The change in size for the region, positive or negative.
   */
  void incrementRegionSize(RegionInfo regionInfo, long delta);

  /**
   * Removes the mapping for the given key, returning the value if one exists in the store.
   *
   * @param regionInfo The key to remove from the store
   * @return The value removed from the store if one exists, otherwise null.
   */
  RegionSize remove(RegionInfo regionInfo);

  /**
   * Returns the number of entries in the store.
   *
   * @return The number of entries in the store.
   */
  int size();

  /**
   * Returns if the store is empty.
   *
   * @return true if there are no entries in the store, otherwise false.
   */
  boolean isEmpty();

  /**
   * Removes all entries from the store.
   */
  void clear();
}