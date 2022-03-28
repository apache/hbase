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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A more restricted interface for HStore. Only gives the caller access to information
 * about store configuration/settings that cannot easily be obtained from XML config object.
 * Example user would be CompactionPolicy that doesn't need entire (H)Store, only this.
 * Add things here as needed.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface StoreConfigInformation {
  /**
   * @return Gets the Memstore flush size for the region that this store works with.
   */
  // TODO: Why is this in here?  It should be in Store and it should return the Store flush size,
  // not the Regions.  St.Ack
  long getMemStoreFlushSize();

  /**
   * @return Gets the cf-specific time-to-live for store files.
   */
  long getStoreFileTtl();

  /**
   * @return Gets the cf-specific compaction check frequency multiplier.
   *         The need for compaction (outside of normal checks during flush, open, etc.) will
   *         be ascertained every multiplier * HConstants.THREAD_WAKE_FREQUENCY milliseconds.
   */
  long getCompactionCheckMultiplier();

  /**
   * The number of files required before flushes for this store will be blocked.
   */
  long getBlockingFileCount();

  RegionInfo getRegionInfo();

  String getColumnFamilyName();
}
