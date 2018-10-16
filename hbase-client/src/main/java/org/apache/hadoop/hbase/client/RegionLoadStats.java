/**
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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * POJO representing region server load
 */
@InterfaceAudience.Public
public class RegionLoadStats {
  int memstoreLoad;
  int heapOccupancy;
  int compactionPressure;

  public RegionLoadStats(int memstoreLoad, int heapOccupancy, int compactionPressure) {
    this.memstoreLoad = memstoreLoad;
    this.heapOccupancy = heapOccupancy;
    this.compactionPressure = compactionPressure;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link #getMemStoreLoad()} instead
   */
  @Deprecated
  public int getMemstoreLoad() {
    return this.memstoreLoad;
  }

  public int getMemStoreLoad() {
    return this.memstoreLoad;
  }

  public int getHeapOccupancy() {
    return this.heapOccupancy;
  }

  public int getCompactionPressure() {
    return this.compactionPressure;
  }
}
