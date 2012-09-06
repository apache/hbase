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

package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.TreeSet;
import java.util.Set;

/**
  * Encapsulates per-region load metrics.
  */
@InterfaceAudience.Private
public class RegionLoad {

  protected HBaseProtos.RegionLoad regionLoadPB;

  public RegionLoad(HBaseProtos.RegionLoad regionLoadPB) {
    this.regionLoadPB = regionLoadPB;
  }

  // Getters
  private String[] getCoprocessors() {
    int numCoprocessors = regionLoadPB.getCoprocessorsCount();
    String [] ret = new String[numCoprocessors];
    for (int i = 0; i < numCoprocessors; ++i) {
      ret[i] = regionLoadPB.getCoprocessors(i).getName();
    }
    return ret;
  }

  /**
   * @return the region name
   */
  public byte[] getName() {
    return regionLoadPB.getRegionSpecifier().getValue().toByteArray();
  }

  /**
   * @return the region name as a string
   */
  public String getNameAsString() {
    return Bytes.toString(getName());
  }

  /**
   * @return the number of stores
   */
  public int getStores() {
    return regionLoadPB.getStores();
  }

  /**
   * @return the number of storefiles
   */
  public int getStorefiles() {
    return regionLoadPB.getStorefiles();
  }

  /**
   * @return the total size of the storefiles, in MB
   */
  public int getStorefileSizeMB() {
    return regionLoadPB.getStorefileSizeMB();
  }

  /**
   * @return the memstore size, in MB
   */
  public int getMemStoreSizeMB() {
    return regionLoadPB.getMemstoreSizeMB();
  }

  /**
   * @return the approximate size of storefile indexes on the heap, in MB
   */
  public int getStorefileIndexSizeMB() {
    return regionLoadPB.getStorefileIndexSizeMB();
  }

  /**
   * @return the number of requests made to region
   */
  public long getRequestsCount() {
    return getReadRequestsCount() + getWriteRequestsCount();
  }

  /**
   * @return the number of read requests made to region
   */
  public long getReadRequestsCount() {
    return regionLoadPB.getReadRequestsCount();
  }

  /**
   * @return the number of write requests made to region
   */
  public long getWriteRequestsCount() {
    return regionLoadPB.getWriteRequestsCount();
  }

  /**
   * @return The current total size of root-level indexes for the region, in KB.
   */
  public int getRootIndexSizeKB() {
    return regionLoadPB.getRootIndexSizeKB();
  }

  /**
   * @return The total size of all index blocks, not just the root level, in KB.
   */
  public int getTotalStaticIndexSizeKB() {
    return regionLoadPB.getTotalStaticIndexSizeKB();
  }

  /**
   * @return The total size of all Bloom filter blocks, not just loaded into the
   * block cache, in KB.
   */
  public int getTotalStaticBloomSizeKB() {
    return regionLoadPB.getTotalStaticBloomSizeKB();
  }

  /**
   * @return the total number of kvs in current compaction
   */
  public long getTotalCompactingKVs() {
    return regionLoadPB.getTotalCompactingKVs();
  }

  /**
   * @return the number of already compacted kvs in current compaction
   */
  public long getCurrentCompactedKVs() {
    return regionLoadPB.getCurrentCompactedKVs();
  }

  /**
   * This does not really belong inside RegionLoad but its being done in the name of expediency.
   * @return the completed sequence Id for the region
   */
  public long getCompleteSequenceId() {
    return regionLoadPB.getCompleteSequenceId();
  }
}
