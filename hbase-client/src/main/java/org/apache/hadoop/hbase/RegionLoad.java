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

import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;

/**
  * Encapsulates per-region load metrics.
  */
@InterfaceAudience.Public
public class RegionLoad {

  protected ClusterStatusProtos.RegionLoad regionLoadPB;

  @InterfaceAudience.Private
  public RegionLoad(ClusterStatusProtos.RegionLoad regionLoadPB) {
    this.regionLoadPB = regionLoadPB;
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
    return Bytes.toStringBinary(getName());
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             ((<a href="https://issues.apache.org/jira/browse/HBASE-3935">HBASE-3935</a>)).
   *             Use {@link #getStorefileIndexSizeKB()} instead.
   */
  @Deprecated
  public int getStorefileIndexSizeMB() {
    // Return value divided by 1024
    return (int) (regionLoadPB.getStorefileIndexSizeKB() >> 10);
  }

  public long getStorefileIndexSizeKB() {
    return regionLoadPB.getStorefileIndexSizeKB();
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
   * @return the number of filtered read requests made to region
   */
  public long getFilteredReadRequestsCount() {
    return regionLoadPB.getFilteredReadRequestsCount();
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

  /**
   * @return completed sequence id per store.
   */
  public List<StoreSequenceId> getStoreCompleteSequenceId() {
    return regionLoadPB.getStoreCompleteSequenceIdList();
  }

  /**
   * @return the uncompressed size of the storefiles in MB.
   */
  public int getStoreUncompressedSizeMB() {
    return regionLoadPB.getStoreUncompressedSizeMB();
  }

  /**
   * @return the data locality of region in the regionserver.
   */
  public float getDataLocality() {
    if (regionLoadPB.hasDataLocality()) {
      return regionLoadPB.getDataLocality();
    }
    return 0.0f;
  }

  /**
   * @return the timestamp of the oldest hfile for any store of this region.
   */
  public long getLastMajorCompactionTs() {
    return regionLoadPB.getLastMajorCompactionTs();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "numberOfStores",
        this.getStores());
    sb = Strings.appendKeyValue(sb, "numberOfStorefiles",
        this.getStorefiles());
    sb = Strings.appendKeyValue(sb, "storefileUncompressedSizeMB",
      this.getStoreUncompressedSizeMB());
    sb = Strings.appendKeyValue(sb, "lastMajorCompactionTimestamp",
      this.getLastMajorCompactionTs());
    sb = Strings.appendKeyValue(sb, "storefileSizeMB",
        this.getStorefileSizeMB());
    if (this.getStoreUncompressedSizeMB() != 0) {
      sb = Strings.appendKeyValue(sb, "compressionRatio",
          String.format("%.4f", (float) this.getStorefileSizeMB() /
              (float) this.getStoreUncompressedSizeMB()));
    }
    sb = Strings.appendKeyValue(sb, "memstoreSizeMB",
        this.getMemStoreSizeMB());
    sb = Strings.appendKeyValue(sb, "storefileIndexSizeKB",
        this.getStorefileIndexSizeKB());
    sb = Strings.appendKeyValue(sb, "readRequestsCount",
        this.getReadRequestsCount());
    sb = Strings.appendKeyValue(sb, "writeRequestsCount",
        this.getWriteRequestsCount());
    sb = Strings.appendKeyValue(sb, "rootIndexSizeKB",
        this.getRootIndexSizeKB());
    sb = Strings.appendKeyValue(sb, "totalStaticIndexSizeKB",
        this.getTotalStaticIndexSizeKB());
    sb = Strings.appendKeyValue(sb, "totalStaticBloomSizeKB",
        this.getTotalStaticBloomSizeKB());
    sb = Strings.appendKeyValue(sb, "totalCompactingKVs",
        this.getTotalCompactingKVs());
    sb = Strings.appendKeyValue(sb, "currentCompactedKVs",
        this.getCurrentCompactedKVs());
    float compactionProgressPct = Float.NaN;
    if (this.getTotalCompactingKVs() > 0) {
      compactionProgressPct = ((float) this.getCurrentCompactedKVs() /
          (float) this.getTotalCompactingKVs());
    }
    sb = Strings.appendKeyValue(sb, "compactionProgressPct",
        compactionProgressPct);
    sb = Strings.appendKeyValue(sb, "completeSequenceId",
        this.getCompleteSequenceId());
    sb = Strings.appendKeyValue(sb, "dataLocality",
        this.getDataLocality());
    return sb.toString();
  }
}
