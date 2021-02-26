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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

/**
 * Encapsulates per-region load metrics.
 * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
 *             Use {@link RegionMetrics} instead.
 */
@InterfaceAudience.Public
@Deprecated
public class RegionLoad implements RegionMetrics {
  // DONT use this pb object since the byte array backed may be modified in rpc layer
  // we keep this pb object for BC.
  protected ClusterStatusProtos.RegionLoad regionLoadPB;
  private final RegionMetrics metrics;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  public RegionLoad(ClusterStatusProtos.RegionLoad regionLoadPB) {
    this.regionLoadPB = regionLoadPB;
    this.metrics = RegionMetricsBuilder.toRegionMetrics(regionLoadPB);
  }

  RegionLoad(RegionMetrics metrics) {
    this.metrics = metrics;
    this.regionLoadPB = RegionMetricsBuilder.toRegionLoad(metrics);
  }

  /**
   * @return the region name
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegionName} instead.
   */
  @Deprecated
  public byte[] getName() {
    return metrics.getRegionName();
  }

  @Override
  public byte[] getRegionName() {
    return metrics.getRegionName();
  }

  @Override
  public int getStoreCount() {
    return metrics.getStoreCount();
  }

  @Override
  public int getStoreFileCount() {
    return metrics.getStoreFileCount();
  }

  @Override
  public Size getStoreFileSize() {
    return metrics.getStoreFileSize();
  }

  @Override
  public Size getMemStoreSize() {
    return metrics.getMemStoreSize();
  }

  @Override
  public long getReadRequestCount() {
    return metrics.getReadRequestCount();
  }

  @Override
  public long getFilteredReadRequestCount() {
    return metrics.getFilteredReadRequestCount();
  }

  @Override
  public Size getStoreFileIndexSize() {
    return metrics.getStoreFileIndexSize();
  }

  @Override
  public long getWriteRequestCount() {
    return metrics.getWriteRequestCount();
  }

  @Override
  public Size getStoreFileRootLevelIndexSize() {
    return metrics.getStoreFileRootLevelIndexSize();
  }

  @Override
  public Size getStoreFileUncompressedDataIndexSize() {
    return metrics.getStoreFileUncompressedDataIndexSize();
  }

  @Override
  public Size getBloomFilterSize() {
    return metrics.getBloomFilterSize();
  }

  @Override
  public long getCompactingCellCount() {
    return metrics.getCompactingCellCount();
  }

  @Override
  public long getCompactedCellCount() {
    return metrics.getCompactedCellCount();
  }

  @Override
  public long getCompletedSequenceId() {
    return metrics.getCompletedSequenceId();
  }

  @Override
  public Map<byte[], Long> getStoreSequenceId() {
    return metrics.getStoreSequenceId();
  }

  @Override
  public Size getUncompressedStoreFileSize() {
    return metrics.getUncompressedStoreFileSize();
  }

  /**
   * @return the number of stores
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getStoreCount} instead.
   */
  @Deprecated
  public int getStores() {
    return metrics.getStoreCount();
  }

  /**
   * @return the number of storefiles
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getStoreFileCount} instead.
   */
  @Deprecated
  public int getStorefiles() {
    return metrics.getStoreFileCount();
  }

  /**
   * @return the total size of the storefiles, in MB
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getStoreFileSize} instead.
   */
  @Deprecated
  public int getStorefileSizeMB() {
    return (int) metrics.getStoreFileSize().get(Size.Unit.MEGABYTE);
  }

  /**
   * @return the memstore size, in MB
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getMemStoreSize} instead.
   */
  @Deprecated
  public int getMemStoreSizeMB() {
    return (int) metrics.getMemStoreSize().get(Size.Unit.MEGABYTE);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             ((<a href="https://issues.apache.org/jira/browse/HBASE-3935">HBASE-3935</a>)).
   *             Use {@link #getStoreFileRootLevelIndexSize} instead.
   */
  @Deprecated
  public int getStorefileIndexSizeMB() {
    // Return value divided by 1024
    return (getRootIndexSizeKB() >> 10);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getStoreFileRootLevelIndexSize()} instead.
   */
  @Deprecated
  public int getStorefileIndexSizeKB() {
    return getRootIndexSizeKB();
  }

  /**
   * @return the number of requests made to region
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRequestCount()} instead.
   */
  @Deprecated
  public long getRequestsCount() {
    return metrics.getRequestCount();
  }

  /**
   * @return the number of read requests made to region
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getReadRequestCount} instead.
   */
  @Deprecated
  public long getReadRequestsCount() {
    return metrics.getReadRequestCount();
  }

  /**
   * @return the number of filtered read requests made to region
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getFilteredReadRequestCount} instead.
   */
  @Deprecated
  public long getFilteredReadRequestsCount() {
    return metrics.getFilteredReadRequestCount();
  }

  /**
   * @return the number of write requests made to region
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getWriteRequestCount} instead.
   */
  @Deprecated
  public long getWriteRequestsCount() {
    return metrics.getWriteRequestCount();
  }

  /**
   * @return The current total size of root-level indexes for the region, in KB.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getStoreFileRootLevelIndexSize} instead.
   */
  @Deprecated
  public int getRootIndexSizeKB() {
    return (int) metrics.getStoreFileRootLevelIndexSize().get(Size.Unit.KILOBYTE);
  }

  /**
   * @return The total size of all index blocks, not just the root level, in KB.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getStoreFileUncompressedDataIndexSize} instead.
   */
  @Deprecated
  public int getTotalStaticIndexSizeKB() {
    return (int) metrics.getStoreFileUncompressedDataIndexSize().get(Size.Unit.KILOBYTE);
  }

  /**
   * @return The total size of all Bloom filter blocks, not just loaded into the
   * block cache, in KB.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getBloomFilterSize} instead.
   */
  @Deprecated
  public int getTotalStaticBloomSizeKB() {
    return (int) metrics.getBloomFilterSize().get(Size.Unit.KILOBYTE);
  }

  /**
   * @return the total number of kvs in current compaction
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getCompactingCellCount} instead.
   */
  @Deprecated
  public long getTotalCompactingKVs() {
    return metrics.getCompactingCellCount();
  }

  /**
   * @return the number of already compacted kvs in current compaction
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getCompactedCellCount} instead.
   */
  @Deprecated
  public long getCurrentCompactedKVs() {
    return metrics.getCompactedCellCount();
  }

  /**
   * This does not really belong inside RegionLoad but its being done in the name of expediency.
   * @return the completed sequence Id for the region
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getCompletedSequenceId} instead.
   */
  @Deprecated
  public long getCompleteSequenceId() {
    return metrics.getCompletedSequenceId();
  }

  /**
   * @return completed sequence id per store.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getStoreSequenceId} instead.
   */
  @Deprecated
  public List<ClusterStatusProtos.StoreSequenceId> getStoreCompleteSequenceId() {
    return metrics.getStoreSequenceId().entrySet().stream()
        .map(s -> ClusterStatusProtos.StoreSequenceId.newBuilder()
                  .setFamilyName(UnsafeByteOperations.unsafeWrap(s.getKey()))
                  .setSequenceId(s.getValue())
                  .build())
        .collect(Collectors.toList());
  }

  /**
   * @return the uncompressed size of the storefiles in MB.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getUncompressedStoreFileSize} instead.
   */
  @Deprecated
  public int getStoreUncompressedSizeMB() {
    return (int) metrics.getUncompressedStoreFileSize().get(Size.Unit.KILOBYTE);
  }

  /**
   * @return the data locality of region in the regionserver.
   */
  @Override
  public float getDataLocality() {
    return metrics.getDataLocality();
  }

  @Override
  public long getLastMajorCompactionTimestamp() {
    return metrics.getLastMajorCompactionTimestamp();
  }

  /**
   * @return the timestamp of the oldest hfile for any store of this region.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getLastMajorCompactionTimestamp} instead.
   */
  @Deprecated
  public long getLastMajorCompactionTs() {
    return metrics.getLastMajorCompactionTimestamp();
  }

  /**
   * @return the reference count for the stores of this region
   */
  public int getStoreRefCount() {
    return metrics.getStoreRefCount();
  }

  @Override
  public int getMaxCompactedStoreFileRefCount() {
    return metrics.getMaxCompactedStoreFileRefCount();
  }

  @Override
  public float getDataLocalityForSsd() {
    return metrics.getDataLocalityForSsd();
  }

  @Override
  public long getBlocksLocalWeight() {
    return metrics.getBlocksLocalWeight();
  }

  @Override
  public long getBlocksLocalWithSsdWeight() {
    return metrics.getBlocksLocalWithSsdWeight();
  }

  @Override
  public long getBlocksTotalWeight() {
    return metrics.getBlocksTotalWeight();
  }

  @Override
  public CompactionState getCompactionState() {
    return metrics.getCompactionState();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "numberOfStores",
        this.getStores());
    Strings.appendKeyValue(sb, "numberOfStorefiles", this.getStorefiles());
    Strings.appendKeyValue(sb, "storeRefCount", this.getStoreRefCount());
    Strings.appendKeyValue(sb, "storefileUncompressedSizeMB",
        this.getStoreUncompressedSizeMB());
    Strings.appendKeyValue(sb, "lastMajorCompactionTimestamp",
        this.getLastMajorCompactionTs());
    Strings.appendKeyValue(sb, "storefileSizeMB", this.getStorefileSizeMB());
    if (this.getStoreUncompressedSizeMB() != 0) {
      Strings.appendKeyValue(sb, "compressionRatio",
          String.format("%.4f", (float) this.getStorefileSizeMB() /
              (float) this.getStoreUncompressedSizeMB()));
    }
    Strings.appendKeyValue(sb, "memstoreSizeMB",
        this.getMemStoreSizeMB());
    Strings.appendKeyValue(sb, "readRequestsCount",
        this.getReadRequestsCount());
    Strings.appendKeyValue(sb, "writeRequestsCount",
        this.getWriteRequestsCount());
    Strings.appendKeyValue(sb, "rootIndexSizeKB",
        this.getRootIndexSizeKB());
    Strings.appendKeyValue(sb, "totalStaticIndexSizeKB",
        this.getTotalStaticIndexSizeKB());
    Strings.appendKeyValue(sb, "totalStaticBloomSizeKB",
        this.getTotalStaticBloomSizeKB());
    Strings.appendKeyValue(sb, "totalCompactingKVs",
        this.getTotalCompactingKVs());
    Strings.appendKeyValue(sb, "currentCompactedKVs",
        this.getCurrentCompactedKVs());
    float compactionProgressPct = Float.NaN;
    if (this.getTotalCompactingKVs() > 0) {
      compactionProgressPct = ((float) this.getCurrentCompactedKVs() /
          (float) this.getTotalCompactingKVs());
    }
    Strings.appendKeyValue(sb, "compactionProgressPct",
        compactionProgressPct);
    Strings.appendKeyValue(sb, "completeSequenceId",
        this.getCompleteSequenceId());
    Strings.appendKeyValue(sb, "dataLocality",
        this.getDataLocality());
    return sb.toString();
  }
}
