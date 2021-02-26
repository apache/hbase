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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@InterfaceAudience.Private
public final class RegionMetricsBuilder {

  public static List<RegionMetrics> toRegionMetrics(
      AdminProtos.GetRegionLoadResponse regionLoadResponse) {
    return regionLoadResponse.getRegionLoadsList().stream()
        .map(RegionMetricsBuilder::toRegionMetrics).collect(Collectors.toList());
  }

  public static RegionMetrics toRegionMetrics(ClusterStatusProtos.RegionLoad regionLoadPB) {
    return RegionMetricsBuilder
        .newBuilder(regionLoadPB.getRegionSpecifier().getValue().toByteArray())
        .setBloomFilterSize(new Size(regionLoadPB.getTotalStaticBloomSizeKB(), Size.Unit.KILOBYTE))
        .setCompactedCellCount(regionLoadPB.getCurrentCompactedKVs())
        .setCompactingCellCount(regionLoadPB.getTotalCompactingKVs())
        .setCompletedSequenceId(regionLoadPB.getCompleteSequenceId())
        .setDataLocality(regionLoadPB.hasDataLocality() ? regionLoadPB.getDataLocality() : 0.0f)
        .setDataLocalityForSsd(regionLoadPB.hasDataLocalityForSsd() ?
          regionLoadPB.getDataLocalityForSsd() : 0.0f)
        .setBlocksLocalWeight(regionLoadPB.hasBlocksLocalWeight() ?
          regionLoadPB.getBlocksLocalWeight() : 0)
        .setBlocksLocalWithSsdWeight(regionLoadPB.hasBlocksLocalWithSsdWeight() ?
          regionLoadPB.getBlocksLocalWithSsdWeight() : 0)
        .setBlocksTotalWeight(regionLoadPB.getBlocksTotalWeight())
        .setCompactionState(ProtobufUtil.createCompactionStateForRegionLoad(
          regionLoadPB.getCompactionState()))
        .setFilteredReadRequestCount(regionLoadPB.getFilteredReadRequestsCount())
        .setStoreFileUncompressedDataIndexSize(new Size(regionLoadPB.getTotalStaticIndexSizeKB(),
          Size.Unit.KILOBYTE))
        .setLastMajorCompactionTimestamp(regionLoadPB.getLastMajorCompactionTs())
        .setMemStoreSize(new Size(regionLoadPB.getMemStoreSizeMB(), Size.Unit.MEGABYTE))
        .setReadRequestCount(regionLoadPB.getReadRequestsCount())
        .setWriteRequestCount(regionLoadPB.getWriteRequestsCount())
        .setStoreFileIndexSize(new Size(regionLoadPB.getStorefileIndexSizeKB(),
          Size.Unit.KILOBYTE))
        .setStoreFileRootLevelIndexSize(new Size(regionLoadPB.getRootIndexSizeKB(),
          Size.Unit.KILOBYTE))
        .setStoreCount(regionLoadPB.getStores())
        .setStoreFileCount(regionLoadPB.getStorefiles())
        .setStoreRefCount(regionLoadPB.getStoreRefCount())
        .setMaxCompactedStoreFileRefCount(regionLoadPB.getMaxCompactedStoreFileRefCount())
        .setStoreFileSize(new Size(regionLoadPB.getStorefileSizeMB(), Size.Unit.MEGABYTE))
        .setStoreSequenceIds(regionLoadPB.getStoreCompleteSequenceIdList().stream()
          .collect(Collectors.toMap(
            (ClusterStatusProtos.StoreSequenceId s) -> s.getFamilyName().toByteArray(),
              ClusterStatusProtos.StoreSequenceId::getSequenceId)))
        .setUncompressedStoreFileSize(
          new Size(regionLoadPB.getStoreUncompressedSizeMB(),Size.Unit.MEGABYTE))
        .build();
  }

  private static List<ClusterStatusProtos.StoreSequenceId> toStoreSequenceId(
      Map<byte[], Long> ids) {
    return ids.entrySet().stream()
        .map(e -> ClusterStatusProtos.StoreSequenceId.newBuilder()
          .setFamilyName(UnsafeByteOperations.unsafeWrap(e.getKey()))
          .setSequenceId(e.getValue())
          .build())
        .collect(Collectors.toList());
  }

  public static ClusterStatusProtos.RegionLoad toRegionLoad(RegionMetrics regionMetrics) {
    return ClusterStatusProtos.RegionLoad.newBuilder()
        .setRegionSpecifier(HBaseProtos.RegionSpecifier
          .newBuilder().setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
          .setValue(UnsafeByteOperations.unsafeWrap(regionMetrics.getRegionName()))
          .build())
        .setTotalStaticBloomSizeKB((int) regionMetrics.getBloomFilterSize()
          .get(Size.Unit.KILOBYTE))
        .setCurrentCompactedKVs(regionMetrics.getCompactedCellCount())
        .setTotalCompactingKVs(regionMetrics.getCompactingCellCount())
        .setCompleteSequenceId(regionMetrics.getCompletedSequenceId())
        .setDataLocality(regionMetrics.getDataLocality())
        .setFilteredReadRequestsCount(regionMetrics.getFilteredReadRequestCount())
        .setTotalStaticIndexSizeKB((int) regionMetrics.getStoreFileUncompressedDataIndexSize()
          .get(Size.Unit.KILOBYTE))
        .setLastMajorCompactionTs(regionMetrics.getLastMajorCompactionTimestamp())
        .setMemStoreSizeMB((int) regionMetrics.getMemStoreSize().get(Size.Unit.MEGABYTE))
        .setReadRequestsCount(regionMetrics.getReadRequestCount())
        .setWriteRequestsCount(regionMetrics.getWriteRequestCount())
        .setStorefileIndexSizeKB((long) regionMetrics.getStoreFileIndexSize()
          .get(Size.Unit.KILOBYTE))
        .setRootIndexSizeKB((int) regionMetrics.getStoreFileRootLevelIndexSize()
          .get(Size.Unit.KILOBYTE))
        .setStores(regionMetrics.getStoreCount())
        .setStorefiles(regionMetrics.getStoreFileCount())
        .setStoreRefCount(regionMetrics.getStoreRefCount())
        .setMaxCompactedStoreFileRefCount(regionMetrics.getMaxCompactedStoreFileRefCount())
        .setStorefileSizeMB((int) regionMetrics.getStoreFileSize().get(Size.Unit.MEGABYTE))
        .addAllStoreCompleteSequenceId(toStoreSequenceId(regionMetrics.getStoreSequenceId()))
        .setStoreUncompressedSizeMB(
          (int) regionMetrics.getUncompressedStoreFileSize().get(Size.Unit.MEGABYTE))
        .build();
  }

  public static RegionMetricsBuilder newBuilder(byte[] name) {
    return new RegionMetricsBuilder(name);
  }

  private final byte[] name;
  private int storeCount;
  private int storeFileCount;
  private int storeRefCount;
  private int maxCompactedStoreFileRefCount;
  private long compactingCellCount;
  private long compactedCellCount;
  private Size storeFileSize = Size.ZERO;
  private Size memStoreSize = Size.ZERO;
  private Size indexSize = Size.ZERO;
  private Size rootLevelIndexSize = Size.ZERO;
  private Size uncompressedDataIndexSize = Size.ZERO;
  private Size bloomFilterSize = Size.ZERO;
  private Size uncompressedStoreFileSize = Size.ZERO;
  private long writeRequestCount;
  private long readRequestCount;
  private long filteredReadRequestCount;
  private long completedSequenceId;
  private Map<byte[], Long> storeSequenceIds = Collections.emptyMap();
  private float dataLocality;
  private long lastMajorCompactionTimestamp;
  private float dataLocalityForSsd;
  private long blocksLocalWeight;
  private long blocksLocalWithSsdWeight;
  private long blocksTotalWeight;
  private CompactionState compactionState;
  private RegionMetricsBuilder(byte[] name) {
    this.name = name;
  }

  public RegionMetricsBuilder setStoreCount(int value) {
    this.storeCount = value;
    return this;
  }
  public RegionMetricsBuilder setStoreFileCount(int value) {
    this.storeFileCount = value;
    return this;
  }
  public RegionMetricsBuilder setStoreRefCount(int value) {
    this.storeRefCount = value;
    return this;
  }
  public RegionMetricsBuilder setMaxCompactedStoreFileRefCount(int value) {
    this.maxCompactedStoreFileRefCount = value;
    return this;
  }
  public RegionMetricsBuilder setCompactingCellCount(long value) {
    this.compactingCellCount = value;
    return this;
  }
  public RegionMetricsBuilder setCompactedCellCount(long value) {
    this.compactedCellCount = value;
    return this;
  }
  public RegionMetricsBuilder setStoreFileSize(Size value) {
    this.storeFileSize = value;
    return this;
  }
  public RegionMetricsBuilder setMemStoreSize(Size value) {
    this.memStoreSize = value;
    return this;
  }
  public RegionMetricsBuilder setStoreFileIndexSize(Size value) {
    this.indexSize = value;
    return this;
  }
  public RegionMetricsBuilder setStoreFileRootLevelIndexSize(Size value) {
    this.rootLevelIndexSize = value;
    return this;
  }
  public RegionMetricsBuilder setStoreFileUncompressedDataIndexSize(Size value) {
    this.uncompressedDataIndexSize = value;
    return this;
  }
  public RegionMetricsBuilder setBloomFilterSize(Size value) {
    this.bloomFilterSize = value;
    return this;
  }
  public RegionMetricsBuilder setUncompressedStoreFileSize(Size value) {
    this.uncompressedStoreFileSize = value;
    return this;
  }
  public RegionMetricsBuilder setWriteRequestCount(long value) {
    this.writeRequestCount = value;
    return this;
  }
  public RegionMetricsBuilder setReadRequestCount(long value) {
    this.readRequestCount = value;
    return this;
  }
  public RegionMetricsBuilder setFilteredReadRequestCount(long value) {
    this.filteredReadRequestCount = value;
    return this;
  }
  public RegionMetricsBuilder setCompletedSequenceId(long value) {
    this.completedSequenceId = value;
    return this;
  }
  public RegionMetricsBuilder setStoreSequenceIds(Map<byte[], Long> value) {
    this.storeSequenceIds = value;
    return this;
  }
  public RegionMetricsBuilder setDataLocality(float value) {
    this.dataLocality = value;
    return this;
  }
  public RegionMetricsBuilder setLastMajorCompactionTimestamp(long value) {
    this.lastMajorCompactionTimestamp = value;
    return this;
  }
  public RegionMetricsBuilder setDataLocalityForSsd(float value) {
    this.dataLocalityForSsd = value;
    return this;
  }
  public RegionMetricsBuilder setBlocksLocalWeight(long value) {
    this.blocksLocalWeight = value;
    return this;
  }
  public RegionMetricsBuilder setBlocksLocalWithSsdWeight(long value) {
    this.blocksLocalWithSsdWeight = value;
    return this;
  }
  public RegionMetricsBuilder setBlocksTotalWeight(long value) {
    this.blocksTotalWeight = value;
    return this;
  }
  public RegionMetricsBuilder setCompactionState(CompactionState compactionState) {
    this.compactionState = compactionState;
    return this;
  }

  public RegionMetrics build() {
    return new RegionMetricsImpl(name,
        storeCount,
        storeFileCount,
        storeRefCount,
        maxCompactedStoreFileRefCount,
        compactingCellCount,
        compactedCellCount,
        storeFileSize,
        memStoreSize,
        indexSize,
        rootLevelIndexSize,
        uncompressedDataIndexSize,
        bloomFilterSize,
        uncompressedStoreFileSize,
        writeRequestCount,
        readRequestCount,
        filteredReadRequestCount,
        completedSequenceId,
        storeSequenceIds,
        dataLocality,
        lastMajorCompactionTimestamp,
        dataLocalityForSsd,
        blocksLocalWeight,
        blocksLocalWithSsdWeight,
        blocksTotalWeight,
        compactionState);
  }

  private static class RegionMetricsImpl implements RegionMetrics {
    private final byte[] name;
    private final int storeCount;
    private final int storeFileCount;
    private final int storeRefCount;
    private final int maxCompactedStoreFileRefCount;
    private final long compactingCellCount;
    private final long compactedCellCount;
    private final Size storeFileSize;
    private final Size memStoreSize;
    private final Size indexSize;
    private final Size rootLevelIndexSize;
    private final Size uncompressedDataIndexSize;
    private final Size bloomFilterSize;
    private final Size uncompressedStoreFileSize;
    private final long writeRequestCount;
    private final long readRequestCount;
    private final long filteredReadRequestCount;
    private final long completedSequenceId;
    private final Map<byte[], Long> storeSequenceIds;
    private final float dataLocality;
    private final long lastMajorCompactionTimestamp;
    private final float dataLocalityForSsd;
    private final long blocksLocalWeight;
    private final long blocksLocalWithSsdWeight;
    private final long blocksTotalWeight;
    private final CompactionState compactionState;
    RegionMetricsImpl(byte[] name,
        int storeCount,
        int storeFileCount,
        int storeRefCount,
        int maxCompactedStoreFileRefCount,
        final long compactingCellCount,
        long compactedCellCount,
        Size storeFileSize,
        Size memStoreSize,
        Size indexSize,
        Size rootLevelIndexSize,
        Size uncompressedDataIndexSize,
        Size bloomFilterSize,
        Size uncompressedStoreFileSize,
        long writeRequestCount,
        long readRequestCount,
        long filteredReadRequestCount,
        long completedSequenceId,
        Map<byte[], Long> storeSequenceIds,
        float dataLocality,
        long lastMajorCompactionTimestamp,
        float dataLocalityForSsd,
        long blocksLocalWeight,
        long blocksLocalWithSsdWeight,
        long blocksTotalWeight,
        CompactionState compactionState) {
      this.name = Preconditions.checkNotNull(name);
      this.storeCount = storeCount;
      this.storeFileCount = storeFileCount;
      this.storeRefCount = storeRefCount;
      this.maxCompactedStoreFileRefCount = maxCompactedStoreFileRefCount;
      this.compactingCellCount = compactingCellCount;
      this.compactedCellCount = compactedCellCount;
      this.storeFileSize = Preconditions.checkNotNull(storeFileSize);
      this.memStoreSize = Preconditions.checkNotNull(memStoreSize);
      this.indexSize = Preconditions.checkNotNull(indexSize);
      this.rootLevelIndexSize = Preconditions.checkNotNull(rootLevelIndexSize);
      this.uncompressedDataIndexSize = Preconditions.checkNotNull(uncompressedDataIndexSize);
      this.bloomFilterSize = Preconditions.checkNotNull(bloomFilterSize);
      this.uncompressedStoreFileSize = Preconditions.checkNotNull(uncompressedStoreFileSize);
      this.writeRequestCount = writeRequestCount;
      this.readRequestCount = readRequestCount;
      this.filteredReadRequestCount = filteredReadRequestCount;
      this.completedSequenceId = completedSequenceId;
      this.storeSequenceIds = Preconditions.checkNotNull(storeSequenceIds);
      this.dataLocality = dataLocality;
      this.lastMajorCompactionTimestamp = lastMajorCompactionTimestamp;
      this.dataLocalityForSsd = dataLocalityForSsd;
      this.blocksLocalWeight = blocksLocalWeight;
      this.blocksLocalWithSsdWeight = blocksLocalWithSsdWeight;
      this.blocksTotalWeight = blocksTotalWeight;
      this.compactionState = compactionState;
    }

    @Override
    public byte[] getRegionName() {
      return name;
    }

    @Override
    public int getStoreCount() {
      return storeCount;
    }

    @Override
    public int getStoreFileCount() {
      return storeFileCount;
    }

    @Override
    public int getStoreRefCount() {
      return storeRefCount;
    }

    @Override
    public int getMaxCompactedStoreFileRefCount() {
      return maxCompactedStoreFileRefCount;
    }

    @Override
    public Size getStoreFileSize() {
      return storeFileSize;
    }

    @Override
    public Size getMemStoreSize() {
      return memStoreSize;
    }

    @Override
    public long getReadRequestCount() {
      return readRequestCount;
    }

    @Override
    public long getFilteredReadRequestCount() {
      return filteredReadRequestCount;
    }

    @Override
    public long getWriteRequestCount() {
      return writeRequestCount;
    }

    @Override
    public Size getStoreFileIndexSize() {
      return indexSize;
    }

    @Override
    public Size getStoreFileRootLevelIndexSize() {
      return rootLevelIndexSize;
    }

    @Override
    public Size getStoreFileUncompressedDataIndexSize() {
      return uncompressedDataIndexSize;
    }

    @Override
    public Size getBloomFilterSize() {
      return bloomFilterSize;
    }

    @Override
    public long getCompactingCellCount() {
      return compactingCellCount;
    }

    @Override
    public long getCompactedCellCount() {
      return compactedCellCount;
    }

    @Override
    public long getCompletedSequenceId() {
      return completedSequenceId;
    }

    @Override
    public Map<byte[], Long> getStoreSequenceId() {
      return Collections.unmodifiableMap(storeSequenceIds);
    }

    @Override
    public Size getUncompressedStoreFileSize() {
      return uncompressedStoreFileSize;
    }

    @Override
    public float getDataLocality() {
      return dataLocality;
    }

    @Override
    public long getLastMajorCompactionTimestamp() {
      return lastMajorCompactionTimestamp;
    }

    @Override
    public float getDataLocalityForSsd() {
      return dataLocalityForSsd;
    }

    @Override
    public long getBlocksLocalWeight() {
      return blocksLocalWeight;
    }

    @Override
    public long getBlocksLocalWithSsdWeight() {
      return blocksLocalWithSsdWeight;
    }

    @Override
    public long getBlocksTotalWeight() {
      return blocksTotalWeight;
    }

    @Override
    public CompactionState getCompactionState() {
      return compactionState;
    }

    @Override
    public String toString() {
      StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "storeCount",
          this.getStoreCount());
      Strings.appendKeyValue(sb, "storeFileCount",
          this.getStoreFileCount());
      Strings.appendKeyValue(sb, "storeRefCount",
        this.getStoreRefCount());
      Strings.appendKeyValue(sb, "maxCompactedStoreFileRefCount",
        this.getMaxCompactedStoreFileRefCount());
      Strings.appendKeyValue(sb, "uncompressedStoreFileSize",
          this.getUncompressedStoreFileSize());
      Strings.appendKeyValue(sb, "lastMajorCompactionTimestamp",
          this.getLastMajorCompactionTimestamp());
      Strings.appendKeyValue(sb, "storeFileSize",
          this.getStoreFileSize());
      if (this.getUncompressedStoreFileSize().get() != 0) {
        Strings.appendKeyValue(sb, "compressionRatio",
            String.format("%.4f",
                (float) this.getStoreFileSize().get(Size.Unit.MEGABYTE) /
                (float) this.getUncompressedStoreFileSize().get(Size.Unit.MEGABYTE)));
      }
      Strings.appendKeyValue(sb, "memStoreSize",
          this.getMemStoreSize());
      Strings.appendKeyValue(sb, "readRequestCount",
          this.getReadRequestCount());
      Strings.appendKeyValue(sb, "writeRequestCount",
          this.getWriteRequestCount());
      Strings.appendKeyValue(sb, "rootLevelIndexSize",
          this.getStoreFileRootLevelIndexSize());
      Strings.appendKeyValue(sb, "uncompressedDataIndexSize",
          this.getStoreFileUncompressedDataIndexSize());
      Strings.appendKeyValue(sb, "bloomFilterSize",
          this.getBloomFilterSize());
      Strings.appendKeyValue(sb, "compactingCellCount",
          this.getCompactingCellCount());
      Strings.appendKeyValue(sb, "compactedCellCount",
          this.getCompactedCellCount());
      float compactionProgressPct = Float.NaN;
      if (this.getCompactingCellCount() > 0) {
        compactionProgressPct = ((float) this.getCompactedCellCount() /
            (float) this.getCompactingCellCount());
      }
      Strings.appendKeyValue(sb, "compactionProgressPct",
          compactionProgressPct);
      Strings.appendKeyValue(sb, "completedSequenceId",
          this.getCompletedSequenceId());
      Strings.appendKeyValue(sb, "dataLocality",
          this.getDataLocality());
      Strings.appendKeyValue(sb, "dataLocalityForSsd",
          this.getDataLocalityForSsd());
      Strings.appendKeyValue(sb, "blocksLocalWeight",
        blocksLocalWeight);
      Strings.appendKeyValue(sb, "blocksLocalWithSsdWeight",
        blocksLocalWithSsdWeight);
      Strings.appendKeyValue(sb, "blocksTotalWeight",
        blocksTotalWeight);
      Strings.appendKeyValue(sb, "compactionState",
        compactionState);
      return sb.toString();
    }
  }

}
