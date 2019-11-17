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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Objects;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

/**
 * This class is used for exporting current state of load on a RegionServer.
 *
 * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
 *             Use {@link ServerMetrics} instead.
 */
@InterfaceAudience.Public
@Deprecated
public class ServerLoad implements ServerMetrics {
  private final ServerMetrics metrics;
  private int stores = 0;
  private int storefiles = 0;
  private int storeUncompressedSizeMB = 0;
  private int storefileSizeMB = 0;
  private int memstoreSizeMB = 0;
  private long storefileIndexSizeKB = 0;
  private long readRequestsCount = 0;
  private long filteredReadRequestsCount = 0;
  private long writeRequestsCount = 0;
  private int rootIndexSizeKB = 0;
  private int totalStaticIndexSizeKB = 0;
  private int totalStaticBloomSizeKB = 0;
  private long totalCompactingKVs = 0;
  private long currentCompactedKVs = 0;

  /**
   * DONT USE this construction. It make a fake server name;
   */
  @InterfaceAudience.Private
  public ServerLoad(ClusterStatusProtos.ServerLoad serverLoad) {
    this(ServerName.valueOf("localhost,1,1"), serverLoad);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  @InterfaceAudience.Private
  public ServerLoad(ServerName name, ClusterStatusProtos.ServerLoad serverLoad) {
    this(ServerMetricsBuilder.toServerMetrics(name, serverLoad));
    this.serverLoad = serverLoad;
  }

  @InterfaceAudience.Private
  public ServerLoad(ServerMetrics metrics) {
    this.metrics = metrics;
    this.serverLoad = ServerMetricsBuilder.toServerLoad(metrics);
    for (RegionMetrics rl : metrics.getRegionMetrics().values()) {
      stores += rl.getStoreCount();
      storefiles += rl.getStoreFileCount();
      storeUncompressedSizeMB += rl.getUncompressedStoreFileSize().get(Size.Unit.MEGABYTE);
      storefileSizeMB += rl.getStoreFileSize().get(Size.Unit.MEGABYTE);
      memstoreSizeMB += rl.getMemStoreSize().get(Size.Unit.MEGABYTE);
      readRequestsCount += rl.getReadRequestCount();
      filteredReadRequestsCount += rl.getFilteredReadRequestCount();
      writeRequestsCount += rl.getWriteRequestCount();
      storefileIndexSizeKB += rl.getStoreFileIndexSize().get(Size.Unit.KILOBYTE);
      rootIndexSizeKB += rl.getStoreFileRootLevelIndexSize().get(Size.Unit.KILOBYTE);
      totalStaticIndexSizeKB += rl.getStoreFileUncompressedDataIndexSize().get(Size.Unit.KILOBYTE);
      totalStaticBloomSizeKB += rl.getBloomFilterSize().get(Size.Unit.KILOBYTE);
      totalCompactingKVs += rl.getCompactingCellCount();
      currentCompactedKVs += rl.getCompactedCellCount();
    }
  }

  /**
   * NOTE: Function name cannot start with "get" because then an OpenDataException is thrown because
   * HBaseProtos.ServerLoad cannot be converted to an open data type(see HBASE-5967).
   * @return the underlying ServerLoad protobuf object
   * @deprecated DONT use this pb object since the byte array backed may be modified in rpc layer
   */
  @InterfaceAudience.Private
  @Deprecated
  public ClusterStatusProtos.ServerLoad obtainServerLoadPB() {
    return serverLoad;
  }

  protected ClusterStatusProtos.ServerLoad serverLoad;

  /**
   * @return number of requests  since last report.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link #getRequestCountPerSecond} instead.
   */
  @Deprecated
  public long getNumberOfRequests() {
    return getRequestCountPerSecond();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             No flag in 2.0
   */
  @Deprecated
  public boolean hasNumberOfRequests() {
    return true;
  }

  /**
   * @return total Number of requests from the start of the region server.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link #getRequestCount} instead.
   */
  @Deprecated
  public long getTotalNumberOfRequests() {
    return getRequestCount();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             No flag in 2.0
   */
  @Deprecated
  public boolean hasTotalNumberOfRequests() {
    return true;
  }

  /**
   * @return the amount of used heap, in MB.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link #getUsedHeapSize} instead.
   */
  @Deprecated
  public int getUsedHeapMB() {
    return (int) getUsedHeapSize().get(Size.Unit.MEGABYTE);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             No flag in 2.0
   */
  @Deprecated
  public boolean hasUsedHeapMB() {
    return true;
  }

  /**
   * @return the maximum allowable size of the heap, in MB.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getMaxHeapSize} instead.
   */
  @Deprecated
  public int getMaxHeapMB() {
    return (int) getMaxHeapSize().get(Size.Unit.MEGABYTE);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             No flag in 2.0
   */
  @Deprecated
  public boolean hasMaxHeapMB() {
    return true;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getStores() {
    return stores;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getStorefiles() {
    return storefiles;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getStoreUncompressedSizeMB() {
    return storeUncompressedSizeMB;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getStorefileSizeInMB() {
    return storefileSizeMB;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getStorefileSizeMB() {
    return storefileSizeMB;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getMemstoreSizeInMB() {
    return memstoreSizeMB;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getMemStoreSizeMB() {
    return memstoreSizeMB;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getStorefileIndexSizeInMB() {
    // Return value divided by 1024
    return (int) (getStorefileIndexSizeKB() >> 10);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public long getStorefileIndexSizeKB() {
    return storefileIndexSizeKB;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public long getReadRequestsCount() {
    return readRequestsCount;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public long getFilteredReadRequestsCount() {
    return filteredReadRequestsCount;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public long getWriteRequestsCount() {
    return writeRequestsCount;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getRootIndexSizeKB() {
    return rootIndexSizeKB;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getTotalStaticIndexSizeKB() {
    return totalStaticIndexSizeKB;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getTotalStaticBloomSizeKB() {
    return totalStaticBloomSizeKB;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public long getTotalCompactingKVs() {
    return totalCompactingKVs;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *     Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public long getCurrentCompactedKVs() {
    return currentCompactedKVs;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public int getNumberOfRegions() {
    return metrics.getRegionMetrics().size();
  }

  @Override
  public ServerName getServerName() {
    return metrics.getServerName();
  }

  @Override
  public long getRequestCountPerSecond() {
    return metrics.getRequestCountPerSecond();
  }

  @Override
  public long getRequestCount() {
    return metrics.getRequestCount();
  }

  @Override
  public Size getUsedHeapSize() {
    return metrics.getUsedHeapSize();
  }

  @Override
  public Size getMaxHeapSize() {
    return metrics.getMaxHeapSize();
  }

  @Override
  public int getInfoServerPort() {
    return metrics.getInfoServerPort();
  }

  /**
   * Call directly from client such as hbase shell
   * @return the list of ReplicationLoadSource
   */
  @Override
  public List<ReplicationLoadSource> getReplicationLoadSourceList() {
    return metrics.getReplicationLoadSourceList();
  }

  /**
   * Call directly from client such as hbase shell
   * @return a map of ReplicationLoadSource list per peer id
   */
  @Override
  public Map<String, List<ReplicationLoadSource>> getReplicationLoadSourceMap() {
    return metrics.getReplicationLoadSourceMap();
  }

  /**
   * Call directly from client such as hbase shell
   * @return ReplicationLoadSink
   */
  @Override
  public ReplicationLoadSink getReplicationLoadSink() {
    return metrics.getReplicationLoadSink();
  }

  @Override
  public Map<byte[], RegionMetrics> getRegionMetrics() {
    return metrics.getRegionMetrics();
  }

  @Override public Map<byte[], UserMetrics> getUserMetrics() {
    return metrics.getUserMetrics();
  }

  @Override
  public Set<String> getCoprocessorNames() {
    return metrics.getCoprocessorNames();
  }

  @Override
  public long getReportTimestamp() {
    return metrics.getReportTimestamp();
  }

  @Override
  public long getLastReportTimestamp() {
    return metrics.getLastReportTimestamp();
  }

  /**
   * Originally, this method factored in the effect of requests going to the
   * server as well. However, this does not interact very well with the current
   * region rebalancing code, which only factors number of regions. For the
   * interim, until we can figure out how to make rebalancing use all the info
   * available, we're just going to make load purely the number of regions.
   *
   * @return load factor for this server.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getNumberOfRegions} instead.
   */
  @Deprecated
  public int getLoad() {
    // See above comment
    // int load = numberOfRequests == 0 ? 1 : numberOfRequests;
    // load *= numberOfRegions == 0 ? 1 : numberOfRegions;
    // return load;
    return getNumberOfRegions();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegionMetrics} instead.
   */
  @Deprecated
  public Map<byte[], RegionLoad> getRegionsLoad() {
    return getRegionMetrics().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new RegionLoad(e.getValue()),
          (v1, v2) -> {
            throw new RuntimeException("key collisions?");
          }, () -> new TreeMap<>(Bytes.BYTES_COMPARATOR)));
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getCoprocessorNames} instead.
   */
  @Deprecated
  public String[] getRegionServerCoprocessors() {
    return getCoprocessorNames().toArray(new String[getCoprocessorNames().size()]);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getCoprocessorNames} instead.
   */
  @Deprecated
  public String[] getRsCoprocessors() {
    return getRegionServerCoprocessors();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRequestCountPerSecond} instead.
   */
  @Deprecated
  public double getRequestsPerSecond() {
    return getRequestCountPerSecond();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "requestsPerSecond",
      Double.valueOf(getRequestsPerSecond()));
    Strings.appendKeyValue(sb, "numberOfOnlineRegions", Integer.valueOf(getNumberOfRegions()));
    Strings.appendKeyValue(sb, "usedHeapMB", Integer.valueOf(this.getUsedHeapMB()));
    Strings.appendKeyValue(sb, "maxHeapMB", Integer.valueOf(getMaxHeapMB()));
    Strings.appendKeyValue(sb, "numberOfStores", Integer.valueOf(this.stores));
    Strings.appendKeyValue(sb, "numberOfStorefiles", Integer.valueOf(this.storefiles));
    Strings.appendKeyValue(sb, "storefileUncompressedSizeMB",
        Integer.valueOf(this.storeUncompressedSizeMB));
    Strings.appendKeyValue(sb, "storefileSizeMB", Integer.valueOf(this.storefileSizeMB));
    if (this.storeUncompressedSizeMB != 0) {
      Strings.appendKeyValue(sb, "compressionRatio", String.format("%.4f",
          (float) this.storefileSizeMB / (float) this.storeUncompressedSizeMB));
    }
    Strings.appendKeyValue(sb, "memstoreSizeMB", Integer.valueOf(this.memstoreSizeMB));
    Strings.appendKeyValue(sb, "storefileIndexSizeKB",
        Long.valueOf(this.storefileIndexSizeKB));
    Strings.appendKeyValue(sb, "readRequestsCount", Long.valueOf(this.readRequestsCount));
    Strings.appendKeyValue(sb, "filteredReadRequestsCount",
        Long.valueOf(this.filteredReadRequestsCount));
    Strings.appendKeyValue(sb, "writeRequestsCount", Long.valueOf(this.writeRequestsCount));
    Strings.appendKeyValue(sb, "rootIndexSizeKB", Integer.valueOf(this.rootIndexSizeKB));
    Strings.appendKeyValue(sb, "totalStaticIndexSizeKB",
        Integer.valueOf(this.totalStaticIndexSizeKB));
    Strings.appendKeyValue(sb, "totalStaticBloomSizeKB",
        Integer.valueOf(this.totalStaticBloomSizeKB));
    Strings.appendKeyValue(sb, "totalCompactingKVs", Long.valueOf(this.totalCompactingKVs));
    Strings.appendKeyValue(sb, "currentCompactedKVs", Long.valueOf(this.currentCompactedKVs));
    float compactionProgressPct = Float.NaN;
    if (this.totalCompactingKVs > 0) {
      compactionProgressPct =
          Float.valueOf((float) this.currentCompactedKVs / this.totalCompactingKVs);
    }
    Strings.appendKeyValue(sb, "compactionProgressPct", compactionProgressPct);

    String[] coprocessorStrings = getRsCoprocessors();
    if (coprocessorStrings != null) {
      Strings.appendKeyValue(sb, "coprocessors", Arrays.toString(coprocessorStrings));
    }
    return sb.toString();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link ServerMetricsBuilder#of(ServerName)} instead.
   */
  @Deprecated
  public static final ServerLoad EMPTY_SERVERLOAD =
      new ServerLoad(ServerName.valueOf("localhost,1,1"),
          ClusterStatusProtos.ServerLoad.newBuilder().build());

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getReportTimestamp} instead.
   */
  @Deprecated
  public long getReportTime() {
    return getReportTimestamp();
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(stores, storefiles, storeUncompressedSizeMB, storefileSizeMB, memstoreSizeMB,
            storefileIndexSizeKB, readRequestsCount, filteredReadRequestsCount, writeRequestsCount,
            rootIndexSizeKB, totalStaticIndexSizeKB, totalStaticBloomSizeKB, totalCompactingKVs,
            currentCompactedKVs);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (other instanceof ServerLoad) {
      ServerLoad sl = ((ServerLoad) other);
      return stores == sl.stores && storefiles == sl.storefiles
          && storeUncompressedSizeMB == sl.storeUncompressedSizeMB
          && storefileSizeMB == sl.storefileSizeMB && memstoreSizeMB == sl.memstoreSizeMB
          && storefileIndexSizeKB == sl.storefileIndexSizeKB
          && readRequestsCount == sl.readRequestsCount
          && filteredReadRequestsCount == sl.filteredReadRequestsCount
          && writeRequestsCount == sl.writeRequestsCount && rootIndexSizeKB == sl.rootIndexSizeKB
          && totalStaticIndexSizeKB == sl.totalStaticIndexSizeKB
          && totalStaticBloomSizeKB == sl.totalStaticBloomSizeKB
          && totalCompactingKVs == sl.totalCompactingKVs
          && currentCompactedKVs == sl.currentCompactedKVs;
    }
    return false;
  }
}
