/**
 * Copyright The Apache Software Foundation
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase;

import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@InterfaceAudience.Private
public final class ServerMetricsBuilder {

  /**
   * @param sn the server name
   * @return a empty metrics
   */
  public static ServerMetrics of(ServerName sn) {
    return newBuilder(sn).build();
  }

  public static ServerMetrics of(ServerName sn, int versionNumber, String version) {
    return newBuilder(sn).setVersionNumber(versionNumber).setVersion(version).build();
  }

  public static ServerMetrics toServerMetrics(ClusterStatusProtos.LiveServerInfo serverInfo) {
    return toServerMetrics(ProtobufUtil.toServerName(serverInfo.getServer()), 0, "0.0.0",
      serverInfo.getServerLoad());
  }

  public static ServerMetrics toServerMetrics(ServerName serverName,
      ClusterStatusProtos.ServerLoad serverLoadPB) {
    return toServerMetrics(serverName, 0, "0.0.0", serverLoadPB);
  }

  public static ServerMetrics toServerMetrics(ServerName serverName, int versionNumber,
      String version, ClusterStatusProtos.ServerLoad serverLoadPB) {
    return ServerMetricsBuilder.newBuilder(serverName)
      .setRequestCountPerSecond(serverLoadPB.getNumberOfRequests())
      .setRequestCount(serverLoadPB.getTotalNumberOfRequests())
      .setInfoServerPort(serverLoadPB.getInfoServerPort())
      .setReadRequestCount(serverLoadPB.getReadRequestsCount())
      .setWriteRequestCount(serverLoadPB.getWriteRequestsCount())
      .setMaxHeapSize(new Size(serverLoadPB.getMaxHeapMB(), Size.Unit.MEGABYTE))
      .setUsedHeapSize(new Size(serverLoadPB.getUsedHeapMB(), Size.Unit.MEGABYTE))
      .setCoprocessorNames(serverLoadPB.getCoprocessorsList().stream()
        .map(HBaseProtos.Coprocessor::getName).collect(Collectors.toList()))
      .setRegionMetrics(serverLoadPB.getRegionLoadsList().stream()
        .map(RegionMetricsBuilder::toRegionMetrics).collect(Collectors.toList()))
        .setUserMetrics(serverLoadPB.getUserLoadsList().stream()
            .map(UserMetricsBuilder::toUserMetrics).collect(Collectors.toList()))
      .setReplicationLoadSources(serverLoadPB.getReplLoadSourceList().stream()
          .map(ProtobufUtil::toReplicationLoadSource).collect(Collectors.toList()))
      .setReplicationLoadSink(serverLoadPB.hasReplLoadSink()
        ? ProtobufUtil.toReplicationLoadSink(serverLoadPB.getReplLoadSink())
        : null)
      .setTasks(serverLoadPB.getTasksList().stream()
          .map(ProtobufUtil::getServerTask).collect(Collectors.toList()))
      .setReportTimestamp(serverLoadPB.getReportEndTime())
      .setLastReportTimestamp(serverLoadPB.getReportStartTime()).setVersionNumber(versionNumber)
      .setVersion(version).build();
  }

  public static List<HBaseProtos.Coprocessor> toCoprocessor(Collection<String> names) {
    return names.stream()
        .map(n -> HBaseProtos.Coprocessor.newBuilder().setName(n).build())
        .collect(Collectors.toList());
  }

  public static ClusterStatusProtos.ServerLoad toServerLoad(ServerMetrics metrics) {
    ClusterStatusProtos.ServerLoad.Builder builder = ClusterStatusProtos.ServerLoad.newBuilder()
        .setNumberOfRequests(metrics.getRequestCountPerSecond())
        .setTotalNumberOfRequests(metrics.getRequestCount())
        .setInfoServerPort(metrics.getInfoServerPort())
        .setMaxHeapMB((int) metrics.getMaxHeapSize().get(Size.Unit.MEGABYTE))
        .setUsedHeapMB((int) metrics.getUsedHeapSize().get(Size.Unit.MEGABYTE))
        .addAllCoprocessors(toCoprocessor(metrics.getCoprocessorNames()))
        .addAllRegionLoads(
            metrics.getRegionMetrics().values().stream().map(RegionMetricsBuilder::toRegionLoad)
                .collect(Collectors.toList()))
        .addAllUserLoads(
            metrics.getUserMetrics().values().stream().map(UserMetricsBuilder::toUserMetrics)
                .collect(Collectors.toList()))
        .addAllReplLoadSource(
            metrics.getReplicationLoadSourceList().stream()
                .map(ProtobufUtil::toReplicationLoadSource).collect(Collectors.toList()))
        .addAllTasks(
            metrics.getTasks().stream().map(ProtobufUtil::toServerTask)
                .collect(Collectors.toList()))
        .setReportStartTime(metrics.getLastReportTimestamp())
        .setReportEndTime(metrics.getReportTimestamp());
    if (metrics.getReplicationLoadSink() != null) {
      builder.setReplLoadSink(ProtobufUtil.toReplicationLoadSink(metrics.getReplicationLoadSink()));
    }
    return builder.build();
  }

  public static ServerMetricsBuilder newBuilder(ServerName sn) {
    return new ServerMetricsBuilder(sn);
  }

  private final ServerName serverName;
  private int versionNumber;
  private String version = "0.0.0";
  private long requestCountPerSecond;
  private long requestCount;
  private long readRequestCount;
  private long writeRequestCount;
  private Size usedHeapSize = Size.ZERO;
  private Size maxHeapSize = Size.ZERO;
  private int infoServerPort;
  private List<ReplicationLoadSource> sources = Collections.emptyList();
  @Nullable
  private ReplicationLoadSink sink = null;
  private final Map<byte[], RegionMetrics> regionStatus = new TreeMap<>(Bytes.BYTES_COMPARATOR);
  private final Map<byte[], UserMetrics> userMetrics = new TreeMap<>(Bytes.BYTES_COMPARATOR);
  private final Set<String> coprocessorNames = new TreeSet<>();
  private long reportTimestamp = EnvironmentEdgeManager.currentTime();
  private long lastReportTimestamp = 0;
  private final List<ServerTask> tasks = new ArrayList<>();

  private ServerMetricsBuilder(ServerName serverName) {
    this.serverName = serverName;
  }

  public ServerMetricsBuilder setVersionNumber(int versionNumber) {
    this.versionNumber = versionNumber;
    return this;
  }

  public ServerMetricsBuilder setVersion(String version) {
    this.version = version;
    return this;
  }

  public ServerMetricsBuilder setRequestCountPerSecond(long value) {
    this.requestCountPerSecond = value;
    return this;
  }

  public ServerMetricsBuilder setRequestCount(long value) {
    this.requestCount = value;
    return this;
  }

  public ServerMetricsBuilder setReadRequestCount(long value) {
    this.readRequestCount = value;
    return this;
  }

  public ServerMetricsBuilder setWriteRequestCount(long value) {
    this.writeRequestCount = value;
    return this;
  }


  public ServerMetricsBuilder setUsedHeapSize(Size value) {
    this.usedHeapSize = value;
    return this;
  }

  public ServerMetricsBuilder setMaxHeapSize(Size value) {
    this.maxHeapSize = value;
    return this;
  }

  public ServerMetricsBuilder setInfoServerPort(int value) {
    this.infoServerPort = value;
    return this;
  }

  public ServerMetricsBuilder setReplicationLoadSources(List<ReplicationLoadSource> value) {
    this.sources = value;
    return this;
  }

  public ServerMetricsBuilder setReplicationLoadSink(ReplicationLoadSink value) {
    this.sink = value;
    return this;
  }

  public ServerMetricsBuilder setRegionMetrics(List<RegionMetrics> value) {
    value.forEach(v -> this.regionStatus.put(v.getRegionName(), v));
    return this;
  }

  public ServerMetricsBuilder setUserMetrics(List<UserMetrics> value) {
    value.forEach(v -> this.userMetrics.put(v.getUserName(), v));
    return this;
  }

  public ServerMetricsBuilder setCoprocessorNames(List<String> value) {
    coprocessorNames.addAll(value);
    return this;
  }

  public ServerMetricsBuilder setReportTimestamp(long value) {
    this.reportTimestamp = value;
    return this;
  }

  public ServerMetricsBuilder setLastReportTimestamp(long value) {
    this.lastReportTimestamp = value;
    return this;
  }

  public ServerMetricsBuilder setTasks(List<ServerTask> tasks) {
    this.tasks.addAll(tasks);
    return this;
  }

  public ServerMetrics build() {
    return new ServerMetricsImpl(
        serverName,
        versionNumber,
        version,
        requestCountPerSecond,
        requestCount,
        readRequestCount,
        writeRequestCount,
        usedHeapSize,
        maxHeapSize,
        infoServerPort,
        sources,
        sink,
        regionStatus,
        coprocessorNames,
        reportTimestamp,
        lastReportTimestamp,
        userMetrics,
        tasks);
  }

  private static class ServerMetricsImpl implements ServerMetrics {
    private final ServerName serverName;
    private final int versionNumber;
    private final String version;
    private final long requestCountPerSecond;
    private final long requestCount;
    private final long readRequestsCount;
    private final long writeRequestsCount;
    private final Size usedHeapSize;
    private final Size maxHeapSize;
    private final int infoServerPort;
    private final List<ReplicationLoadSource> sources;
    @Nullable
    private final ReplicationLoadSink sink;
    private final Map<byte[], RegionMetrics> regionStatus;
    private final Set<String> coprocessorNames;
    private final long reportTimestamp;
    private final long lastReportTimestamp;
    private final Map<byte[], UserMetrics> userMetrics;
    private final List<ServerTask> tasks;

    ServerMetricsImpl(ServerName serverName, int versionNumber, String version,
        long requestCountPerSecond, long requestCount, long readRequestsCount,
        long writeRequestsCount, Size usedHeapSize, Size maxHeapSize,
        int infoServerPort, List<ReplicationLoadSource> sources, ReplicationLoadSink sink,
        Map<byte[], RegionMetrics> regionStatus, Set<String> coprocessorNames,
        long reportTimestamp, long lastReportTimestamp, Map<byte[], UserMetrics> userMetrics,
        List<ServerTask> tasks) {
      this.serverName = Preconditions.checkNotNull(serverName);
      this.versionNumber = versionNumber;
      this.version = version;
      this.requestCountPerSecond = requestCountPerSecond;
      this.requestCount = requestCount;
      this.readRequestsCount = readRequestsCount;
      this.writeRequestsCount = writeRequestsCount;
      this.usedHeapSize = Preconditions.checkNotNull(usedHeapSize);
      this.maxHeapSize = Preconditions.checkNotNull(maxHeapSize);
      this.infoServerPort = infoServerPort;
      this.sources = Preconditions.checkNotNull(sources);
      this.sink = sink;
      this.regionStatus = Preconditions.checkNotNull(regionStatus);
      this.userMetrics = Preconditions.checkNotNull(userMetrics);
      this.coprocessorNames =Preconditions.checkNotNull(coprocessorNames);
      this.reportTimestamp = reportTimestamp;
      this.lastReportTimestamp = lastReportTimestamp;
      this.tasks = tasks;
    }

    @Override
    public ServerName getServerName() {
      return serverName;
    }

    @Override
    public int getVersionNumber() {
      return versionNumber;
    }

    public String getVersion() {
      return version;
    }

    @Override
    public long getRequestCountPerSecond() {
      return requestCountPerSecond;
    }

    @Override
    public long getRequestCount() {
      return requestCount;
    }

    @Override
    public long getReadRequestsCount() {
      return readRequestsCount;
    }

    @Override
    public long getWriteRequestsCount() {
      return writeRequestsCount;
    }

    @Override
    public Size getUsedHeapSize() {
      return usedHeapSize;
    }

    @Override
    public Size getMaxHeapSize() {
      return maxHeapSize;
    }

    @Override
    public int getInfoServerPort() {
      return infoServerPort;
    }

    @Override
    public List<ReplicationLoadSource> getReplicationLoadSourceList() {
      return Collections.unmodifiableList(sources);
    }

    @Override
    public Map<String, List<ReplicationLoadSource>> getReplicationLoadSourceMap(){
      Map<String,List<ReplicationLoadSource>> sourcesMap = new HashMap<>();
      for(ReplicationLoadSource loadSource : sources){
        sourcesMap.computeIfAbsent(loadSource.getPeerID(),
          peerId -> new ArrayList<>()).add(loadSource);
      }
      return sourcesMap;
    }

    @Override
    public ReplicationLoadSink getReplicationLoadSink() {
      return sink;
    }

    @Override
    public Map<byte[], RegionMetrics> getRegionMetrics() {
      return Collections.unmodifiableMap(regionStatus);
    }

    @Override
    public Map<byte[], UserMetrics> getUserMetrics() {
      return Collections.unmodifiableMap(userMetrics);
    }

    @Override
    public Set<String> getCoprocessorNames() {
      return Collections.unmodifiableSet(coprocessorNames);
    }

    @Override
    public long getReportTimestamp() {
      return reportTimestamp;
    }

    @Override
    public long getLastReportTimestamp() {
      return lastReportTimestamp;
    }

    @Override
    public List<ServerTask> getTasks() {
      return tasks;
    }

    @Override
    public String toString() {
      int storeCount = 0;
      int storeFileCount = 0;
      int storeRefCount = 0;
      int maxCompactedStoreFileRefCount = 0;
      long uncompressedStoreFileSizeMB = 0;
      long storeFileSizeMB = 0;
      long memStoreSizeMB = 0;
      long storefileIndexSizeKB = 0;
      long rootLevelIndexSizeKB = 0;
      long readRequestsCount = 0;
      long cpRequestsCount = 0;
      long writeRequestsCount = 0;
      long filteredReadRequestsCount = 0;
      long bloomFilterSizeMB = 0;
      long compactingCellCount = 0;
      long compactedCellCount = 0;
      for (RegionMetrics r : getRegionMetrics().values()) {
        storeCount += r.getStoreCount();
        storeFileCount += r.getStoreFileCount();
        storeRefCount += r.getStoreRefCount();
        int currentMaxCompactedStoreFileRefCount = r.getMaxCompactedStoreFileRefCount();
        maxCompactedStoreFileRefCount = Math.max(maxCompactedStoreFileRefCount,
          currentMaxCompactedStoreFileRefCount);
        uncompressedStoreFileSizeMB += r.getUncompressedStoreFileSize().get(Size.Unit.MEGABYTE);
        storeFileSizeMB += r.getStoreFileSize().get(Size.Unit.MEGABYTE);
        memStoreSizeMB += r.getMemStoreSize().get(Size.Unit.MEGABYTE);
        storefileIndexSizeKB += r.getStoreFileUncompressedDataIndexSize().get(Size.Unit.KILOBYTE);
        readRequestsCount += r.getReadRequestCount();
        cpRequestsCount += r.getCpRequestCount();
        writeRequestsCount += r.getWriteRequestCount();
        filteredReadRequestsCount += r.getFilteredReadRequestCount();
        rootLevelIndexSizeKB += r.getStoreFileRootLevelIndexSize().get(Size.Unit.KILOBYTE);
        bloomFilterSizeMB += r.getBloomFilterSize().get(Size.Unit.MEGABYTE);
        compactedCellCount += r.getCompactedCellCount();
        compactingCellCount += r.getCompactingCellCount();
      }
      StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "requestsPerSecond",
            Double.valueOf(getRequestCountPerSecond()));
      Strings.appendKeyValue(sb, "numberOfOnlineRegions",
          Integer.valueOf(getRegionMetrics().size()));
      Strings.appendKeyValue(sb, "usedHeapMB", getUsedHeapSize());
      Strings.appendKeyValue(sb, "maxHeapMB", getMaxHeapSize());
      Strings.appendKeyValue(sb, "numberOfStores", storeCount);
      Strings.appendKeyValue(sb, "numberOfStorefiles", storeFileCount);
      Strings.appendKeyValue(sb, "storeRefCount", storeRefCount);
      Strings.appendKeyValue(sb, "maxCompactedStoreFileRefCount",
        maxCompactedStoreFileRefCount);
      Strings.appendKeyValue(sb, "storefileUncompressedSizeMB", uncompressedStoreFileSizeMB);
      Strings.appendKeyValue(sb, "storefileSizeMB", storeFileSizeMB);
      if (uncompressedStoreFileSizeMB != 0) {
        Strings.appendKeyValue(sb, "compressionRatio", String.format("%.4f",
            (float) storeFileSizeMB / (float) uncompressedStoreFileSizeMB));
      }
      Strings.appendKeyValue(sb, "memstoreSizeMB", memStoreSizeMB);
      Strings.appendKeyValue(sb, "readRequestsCount", readRequestsCount);
      Strings.appendKeyValue(sb, "cpRequestsCount", cpRequestsCount);
      Strings.appendKeyValue(sb, "filteredReadRequestsCount", filteredReadRequestsCount);
      Strings.appendKeyValue(sb, "writeRequestsCount", writeRequestsCount);
      Strings.appendKeyValue(sb, "rootIndexSizeKB", rootLevelIndexSizeKB);
      Strings.appendKeyValue(sb, "totalStaticIndexSizeKB", storefileIndexSizeKB);
      Strings.appendKeyValue(sb, "totalStaticBloomSizeKB", bloomFilterSizeMB);
      Strings.appendKeyValue(sb, "totalCompactingKVs", compactingCellCount);
      Strings.appendKeyValue(sb, "currentCompactedKVs", compactedCellCount);
      float compactionProgressPct = Float.NaN;
      if (compactingCellCount > 0) {
        compactionProgressPct =
            Float.valueOf((float) compactedCellCount / compactingCellCount);
      }
      Strings.appendKeyValue(sb, "compactionProgressPct", compactionProgressPct);
      Strings.appendKeyValue(sb, "coprocessors", getCoprocessorNames());
      return sb.toString();
    }
  }
}
