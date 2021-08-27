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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.ReplicationServerLoad;

@InterfaceAudience.Private
public final class ReplicationServerMetricsBuilder {

  public static ReplicationServerMetrics toServerMetrics(ServerName serverName, int versionNumber,
      String version, ReplicationServerLoad serverLoadPB) {
    return ReplicationServerMetricsBuilder.newBuilder(serverName)
      .setRequestCountPerSecond(serverLoadPB.getNumberOfRequests())
      .setRequestCount(serverLoadPB.getTotalNumberOfRequests())
      .setInfoServerPort(serverLoadPB.getInfoServerPort())
      .setReplicationLoadSources(serverLoadPB.getReplLoadSourceList().stream()
          .map(ProtobufUtil::toReplicationLoadSource).collect(Collectors.toList()))
      .setReplicationLoadSink(ProtobufUtil.toReplicationLoadSink(serverLoadPB.getReplLoadSink()))
      .setReportTimestamp(serverLoadPB.getReportEndTime())
      .setLastReportTimestamp(serverLoadPB.getReportStartTime()).setVersionNumber(versionNumber)
      .setVersion(version)
      .setQueueNodes(serverLoadPB.getQueueNodeList())
      .build();
  }

  public static ReplicationServerLoad toServerLoad(ReplicationServerMetrics metrics) {
    ReplicationServerLoad.Builder builder = ReplicationServerLoad.newBuilder()
      .setNumberOfRequests(metrics.getRequestCountPerSecond())
      .setTotalNumberOfRequests(metrics.getRequestCount())
      .setInfoServerPort(metrics.getInfoServerPort())
      .addAllReplLoadSource(metrics.getReplicationLoadSourceList().stream()
        .map(ProtobufUtil::toReplicationLoadSource)
        .collect(Collectors.toList()))
      .setReportStartTime(metrics.getLastReportTimestamp())
      .setReportEndTime(metrics.getReportTimestamp())
      .addAllQueueNode(metrics.getQueueNodes())
      .setReplLoadSink(ProtobufUtil.toReplicationLoadSink(metrics.getReplicationLoadSink()));
    return builder.build();
  }

  public static ReplicationServerMetricsBuilder newBuilder(ServerName sn) {
    return new ReplicationServerMetricsBuilder(sn);
  }

  private final ServerName serverName;
  private int versionNumber;
  private String version = "0.0.0";
  private long requestCountPerSecond;
  private long requestCount;
  private int infoServerPort;
  private List<ReplicationLoadSource> sources = Collections.emptyList();
  private ReplicationLoadSink sink;
  private long reportTimestamp = System.currentTimeMillis();
  private long lastReportTimestamp = 0;
  private List<String> queueNodes;
  private ReplicationServerMetricsBuilder(ServerName serverName) {
    this.serverName = serverName;
  }

  public ReplicationServerMetricsBuilder setVersionNumber(int versionNumber) {
    this.versionNumber = versionNumber;
    return this;
  }

  public ReplicationServerMetricsBuilder setVersion(String version) {
    this.version = version;
    return this;
  }

  public ReplicationServerMetricsBuilder setRequestCountPerSecond(long value) {
    this.requestCountPerSecond = value;
    return this;
  }

  public ReplicationServerMetricsBuilder setRequestCount(long value) {
    this.requestCount = value;
    return this;
  }

  public ReplicationServerMetricsBuilder setInfoServerPort(int value) {
    this.infoServerPort = value;
    return this;
  }

  public ReplicationServerMetricsBuilder setReplicationLoadSources(
    List<ReplicationLoadSource> value) {
    this.sources = value;
    return this;
  }

  public ReplicationServerMetricsBuilder setReplicationLoadSink(ReplicationLoadSink value) {
    this.sink = value;
    return this;
  }

  public ReplicationServerMetricsBuilder setReportTimestamp(long value) {
    this.reportTimestamp = value;
    return this;
  }

  public ReplicationServerMetricsBuilder setLastReportTimestamp(long value) {
    this.lastReportTimestamp = value;
    return this;
  }

  public ReplicationServerMetricsBuilder setQueueNodes(List<String> queueNodes) {
    this.queueNodes = queueNodes;
    return this;
  }

  public ReplicationServerMetrics build() {
    return new ReplicationServerMetricsImpl(
      serverName,
      versionNumber,
      version,
      requestCountPerSecond,
      requestCount,
      infoServerPort,
      sources,
      sink,
      reportTimestamp,
      lastReportTimestamp,
      queueNodes
    );
  }

  private static class ReplicationServerMetricsImpl implements ReplicationServerMetrics {
    private final ServerName serverName;
    private final int versionNumber;
    private final String version;
    private final long requestCountPerSecond;
    private final long requestCount;
    private final int infoServerPort;
    private final List<ReplicationLoadSource> sources;
    private final ReplicationLoadSink sink;
    private final long reportTimestamp;
    private final long lastReportTimestamp;
    private final List<String> queueNodes;

    ReplicationServerMetricsImpl(ServerName serverName, int versionNumber, String version,
      long requestCountPerSecond, long requestCount, int infoServerPort,
      List<ReplicationLoadSource> sources, ReplicationLoadSink sink, long reportTimestamp,
      long lastReportTimestamp, List<String> queueNodes) {
      this.serverName = Preconditions.checkNotNull(serverName);
      this.versionNumber = versionNumber;
      this.version = version;
      this.requestCountPerSecond = requestCountPerSecond;
      this.requestCount = requestCount;
      this.infoServerPort = infoServerPort;
      this.sources = Preconditions.checkNotNull(sources);
      this.sink = sink;
      this.reportTimestamp = reportTimestamp;
      this.lastReportTimestamp = lastReportTimestamp;
      this.queueNodes = queueNodes;
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
          peerId -> new ArrayList()).add(loadSource);
      }
      return sourcesMap;
    }

    @Override
    public ReplicationLoadSink getReplicationLoadSink() {
      return sink;
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
    public List<String> getQueueNodes() {
      return queueNodes;
    }

    @Override
    public String toString() {
      StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "numberOfQueues",
        queueNodes.size());
      return sb.toString();
    }
  }
}
