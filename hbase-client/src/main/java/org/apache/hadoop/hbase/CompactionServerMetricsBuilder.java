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
import java.util.List;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;


@InterfaceAudience.Private
public final class CompactionServerMetricsBuilder {

  /**
   * @param sn the server name
   * @return a empty metrics
   */
  public static CompactionServerMetrics of(ServerName sn) {
    return newBuilder(sn).build();
  }

  public static CompactionServerMetrics of(ServerName sn, int versionNumber, String version) {
    return newBuilder(sn).setVersionNumber(versionNumber).setVersion(version).build();
  }

  public static CompactionServerMetrics toCompactionServerMetrics(ServerName serverName,
      ClusterStatusProtos.CompactionServerLoad serverLoadPB) {
    return toCompactionServerMetrics(serverName, 0, "0.0.0", serverLoadPB);
  }

  public static CompactionServerMetrics toCompactionServerMetrics(ServerName serverName,
    int versionNumber, String version, ClusterStatusProtos.CompactionServerLoad serverLoadPB) {
    return CompactionServerMetricsBuilder.newBuilder(serverName)
      .setInfoServerPort(serverLoadPB.getInfoServerPort())
      .setCompactedCellCount(serverLoadPB.getCompactedCells())
      .setCompactingCellCount(serverLoadPB.getCompactingCells())
      .addCompactionTasks(serverLoadPB.getCompactionTasksList())
      .setTotalNumberOfRequests(serverLoadPB.getTotalNumberOfRequests())
      .setLastReportTimestamp(serverLoadPB.getReportStartTime()).setVersionNumber(versionNumber)
      .setVersion(version).build();
  }

  public static CompactionServerMetricsBuilder newBuilder(ServerName sn) {
    return new CompactionServerMetricsBuilder(sn);
  }

  private final ServerName serverName;
  private int versionNumber;
  private String version = "0.0.0";
  private int infoServerPort;
  private long compactingCellCount;
  private long compactedCellCount;
  private long totalNumberOfRequests;
  private final List<String> compactionTasks = new ArrayList<>();
  private long reportTimestamp = System.currentTimeMillis();
  private long lastReportTimestamp = 0;
  private CompactionServerMetricsBuilder(ServerName serverName) {
    this.serverName = serverName;
  }

  public CompactionServerMetricsBuilder setVersionNumber(int versionNumber) {
    this.versionNumber = versionNumber;
    return this;
  }

  public CompactionServerMetricsBuilder setVersion(String version) {
    this.version = version;
    return this;
  }


  public CompactionServerMetricsBuilder addCompactionTasks(List<String> compactionTasks) {
    this.compactionTasks.addAll(compactionTasks);
    return this;
  }

  public CompactionServerMetricsBuilder setTotalNumberOfRequests(long totalNumberOfRequests) {
    this.totalNumberOfRequests = totalNumberOfRequests;
    return this;
  }

  public CompactionServerMetricsBuilder setInfoServerPort(int value) {
    this.infoServerPort = value;
    return this;
  }


  public CompactionServerMetricsBuilder setCompactingCellCount(long value) {
    this.compactingCellCount = value;
    return this;
  }

  public CompactionServerMetricsBuilder setCompactedCellCount(long value) {
    this.compactedCellCount = value;
    return this;
  }

  public CompactionServerMetricsBuilder setReportTimestamp(long value) {
    this.reportTimestamp = value;
    return this;
  }

  public CompactionServerMetricsBuilder setLastReportTimestamp(long value) {
    this.lastReportTimestamp = value;
    return this;
  }

  public CompactionServerMetrics build() {
    return new CompactionServerMetricsImpl(
      serverName,
      versionNumber,
      version,
      infoServerPort,
      compactingCellCount,
      compactedCellCount,
      compactionTasks,
      totalNumberOfRequests,
      reportTimestamp,
      lastReportTimestamp);
  }

  private static class CompactionServerMetricsImpl implements CompactionServerMetrics {
    private final ServerName serverName;
    private final int versionNumber;
    private final String version;
    private final int infoServerPort;
    private final long compactingCellCount;
    private final long compactedCellCount;
    private final List<String> compactionTasks = new ArrayList<>();
    private final long totalNumberOfRequests;
    private final long reportTimestamp;
    private final long lastReportTimestamp;

    CompactionServerMetricsImpl(ServerName serverName, int versionNumber, String version,
      int infoServerPort, long compactingCellCount, long compactedCellCount,
      List<String> compactionTasks, long totalNumberOfRequests, long reportTimestamp,
      long lastReportTimestamp) {
      this.serverName = Preconditions.checkNotNull(serverName);
      this.versionNumber = versionNumber;
      this.version = version;
      this.infoServerPort = infoServerPort;
      this.compactingCellCount = compactingCellCount;
      this.compactedCellCount = compactedCellCount;
      this.totalNumberOfRequests = totalNumberOfRequests;
      this.reportTimestamp = reportTimestamp;
      this.lastReportTimestamp = lastReportTimestamp;
      this.compactionTasks.addAll(compactionTasks);
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

    public long getCompactingCellCount() {
      return compactingCellCount;
    }

    public long getCompactedCellCount() {
      return compactedCellCount;
    }

    public List<String> getCompactionTasks() {
      return compactionTasks;
    }

    public long getTotalNumberOfRequests() {
      return totalNumberOfRequests;
    }

    @Override
    public int getInfoServerPort() {
      return infoServerPort;
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
    public String toString() {
      StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "totalCompactingKVs",
        Double.valueOf(compactingCellCount));
      Strings.appendKeyValue(sb, "currentCompactedKVs", compactedCellCount);
      float compactionProgressPct = Float.NaN;
      if (compactingCellCount > 0) {
        compactionProgressPct = Float.valueOf((float) compactedCellCount / compactingCellCount);
      }
      Strings.appendKeyValue(sb, "compactionProgressPct", compactionProgressPct);
      Strings.appendKeyValue(sb, "compactionTaskNum", compactionTasks.size());
      Strings.appendKeyValue(sb, "totalNumberOfRequests", totalNumberOfRequests);
      return sb.toString();
    }
  }
}
