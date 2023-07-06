/*
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

import com.codahale.metrics.Snapshot;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A statistical sample of {@link MetricsConnection}.
 */
@InterfaceAudience.Public
public final class MetricsConnectionSnapshot {
  private final long connectionCount;
  private final long metaCacheHits;
  private final long metaCacheMisses;
  private final CallTrackerSnapshot getTrackerSnap;
  private final CallTrackerSnapshot scanTrackerSnap;
  private final CallTrackerSnapshot appendTrackerSnap;
  private final CallTrackerSnapshot deleteTrackerSnap;
  private final CallTrackerSnapshot incrementTrackerSnap;
  private final CallTrackerSnapshot putTrackerSnap;
  private final CallTrackerSnapshot multiTrackerSnap;
  private final RunnerStatsSnapshot runnerStatsSnap;
  private final long metaCacheNumClearServer;
  private final long metaCacheNumClearRegion;
  private final long hedgedReadOps;
  private final long hedgedReadWin;
  private final Snapshot concurrentCallsPerServerHistSnap;
  private final Snapshot numActionsPerServerHistSnap;
  private final long nsLookups;
  private final long nsLookupsFailed;
  private final Snapshot overloadedBackoffTimerSnap;
  private final double executorPoolUsageRatio;
  private final double metaPoolUsageRatio;
  private final Map<String, Snapshot> rpcTimersSnap;
  private final Map<String, Snapshot> rpcHistSnap;
  private final Map<String, Long> cacheDroppingExceptions;
  private final Map<String, Long> rpcCounters;
  private final Map<ServerName, Map<byte[], RegionStatsSnapshot>> serverStats;

  public static class CallTrackerSnapshot {
    private final String name;
    private final Snapshot callTimerSnap;
    private final Snapshot reqHistSnap;
    private final Snapshot respHistSnap;

    public CallTrackerSnapshot(String name, Snapshot callTimerSnap, Snapshot reqHistSnap,
      Snapshot respHistSnap) {
      this.name = name;
      this.callTimerSnap = callTimerSnap;
      this.reqHistSnap = reqHistSnap;
      this.respHistSnap = respHistSnap;
    }

    public String getName() {
      return name;
    }

    public Snapshot getCallTimerSnap() {
      return callTimerSnap;
    }

    public Snapshot getReqHistSnap() {
      return reqHistSnap;
    }

    public Snapshot getRespHistSnap() {
      return respHistSnap;
    }

    static CallTrackerSnapshot snapshot(MetricsConnection.CallTracker callTracker) {
      return new CallTrackerSnapshot(callTracker.getName(), callTracker.callTimer.getSnapshot(),
        callTracker.reqHist.getSnapshot(), callTracker.respHist.getSnapshot());
    }
  }

  public static class RunnerStatsSnapshot {
    private final long normalRunners;
    private final long delayRunners;
    private final Snapshot delayIntervalHistSnap;

    public RunnerStatsSnapshot(long normalRunners, long delayRunners,
      Snapshot delayIntervalHistSnap) {
      this.normalRunners = normalRunners;
      this.delayRunners = delayRunners;
      this.delayIntervalHistSnap = delayIntervalHistSnap;
    }

    public long getNormalRunners() {
      return normalRunners;
    }

    public long getDelayRunners() {
      return delayRunners;
    }

    public Snapshot getDelayIntervalHistSnap() {
      return delayIntervalHistSnap;
    }

    static RunnerStatsSnapshot snapshot(MetricsConnection.RunnerStats runnerStats) {
      return new RunnerStatsSnapshot(runnerStats.normalRunners.getCount(),
        runnerStats.delayRunners.getCount(), runnerStats.delayIntervalHist.getSnapshot());
    }
  }

  public static class RegionStatsSnapshot {
    private final String name;
    private final Snapshot memstoreLoadHistSnap;
    private final Snapshot heapOccupancyHistSnap;

    public RegionStatsSnapshot(String name, Snapshot memstoreLoadHistSnap,
      Snapshot heapOccupancyHistSnap) {
      this.name = name;
      this.memstoreLoadHistSnap = memstoreLoadHistSnap;
      this.heapOccupancyHistSnap = heapOccupancyHistSnap;
    }

    public String getName() {
      return name;
    }

    public Snapshot getMemstoreLoadHistSnap() {
      return memstoreLoadHistSnap;
    }

    public Snapshot getHeapOccupancyHistSnap() {
      return heapOccupancyHistSnap;
    }

    static RegionStatsSnapshot snapshot(MetricsConnection.RegionStats regionStats) {
      return new RegionStatsSnapshot(regionStats.name, regionStats.memstoreLoadHist.getSnapshot(),
        regionStats.heapOccupancyHist.getSnapshot());
    }
  }

  public MetricsConnectionSnapshot(long connectionCount, long metaCacheHits, long metaCacheMisses,
    CallTrackerSnapshot getTrackerSnap, CallTrackerSnapshot scanTrackerSnap,
    CallTrackerSnapshot appendTrackerSnap, CallTrackerSnapshot deleteTrackerSnap,
    CallTrackerSnapshot incrementTrackerSnap, CallTrackerSnapshot putTrackerSnap,
    CallTrackerSnapshot multiTrackerSnap, RunnerStatsSnapshot runnerStatsSnap,
    long metaCacheNumClearServer, long metaCacheNumClearRegion, long hedgedReadOps,
    long hedgedReadWin, Snapshot concurrentCallsPerServerHistSnap,
    Snapshot numActionsPerServerHistSnap, long nsLookups, long nsLookupsFailed,
    Snapshot overloadedBackoffTimerSnap, double executorPoolUsageRatio, double metaPoolUsageRatio,
    Map<String, Snapshot> rpcTimersSnap, Map<String, Snapshot> rpcHistSnap,
    Map<String, Long> cacheDroppingExceptions, Map<String, Long> rpcCounters,
    Map<ServerName, Map<byte[], RegionStatsSnapshot>> serverStats) {
    this.connectionCount = connectionCount;
    this.metaCacheHits = metaCacheHits;
    this.metaCacheMisses = metaCacheMisses;
    this.getTrackerSnap = getTrackerSnap;
    this.scanTrackerSnap = scanTrackerSnap;
    this.appendTrackerSnap = appendTrackerSnap;
    this.deleteTrackerSnap = deleteTrackerSnap;
    this.incrementTrackerSnap = incrementTrackerSnap;
    this.putTrackerSnap = putTrackerSnap;
    this.multiTrackerSnap = multiTrackerSnap;
    this.runnerStatsSnap = runnerStatsSnap;
    this.metaCacheNumClearServer = metaCacheNumClearServer;
    this.metaCacheNumClearRegion = metaCacheNumClearRegion;
    this.hedgedReadOps = hedgedReadOps;
    this.hedgedReadWin = hedgedReadWin;
    this.concurrentCallsPerServerHistSnap = concurrentCallsPerServerHistSnap;
    this.numActionsPerServerHistSnap = numActionsPerServerHistSnap;
    this.nsLookups = nsLookups;
    this.nsLookupsFailed = nsLookupsFailed;
    this.overloadedBackoffTimerSnap = overloadedBackoffTimerSnap;
    this.executorPoolUsageRatio = executorPoolUsageRatio;
    this.metaPoolUsageRatio = metaPoolUsageRatio;
    this.rpcTimersSnap = rpcTimersSnap;
    this.rpcHistSnap = rpcHistSnap;
    this.cacheDroppingExceptions = cacheDroppingExceptions;
    this.rpcCounters = rpcCounters;
    this.serverStats = serverStats;
  }

  static MetricsConnectionSnapshotBuilder newBuilder() {
    return new MetricsConnectionSnapshotBuilder();
  }

  static class MetricsConnectionSnapshotBuilder {
    private long connectionCount;
    private long metaCacheHits;
    private long metaCacheMisses;
    private CallTrackerSnapshot getTrackerSnap;
    private CallTrackerSnapshot scanTrackerSnap;
    private CallTrackerSnapshot appendTrackerSnap;
    private CallTrackerSnapshot deleteTrackerSnap;
    private CallTrackerSnapshot incrementTrackerSnap;
    private CallTrackerSnapshot putTrackerSnap;
    private CallTrackerSnapshot multiTrackerSnap;
    private RunnerStatsSnapshot runnerStatsSnap;
    private long metaCacheNumClearServer;
    private long metaCacheNumClearRegion;
    private long hedgedReadOps;
    private long hedgedReadWin;
    private Snapshot concurrentCallsPerServerHistSnap;
    private Snapshot numActionsPerServerHistSnap;
    private long nsLookups;
    private long nsLookupsFailed;
    private Snapshot overloadedBackoffTimerSnap;
    private double executorPoolUsageRatio;
    private double metaPoolUsageRatio;
    private Map<String, Snapshot> rpcTimersSnap;
    private Map<String, Snapshot> rpcHistSnap;
    private Map<String, Long> cacheDroppingExceptions;
    private Map<String, Long> rpcCounters;
    private Map<ServerName, Map<byte[], RegionStatsSnapshot>> serverStats;

    public MetricsConnectionSnapshotBuilder connectionCount(long connectionCount) {
      this.connectionCount = connectionCount;
      return this;
    }

    public MetricsConnectionSnapshotBuilder metaCacheHits(long metaCacheHits) {
      this.metaCacheHits = metaCacheHits;
      return this;
    }

    public MetricsConnectionSnapshotBuilder metaCacheMisses(long metaCacheMisses) {
      this.metaCacheMisses = metaCacheMisses;
      return this;
    }

    public MetricsConnectionSnapshotBuilder getTrackerSnap(CallTrackerSnapshot getTrackerSnap) {
      this.getTrackerSnap = getTrackerSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder scanTrackerSnap(CallTrackerSnapshot scanTrackerSnap) {
      this.scanTrackerSnap = scanTrackerSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder
      appendTrackerSnap(CallTrackerSnapshot appendTrackerSnap) {
      this.appendTrackerSnap = appendTrackerSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder
      deleteTrackerSnap(CallTrackerSnapshot deleteTrackerSnap) {
      this.deleteTrackerSnap = deleteTrackerSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder
      incrementTrackerSnap(CallTrackerSnapshot incrementTrackerSnap) {
      this.incrementTrackerSnap = incrementTrackerSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder putTrackerSnap(CallTrackerSnapshot putTrackerSnap) {
      this.putTrackerSnap = putTrackerSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder multiTrackerSnap(CallTrackerSnapshot multiTrackerSnap) {
      this.multiTrackerSnap = multiTrackerSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder runnerStatsSnap(RunnerStatsSnapshot runnerStatsSnap) {
      this.runnerStatsSnap = runnerStatsSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder metaCacheNumClearServer(long metaCacheNumClearServer) {
      this.metaCacheNumClearServer = metaCacheNumClearServer;
      return this;
    }

    public MetricsConnectionSnapshotBuilder metaCacheNumClearRegion(long metaCacheNumClearRegion) {
      this.metaCacheNumClearRegion = metaCacheNumClearRegion;
      return this;
    }

    public MetricsConnectionSnapshotBuilder hedgedReadOps(long hedgedReadOps) {
      this.hedgedReadOps = hedgedReadOps;
      return this;
    }

    public MetricsConnectionSnapshotBuilder hedgedReadWin(long hedgedReadWin) {
      this.hedgedReadWin = hedgedReadWin;
      return this;
    }

    public MetricsConnectionSnapshotBuilder
      concurrentCallsPerServerHistSnap(Snapshot concurrentCallsPerServerHistSnap) {
      this.concurrentCallsPerServerHistSnap = concurrentCallsPerServerHistSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder
      numActionsPerServerHistSnap(Snapshot numActionsPerServerHistSnap) {
      this.numActionsPerServerHistSnap = numActionsPerServerHistSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder nsLookups(long nsLookups) {
      this.nsLookups = nsLookups;
      return this;
    }

    public MetricsConnectionSnapshotBuilder nsLookupsFailed(long nsLookupsFailed) {
      this.nsLookupsFailed = nsLookupsFailed;
      return this;
    }

    public MetricsConnectionSnapshotBuilder
      overloadedBackoffTimerSnap(Snapshot overloadedBackoffTimerSnap) {
      this.overloadedBackoffTimerSnap = overloadedBackoffTimerSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder executorPoolUsageRatio(double executorPoolUsageRatio) {
      this.executorPoolUsageRatio = executorPoolUsageRatio;
      return this;
    }

    public MetricsConnectionSnapshotBuilder metaPoolUsageRatio(double metaPoolUsageRatio) {
      this.metaPoolUsageRatio = metaPoolUsageRatio;
      return this;
    }

    public MetricsConnectionSnapshotBuilder rpcTimersSnap(Map<String, Snapshot> rpcTimersSnap) {
      this.rpcTimersSnap = rpcTimersSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder rpcHistSnap(Map<String, Snapshot> rpcHistSnap) {
      this.rpcHistSnap = rpcHistSnap;
      return this;
    }

    public MetricsConnectionSnapshotBuilder
      cacheDroppingExceptions(Map<String, Long> cacheDroppingExceptions) {
      this.cacheDroppingExceptions = cacheDroppingExceptions;
      return this;
    }

    public MetricsConnectionSnapshotBuilder rpcCounters(Map<String, Long> rpcCounters) {
      this.rpcCounters = rpcCounters;
      return this;
    }

    public MetricsConnectionSnapshotBuilder
      serverStats(Map<ServerName, Map<byte[], RegionStatsSnapshot>> serverStats) {
      this.serverStats = serverStats;
      return this;
    }

    public MetricsConnectionSnapshot build() {
      return new MetricsConnectionSnapshot(connectionCount, metaCacheHits, metaCacheMisses,
        getTrackerSnap, scanTrackerSnap, appendTrackerSnap, deleteTrackerSnap, incrementTrackerSnap,
        putTrackerSnap, multiTrackerSnap, runnerStatsSnap, metaCacheNumClearServer,
        metaCacheNumClearRegion, hedgedReadOps, hedgedReadWin, concurrentCallsPerServerHistSnap,
        numActionsPerServerHistSnap, nsLookups, nsLookupsFailed, overloadedBackoffTimerSnap,
        executorPoolUsageRatio, metaPoolUsageRatio, rpcTimersSnap, rpcHistSnap,
        cacheDroppingExceptions, rpcCounters, serverStats);
    }
  }

  public long getConnectionCount() {
    return connectionCount;
  }

  public long getMetaCacheHits() {
    return metaCacheHits;
  }

  public long getMetaCacheMisses() {
    return metaCacheMisses;
  }

  public CallTrackerSnapshot getGetTrackerSnap() {
    return getTrackerSnap;
  }

  public CallTrackerSnapshot getScanTrackerSnap() {
    return scanTrackerSnap;
  }

  public CallTrackerSnapshot getAppendTrackerSnap() {
    return appendTrackerSnap;
  }

  public CallTrackerSnapshot getDeleteTrackerSnap() {
    return deleteTrackerSnap;
  }

  public CallTrackerSnapshot getIncrementTrackerSnap() {
    return incrementTrackerSnap;
  }

  public CallTrackerSnapshot getPutTrackerSnap() {
    return putTrackerSnap;
  }

  public CallTrackerSnapshot getMultiTrackerSnap() {
    return multiTrackerSnap;
  }

  public RunnerStatsSnapshot getRunnerStatsSnap() {
    return runnerStatsSnap;
  }

  public long getMetaCacheNumClearServer() {
    return metaCacheNumClearServer;
  }

  public long getMetaCacheNumClearRegion() {
    return metaCacheNumClearRegion;
  }

  public long getHedgedReadOps() {
    return hedgedReadOps;
  }

  public long getHedgedReadWin() {
    return hedgedReadWin;
  }

  public Snapshot getConcurrentCallsPerServerHistSnap() {
    return concurrentCallsPerServerHistSnap;
  }

  public Snapshot getNumActionsPerServerHistSnap() {
    return numActionsPerServerHistSnap;
  }

  public long getNsLookups() {
    return nsLookups;
  }

  public long getNsLookupsFailed() {
    return nsLookupsFailed;
  }

  public Snapshot getOverloadedBackoffTimerSnap() {
    return overloadedBackoffTimerSnap;
  }

  public double getExecutorPoolUsageRatio() {
    return executorPoolUsageRatio;
  }

  public double getMetaPoolUsageRatio() {
    return metaPoolUsageRatio;
  }

  public Map<String, Snapshot> getRpcTimersSnap() {
    return rpcTimersSnap;
  }

  public Map<String, Snapshot> getRpcHistSnap() {
    return rpcHistSnap;
  }

  public Map<String, Long> getCacheDroppingExceptions() {
    return cacheDroppingExceptions;
  }

  public Map<String, Long> getRpcCounters() {
    return rpcCounters;
  }

  public Map<ServerName, Map<byte[], RegionStatsSnapshot>> getServerStats() {
    return serverStats;
  }

  static <K, IN, OUT> Map<K, OUT> snapshotMap(Map<K, IN> map, Function<IN, OUT> snapshotFunction) {
    Map<K, OUT> snapshot = new HashMap<>();
    for (Map.Entry<K, IN> entry : map.entrySet()) {
      snapshot.put(entry.getKey(), snapshotFunction.apply(entry.getValue()));
    }
    return snapshot;
  }
}
