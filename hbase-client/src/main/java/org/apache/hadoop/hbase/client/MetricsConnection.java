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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.JmxReporter;
import com.yammer.metrics.util.RatioGauge;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class is for maintaining the various connection statistics and publishing them through
 * the metrics interfaces.
 *
 * This class manages its own {@link MetricsRegistry} and {@link JmxReporter} so as to not
 * conflict with other uses of Yammer Metrics within the client application. Instantiating
 * this class implicitly creates and "starts" instances of these classes; be sure to call
 * {@link #shutdown()} to terminate the thread pools they allocate.
 */
@InterfaceAudience.Private
public class MetricsConnection {

  /** Set this key to {@code true} to enable metrics collection of client requests. */
  public static final String CLIENT_SIDE_METRICS_ENABLED_KEY = "hbase.client.metrics.enable";

  private static final String DRTN_BASE = "rpcCallDurationMs_";
  private static final String REQ_BASE = "rpcCallRequestSizeBytes_";
  private static final String RESP_BASE = "rpcCallResponseSizeBytes_";
  private static final String MEMLOAD_BASE = "memstoreLoad_";
  private static final String HEAP_BASE = "heapOccupancy_";
  private static final String CLIENT_SVC = ClientService.getDescriptor().getName();

  /** A container class for collecting details about the RPC call as it percolates. */
  public static class CallStats {
    private long requestSizeBytes = 0;
    private long responseSizeBytes = 0;
    private long startTime = 0;
    private long callTimeMs = 0;

    public long getRequestSizeBytes() {
      return requestSizeBytes;
    }

    public void setRequestSizeBytes(long requestSizeBytes) {
      this.requestSizeBytes = requestSizeBytes;
    }

    public long getResponseSizeBytes() {
      return responseSizeBytes;
    }

    public void setResponseSizeBytes(long responseSizeBytes) {
      this.responseSizeBytes = responseSizeBytes;
    }

    public long getStartTime() {
      return startTime;
    }

    public void setStartTime(long startTime) {
      this.startTime = startTime;
    }

    public long getCallTimeMs() {
      return callTimeMs;
    }

    public void setCallTimeMs(long callTimeMs) {
      this.callTimeMs = callTimeMs;
    }
  }

  @VisibleForTesting
  protected final class CallTracker {
    private final String name;
    @VisibleForTesting final Timer callTimer;
    @VisibleForTesting final Histogram reqHist;
    @VisibleForTesting final Histogram respHist;

    private CallTracker(MetricsRegistry registry, String name, String subName, String scope) {
      StringBuilder sb = new StringBuilder(CLIENT_SVC).append("_").append(name);
      if (subName != null) {
        sb.append("(").append(subName).append(")");
      }
      this.name = sb.toString();
      this.callTimer = registry.newTimer(MetricsConnection.class, DRTN_BASE + this.name, scope);
      this.reqHist = registry.newHistogram(MetricsConnection.class, REQ_BASE + this.name, scope);
      this.respHist = registry.newHistogram(MetricsConnection.class, RESP_BASE + this.name, scope);
    }

    private CallTracker(MetricsRegistry registry, String name, String scope) {
      this(registry, name, null, scope);
    }

    public void updateRpc(CallStats stats) {
      this.callTimer.update(stats.getCallTimeMs(), TimeUnit.MILLISECONDS);
      this.reqHist.update(stats.getRequestSizeBytes());
      this.respHist.update(stats.getResponseSizeBytes());
    }

    @Override
    public String toString() {
      return "CallTracker:" + name;
    }
  }

  protected static class RegionStats {
    final String name;
    final Histogram memstoreLoadHist;
    final Histogram heapOccupancyHist;

    public RegionStats(MetricsRegistry registry, String name) {
      this.name = name;
      this.memstoreLoadHist = registry.newHistogram(MetricsConnection.class,
          MEMLOAD_BASE + this.name);
      this.heapOccupancyHist = registry.newHistogram(MetricsConnection.class,
          HEAP_BASE + this.name);
    }

    public void update(ClientProtos.RegionLoadStats regionStatistics) {
      this.memstoreLoadHist.update(regionStatistics.getMemstoreLoad());
      this.heapOccupancyHist.update(regionStatistics.getHeapOccupancy());
    }
  }

  @VisibleForTesting
  protected static class RunnerStats {
    final Counter normalRunners;
    final Counter delayRunners;
    final Histogram delayIntevalHist;

    public RunnerStats(MetricsRegistry registry) {
      this.normalRunners = registry.newCounter(MetricsConnection.class, "normalRunnersCount");
      this.delayRunners = registry.newCounter(MetricsConnection.class, "delayRunnersCount");
      this.delayIntevalHist = registry.newHistogram(MetricsConnection.class, "delayIntervalHist");
    }

    public void incrNormalRunners() {
      this.normalRunners.inc();
    }

    public void incrDelayRunners() {
      this.delayRunners.inc();
    }

    public void updateDelayInterval(long interval) {
      this.delayIntevalHist.update(interval);
    }
  }

  @VisibleForTesting
  protected ConcurrentHashMap<ServerName, ConcurrentMap<byte[], RegionStats>> serverStats
          = new ConcurrentHashMap<ServerName, ConcurrentMap<byte[], RegionStats>>();

  public void updateServerStats(ServerName serverName, byte[] regionName,
                                Object r) {
    if (!(r instanceof Result)) {
      return;
    }
    Result result = (Result) r;
    ClientProtos.RegionLoadStats stats = result.getStats();
    if(stats == null){
      return;
    }
    String name = serverName.getServerName() + "," + Bytes.toStringBinary(regionName);
    ConcurrentMap<byte[], RegionStats> rsStats = null;
    if (serverStats.containsKey(serverName)) {
      rsStats = serverStats.get(serverName);
    } else {
      rsStats = serverStats.putIfAbsent(serverName,
          new ConcurrentSkipListMap<byte[], RegionStats>(Bytes.BYTES_COMPARATOR));
      if (rsStats == null) {
        rsStats = serverStats.get(serverName);
      }
    }
    RegionStats regionStats = null;
    if (rsStats.containsKey(regionName)) {
      regionStats = rsStats.get(regionName);
    } else {
      regionStats = rsStats.putIfAbsent(regionName, new RegionStats(this.registry, name));
      if (regionStats == null) {
        regionStats = rsStats.get(regionName);
      }
    }
    regionStats.update(stats);
  }


  /** A lambda for dispatching to the appropriate metric factory method */
  private static interface NewMetric<T> {
    T newMetric(Class<?> clazz, String name, String scope);
  }

  /** Anticipated number of metric entries */
  private static final int CAPACITY = 50;
  /** Default load factor from {@link java.util.HashMap#DEFAULT_LOAD_FACTOR} */
  private static final float LOAD_FACTOR = 0.75f;
  /**
   * Anticipated number of concurrent accessor threads, from
   * {@link ConnectionManager.HConnectionImplementation#getBatchPool()}
   */
  private static final int CONCURRENCY_LEVEL = 256;

  private final MetricsRegistry registry;
  private final JmxReporter reporter;
  private final String scope;

  private final NewMetric<Timer> timerFactory = new NewMetric<Timer>() {
    @Override public Timer newMetric(Class<?> clazz, String name, String scope) {
      return registry.newTimer(clazz, name, scope);
    }
  };

  private final NewMetric<Histogram> histogramFactory = new NewMetric<Histogram>() {
    @Override public Histogram newMetric(Class<?> clazz, String name, String scope) {
      return registry.newHistogram(clazz, name, scope);
    }
  };

  // static metrics

  @VisibleForTesting protected final Counter metaCacheHits;
  @VisibleForTesting protected final Counter metaCacheMisses;
  @VisibleForTesting protected final CallTracker getTracker;
  @VisibleForTesting protected final CallTracker scanTracker;
  @VisibleForTesting protected final CallTracker appendTracker;
  @VisibleForTesting protected final CallTracker deleteTracker;
  @VisibleForTesting protected final CallTracker incrementTracker;
  @VisibleForTesting protected final CallTracker putTracker;
  @VisibleForTesting protected final CallTracker multiTracker;
  @VisibleForTesting protected final RunnerStats runnerStats;

  // dynamic metrics

  // These maps are used to cache references to the metric instances that are managed by the
  // registry. I don't think their use perfectly removes redundant allocations, but it's
  // a big improvement over calling registry.newMetric each time.
  @VisibleForTesting protected final ConcurrentMap<String, Timer> rpcTimers =
      new ConcurrentHashMap<>(CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
  @VisibleForTesting protected final ConcurrentMap<String, Histogram> rpcHistograms =
      new ConcurrentHashMap<>(CAPACITY * 2 /* tracking both request and response sizes */,
          LOAD_FACTOR, CONCURRENCY_LEVEL);

  public MetricsConnection(final ConnectionManager.HConnectionImplementation conn) {
    this.scope = conn.toString();
    this.registry = new MetricsRegistry();
    final ThreadPoolExecutor batchPool = (ThreadPoolExecutor) conn.getCurrentBatchPool();
    final ThreadPoolExecutor metaPool = (ThreadPoolExecutor) conn.getCurrentMetaLookupPool();

    this.registry.newGauge(this.getClass(), "executorPoolActiveThreads", scope,
        new RatioGauge() {
          @Override protected double getNumerator() {
            return batchPool.getActiveCount();
          }
          @Override protected double getDenominator() {
            return batchPool.getMaximumPoolSize();
          }
        });
    this.registry.newGauge(this.getClass(), "metaPoolActiveThreads", scope,
        new RatioGauge() {
          @Override protected double getNumerator() {
            return metaPool.getActiveCount();
          }
          @Override protected double getDenominator() {
            return metaPool.getMaximumPoolSize();
          }
        });
    this.metaCacheHits = registry.newCounter(this.getClass(), "metaCacheHits", scope);
    this.metaCacheMisses = registry.newCounter(this.getClass(), "metaCacheMisses", scope);
    this.getTracker = new CallTracker(this.registry, "Get", scope);
    this.scanTracker = new CallTracker(this.registry, "Scan", scope);
    this.appendTracker = new CallTracker(this.registry, "Mutate", "Append", scope);
    this.deleteTracker = new CallTracker(this.registry, "Mutate", "Delete", scope);
    this.incrementTracker = new CallTracker(this.registry, "Mutate", "Increment", scope);
    this.putTracker = new CallTracker(this.registry, "Mutate", "Put", scope);
    this.multiTracker = new CallTracker(this.registry, "Multi", scope);
    this.runnerStats = new RunnerStats(this.registry);

    this.reporter = new JmxReporter(this.registry);
    this.reporter.start();
  }

  public void shutdown() {
    this.reporter.shutdown();
    this.registry.shutdown();
  }

  /** Produce an instance of {@link CallStats} for clients to attach to RPCs. */
  public static CallStats newCallStats() {
    // TODO: instance pool to reduce GC?
    return new CallStats();
  }

  /** Increment the number of meta cache hits. */
  public void incrMetaCacheHit() {
    metaCacheHits.inc();
  }

  /** Increment the number of meta cache misses. */
  public void incrMetaCacheMiss() {
    metaCacheMisses.inc();
  }

  /** Increment the number of normal runner counts. */
  public void incrNormalRunners() {
    this.runnerStats.incrNormalRunners();
  }

  /** Increment the number of delay runner counts. */
  public void incrDelayRunners() {
    this.runnerStats.incrDelayRunners();
  }

  /** Update delay interval of delay runner. */
  public void updateDelayInterval(long interval) {
    this.runnerStats.updateDelayInterval(interval);
  }

  /**
   * Get a metric for {@code key} from {@code map}, or create it with {@code factory}.
   */
  private <T> T getMetric(String key, ConcurrentMap<String, T> map, NewMetric<T> factory) {
    T t = map.get(key);
    if (t == null) {
      t = factory.newMetric(this.getClass(), key, scope);
      map.putIfAbsent(key, t);
    }
    return t;
  }

  /** Update call stats for non-critical-path methods */
  private void updateRpcGeneric(MethodDescriptor method, CallStats stats) {
    final String methodName = method.getService().getName() + "_" + method.getName();
    getMetric(DRTN_BASE + methodName, rpcTimers, timerFactory)
        .update(stats.getCallTimeMs(), TimeUnit.MILLISECONDS);
    getMetric(REQ_BASE + methodName, rpcHistograms, histogramFactory)
        .update(stats.getRequestSizeBytes());
    getMetric(RESP_BASE + methodName, rpcHistograms, histogramFactory)
        .update(stats.getResponseSizeBytes());
  }

  /** Report RPC context to metrics system. */
  public void updateRpc(MethodDescriptor method, Message param, CallStats stats) {
    // this implementation is tied directly to protobuf implementation details. would be better
    // if we could dispatch based on something static, ie, request Message type.
    if (method.getService() == ClientService.getDescriptor()) {
      switch(method.getIndex()) {
      case 0:
        assert "Get".equals(method.getName());
        getTracker.updateRpc(stats);
        return;
      case 1:
        assert "Mutate".equals(method.getName());
        final MutationType mutationType = ((MutateRequest) param).getMutation().getMutateType();
        switch(mutationType) {
        case APPEND:
          appendTracker.updateRpc(stats);
          return;
        case DELETE:
          deleteTracker.updateRpc(stats);
          return;
        case INCREMENT:
          incrementTracker.updateRpc(stats);
          return;
        case PUT:
          putTracker.updateRpc(stats);
          return;
        default:
          throw new RuntimeException("Unrecognized mutation type " + mutationType);
        }
      case 2:
        assert "Scan".equals(method.getName());
        scanTracker.updateRpc(stats);
        return;
      case 3:
        assert "BulkLoadHFile".equals(method.getName());
        // use generic implementation
        break;
      case 4:
        assert "ExecService".equals(method.getName());
        // use generic implementation
        break;
      case 5:
        assert "ExecRegionServerService".equals(method.getName());
        // use generic implementation
        break;
      case 6:
        assert "Multi".equals(method.getName());
        multiTracker.updateRpc(stats);
        return;
      default:
        throw new RuntimeException("Unrecognized ClientService RPC type " + method.getFullName());
      }
    }
    // Fallback to dynamic registry lookup for DDL methods.
    updateRpcGeneric(method, stats);
  }
}
