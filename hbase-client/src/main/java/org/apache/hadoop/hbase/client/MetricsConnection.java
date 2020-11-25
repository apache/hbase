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

import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is for maintaining the various connection statistics and publishing them through
 * the metrics interfaces.
 *
 * This class manages its own {@link MetricRegistry} and {@link JmxReporter} so as to not
 * conflict with other uses of Yammer Metrics within the client application. Instantiating
 * this class implicitly creates and "starts" instances of these classes; be sure to call
 * {@link #shutdown()} to terminate the thread pools they allocate.
 */
@InterfaceAudience.Private
public class MetricsConnection implements StatisticTrackable {

  /** Set this key to {@code true} to enable metrics collection of client requests. */
  public static final String CLIENT_SIDE_METRICS_ENABLED_KEY = "hbase.client.metrics.enable";

  private static final String CNT_BASE = "rpcCount_";
  private static final String DRTN_BASE = "rpcCallDurationMs_";
  private static final String REQ_BASE = "rpcCallRequestSizeBytes_";
  private static final String RESP_BASE = "rpcCallResponseSizeBytes_";
  private static final String MEMLOAD_BASE = "memstoreLoad_";
  private static final String HEAP_BASE = "heapOccupancy_";
  private static final String CACHE_BASE = "cacheDroppingExceptions_";
  private static final String UNKNOWN_EXCEPTION = "UnknownException";
  private static final String CLIENT_SVC = ClientService.getDescriptor().getName();

  /** A container class for collecting details about the RPC call as it percolates. */
  public static class CallStats {
    private long requestSizeBytes = 0;
    private long responseSizeBytes = 0;
    private long startTime = 0;
    private long callTimeMs = 0;
    private int concurrentCallsPerServer = 0;
    private int numActionsPerServer = 0;

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

    public int getConcurrentCallsPerServer() {
      return concurrentCallsPerServer;
    }

    public void setConcurrentCallsPerServer(int callsPerServer) {
      this.concurrentCallsPerServer = callsPerServer;
    }

    public int getNumActionsPerServer() {
      return numActionsPerServer;
    }

    public void setNumActionsPerServer(int numActionsPerServer) {
      this.numActionsPerServer = numActionsPerServer;
    }
  }

  protected static final class CallTracker {
    private final String name;
    final Timer callTimer;
    final Histogram reqHist;
    final Histogram respHist;

    private CallTracker(MetricRegistry registry, String name, String subName, String scope) {
      StringBuilder sb = new StringBuilder(CLIENT_SVC).append("_").append(name);
      if (subName != null) {
        sb.append("(").append(subName).append(")");
      }
      this.name = sb.toString();
      this.callTimer = registry.timer(name(MetricsConnection.class,
        DRTN_BASE + this.name, scope));
      this.reqHist = registry.histogram(name(MetricsConnection.class,
        REQ_BASE + this.name, scope));
      this.respHist = registry.histogram(name(MetricsConnection.class,
        RESP_BASE + this.name, scope));
    }

    private CallTracker(MetricRegistry registry, String name, String scope) {
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

    public RegionStats(MetricRegistry registry, String name) {
      this.name = name;
      this.memstoreLoadHist = registry.histogram(name(MetricsConnection.class,
          MEMLOAD_BASE + this.name));
      this.heapOccupancyHist = registry.histogram(name(MetricsConnection.class,
          HEAP_BASE + this.name));
    }

    public void update(RegionLoadStats regionStatistics) {
      this.memstoreLoadHist.update(regionStatistics.getMemStoreLoad());
      this.heapOccupancyHist.update(regionStatistics.getHeapOccupancy());
    }
  }

  protected static class RunnerStats {
    final Counter normalRunners;
    final Counter delayRunners;
    final Histogram delayIntevalHist;

    public RunnerStats(MetricRegistry registry) {
      this.normalRunners = registry.counter(
        name(MetricsConnection.class, "normalRunnersCount"));
      this.delayRunners = registry.counter(
        name(MetricsConnection.class, "delayRunnersCount"));
      this.delayIntevalHist = registry.histogram(
        name(MetricsConnection.class, "delayIntervalHist"));
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

  protected ConcurrentHashMap<ServerName, ConcurrentMap<byte[], RegionStats>> serverStats
          = new ConcurrentHashMap<>();

  public void updateServerStats(ServerName serverName, byte[] regionName,
                                Object r) {
    if (!(r instanceof Result)) {
      return;
    }
    Result result = (Result) r;
    RegionLoadStats stats = result.getStats();
    if (stats == null) {
      return;
    }
    updateRegionStats(serverName, regionName, stats);
  }

  @Override
  public void updateRegionStats(ServerName serverName, byte[] regionName, RegionLoadStats stats) {
    String name = serverName.getServerName() + "," + Bytes.toStringBinary(regionName);
    ConcurrentMap<byte[], RegionStats> rsStats = computeIfAbsent(serverStats, serverName,
      () -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
    RegionStats regionStats =
        computeIfAbsent(rsStats, regionName, () -> new RegionStats(this.registry, name));
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
   * Anticipated number of concurrent accessor threads
   */
  private static final int CONCURRENCY_LEVEL = 256;

  private final MetricRegistry registry;
  private final JmxReporter reporter;
  private final String scope;

  private final NewMetric<Timer> timerFactory = new NewMetric<Timer>() {
    @Override public Timer newMetric(Class<?> clazz, String name, String scope) {
      return registry.timer(name(clazz, name, scope));
    }
  };

  private final NewMetric<Histogram> histogramFactory = new NewMetric<Histogram>() {
    @Override public Histogram newMetric(Class<?> clazz, String name, String scope) {
      return registry.histogram(name(clazz, name, scope));
    }
  };

  private final NewMetric<Counter> counterFactory = new NewMetric<Counter>() {
    @Override public Counter newMetric(Class<?> clazz, String name, String scope) {
      return registry.counter(name(clazz, name, scope));
    }
  };

  // static metrics

  protected final Counter metaCacheHits;
  protected final Counter metaCacheMisses;
  protected final CallTracker getTracker;
  protected final CallTracker scanTracker;
  protected final CallTracker appendTracker;
  protected final CallTracker deleteTracker;
  protected final CallTracker incrementTracker;
  protected final CallTracker putTracker;
  protected final CallTracker multiTracker;
  protected final RunnerStats runnerStats;
  protected final Counter metaCacheNumClearServer;
  protected final Counter metaCacheNumClearRegion;
  protected final Counter hedgedReadOps;
  protected final Counter hedgedReadWin;
  protected final Histogram concurrentCallsPerServerHist;
  protected final Histogram numActionsPerServerHist;

  // dynamic metrics

  // These maps are used to cache references to the metric instances that are managed by the
  // registry. I don't think their use perfectly removes redundant allocations, but it's
  // a big improvement over calling registry.newMetric each time.
  protected final ConcurrentMap<String, Timer> rpcTimers =
      new ConcurrentHashMap<>(CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
  protected final ConcurrentMap<String, Histogram> rpcHistograms =
      new ConcurrentHashMap<>(CAPACITY * 2 /* tracking both request and response sizes */,
          LOAD_FACTOR, CONCURRENCY_LEVEL);
  private final ConcurrentMap<String, Counter> cacheDroppingExceptions =
    new ConcurrentHashMap<>(CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
  protected final ConcurrentMap<String, Counter>  rpcCounters =
      new ConcurrentHashMap<>(CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);

  MetricsConnection(String scope, Supplier<ThreadPoolExecutor> batchPool,
      Supplier<ThreadPoolExecutor> metaPool) {
    this.scope = scope;
    this.registry = new MetricRegistry();
    this.registry.register(getExecutorPoolName(),
        new RatioGauge() {
          @Override
          protected Ratio getRatio() {
            ThreadPoolExecutor pool = batchPool.get();
            if (pool == null) {
              return Ratio.of(0, 0);
            }
            return Ratio.of(pool.getActiveCount(), pool.getMaximumPoolSize());
          }
        });
    this.registry.register(getMetaPoolName(),
        new RatioGauge() {
          @Override
          protected Ratio getRatio() {
            ThreadPoolExecutor pool = metaPool.get();
            if (pool == null) {
              return Ratio.of(0, 0);
            }
            return Ratio.of(pool.getActiveCount(), pool.getMaximumPoolSize());
          }
        });
    this.metaCacheHits = registry.counter(name(this.getClass(), "metaCacheHits", scope));
    this.metaCacheMisses = registry.counter(name(this.getClass(), "metaCacheMisses", scope));
    this.metaCacheNumClearServer = registry.counter(name(this.getClass(),
      "metaCacheNumClearServer", scope));
    this.metaCacheNumClearRegion = registry.counter(name(this.getClass(),
      "metaCacheNumClearRegion", scope));
    this.hedgedReadOps = registry.counter(name(this.getClass(), "hedgedReadOps", scope));
    this.hedgedReadWin = registry.counter(name(this.getClass(), "hedgedReadWin", scope));
    this.getTracker = new CallTracker(this.registry, "Get", scope);
    this.scanTracker = new CallTracker(this.registry, "Scan", scope);
    this.appendTracker = new CallTracker(this.registry, "Mutate", "Append", scope);
    this.deleteTracker = new CallTracker(this.registry, "Mutate", "Delete", scope);
    this.incrementTracker = new CallTracker(this.registry, "Mutate", "Increment", scope);
    this.putTracker = new CallTracker(this.registry, "Mutate", "Put", scope);
    this.multiTracker = new CallTracker(this.registry, "Multi", scope);
    this.runnerStats = new RunnerStats(this.registry);
    this.concurrentCallsPerServerHist = registry.histogram(name(MetricsConnection.class,
      "concurrentCallsPerServer", scope));
    this.numActionsPerServerHist = registry.histogram(name(MetricsConnection.class,
      "numActionsPerServer", scope));

    this.reporter = JmxReporter.forRegistry(this.registry).build();
    this.reporter.start();
  }

  final String getExecutorPoolName() {
    return name(getClass(), "executorPoolActiveThreads", scope);
  }

  final String getMetaPoolName() {
    return name(getClass(), "metaPoolActiveThreads", scope);
  }

  MetricRegistry getMetricRegistry() {
    return registry;
  }

  public void shutdown() {
    this.reporter.stop();
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

  /** Increment the number of meta cache drops requested for entire RegionServer. */
  public void incrMetaCacheNumClearServer() {
    metaCacheNumClearServer.inc();
  }

  /** Increment the number of meta cache drops requested for individual region. */
  public void incrMetaCacheNumClearRegion() {
    metaCacheNumClearRegion.inc();
  }

  /** Increment the number of meta cache drops requested for individual region. */
  public void incrMetaCacheNumClearRegion(int count) {
    metaCacheNumClearRegion.inc(count);
  }

  /** Increment the number of hedged read that have occurred. */
  public void incrHedgedReadOps() {
    hedgedReadOps.inc();
  }

  /** Increment the number of hedged read returned faster than the original read. */
  public void incrHedgedReadWin() {
    hedgedReadWin.inc();
  }

  /** Increment the number of normal runner counts. */
  public void incrNormalRunners() {
    this.runnerStats.incrNormalRunners();
  }

  /** Increment the number of delay runner counts and update delay interval of delay runner. */
  public void incrDelayRunnersAndUpdateDelayInterval(long interval) {
    this.runnerStats.incrDelayRunners();
    this.runnerStats.updateDelayInterval(interval);
  }

  /**
   * Get a metric for {@code key} from {@code map}, or create it with {@code factory}.
   */
  private <T> T getMetric(String key, ConcurrentMap<String, T> map, NewMetric<T> factory) {
    return computeIfAbsent(map, key, () -> factory.newMetric(getClass(), key, scope));
  }

  /** Update call stats for non-critical-path methods */
  private void updateRpcGeneric(String methodName, CallStats stats) {
    getMetric(DRTN_BASE + methodName, rpcTimers, timerFactory)
        .update(stats.getCallTimeMs(), TimeUnit.MILLISECONDS);
    getMetric(REQ_BASE + methodName, rpcHistograms, histogramFactory)
        .update(stats.getRequestSizeBytes());
    getMetric(RESP_BASE + methodName, rpcHistograms, histogramFactory)
        .update(stats.getResponseSizeBytes());
  }

  /** Report RPC context to metrics system. */
  public void updateRpc(MethodDescriptor method, Message param, CallStats stats) {
    int callsPerServer = stats.getConcurrentCallsPerServer();
    if (callsPerServer > 0) {
      concurrentCallsPerServerHist.update(callsPerServer);
    }
    // Update the counter that tracks RPCs by type.
    final String methodName = method.getService().getName() + "_" + method.getName();
    getMetric(CNT_BASE + methodName, rpcCounters, counterFactory).inc();
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
          assert "PrepareBulkLoad".equals(method.getName());
          // use generic implementation
          break;
        case 5:
          assert "CleanupBulkLoad".equals(method.getName());
          // use generic implementation
          break;
        case 6:
          assert "ExecService".equals(method.getName());
          // use generic implementation
          break;
        case 7:
          assert "ExecRegionServerService".equals(method.getName());
          // use generic implementation
          break;
        case 8:
          assert "Multi".equals(method.getName());
          numActionsPerServerHist.update(stats.getNumActionsPerServer());
          multiTracker.updateRpc(stats);
          return;
        default:
          throw new RuntimeException("Unrecognized ClientService RPC type " + method.getFullName());
      }
    }
    // Fallback to dynamic registry lookup for DDL methods.
    updateRpcGeneric(methodName, stats);
  }

  public void incrCacheDroppingExceptions(Object exception) {
    getMetric(CACHE_BASE +
      (exception == null? UNKNOWN_EXCEPTION : exception.getClass().getSimpleName()),
      cacheDroppingExceptions, counterFactory).inc();
  }
}
