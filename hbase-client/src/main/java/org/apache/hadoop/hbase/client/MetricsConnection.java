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
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.RatioGauge;
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

import static com.codahale.metrics.MetricRegistry.name;

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

  private static final String DRTN_BASE = "rpcCallDurationMs_";
  private static final String REQ_BASE = "rpcCallRequestSizeBytes_";
  private static final String RESP_BASE = "rpcCallResponseSizeBytes_";
  private static final String MEMLOAD_BASE = "memstoreLoad_";
  private static final String HEAP_BASE = "heapOccupancy_";
  private static final String CACHE_BASE = "cacheDroppingExceptions_";
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
  protected static final class CallTracker {
    private final String name;
    @VisibleForTesting final Timer callTimer;
    @VisibleForTesting final Histogram reqHist;
    @VisibleForTesting final Histogram respHist;

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
    if (stats == null) {
      return;
    }
    updateRegionStats(serverName, regionName, stats);
  }

  @Override
  public void updateRegionStats(ServerName serverName, byte[] regionName,
    ClientProtos.RegionLoadStats stats) {
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
   * {@link ConnectionImplementation#getBatchPool()}
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
  private final Counter metaCacheNumClearServer;
  private final Counter metaCacheNumClearRegion;

  // dynamic metrics

  // These maps are used to cache references to the metric instances that are managed by the
  // registry. I don't think their use perfectly removes redundant allocations, but it's
  // a big improvement over calling registry.newMetric each time.
  @VisibleForTesting protected final ConcurrentMap<String, Timer> rpcTimers =
      new ConcurrentHashMap<>(CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
  @VisibleForTesting protected final ConcurrentMap<String, Histogram> rpcHistograms =
      new ConcurrentHashMap<>(CAPACITY * 2 /* tracking both request and response sizes */,
          LOAD_FACTOR, CONCURRENCY_LEVEL);
  private final ConcurrentMap<String, Counter> cacheDroppingExceptions =
    new ConcurrentHashMap<>(CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);

  public MetricsConnection(final ConnectionImplementation conn) {
    this.scope = conn.toString();
    this.registry = new MetricRegistry();
    final ThreadPoolExecutor batchPool = (ThreadPoolExecutor) conn.getCurrentBatchPool();
    final ThreadPoolExecutor metaPool = (ThreadPoolExecutor) conn.getCurrentMetaLookupPool();

    this.registry.register(name(this.getClass(), "executorPoolActiveThreads", scope),
        new RatioGauge() {
          @Override
          protected Ratio getRatio() {
            return Ratio.of(batchPool.getActiveCount(), batchPool.getMaximumPoolSize());
          }
        });
    this.registry.register(name(this.getClass(), "metaPoolActiveThreads", scope),
        new RatioGauge() {
          @Override
          protected Ratio getRatio() {
            return Ratio.of(metaPool.getActiveCount(), metaPool.getMaximumPoolSize());
          }
        });
    this.metaCacheHits = registry.counter(name(this.getClass(), "metaCacheHits", scope));
    this.metaCacheMisses = registry.counter(name(this.getClass(), "metaCacheMisses", scope));
    this.metaCacheNumClearServer = registry.counter(name(this.getClass(),
      "metaCacheNumClearServer", scope));
    this.metaCacheNumClearRegion = registry.counter(name(this.getClass(),
      "metaCacheNumClearRegion", scope));
    this.getTracker = new CallTracker(this.registry, "Get", scope);
    this.scanTracker = new CallTracker(this.registry, "Scan", scope);
    this.appendTracker = new CallTracker(this.registry, "Mutate", "Append", scope);
    this.deleteTracker = new CallTracker(this.registry, "Mutate", "Delete", scope);
    this.incrementTracker = new CallTracker(this.registry, "Mutate", "Increment", scope);
    this.putTracker = new CallTracker(this.registry, "Mutate", "Put", scope);
    this.multiTracker = new CallTracker(this.registry, "Multi", scope);
    this.runnerStats = new RunnerStats(this.registry);

    this.reporter = JmxReporter.forRegistry(this.registry).build();
    this.reporter.start();
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
      T tmp = map.putIfAbsent(key, t);
      t = (tmp == null) ? t : tmp;
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

  public void incrCacheDroppingExceptions(Object exception) {
    getMetric(CACHE_BASE + exception.getClass().getSimpleName(),
      cacheDroppingExceptions, counterFactory).inc();
  }
}
