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

import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;

/**
 * This class is for maintaining the various connection statistics and publishing them through the
 * metrics interfaces. This class manages its own {@link MetricRegistry} and {@link JmxReporter} so
 * as to not conflict with other uses of Yammer Metrics within the client application. Calling
 * {@link #getMetricsConnection(Configuration, String, Supplier, Supplier)} implicitly creates and
 * "starts" instances of these classes; be sure to call {@link #deleteMetricsConnection(String)} to
 * terminate the thread pools they allocate. The metrics reporter will be shutdown
 * {@link #shutdown()} when all connections within this metrics instances are closed.
 */
@InterfaceAudience.Private
public final class MetricsConnection implements StatisticTrackable {

  private static final ConcurrentMap<String, MetricsConnection> METRICS_INSTANCES =
    new ConcurrentHashMap<>();

  static MetricsConnection getMetricsConnection(final Configuration conf, final String scope,
    Supplier<ThreadPoolExecutor> batchPool, Supplier<ThreadPoolExecutor> metaPool) {
    return METRICS_INSTANCES.compute(scope, (s, metricsConnection) -> {
      if (metricsConnection == null) {
        MetricsConnection newMetricsConn = new MetricsConnection(conf, scope, batchPool, metaPool);
        newMetricsConn.incrConnectionCount();
        return newMetricsConn;
      } else {
        metricsConnection.addThreadPools(batchPool, metaPool);
        metricsConnection.incrConnectionCount();
        return metricsConnection;
      }
    });
  }

  static void deleteMetricsConnection(final String scope) {
    METRICS_INSTANCES.computeIfPresent(scope, (s, metricsConnection) -> {
      metricsConnection.decrConnectionCount();
      if (metricsConnection.getConnectionCount() == 0) {
        metricsConnection.shutdown();
        return null;
      }
      return metricsConnection;
    });
  }

  /** Set this key to {@code true} to enable metrics collection of client requests. */
  public static final String CLIENT_SIDE_METRICS_ENABLED_KEY = "hbase.client.metrics.enable";

  /** Set this key to {@code true} to enable table metrics collection of client requests. */
  public static final String CLIENT_SIDE_TABLE_METRICS_ENABLED_KEY =
    "hbase.client.table.metrics.enable";

  /**
   * Set to specify a custom scope for the metrics published through {@link MetricsConnection}. The
   * scope is added to JMX MBean objectName, and defaults to a combination of the Connection's
   * clusterId and hashCode. For example, a default value for a connection to cluster "foo" might be
   * "foo-7d9d0818", where "7d9d0818" is the hashCode of the underlying AsyncConnectionImpl. Users
   * may set this key to give a more contextual name for this scope. For example, one might want to
   * differentiate a read connection from a write connection by setting the scopes to "foo-read" and
   * "foo-write" respectively. Scope is the only thing that lends any uniqueness to the metrics.
   * Care should be taken to avoid using the same scope for multiple Connections, otherwise the
   * metrics may aggregate in unforeseen ways.
   */
  public static final String METRICS_SCOPE_KEY = "hbase.client.metrics.scope";

  /**
   * Returns the scope for a MetricsConnection based on the configured {@link #METRICS_SCOPE_KEY} or
   * by generating a default from the passed clusterId and connectionObj's hashCode.
   * @param conf          configuration for the connection
   * @param clusterId     clusterId for the connection
   * @param connectionObj either a Connection or AsyncConnectionImpl, the instance creating this
   *                      MetricsConnection.
   */
  static String getScope(Configuration conf, String clusterId, Object connectionObj) {
    return conf.get(METRICS_SCOPE_KEY,
      clusterId + "@" + Integer.toHexString(connectionObj.hashCode()));
  }

  private static final String CNT_BASE = "rpcCount_";
  private static final String FAILURE_CNT_BASE = "rpcFailureCount_";
  private static final String TOTAL_EXCEPTION_CNT = "rpcTotalExceptions";
  private static final String LOCAL_EXCEPTION_CNT_BASE = "rpcLocalExceptions_";
  private static final String REMOTE_EXCEPTION_CNT_BASE = "rpcRemoteExceptions_";
  private static final String DRTN_BASE = "rpcCallDurationMs_";
  private static final String REQ_BASE = "rpcCallRequestSizeBytes_";
  private static final String RESP_BASE = "rpcCallResponseSizeBytes_";
  private static final String MEMLOAD_BASE = "memstoreLoad_";
  private static final String HEAP_BASE = "heapOccupancy_";
  private static final String CACHE_BASE = "cacheDroppingExceptions_";
  private static final String UNKNOWN_EXCEPTION = "UnknownException";
  private static final String NS_LOOKUPS = "nsLookups";
  private static final String NS_LOOKUPS_FAILED = "nsLookupsFailed";
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
      this.callTimer = registry.timer(name(MetricsConnection.class, DRTN_BASE + this.name, scope));
      this.reqHist = registry.histogram(name(MetricsConnection.class, REQ_BASE + this.name, scope));
      this.respHist =
        registry.histogram(name(MetricsConnection.class, RESP_BASE + this.name, scope));
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
      this.memstoreLoadHist =
        registry.histogram(name(MetricsConnection.class, MEMLOAD_BASE + this.name));
      this.heapOccupancyHist =
        registry.histogram(name(MetricsConnection.class, HEAP_BASE + this.name));
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
      this.normalRunners = registry.counter(name(MetricsConnection.class, "normalRunnersCount"));
      this.delayRunners = registry.counter(name(MetricsConnection.class, "delayRunnersCount"));
      this.delayIntevalHist =
        registry.histogram(name(MetricsConnection.class, "delayIntervalHist"));
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

  private ConcurrentHashMap<ServerName, ConcurrentMap<byte[], RegionStats>> serverStats =
    new ConcurrentHashMap<>();

  public void updateServerStats(ServerName serverName, byte[] regionName, Object r) {
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
  private final boolean tableMetricsEnabled;

  private final NewMetric<Timer> timerFactory = new NewMetric<Timer>() {
    @Override
    public Timer newMetric(Class<?> clazz, String name, String scope) {
      return registry.timer(name(clazz, name, scope));
    }
  };

  private final NewMetric<Histogram> histogramFactory = new NewMetric<Histogram>() {
    @Override
    public Histogram newMetric(Class<?> clazz, String name, String scope) {
      return registry.histogram(name(clazz, name, scope));
    }
  };

  private final NewMetric<Counter> counterFactory = new NewMetric<Counter>() {
    @Override
    public Counter newMetric(Class<?> clazz, String name, String scope) {
      return registry.counter(name(clazz, name, scope));
    }
  };

  // List of thread pool per connection of the metrics.
  private final List<Supplier<ThreadPoolExecutor>> batchPools = new ArrayList<>();
  private final List<Supplier<ThreadPoolExecutor>> metaPools = new ArrayList<>();

  // static metrics

  private final Counter connectionCount;
  private final Counter metaCacheHits;
  private final Counter metaCacheMisses;
  private final CallTracker getTracker;
  private final CallTracker scanTracker;
  private final CallTracker appendTracker;
  private final CallTracker deleteTracker;
  private final CallTracker incrementTracker;
  private final CallTracker putTracker;
  private final CallTracker multiTracker;
  private final RunnerStats runnerStats;
  private final Counter metaCacheNumClearServer;
  private final Counter metaCacheNumClearRegion;
  private final Counter hedgedReadOps;
  private final Counter hedgedReadWin;
  private final Histogram concurrentCallsPerServerHist;
  private final Histogram numActionsPerServerHist;
  private final Counter nsLookups;
  private final Counter nsLookupsFailed;
  private final Timer overloadedBackoffTimer;

  // dynamic metrics

  // These maps are used to cache references to the metric instances that are managed by the
  // registry. I don't think their use perfectly removes redundant allocations, but it's
  // a big improvement over calling registry.newMetric each time.
  private final ConcurrentMap<String, Timer> rpcTimers =
    new ConcurrentHashMap<>(CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
  private final ConcurrentMap<String, Histogram> rpcHistograms = new ConcurrentHashMap<>(
    CAPACITY * 2 /* tracking both request and response sizes */, LOAD_FACTOR, CONCURRENCY_LEVEL);
  private final ConcurrentMap<String, Counter> cacheDroppingExceptions =
    new ConcurrentHashMap<>(CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
  private final ConcurrentMap<String, Counter> rpcCounters =
    new ConcurrentHashMap<>(CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);

  private MetricsConnection(Configuration conf, String scope,
    Supplier<ThreadPoolExecutor> batchPool, Supplier<ThreadPoolExecutor> metaPool) {
    this.scope = scope;
    this.tableMetricsEnabled = conf.getBoolean(CLIENT_SIDE_TABLE_METRICS_ENABLED_KEY, false);
    addThreadPools(batchPool, metaPool);
    this.registry = new MetricRegistry();
    this.registry.register(getExecutorPoolName(), new RatioGauge() {
      @Override
      protected Ratio getRatio() {
        int numerator = 0;
        int denominator = 0;
        for (Supplier<ThreadPoolExecutor> poolSupplier : batchPools) {
          ThreadPoolExecutor pool = poolSupplier.get();
          if (pool != null) {
            int activeCount = pool.getActiveCount();
            int maxPoolSize = pool.getMaximumPoolSize();
            /* The max thread usage ratio among batch pools of all connections */
            if (numerator == 0 || (numerator * maxPoolSize) < (activeCount * denominator)) {
              numerator = activeCount;
              denominator = maxPoolSize;
            }
          }
        }
        return Ratio.of(numerator, denominator);
      }
    });
    this.registry.register(getMetaPoolName(), new RatioGauge() {
      @Override
      protected Ratio getRatio() {
        int numerator = 0;
        int denominator = 0;
        for (Supplier<ThreadPoolExecutor> poolSupplier : metaPools) {
          ThreadPoolExecutor pool = poolSupplier.get();
          if (pool != null) {
            int activeCount = pool.getActiveCount();
            int maxPoolSize = pool.getMaximumPoolSize();
            /* The max thread usage ratio among meta lookup pools of all connections */
            if (numerator == 0 || (numerator * maxPoolSize) < (activeCount * denominator)) {
              numerator = activeCount;
              denominator = maxPoolSize;
            }
          }
        }
        return Ratio.of(numerator, denominator);
      }
    });
    this.connectionCount = registry.counter(name(this.getClass(), "connectionCount", scope));
    this.metaCacheHits = registry.counter(name(this.getClass(), "metaCacheHits", scope));
    this.metaCacheMisses = registry.counter(name(this.getClass(), "metaCacheMisses", scope));
    this.metaCacheNumClearServer =
      registry.counter(name(this.getClass(), "metaCacheNumClearServer", scope));
    this.metaCacheNumClearRegion =
      registry.counter(name(this.getClass(), "metaCacheNumClearRegion", scope));
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
    this.concurrentCallsPerServerHist =
      registry.histogram(name(MetricsConnection.class, "concurrentCallsPerServer", scope));
    this.numActionsPerServerHist =
      registry.histogram(name(MetricsConnection.class, "numActionsPerServer", scope));
    this.nsLookups = registry.counter(name(this.getClass(), NS_LOOKUPS, scope));
    this.nsLookupsFailed = registry.counter(name(this.getClass(), NS_LOOKUPS_FAILED, scope));
    this.overloadedBackoffTimer =
      registry.timer(name(this.getClass(), "overloadedBackoffDurationMs", scope));

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

  /** scope of the metrics object */
  public String getMetricScope() {
    return scope;
  }

  /** serverStats metric */
  public ConcurrentHashMap<ServerName, ConcurrentMap<byte[], RegionStats>> getServerStats() {
    return serverStats;
  }

  /** runnerStats metric */
  public RunnerStats getRunnerStats() {
    return runnerStats;
  }

  /** metaCacheNumClearServer metric */
  public Counter getMetaCacheNumClearServer() {
    return metaCacheNumClearServer;
  }

  /** metaCacheNumClearRegion metric */
  public Counter getMetaCacheNumClearRegion() {
    return metaCacheNumClearRegion;
  }

  /** hedgedReadOps metric */
  public Counter getHedgedReadOps() {
    return hedgedReadOps;
  }

  /** hedgedReadWin metric */
  public Counter getHedgedReadWin() {
    return hedgedReadWin;
  }

  /** numActionsPerServerHist metric */
  public Histogram getNumActionsPerServerHist() {
    return numActionsPerServerHist;
  }

  /** rpcCounters metric */
  public ConcurrentMap<String, Counter> getRpcCounters() {
    return rpcCounters;
  }

  /** rpcTimers metric */
  public ConcurrentMap<String, Timer> getRpcTimers() {
    return rpcTimers;
  }

  /** rpcHistograms metric */
  public ConcurrentMap<String, Histogram> getRpcHistograms() {
    return rpcHistograms;
  }

  /** getTracker metric */
  public CallTracker getGetTracker() {
    return getTracker;
  }

  /** scanTracker metric */
  public CallTracker getScanTracker() {
    return scanTracker;
  }

  /** multiTracker metric */
  public CallTracker getMultiTracker() {
    return multiTracker;
  }

  /** appendTracker metric */
  public CallTracker getAppendTracker() {
    return appendTracker;
  }

  /** deleteTracker metric */
  public CallTracker getDeleteTracker() {
    return deleteTracker;
  }

  /** incrementTracker metric */
  public CallTracker getIncrementTracker() {
    return incrementTracker;
  }

  /** putTracker metric */
  public CallTracker getPutTracker() {
    return putTracker;
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

  public long getMetaCacheMisses() {
    return metaCacheMisses.getCount();
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

  /** Update the overloaded backoff time **/
  public void incrementServerOverloadedBackoffTime(long time, TimeUnit timeUnit) {
    overloadedBackoffTimer.update(time, timeUnit);
  }

  /** Return the connection count of the metrics within a scope */
  public long getConnectionCount() {
    return connectionCount.getCount();
  }

  /** Increment the connection count of the metrics within a scope */
  private void incrConnectionCount() {
    connectionCount.inc();
  }

  /** Decrement the connection count of the metrics within a scope */
  private void decrConnectionCount() {
    connectionCount.dec();
  }

  /** Add thread pools of additional connections to the metrics */
  private void addThreadPools(Supplier<ThreadPoolExecutor> batchPool,
    Supplier<ThreadPoolExecutor> metaPool) {
    batchPools.add(batchPool);
    metaPools.add(metaPool);
  }

  /**
   * Get a metric for {@code key} from {@code map}, or create it with {@code factory}.
   */
  private <T> T getMetric(String key, ConcurrentMap<String, T> map, NewMetric<T> factory) {
    return computeIfAbsent(map, key, () -> factory.newMetric(getClass(), key, scope));
  }

  /** Update call stats for non-critical-path methods */
  private void updateRpcGeneric(String methodName, CallStats stats) {
    getMetric(DRTN_BASE + methodName, rpcTimers, timerFactory).update(stats.getCallTimeMs(),
      TimeUnit.MILLISECONDS);
    getMetric(REQ_BASE + methodName, rpcHistograms, histogramFactory)
      .update(stats.getRequestSizeBytes());
    getMetric(RESP_BASE + methodName, rpcHistograms, histogramFactory)
      .update(stats.getResponseSizeBytes());
  }

  private void shutdown() {
    this.reporter.stop();
  }

  /** Report RPC context to metrics system. */
  public void updateRpc(MethodDescriptor method, TableName tableName, Message param,
    CallStats stats, Throwable e) {
    int callsPerServer = stats.getConcurrentCallsPerServer();
    if (callsPerServer > 0) {
      concurrentCallsPerServerHist.update(callsPerServer);
    }
    // Update the counter that tracks RPCs by type.
    StringBuilder methodName = new StringBuilder();
    methodName.append(method.getService().getName()).append("_").append(method.getName());
    // Distinguish mutate types.
    if ("Mutate".equals(method.getName())) {
      final MutationType type = ((MutateRequest) param).getMutation().getMutateType();
      switch (type) {
        case APPEND:
          methodName.append("(Append)");
          break;
        case DELETE:
          methodName.append("(Delete)");
          break;
        case INCREMENT:
          methodName.append("(Increment)");
          break;
        case PUT:
          methodName.append("(Put)");
          break;
        default:
          methodName.append("(Unknown)");
      }
    }
    getMetric(CNT_BASE + methodName, rpcCounters, counterFactory).inc();
    if (e != null) {
      getMetric(FAILURE_CNT_BASE + methodName, rpcCounters, counterFactory).inc();
      getMetric(TOTAL_EXCEPTION_CNT, rpcCounters, counterFactory).inc();
      if (e instanceof RemoteException) {
        String fullClassName = ((RemoteException) e).getClassName();
        String simpleClassName = (fullClassName != null)
          ? fullClassName.substring(fullClassName.lastIndexOf(".") + 1)
          : "unknown";
        getMetric(REMOTE_EXCEPTION_CNT_BASE + simpleClassName, rpcCounters, counterFactory).inc();
      } else {
        getMetric(LOCAL_EXCEPTION_CNT_BASE + e.getClass().getSimpleName(), rpcCounters,
          counterFactory).inc();
      }
    }
    // this implementation is tied directly to protobuf implementation details. would be better
    // if we could dispatch based on something static, ie, request Message type.
    if (method.getService() == ClientService.getDescriptor()) {
      switch (method.getIndex()) {
        case 0:
          assert "Get".equals(method.getName());
          getTracker.updateRpc(stats);
          updateTableMetric(methodName.toString(), tableName, stats, e);
          return;
        case 1:
          assert "Mutate".equals(method.getName());
          final MutationType mutationType = ((MutateRequest) param).getMutation().getMutateType();
          switch (mutationType) {
            case APPEND:
              appendTracker.updateRpc(stats);
              break;
            case DELETE:
              deleteTracker.updateRpc(stats);
              break;
            case INCREMENT:
              incrementTracker.updateRpc(stats);
              break;
            case PUT:
              putTracker.updateRpc(stats);
              break;
            default:
              throw new RuntimeException("Unrecognized mutation type " + mutationType);
          }
          updateTableMetric(methodName.toString(), tableName, stats, e);
          return;
        case 2:
          assert "Scan".equals(method.getName());
          scanTracker.updateRpc(stats);
          updateTableMetric(methodName.toString(), tableName, stats, e);
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
          updateTableMetric(methodName.toString(), tableName, stats, e);
          return;
        default:
          throw new RuntimeException("Unrecognized ClientService RPC type " + method.getFullName());
      }
    }
    // Fallback to dynamic registry lookup for DDL methods.
    updateRpcGeneric(methodName.toString(), stats);
  }

  /** Report table rpc context to metrics system. */
  private void updateTableMetric(String methodName, TableName tableName, CallStats stats,
    Throwable e) {
    if (tableMetricsEnabled) {
      if (methodName != null) {
        String table = tableName != null && StringUtils.isNotEmpty(tableName.getNameAsString())
          ? tableName.getNameAsString()
          : "unknown";
        String metricKey = methodName + "_" + table;
        // update table rpc context to metrics system,
        // includes rpc call duration, rpc call request/response size(bytes).
        updateRpcGeneric(metricKey, stats);
        if (e != null) {
          // rpc failure call counter with table name.
          getMetric(FAILURE_CNT_BASE + metricKey, rpcCounters, counterFactory).inc();
        }
      }
    }
  }

  public void incrCacheDroppingExceptions(Object exception) {
    getMetric(
      CACHE_BASE + (exception == null ? UNKNOWN_EXCEPTION : exception.getClass().getSimpleName()),
      cacheDroppingExceptions, counterFactory).inc();
  }

  public void incrNsLookups() {
    this.nsLookups.inc();
  }

  public void incrNsLookupsFailed() {
    this.nsLookupsFailed.inc();
  }
}
