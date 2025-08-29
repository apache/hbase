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
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;

/**
 * Cache that keeps track of the quota settings for the users and tables that are interacting with
 * it.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class QuotaCache implements Stoppable {
  private static final Logger LOG = LoggerFactory.getLogger(QuotaCache.class);

  public static final String REFRESH_CONF_KEY = "hbase.quota.refresh.period";
  public static final String TABLE_REGION_STATES_CACHE_TTL_MS =
    "hbase.quota.cache.ttl.region.states.ms";
  public static final String REGION_SERVERS_SIZE_CACHE_TTL_MS =
    "hbase.quota.cache.ttl.servers.size.ms";

  // defines the request attribute key which, when provided, will override the request's username
  // from the perspective of user quotas
  public static final String QUOTA_USER_REQUEST_ATTRIBUTE_OVERRIDE_KEY =
    "hbase.quota.user.override.key";
  private static final int REFRESH_DEFAULT_PERIOD = 43_200_000; // 12 hours
  private static final int EVICT_PERIOD_FACTOR = 5;

  // for testing purpose only, enforce the cache to be always refreshed
  static boolean TEST_FORCE_REFRESH = false;
  // for testing purpose only, block cache refreshes to reliably verify state
  static boolean TEST_BLOCK_REFRESH = false;

  private final ConcurrentMap<String, QuotaState> namespaceQuotaCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<TableName, QuotaState> tableQuotaCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, UserQuotaState> userQuotaCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, QuotaState> regionServerQuotaCache =
    new ConcurrentHashMap<>();
  private volatile boolean exceedThrottleQuotaEnabled = false;
  // factors used to divide cluster scope quota into machine scope quota
  private volatile double machineQuotaFactor = 1;
  private final ConcurrentHashMap<TableName, Double> tableMachineQuotaFactors =
    new ConcurrentHashMap<>();
  private final RegionServerServices rsServices;
  private final String userOverrideRequestAttributeKey;

  private QuotaRefresherChore refreshChore;
  private boolean stopped = true;

  private final Fetcher<String, UserQuotaState> userQuotaStateFetcher = new Fetcher<>() {
    @Override
    public Get makeGet(final String user) {
      final Set<String> namespaces = QuotaCache.this.namespaceQuotaCache.keySet();
      final Set<TableName> tables = QuotaCache.this.tableQuotaCache.keySet();
      return QuotaUtil.makeGetForUserQuotas(user, tables, namespaces);
    }

    @Override
    public Map<String, UserQuotaState> fetchEntries(final List<Get> gets) throws IOException {
      return QuotaUtil.fetchUserQuotas(rsServices.getConnection(), gets, tableMachineQuotaFactors,
        machineQuotaFactor);
    }
  };

  private final Fetcher<String, QuotaState> regionServerQuotaStateFetcher = new Fetcher<>() {
    @Override
    public Get makeGet(final String regionServer) {
      return QuotaUtil.makeGetForRegionServerQuotas(regionServer);
    }

    @Override
    public Map<String, QuotaState> fetchEntries(final List<Get> gets) throws IOException {
      return QuotaUtil.fetchRegionServerQuotas(rsServices.getConnection(), gets);
    }
  };

  private final Fetcher<TableName, QuotaState> tableQuotaStateFetcher = new Fetcher<>() {
    @Override
    public Get makeGet(final TableName table) {
      return QuotaUtil.makeGetForTableQuotas(table);
    }

    @Override
    public Map<TableName, QuotaState> fetchEntries(final List<Get> gets) throws IOException {
      return QuotaUtil.fetchTableQuotas(rsServices.getConnection(), gets, tableMachineQuotaFactors);
    }
  };

  private final Fetcher<String, QuotaState> namespaceQuotaStateFetcher = new Fetcher<>() {
    @Override
    public Get makeGet(final String namespace) {
      return QuotaUtil.makeGetForNamespaceQuotas(namespace);
    }

    @Override
    public Map<String, QuotaState> fetchEntries(final List<Get> gets) throws IOException {
      return QuotaUtil.fetchNamespaceQuotas(rsServices.getConnection(), gets, machineQuotaFactor);
    }
  };

  public QuotaCache(final RegionServerServices rsServices) {
    this.rsServices = rsServices;
    this.userOverrideRequestAttributeKey =
      rsServices.getConfiguration().get(QUOTA_USER_REQUEST_ATTRIBUTE_OVERRIDE_KEY);
  }

  public void start() throws IOException {
    stopped = false;

    Configuration conf = rsServices.getConfiguration();
    // Refresh the cache every 12 hours, and every time a quota is changed, and every time a
    // configuration
    // reload is triggered. Periodic reloads are kept to a minimum to avoid flooding the
    // RegionServer
    // holding the hbase:quota table with requests.
    int period = conf.getInt(REFRESH_CONF_KEY, REFRESH_DEFAULT_PERIOD);
    refreshChore = new QuotaRefresherChore(conf, period, this);
    rsServices.getChoreService().scheduleChore(refreshChore);
  }

  @Override
  public void stop(final String why) {
    if (refreshChore != null) {
      LOG.debug("Stopping QuotaRefresherChore chore.");
      refreshChore.shutdown(true);
    }
    stopped = true;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  /**
   * Returns the limiter associated to the specified user/table.
   * @param ugi   the user to limit
   * @param table the table to limit
   * @return the limiter associated to the specified user/table
   */
  public QuotaLimiter getUserLimiter(final UserGroupInformation ugi, final TableName table) {
    if (table.isSystemTable()) {
      return NoopQuotaLimiter.get();
    }
    return getUserQuotaState(ugi).getTableLimiter(table);
  }

  /**
   * Returns the QuotaState associated to the specified user.
   * @param ugi the user
   * @return the quota info associated to specified user
   */
  public UserQuotaState getUserQuotaState(final UserGroupInformation ugi) {
    String user = getQuotaUserName(ugi);
    if (!userQuotaCache.containsKey(user)) {
      userQuotaCache.put(user,
        QuotaUtil.buildDefaultUserQuotaState(rsServices.getConfiguration(), 0L));
      fetch("user", userQuotaCache, userQuotaStateFetcher);
    }
    return userQuotaCache.get(user);
  }

  /**
   * Returns the limiter associated to the specified table.
   * @param table the table to limit
   * @return the limiter associated to the specified table
   */
  public QuotaLimiter getTableLimiter(final TableName table) {
    if (!tableQuotaCache.containsKey(table)) {
      tableQuotaCache.put(table, new QuotaState());
      fetch("table", tableQuotaCache, tableQuotaStateFetcher);
    }
    return tableQuotaCache.get(table).getGlobalLimiter();
  }

  /**
   * Returns the limiter associated to the specified namespace.
   * @param namespace the namespace to limit
   * @return the limiter associated to the specified namespace
   */
  public QuotaLimiter getNamespaceLimiter(final String namespace) {
    if (!namespaceQuotaCache.containsKey(namespace)) {
      namespaceQuotaCache.put(namespace, new QuotaState());
      fetch("namespace", namespaceQuotaCache, namespaceQuotaStateFetcher);
    }
    return namespaceQuotaCache.get(namespace).getGlobalLimiter();
  }

  /**
   * Returns the limiter associated to the specified region server.
   * @param regionServer the region server to limit
   * @return the limiter associated to the specified region server
   */
  public QuotaLimiter getRegionServerQuotaLimiter(final String regionServer) {
    if (!regionServerQuotaCache.containsKey(regionServer)) {
      regionServerQuotaCache.put(regionServer, new QuotaState());
      fetch("regionServer", regionServerQuotaCache, regionServerQuotaStateFetcher);
    }
    return regionServerQuotaCache.get(regionServer).getGlobalLimiter();
  }

  protected boolean isExceedThrottleQuotaEnabled() {
    return exceedThrottleQuotaEnabled;
  }

  private <K, V extends QuotaState> void fetch(final String type, final Map<K, V> quotasMap,
    final Fetcher<K, V> fetcher) {
    // Find the quota entries to update
    List<Get> gets = quotasMap.keySet().stream().map(fetcher::makeGet).collect(Collectors.toList());

    // fetch and update the quota entries
    if (!gets.isEmpty()) {
      try {
        for (Map.Entry<K, V> entry : fetcher.fetchEntries(gets).entrySet()) {
          V quotaInfo = quotasMap.putIfAbsent(entry.getKey(), entry.getValue());
          if (quotaInfo != null) {
            quotaInfo.update(entry.getValue());
          }

          if (LOG.isTraceEnabled()) {
            LOG.trace("Loading {} key={} quotas={}", type, entry.getKey(), quotaInfo);
          }
        }
      } catch (IOException e) {
        LOG.warn("Unable to read {} from quota table", type, e);
      }
    }
  }

  /**
   * Applies a request attribute user override if available, otherwise returns the UGI's short
   * username
   * @param ugi The request's UserGroupInformation
   */
  private String getQuotaUserName(final UserGroupInformation ugi) {
    if (userOverrideRequestAttributeKey == null) {
      return ugi.getShortUserName();
    }

    Optional<RpcCall> rpcCall = RpcServer.getCurrentCall();
    if (!rpcCall.isPresent()) {
      return ugi.getShortUserName();
    }

    byte[] override = rpcCall.get().getRequestAttribute(userOverrideRequestAttributeKey);
    if (override == null) {
      return ugi.getShortUserName();
    }
    return Bytes.toString(override);
  }

  void triggerCacheRefresh() {
    refreshChore.triggerNow();
  }

  void forceSynchronousCacheRefresh() {
    refreshChore.chore();
  }

  Map<String, QuotaState> getNamespaceQuotaCache() {
    return namespaceQuotaCache;
  }

  Map<String, QuotaState> getRegionServerQuotaCache() {
    return regionServerQuotaCache;
  }

  Map<TableName, QuotaState> getTableQuotaCache() {
    return tableQuotaCache;
  }

  Map<String, UserQuotaState> getUserQuotaCache() {
    return userQuotaCache;
  }

  // TODO: Remove this once we have the notification bus
  private class QuotaRefresherChore extends ScheduledChore {
    // Querying cluster metrics so often, per-RegionServer, limits horizontal scalability.
    // So we cache the results to reduce that load.
    private final RefreshableExpiringValueCache<ClusterMetrics> tableRegionStatesClusterMetrics;
    private final RefreshableExpiringValueCache<Integer> regionServersSize;

    public QuotaRefresherChore(Configuration conf, final int period, final Stoppable stoppable) {
      super("QuotaRefresherChore", stoppable, period);

      Duration tableRegionStatesCacheTtl =
        Duration.ofMillis(conf.getLong(TABLE_REGION_STATES_CACHE_TTL_MS, period));
      this.tableRegionStatesClusterMetrics =
        new RefreshableExpiringValueCache<>("tableRegionStatesClusterMetrics",
          tableRegionStatesCacheTtl, () -> rsServices.getConnection().getAdmin()
            .getClusterMetrics(EnumSet.of(Option.SERVERS_NAME, Option.TABLE_TO_REGIONS_COUNT)));

      Duration regionServersSizeCacheTtl =
        Duration.ofMillis(conf.getLong(REGION_SERVERS_SIZE_CACHE_TTL_MS, period));
      regionServersSize =
        new RefreshableExpiringValueCache<>("regionServersSize", regionServersSizeCacheTtl,
          () -> rsServices.getConnection().getAdmin().getRegionServers().size());
    }

    @Override
    public synchronized boolean triggerNow() {
      tableRegionStatesClusterMetrics.invalidate();
      regionServersSize.invalidate();
      return super.triggerNow();
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "GC_UNRELATED_TYPES",
        justification = "I do not understand why the complaints, it looks good to me -- FIX")
    protected void chore() {
      while (TEST_BLOCK_REFRESH) {
        LOG.info("TEST_BLOCK_REFRESH=true, so blocking QuotaCache refresh until it is false");
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      // Prefetch online tables/namespaces
      for (TableName table : ((HRegionServer) QuotaCache.this.rsServices).getOnlineTables()) {
        if (table.isSystemTable()) {
          continue;
        }
        QuotaCache.this.tableQuotaCache.computeIfAbsent(table, key -> new QuotaState());

        final String ns = table.getNamespaceAsString();

        QuotaCache.this.namespaceQuotaCache.computeIfAbsent(ns, key -> new QuotaState());
      }

      QuotaCache.this.regionServerQuotaCache
        .computeIfAbsent(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY, key -> new QuotaState());

      updateQuotaFactors();
      fetchAndEvict("namespace", QuotaCache.this.namespaceQuotaCache, namespaceQuotaStateFetcher);
      fetchAndEvict("table", QuotaCache.this.tableQuotaCache, tableQuotaStateFetcher);
      fetchAndEvict("user", QuotaCache.this.userQuotaCache, userQuotaStateFetcher);
      fetchAndEvict("regionServer", QuotaCache.this.regionServerQuotaCache,
        regionServerQuotaStateFetcher);
      fetchExceedThrottleQuota();
    }

    private void fetchExceedThrottleQuota() {
      try {
        QuotaCache.this.exceedThrottleQuotaEnabled =
          QuotaUtil.isExceedThrottleQuotaEnabled(rsServices.getConnection());
      } catch (IOException e) {
        LOG.warn("Unable to read if exceed throttle quota enabled from quota table", e);
      }
    }

    private <K, V extends QuotaState> void fetchAndEvict(final String type,
      final ConcurrentMap<K, V> quotasMap, final Fetcher<K, V> fetcher) {
      long now = EnvironmentEdgeManager.currentTime();
      long evictPeriod = getPeriod() * EVICT_PERIOD_FACTOR;
      // Find the quota entries to update
      List<Get> gets = new ArrayList<>();
      List<K> toRemove = new ArrayList<>();
      for (Map.Entry<K, V> entry : quotasMap.entrySet()) {
        long lastQuery = entry.getValue().getLastQuery();
        if (lastQuery > 0 && (now - lastQuery) >= evictPeriod) {
          toRemove.add(entry.getKey());
        } else {
          gets.add(fetcher.makeGet(entry.getKey()));
        }
      }

      for (final K key : toRemove) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("evict " + type + " key=" + key);
        }
        quotasMap.remove(key);
      }

      // fetch and update the quota entries
      if (!gets.isEmpty()) {
        try {
          for (Map.Entry<K, V> entry : fetcher.fetchEntries(gets).entrySet()) {
            V quotaInfo = quotasMap.putIfAbsent(entry.getKey(), entry.getValue());
            if (quotaInfo != null) {
              quotaInfo.update(entry.getValue());
            }

            if (LOG.isTraceEnabled()) {
              LOG.trace("refresh " + type + " key=" + entry.getKey() + " quotas=" + quotaInfo);
            }
          }
        } catch (IOException e) {
          LOG.warn("Unable to read " + type + " from quota table", e);
        }
      }
    }

    /**
     * Update quota factors which is used to divide cluster scope quota into machine scope quota For
     * user/namespace/user over namespace quota, use [1 / RSNum] as machine factor. For table/user
     * over table quota, use [1 / TotalTableRegionNum * MachineTableRegionNum] as machine factor.
     */
    private void updateQuotaFactors() {
      boolean hasTableQuotas = !tableQuotaCache.entrySet().isEmpty()
        || userQuotaCache.values().stream().anyMatch(UserQuotaState::hasTableLimiters);
      if (hasTableQuotas) {
        updateTableMachineQuotaFactors();
      } else {
        updateOnlyMachineQuotaFactors();
      }
    }

    /**
     * This method is cheaper than {@link #updateTableMachineQuotaFactors()} and should be used if
     * we don't have any table quotas in the cache.
     */
    private void updateOnlyMachineQuotaFactors() {
      Optional<Integer> rsSize = regionServersSize.get();
      if (rsSize.isPresent()) {
        updateMachineQuotaFactors(rsSize.get());
      } else {
        regionServersSize.refresh();
      }
    }

    /**
     * This will call {@link #updateMachineQuotaFactors(int)}, and then update the table machine
     * factors as well. This relies on a more expensive query for ClusterMetrics.
     */
    private void updateTableMachineQuotaFactors() {
      Optional<ClusterMetrics> clusterMetricsMaybe = tableRegionStatesClusterMetrics.get();
      if (!clusterMetricsMaybe.isPresent()) {
        tableRegionStatesClusterMetrics.refresh();
        return;
      }
      ClusterMetrics clusterMetrics = clusterMetricsMaybe.get();
      updateMachineQuotaFactors(clusterMetrics.getServersName().size());

      Map<TableName, RegionStatesCount> tableRegionStatesCount =
        clusterMetrics.getTableRegionStatesCount();

      // Update table machine quota factors
      for (TableName tableName : tableQuotaCache.keySet()) {
        if (tableRegionStatesCount.containsKey(tableName)) {
          double factor = 1;
          try {
            long regionSize = tableRegionStatesCount.get(tableName).getOpenRegions();
            if (regionSize == 0) {
              factor = 0;
            } else {
              int localRegionSize = rsServices.getRegions(tableName).size();
              factor = 1.0 * localRegionSize / regionSize;
            }
          } catch (IOException e) {
            LOG.warn("Get table regions failed: {}", tableName, e);
          }
          tableMachineQuotaFactors.put(tableName, factor);
        } else {
          // TableName might have already been dropped (outdated)
          tableMachineQuotaFactors.remove(tableName);
        }
      }
    }

    private void updateMachineQuotaFactors(int rsSize) {
      if (rsSize != 0) {
        // TODO if use rs group, the cluster limit should be shared by the rs group
        machineQuotaFactor = 1.0 / rsSize;
      }
    }
  }

  static class RefreshableExpiringValueCache<T> {
    private final String name;
    private final LoadingCache<String, Optional<T>> cache;

    RefreshableExpiringValueCache(String name, Duration refreshPeriod,
      ThrowingSupplier<T> supplier) {
      this.name = name;
      this.cache =
        CacheBuilder.newBuilder().expireAfterWrite(refreshPeriod.toMillis(), TimeUnit.MILLISECONDS)
          .build(new CacheLoader<>() {
            @Override
            public Optional<T> load(String key) {
              try {
                return Optional.of(supplier.get());
              } catch (Exception e) {
                LOG.warn("Failed to refresh cache {}", name, e);
                return Optional.empty();
              }
            }
          });
    }

    Optional<T> get() {
      return cache.getUnchecked(name);
    }

    void refresh() {
      cache.refresh(name);
    }

    void invalidate() {
      cache.invalidate(name);
    }
  }

  @FunctionalInterface
  static interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  interface Fetcher<Key, Value> {
    Get makeGet(Key key);

    Map<Key, Value> fetchEntries(List<Get> gets) throws IOException;
  }
}
