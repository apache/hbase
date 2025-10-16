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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
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

  private final Object initializerLock = new Object();
  private volatile boolean initialized = false;

  private volatile Map<String, QuotaState> namespaceQuotaCache = new HashMap<>();
  private volatile Map<TableName, QuotaState> tableQuotaCache = new HashMap<>();
  private volatile Map<String, UserQuotaState> userQuotaCache = new HashMap<>();
  private volatile Map<String, QuotaState> regionServerQuotaCache = new HashMap<>();

  private volatile boolean exceedThrottleQuotaEnabled = false;
  // factors used to divide cluster scope quota into machine scope quota
  private volatile double machineQuotaFactor = 1;
  private final ConcurrentHashMap<TableName, Double> tableMachineQuotaFactors =
    new ConcurrentHashMap<>();
  private final RegionServerServices rsServices;
  private final String userOverrideRequestAttributeKey;

  private QuotaRefresherChore refreshChore;
  private boolean stopped = true;

  public QuotaCache(final RegionServerServices rsServices) {
    this.rsServices = rsServices;
    this.userOverrideRequestAttributeKey =
      rsServices.getConfiguration().get(QUOTA_USER_REQUEST_ATTRIBUTE_OVERRIDE_KEY);
  }

  public void start() throws IOException {
    stopped = false;

    Configuration conf = rsServices.getConfiguration();
    // Refresh the cache every 12 hours, and every time a quota is changed, and every time a
    // configuration reload is triggered. Periodic reloads are kept to a minimum to avoid
    // flooding the RegionServer holding the hbase:quota table with requests.
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

  private void ensureInitialized() {
    if (!initialized) {
      synchronized (initializerLock) {
        if (!initialized) {
          refreshChore.chore();
          initialized = true;
        }
      }
    }
  }

  private Map<String, UserQuotaState> fetchUserQuotaStateEntries() throws IOException {
    return QuotaUtil.fetchUserQuotas(rsServices.getConfiguration(), rsServices.getConnection(),
      tableMachineQuotaFactors, machineQuotaFactor);
  }

  private Map<String, QuotaState> fetchRegionServerQuotaStateEntries() throws IOException {
    return QuotaUtil.fetchRegionServerQuotas(rsServices.getConfiguration(),
      rsServices.getConnection());
  }

  private Map<TableName, QuotaState> fetchTableQuotaStateEntries() throws IOException {
    return QuotaUtil.fetchTableQuotas(rsServices.getConfiguration(), rsServices.getConnection(),
      tableMachineQuotaFactors);
  }

  private Map<String, QuotaState> fetchNamespaceQuotaStateEntries() throws IOException {
    return QuotaUtil.fetchNamespaceQuotas(rsServices.getConfiguration(), rsServices.getConnection(),
      machineQuotaFactor);
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
    ensureInitialized();
    // local reference because the chore thread may assign to userQuotaCache
    Map<String, UserQuotaState> cache = userQuotaCache;
    if (!cache.containsKey(user)) {
      cache.put(user, QuotaUtil.buildDefaultUserQuotaState(rsServices.getConfiguration()));
    }
    return cache.get(user);
  }

  /**
   * Returns the limiter associated to the specified table.
   * @param table the table to limit
   * @return the limiter associated to the specified table
   */
  public QuotaLimiter getTableLimiter(final TableName table) {
    ensureInitialized();
    // local reference because the chore thread may assign to tableQuotaCache
    Map<TableName, QuotaState> cache = tableQuotaCache;
    if (!cache.containsKey(table)) {
      cache.put(table, new QuotaState());
    }
    return cache.get(table).getGlobalLimiter();
  }

  /**
   * Returns the limiter associated to the specified namespace.
   * @param namespace the namespace to limit
   * @return the limiter associated to the specified namespace
   */
  public QuotaLimiter getNamespaceLimiter(final String namespace) {
    ensureInitialized();
    // local reference because the chore thread may assign to namespaceQuotaCache
    Map<String, QuotaState> cache = namespaceQuotaCache;
    if (!cache.containsKey(namespace)) {
      cache.put(namespace, new QuotaState());
    }
    return cache.get(namespace).getGlobalLimiter();
  }

  /**
   * Returns the limiter associated to the specified region server.
   * @param regionServer the region server to limit
   * @return the limiter associated to the specified region server
   */
  public QuotaLimiter getRegionServerQuotaLimiter(final String regionServer) {
    ensureInitialized();
    // local reference because the chore thread may assign to regionServerQuotaCache
    Map<String, QuotaState> cache = regionServerQuotaCache;
    if (!cache.containsKey(regionServer)) {
      cache.put(regionServer, new QuotaState());
    }
    return cache.get(regionServer).getGlobalLimiter();
  }

  protected boolean isExceedThrottleQuotaEnabled() {
    return exceedThrottleQuotaEnabled;
  }

  /**
   * Applies a request attribute user override if available, otherwise returns the UGI's short
   * username
   * @param ugi The request's UserGroupInformation
   */
  String getQuotaUserName(final UserGroupInformation ugi) {
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

  /** visible for testing */
  Map<String, QuotaState> getNamespaceQuotaCache() {
    return namespaceQuotaCache;
  }

  /** visible for testing */
  Map<String, QuotaState> getRegionServerQuotaCache() {
    return regionServerQuotaCache;
  }

  /** visible for testing */
  Map<TableName, QuotaState> getTableQuotaCache() {
    return tableQuotaCache;
  }

  /** visible for testing */
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
    protected void chore() {
      updateQuotaFactors();

      try {
        Map<String, UserQuotaState> newUserQuotaCache = new HashMap<>(fetchUserQuotaStateEntries());
        updateNewCacheFromOld(userQuotaCache, newUserQuotaCache);
        userQuotaCache = newUserQuotaCache;
      } catch (IOException e) {
        LOG.error("Error while fetching user quotas", e);
      }

      try {
        Map<String, QuotaState> newRegionServerQuotaCache =
          new HashMap<>(fetchRegionServerQuotaStateEntries());
        updateNewCacheFromOld(regionServerQuotaCache, newRegionServerQuotaCache);
        regionServerQuotaCache = newRegionServerQuotaCache;
      } catch (IOException e) {
        LOG.error("Error while fetching region server quotas", e);
      }

      try {
        Map<TableName, QuotaState> newTableQuotaCache =
          new HashMap<>(fetchTableQuotaStateEntries());
        updateNewCacheFromOld(tableQuotaCache, newTableQuotaCache);
        tableQuotaCache = newTableQuotaCache;
      } catch (IOException e) {
        LOG.error("Error while refreshing table quotas", e);
      }

      try {
        Map<String, QuotaState> newNamespaceQuotaCache =
          new HashMap<>(fetchNamespaceQuotaStateEntries());
        updateNewCacheFromOld(namespaceQuotaCache, newNamespaceQuotaCache);
        namespaceQuotaCache = newNamespaceQuotaCache;
      } catch (IOException e) {
        LOG.error("Error while refreshing namespace quotas", e);
      }

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

  /** visible for testing */
  static <K, V extends QuotaState> void updateNewCacheFromOld(Map<K, V> oldCache,
    Map<K, V> newCache) {
    for (Map.Entry<K, V> entry : oldCache.entrySet()) {
      K key = entry.getKey();
      if (newCache.containsKey(key)) {
        V newState = newCache.get(key);
        V oldState = entry.getValue();
        oldState.update(newState);
        newCache.put(key, oldState);
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

}
