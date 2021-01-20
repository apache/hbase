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

import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache that keeps track of the quota settings for the users and tables that
 * are interacting with it.
 *
 * To avoid blocking the operations if the requested quota is not in cache
 * an "empty quota" will be returned and the request to fetch the quota information
 * will be enqueued for the next refresh.
 *
 * TODO: At the moment the Cache has a Chore that will be triggered every 5min
 * or on cache-miss events. Later the Quotas will be pushed using the notification system.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class QuotaCache implements Stoppable {
  private static final Logger LOG = LoggerFactory.getLogger(QuotaCache.class);

  public static final String REFRESH_CONF_KEY = "hbase.quota.refresh.period";
  private static final int REFRESH_DEFAULT_PERIOD = 5 * 60000; // 5min
  private static final int EVICT_PERIOD_FACTOR = 5; // N * REFRESH_DEFAULT_PERIOD

  // for testing purpose only, enforce the cache to be always refreshed
  static boolean TEST_FORCE_REFRESH = false;

  private final ConcurrentHashMap<String, QuotaState> namespaceQuotaCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<TableName, QuotaState> tableQuotaCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, UserQuotaState> userQuotaCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, QuotaState> regionServerQuotaCache =
      new ConcurrentHashMap<>();
  private volatile boolean exceedThrottleQuotaEnabled = false;
  // factors used to divide cluster scope quota into machine scope quota
  private volatile double machineQuotaFactor = 1;
  private final ConcurrentHashMap<TableName, Double> tableMachineQuotaFactors =
      new ConcurrentHashMap<>();
  private final RegionServerServices rsServices;

  private QuotaRefresherChore refreshChore;
  private boolean stopped = true;

  public QuotaCache(final RegionServerServices rsServices) {
    this.rsServices = rsServices;
  }

  public void start() throws IOException {
    stopped = false;

    // TODO: This will be replaced once we have the notification bus ready.
    Configuration conf = rsServices.getConfiguration();
    int period = conf.getInt(REFRESH_CONF_KEY, REFRESH_DEFAULT_PERIOD);
    refreshChore = new QuotaRefresherChore(period, this);
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
   *
   * @param ugi the user to limit
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
    return computeIfAbsent(userQuotaCache, ugi.getShortUserName(), UserQuotaState::new,
      this::triggerCacheRefresh);
  }

  /**
   * Returns the limiter associated to the specified table.
   *
   * @param table the table to limit
   * @return the limiter associated to the specified table
   */
  public QuotaLimiter getTableLimiter(final TableName table) {
    return getQuotaState(this.tableQuotaCache, table).getGlobalLimiter();
  }

  /**
   * Returns the limiter associated to the specified namespace.
   *
   * @param namespace the namespace to limit
   * @return the limiter associated to the specified namespace
   */
  public QuotaLimiter getNamespaceLimiter(final String namespace) {
    return getQuotaState(this.namespaceQuotaCache, namespace).getGlobalLimiter();
  }

  /**
   * Returns the limiter associated to the specified region server.
   *
   * @param regionServer the region server to limit
   * @return the limiter associated to the specified region server
   */
  public QuotaLimiter getRegionServerQuotaLimiter(final String regionServer) {
    return getQuotaState(this.regionServerQuotaCache, regionServer).getGlobalLimiter();
  }

  protected boolean isExceedThrottleQuotaEnabled() {
    return exceedThrottleQuotaEnabled;
  }

  /**
   * Returns the QuotaState requested. If the quota info is not in cache an empty one will be
   * returned and the quota request will be enqueued for the next cache refresh.
   */
  private <K> QuotaState getQuotaState(final ConcurrentHashMap<K, QuotaState> quotasMap,
      final K key) {
    return computeIfAbsent(quotasMap, key, QuotaState::new, this::triggerCacheRefresh);
  }

  void triggerCacheRefresh() {
    refreshChore.triggerNow();
  }

  long getLastUpdate() {
    return refreshChore.lastUpdate;
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
    private long lastUpdate = 0;

    public QuotaRefresherChore(final int period, final Stoppable stoppable) {
      super("QuotaRefresherChore", stoppable, period);
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="GC_UNRELATED_TYPES",
      justification="I do not understand why the complaints, it looks good to me -- FIX")
    protected void chore() {
      // Prefetch online tables/namespaces
      for (TableName table: ((HRegionServer)QuotaCache.this.rsServices).getOnlineTables()) {
        if (table.isSystemTable()) continue;
        if (!QuotaCache.this.tableQuotaCache.containsKey(table)) {
          QuotaCache.this.tableQuotaCache.putIfAbsent(table, new QuotaState());
        }
        String ns = table.getNamespaceAsString();
        if (!QuotaCache.this.namespaceQuotaCache.containsKey(ns)) {
          QuotaCache.this.namespaceQuotaCache.putIfAbsent(ns, new QuotaState());
        }
      }
      QuotaCache.this.regionServerQuotaCache.putIfAbsent(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY,
        new QuotaState());

      updateQuotaFactors();
      fetchNamespaceQuotaState();
      fetchTableQuotaState();
      fetchUserQuotaState();
      fetchRegionServerQuotaState();
      fetchExceedThrottleQuota();
      lastUpdate = EnvironmentEdgeManager.currentTime();
    }

    private void fetchNamespaceQuotaState() {
      fetch("namespace", QuotaCache.this.namespaceQuotaCache, new Fetcher<String, QuotaState>() {
        @Override
        public Get makeGet(final Map.Entry<String, QuotaState> entry) {
          return QuotaUtil.makeGetForNamespaceQuotas(entry.getKey());
        }

        @Override
        public Map<String, QuotaState> fetchEntries(final List<Get> gets)
            throws IOException {
          return QuotaUtil.fetchNamespaceQuotas(rsServices.getConnection(), gets,
            machineQuotaFactor);
        }
      });
    }

    private void fetchTableQuotaState() {
      fetch("table", QuotaCache.this.tableQuotaCache, new Fetcher<TableName, QuotaState>() {
        @Override
        public Get makeGet(final Map.Entry<TableName, QuotaState> entry) {
          return QuotaUtil.makeGetForTableQuotas(entry.getKey());
        }

        @Override
        public Map<TableName, QuotaState> fetchEntries(final List<Get> gets)
            throws IOException {
          return QuotaUtil.fetchTableQuotas(rsServices.getConnection(), gets,
            tableMachineQuotaFactors);
        }
      });
    }

    private void fetchUserQuotaState() {
      final Set<String> namespaces = QuotaCache.this.namespaceQuotaCache.keySet();
      final Set<TableName> tables = QuotaCache.this.tableQuotaCache.keySet();
      fetch("user", QuotaCache.this.userQuotaCache, new Fetcher<String, UserQuotaState>() {
        @Override
        public Get makeGet(final Map.Entry<String, UserQuotaState> entry) {
          return QuotaUtil.makeGetForUserQuotas(entry.getKey(), tables, namespaces);
        }

        @Override
        public Map<String, UserQuotaState> fetchEntries(final List<Get> gets)
            throws IOException {
          return QuotaUtil.fetchUserQuotas(rsServices.getConnection(), gets,
            tableMachineQuotaFactors, machineQuotaFactor);
        }
      });
    }

    private void fetchRegionServerQuotaState() {
      fetch("regionServer", QuotaCache.this.regionServerQuotaCache,
        new Fetcher<String, QuotaState>() {
          @Override
          public Get makeGet(final Map.Entry<String, QuotaState> entry) {
            return QuotaUtil.makeGetForRegionServerQuotas(entry.getKey());
          }

          @Override
          public Map<String, QuotaState> fetchEntries(final List<Get> gets) throws IOException {
            return QuotaUtil.fetchRegionServerQuotas(rsServices.getConnection(), gets);
          }
        });
    }

    private void fetchExceedThrottleQuota() {
      try {
        QuotaCache.this.exceedThrottleQuotaEnabled =
            QuotaUtil.isExceedThrottleQuotaEnabled(rsServices.getConnection());
      } catch (IOException e) {
        LOG.warn("Unable to read if exceed throttle quota enabled from quota table", e);
      }
    }

    private <K, V extends QuotaState> void fetch(final String type,
        final ConcurrentHashMap<K, V> quotasMap, final Fetcher<K, V> fetcher) {
      long now = EnvironmentEdgeManager.currentTime();
      long refreshPeriod = getPeriod();
      long evictPeriod = refreshPeriod * EVICT_PERIOD_FACTOR;

      // Find the quota entries to update
      List<Get> gets = new ArrayList<>();
      List<K> toRemove = new ArrayList<>();
      for (Map.Entry<K, V> entry: quotasMap.entrySet()) {
        long lastUpdate = entry.getValue().getLastUpdate();
        long lastQuery = entry.getValue().getLastQuery();
        if (lastQuery > 0 && (now - lastQuery) >= evictPeriod) {
          toRemove.add(entry.getKey());
        } else if (TEST_FORCE_REFRESH || (now - lastUpdate) >= refreshPeriod) {
          gets.add(fetcher.makeGet(entry));
        }
      }

      for (final K key: toRemove) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("evict " + type + " key=" + key);
        }
        quotasMap.remove(key);
      }

      // fetch and update the quota entries
      if (!gets.isEmpty()) {
        try {
          for (Map.Entry<K, V> entry: fetcher.fetchEntries(gets).entrySet()) {
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
     * Update quota factors which is used to divide cluster scope quota into machine scope quota
     *
     * For user/namespace/user over namespace quota, use [1 / RSNum] as machine factor.
     * For table/user over table quota, use [1 / TotalTableRegionNum * MachineTableRegionNum]
     * as machine factor.
     */
    private void updateQuotaFactors() {
      // Update machine quota factor
      try {
        int rsSize = rsServices.getConnection().getAdmin()
            .getClusterMetrics(EnumSet.of(Option.SERVERS_NAME)).getServersName().size();
        if (rsSize != 0) {
          // TODO if use rs group, the cluster limit should be shared by the rs group
          machineQuotaFactor = 1.0 / rsSize;
        }
      } catch (IOException e) {
        LOG.warn("Get live region servers failed", e);
      }

      // Update table machine quota factors
      for (TableName tableName : tableQuotaCache.keySet()) {
        double factor = 1;
        try {
          long regionSize =
              MetaTableAccessor.getTableRegions(rsServices.getConnection(), tableName, true)
                  .stream().filter(regionInfo -> !regionInfo.isOffline()).count();
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
      }
    }
  }

  static interface Fetcher<Key, Value> {
    Get makeGet(Map.Entry<Key, Value> entry);
    Map<Key, Value> fetchEntries(List<Get> gets) throws IOException;
  }
}
