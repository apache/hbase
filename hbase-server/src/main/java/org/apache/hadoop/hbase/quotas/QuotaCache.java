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

package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;

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
  private static final Log LOG = LogFactory.getLog(QuotaCache.class);

  public static final String REFRESH_CONF_KEY = "hbase.quota.refresh.period";
  private static final int REFRESH_DEFAULT_PERIOD = 5 * 60000; // 5min
  private static final int EVICT_PERIOD_FACTOR = 5; // N * REFRESH_DEFAULT_PERIOD

  // for testing purpose only, enforce the cache to be always refreshed
  static boolean TEST_FORCE_REFRESH = false;

  private final ConcurrentHashMap<String, QuotaState> namespaceQuotaCache =
      new ConcurrentHashMap<String, QuotaState>();
  private final ConcurrentHashMap<TableName, QuotaState> tableQuotaCache =
      new ConcurrentHashMap<TableName, QuotaState>();
  private final ConcurrentHashMap<String, UserQuotaState> userQuotaCache =
      new ConcurrentHashMap<String, UserQuotaState>();
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
    Threads.setDaemonThreadRunning(refreshChore.getThread());
  }

  @Override
  public void stop(final String why) {
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
   *
   * @param ugi the user
   * @return the quota info associated to specified user
   */
  public UserQuotaState getUserQuotaState(final UserGroupInformation ugi) {
    String key = ugi.getShortUserName();
    UserQuotaState quotaInfo = userQuotaCache.get(key);
    if (quotaInfo == null) {
      quotaInfo = new UserQuotaState();
      if (userQuotaCache.putIfAbsent(key, quotaInfo) == null) {
        triggerCacheRefresh();
      }
    }
    return quotaInfo;
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
   * Returns the QuotaState requested.
   * If the quota info is not in cache an empty one will be returned
   * and the quota request will be enqueued for the next cache refresh.
   */
  private <K> QuotaState getQuotaState(final ConcurrentHashMap<K, QuotaState> quotasMap,
      final K key) {
    QuotaState quotaInfo = quotasMap.get(key);
    if (quotaInfo == null) {
      quotaInfo = new QuotaState();
      if (quotasMap.putIfAbsent(key, quotaInfo) == null) {
        triggerCacheRefresh();
      }
    }
    return quotaInfo;
  }

  private Configuration getConfiguration() {
    return rsServices.getConfiguration();
  }

  @VisibleForTesting
  void triggerCacheRefresh() {
    refreshChore.triggerNow();
  }

  @VisibleForTesting
  long getLastUpdate() {
    return refreshChore.lastUpdate;
  }

  @VisibleForTesting
  Map<String, QuotaState> getNamespaceQuotaCache() {
    return namespaceQuotaCache;
  }

  @VisibleForTesting
  Map<TableName, QuotaState> getTableQuotaCache() {
    return tableQuotaCache;
  }

  @VisibleForTesting
  Map<String, UserQuotaState> getUserQuotaCache() {
    return userQuotaCache;
  }

  // TODO: Remove this once we have the notification bus
  private class QuotaRefresherChore extends Chore {
    private long lastUpdate = 0;

    public QuotaRefresherChore(final int period, final Stoppable stoppable) {
      super("QuotaRefresherChore", period, stoppable);
    }

    @Override
    protected void chore() {
      // Prefetch online tables/namespaces
      for (TableName table: QuotaCache.this.rsServices.getOnlineTables()) {
        if (table.isSystemTable()) continue;
        if (!QuotaCache.this.tableQuotaCache.contains(table)) {
          QuotaCache.this.tableQuotaCache.putIfAbsent(table, new QuotaState());
        }
        String ns = table.getNamespaceAsString();
        if (!QuotaCache.this.namespaceQuotaCache.contains(ns)) {
          QuotaCache.this.namespaceQuotaCache.putIfAbsent(ns, new QuotaState());
        }
      }

      fetchNamespaceQuotaState();
      fetchTableQuotaState();
      fetchUserQuotaState();
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
          return QuotaUtil.fetchNamespaceQuotas(QuotaCache.this.getConfiguration(), gets);
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
          return QuotaUtil.fetchTableQuotas(QuotaCache.this.getConfiguration(), gets);
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
          return QuotaUtil.fetchUserQuotas(QuotaCache.this.getConfiguration(), gets);
        }
      });
    }

    private <K, V extends QuotaState> void fetch(final String type,
        final ConcurrentHashMap<K, V> quotasMap, final Fetcher<K, V> fetcher) {
      long now = EnvironmentEdgeManager.currentTime();
      long refreshPeriod = getPeriod();
      long evictPeriod = refreshPeriod * EVICT_PERIOD_FACTOR;

      // Find the quota entries to update
      List<Get> gets = new ArrayList<Get>();
      List<K> toRemove = new ArrayList<K>();
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
  }

  static interface Fetcher<Key, Value> {
    Get makeGet(Map.Entry<Key, Value> entry);
    Map<Key, Value> fetchEntries(List<Get> gets) throws IOException;
  }
}
