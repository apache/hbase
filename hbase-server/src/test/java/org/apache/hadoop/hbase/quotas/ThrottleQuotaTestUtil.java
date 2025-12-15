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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class ThrottleQuotaTestUtil {

  private final static Logger LOG = LoggerFactory.getLogger(ThrottleQuotaTestUtil.class);
  static ManualEnvironmentEdge envEdge = new ManualEnvironmentEdge();
  private final static int REFRESH_TIME = 30 * 60000;
  static {
    envEdge.setValue(EnvironmentEdgeManager.currentTime());
    // only active the envEdge for quotas package
    EnvironmentEdgeManagerTestHelper.injectEdgeForPackage(envEdge,
      ThrottleQuotaTestUtil.class.getPackage().getName());
  }

  private ThrottleQuotaTestUtil() {
    // Hide utility class constructor
    LOG.debug("Call constructor of ThrottleQuotaTestUtil");
  }

  static int doPuts(int maxOps, byte[] family, byte[] qualifier, final Table... tables) {
    return doPuts(maxOps, -1, family, qualifier, tables);
  }

  static int doPuts(int maxOps, int valueSize, byte[] family, byte[] qualifier,
    final Table... tables) {
    int count = 0;
    try {
      while (count < maxOps) {
        Put put = new Put(Bytes.toBytes("row-" + count));
        byte[] value;
        if (valueSize < 0) {
          value = Bytes.toBytes("data-" + count);
        } else {
          value = generateValue(valueSize);
        }
        put.addColumn(family, qualifier, value);
        for (final Table table : tables) {
          table.put(put);
        }
        count += tables.length;
      }
    } catch (IOException e) {
      LOG.error("put failed after nRetries=" + count, e);
    }
    return count;
  }

  private static byte[] generateValue(int valueSize) {
    byte[] bytes = new byte[valueSize];
    for (int i = 0; i < valueSize; i++) {
      bytes[i] = 'a';
    }
    return bytes;
  }

  static long doGets(int maxOps, final Table... tables) {
    int count = 0;
    try {
      while (count < maxOps) {
        Get get = new Get(Bytes.toBytes("row-" + count));
        for (final Table table : tables) {
          table.get(get);
        }
        count += tables.length;
      }
    } catch (IOException e) {
      LOG.error("get failed after nRetries=" + count, e);
    }
    return count;
  }

  static long doGets(int maxOps, byte[] family, byte[] qualifier, final Table... tables) {
    int count = 0;
    try {
      while (count < maxOps) {
        Get get = new Get(Bytes.toBytes("row-" + count));
        get.addColumn(family, qualifier);
        for (final Table table : tables) {
          table.get(get);
        }
        count += tables.length;
      }
    } catch (IOException e) {
      LOG.error("get failed after nRetries=" + count, e);
    }
    return count;
  }

  static long doIncrements(int maxOps, byte[] family, byte[] qualifier, final Table... tables) {
    int count = 0;
    try {
      while (count < maxOps) {
        Increment inc = new Increment(Bytes.toBytes("row-" + count));
        inc.addColumn(family, qualifier, 1L);
        for (final Table table : tables) {
          table.increment(inc);
        }
        count += tables.length;
      }
    } catch (IOException e) {
      LOG.error("increment failed after nRetries=" + count, e);
    }
    return count;
  }

  static long doMultiGets(int maxOps, int batchSize, int rowCount, byte[] family, byte[] qualifier,
    final Table... tables) {
    int opCount = 0;
    Random random = new Random();
    try {
      while (opCount < maxOps) {
        List<Get> gets = new ArrayList<>(batchSize);
        while (gets.size() < batchSize) {
          Get get = new Get(Bytes.toBytes("row-" + random.nextInt(rowCount)));
          get.addColumn(family, qualifier);
          gets.add(get);
        }
        for (final Table table : tables) {
          table.get(gets);
        }
        opCount += tables.length;
      }
    } catch (IOException e) {
      LOG.error("multiget failed after nRetries=" + opCount, e);
    }
    return opCount;
  }

  static long doScans(int desiredRows, Table table, int caching) {
    int count = 0;
    try {
      Scan scan = new Scan();
      scan.setCaching(caching);
      scan.setCacheBlocks(false);
      ResultScanner scanner = table.getScanner(scan);
      while (count < desiredRows) {
        scanner.next();
        count += 1;
      }
    } catch (IOException e) {
      LOG.error("scan failed after nRetries=" + count, e);
    }
    return count;
  }

  static void triggerUserCacheRefresh(HBaseTestingUtility testUtil, boolean bypass,
    TableName... tables) throws Exception {
    triggerCacheRefresh(testUtil, bypass, true, false, false, false, false, tables);
  }

  static void triggerTableCacheRefresh(HBaseTestingUtility testUtil, boolean bypass,
    TableName... tables) throws Exception {
    triggerCacheRefresh(testUtil, bypass, false, true, false, false, false, tables);
  }

  static void triggerNamespaceCacheRefresh(HBaseTestingUtility testUtil, boolean bypass,
    TableName... tables) throws Exception {
    triggerCacheRefresh(testUtil, bypass, false, false, true, false, false, tables);
  }

  static void triggerRegionServerCacheRefresh(HBaseTestingUtility testUtil, boolean bypass)
    throws Exception {
    triggerCacheRefresh(testUtil, bypass, false, false, false, true, false);
  }

  static void triggerExceedThrottleQuotaCacheRefresh(HBaseTestingUtility testUtil,
    boolean exceedEnabled) throws Exception {
    triggerCacheRefresh(testUtil, exceedEnabled, false, false, false, false, true);
  }

  private static void triggerCacheRefresh(HBaseTestingUtility testUtil, boolean bypass,
    boolean userLimiter, boolean tableLimiter, boolean nsLimiter, boolean rsLimiter,
    boolean exceedThrottleQuota, final TableName... tables) throws Exception {
    envEdge.incValue(2 * REFRESH_TIME);
    for (RegionServerThread rst : testUtil.getMiniHBaseCluster().getRegionServerThreads()) {
      RegionServerRpcQuotaManager quotaManager =
        rst.getRegionServer().getRegionServerRpcQuotaManager();
      QuotaCache quotaCache = quotaManager.getQuotaCache();
      quotaCache.forceSynchronousCacheRefresh();
      Thread.sleep(250);
      testUtil.waitFor(60000, 250, new ExplainingPredicate<Exception>() {

        @Override
        public boolean evaluate() throws Exception {
          boolean isUpdated = true;
          for (TableName table : tables) {
            if (userLimiter) {
              boolean isUserBypass =
                quotaCache.getUserLimiter(User.getCurrent().getUGI(), table).isBypass();
              if (isUserBypass != bypass) {
                LOG.info(
                  "User limiter for user={}, table={} not refreshed, bypass expected {}, actual {}",
                  User.getCurrent(), table, bypass, isUserBypass);
                envEdge.incValue(100);
                isUpdated = false;
                break;
              }
            }
            if (tableLimiter) {
              boolean isTableBypass = quotaCache.getTableLimiter(table).isBypass();
              if (isTableBypass != bypass) {
                LOG.info("Table limiter for table={} not refreshed, bypass expected {}, actual {}",
                  table, bypass, isTableBypass);
                envEdge.incValue(100);
                isUpdated = false;
                break;
              }
            }
            if (nsLimiter) {
              boolean isNsBypass =
                quotaCache.getNamespaceLimiter(table.getNamespaceAsString()).isBypass();
              if (isNsBypass != bypass) {
                LOG.info(
                  "Namespace limiter for namespace={} not refreshed, bypass expected {}, actual {}",
                  table.getNamespaceAsString(), bypass, isNsBypass);
                envEdge.incValue(100);
                isUpdated = false;
                break;
              }
            }
          }
          if (rsLimiter) {
            boolean rsIsBypass = quotaCache
              .getRegionServerQuotaLimiter(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY).isBypass();
            if (rsIsBypass != bypass) {
              LOG.info("RegionServer limiter not refreshed, bypass expected {}, actual {}", bypass,
                rsIsBypass);
              envEdge.incValue(100);
              isUpdated = false;
            }
          }
          if (exceedThrottleQuota) {
            if (quotaCache.isExceedThrottleQuotaEnabled() != bypass) {
              LOG.info("ExceedThrottleQuotaEnabled not refreshed, bypass expected {}, actual {}",
                bypass, quotaCache.isExceedThrottleQuotaEnabled());
              envEdge.incValue(100);
              isUpdated = false;
            }
          }
          if (isUpdated) {
            return true;
          }
          quotaCache.triggerCacheRefresh();
          return false;
        }

        @Override
        public String explainFailure() throws Exception {
          return "Quota cache is still not refreshed";
        }
      });

      LOG.debug("QuotaCache");
      LOG.debug(Objects.toString(quotaCache.getNamespaceQuotaCache()));
      LOG.debug(Objects.toString(quotaCache.getTableQuotaCache()));
      LOG.debug(Objects.toString(quotaCache.getUserQuotaCache()));
      LOG.debug(Objects.toString(quotaCache.getRegionServerQuotaCache()));
    }
  }

  static Set<QuotaCache> getQuotaCaches(HBaseTestingUtility testUtil) {
    Set<QuotaCache> quotaCaches = new HashSet<>();
    for (RegionServerThread rst : testUtil.getMiniHBaseCluster().getRegionServerThreads()) {
      RegionServerRpcQuotaManager quotaManager =
        rst.getRegionServer().getRegionServerRpcQuotaManager();
      quotaCaches.add(quotaManager.getQuotaCache());
    }
    return quotaCaches;
  }

  static void waitMinuteQuota() {
    envEdge.incValue(70000);
  }

  static void clearQuotaCache(HBaseTestingUtility testUtil) {
    for (RegionServerThread rst : testUtil.getMiniHBaseCluster().getRegionServerThreads()) {
      RegionServerRpcQuotaManager quotaManager =
        rst.getRegionServer().getRegionServerRpcQuotaManager();
      QuotaCache quotaCache = quotaManager.getQuotaCache();
      quotaCache.getNamespaceQuotaCache().clear();
      quotaCache.getTableQuotaCache().clear();
      quotaCache.getUserQuotaCache().clear();
      quotaCache.getRegionServerQuotaCache().clear();
    }
  }
}
