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
import java.util.Objects;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
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
  private static ManualEnvironmentEdge envEdge = new ManualEnvironmentEdge();
  private final static int REFRESH_TIME = 30 * 60000;
  static {
    envEdge.setValue(EnvironmentEdgeManager.currentTime());
    EnvironmentEdgeManagerTestHelper.injectEdge(envEdge);
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

      quotaCache.triggerCacheRefresh();
      // sleep for cache update
      Thread.sleep(250);

      for (TableName table : tables) {
        quotaCache.getTableLimiter(table);
      }

      boolean isUpdated = false;
      while (!isUpdated) {
        quotaCache.triggerCacheRefresh();
        isUpdated = true;
        for (TableName table : tables) {
          boolean isBypass = true;
          if (userLimiter) {
            isBypass = quotaCache.getUserLimiter(User.getCurrent().getUGI(), table).isBypass();
          }
          if (tableLimiter) {
            isBypass &= quotaCache.getTableLimiter(table).isBypass();
          }
          if (nsLimiter) {
            isBypass &= quotaCache.getNamespaceLimiter(table.getNamespaceAsString()).isBypass();
          }
          if (isBypass != bypass) {
            envEdge.incValue(100);
            isUpdated = false;
            break;
          }
        }
        if (rsLimiter) {
          boolean rsIsBypass = quotaCache
              .getRegionServerQuotaLimiter(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY).isBypass();
          if (rsIsBypass != bypass) {
            envEdge.incValue(100);
            isUpdated = false;
          }
        }
        if (exceedThrottleQuota) {
          if (quotaCache.isExceedThrottleQuotaEnabled() != bypass) {
            envEdge.incValue(100);
            isUpdated = false;
          }
        }
      }

      LOG.debug("QuotaCache");
      LOG.debug(Objects.toString(quotaCache.getNamespaceQuotaCache()));
      LOG.debug(Objects.toString(quotaCache.getTableQuotaCache()));
      LOG.debug(Objects.toString(quotaCache.getUserQuotaCache()));
      LOG.debug(Objects.toString(quotaCache.getRegionServerQuotaCache()));
    }
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
