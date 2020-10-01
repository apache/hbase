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

package org.apache.hadoop.hbase.regionserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsExecutor;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@InterfaceAudience.Private
public class MetricsTableWrapperAggregateImpl implements MetricsTableWrapperAggregate, Closeable {
  private final HRegionServer regionServer;
  private ScheduledExecutorService executor;
  private Runnable runnable;
  private long period;
  private ScheduledFuture<?> tableMetricsUpdateTask;
  private ConcurrentHashMap<TableName, MetricsTableValues> metricsTableMap
    = new ConcurrentHashMap<>();

  public MetricsTableWrapperAggregateImpl(final HRegionServer regionServer) {
    this.regionServer = regionServer;
    this.period = regionServer.getConfiguration().getLong(HConstants.REGIONSERVER_METRICS_PERIOD,
      HConstants.DEFAULT_REGIONSERVER_METRICS_PERIOD) + 1000;
    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new TableMetricsWrapperRunnable();
    this.tableMetricsUpdateTask = this.executor.scheduleWithFixedDelay(this.runnable, period,
      period, TimeUnit.MILLISECONDS);
  }

  public class TableMetricsWrapperRunnable implements Runnable {

    @Override
    public void run() {
      Map<TableName, MetricsTableValues> localMetricsTableMap = new HashMap<>();
      for (Region r : regionServer.getOnlineRegionsLocalContext()) {
        TableName tbl = r.getTableDescriptor().getTableName();
        MetricsTableValues mt = localMetricsTableMap.get(tbl);
        if (mt == null) {
          mt = new MetricsTableValues();
          localMetricsTableMap.put(tbl, mt);
        }
        long memstoreReadCount = 0L;
        long mixedReadCount = 0L;
        String tempKey = null;
        if (r.getStores() != null) {
          String familyName = null;
          for (Store store : r.getStores()) {
            familyName = store.getColumnFamilyName();

            mt.storeFileCount += store.getStorefilesCount();
            mt.memstoreSize += (store.getMemStoreSize().getDataSize()
                + store.getMemStoreSize().getHeapSize() + store.getMemStoreSize().getOffHeapSize());
            mt.storeFileSize += store.getStorefilesSize();
            mt.referenceFileCount += store.getNumReferenceFiles();
            if (store.getMaxStoreFileAge().isPresent()) {
              mt.maxStoreFileAge =
                  Math.max(mt.maxStoreFileAge, store.getMaxStoreFileAge().getAsLong());
            }
            if (store.getMinStoreFileAge().isPresent()) {
              mt.minStoreFileAge =
                  Math.min(mt.minStoreFileAge, store.getMinStoreFileAge().getAsLong());
            }
            if (store.getAvgStoreFileAge().isPresent()) {
              mt.totalStoreFileAge =
                  (long) store.getAvgStoreFileAge().getAsDouble() * store.getStorefilesCount();
            }
            mt.storeCount += 1;
            tempKey = tbl.getNameAsString() + HASH + familyName;
            Long tempVal = mt.perStoreMemstoreOnlyReadCount.get(tempKey);
            if (tempVal == null) {
              tempVal = 0L;
            }
            memstoreReadCount = store.getMemstoreOnlyRowReadsCount() + tempVal;
            tempVal = mt.perStoreMixedReadCount.get(tempKey);
            if (tempVal == null) {
              tempVal = 0L;
            }
            mixedReadCount = store.getMixedRowReadsCount() + tempVal;
            // accumulate the count
            mt.perStoreMemstoreOnlyReadCount.put(tempKey, memstoreReadCount);
            mt.perStoreMixedReadCount.put(tempKey, mixedReadCount);
          }

          mt.regionCount += 1;

          mt.readRequestCount += r.getReadRequestsCount();
          mt.filteredReadRequestCount += r.getFilteredReadRequestsCount();
          mt.writeRequestCount += r.getWriteRequestsCount();
        }
      }

      for (Map.Entry<TableName, MetricsTableValues> entry : localMetricsTableMap.entrySet()) {
        TableName tbl = entry.getKey();
        if (metricsTableMap.get(tbl) == null) {
          // this will add the Wrapper to the list of TableMetrics
          CompatibilitySingletonFactory
              .getInstance(MetricsRegionServerSourceFactory.class)
              .getTableAggregate()
              .getOrCreateTableSource(tbl.getNameAsString(), MetricsTableWrapperAggregateImpl.this);
        }
        metricsTableMap.put(entry.getKey(), entry.getValue());
      }
      Set<TableName> existingTableNames = Sets.newHashSet(metricsTableMap.keySet());
      existingTableNames.removeAll(localMetricsTableMap.keySet());
      MetricsTableAggregateSource agg = CompatibilitySingletonFactory
          .getInstance(MetricsRegionServerSourceFactory.class).getTableAggregate();
      for (TableName table : existingTableNames) {
        agg.deleteTableSource(table.getNameAsString());
        if (metricsTableMap.get(table) != null) {
          metricsTableMap.remove(table);
        }
      }
    }
  }

  @Override
  public long getReadRequestCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.readRequestCount;
    }
  }

  @Override
  public Map<String, Long> getMemstoreOnlyRowReadsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return null;
    } else {
      return metricsTable.perStoreMemstoreOnlyReadCount;
    }
  }

  @Override
  public Map<String, Long> getMixedRowReadsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return null;
    } else {
      return metricsTable.perStoreMixedReadCount;
    }
  }

  public long getCpRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.cpRequestCount;
    }
  }

  public long getFilteredReadRequestCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.filteredReadRequestCount;
  }

  @Override
  public long getWriteRequestCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.writeRequestCount;
    }
  }

  @Override
  public long getTotalRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.readRequestCount + metricsTable.writeRequestCount;
    }
  }

  @Override
  public long getMemStoreSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.memstoreSize;
    }
  }

  @Override
  public long getStoreFileSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.storeFileSize;
    }
  }

  @Override
  public long getTableSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.memstoreSize + metricsTable.storeFileSize;
    }
  }

  public long getNumRegions(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.regionCount;
  }

  @Override
  public long getNumStores(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.storeCount;
  }

  @Override
  public long getNumStoreFiles(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.storeFileCount;
  }

  @Override
  public long getMaxStoreFileAge(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.maxStoreFileAge;
  }

  @Override
  public long getMinStoreFileAge(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.minStoreFileAge == Long.MAX_VALUE ? 0 : metricsTable.minStoreFileAge;
  }

  @Override
  public long getAvgStoreFileAge(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }

    return metricsTable.storeFileCount == 0
        ? 0
        : (metricsTable.totalStoreFileAge / metricsTable.storeFileCount);
  }

  @Override
  public long getNumReferenceFiles(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.referenceFileCount;
  }

  @Override
  public long getAvgRegionSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.regionCount == 0
        ? 0
        : (metricsTable.memstoreSize + metricsTable.storeFileSize) / metricsTable.regionCount;
  }

  public long getCpRequestCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.cpRequestCount;
  }

  @Override
  public void close() throws IOException {
    tableMetricsUpdateTask.cancel(true);
  }

  private static class MetricsTableValues {
    long readRequestCount;
    long filteredReadRequestCount;
    long writeRequestCount;
    long memstoreSize;
    long regionCount;
    long storeCount;
    long storeFileCount;
    long storeFileSize;
    long maxStoreFileAge;
    long minStoreFileAge = Long.MAX_VALUE;
    long totalStoreFileAge;
    long referenceFileCount;
    long cpRequestCount;
    Map<String, Long> perStoreMemstoreOnlyReadCount = new HashMap<>();
    Map<String, Long> perStoreMixedReadCount = new HashMap<>();
  }

}
