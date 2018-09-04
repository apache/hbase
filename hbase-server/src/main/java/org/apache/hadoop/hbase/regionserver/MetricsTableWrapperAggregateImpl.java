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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsExecutor;

import com.google.common.collect.Sets;

@InterfaceAudience.Private
public class MetricsTableWrapperAggregateImpl implements MetricsTableWrapperAggregate, Closeable {
  private final HRegionServer regionServer;
  private ScheduledExecutorService executor;
  private Runnable runnable;
  private long period;
  private ScheduledFuture<?> tableMetricsUpdateTask;
  private ConcurrentHashMap<TableName, MetricsTableValues> metricsTableMap = new ConcurrentHashMap<>();

  public MetricsTableWrapperAggregateImpl(final HRegionServer regionServer) {
    this.regionServer = regionServer;
    this.period = regionServer.conf.getLong(HConstants.REGIONSERVER_METRICS_PERIOD,
      HConstants.DEFAULT_REGIONSERVER_METRICS_PERIOD) + 1000;
    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new TableMetricsWrapperRunnable();
    this.tableMetricsUpdateTask = this.executor.scheduleWithFixedDelay(this.runnable, period, this.period,
      TimeUnit.MILLISECONDS);
  }

  public class TableMetricsWrapperRunnable implements Runnable {

    @Override
    public void run() {
      Map<TableName, MetricsTableValues> localMetricsTableMap = new HashMap<>();

      for (Region r : regionServer.getOnlineRegionsLocalContext()) {
        TableName tbl= r.getTableDesc().getTableName();
        MetricsTableValues metricsTable = localMetricsTableMap.get(tbl);
        if (metricsTable == null) {
          metricsTable = new MetricsTableValues();
          localMetricsTableMap.put(tbl, metricsTable);
        }
        long tempStorefilesSize = 0;
        for (Store store : r.getStores()) {
          tempStorefilesSize += store.getStorefilesSize();
        }
        metricsTable.setMemstoresSize(metricsTable.getMemstoresSize() + r.getMemstoreSize());
        metricsTable.setStoreFilesSize(metricsTable.getStoreFilesSize() + tempStorefilesSize);
        metricsTable.setTableSize(metricsTable.getMemstoresSize() + metricsTable.getStoreFilesSize());
        metricsTable.setReadRequestsCount(metricsTable.getReadRequestsCount() + r.getReadRequestsCount());
        metricsTable.setWriteRequestsCount(metricsTable.getWriteRequestsCount() + r.getWriteRequestsCount());
        metricsTable.setTotalRequestsCount(metricsTable.getReadRequestsCount() + metricsTable.getWriteRequestsCount());
      }

      for(Map.Entry<TableName, MetricsTableValues> entry : localMetricsTableMap.entrySet()) {
        TableName tbl = entry.getKey();
        if (metricsTableMap.get(tbl) == null) {
          MetricsTableSource tableSource = CompatibilitySingletonFactory
              .getInstance(MetricsRegionServerSourceFactory.class).createTable(tbl.getNameAsString(),
                MetricsTableWrapperAggregateImpl.this);
          CompatibilitySingletonFactory
          .getInstance(MetricsRegionServerSourceFactory.class).getTableAggregate()
          .register(tbl.getNameAsString(), tableSource);
        }
        metricsTableMap.put(entry.getKey(), entry.getValue());
      }
      Set<TableName> existingTableNames = Sets.newHashSet(metricsTableMap.keySet());
      existingTableNames.removeAll(localMetricsTableMap.keySet());
      MetricsTableAggregateSource agg = CompatibilitySingletonFactory
          .getInstance(MetricsRegionServerSourceFactory.class).getTableAggregate();
      for (TableName table : existingTableNames) {
        agg.deregister(table.getNameAsString());
        if (metricsTableMap.get(table) != null) {
          metricsTableMap.remove(table);
        }
      }
    }
  }

  @Override
  public long getReadRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getReadRequestsCount();
  }

  @Override
  public long getWriteRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getWriteRequestsCount();
  }

  @Override
  public long getTotalRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getTotalRequestsCount();
  }

  @Override
  public long getMemstoresSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getMemstoresSize();
  }

  @Override
  public long getStoreFilesSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getStoreFilesSize();
  }

  @Override
  public long getTableSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getTableSize();
  }

  @Override
  public void close() throws IOException {
    tableMetricsUpdateTask.cancel(true);
  }

  private static class MetricsTableValues {

    private long totalRequestsCount;
    private long readRequestsCount;
    private long writeRequestsCount;
    private long memstoresSize;
    private long storeFilesSize;
    private long tableSize;

    public long getTotalRequestsCount() {
      return totalRequestsCount;
    }

    public void setTotalRequestsCount(long totalRequestsCount) {
      this.totalRequestsCount = totalRequestsCount;
    }

    public long getReadRequestsCount() {
      return readRequestsCount;
    }

    public void setReadRequestsCount(long readRequestsCount) {
      this.readRequestsCount = readRequestsCount;
    }

    public long getWriteRequestsCount() {
      return writeRequestsCount;
    }

    public void setWriteRequestsCount(long writeRequestsCount) {
      this.writeRequestsCount = writeRequestsCount;
    }

    public long getMemstoresSize() {
      return memstoresSize;
    }

    public void setMemstoresSize(long memstoresSize) {
      this.memstoresSize = memstoresSize;
    }

    public long getStoreFilesSize() {
      return storeFilesSize;
    }

    public void setStoreFilesSize(long storeFilesSize) {
      this.storeFilesSize = storeFilesSize;
    }

    public long getTableSize() {
      return tableSize;
    }

    public void setTableSize(long tableSize) {
      this.tableSize = tableSize;
    }
  }

}
