/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation of {@link MetricsTableQueryMeter} to track query per second for each table in
 * a RegionServer.
 */
@InterfaceAudience.Private
public class MetricsTableQueryMeterImpl implements MetricsTableQueryMeter {
  private final Map<TableName, TableMeters> metersByTable = new ConcurrentHashMap<>();
  private final MetricRegistry metricRegistry;

  public MetricsTableQueryMeterImpl(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  private static class TableMeters {
    final Meter tableReadQueryMeter;
    final Meter tableWriteQueryMeter;

    TableMeters(MetricRegistry metricRegistry, TableName tableName) {
      this.tableReadQueryMeter = metricRegistry.meter(qualifyMetricsName(tableName,
        TABLE_READ_QUERY_PER_SECOND));
      this.tableWriteQueryMeter =
        metricRegistry.meter(qualifyMetricsName(tableName, TABLE_WRITE_QUERY_PER_SECOND));
    }

    public void updateTableReadQueryMeter(long count) {
      tableReadQueryMeter.mark(count);
    }

    public void updateTableReadQueryMeter() {
      tableReadQueryMeter.mark();
    }

    public void updateTableWriteQueryMeter(long count) {
      tableWriteQueryMeter.mark(count);
    }

    public void updateTableWriteQueryMeter() {
      tableWriteQueryMeter.mark();
    }
  }

  private static String qualifyMetricsName(TableName tableName, String metric) {
    StringBuilder sb = new StringBuilder();
    sb.append("Namespace_").append(tableName.getNamespaceAsString());
    sb.append("_table_").append(tableName.getQualifierAsString());
    sb.append("_metric_").append(metric);
    return sb.toString();
  }

  private TableMeters getOrCreateTableMeter(TableName tableName) {
    return metersByTable.computeIfAbsent(tableName, tbn -> new TableMeters(metricRegistry, tbn));
  }

  @Override
  public void updateTableReadQueryMeter(TableName tableName, long count) {
    getOrCreateTableMeter(tableName).updateTableReadQueryMeter(count);
  }

  @Override
  public void updateTableReadQueryMeter(TableName tableName) {
    getOrCreateTableMeter(tableName).updateTableReadQueryMeter();
  }

  @Override
  public void updateTableWriteQueryMeter(TableName tableName, long count) {
    getOrCreateTableMeter(tableName).updateTableWriteQueryMeter(count);
  }

  @Override
  public void updateTableWriteQueryMeter(TableName tableName) {
    getOrCreateTableMeter(tableName).updateTableWriteQueryMeter();
  }
}
