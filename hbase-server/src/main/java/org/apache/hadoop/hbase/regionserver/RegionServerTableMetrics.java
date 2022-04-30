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

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Captures operation metrics by table. Separates metrics collection for table metrics away from
 * {@link MetricsRegionServer} for encapsulation and ease of testing.
 */
@InterfaceAudience.Private
public class RegionServerTableMetrics {

  private final MetricsTableLatencies latencies;
  private MetricsTableQueryMeter queryMeter;

  public RegionServerTableMetrics(boolean enableTableQueryMeter) {
    latencies = CompatibilitySingletonFactory.getInstance(MetricsTableLatencies.class);
    if (enableTableQueryMeter) {
      queryMeter = new MetricsTableQueryMeterImpl(MetricRegistries.global().
        get(((MetricsTableLatenciesImpl) latencies).getMetricRegistryInfo()).get());
    }
  }

  public void updatePut(TableName table, long time) {
    latencies.updatePut(table.getNameAsString(), time);
  }

  public void updatePutBatch(TableName table, long time) {
    latencies.updatePutBatch(table.getNameAsString(), time);
  }

  public void updateGet(TableName table, long time) {
    latencies.updateGet(table.getNameAsString(), time);
  }

  public void updateIncrement(TableName table, long time) {
    latencies.updateIncrement(table.getNameAsString(), time);
  }

  public void updateAppend(TableName table, long time) {
    latencies.updateAppend(table.getNameAsString(), time);
  }

  public void updateDelete(TableName table, long time) {
    latencies.updateDelete(table.getNameAsString(), time);
  }

  public void updateDeleteBatch(TableName table, long time) {
    latencies.updateDeleteBatch(table.getNameAsString(), time);
  }

  public void updateCheckAndDelete(TableName table, long time) {
    latencies.updateCheckAndDelete(table.getNameAsString(), time);
  }

  public void updateCheckAndPut(TableName table, long time) {
    latencies.updateCheckAndPut(table.getNameAsString(), time);
  }

  public void updateCheckAndMutate(TableName table, long time) {
    latencies.updateCheckAndMutate(table.getNameAsString(), time);
  }

  public void updateScanTime(TableName table, long time) {
    latencies.updateScanTime(table.getNameAsString(), time);
  }

  public void updateScanSize(TableName table, long size) {
    latencies.updateScanSize(table.getNameAsString(), size);
  }

  public void updateTableReadQueryMeter(TableName table, long count) {
    if (queryMeter != null) {
      queryMeter.updateTableReadQueryMeter(table, count);
    }
  }

  public void updateTableWriteQueryMeter(TableName table, long count) {
    if (queryMeter != null) {
      queryMeter.updateTableWriteQueryMeter(table, count);
    }
  }

  public void updateTableWriteQueryMeter(TableName table) {
    if (queryMeter != null) {
      queryMeter.updateTableWriteQueryMeter(table);
    }
  }
}
