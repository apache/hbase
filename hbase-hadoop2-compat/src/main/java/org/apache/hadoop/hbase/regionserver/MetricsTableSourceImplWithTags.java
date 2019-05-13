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

import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.lib.MutableSizeHistogram;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;

import org.apache.yetus.audience.InterfaceAudience;

/**
 *  MetricsTableSource implementation that uses metrics tags to output table name, rather than
 *  writing it as a part of the metric name. Note that there are 3 ways to write table metrics in
 *  HBase currently - MetricsTableSource+registry, MetricsTableSource.snapshot, as well as a
 *  separate class for latencies. This handles two of them, being MetricsTableSource.
 */
@InterfaceAudience.Private
public class MetricsTableSourceImplWithTags extends MetricsTableSourceImpl {
  public final static MetricsInfo TABLE_TAG_INFO = Interns.info("HBaseTable", "");
  private final String tableNameStr;
  private final MetricsTag metricsTag;

  public MetricsTableSourceImplWithTags(String tblName,
      MetricsTableAggregateSourceImpl aggregate, MetricsTableWrapperAggregate tblWrapperAgg) {
    super(tblName, aggregate, tblWrapperAgg);
    this.tableNameStr = getTableName();
    this.metricsTag = new MetricsTag(TABLE_TAG_INFO, tableNameStr);
  }

  @Override
  protected MutableHistogram newHistogram(String name, String desc) {
    return registry.newScopedHistogram(tableNameStr, name, desc);
  }

  @Override
  protected MutableFastCounter newCounter(String name, String desc) {
    return registry.newScopedCounter(tableNameStr, name, desc, 0L);
  }

  @Override
  protected MutableSizeHistogram newSizeHisto(String name, String desc) {
    return registry.newScopedSizeHistogram(tableNameStr, name, desc);
  }

  @Override
  protected MutableTimeHistogram newTimeHisto(String name, String desc) {
    return registry.newScopedTimeHistogram(tableNameStr, name, desc);
  }

  @Override
  protected void removeNonHistogram(String name) {
    registry.removeScopedMetric(tableNameStr, name);
  }

  @Override
  protected void removeHistogram(String name) {
    registry.removeScopedHistogramMetrics(tableNameStr, name);
  }

  @Override
  protected MetricsInfo createMetricsInfo(String name, String desc) {
    return Interns.info(name, desc);
  }

  @Override
  public String getScope() {
    return tableNameStr;
  }

  @Override
  protected void addTags(MetricsRecordBuilder mrb) {
    mrb.add(metricsTag);
  }
}
