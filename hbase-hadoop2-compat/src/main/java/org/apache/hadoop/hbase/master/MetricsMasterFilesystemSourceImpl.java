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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;

@InterfaceAudience.Private
public class MetricsMasterFilesystemSourceImpl
    extends BaseSourceImpl
    implements MetricsMasterFileSystemSource {

  private MetricHistogram splitSizeHisto;
  private MetricHistogram splitTimeHisto;
  private MetricHistogram metaSplitTimeHisto;
  private MetricHistogram metaSplitSizeHisto;

  public MetricsMasterFilesystemSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsMasterFilesystemSourceImpl(String metricsName,
                                           String metricsDescription,
                                           String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void init() {
    splitSizeHisto = metricsRegistry.newSizeHistogram(SPLIT_SIZE_NAME, SPLIT_SIZE_DESC);
    splitTimeHisto = metricsRegistry.newTimeHistogram(SPLIT_TIME_NAME, SPLIT_TIME_DESC);
    metaSplitTimeHisto =
        metricsRegistry.newTimeHistogram(META_SPLIT_TIME_NAME, META_SPLIT_TIME_DESC);
    metaSplitSizeHisto =
        metricsRegistry.newSizeHistogram(META_SPLIT_SIZE_NAME, META_SPLIT_SIZE_DESC);
  }

  @Override
  public void updateSplitTime(long time) {
    splitTimeHisto.add(time);
  }

  @Override
  public void updateSplitSize(long size) {
    splitSizeHisto.add(size);
  }


  @Override
  public void updateMetaWALSplitTime(long time) {
    metaSplitTimeHisto.add(time);
  }

  @Override
  public void updateMetaWALSplitSize(long size) {
    metaSplitSizeHisto.add(size);
  }
}
