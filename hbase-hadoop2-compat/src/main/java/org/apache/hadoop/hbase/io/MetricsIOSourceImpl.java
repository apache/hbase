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

package org.apache.hadoop.hbase.io;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsIOSourceImpl extends BaseSourceImpl implements MetricsIOSource {

  private final MetricsIOWrapper wrapper;

  private final MetricHistogram fsReadTimeHisto;
  private final MetricHistogram fsPReadTimeHisto;
  private final MetricHistogram fsWriteTimeHisto;

  public MetricsIOSourceImpl(MetricsIOWrapper wrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, wrapper);
  }

  public MetricsIOSourceImpl(String metricsName,
      String metricsDescription,
      String metricsContext,
      String metricsJmxContext,
      MetricsIOWrapper wrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    this.wrapper = wrapper;

    fsReadTimeHisto = getMetricsRegistry()
        .newTimeHistogram(FS_READ_TIME_HISTO_KEY, FS_READ_TIME_HISTO_DESC);
    fsPReadTimeHisto = getMetricsRegistry()
        .newTimeHistogram(FS_PREAD_TIME_HISTO_KEY, FS_PREAD_TIME_HISTO_DESC);
    fsWriteTimeHisto = getMetricsRegistry()
        .newTimeHistogram(FS_WRITE_HISTO_KEY, FS_WRITE_TIME_HISTO_DESC);
  }

  @Override
  public void updateFsReadTime(long t) {
    fsReadTimeHisto.add(t);
  };

  @Override
  public void updateFsPReadTime(long t) {
    fsPReadTimeHisto.add(t);
  };

  @Override
  public void updateFsWriteTime(long t) {
    fsWriteTimeHisto.add(t);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);

    // wrapper can be null because this function is called inside of init.
    if (wrapper != null) {
      mrb.addCounter(Interns.info(CHECKSUM_FAILURES_KEY, CHECKSUM_FAILURES_DESC),
        wrapper.getChecksumFailures());
    }

    metricsRegistry.snapshot(mrb, all);
  }

}
