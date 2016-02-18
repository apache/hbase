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
public class MetricsSnapshotSourceImpl extends BaseSourceImpl implements MetricsSnapshotSource {

  private MetricHistogram snapshotTimeHisto;
  private MetricHistogram snapshotCloneTimeHisto;
  private MetricHistogram snapshotRestoreTimeHisto;

  public MetricsSnapshotSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsSnapshotSourceImpl(String metricsName,
                                   String metricsDescription,
                                   String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void init() {
    snapshotTimeHisto = metricsRegistry.newTimeHistogram(
        SNAPSHOT_TIME_NAME, SNAPSHOT_TIME_DESC);
    snapshotCloneTimeHisto = metricsRegistry.newTimeHistogram(
        SNAPSHOT_CLONE_TIME_NAME, SNAPSHOT_CLONE_TIME_DESC);
    snapshotRestoreTimeHisto = metricsRegistry.newTimeHistogram(
        SNAPSHOT_RESTORE_TIME_NAME, SNAPSHOT_RESTORE_TIME_DESC);
  }

  @Override
  public void updateSnapshotTime(long time) {
    snapshotTimeHisto.add(time);
  }

  @Override
  public void updateSnapshotCloneTime(long time) {
    snapshotCloneTimeHisto.add(time);
  }

  @Override
  public void updateSnapshotRestoreTime(long time) {
    snapshotRestoreTimeHisto.add(time);
  }
}
