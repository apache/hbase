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

package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;

/**
 * Hadoop1 implementation of MetricsMasterSource. Implements BaseSource through BaseSourceImpl,
 * following the pattern
 */
@InterfaceAudience.Private
public class MetricsEditsReplaySourceImpl extends BaseSourceImpl implements
    MetricsEditsReplaySource {

  private static final Log LOG = LogFactory.getLog(MetricsEditsReplaySourceImpl.class.getName());

  private MetricHistogram replayTimeHisto;
  private MetricHistogram replayBatchSizeHisto;
  private MetricHistogram replayDataSizeHisto;

  public MetricsEditsReplaySourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsEditsReplaySourceImpl(String metricsName,
                                      String metricsDescription,
                                      String metricsContext,
                                      String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void init() {
    super.init();
    replayTimeHisto = metricsRegistry.newTimeHistogram(REPLAY_TIME_NAME, REPLAY_TIME_DESC);
    replayBatchSizeHisto = metricsRegistry.newSizeHistogram(REPLAY_BATCH_SIZE_NAME,
      REPLAY_BATCH_SIZE_DESC);
    replayDataSizeHisto = metricsRegistry
        .newSizeHistogram(REPLAY_DATA_SIZE_NAME, REPLAY_DATA_SIZE_DESC);
  }

  @Override
  public void updateReplayTime(long time) {
    replayTimeHisto.add(time);
  }

  @Override
  public void updateReplayBatchSize(long size) {
    replayBatchSizeHisto.add(size);
  }

  @Override
  public void updateReplayDataSize(long size) {
    replayDataSizeHisto.add(size);
  }
}
