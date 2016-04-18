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
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;

/**
 * Hadoop1 implementation of MetricsMasterSource.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsMasterSourceImpl
    extends BaseSourceImpl implements MetricsMasterSource {

  private final MetricsMasterWrapper masterWrapper;
  private MetricMutableCounterLong clusterRequestsCounter;

  // pause monitor metrics
  private final MetricMutableCounterLong infoPauseThresholdExceeded;
  private final MetricMutableCounterLong warnPauseThresholdExceeded;
  private final MetricHistogram pausesWithGc;
  private final MetricHistogram pausesWithoutGc;

  public MetricsMasterSourceImpl(MetricsMasterWrapper masterWrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, masterWrapper);
  }

  public MetricsMasterSourceImpl(String metricsName,
                                 String metricsDescription,
                                 String metricsContext,
                                 String metricsJmxContext,
                                 MetricsMasterWrapper masterWrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.masterWrapper = masterWrapper;

    // pause monitor metrics
    infoPauseThresholdExceeded = getMetricsRegistry().newCounter(INFO_THRESHOLD_COUNT_KEY,
      INFO_THRESHOLD_COUNT_DESC, 0L);
    warnPauseThresholdExceeded = getMetricsRegistry().newCounter(WARN_THRESHOLD_COUNT_KEY,
      WARN_THRESHOLD_COUNT_DESC, 0L);
    pausesWithGc = getMetricsRegistry().newTimeHistogram(PAUSE_TIME_WITH_GC_KEY);
    pausesWithoutGc = getMetricsRegistry().newTimeHistogram(PAUSE_TIME_WITHOUT_GC_KEY);
  }

  @Override
  public void init() {
    super.init();
    clusterRequestsCounter = metricsRegistry.newCounter(CLUSTER_REQUESTS_NAME, "", 0l);
  }

  public void incRequests(final int inc) {
    this.clusterRequestsCounter.incr(inc);
  }

  /**
   * Method to export all the metrics.
   *
   * @param metricsBuilder Builder to accept metrics
   * @param all            push all or only changed?
   */
  @Override
  public void getMetrics(MetricsBuilder metricsBuilder, boolean all) {

    MetricsRecordBuilder metricsRecordBuilder = metricsBuilder.addRecord(metricsName);

    // masterWrapper can be null because this function is called inside of init.
    if (masterWrapper != null) {
      metricsRecordBuilder
          .addGauge(MASTER_ACTIVE_TIME_NAME,
              MASTER_ACTIVE_TIME_DESC, masterWrapper.getActiveTime())
          .addGauge(MASTER_START_TIME_NAME,
              MASTER_START_TIME_DESC, masterWrapper.getStartTime())
          .addGauge(AVERAGE_LOAD_NAME, AVERAGE_LOAD_DESC, masterWrapper.getAverageLoad())
          .tag(LIVE_REGION_SERVERS_NAME, LIVE_REGION_SERVERS_DESC,
                masterWrapper.getRegionServers())
          .addGauge(NUM_REGION_SERVERS_NAME,
              NUMBER_OF_REGION_SERVERS_DESC, masterWrapper.getNumRegionServers())
          .tag(DEAD_REGION_SERVERS_NAME, DEAD_REGION_SERVERS_DESC,
                masterWrapper.getDeadRegionServers())
          .addGauge(NUM_DEAD_REGION_SERVERS_NAME,
              NUMBER_OF_DEAD_REGION_SERVERS_DESC,
              masterWrapper.getNumDeadRegionServers())
          .tag(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC, masterWrapper.getZookeeperQuorum())
          .tag(SERVER_NAME_NAME, SERVER_NAME_DESC, masterWrapper.getServerName())
          .tag(CLUSTER_ID_NAME, CLUSTER_ID_DESC, masterWrapper.getClusterId())
          .tag(IS_ACTIVE_MASTER_NAME,
              IS_ACTIVE_MASTER_DESC,
              String.valueOf(masterWrapper.getIsActiveMaster()));
    }

    metricsRegistry.snapshot(metricsRecordBuilder, all);
  }

  @Override
  public void incInfoThresholdExceeded(int count) {
    infoPauseThresholdExceeded.incr(count);
  }

  @Override
  public void incWarnThresholdExceeded(int count) {
    warnPauseThresholdExceeded.incr(count);
  }

  @Override
  public void updatePauseTimeWithGc(long t) {
    pausesWithGc.add(t);
  }

  @Override
  public void updatePauseTimeWithoutGc(long t) {
    pausesWithoutGc.add(t);
  }
}
