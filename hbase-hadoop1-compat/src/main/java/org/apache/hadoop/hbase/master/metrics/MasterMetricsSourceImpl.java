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

package org.apache.hadoop.hbase.master.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.metrics.BaseMetricsSourceImpl;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MetricMutableHistogram;

/** Hadoop1 implementation of MasterMetricsSource. */
public class MasterMetricsSourceImpl
    extends BaseMetricsSourceImpl implements MasterMetricsSource {

  private static final Log LOG = LogFactory.getLog(MasterMetricsSourceImpl.class.getName());

  MetricMutableCounterLong clusterRequestsCounter;
  MetricMutableGaugeLong ritGauge;
  MetricMutableGaugeLong ritCountOverThresholdGauge;
  MetricMutableGaugeLong ritOldestAgeGauge;

  private final MasterMetricsWrapper masterWrapper;
  private MetricMutableHistogram splitTimeHisto;
  private MetricMutableHistogram splitSizeHisto;

  public MasterMetricsSourceImpl(MasterMetricsWrapper masterWrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, masterWrapper);
  }

  public MasterMetricsSourceImpl(String metricsName,
                                 String metricsDescription,
                                 String metricsContext,
                                 String metricsJmxContext,
                                 MasterMetricsWrapper masterWrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.masterWrapper = masterWrapper;
  }

  @Override
  public void init() {
    super.init();
    clusterRequestsCounter = metricsRegistry.newCounter(CLUSTER_REQUESTS_NAME, "", 0l);
    ritGauge = metricsRegistry.newGauge(RIT_COUNT_NAME, "", 0l);
    ritCountOverThresholdGauge = metricsRegistry.newGauge(RIT_COUNT_OVER_THRESHOLD_NAME, "", 0l);
    ritOldestAgeGauge = metricsRegistry.newGauge(RIT_OLDEST_AGE_NAME, "", 0l);
    splitTimeHisto = metricsRegistry.newHistogram(SPLIT_SIZE_NAME, SPLIT_SIZE_DESC);
    splitSizeHisto = metricsRegistry.newHistogram(SPLIT_TIME_NAME, SPLIT_TIME_DESC);
  }

  public void incRequests(final int inc) {
    this.clusterRequestsCounter.incr(inc);
  }

  public void setRIT(int ritCount) {
    ritGauge.set(ritCount);
  }

  public void setRITCountOverThreshold(int ritCount) {
    ritCountOverThresholdGauge.set(ritCount);
  }

  public void setRITOldestAge(long ritCount) {
    ritOldestAgeGauge.set(ritCount);
  }

  @Override
  public void updateSplitTime(long time) {
    splitTimeHisto.add(time);
  }

  @Override
  public void updateSplitSize(long size) {
    splitSizeHisto.add(size);
  }

  /**
   * Method to export all the metrics.
   *
   * @param metricsBuilder Builder to accept metrics
   * @param all            push all or only changed?
   */
  @Override
  public void getMetrics(MetricsBuilder metricsBuilder, boolean all) {

    MetricsRecordBuilder metricsRecordBuilder = metricsBuilder.addRecord(metricsName)
                                                              .setContext(metricsContext);

    // masterWrapper can be null because this function is called inside of init.
    if (masterWrapper != null) {
      metricsRecordBuilder
          .addGauge(MASTER_ACTIVE_TIME_NAME,
              MASTER_ACTIVE_TIME_DESC, masterWrapper.getMasterActiveTime())
          .addGauge(MASTER_START_TIME_NAME,
              MASTER_START_TIME_DESC, masterWrapper.getMasterStartTime())
          .addGauge(AVERAGE_LOAD_NAME, AVERAGE_LOAD_DESC, masterWrapper.getAverageLoad())
          .addGauge(NUM_REGION_SERVERS_NAME,
              NUMBER_OF_REGION_SERVERS_DESC, masterWrapper.getRegionServers())
          .addGauge(NUM_DEAD_REGION_SERVERS_NAME,
              NUMBER_OF_DEAD_REGION_SERVERS_DESC,
              masterWrapper.getDeadRegionServers())
          .tag(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC, masterWrapper.getZookeeperQuorum())
          .tag(SERVER_NAME_NAME, SERVER_NAME_DESC, masterWrapper.getServerName())
          .tag(CLUSTER_ID_NAME, CLUSTER_ID_DESC, masterWrapper.getClusterId())
          .tag(IS_ACTIVE_MASTER_NAME,
              IS_ACTIVE_MASTER_DESC,
              String.valueOf(masterWrapper.getIsActiveMaster()));
    }

    metricsRegistry.snapshot(metricsRecordBuilder, true);
  }

}
