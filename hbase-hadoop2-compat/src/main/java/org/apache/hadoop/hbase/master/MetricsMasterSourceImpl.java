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
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Hadoop2 implementation of MetricsMasterSource.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsMasterSourceImpl
    extends BaseSourceImpl implements MetricsMasterSource {

  private final MetricsMasterWrapper masterWrapper;
  private MutableCounterLong clusterRequestsCounter;

  public MetricsMasterSourceImpl(MetricsMasterWrapper masterWrapper) {
    this(METRICS_NAME,
        METRICS_DESCRIPTION,
        METRICS_CONTEXT,
        METRICS_JMX_CONTEXT,
        masterWrapper);
  }

  public MetricsMasterSourceImpl(String metricsName,
                                 String metricsDescription,
                                 String metricsContext,
                                 String metricsJmxContext,
                                 MetricsMasterWrapper masterWrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.masterWrapper = masterWrapper;

  }

  @Override
  public void init() {
    super.init();
    clusterRequestsCounter = metricsRegistry.newCounter(CLUSTER_REQUESTS_NAME, "", 0l);
  }

  @Override
  public void incRequests(final long inc) {
    this.clusterRequestsCounter.incr(inc);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {

    MetricsRecordBuilder metricsRecordBuilder = metricsCollector.addRecord(metricsName);

    // masterWrapper can be null because this function is called inside of init.
    if (masterWrapper != null) {
      metricsRecordBuilder
          .addGauge(Interns.info(MASTER_ACTIVE_TIME_NAME,
              MASTER_ACTIVE_TIME_DESC), masterWrapper.getActiveTime())
          .addGauge(Interns.info(MASTER_START_TIME_NAME,
              MASTER_START_TIME_DESC), masterWrapper.getStartTime())
          .addGauge(Interns.info(AVERAGE_LOAD_NAME, AVERAGE_LOAD_DESC),
              masterWrapper.getAverageLoad())
          .tag(Interns.info(LIVE_REGION_SERVERS_NAME, LIVE_REGION_SERVERS_DESC),
                masterWrapper.getRegionServers())
          .addGauge(Interns.info(NUM_REGION_SERVERS_NAME,
              NUMBER_OF_REGION_SERVERS_DESC), masterWrapper.getNumRegionServers())
          .tag(Interns.info(DEAD_REGION_SERVERS_NAME, DEAD_REGION_SERVERS_DESC),
                masterWrapper.getDeadRegionServers())
          .addGauge(Interns.info(NUM_DEAD_REGION_SERVERS_NAME,
              NUMBER_OF_DEAD_REGION_SERVERS_DESC),
              masterWrapper.getNumDeadRegionServers())
          .tag(Interns.info(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC),
              masterWrapper.getZookeeperQuorum())
          .tag(Interns.info(SERVER_NAME_NAME, SERVER_NAME_DESC), masterWrapper.getServerName())
          .tag(Interns.info(CLUSTER_ID_NAME, CLUSTER_ID_DESC), masterWrapper.getClusterId())
          .tag(Interns.info(IS_ACTIVE_MASTER_NAME,
              IS_ACTIVE_MASTER_DESC),
              String.valueOf(masterWrapper.getIsActiveMaster()));
    }

    metricsRegistry.snapshot(metricsRecordBuilder, all);
  }

}
