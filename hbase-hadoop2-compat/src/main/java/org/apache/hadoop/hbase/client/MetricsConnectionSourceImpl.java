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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

public class MetricsConnectionSourceImpl
    extends BaseSourceImpl implements MetricsConnectionSource {

  // wrapper for access statistics collected in Connection instance
  private final MetricsConnectionWrapper wrapper;

  // Hadoop Metric2 objects for additional monitoring

  private final MutableCounterLong metaCacheHit;
  private final MutableCounterLong metaCacheMiss;

  public MetricsConnectionSourceImpl(MetricsConnectionWrapper wrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT,
        METRICS_JMX_CONTEXT + wrapper.getId(), wrapper);
  }

  public MetricsConnectionSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext, MetricsConnectionWrapper wrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.wrapper = wrapper;

    metaCacheHit = getMetricsRegistry().newCounter(META_CACHE_HIT_NAME, META_CACHE_HIT_DESC, 0l);
    metaCacheMiss =
        getMetricsRegistry().newCounter(META_CACHE_MISS_NAME, META_CACHE_MISS_DESC, 0l);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {

    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);

    if (wrapper != null) {
      mrb.addGauge(Interns.info(META_LOOKUP_POOL_LARGEST_SIZE_NAME,
              META_LOOKUP_POOL_LARGEST_SIZE_DESC), wrapper.getMetaLookupPoolLargestPoolSize())
          .addGauge(Interns.info(META_LOOKUP_POOL_ACTIVE_THREAD_NAME,
              META_LOOKUP_POOL_ACTIVE_THREAD_DESC), wrapper.getMetaLookupPoolActiveCount())
          .tag(Interns.info(BATCH_POOL_ID_NAME, BATCH_POOL_ID_DESC), wrapper.getBatchPoolId())
          .addGauge(Interns.info(BATCH_POOL_ACTIVE_THREAD_NAME, BATCH_POOL_ACTIVE_THREAD_DESC),
              wrapper.getBatchPoolActiveCount())
          .addGauge(Interns.info(BATCH_POOL_LARGEST_SIZE_NAME, BATCH_POOL_LARGEST_SIZE_DESC),
              wrapper.getBatchPoolLargestPoolSize())
          .tag(Interns.info(CONNECTION_ID_NAME, CONNECTION_ID_DESC), wrapper.getId())
          .tag(Interns.info(USER_NAME_NAME, USER_NAME_DESC), wrapper.getUserName())
          .tag(Interns.info(CLUSTER_ID_NAME, CLUSTER_ID_DESC), wrapper.getClusterId())
          .tag(Interns.info(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC),
              wrapper.getZookeeperQuorum())
          .tag(Interns.info(ZOOKEEPER_ZNODE_NAME, ZOOKEEPER_ZNODE_DESC),
              wrapper.getZookeeperBaseNode());
    }

    metricsRegistry.snapshot(mrb, all);
  }

  @Override public void incrMetaCacheHit() {
    metaCacheHit.incr();
  }

  @Override public void incrMetaCacheMiss() {
    metaCacheMiss.incr();
  }
}
