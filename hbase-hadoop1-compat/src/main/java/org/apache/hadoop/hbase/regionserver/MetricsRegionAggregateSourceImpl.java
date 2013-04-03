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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MetricsRegionAggregateSourceImpl extends BaseSourceImpl
    implements MetricsRegionAggregateSource {
  private final Log LOG = LogFactory.getLog(this.getClass());
  // lock to guard against concurrent access to regionSources
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final TreeSet<MetricsRegionSourceImpl> regionSources =
      new TreeSet<MetricsRegionSourceImpl>();

  public MetricsRegionAggregateSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }


  public MetricsRegionAggregateSourceImpl(String metricsName,
                                          String metricsDescription,
                                          String metricsContext,
                                          String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void register(MetricsRegionSource source) {
    lock.writeLock().lock();
    try {
      regionSources.add((MetricsRegionSourceImpl) source);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deregister(MetricsRegionSource source) {
    lock.writeLock().lock();
    try {
      regionSources.remove(source);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Yes this is a get function that doesn't return anything.  Thanks Hadoop for breaking all
   * expectations of java programmers.  Instead of returning anything Hadoop metrics expects
   * getMetrics to push the metrics into the metricsBuilder.
   *
   * @param metricsBuilder Builder to accept metrics
   * @param all            push all or only changed?
   */
  @Override
  public void getMetrics(MetricsBuilder metricsBuilder, boolean all) {

    MetricsRecordBuilder mrb = metricsBuilder.addRecord(metricsName)
        .setContext(metricsContext);

    if (regionSources != null) {
      lock.readLock().lock();
      try {
        for (MetricsRegionSourceImpl regionMetricSource : regionSources) {
          regionMetricSource.snapshot(mrb, all);
        }
      } finally {
        lock.readLock().unlock();
      }
    }

    metricsRegistry.snapshot(mrb, all);
  }
}
