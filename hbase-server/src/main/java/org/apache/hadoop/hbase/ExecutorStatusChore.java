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
package org.apache.hadoop.hbase;

import java.util.Map;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorStatus;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceImpl;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The Class ExecutorStatusChore for collect Executor status info periodically
 * and report to metrics system
 */
@InterfaceAudience.Private
public class ExecutorStatusChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckChore.class);
  public static final String WAKE_FREQ = "hbase.executors.status.collect.period";
  public static final int DEFAULT_WAKE_FREQ = 60000;
  private ExecutorService service;
  private DynamicMetricsRegistry metricsRegistry;

  public ExecutorStatusChore(int sleepTime, Stoppable stopper, ExecutorService service,
      MetricsRegionServerSource metrics) {
    super("ExecutorStatusChore", stopper, sleepTime);
    LOG.info("ExecutorStatusChore runs every {} ", StringUtils.formatTime(sleepTime));
    this.service = service;
    this.metricsRegistry = ((MetricsRegionServerSourceImpl) metrics).getMetricsRegistry();
  }

  @Override
  protected void chore() {
    try{
      // thread pool monitor
      Map<String, ExecutorStatus> statuses = service.getAllExecutorStatuses();
      for (Map.Entry<String, ExecutorStatus> statusEntry : statuses.entrySet()) {
        String name = statusEntry.getKey();
        // Executor's name is generate by ExecutorType#getExecutorName
        // include ExecutorType & Servername(split by '-'), here we only need the ExecutorType
        String poolName = name.split("-")[0];
        ExecutorStatus status = statusEntry.getValue();
        MutableGaugeLong queued = metricsRegistry.getGauge(poolName + "_queued", 0L);
        MutableGaugeLong running = metricsRegistry.getGauge(poolName + "_running", 0L);
        int queueSize = status.getQueuedEvents().size();
        int runningSize = status.getRunning().size();
        if (queueSize > 0) {
          LOG.warn("{}'s size info, queued: {}, running: {}", poolName, queueSize, runningSize);
        }
        queued.set(queueSize);
        running.set(runningSize);
      }
    } catch(Throwable e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @VisibleForTesting
  public Pair<Long, Long> getExecutorStatus(String poolName) {
    MutableGaugeLong running = metricsRegistry.getGauge(poolName + "_running", 0L);
    MutableGaugeLong queued = metricsRegistry.getGauge(poolName + "_queued", 0L);
    return new Pair<Long, Long>(running.value(), queued.value());
  }
}
