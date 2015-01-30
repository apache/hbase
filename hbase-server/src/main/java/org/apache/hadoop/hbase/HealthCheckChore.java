/**
 * Copyright 2011 The Apache Software Foundation
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HealthChecker.HealthCheckerExitStatus;
import org.apache.hadoop.util.StringUtils;

/**
 * The Class HealthCheckChore for running health checker regularly.
 */
public class HealthCheckChore extends ScheduledChore {
  private static Log LOG = LogFactory.getLog(HealthCheckChore.class);
  private HealthChecker healthChecker;
  private Configuration config;
  private int threshold;
  private int numTimesUnhealthy = 0;
  private long failureWindow;
  private long startWindow;

  public HealthCheckChore(int sleepTime, Stoppable stopper, Configuration conf) {
    super("HealthChecker", stopper, sleepTime);
    LOG.info("Health Check Chore runs every " + StringUtils.formatTime(sleepTime));
    this.config = conf;
    String healthCheckScript = this.config.get(HConstants.HEALTH_SCRIPT_LOC);
    long scriptTimeout = this.config.getLong(HConstants.HEALTH_SCRIPT_TIMEOUT,
      HConstants.DEFAULT_HEALTH_SCRIPT_TIMEOUT);
    healthChecker = new HealthChecker();
    healthChecker.init(healthCheckScript, scriptTimeout);
    this.threshold = config.getInt(HConstants.HEALTH_FAILURE_THRESHOLD,
      HConstants.DEFAULT_HEALTH_FAILURE_THRESHOLD);
    this.failureWindow = (long)this.threshold * (long)sleepTime;
  }

  @Override
  protected void chore() {
    HealthReport report = healthChecker.checkHealth();
    boolean isHealthy = (report.getStatus() == HealthCheckerExitStatus.SUCCESS);
    if (!isHealthy) {
      boolean needToStop = decideToStop();
      if (needToStop) {
        getStopper().stop(
          "The  node reported unhealthy " + threshold + " number of times consecutively.");
      }
      // Always log health report.
      LOG.info("Health status at " + StringUtils.formatTime(System.currentTimeMillis()) + " : "
          + report.getHealthReport());
    }
  }

  private boolean decideToStop() {
    boolean stop = false;
    if (numTimesUnhealthy == 0) {
      // First time we are seeing a failure. No need to stop, just
      // record the time.
      numTimesUnhealthy++;
      startWindow = System.currentTimeMillis();
    } else {
      if ((System.currentTimeMillis() - startWindow) < failureWindow) {
        numTimesUnhealthy++;
        if (numTimesUnhealthy == threshold) {
          stop = true;
        } else {
          stop = false;
        }
      } else {
        // Outside of failure window, so we reset to 1.
        numTimesUnhealthy = 1;
        startWindow = System.currentTimeMillis();
        stop = false;
      }
    }
    return stop;
  }

}
