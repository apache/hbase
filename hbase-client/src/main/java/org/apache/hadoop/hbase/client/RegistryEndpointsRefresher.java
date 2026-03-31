/*
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Thread safe utility that keeps registry end points used by {@link ConnectionRegistry} up to date.
 * By default the refresh happens periodically (configured via {@code intervalSecsConfigName}). The
 * refresh can also be triggered on demand via {@link #refreshNow()}. To prevent a flood of
 * on-demand refreshes we expect that any attempts two should be spaced at least
 * {@code minIntervalSecsConfigName} seconds apart.
 */
@InterfaceAudience.Private
final class RegistryEndpointsRefresher {

  private static final Logger LOG = LoggerFactory.getLogger(RegistryEndpointsRefresher.class);

  private static final int PERIODIC_REFRESH_INTERVAL_SECS_DEFAULT = 300;

  private static final int MIN_SECS_BETWEEN_REFRESHES_DEFAULT = 60;

  private final Thread thread;
  private final Refresher refresher;
  private final long initialDelayMs;
  private final long periodicRefreshMs;
  private final long minTimeBetweenRefreshesMs;

  private boolean refreshNow = false;
  private boolean stopped = false;

  synchronized void stop() {
    stopped = true;
    notifyAll();
  }

  private long getRefreshIntervalMs(boolean firstRefresh) {
    if (refreshNow) {
      return minTimeBetweenRefreshesMs;
    }
    if (firstRefresh) {
      return initialDelayMs;
    }
    return periodicRefreshMs;
  }

  // The main loop for the refresh thread.
  private void mainLoop() {
    long lastRefreshTime = EnvironmentEdgeManager.currentTime();
    boolean firstRefresh = true;
    for (;;) {
      synchronized (this) {
        for (;;) {
          if (stopped) {
            LOG.info("Registry end points refresher loop exited.");
            return;
          }
          // if refreshNow is true, then we will wait until minTimeBetweenRefreshesMs elapsed,
          // otherwise wait until periodicRefreshMs elapsed
          long waitTime = getRefreshIntervalMs(firstRefresh)
            - (EnvironmentEdgeManager.currentTime() - lastRefreshTime);
          if (waitTime <= 0) {
            // we are going to refresh, reset this flag
            firstRefresh = false;
            refreshNow = false;
            break;
          }
          try {
            wait(waitTime);
          } catch (InterruptedException e) {
            LOG.warn("Interrupted during wait", e);
            Thread.currentThread().interrupt();
            continue;
          }
        }
      }
      LOG.debug("Attempting to refresh registry end points");
      try {
        refresher.refresh();
      } catch (IOException e) {
        LOG.warn("Error refresh registry end points", e);
      }
      // We do not think it is a big deal to fail one time, so no matter what is refresh result, we
      // just update this refresh time and wait for the next round. If later this becomes critical,
      // could change to only update this value when we have done a successful refreshing.
      lastRefreshTime = EnvironmentEdgeManager.currentTime();
      LOG.debug("Finished refreshing registry end points");
    }
  }

  @FunctionalInterface
  public interface Refresher {

    void refresh() throws IOException;
  }

  private RegistryEndpointsRefresher(long initialDelayMs, long periodicRefreshMs,
    long minTimeBetweenRefreshesMs, Refresher refresher) {
    this.initialDelayMs = initialDelayMs;
    this.periodicRefreshMs = periodicRefreshMs;
    this.minTimeBetweenRefreshesMs = minTimeBetweenRefreshesMs;
    this.refresher = refresher;
    thread = new Thread(this::mainLoop);
    thread.setName("Registry-endpoints-refresh-end-points");
    thread.setDaemon(true);
    thread.start();
  }

  /**
   * Notifies the refresher thread to refresh the configuration. This does not guarantee a refresh.
   * See class comment for details.
   */
  synchronized void refreshNow() {
    refreshNow = true;
    notifyAll();
  }

  /**
   * Create a {@link RegistryEndpointsRefresher}. If the interval secs configured via
   * {@code intervalSecsConfigName} is less than zero, will return null here, which means disable
   * refreshing of endpoints.
   */
  static RegistryEndpointsRefresher create(Configuration conf, String initialDelaySecsConfigName,
    String intervalSecsConfigName, String minIntervalSecsConfigName, Refresher refresher) {
    long periodicRefreshMs = TimeUnit.SECONDS
      .toMillis(conf.getLong(intervalSecsConfigName, PERIODIC_REFRESH_INTERVAL_SECS_DEFAULT));
    if (periodicRefreshMs <= 0) {
      return null;
    }
    long initialDelayMs = Math.max(1,
      TimeUnit.SECONDS.toMillis(conf.getLong(initialDelaySecsConfigName, periodicRefreshMs / 10)));
    long minTimeBetweenRefreshesMs = TimeUnit.SECONDS
      .toMillis(conf.getLong(minIntervalSecsConfigName, MIN_SECS_BETWEEN_REFRESHES_DEFAULT));
    Preconditions.checkArgument(minTimeBetweenRefreshesMs <= periodicRefreshMs);
    return new RegistryEndpointsRefresher(initialDelayMs, periodicRefreshMs,
      minTimeBetweenRefreshesMs, refresher);
  }
}
