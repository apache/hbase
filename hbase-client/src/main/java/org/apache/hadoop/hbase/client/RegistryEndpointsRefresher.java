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
class RegistryEndpointsRefresher {

  private static final Logger LOG = LoggerFactory.getLogger(RegistryEndpointsRefresher.class);

  public static final String PERIODIC_REFRESH_INTERVAL_SECS =
    "hbase.client.rpc_registry.refresh_interval_secs";
  private static final int PERIODIC_REFRESH_INTERVAL_SECS_DEFAULT = 300;

  public static final String MIN_SECS_BETWEEN_REFRESHES =
    "hbase.client.rpc_registry.min_secs_between_refreshes";
  private static final int MIN_SECS_BETWEEN_REFRESHES_DEFAULT = 60;

  private final Thread thread;
  private final Refresher refresher;
  private final long periodicRefreshMs;
  private final long minTimeBetweenRefreshesMs;

  private boolean refreshNow = false;
  private boolean stopped = false;

  public void start() {
    thread.start();
  }

  public synchronized void stop() {
    stopped = true;
    notifyAll();
  }

  // The main loop for the refresh thread.
  private void mainLoop() {
    long lastRefreshTime = EnvironmentEdgeManager.currentTime();
    for (;;) {
      synchronized (this) {
        for (;;) {
          if (stopped) {
            LOG.info("Registry end points refresher loop exited.");
            return;
          }
          // if refreshNow is true, then we will wait until minTimeBetweenRefreshesMs elapsed,
          // otherwise wait until periodicRefreshMs elapsed
          long waitTime = (refreshNow ? minTimeBetweenRefreshesMs : periodicRefreshMs) -
            (EnvironmentEdgeManager.currentTime() - lastRefreshTime);
          if (waitTime <= 0) {
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
        // we are going to refresh, reset this flag
        refreshNow = false;
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

  RegistryEndpointsRefresher(Configuration conf, String intervalSecsConfigName,
    String minIntervalSecsConfigName, Refresher refresher) {
    periodicRefreshMs = TimeUnit.SECONDS
      .toMillis(conf.getLong(intervalSecsConfigName, PERIODIC_REFRESH_INTERVAL_SECS_DEFAULT));
    minTimeBetweenRefreshesMs = TimeUnit.SECONDS
      .toMillis(conf.getLong(minIntervalSecsConfigName, MIN_SECS_BETWEEN_REFRESHES_DEFAULT));
    Preconditions.checkArgument(periodicRefreshMs > 0);
    Preconditions.checkArgument(minTimeBetweenRefreshesMs < periodicRefreshMs);
    thread = new Thread(this::mainLoop);
    thread.setName("Registry-endpoints-refresh-end-points");
    thread.setDaemon(true);
    this.refresher = refresher;
  }

  /**
   * Notifies the refresher thread to refresh the configuration. This does not guarantee a refresh.
   * See class comment for details.
   */
  synchronized void refreshNow() {
    refreshNow = true;
    notifyAll();
  }
}
