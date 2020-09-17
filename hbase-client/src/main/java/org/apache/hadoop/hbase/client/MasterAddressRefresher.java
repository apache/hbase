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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClientMetaService;

/**
 * Thread safe utility that keeps master end points used by {@link MasterRegistry} up to date. This
 * uses the RPC {@link ClientMetaService#getMasters} to fetch the latest list of registered masters.
 * By default the refresh happens periodically (configured via
 * {@link #PERIODIC_REFRESH_INTERVAL_SECS}). The refresh can also be triggered on demand via
 * {@link #refreshNow()}. To prevent a flood of on-demand refreshes we expect that any attempts two
 * should be spaced at least {@link #MIN_SECS_BETWEEN_REFRESHES} seconds apart.
 */
@InterfaceAudience.Private
public class MasterAddressRefresher implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MasterAddressRefresher.class);
  public static final String PERIODIC_REFRESH_INTERVAL_SECS =
      "hbase.client.master_registry.refresh_interval_secs";
  private static final int PERIODIC_REFRESH_INTERVAL_SECS_DEFAULT = 300;
  public static final String MIN_SECS_BETWEEN_REFRESHES =
      "hbase.client.master_registry.min_secs_between_refreshes";
  private static final int MIN_SECS_BETWEEN_REFRESHES_DEFAULT = 60;

  private final ExecutorService pool;
  private final MasterRegistry registry;
  private final long periodicRefreshMs;
  private final long timeBetweenRefreshesMs;
  private final Object refreshMasters = new Object();

  @Override
  public void close() {
    pool.shutdownNow();
  }

  /**
   * Thread that refreshes the master end points until it is interrupted via {@link #close()}.
   * Multiple callers attempting to refresh at the same time synchronize on {@link #refreshMasters}.
   */
  private class RefreshThread implements Runnable {
    @Override
    public void run() {
      long lastRpcTs = 0;
      while (!Thread.interrupted()) {
        try {
          // Spurious wake ups are okay, worst case we make an extra RPC call to refresh. We won't
          // have duplicate refreshes because once the thread is past the wait(), notify()s are
          // ignored until the thread is back to the waiting state.
          synchronized (refreshMasters) {
            refreshMasters.wait(periodicRefreshMs);
          }
          long currentTs = EnvironmentEdgeManager.currentTime();
          if (lastRpcTs != 0 && currentTs - lastRpcTs <= timeBetweenRefreshesMs) {
            continue;
          }
          lastRpcTs = currentTs;
          LOG.debug("Attempting to refresh master address end points.");
          Set<ServerName> newMasters = new HashSet<>(registry.getMasters().get());
          registry.populateMasterStubs(newMasters);
          LOG.debug("Finished refreshing master end points. {}", newMasters);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted during wait, aborting refresh-masters-thread.", e);
          break;
        } catch (ExecutionException | IOException e) {
          LOG.debug("Error populating latest list of masters.", e);
        }
      }
      LOG.info("Master end point refresher loop exited.");
    }
  }

  MasterAddressRefresher(Configuration conf, MasterRegistry registry) {
    pool = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("master-registry-refresh-end-points").setDaemon(true).build());
    periodicRefreshMs = TimeUnit.SECONDS.toMillis(conf.getLong(PERIODIC_REFRESH_INTERVAL_SECS,
        PERIODIC_REFRESH_INTERVAL_SECS_DEFAULT));
    timeBetweenRefreshesMs = TimeUnit.SECONDS.toMillis(conf.getLong(MIN_SECS_BETWEEN_REFRESHES,
        MIN_SECS_BETWEEN_REFRESHES_DEFAULT));
    Preconditions.checkArgument(periodicRefreshMs > 0);
    Preconditions.checkArgument(timeBetweenRefreshesMs < periodicRefreshMs);
    this.registry = registry;
    pool.submit(new RefreshThread());
  }

  /**
   * Notifies the refresher thread to refresh the configuration. This does not guarantee a refresh.
   * See class comment for details.
   */
  void refreshNow() {
    synchronized (refreshMasters) {
      refreshMasters.notify();
    }
  }
}
