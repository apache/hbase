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

import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Tracks the statistics for multiple regions
 */
@InterfaceAudience.Private
public class ServerStatisticTracker implements StatisticTrackable {

  private final ConcurrentHashMap<ServerName, ServerStatistics> stats = new ConcurrentHashMap<>();

  @Override
  public void updateRegionStats(ServerName server, byte[] region, RegionLoadStats currentStats) {
    computeIfAbsent(stats, server, ServerStatistics::new).update(region, currentStats);
  }

  public ServerStatistics getStats(ServerName server) {
    return this.stats.get(server);
  }

  public static ServerStatisticTracker create(Configuration conf) {
    if (!conf.getBoolean(HConstants.ENABLE_CLIENT_BACKPRESSURE,
        HConstants.DEFAULT_ENABLE_CLIENT_BACKPRESSURE)) {
      return null;
    }
    return new ServerStatisticTracker();
  }
}
