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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the statistics for multiple regions
 */
@InterfaceAudience.Private
public class ServerStatisticTracker {

  private final ConcurrentHashMap<ServerName, ServerStatistics> stats =
      new ConcurrentHashMap<ServerName, ServerStatistics>();

  public void updateRegionStats(ServerName server, byte[] region, ClientProtos.RegionLoadStats
      currentStats) {
    ServerStatistics stat = stats.get(server);

    if (stat == null) {
      stat = stats.get(server);
      // We don't have stats for that server yet, so we need to make an entry.
      // If we race with another thread it's a harmless unnecessary allocation.
      if (stat == null) {
        stat = new ServerStatistics();
        ServerStatistics old = stats.putIfAbsent(server, stat);
        if (old != null) {
          stat = old;
	}
      }
    }
    stat.update(region, currentStats);
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

  @VisibleForTesting
  ServerStatistics getServerStatsForTesting(ServerName server) {
    return stats.get(server);
  }
}
