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

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

/**
 * A {@link Result} with some statistics about the server/region status
 */
@InterfaceAudience.Private
public final class ResultStatsUtil {

  private ResultStatsUtil() {
    //private ctor for util class
  }

  /**
   * Update the stats for the specified region if the result is an instance of {@link
   * ResultStatsUtil}
   *
   * @param r object that contains the result and possibly the statistics about the region
   * @param serverStats stats tracker to update from the result
   * @param server server from which the result was obtained
   * @param regionName full region name for the stats.
   * @return the underlying {@link Result} if the passed result is an {@link
   * ResultStatsUtil} or just returns the result;
   */
  public static <T> T updateStats(T r, ServerStatisticTracker serverStats,
      ServerName server, byte[] regionName) {
    if (!(r instanceof Result)) {
      return r;
    }
    Result result = (Result) r;
    // early exit if there are no stats to collect
    ClientProtos.RegionLoadStats stats = result.getStats();
    if(stats == null){
      return r;
    }

    if (regionName != null) {
      serverStats.updateRegionStats(server, regionName, stats);
    }

    return r;
  }

  public static <T> T updateStats(T r, ServerStatisticTracker stats,
      HRegionLocation regionLocation) {
    byte[] regionName = null;
    ServerName server = null;
    if (regionLocation != null) {
      server = regionLocation.getServerName();
      regionName = regionLocation.getRegionInfo().getRegionName();
    }

    return updateStats(r, stats, server, regionName);
  }
}