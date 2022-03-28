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
package org.apache.hadoop.hbase.hbtop.screen.top;

import java.util.Objects;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Represents the summary of the metrics.
 */
@InterfaceAudience.Private
public class Summary {
  private final String currentTime;
  private final String version;
  private final String clusterId;
  private final int servers;
  private final int liveServers;
  private final int deadServers;
  private final int regionCount;
  private final int ritCount;
  private final double averageLoad;
  private final long aggregateRequestPerSecond;

  public Summary(String currentTime, String version, String clusterId, int servers,
    int liveServers, int deadServers, int regionCount, int ritCount, double averageLoad,
    long aggregateRequestPerSecond) {
    this.currentTime = Objects.requireNonNull(currentTime);
    this.version = Objects.requireNonNull(version);
    this.clusterId = Objects.requireNonNull(clusterId);
    this.servers = servers;
    this.liveServers = liveServers;
    this.deadServers = deadServers;
    this.regionCount = regionCount;
    this.ritCount = ritCount;
    this.averageLoad = averageLoad;
    this.aggregateRequestPerSecond = aggregateRequestPerSecond;
  }

  public String getCurrentTime() {
    return currentTime;
  }

  public String getVersion() {
    return version;
  }

  public String getClusterId() {
    return clusterId;
  }

  public int getServers() {
    return servers;
  }

  public int getLiveServers() {
    return liveServers;
  }

  public int getDeadServers() {
    return deadServers;
  }

  public int getRegionCount() {
    return regionCount;
  }

  public int getRitCount() {
    return ritCount;
  }

  public double getAverageLoad() {
    return averageLoad;
  }

  public long getAggregateRequestPerSecond() {
    return aggregateRequestPerSecond;
  }
}
