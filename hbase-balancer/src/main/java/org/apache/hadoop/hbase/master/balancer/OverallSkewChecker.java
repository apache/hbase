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
package org.apache.hadoop.hbase.master.balancer;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The cluster status checker for {@link StochasticLoadBalancer}, if the skew table counts up to a
 * configured ratio, the simplified balancer will be executed, it is aimed to balance the regions as
 * soon as possible. For example, when there are large-scaled restart of RSes, or expansion for
 * groups or cluster, we hope the balancer can execute as soon as possible, but the
 * StochasticLoadBalancer may need a lot of time to compute costs.
 */
@InterfaceAudience.Private
class OverallSkewChecker implements StochasticLoadBalancer.ClusterStatusBalanceChecker {
  private static final Logger LOG = LoggerFactory.getLogger(OverallSkewChecker.class);

  private static final String BALANCER_OVERALL_SKEW_PERCENT_KEY =
    "hbase.master.balancer.overall.skew.percentage";

  private Configuration conf;

  public OverallSkewChecker(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean
    needsCoarseBalance(Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) {
    if (this.conf == null) {
      LOG.warn("Configuration should not be null");
      return false;
    }
    if (isClusterSkew(loadOfAllTable)) {
      LOG.info("Balancer checker found cluster is skew, will perform force balance, "
        + "table count is {}", loadOfAllTable.size());
      return true;
    }
    LOG.debug("Balancer checked cluster, it is not skew now, tables count is {}",
      loadOfAllTable.size());
    return false;
  }

  public boolean isClusterSkew(Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) {
    if (loadOfAllTable == null || loadOfAllTable.isEmpty()) {
      return false;
    }
    int skewCount = 0;
    for (Map.Entry<TableName, Map<ServerName, List<RegionInfo>>> entry : loadOfAllTable
      .entrySet()) {
      if (isSkew(entry.getValue())) {
        LOG.info("Table: " + entry.getKey().getNameAsString() + " regions count is skew");
        if (LOG.isDebugEnabled()) {
          StringBuilder sb = new StringBuilder(
            "Table: " + entry.getKey().getNameAsString() + " regions distribution is: ");
          for (Map.Entry<ServerName, List<RegionInfo>> regionDistribution : entry.getValue()
            .entrySet()) {
            sb.append(regionDistribution.getKey().getServerName()).append(" count=")
              .append(regionDistribution.getValue().size()).append(":");
            sb.append(regionDistribution.getValue());
          }
          LOG.debug(sb.toString());
        }
        skewCount++;
      }
    }
    LOG.info("Cluster is skew, skew table count={}, load table count={}", skewCount,
      loadOfAllTable.size());
    return skewCount > 0 && (double) skewCount / loadOfAllTable.size()
        > conf.getDouble(BALANCER_OVERALL_SKEW_PERCENT_KEY, 0.5);
  }

  private boolean isSkew(Map<ServerName, List<RegionInfo>> regions) {
    if (regions == null || regions.isEmpty()) {
      return false;
    }
    int max = 0;
    int min = Integer.MAX_VALUE;
    for (Map.Entry<ServerName, List<RegionInfo>> entry : regions.entrySet()) {
      int count = entry.getValue() == null ? 0 : entry.getValue().size();
      max = Math.max(max, count);
      min = Math.min(min, count);
    }
    return max - min > 2;
  }
}
