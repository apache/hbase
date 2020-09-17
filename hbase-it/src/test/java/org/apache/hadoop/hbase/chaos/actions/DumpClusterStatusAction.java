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

package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.net.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action to dump the cluster status.
 */
public class DumpClusterStatusAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(DumpClusterStatusAction.class);

  private Set<Address> initialRegionServers;

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void init(ActionContext context) throws IOException {
    super.init(context);
    initialRegionServers = collectKnownRegionServers(initialStatus);
  }

  @Override
  public void perform() throws Exception {
    getLogger().debug("Performing action: Dump cluster status");
    final ClusterMetrics currentMetrics = cluster.getClusterMetrics();
    getLogger().info("Cluster status\n{}", currentMetrics);
    reportMissingRegionServers(currentMetrics);
    reportNewRegionServers(currentMetrics);
  }

  /**
   * Build a set of all the host:port pairs of region servers known to this cluster.
   */
  private static Set<Address> collectKnownRegionServers(final ClusterMetrics clusterMetrics) {
    final Set<Address> regionServers = clusterMetrics.getLiveServerMetrics()
      .keySet()
      .stream()
      .map(ServerName::getAddress)
      .collect(Collectors.toSet());
    clusterMetrics.getDeadServerNames()
      .stream()
      .map(ServerName::getAddress)
      .forEach(regionServers::add);
    return Collections.unmodifiableSet(regionServers);
  }

  private void reportMissingRegionServers(final ClusterMetrics clusterMetrics) {
    final Set<Address> regionServers = collectKnownRegionServers(clusterMetrics);
    final Set<Address> missingRegionServers = new HashSet<>(initialRegionServers);
    missingRegionServers.removeAll(regionServers);
    if (!missingRegionServers.isEmpty()) {
      final StringBuilder stringBuilder = new StringBuilder()
        .append("region server(s) are missing from this cluster report");
      missingRegionServers.stream()
        .sorted()
        .forEach(address -> stringBuilder.append("\n  ").append(address));
      getLogger().warn(stringBuilder.toString());
    }
  }

  private void reportNewRegionServers(final ClusterMetrics clusterMetrics) {
    final Set<Address> regionServers = collectKnownRegionServers(clusterMetrics);
    final Set<Address> newRegionServers = new HashSet<>(regionServers);
    newRegionServers.removeAll(initialRegionServers);
    if (!newRegionServers.isEmpty()) {
      final StringBuilder stringBuilder = new StringBuilder()
        .append("region server(s) are new for this cluster report");
      newRegionServers.stream()
        .sorted()
        .forEach(address -> stringBuilder.append("\n  ").append(address));
      getLogger().warn(stringBuilder.toString());
    }
  }
}
