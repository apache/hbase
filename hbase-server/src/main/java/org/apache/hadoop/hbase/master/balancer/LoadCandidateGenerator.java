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

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class LoadCandidateGenerator extends CandidateGenerator {

  @Override
  BaseLoadBalancer.Cluster.Action generate(BaseLoadBalancer.Cluster cluster) {
    cluster.sortServersByRegionCount();
    int thisServer = pickMostLoadedServer(cluster, -1);
    int otherServer = pickLeastLoadedServer(cluster, thisServer);
    return pickRandomRegions(cluster, thisServer, otherServer);
  }

  private int pickLeastLoadedServer(final BaseLoadBalancer.Cluster cluster, int thisServer) {
    Integer[] servers = cluster.serverIndicesSortedByRegionCount;

    int index = 0;
    while (servers[index] == null || servers[index] == thisServer) {
      index++;
      if (index == servers.length) {
        return -1;
      }
    }
    return servers[index];
  }

  private int pickMostLoadedServer(final BaseLoadBalancer.Cluster cluster, int thisServer) {
    Integer[] servers = cluster.serverIndicesSortedByRegionCount;

    int index = servers.length - 1;
    while (servers[index] == null || servers[index] == thisServer) {
      index--;
      if (index < 0) {
        return -1;
      }
    }
    return servers[index];
  }

}
