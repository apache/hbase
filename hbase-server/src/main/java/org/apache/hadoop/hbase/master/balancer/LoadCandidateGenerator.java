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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class LoadCandidateGenerator extends CandidateGenerator {

  @Override BaseLoadBalancer.Cluster.Action generate(BaseLoadBalancer.Cluster cluster) {
    cluster.sortServersByRegionCount();
    int thisServer = pickMostLoadedServer(cluster, -1);
    int otherServer = pickLeastLoadedServer(cluster, thisServer);
    return pickRandomRegions(cluster, thisServer, otherServer);
  }

  private int pickLeastLoadedServer(final BaseLoadBalancer.Cluster cluster, int thisServer) {
    Integer[] servers = cluster.serverIndicesSortedByRegionCount;

    int selectedIndex = -1;
    double currentLargestRandom = -1;
    for (int i = 0; i < servers.length; i++) {
      if (servers[i] == null || servers[i] == thisServer) {
        continue;
      }
      if (selectedIndex != -1 && cluster.getNumRegionsComparator().compare(servers[i],
        servers[selectedIndex]) != 0) {
        // Exhausted servers of the same region count
        break;
      }
      // we don't know how many servers have the same region count, we will randomly select one
      // using a simplified inline reservoir sampling by assignmening a random number to  stream
      // data and choose the greatest one. (http://gregable.com/2007/10/reservoir-sampling.html)
      double currentRandom = ThreadLocalRandom.current().nextDouble();
      if (currentRandom > currentLargestRandom) {
        selectedIndex = i;
        currentLargestRandom = currentRandom;
      }
    }
    return selectedIndex == -1 ? -1 : servers[selectedIndex];
  }

  private int pickMostLoadedServer(final BaseLoadBalancer.Cluster cluster, int thisServer) {
    Integer[] servers = cluster.serverIndicesSortedByRegionCount;

    int selectedIndex = -1;
    double currentLargestRandom = -1;
    for (int i = servers.length - 1; i >= 0; i--) {
      if (servers[i] == null || servers[i] == thisServer) {
        continue;
      }
      if (selectedIndex != -1
        && cluster.getNumRegionsComparator().compare(servers[i], servers[selectedIndex]) != 0) {
        // Exhausted servers of the same region count
        break;
      }
      // we don't know how many servers have the same region count, we will randomly select one
      // using a simplified inline reservoir sampling by assignmening a random number to  stream
      // data and choose the greatest one. (http://gregable.com/2007/10/reservoir-sampling.html)
      double currentRandom = ThreadLocalRandom.current().nextDouble();
      if (currentRandom > currentLargestRandom) {
        selectedIndex = i;
        currentLargestRandom = currentRandom;
      }
    }
    return selectedIndex == -1 ? -1 : servers[selectedIndex];
  }

}
