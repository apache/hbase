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

package org.apache.hadoop.hbase.chaos.actions;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;

/**
* Action that tries to unbalance the regions of a cluster.
*/
public class UnbalanceRegionsAction extends Action {
  private double fractionOfRegions;
  private double fractionOfServers;

  /**
   * Unbalances the regions on the cluster by choosing "target" servers, and moving
   * some regions from each of the non-target servers to random target servers.
   * @param fractionOfRegions Fraction of regions to move from each server.
   * @param fractionOfServers Fraction of servers to be chosen as targets.
   */
  public UnbalanceRegionsAction(double fractionOfRegions, double fractionOfServers) {
    this.fractionOfRegions = fractionOfRegions;
    this.fractionOfServers = fractionOfServers;
  }

  @Override
  public void perform() throws Exception {
    LOG.info("Unbalancing regions");
    ClusterStatus status = this.cluster.getClusterStatus();
    List<ServerName> victimServers = new LinkedList<ServerName>(status.getServers());
    int targetServerCount = (int)Math.ceil(fractionOfServers * victimServers.size());
    List<ServerName> targetServers = new ArrayList<ServerName>(targetServerCount);
    for (int i = 0; i < targetServerCount; ++i) {
      int victimIx = RandomUtils.nextInt(victimServers.size());
      targetServers.add(victimServers.remove(victimIx));
    }
    unbalanceRegions(status, victimServers, targetServers, fractionOfRegions);
  }
}
