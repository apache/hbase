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

import java.util.List;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;

/**
 * Restarts a ratio of the running regionservers at the same time
 */
public class BatchRestartRsAction extends RestartActionBaseAction {
  float ratio; //ratio of regionservers to restart

  public BatchRestartRsAction(long sleepTime, float ratio) {
    super(sleepTime);
    this.ratio = ratio;
  }

  @Override
  public void perform() throws Exception {
    LOG.info(String.format("Performing action: Batch restarting %d%% of region servers",
        (int)(ratio * 100)));
    List<ServerName> selectedServers = PolicyBasedChaosMonkey.selectRandomItems(getCurrentServers(),
        ratio);

    for (ServerName server : selectedServers) {
      LOG.info("Killing region server:" + server);
      cluster.killRegionServer(server);
    }

    for (ServerName server : selectedServers) {
      cluster.waitForRegionServerToStop(server, PolicyBasedChaosMonkey.TIMEOUT);
    }

    LOG.info("Killed " + selectedServers.size() + " region servers. Reported num of rs:"
        + cluster.getClusterStatus().getServersSize());

    sleep(sleepTime);

    for (ServerName server : selectedServers) {
      LOG.info("Starting region server:" + server.getHostname());
      cluster.startRegionServer(server.getHostname(), server.getPort());

    }
    for (ServerName server : selectedServers) {
      cluster.waitForRegionServerToStart(server.getHostname(), server.getPort(), PolicyBasedChaosMonkey.TIMEOUT);
    }
    LOG.info("Started " + selectedServers.size() +" region servers. Reported num of rs:"
        + cluster.getClusterStatus().getServersSize());
  }
}
