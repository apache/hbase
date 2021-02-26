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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Restarts a ratio of the running regionservers at the same time
 */
public class BatchRestartRsAction extends RestartActionBaseAction {
  float ratio; //ratio of regionservers to restart
  private static final Logger LOG = LoggerFactory.getLogger(BatchRestartRsAction.class);

  public BatchRestartRsAction(long sleepTime, float ratio) {
    super(sleepTime);
    this.ratio = ratio;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    getLogger().info(String.format("Performing action: Batch restarting %d%% of region servers",
        (int)(ratio * 100)));
    List<ServerName> selectedServers = PolicyBasedChaosMonkey.selectRandomItems(getCurrentServers(),
        ratio);

    Set<ServerName> killedServers = new HashSet<>();

    for (ServerName server : selectedServers) {
      // Don't keep killing servers if we're
      // trying to stop the monkey.
      if (context.isStopping()) {
        break;
      }
      getLogger().info("Killing region server:" + server);
      cluster.killRegionServer(server);
      killedServers.add(server);
    }

    for (ServerName server : killedServers) {
      cluster.waitForRegionServerToStop(server, PolicyBasedChaosMonkey.TIMEOUT);
    }

    getLogger().info("Killed " + killedServers.size() + " region servers. Reported num of rs:"
        + cluster.getClusterMetrics().getLiveServerMetrics().size());

    sleep(sleepTime);

    for (ServerName server : killedServers) {
      getLogger().info("Starting region server:" + server.getHostname());
      cluster.startRegionServer(server.getHostname(), server.getPort());

    }
    for (ServerName server : killedServers) {
      cluster.waitForRegionServerToStart(server.getHostname(),
          server.getPort(),
          PolicyBasedChaosMonkey.TIMEOUT);
    }
    getLogger().info("Started " + killedServers.size() +" region servers. Reported num of rs:"
        + cluster.getClusterMetrics().getLiveServerMetrics().size());
  }
}
