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

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;

/**
 * Restarts a ratio of the regionservers in a rolling fashion. At each step, either kills a
 * server, or starts one, sleeping randomly (0-sleepTime) in between steps.
 */
public class RollingBatchRestartRsAction extends BatchRestartRsAction {
  public RollingBatchRestartRsAction(long sleepTime, float ratio) {
    super(sleepTime, ratio);
  }

  @Override
  public void perform() throws Exception {
    LOG.info(String.format("Performing action: Rolling batch restarting %d%% of region servers",
        (int)(ratio * 100)));
    List<ServerName> selectedServers = PolicyBasedChaosMonkey.selectRandomItems(getCurrentServers(),
        ratio);

    Queue<ServerName> serversToBeKilled = new LinkedList<ServerName>(selectedServers);
    Queue<ServerName> deadServers = new LinkedList<ServerName>();

    //
    while (!serversToBeKilled.isEmpty() || !deadServers.isEmpty()) {
      boolean action = true; //action true = kill server, false = start server

      if (serversToBeKilled.isEmpty() || deadServers.isEmpty()) {
        action = deadServers.isEmpty();
      } else {
        action = RandomUtils.nextBoolean();
      }

      if (action) {
        ServerName server = serversToBeKilled.remove();
        killRs(server);
        deadServers.add(server);
      } else {
        ServerName server = deadServers.remove();
        startRs(server);
      }

      sleep(RandomUtils.nextInt((int)sleepTime));
    }
  }
}
