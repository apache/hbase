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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;

/**
 * Restarts a ratio of the regionservers in a rolling fashion. At each step, either kills a
 * server, or starts one, sleeping randomly (0-sleepTime) in between steps.
 */
public class RollingBatchRestartRsAction extends BatchRestartRsAction {
  private static Log LOG = LogFactory.getLog(RollingBatchRestartRsAction.class);

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
        try {
          killRs(server);
        } catch (org.apache.hadoop.util.Shell.ExitCodeException e) {
          // We've seen this in test runs where we timeout but the kill went through. HBASE-9743
          // So, add to deadServers even if exception so the start gets called.
          LOG.info("Problem killing but presume successful; code=" + e.getExitCode(), e);
        }
        deadServers.add(server);
      } else {
        try {
          ServerName server = deadServers.remove();
          startRs(server);
        } catch (org.apache.hadoop.util.Shell.ExitCodeException e) {
          // The start may fail but better to just keep going though we may lose server.
          //
          LOG.info("Problem starting, will retry; code=" + e.getExitCode(), e);
        }
      }

      sleep(RandomUtils.nextInt((int)sleepTime));
    }
  }

  /**
   * Small test to ensure the class basically works.
   * @param args
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    RollingBatchRestartRsAction action = new RollingBatchRestartRsAction(1, 1.0f) {
      private int invocations = 0;
      @Override
      protected ServerName[] getCurrentServers() throws IOException {
        final int count = 4;
        List<ServerName> serverNames = new ArrayList<ServerName>(count);
        for (int i = 0; i < 4; i++) {
          serverNames.add(ServerName.valueOf(i + ".example.org", i, i));
        }
        return serverNames.toArray(new ServerName [] {});
      }

      @Override
      protected void killRs(ServerName server) throws IOException {
        LOG.info("Killed " + server);
        if (this.invocations++ % 3 == 0) {
          throw new org.apache.hadoop.util.Shell.ExitCodeException(-1, "Failed");
        }
      }

      @Override
      protected void startRs(ServerName server) throws IOException {
        LOG.info("Started " + server);
        if (this.invocations++ % 3 == 0) {
          throw new org.apache.hadoop.util.Shell.ExitCodeException(-1, "Failed");
        }
      }
    };

    action.perform();
  }
}