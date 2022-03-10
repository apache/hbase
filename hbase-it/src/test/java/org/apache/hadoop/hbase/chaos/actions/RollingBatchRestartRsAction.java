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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Restarts a ratio of the regionservers in a rolling fashion. At each step, either kills a
 * server, or starts one, sleeping randomly (0-sleepTime) in between steps.
 * The parameter maxDeadServers limits the maximum number of servers that
 * can be down at the same time during rolling restarts.
 */
public class RollingBatchRestartRsAction extends BatchRestartRsAction {
  private static final Logger LOG = LoggerFactory.getLogger(RollingBatchRestartRsAction.class);
  protected int maxDeadServers; // number of maximum dead servers at any given time. Defaults to 5

  public RollingBatchRestartRsAction(long sleepTime, float ratio) {
    this(sleepTime, ratio, 5);
  }

  public RollingBatchRestartRsAction(long sleepTime, float ratio, int maxDeadServers) {
    super(sleepTime, ratio);
    this.maxDeadServers = maxDeadServers;
  }

  public RollingBatchRestartRsAction(long sleepTime, float ratio, int maxDeadServers,
    boolean skipMetaRS) {
    this(sleepTime, ratio, maxDeadServers);
    this.skipMetaRS = skipMetaRS;
  }

  enum KillOrStart {
    KILL,
    START
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    getLogger().info("Performing action: Rolling batch restarting {}% of region servers",
        (int)(ratio * 100));
    List<ServerName> selectedServers = selectServers();
    Queue<ServerName> serversToBeKilled = new LinkedList<>(selectedServers);
    LinkedList<ServerName> deadServers = new LinkedList<>();
    Random rand = ThreadLocalRandom.current();
    // loop while there are servers to be killed or dead servers to be restarted
    while ((!serversToBeKilled.isEmpty() || !deadServers.isEmpty())  && !context.isStopping()) {

      final KillOrStart action;
      if (serversToBeKilled.isEmpty()) { // no more servers to kill
        action = KillOrStart.START;
      } else if (deadServers.isEmpty()) {
        action = KillOrStart.KILL; // no more servers to start
      } else if (deadServers.size() >= maxDeadServers) {
        // we have too many dead servers. Don't kill any more
        action = KillOrStart.START;
      } else {
        // do a coin toss
        action = rand.nextBoolean() ? KillOrStart.KILL : KillOrStart.START;
      }

      ServerName server;

      switch (action) {
        case KILL:
          server = serversToBeKilled.remove();
          try {
            killRs(server);
          } catch (org.apache.hadoop.util.Shell.ExitCodeException e) {
            // We've seen this in test runs where we timeout but the kill went through. HBASE-9743
            // So, add to deadServers even if exception so the start gets called.
            getLogger().info("Problem killing but presume successful; code={}", e.getExitCode(), e);
          }
          deadServers.add(server);
          break;
        case START:
          server = Objects.requireNonNull(deadServers.peek());
          try {
            startRs(server);
            // only remove the server from the known dead list if `startRs` succeeds.
            deadServers.remove(server);
          } catch (org.apache.hadoop.util.Shell.ExitCodeException e) {
            // The start may fail but better to just keep going though we may lose server.
            // Shuffle the dead list to avoid getting stuck on a single stubborn host.
            Collections.shuffle(deadServers);
            getLogger().info(
              "Problem starting {}, will retry; code={}", server, e.getExitCode(), e);
          }
          break;
      }

      sleep(rand.nextInt((int)sleepTime));
    }
  }

  protected List<ServerName> selectServers() throws IOException {
    return PolicyBasedChaosMonkey.selectRandomItems(getCurrentServers(), ratio);
  }

  /**
   * Small test to ensure the class basically works.
   */
  public static void main(final String[] args) throws Exception {
    RollingBatchRestartRsAction action = new RollingBatchRestartRsAction(1, 1.0f) {
      private int invocations = 0;
      @Override
      protected ServerName[] getCurrentServers() {
        final int count = 4;
        List<ServerName> serverNames = new ArrayList<>(count);
        for (int i = 0; i < 4; i++) {
          serverNames.add(ServerName.valueOf(i + ".example.org", i, i));
        }
        return serverNames.toArray(new ServerName[0]);
      }

      @Override
      protected void killRs(ServerName server) throws IOException {
        LOG.info("Killed {}", server);
        if (this.invocations++ % 3 == 0) {
          throw new org.apache.hadoop.util.Shell.ExitCodeException(-1, "Failed");
        }
      }

      @Override
      protected void startRs(ServerName server) throws IOException {
        LOG.info("Started {}", server);
        if (this.invocations++ % 3 == 0) {
          throw new org.apache.hadoop.util.Shell.ExitCodeException(-1, "Failed");
        }
      }
    };

    action.perform();
  }
}
