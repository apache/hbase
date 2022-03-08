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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Suspend then resume a ratio of the regionservers in a rolling fashion. At each step, either
 * suspend a server, or resume one, sleeping (sleepTime) in between steps. The parameter
 * maxSuspendedServers limits the maximum number of servers that can be down at the same time
 * during rolling restarts.
 */
public class RollingBatchSuspendResumeRsAction extends Action {
  private static final Logger LOG =
      LoggerFactory.getLogger(RollingBatchSuspendResumeRsAction.class);
  private final float ratio;
  private final long sleepTime;
  private final int maxSuspendedServers; // number of maximum suspended servers at any given time.

  public RollingBatchSuspendResumeRsAction(long sleepTime, float ratio) {
    this(sleepTime, ratio, 5);
  }

  public RollingBatchSuspendResumeRsAction(long sleepTime, float ratio, int maxSuspendedServers) {
    this.ratio = ratio;
    this.sleepTime = sleepTime;
    this.maxSuspendedServers = maxSuspendedServers;
  }

  enum SuspendOrResume {
    SUSPEND, RESUME
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    getLogger().info("Performing action: Rolling batch restarting {}% of region servers",
      (int) (ratio * 100));
    List<ServerName> selectedServers = selectServers();
    Queue<ServerName> serversToBeSuspended = new LinkedList<>(selectedServers);
    Queue<ServerName> suspendedServers = new LinkedList<>();
    Random rand = ThreadLocalRandom.current();
    // loop while there are servers to be suspended or suspended servers to be resumed
    while ((!serversToBeSuspended.isEmpty() || !suspendedServers.isEmpty()) && !context
        .isStopping()) {

      final SuspendOrResume action;
      if (serversToBeSuspended.isEmpty()) { // no more servers to suspend
        action = SuspendOrResume.RESUME;
      } else if (suspendedServers.isEmpty()) {
        action = SuspendOrResume.SUSPEND; // no more servers to resume
      } else if (suspendedServers.size() >= maxSuspendedServers) {
        // we have too many suspended servers. Don't suspend any more
        action = SuspendOrResume.RESUME;
      } else {
        // do a coin toss
        action = rand.nextBoolean() ? SuspendOrResume.SUSPEND : SuspendOrResume.RESUME;
      }

      ServerName server;
      switch (action) {
        case SUSPEND:
          server = serversToBeSuspended.remove();
          try {
            suspendRs(server);
          } catch (Shell.ExitCodeException e) {
            LOG.warn("Problem suspending but presume successful; code={}", e.getExitCode(), e);
          }
          suspendedServers.add(server);
          break;
        case RESUME:
          server = suspendedServers.remove();
          try {
            resumeRs(server);
          } catch (Shell.ExitCodeException e) {
            LOG.info("Problem resuming, will retry; code={}", e.getExitCode(), e);
          }
          break;
      }

      getLogger().info("Sleeping for:{}", sleepTime);
      Threads.sleep(sleepTime);
    }
  }

  protected List<ServerName> selectServers() throws IOException {
    return PolicyBasedChaosMonkey.selectRandomItems(getCurrentServers(), ratio);
  }

}
