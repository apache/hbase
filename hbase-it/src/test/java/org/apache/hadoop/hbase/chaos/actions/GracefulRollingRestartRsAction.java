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
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.RegionMover;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gracefully restarts every regionserver in a rolling fashion. At each step, it unloads,
 * restarts the loads every rs server sleeping randomly (0-sleepTime) in between servers.
 */
public class GracefulRollingRestartRsAction extends RestartActionBaseAction {
  private static final Logger LOG = LoggerFactory.getLogger(GracefulRollingRestartRsAction.class);

  public GracefulRollingRestartRsAction(long sleepTime) {
    super(sleepTime);
  }

  @Override
  public void perform() throws Exception {
    LOG.info("Performing action: Rolling restarting non-master region servers");
    List<ServerName> selectedServers = selectServers();

    LOG.info("Disabling balancer to make unloading possible");
    setBalancer(false, true);

    for (ServerName server : selectedServers) {
      String rsName = server.getAddress().toString();
      try (RegionMover rm =
          new RegionMover.RegionMoverBuilder(rsName, getConf()).ack(true).build()) {
        LOG.info("Unloading {}", server);
        rm.unload();
        LOG.info("Restarting {}", server);
        gracefulRestartRs(server, sleepTime);
        LOG.info("Loading {}", server);
        rm.load();
      } catch (Shell.ExitCodeException e) {
        LOG.info("Problem restarting but presume successful; code={}", e.getExitCode(), e);
      }
      sleep(RandomUtils.nextInt(0, (int)sleepTime));
    }
    LOG.info("Enabling balancer");
    setBalancer(true, true);
  }

  protected List<ServerName> selectServers() throws IOException {
    return Arrays.asList(getCurrentServers());
  }

}
