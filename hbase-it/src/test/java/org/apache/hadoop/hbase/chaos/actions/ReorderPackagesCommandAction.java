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

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Reorder network packages on a random regionserver.
 */
public class ReorderPackagesCommandAction extends TCCommandAction {
  private static final Logger LOG = LoggerFactory.getLogger(ReorderPackagesCommandAction.class);
  private float ratio;
  private long duration;
  private long delay;

  /**
   * Reorder network packages on a random regionserver.
   *
   * @param ratio the ratio of packages reordered
   * @param duration the time this issue persists in milliseconds
   * @param delay the delay between reordered and non-reordered packages in milliseconds
   * @param timeout the timeout for executing required commands on the region server in milliseconds
   * @param network network interface the regionserver uses for communication
   */
  public ReorderPackagesCommandAction(float ratio, long duration, long delay, long timeout,
      String network) {
    super(timeout, network);
    this.ratio = ratio;
    this.duration = duration;
    this.delay = delay;
  }

  protected void localPerform() throws IOException {
    LOG.info("Starting to execute ReorderPackagesCommandAction");
    ServerName server = PolicyBasedChaosMonkey.selectRandomItem(getCurrentServers());
    String hostname = server.getHostname();

    try {
      clusterManager.execSudoWithRetries(hostname, timeout, getCommand(ADD));
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      LOG.debug("Failed to run the command for the full duration", e);
    } finally {
      clusterManager.execSudoWithRetries(hostname, timeout, getCommand(DELETE));
    }

    LOG.info("Finished to execute ReorderPackagesCommandAction");
  }

  private String getCommand(String operation){
    return String.format("tc qdisc %s dev %s root netem delay %sms reorder %s%% 50%",
        operation, network, delay, ratio * 100);
  }
}
