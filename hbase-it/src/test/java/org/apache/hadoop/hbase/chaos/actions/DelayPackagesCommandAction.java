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
 * Action adds latency to communication on a random regionserver.
 */
public class DelayPackagesCommandAction extends TCCommandAction {
  private static final Logger LOG = LoggerFactory.getLogger(DelayPackagesCommandAction.class);
  private long delay;
  private long duration;

  /**
   * Adds latency to communication on a random region server
   *
   * @param delay the latency wil be delay +/-50% in milliseconds
   * @param duration the time this issue persists in milliseconds
   * @param timeout the timeout for executing required commands on the region server in milliseconds
   * @param network network interface the regionserver uses for communication
   */
  public DelayPackagesCommandAction(long delay, long duration, long timeout, String network) {
    super(timeout, network);
    this.delay = delay;
    this.duration = duration;
  }

  protected void localPerform() throws IOException {
    LOG.info("Starting to execute DelayPackagesCommandAction");
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

    LOG.info("Finished to execute DelayPackagesCommandAction");
  }

  private String getCommand(String operation){
    return String.format("tc qdisc %s dev %s root netem delay %sms %sms",
        operation, network, delay, delay/2);
  }
}
