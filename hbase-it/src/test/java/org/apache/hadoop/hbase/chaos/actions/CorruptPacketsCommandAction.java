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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Corrupt network packets on a random regionserver.
 */
public class CorruptPacketsCommandAction extends TCCommandAction {
  private static final Logger LOG = LoggerFactory.getLogger(CorruptPacketsCommandAction.class);
  private final float ratio;
  private final long duration;

  /**
   * Corrupt network packets on a random regionserver.
   *
   * @param ratio the ratio of packets corrupted
   * @param duration the time this issue persists in milliseconds
   * @param timeout the timeout for executing required commands on the region server in milliseconds
   * @param network network interface the regionserver uses for communication
   */
  public CorruptPacketsCommandAction(float ratio, long duration, long timeout, String network) {
    super(timeout, network);
    this.ratio = ratio;
    this.duration = duration;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  protected void localPerform() throws IOException {
    getLogger().info("Starting to execute CorruptPacketsCommandAction");
    ServerName server = PolicyBasedChaosMonkey.selectRandomItem(getCurrentServers());
    String hostname = server.getHostname();

    try {
      clusterManager.execSudoWithRetries(hostname, timeout, getCommand(ADD));
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      getLogger().debug("Failed to run the command for the full duration", e);
    } finally {
      clusterManager.execSudoWithRetries(hostname, timeout, getCommand(DELETE));
    }

    getLogger().info("Finished to execute CorruptPacketsCommandAction");
  }

  private String getCommand(String operation){
    return String.format("tc qdisc %s dev %s root netem corrupt %s%%", operation, network,
        ratio * 100);
  }
}
