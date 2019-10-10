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
 * Duplicate network packages on a random regionserver.
 */
public class DuplicatePackagesCommandAction extends TCCommandAction {
  private static final Logger LOG = LoggerFactory.getLogger(DuplicatePackagesCommandAction.class);
  private float ratio;
  private long duration;

  /**
   * Duplicate network packages on a random regionserver.
   *
   * @param ratio the ratio of packages duplicated
   * @param duration the time this issue persists in milliseconds
   * @param timeout the timeout for executing required commands on the region server in milliseconds
   */
  public DuplicatePackagesCommandAction(float ratio, long duration, long timeout, String network) {
    super(timeout, network);
    this.ratio = ratio;
    this.duration = duration;
  }

  protected void localPerform() throws IOException {
    LOG.info("Starting to execute DuplicatePackagesCommandAction");
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

    LOG.info("Finished to execute DuplicatePackagesCommandAction");
  }

  private String getCommand(String operation){
    return String.format("tc qdisc %s dev %s root netem duplicate %s%%", operation, network,
        ratio * 100);
  }
}
