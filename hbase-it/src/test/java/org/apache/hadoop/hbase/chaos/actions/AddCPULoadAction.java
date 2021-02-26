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
 * Action that adds high cpu load to a random regionserver for a given duration
 */
public class AddCPULoadAction extends SudoCommandAction {
  private static final Logger LOG = LoggerFactory.getLogger(AddCPULoadAction.class);
  private static final String CPU_LOAD_COMMAND =
      "seq 1 %s | xargs -I{} -n 1 -P %s timeout %s dd if=/dev/urandom of=/dev/null bs=1M " +
          "iflag=fullblock";

  private final long duration;
  private final long processes;

  /**
   * Add high load to cpu
   *
   * @param duration  Duration that this thread should generate the load for in milliseconds
   * @param processes The number of parallel processes, should be equal to cpu threads for max load
   */
  public AddCPULoadAction(long duration, long processes, long timeout) {
    super(timeout);
    this.duration = duration;
    this.processes = processes;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  protected void localPerform() throws IOException {
    getLogger().info("Starting to execute AddCPULoadAction");
    ServerName server = PolicyBasedChaosMonkey.selectRandomItem(getCurrentServers());
    String hostname = server.getHostname();

    try {
      clusterManager.execSudo(hostname, timeout, getCommand());
    } catch (IOException ex){
      //This will always happen. We use timeout to kill a continuously running process
      //after the duration expires
    }
    getLogger().info("Finished to execute AddCPULoadAction");
  }

  private String getCommand(){
    return String.format(CPU_LOAD_COMMAND, processes, processes, duration/1000f);
  }
}
