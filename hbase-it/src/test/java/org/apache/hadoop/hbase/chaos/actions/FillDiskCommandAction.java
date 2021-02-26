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
 * Fill the disk on a random regionserver.
 */
public class FillDiskCommandAction extends SudoCommandAction {
  private static final Logger LOG = LoggerFactory.getLogger(FillDiskCommandAction.class);
  private final long size;
  private final long duration;
  private final String path;

  /**
   * Fill the disk on a random regionserver.
   * Please note that the file will be created regardless of the set duration or timeout.
   * So please use timeout and duration big enough to avoid complication caused by retries.
   *
   * @param size size of the generated file in MB or fill the disk if set to 0
   * @param duration the time this issue persists in milliseconds
   * @param path the path to the generated file
   * @param timeout the timeout for executing required commands on the region server in milliseconds
   */
  public FillDiskCommandAction(long size, long duration, String path, long timeout) {
    super(timeout);
    this.size = size;
    this.duration = duration;
    this.path = path;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  protected void localPerform() throws IOException {
    getLogger().info("Starting to execute FillDiskCommandAction");
    ServerName server = PolicyBasedChaosMonkey.selectRandomItem(getCurrentServers());
    String hostname = server.getHostname();

    try {
      clusterManager.execSudo(hostname, duration, getFillCommand());
    } catch (IOException ex) {
      getLogger().info("Potential timeout. We try to stop the dd process on target machine");
      clusterManager.execSudoWithRetries(hostname, timeout, getStopCommand());
      throw ex;
    } finally {
      clusterManager.execSudoWithRetries(hostname, timeout, getClearCommand());
      getLogger().info("Finished to execute FillDiskCommandAction");
    }
  }

  private String getFillCommand(){
    if (size == 0){
      return String.format("dd if=/dev/urandom of=%s/garbage bs=1M iflag=fullblock", path);
    }
    return String.format("dd if=/dev/urandom of=%s/garbage bs=1M count=%s iflag=fullblock",
        path, size);
  }

  private String getClearCommand(){
    return String.format("rm -f %s/garbage", path);
  }

  private String getStopCommand() {
    return "killall dd";
  }
}
