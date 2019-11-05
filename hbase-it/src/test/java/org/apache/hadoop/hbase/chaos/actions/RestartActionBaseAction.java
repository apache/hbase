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
import org.apache.hadoop.hbase.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Base class for restarting HBaseServer's
*/
public class RestartActionBaseAction extends Action {
  private static final Logger LOG =
          LoggerFactory.getLogger(RestartActionBaseAction.class);
  long sleepTime; // how long should we sleep

  public RestartActionBaseAction(long sleepTime) {
    this.sleepTime = sleepTime;
  }

  void sleep(long sleepTime) {
    LOG.info("Sleeping for:" + sleepTime);
    Threads.sleep(sleepTime);
  }

  void restartMaster(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the kill if we're stopping
    if (context.isStopping()) {
      return;
    }

    LOG.info("Killing master: {}", server);
    killMaster(server);
    sleep(sleepTime);
    LOG.info("Starting master: {}", server);
    startMaster(server);
  }

  /**
   * Stop and then restart the region server instead of killing it.
   * @param server hostname to restart the regionserver on
   * @param sleepTime number of milliseconds between stop and restart
   * @throws IOException if something goes wrong
   */
  void gracefulRestartRs(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the stop if we're stopping already
    if (context.isStopping()) {
      return;
    }
    LOG.info("Stopping region server: {}", server);
    stopRs(server);
    sleep(sleepTime);
    LOG.info("Starting region server: {}", server);
    startRs(server);
  }

  void restartRs(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the kill if we're stopping
    if (context.isStopping()) {
      return;
    }
    LOG.info("Killing region server: {}", server);
    killRs(server);
    sleep(sleepTime);
    LOG.info("Starting region server: {}", server);
    startRs(server);
  }

  void restartZKNode(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the kill if we're stopping
    if (context.isStopping()) {
      return;
    }
    LOG.info("Killing zookeeper node: {}", server);
    killZKNode(server);
    sleep(sleepTime);
    LOG.info("Starting zookeeper node: {}", server);
    startZKNode(server);
  }

  void restartDataNode(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the kill if we're stopping
    if (context.isStopping()) {
      return;
    }
    LOG.info("Killing data node: {}", server);
    killDataNode(server);
    sleep(sleepTime);
    LOG.info("Starting data node: {}", server);
    startDataNode(server);
  }

  void restartNameNode(ServerName server, long sleepTime) throws IOException {
    sleepTime = Math.max(sleepTime, 1000);
    // Don't try the kill if we're stopping
    if (context.isStopping()) {
      return;
    }
    LOG.info("Killing name node: {}", server);
    killNameNode(server);
    sleep(sleepTime);
    LOG.info("Starting name node: {}", server);
    startNameNode(server);
  }

}
