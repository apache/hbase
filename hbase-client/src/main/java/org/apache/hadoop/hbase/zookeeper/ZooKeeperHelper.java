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
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.common.base.Stopwatch;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.ZooKeeper;


/**
 * Methods that help working with ZooKeeper
 */
@InterfaceAudience.Private
public final class ZooKeeperHelper {
  // This class cannot be instantiated
  private ZooKeeperHelper() {
  }

  /**
   * Get a ZooKeeper instance and wait until it connected before returning.
   * @param sessionTimeoutMs Used as session timeout passed to the created ZooKeeper AND as the
   *   timeout to wait on connection establishment.
   */
  public static ZooKeeper getConnectedZooKeeper(String connectString, int sessionTimeoutMs)
      throws IOException {
    ZooKeeper zookeeper = new ZooKeeper(connectString, sessionTimeoutMs, e -> {});
    return ensureConnectedZooKeeper(zookeeper, sessionTimeoutMs);
  }

  /**
   * Ensure passed zookeeper is connected.
   * @param timeout Time to wait on established Connection
   */
  public static ZooKeeper ensureConnectedZooKeeper(ZooKeeper zookeeper, int timeout)
      throws ZooKeeperConnectionException {
    if (zookeeper.getState().isConnected()) {
      return zookeeper;
    }
    Stopwatch stopWatch = Stopwatch.createStarted();
    // Make sure we are connected before we hand it back.
    while(!zookeeper.getState().isConnected()) {
      Threads.sleep(1);
      if (stopWatch.elapsed(TimeUnit.MILLISECONDS) > timeout) {
        throw new ZooKeeperConnectionException("Failed connect after waiting " +
            stopWatch.elapsed(TimeUnit.MILLISECONDS) + "ms (zk session timeout); " +
            zookeeper);
      }
    }
    return zookeeper;
  }
}
