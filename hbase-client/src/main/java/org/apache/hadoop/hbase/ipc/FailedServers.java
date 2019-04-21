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
package org.apache.hadoop.hbase.ipc;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * A class to manage a list of servers that failed recently.
 */
@InterfaceAudience.Private
public class FailedServers {
  private final Map<String, Long> failedServers = new HashMap<String, Long>();
  private long latestExpiry = 0;
  private final int recheckServersTimeout;
  private static final Logger LOG = LoggerFactory.getLogger(FailedServers.class);

  public FailedServers(Configuration conf) {
    this.recheckServersTimeout = conf.getInt(
        RpcClient.FAILED_SERVER_EXPIRY_KEY, RpcClient.FAILED_SERVER_EXPIRY_DEFAULT);
  }

  /**
   * Add an address to the list of the failed servers list.
   */
  public synchronized void addToFailedServers(InetSocketAddress address, Throwable throwable) {
    final long expiry = EnvironmentEdgeManager.currentTime() + recheckServersTimeout;
    this.failedServers.put(address.toString(), expiry);
    this.latestExpiry = expiry;
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Added failed server with address " + address.toString() + " to list caused by "
            + throwable.toString());
    }
  }

  /**
   * Check if the server should be considered as bad. Clean the old entries of the list.
   *
   * @return true if the server is in the failed servers list
   */
  public synchronized boolean isFailedServer(final InetSocketAddress address) {
    if (failedServers.isEmpty()) {
      return false;
    }
    final long now = EnvironmentEdgeManager.currentTime();
    if (now > this.latestExpiry) {
      failedServers.clear();
      return false;
    }
    String key = address.toString();
    Long expiry = this.failedServers.get(key);
    if (expiry == null) {
      return false;
    }
    if (expiry >= now) {
      return true;
    } else {
      this.failedServers.remove(key);
    }
    return false;
  }
}
