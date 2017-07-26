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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

/**
 * A class to manage a list of servers that failed recently.
 */
@InterfaceAudience.Private
public class FailedServers {
  private final LinkedList<Pair<Long, String>> failedServers = new
      LinkedList<Pair<Long, String>>();
  private final int recheckServersTimeout;
  private static final Log LOG = LogFactory.getLog(FailedServers.class);

  public FailedServers(Configuration conf) {
    this.recheckServersTimeout = conf.getInt(
        RpcClient.FAILED_SERVER_EXPIRY_KEY, RpcClient.FAILED_SERVER_EXPIRY_DEFAULT);
  }

  /**
   * Add an address to the list of the failed servers list.
   */
  public synchronized void addToFailedServers(InetSocketAddress address, Throwable throwable) {
    final long expiry = EnvironmentEdgeManager.currentTime() + recheckServersTimeout;
    failedServers.addFirst(new Pair<Long, String>(expiry, address.toString()));
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

    final String lookup = address.toString();
    final long now = EnvironmentEdgeManager.currentTime();

    // iterate, looking for the search entry and cleaning expired entries
    Iterator<Pair<Long, String>> it = failedServers.iterator();
    while (it.hasNext()) {
      Pair<Long, String> cur = it.next();
      if (cur.getFirst() < now) {
        it.remove();
      } else {
        if (lookup.equals(cur.getSecond())) {
          return true;
        }
      }
    }

    return false;
  }
}
