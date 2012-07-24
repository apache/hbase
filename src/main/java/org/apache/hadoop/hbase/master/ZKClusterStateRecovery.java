/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.KeeperException.NoNodeException;

/**
 * A utility to recover previous cluster state from ZK on master startup.
 */
public class ZKClusterStateRecovery {

  private static final Log LOG =
      LogFactory.getLog(ZKClusterStateRecovery.class.getName());

  /**
   * We read the list of live region servers from ZK at startup. This is used
   * to decide what logs to split and to select regionservers to assign ROOT
   * and META on master failover.
   */
  private Set<String> liveRSNamesAtStartup;

  /** An unmodifiable wrapper around {@link #liveRSNamesAtStartup} */
  private Set<String> liveRSNamesAtStartupUnmodifiable;

  private final HMaster master;
  private final ZooKeeperWrapper zkw;

  public ZKClusterStateRecovery(HMaster master, ServerConnection connection) {
    this.master = master;
    zkw = master.getZooKeeperWrapper();
  }

  /**
   * Register live regionservers that we read from ZK with ServerManager. We do this after starting
   * RPC threads but before log splitting.
   */
  void registerLiveRegionServers() throws IOException {
    liveRSNamesAtStartup = zkw.getLiveRSNames();
    liveRSNamesAtStartupUnmodifiable = Collections.unmodifiableSet(liveRSNamesAtStartup);

    Set<String> rsNamesToAdd = new TreeSet<String>(liveRSNamesAtStartup);

    boolean needToSleep = false;  // no need to sleep at the first iteration

    while (!rsNamesToAdd.isEmpty()) {
      if (needToSleep) {
        Threads.sleepRetainInterrupt(HConstants.SOCKET_RETRY_WAIT_MS);
      }

      // We cannot modify rsNamesToAdd as we iterate it, so we add RS names to retry to this list.
      Set<String> rsLeftToAdd = new TreeSet<String>();

      for (String rsName : rsNamesToAdd) {
        if (master.isStopped()) {
          throw new IOException("Master is shutting down");
        }
        if (Thread.interrupted()) {
          throw new IOException("Interrupted when scanning live RS directory in ZK");
        }

        HServerInfo serverInfo;
        try {
          serverInfo = HServerInfo.fromServerName(rsName);
        } catch (IllegalArgumentException ex) {
          // This error is probably not going to fix itself automatically. Exit the retry loop.
          throw new IOException("Read invalid server name for live RS directory in ZK: " + rsName,
              ex);
        }

        try {
          master.getServerManager().recordNewServer(serverInfo);
        } catch (IOException ex) {
          if (ex.getCause() instanceof NoNodeException) {
            // This regionserver has disappeared, don't try to register it. This will also ensure
            // that we split the logs for this regionserver as part of initial log splitting.
            LOG.info("Regionserver znode " + rsName + " disappeared, not registering");
            liveRSNamesAtStartup.remove(rsName);
          } else {
            LOG.error("Error recording a new regionserver: " + serverInfo.getServerName()
                + ", will retry", ex);
            rsLeftToAdd.add(rsName);
          }
        }
      }

      rsNamesToAdd = rsLeftToAdd;
      needToSleep = true;  // will sleep before the re-trying
    }
  }

  public Set<String> liveRegionServersAtStartup() {
    return liveRSNamesAtStartupUnmodifiable;
  }

}
