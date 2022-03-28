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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.InterruptedIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of HMaster that always runs as a stand by and never transitions to active.
 */
public class AlwaysStandByHMaster extends HMaster {
  /**
   * An implementation of ActiveMasterManager that never transitions it's master to active state. It
   * always remains as a stand by master. With the master registry implementation (HBASE-18095) it
   * is expected to have at least one active / standby master always running at any point in time
   * since they serve as the gateway for client connections.
   *
   * With this implementation, tests can simulate the scenario of not having an active master yet
   * the client connections to the cluster succeed.
   */
  private static class AlwaysStandByMasterManager extends ActiveMasterManager {
    private static final Logger LOG =
        LoggerFactory.getLogger(AlwaysStandByMasterManager.class);

    AlwaysStandByMasterManager(ZKWatcher watcher, ServerName sn, Server master)
        throws InterruptedIOException {
      super(watcher, sn, master);
    }

    /**
     * An implementation that never transitions to an active master.
     */
    boolean blockUntilBecomingActiveMaster(int checkInterval, MonitoredTask startupStatus) {
      while (!(master.isAborted() || master.isStopped())) {
        startupStatus.setStatus("Forever looping to stay as a standby master.");
        try {
          activeMasterServerName = null;
          try {
            if (MasterAddressTracker.getMasterAddress(watcher) != null) {
              clusterHasActiveMaster.set(true);
            }
          } catch (IOException e) {
            // pass, we will get notified when some other active master creates the znode.
          }
          Threads.sleepWithoutInterrupt(1000);
        } catch (KeeperException e) {
          master.abort("Received an unexpected KeeperException, aborting", e);
          return false;
        }
        synchronized (this.clusterHasActiveMaster) {
          while (clusterHasActiveMaster.get() && !master.isStopped()) {
            try {
              clusterHasActiveMaster.wait(checkInterval);
            } catch (InterruptedException e) {
              // We expect to be interrupted when a master dies,
              //  will fall out if so
              LOG.debug("Interrupted waiting for master to die", e);
            }
          }
          if (clusterShutDown.get()) {
            this.master.stop(
                "Cluster went down before this master became active");
          }
        }
      }
      return false;
    }
  }

  public AlwaysStandByHMaster(Configuration conf) throws IOException {
    super(conf);
  }

  protected ActiveMasterManager createActiveMasterManager(ZKWatcher zk, ServerName sn,
      org.apache.hadoop.hbase.Server server) throws InterruptedIOException {
    return new AlwaysStandByMasterManager(zk, sn, server);
  }
}
