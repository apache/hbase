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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * In a scenario of Replication based Disaster/Recovery, when hbase
 * Master-Cluster crashes, this tool is used to sync-up the delta from Master to
 * Slave using the info from Zookeeper. The tool will run on Master-Cluser, and
 * assume ZK, Filesystem and NetWork still available after hbase crashes
 *
 * hbase org.apache.hadoop.hbase.replication.regionserver.ReplicationSyncUp
 */

public class ReplicationSyncUp extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(ReplicationSyncUp.class.getName());

  private static Configuration conf;

  private static final long SLEEP_TIME = 10000;

  // although the tool is designed to be run on command line
  // this api is provided for executing the tool through another app
  public static void setConfigure(Configuration config) {
    conf = config;
  }

  /**
   * Main program
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (conf == null) conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new ReplicationSyncUp(), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    Replication replication;
    ReplicationSourceManager manager;
    FileSystem fs;
    Path oldLogDir, logDir, rootDir;
    ZooKeeperWatcher zkw;

    Abortable abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    };

    zkw =
        new ZooKeeperWatcher(conf, "syncupReplication" + System.currentTimeMillis(), abortable,
            true);

    rootDir = FSUtils.getRootDir(conf);
    fs = FileSystem.get(conf);
    oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);

    System.out.println("Start Replication Server start");
    replication = new Replication(new DummyServer(zkw), fs, logDir, oldLogDir);
    manager = replication.getReplicationManager();
    manager.init();

    try {
      int numberOfOldSource = 1; // default wait once
      while (numberOfOldSource > 0) {
        Thread.sleep(SLEEP_TIME);
        numberOfOldSource = manager.getOldSources().size();
      }
    } catch (InterruptedException e) {
      System.err.println("didn't wait long enough:" + e);
      return (-1);
    }

    manager.join();
    zkw.close();

    return (0);
  }

  static class DummyServer implements Server {
    String hostname;
    ZooKeeperWatcher zkw;

    DummyServer(ZooKeeperWatcher zkw) {
      // an unique name in case the first run fails
      hostname = System.currentTimeMillis() + ".SyncUpTool.replication.org";
      this.zkw = zkw;
    }

    DummyServer(String hostname) {
      this.hostname = hostname;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return zkw;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override
    public MetaTableLocator getMetaTableLocator() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf(hostname, 1234, 1L);
    }

    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void stop(String why) {
    }

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public ClusterConnection getConnection() {
      return null;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }

    @Override
    public ClusterConnection getClusterConnection() {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
