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

import java.io.IOException;
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
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * In a scenario of Replication based Disaster/Recovery, when hbase Master-Cluster crashes, this
 * tool is used to sync-up the delta from Master to Slave using the info from ZooKeeper. The tool
 * will run on Master-Cluser, and assume ZK, Filesystem and NetWork still available after hbase
 * crashes
 *
 * <pre>
 * hbase org.apache.hadoop.hbase.replication.regionserver.ReplicationSyncUp
 * </pre>
 */
@InterfaceAudience.Private
public class ReplicationSyncUp extends Configured implements Tool {

  private static final long SLEEP_TIME = 10000;

  /**
   * Main program
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(HBaseConfiguration.create(), new ReplicationSyncUp(), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    Abortable abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    };
    Configuration conf = getConf();
    try (ZKWatcher zkw =
      new ZKWatcher(conf, "syncupReplication" + System.currentTimeMillis(), abortable, true)) {
      Path walRootDir = FSUtils.getWALRootDir(conf);
      FileSystem fs = FSUtils.getWALFileSystem(conf);
      Path oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
      Path logDir = new Path(walRootDir, HConstants.HREGION_LOGDIR_NAME);

      System.out.println("Start Replication Server start");
      Replication replication = new Replication();
      replication.initialize(new DummyServer(zkw), fs, logDir, oldLogDir, null);
      ReplicationSourceManager manager = replication.getReplicationManager();
      manager.init().get();
      while (manager.activeFailoverTaskCount() > 0) {
        Thread.sleep(SLEEP_TIME);
      }
      while (manager.getOldSources().size() > 0) {
        Thread.sleep(SLEEP_TIME);
      }
      manager.join();
    } catch (InterruptedException e) {
      System.err.println("didn't wait long enough:" + e);
      return -1;
    }
    return 0;
  }

  class DummyServer implements Server {
    String hostname;
    ZKWatcher zkw;

    DummyServer(ZKWatcher zkw) {
      // an unique name in case the first run fails
      hostname = System.currentTimeMillis() + ".SyncUpTool.replication.org";
      this.zkw = zkw;
    }

    DummyServer(String hostname) {
      this.hostname = hostname;
    }

    @Override
    public Configuration getConfiguration() {
      return getConf();
    }

    @Override
    public ZKWatcher getZooKeeper() {
      return zkw;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
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
    public Connection getConnection() {
      return null;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }

    @Override
    public FileSystem getFileSystem() {
      return null;
    }

    @Override
    public boolean isStopping() {
      return false;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return null;
    }

    @Override
    public AsyncClusterConnection getAsyncClusterConnection() {
      return null;
    }
  }
}
