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
package org.apache.hadoop.hbase;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helpers for testing HBase that do not depend on specific server/etc. things. The main difference
 * from {@link HBaseCommonTestingUtility} is that we can start a zookeeper cluster.
 */
@InterfaceAudience.Public
public class HBaseZKTestingUtility extends HBaseCommonTestingUtility {
  private MiniZooKeeperCluster zkCluster;

  /**
   * Set if we were passed a zkCluster. If so, we won't shutdown zk as part of general shutdown.
   */
  private boolean passedZkCluster;

  protected ZKWatcher zooKeeperWatcher;

  /** Directory (a subdirectory of dataTestDir) used by the dfs cluster if any */
  protected File clusterTestDir;

  public HBaseZKTestingUtility() {
    this(HBaseConfiguration.create());
  }

  public HBaseZKTestingUtility(Configuration conf) {
    super(conf);
  }

  /**
   * @return Where the cluster will write data on the local subsystem. Creates it if it does not
   *         exist already. A subdir of {@code HBaseCommonTestingUtility#getBaseTestDir()}
   */
  Path getClusterTestDir() {
    if (clusterTestDir == null) {
      setupClusterTestDir();
    }
    return new Path(clusterTestDir.getAbsolutePath());
  }

  /**
   * Creates a directory for the cluster, under the test data
   */
  protected void setupClusterTestDir() {
    if (clusterTestDir != null) {
      return;
    }

    // Using randomUUID ensures that multiple clusters can be launched by
    // a same test, if it stops & starts them
    Path testDir = getDataTestDir("cluster_" + getRandomUUID().toString());
    clusterTestDir = new File(testDir.toString()).getAbsoluteFile();
    // Have it cleaned up on exit
    boolean b = deleteOnExit();
    if (b) {
      clusterTestDir.deleteOnExit();
    }
    LOG.info("Created new mini-cluster data directory: " + clusterTestDir + ", deleteOnExit=" + b);
  }

  /**
   * Call this if you only want a zk cluster.
   * @see #shutdownMiniZKCluster()
   * @return zk cluster started.
   */
  public MiniZooKeeperCluster startMiniZKCluster() throws Exception {
    return startMiniZKCluster(1);
  }

  /**
   * Call this if you only want a zk cluster.
   * @see #shutdownMiniZKCluster()
   * @return zk cluster started.
   */
  public MiniZooKeeperCluster startMiniZKCluster(int zooKeeperServerNum, int... clientPortList)
      throws Exception {
    setupClusterTestDir();
    return startMiniZKCluster(clusterTestDir, zooKeeperServerNum, clientPortList);
  }

  /**
   * Start a mini ZK cluster. If the property "test.hbase.zookeeper.property.clientPort" is set the
   * port mentioned is used as the default port for ZooKeeper.
   */
  private MiniZooKeeperCluster startMiniZKCluster(File dir, int zooKeeperServerNum,
      int[] clientPortList) throws Exception {
    if (this.zkCluster != null) {
      throw new IOException("Cluster already running at " + dir);
    }
    this.passedZkCluster = false;
    this.zkCluster = new MiniZooKeeperCluster(this.getConfiguration());
    int defPort = this.conf.getInt("test.hbase.zookeeper.property.clientPort", 0);
    if (defPort > 0) {
      // If there is a port in the config file, we use it.
      this.zkCluster.setDefaultClientPort(defPort);
    }

    if (clientPortList != null) {
      // Ignore extra client ports
      int clientPortListSize = Math.min(clientPortList.length, zooKeeperServerNum);
      for (int i = 0; i < clientPortListSize; i++) {
        this.zkCluster.addClientPort(clientPortList[i]);
      }
    }
    int clientPort = this.zkCluster.startup(dir, zooKeeperServerNum);
    this.conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));
    return this.zkCluster;
  }

  public MiniZooKeeperCluster getZkCluster() {
    return zkCluster;
  }

  public void setZkCluster(MiniZooKeeperCluster zkCluster) {
    this.passedZkCluster = true;
    this.zkCluster = zkCluster;
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkCluster.getClientPort());
  }

  /**
   * Shuts down zk cluster created by call to {@link #startMiniZKCluster()} or does nothing.
   * @see #startMiniZKCluster()
   */
  public void shutdownMiniZKCluster() throws IOException {
    if (!passedZkCluster && this.zkCluster != null) {
      this.zkCluster.shutdown();
      this.zkCluster = null;
    }
  }

  /**
   * Returns a ZKWatcher instance. This instance is shared between HBaseTestingUtility instance
   * users. Don't close it, it will be closed automatically when the cluster shutdowns
   * @return The ZKWatcher instance.
   */
  public synchronized ZKWatcher getZooKeeperWatcher() throws IOException {
    if (zooKeeperWatcher == null) {
      zooKeeperWatcher = new ZKWatcher(conf, "testing utility", new Abortable() {
        @Override
        public void abort(String why, Throwable e) {
          throw new RuntimeException("Unexpected abort in HBaseZKTestingUtility:" + why, e);
        }

        @Override
        public boolean isAborted() {
          return false;
        }
      });
    }
    return zooKeeperWatcher;
  }

  /**
   * Gets a ZKWatcher.
   */
  public static ZKWatcher getZooKeeperWatcher(HBaseZKTestingUtility testUtil) throws IOException {
    return new ZKWatcher(testUtil.getConfiguration(), "unittest", new Abortable() {
      boolean aborted = false;

      @Override
      public void abort(String why, Throwable e) {
        aborted = true;
        throw new RuntimeException("Fatal ZK error, why=" + why, e);
      }

      @Override
      public boolean isAborted() {
        return aborted;
      }
    });
  }

  /**
   * @return True if we removed the test dirs
   */
  @Override
  public boolean cleanupTestDir() {
    boolean ret = super.cleanupTestDir();
    if (deleteDir(this.clusterTestDir)) {
      this.clusterTestDir = null;
      return ret;
    }
    return false;
  }
}
