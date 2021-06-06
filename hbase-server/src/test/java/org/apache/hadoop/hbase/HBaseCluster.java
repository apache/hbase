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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;

/**
 * This class defines methods that can help with managing HBase clusters
 * from unit tests and system tests. There are 3 types of cluster deployments:
 * <ul>
 * <li><b>MiniHBaseCluster:</b> each server is run in the same JVM in separate threads,
 * used by unit tests</li>
 * <li><b>DistributedHBaseCluster:</b> the cluster is pre-deployed, system and integration tests can
 * interact with the cluster. </li>
 * <li><b>ProcessBasedLocalHBaseCluster:</b> each server is deployed locally but in separate
 * JVMs. </li>
 * </ul>
 * <p>
 * HBaseCluster unifies the way tests interact with the cluster, so that the same test can
 * be run against a mini-cluster during unit test execution, or a distributed cluster having
 * tens/hundreds of nodes during execution of integration tests.
 *
 * <p>
 * HBaseCluster exposes client-side public interfaces to tests, so that tests does not assume
 * running in a particular mode. Not all the tests are suitable to be run on an actual cluster,
 * and some tests will still need to mock stuff and introspect internal state. For those use
 * cases from unit tests, or if more control is needed, you can use the subclasses directly.
 * In that sense, this class does not abstract away <strong>every</strong> interface that
 * MiniHBaseCluster or DistributedHBaseCluster provide.
 */
@InterfaceAudience.Public
public abstract class HBaseCluster implements Closeable, Configurable {
  // Log is being used in DistributedHBaseCluster class, hence keeping it as package scope
  static final Logger LOG = LoggerFactory.getLogger(HBaseCluster.class.getName());
  protected Configuration conf;

  /** the status of the cluster before we begin */
  protected ClusterMetrics initialClusterStatus;

  /**
   * Construct an HBaseCluster
   * @param conf Configuration to be used for cluster
   */
  public HBaseCluster(Configuration conf) {
    setConf(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Returns a ClusterMetrics for this HBase cluster.
   * @see #getInitialClusterMetrics()
   */
  public abstract ClusterMetrics getClusterMetrics() throws IOException;

  /**
   * Returns a ClusterStatus for this HBase cluster as observed at the
   * starting of the HBaseCluster
   */
  public ClusterMetrics getInitialClusterMetrics() throws IOException {
    return initialClusterStatus;
  }

  /**
   * Returns an {@link MasterService.BlockingInterface} to the active master
   */
  public abstract MasterService.BlockingInterface getMasterAdminService()
  throws IOException;

  /**
   * Returns an AdminProtocol interface to the regionserver
   */
  public abstract AdminService.BlockingInterface getAdminProtocol(ServerName serverName)
  throws IOException;

  /**
   * Returns a ClientProtocol interface to the regionserver
   */
  public abstract ClientService.BlockingInterface getClientProtocol(ServerName serverName)
  throws IOException;

  /**
   * Starts a new region server on the given hostname or if this is a mini/local cluster,
   * starts a region server locally.
   * @param hostname the hostname to start the regionserver on
   * @throws IOException if something goes wrong
   */
  public abstract void startRegionServer(String hostname, int port) throws IOException;

  /**
   * Kills the region server process if this is a distributed cluster, otherwise
   * this causes the region server to exit doing basic clean up only.
   * @throws IOException if something goes wrong
   */
  public abstract void killRegionServer(ServerName serverName) throws IOException;

  /**
   * Keeping track of killed servers and being able to check if a particular server was killed makes
   * it possible to do fault tolerance testing for dead servers in a deterministic way. A concrete
   * example of such case is - killing servers and waiting for all regions of a particular table
   * to be assigned. We can check for server column in META table and that its value is not one
   * of the killed servers.
   */
  public abstract boolean isKilledRS(ServerName serverName);

  /**
   * Stops the given region server, by attempting a gradual stop.
   * @throws IOException if something goes wrong
   */
  public abstract void stopRegionServer(ServerName serverName) throws IOException;

  /**
   * Wait for the specified region server to join the cluster
   * @throws IOException if something goes wrong or timeout occurs
   */
  public void waitForRegionServerToStart(String hostname, int port, long timeout)
      throws IOException {
    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < timeout) {
      for (ServerName server : getClusterMetrics().getLiveServerMetrics().keySet()) {
        if (server.getHostname().equals(hostname) && server.getPort() == port) {
          return;
        }
      }
      Threads.sleep(100);
    }
    throw new IOException("did timeout " + timeout + "ms waiting for region server to start: "
        + hostname);
  }

  /**
   * Wait for the specified region server to stop the thread / process.
   * @throws IOException if something goes wrong or timeout occurs
   */
  public abstract void waitForRegionServerToStop(ServerName serverName, long timeout)
      throws IOException;

  /**
   * Suspend the region server
   * @param serverName the hostname to suspend the regionserver on
   * @throws IOException if something goes wrong
   */
  public abstract void suspendRegionServer(ServerName serverName) throws IOException;

  /**
   * Resume the region server
   * @param serverName the hostname to resume the regionserver on
   * @throws IOException if something goes wrong
   */
  public abstract void resumeRegionServer(ServerName serverName) throws IOException;

  /**
   * Starts a new zookeeper node on the given hostname or if this is a mini/local cluster,
   * silently logs warning message.
   * @param hostname the hostname to start the regionserver on
   * @throws IOException if something goes wrong
   */
  public abstract void startZkNode(String hostname, int port) throws IOException;

  /**
   * Kills the zookeeper node process if this is a distributed cluster, otherwise,
   * this causes master to exit doing basic clean up only.
   * @throws IOException if something goes wrong
   */
  public abstract void killZkNode(ServerName serverName) throws IOException;

  /**
   * Stops the region zookeeper if this is a distributed cluster, otherwise
   * silently logs warning message.
   * @throws IOException if something goes wrong
   */
  public abstract void stopZkNode(ServerName serverName) throws IOException;

  /**
   * Wait for the specified zookeeper node to join the cluster
   * @throws IOException if something goes wrong or timeout occurs
   */
  public abstract void waitForZkNodeToStart(ServerName serverName, long timeout)
    throws IOException;

  /**
   * Wait for the specified zookeeper node to stop the thread / process.
   * @throws IOException if something goes wrong or timeout occurs
   */
  public abstract void waitForZkNodeToStop(ServerName serverName, long timeout)
    throws IOException;

  /**
   * Starts a new datanode on the given hostname or if this is a mini/local cluster,
   * silently logs warning message.
   * @throws IOException if something goes wrong
   */
  public abstract void startDataNode(ServerName serverName) throws IOException;

  /**
   * Kills the datanode process if this is a distributed cluster, otherwise,
   * this causes master to exit doing basic clean up only.
   * @throws IOException if something goes wrong
   */
  public abstract void killDataNode(ServerName serverName) throws IOException;

  /**
   * Stops the datanode if this is a distributed cluster, otherwise
   * silently logs warning message.
   * @throws IOException if something goes wrong
   */
  public abstract void stopDataNode(ServerName serverName) throws IOException;

  /**
   * Wait for the specified datanode to join the cluster
   * @throws IOException if something goes wrong or timeout occurs
   */
  public abstract void waitForDataNodeToStart(ServerName serverName, long timeout)
    throws IOException;

  /**
   * Wait for the specified datanode to stop the thread / process.
   * @throws IOException if something goes wrong or timeout occurs
   */
  public abstract void waitForDataNodeToStop(ServerName serverName, long timeout)
    throws IOException;

  /**
   * Starts a new namenode on the given hostname or if this is a mini/local cluster, silently logs
   * warning message.
   * @throws IOException if something goes wrong
   */
  public abstract void startNameNode(ServerName serverName) throws IOException;

  /**
   * Kills the namenode process if this is a distributed cluster, otherwise, this causes master to
   * exit doing basic clean up only.
   * @throws IOException if something goes wrong
   */
  public abstract void killNameNode(ServerName serverName) throws IOException;

  /**
   * Stops the namenode if this is a distributed cluster, otherwise silently logs warning message.
   * @throws IOException if something goes wrong
   */
  public abstract void stopNameNode(ServerName serverName) throws IOException;

  /**
   * Wait for the specified namenode to join the cluster
   * @throws IOException if something goes wrong or timeout occurs
   */
  public abstract void waitForNameNodeToStart(ServerName serverName, long timeout)
      throws IOException;

  /**
   * Wait for the specified namenode to stop
   * @throws IOException if something goes wrong or timeout occurs
   */
  public abstract void waitForNameNodeToStop(ServerName serverName, long timeout)
      throws IOException;

  /**
   * Starts a new master on the given hostname or if this is a mini/local cluster,
   * starts a master locally.
   * @param hostname the hostname to start the master on
   * @throws IOException if something goes wrong
   */
  public abstract void startMaster(String hostname, int port) throws IOException;

  /**
   * Kills the master process if this is a distributed cluster, otherwise,
   * this causes master to exit doing basic clean up only.
   * @throws IOException if something goes wrong
   */
  public abstract void killMaster(ServerName serverName) throws IOException;

  /**
   * Stops the given master, by attempting a gradual stop.
   * @throws IOException if something goes wrong
   */
  public abstract void stopMaster(ServerName serverName) throws IOException;

  /**
   * Wait for the specified master to stop the thread / process.
   * @throws IOException if something goes wrong or timeout occurs
   */
  public abstract void waitForMasterToStop(ServerName serverName, long timeout)
      throws IOException;

  /**
   * Blocks until there is an active master and that master has completed
   * initialization.
   *
   * @return true if an active master becomes available.  false if there are no
   *         masters left.
   * @throws IOException if something goes wrong or timeout occurs
   */
  public boolean waitForActiveAndReadyMaster()
      throws IOException {
    return waitForActiveAndReadyMaster(Long.MAX_VALUE);
  }

  /**
   * Blocks until there is an active master and that master has completed
   * initialization.
   * @param timeout the timeout limit in ms
   * @return true if an active master becomes available.  false if there are no
   *         masters left.
   */
  public abstract boolean waitForActiveAndReadyMaster(long timeout)
      throws IOException;

  /**
   * Wait for HBase Cluster to shut down.
   */
  public abstract void waitUntilShutDown() throws IOException;

  /**
   * Shut down the HBase cluster
   */
  public abstract void shutdown() throws IOException;

  /**
   * Restores the cluster to it's initial state if this is a real cluster,
   * otherwise does nothing.
   * This is a best effort restore. If the servers are not reachable, or insufficient
   * permissions, etc. restoration might be partial.
   * @return whether restoration is complete
   */
  public boolean restoreInitialStatus() throws IOException {
    return restoreClusterMetrics(getInitialClusterMetrics());
  }

  /**
   * Restores the cluster to given state if this is a real cluster,
   * otherwise does nothing.
   * This is a best effort restore. If the servers are not reachable, or insufficient
   * permissions, etc. restoration might be partial.
   * @return whether restoration is complete
   */
  public boolean restoreClusterMetrics(ClusterMetrics desiredStatus) throws IOException {
    return true;
  }

  /**
   * Get the ServerName of region server serving the first hbase:meta region
   */
  public ServerName getServerHoldingMeta() throws IOException {
    return getServerHoldingRegion(TableName.META_TABLE_NAME,
      RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionName());
  }

  /**
   * Get the ServerName of region server serving the specified region
   * @param regionName Name of the region in bytes
   * @param tn Table name that has the region.
   * @return ServerName that hosts the region or null
   */
  public abstract ServerName getServerHoldingRegion(final TableName tn, byte[] regionName)
      throws IOException;

  /**
   * @return whether we are interacting with a distributed cluster as opposed to an
   * in-process mini/local cluster.
   */
  public boolean isDistributedCluster() {
    return false;
  }

  /**
   * Closes all the resources held open for this cluster. Note that this call does not shutdown
   * the cluster.
   * @see #shutdown()
   */
  @Override
  public abstract void close() throws IOException;

  /**
   * Wait for the namenode.
   *
   * @throws InterruptedException
   */
  public void waitForNamenodeAvailable() throws InterruptedException {
  }

  public void waitForDatanodesRegistered(int nbDN) throws Exception {
  }
}
