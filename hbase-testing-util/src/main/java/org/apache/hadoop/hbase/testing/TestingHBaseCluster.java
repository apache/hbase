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
package org.apache.hadoop.hbase.testing;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A mini hbase cluster used for testing.
 * <p/>
 * It will also start the necessary zookeeper cluster and dfs cluster. But we will not provide
 * methods for controlling the zookeeper cluster and dfs cluster, as end users do not need to test
 * the HBase behavior when these systems are broken.
 * <p/>
 * The implementation is not required to be thread safe, so do not call different methods
 * concurrently.
 */
@InterfaceAudience.Public
public interface TestingHBaseCluster {

  /**
   * Get configuration of this cluster.
   * <p/>
   * You could use the returned {@link Configuration} to create
   * {@link org.apache.hadoop.hbase.client.Connection} for accessing the testing cluster.
   */
  Configuration getConf();

  /**
   * Start a new master with localhost and random port.
   */
  void startMaster() throws Exception;

  /**
   * Start a new master bind on the given host and port.
   */
  void startMaster(String hostname, int port) throws Exception;

  /**
   * Stop the given master.
   * <p/>
   * Wait on the returned {@link CompletableFuture} to wait on the master quit. The differences
   * comparing to {@link org.apache.hadoop.hbase.client.Admin#stopMaster()} is that first, we could
   * also stop backup masters here, second, this method does not always fail since we do not use rpc
   * to stop the master.
   */
  CompletableFuture<Void> stopMaster(ServerName serverName) throws Exception;

  /**
   * Start a new region server with localhost and random port.
   */
  void startRegionServer() throws Exception;

  /**
   * Start a new region server bind on the given host and port.
   */
  void startRegionServer(String hostname, int port) throws Exception;

  /**
   * Stop the given region server.
   * <p/>
   * Wait on the returned {@link CompletableFuture} to wait on the master quit. The difference
   * comparing to {@link org.apache.hadoop.hbase.client.Admin#stopMaster()} is that this method does
   * not always fail since we do not use rpc to stop the region server.
   */
  CompletableFuture<Void> stopRegionServer(ServerName serverName) throws Exception;

  /**
   * Stop the hbase cluster.
   * <p/>
   * You need to call {@link #start()} first before calling this method, otherwise an
   * {@link IllegalStateException} will be thrown. If the hbase is not running because you have
   * already stopped the cluster, an {@link IllegalStateException} will be thrown too.
   */
  void stopHBaseCluster() throws Exception;

  /**
   * Start the hbase cluster.
   * <p/>
   * This is used to start the hbase cluster again after you call {@link #stopHBaseCluster()}. If
   * the cluster is already running or you have not called {@link #start()} yet, an
   * {@link IllegalStateException} will be thrown.
   */
  void startHBaseCluster() throws Exception;

  /**
   * Return whether the hbase cluster is running.
   */
  boolean isHBaseClusterRunning();

  /**
   * Start the whole mini cluster, including zookeeper cluster, dfs cluster and hbase cluster.
   * <p/>
   * You can only call this method once at the beginning, unless you have called {@link #stop()} to
   * shutdown the cluster completely, and then you can call this method to start the whole cluster
   * again. An {@link IllegalStateException} will be thrown if you call this method incorrectly.
   */
  void start() throws Exception;

  /**
   * Get the address of active master if there is one.
   */
  Optional<ServerName> getActiveMasterAddress();

  /**
   * Get all the backup master addresses.
   */
  List<ServerName> getBackupMasterAddresses();

  /**
   * Get all the region server addresses.
   */
  List<ServerName> getRegionServerAddresses();

  /**
   * Get the info port of the active master if there is one.
   */
  Optional<Integer> getActiveMasterInfoPort();

  /**
   * Get the info port of the active NameNode if there is one.
   */
  Optional<Integer> getActiveNameNodeInfoPort();

  /**
   * Get the client port of the ZooKeeper if there is one.
   */
  Optional<Integer> getActiveZooKeeperClientPort();

  /**
   * Get the list of master addresses.
   */
  List<String> getMasterAddresses();

  /**
   * Get the server side {@link Region} interface for the specific region.
   * <p/>
   * This is used for CPs to test something which can only be accessed at server side, such as tags.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
  Optional<Region> getRegion(RegionInfo regionInfo);

  /**
   * Get the server side {@link OnlineRegions} interface for the specific region server.
   * <p/>
   * You could list the addresses of all the region server through the
   * {@link #getRegionServerAddresses()} method.
   * <p/>
   * This is used for CPs to test something which can only be accessed at server side, such as tags.
   * And also you could use the returned interface to get all regions on this region server, etc.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
  Optional<OnlineRegions> getOnlineRegionsInterface(ServerName serverName);

  /**
   * Return whether the cluster is running.
   * <p/>
   * Notice that, this only means you have called {@link #start()} and have not called
   * {@link #stop()} yet. If you want to make sure the hbase cluster is running, use
   * {@link #isHBaseClusterRunning()}.
   */
  boolean isClusterRunning();

  /**
   * Stop the whole mini cluster, including zookeeper cluster, dfs cluster and hbase cluster.
   * <p/>
   * You can only call this method after calling {@link #start()}, otherwise an
   * {@link IllegalStateException} will be thrown.
   */
  void stop() throws Exception;

  /**
   * Create a {@link TestingHBaseCluster}. You need to call {@link #start()} of the returned
   * {@link TestingHBaseCluster} to actually start the mini testing cluster.
   */
  static TestingHBaseCluster create(TestingHBaseClusterOption option) {
    return new TestingHBaseClusterImpl(option);
  }
}
