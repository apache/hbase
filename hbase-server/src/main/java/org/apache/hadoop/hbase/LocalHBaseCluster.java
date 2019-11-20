/**
 *
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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;

import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.JVMClusterUtil;

/**
 * This class creates a single process HBase cluster. One thread is created for
 * a master and one per region server.
 *
 * Call {@link #startup()} to start the cluster running and {@link #shutdown()}
 * to close it all down. {@link #join} the cluster is you want to wait on
 * shutdown completion.
 *
 * <p>Runs master on port 16000 by default.  Because we can't just kill the
 * process -- not till HADOOP-1700 gets fixed and even then.... -- we need to
 * be able to find the master with a remote client to run shutdown.  To use a
 * port other than 16000, set the hbase.master to a value of 'local:PORT':
 * that is 'local', not 'localhost', and the port number the master should use
 * instead of 16000.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LocalHBaseCluster {
  private static final Log LOG = LogFactory.getLog(LocalHBaseCluster.class);
  private final List<JVMClusterUtil.MasterThread> masterThreads =
    new CopyOnWriteArrayList<JVMClusterUtil.MasterThread>();
  private final List<JVMClusterUtil.RegionServerThread> regionThreads =
    new CopyOnWriteArrayList<JVMClusterUtil.RegionServerThread>();
  private final static int DEFAULT_NO = 1;
  /** local mode */
  public static final String LOCAL = "local";
  /** 'local:' */
  public static final String LOCAL_COLON = LOCAL + ":";
  public static final String ASSIGN_RANDOM_PORTS = "hbase.localcluster.assign.random.ports";

  private final Configuration conf;
  private final Class<? extends HMaster> masterClass;
  private final Class<? extends HRegionServer> regionServerClass;

  /**
   * Constructor.
   * @param conf
   * @throws IOException
   */
  public LocalHBaseCluster(final Configuration conf)
  throws IOException {
    this(conf, DEFAULT_NO);
  }

  /**
   * Constructor.
   * @param conf Configuration to use.  Post construction has the master's
   * address.
   * @param noRegionServers Count of regionservers to start.
   * @throws IOException
   */
  public LocalHBaseCluster(final Configuration conf, final int noRegionServers)
  throws IOException {
    this(conf, 1, noRegionServers, getMasterImplementation(conf),
        getRegionServerImplementation(conf));
  }

  /**
   * Constructor.
   * @param conf Configuration to use.  Post construction has the active master
   * address.
   * @param noMasters Count of masters to start.
   * @param noRegionServers Count of regionservers to start.
   * @throws IOException
   */
  public LocalHBaseCluster(final Configuration conf, final int noMasters,
      final int noRegionServers)
  throws IOException {
    this(conf, noMasters, noRegionServers, getMasterImplementation(conf),
        getRegionServerImplementation(conf));
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends HRegionServer> getRegionServerImplementation(final Configuration conf) {
    return (Class<? extends HRegionServer>)conf.getClass(HConstants.REGION_SERVER_IMPL,
       HRegionServer.class);
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends HMaster> getMasterImplementation(final Configuration conf) {
    return (Class<? extends HMaster>)conf.getClass(HConstants.MASTER_IMPL,
       HMaster.class);
  }

  /**
   * Constructor.
   * @param conf Configuration to use.  Post construction has the master's
   * address.
   * @param noMasters Count of masters to start.
   * @param noRegionServers Count of regionservers to start.
   * @param masterClass
   * @param regionServerClass
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public LocalHBaseCluster(final Configuration conf, final int noMasters,
    final int noRegionServers, final Class<? extends HMaster> masterClass,
    final Class<? extends HRegionServer> regionServerClass)
  throws IOException {
    this.conf = conf;

    // Always have masters and regionservers come up on port '0' so we don't
    // clash over default ports.
    if (conf.getBoolean(ASSIGN_RANDOM_PORTS, true)) {
      conf.set(HConstants.MASTER_PORT, "0");
      conf.set(HConstants.REGIONSERVER_PORT, "0");
      if (conf.getInt(HConstants.REGIONSERVER_INFO_PORT, 0) != -1) {
        conf.set(HConstants.REGIONSERVER_INFO_PORT, "0");
      }
      if (conf.getInt(HConstants.MASTER_INFO_PORT, 0) != -1) {
        conf.set(HConstants.MASTER_INFO_PORT, "0");
      }
    }

    this.masterClass = (Class<? extends HMaster>)
      conf.getClass(HConstants.MASTER_IMPL, masterClass);
    // Start the HMasters.
    for (int i = 0; i < noMasters; i++) {
      addMaster(new Configuration(conf), i);
    }

    // Populate the master address host ports in the config. This is needed if a master based
    // registry is configured for client metadata services (HBASE-18095)
    List<String> masterHostPorts = new ArrayList<>();
    for (JVMClusterUtil.MasterThread masterThread: getMasters()) {
      masterHostPorts.add(masterThread.getMaster().getServerName().getAddress().toString());
    }
    conf.set(HConstants.MASTER_ADDRS_KEY, Joiner.on(",").join(masterHostPorts));

    // Start the HRegionServers.
    this.regionServerClass =
      (Class<? extends HRegionServer>)conf.getClass(HConstants.REGION_SERVER_IMPL,
       regionServerClass);

    for (int i = 0; i < noRegionServers; i++) {
      addRegionServer(new Configuration(conf), i);
    }
  }

  public JVMClusterUtil.RegionServerThread addRegionServer()
      throws IOException {
    return addRegionServer(new Configuration(conf), this.regionThreads.size());
  }

  @SuppressWarnings("unchecked")
  public JVMClusterUtil.RegionServerThread addRegionServer(
      Configuration config, final int index)
  throws IOException {
    // Create each regionserver with its own Configuration instance so each has
    // its HConnection instance rather than share (see HBASE_INSTANCES down in
    // the guts of HConnectionManager.

    // Also, create separate CoordinatedStateManager instance per Server.
    // This is special case when we have to have more than 1 CoordinatedStateManager
    // within 1 process.
    CoordinatedStateManager cp = CoordinatedStateManagerFactory.getCoordinatedStateManager(conf);

    JVMClusterUtil.RegionServerThread rst =
        JVMClusterUtil.createRegionServerThread(config, cp, (Class<? extends HRegionServer>) conf
            .getClass(HConstants.REGION_SERVER_IMPL, this.regionServerClass), index);

    this.regionThreads.add(rst);
    return rst;
  }

  public JVMClusterUtil.RegionServerThread addRegionServer(
      final Configuration config, final int index, User user)
  throws IOException, InterruptedException {
    return user.runAs(
        new PrivilegedExceptionAction<JVMClusterUtil.RegionServerThread>() {
          @Override
          public JVMClusterUtil.RegionServerThread run() throws Exception {
            return addRegionServer(config, index);
          }
        });
  }

  public JVMClusterUtil.MasterThread addMaster() throws IOException {
    return addMaster(new Configuration(conf), this.masterThreads.size());
  }

  public JVMClusterUtil.MasterThread addMaster(Configuration c, final int index)
      throws IOException {
    // Create each master with its own Configuration instance so each has
    // its HConnection instance rather than share (see HBASE_INSTANCES down in
    // the guts of HConnectionManager.

    // Also, create separate CoordinatedStateManager instance per Server.
    // This is special case when we have to have more than 1 CoordinatedStateManager
    // within 1 process.
    CoordinatedStateManager cp = CoordinatedStateManagerFactory.getCoordinatedStateManager(conf);

    JVMClusterUtil.MasterThread mt = JVMClusterUtil.createMasterThread(c, cp,
        (Class<? extends HMaster>) conf.getClass(HConstants.MASTER_IMPL, this.masterClass), index);
    this.masterThreads.add(mt);
    return mt;
  }

  public JVMClusterUtil.MasterThread addMaster(
      final Configuration c, final int index, User user)
  throws IOException, InterruptedException {
    return user.runAs(
        new PrivilegedExceptionAction<JVMClusterUtil.MasterThread>() {
          @Override
          public JVMClusterUtil.MasterThread run() throws Exception {
            return addMaster(c, index);
          }
        });
  }

  /**
   * @param serverNumber
   * @return region server
   */
  public HRegionServer getRegionServer(int serverNumber) {
    return regionThreads.get(serverNumber).getRegionServer();
  }

  /**
   * @return Read-only list of region server threads.
   */
  public List<JVMClusterUtil.RegionServerThread> getRegionServers() {
    return Collections.unmodifiableList(this.regionThreads);
  }

  /**
   * @return List of running servers (Some servers may have been killed or
   * aborted during lifetime of cluster; these servers are not included in this
   * list).
   */
  public List<JVMClusterUtil.RegionServerThread> getLiveRegionServers() {
    List<JVMClusterUtil.RegionServerThread> liveServers =
      new ArrayList<JVMClusterUtil.RegionServerThread>();
    List<RegionServerThread> list = getRegionServers();
    for (JVMClusterUtil.RegionServerThread rst: list) {
      if (rst.isAlive()) liveServers.add(rst);
      else LOG.info("Not alive " + rst.getName());
    }
    return liveServers;
  }

  /**
   * @return the Configuration used by this LocalHBaseCluster
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Wait for the specified region server to stop. Removes this thread from list of running threads.
   * @return Name of region server that just went down.
   */
  public String waitOnRegionServer(int serverNumber) {
    JVMClusterUtil.RegionServerThread regionServerThread = this.regionThreads.get(serverNumber);
    return waitOnRegionServer(regionServerThread);
  }

  /**
   * Wait for the specified region server to stop. Removes this thread from list of running threads.
   * @return Name of region server that just went down.
   */
  public String waitOnRegionServer(JVMClusterUtil.RegionServerThread rst) {
    while (rst.isAlive()) {
      try {
        LOG.info("Waiting on " + rst.getRegionServer().toString());
        rst.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    regionThreads.remove(rst);
    return rst.getName();
  }

  /**
   * @return the HMaster thread
   */
  public HMaster getMaster(int serverNumber) {
    return masterThreads.get(serverNumber).getMaster();
  }

  /**
   * Gets the current active master, if available.  If no active master, returns
   * null.
   * @return the HMaster for the active master
   */
  public HMaster getActiveMaster() {
    for (JVMClusterUtil.MasterThread mt : masterThreads) {
      // Ensure that the current active master is not stopped.
      // We don't want to return a stopping master as an active master.
      if (mt.getMaster().isActiveMaster() && !mt.getMaster().isStopped()) {
        return mt.getMaster();
      }
    }
    return null;
  }

  /**
   * @return Read-only list of master threads.
   */
  public List<JVMClusterUtil.MasterThread> getMasters() {
    return Collections.unmodifiableList(this.masterThreads);
  }

  /**
   * @return List of running master servers (Some servers may have been killed
   * or aborted during lifetime of cluster; these servers are not included in
   * this list).
   */
  public List<JVMClusterUtil.MasterThread> getLiveMasters() {
    List<JVMClusterUtil.MasterThread> liveServers = new ArrayList<>();
    List<JVMClusterUtil.MasterThread> list = getMasters();
    for (JVMClusterUtil.MasterThread mt: list) {
      if (mt.isAlive()) {
        liveServers.add(mt);
      }
    }
    return liveServers;
  }

  /**
   * Wait for the specified master to stop. Removes this thread from list of running threads.
   * @return Name of master that just went down.
   */
  public String waitOnMaster(int serverNumber) {
    JVMClusterUtil.MasterThread masterThread = this.masterThreads.get(serverNumber);
    return waitOnMaster(masterThread);
  }

  /**
   * Wait for the specified master to stop. Removes this thread from list of running threads.
   * @return Name of master that just went down.
   */
  public String waitOnMaster(JVMClusterUtil.MasterThread masterThread) {
    while (masterThread.isAlive()) {
      try {
        LOG.info("Waiting on " + masterThread.getMaster().getServerName().toString());
        masterThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    masterThreads.remove(masterThread);
    return masterThread.getName();
  }

  /**
   * Wait for Mini HBase Cluster to shut down.
   * Presumes you've already called {@link #shutdown()}.
   */
  public void join() {
    if (this.regionThreads != null) {
      for(Thread t: this.regionThreads) {
        if (t.isAlive()) {
          try {
            Threads.threadDumpingIsAlive(t);
          } catch (InterruptedException e) {
            LOG.debug("Interrupted", e);
          }
        }
      }
    }
    if (this.masterThreads != null) {
      for (Thread t : this.masterThreads) {
        if (t.isAlive()) {
          try {
            Threads.threadDumpingIsAlive(t);
          } catch (InterruptedException e) {
            LOG.debug("Interrupted", e);
          }
        }
      }
    }
  }

  /**
   * Start the cluster.
   */
  public void startup() throws IOException {
    JVMClusterUtil.startup(this.masterThreads, this.regionThreads);
  }

  /**
   * Shut down the mini HBase cluster
   */
  public void shutdown() {
    JVMClusterUtil.shutdown(this.masterThreads, this.regionThreads);
  }

  /**
   * @param c Configuration to check.
   * @return True if a 'local' address in hbase.master value.
   */
  public static boolean isLocal(final Configuration c) {
    boolean mode = c.getBoolean(HConstants.CLUSTER_DISTRIBUTED, HConstants.DEFAULT_CLUSTER_DISTRIBUTED);
    return(mode == HConstants.CLUSTER_IS_LOCAL);
  }

  /**
   * Test things basically work.
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    LocalHBaseCluster cluster = new LocalHBaseCluster(conf);
    cluster.startup();
    Admin admin = new HBaseAdmin(conf);
    try {
      HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf(cluster.getClass().getName()));
      admin.createTable(htd);
    } finally {
      admin.close();
    }
    cluster.shutdown();
  }
}
