/**
 * Copyright 2007 The Apache Software Foundation
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;

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
 * <p>Runs master on port 60000 by default.  Because we can't just kill the
 * process -- not till HADOOP-1700 gets fixed and even then.... -- we need to
 * be able to find the master with a remote client to run shutdown.  To use a
 * port other than 60000, set the hbase.master to a value of 'local:PORT':
 * that is 'local', not 'localhost', and the port number the master should use
 * instead of 60000.
 *
 * <p>To make 'local' mode more responsive, make values such as
 * <code>hbase.regionserver.msginterval</code>,
 * <code>hbase.master.meta.thread.rescanfrequency</code>, and
 * <code>hbase.server.thread.wakefrequency</code> a second or less.
 */
public class LocalHBaseCluster {
  static final Log LOG = LogFactory.getLog(LocalHBaseCluster.class);
  private final List<HMaster> masters =
    new CopyOnWriteArrayList<HMaster>();
  private final List<JVMClusterUtil.RegionServerThread> regionThreads =
    new CopyOnWriteArrayList<JVMClusterUtil.RegionServerThread>();
  private final static int DEFAULT_NO = 1;
  /** local mode */
  public static final String LOCAL = "local";
  /** 'local:' */
  public static final String LOCAL_COLON = LOCAL + ":";
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
    this(conf, 1, noRegionServers, HMaster.class,
        getRegionServerImplementation(conf));
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends HRegionServer> getRegionServerImplementation(final Configuration conf) {
    return (Class<? extends HRegionServer>)conf.getClass(HConstants.REGION_SERVER_IMPL,
       HRegionServer.class);
  }

  /**
   * Constructor.
   * @param conf Configuration to use.  Post construction has the master's
   * address.
   * @param noMasters Count of masters to start.
   * @param noRegionServers Count of regionservers to start.
   * @param masterClass master implementation to use if not specified by conf
   * @param regionServerClass RS implementation to use if not specified by conf
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
    conf.set(HConstants.MASTER_PORT, "0");

    boolean writeThriftPortToMeta =
      conf.getBoolean(HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META,
      HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META_DEFAULT);


    // If we are writing the thrift port to meta, then we can start the
    // thrift server at any ephemeral port. Otherwise, we start it at the
    // default port. But then, we can only start one server. Similarly for
    // RPC server.
    if (writeThriftPortToMeta) {
      conf.setInt(HConstants.REGIONSERVER_SWIFT_PORT, 0);
      conf.setInt(HConstants.REGIONSERVER_PORT,
                  HConstants.DEFAULT_REGIONSERVER_PORT);
    } else {
      conf.setInt(HConstants.REGIONSERVER_SWIFT_PORT,
                  HConstants.DEFAULT_REGIONSERVER_SWIFT_PORT);
      conf.setInt(HConstants.REGIONSERVER_PORT, 0);
    }

    // Start the HMasters.
    this.masterClass =
      (Class<? extends HMaster>)conf.getClass(HConstants.MASTER_IMPL,
          masterClass);
    for (int i = 0; i < noMasters; i++) {
      addMaster();
    }
    // Start the HRegionServers.
    this.regionServerClass =
      (Class<? extends HRegionServer>)conf.getClass(HConstants.REGION_SERVER_IMPL,
       regionServerClass);

    for (int i = 0; i < noRegionServers; i++) {
      addRegionServer(i);
    }
  }

  public JVMClusterUtil.RegionServerThread addRegionServer() throws IOException {
    return addRegionServer(this.regionThreads.size());
  }

  public JVMClusterUtil.RegionServerThread addRegionServer(final int index)
  throws IOException {
    JVMClusterUtil.RegionServerThread rst = JVMClusterUtil.createRegionServerThread(this.conf,
        this.regionServerClass, index);
    this.regionThreads.add(rst);
    return rst;
  }

  /**
   * Adds a master to the list of masters in the cluster. Each new master gets
   * its own copy of the configuration.
   * @return the new master
   */
  public HMaster addMaster() throws IOException {
    Configuration masterConf = HBaseConfiguration.create(conf);
    // Creating each master with its own Configuration instance so each has
    // its HConnection instance rather than share (see HBASE_INSTANCES down in
    // the guts of HConnectionManager).
    HMaster m = JVMClusterUtil.createMaster(masterConf, this.masterClass);
    this.masters.add(m);
    return m;
  }

  /**
   * @param serverNumber
   * @return region server
   */
  public HRegionServer getRegionServer(int serverNumber) {
    return regionThreads.get(serverNumber).getRegionServer();
  }

  /**
   * Returns the current active master, if available.
   * @return the active HMaster, null if none is active.
   */
  public HMaster getActiveMaster() {
    for (HMaster master : masters) {
      if (master.isActiveMaster()) {
        return master;
      }
    }
    return null;
  }

  /**
   * For use in unit tests with only one master. If there are multiple masters,
   * use {@link #getActiveMaster()}.
   *
   * @return the HMaster object
   */
  public HMaster getMaster() {
    final int numMasters = masters.size();
    if (numMasters != 1) {
      throw new AssertionError("one master expected, got " + numMasters);
    }
    return this.masters.get(0);
  }

  /**
   * @return Read-only list of master threads.
   */
  public List<HMaster> getMasters() {
    return Collections.unmodifiableList(this.masters);
  }

  /**
   * Wait for the specified master to stop, and removes this thread from list
   * of running threads.
   * 
   * @param serverNumber the 0-based index of the master to stop
   * @return Name of master that just went down.
   */
  public String waitOnMasterStop(int serverNumber) {
    HMaster master = masters.remove(serverNumber);
    boolean interrupted = false;
    while (master.isAlive()) {
      try {
        LOG.info("Waiting on " +
          master.getServerName().toString());
        master.join();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return master.getName();
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
    for (JVMClusterUtil.RegionServerThread rst: getRegionServers()) {
      if (rst.isAlive()) liveServers.add(rst);
    }
    return liveServers;
  }

  /**
   * Wait for the specified region server to stop
   * Removes this thread from list of running threads.
   * @param serverNumber
   * @return Name of region server that just went down.
   */
  public String waitOnRegionServer(int serverNumber) {
    JVMClusterUtil.RegionServerThread regionServerThread =
      this.regionThreads.remove(serverNumber);
    while (regionServerThread.isAlive()) {
      try {
        LOG.info("Waiting on " +
          regionServerThread.getRegionServer().getHServerInfo().toString());
        regionServerThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return regionServerThread.getName();
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
              t.join();
          } catch (InterruptedException e) {
            // continue
          }
        }
      }
    }
    if (this.masters != null) {
      for (HasThread t : this.masters) {
        if (t.isAlive()) {
          try {
            t.join();
          } catch (InterruptedException e) {
            // continue
          }
        }
      }
    }
  }

  /**
   * Start the cluster.
   */
  public void startup() throws IOException {
    JVMClusterUtil.startup(this.masters, this.regionThreads);
  }

  /**
   * Shut down the mini HBase cluster
   */
  public void shutdown() {
    JVMClusterUtil.shutdown(this.masters, this.regionThreads);
  }

  /**
   * @param c Configuration to check.
   * @return True if a 'local' address in hbase.master value.
   */
  public static boolean isLocal(final Configuration c) {
    final String mode = c.get(HConstants.CLUSTER_DISTRIBUTED);
    return mode == null || mode.equals(HConstants.CLUSTER_IS_LOCAL);
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
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor htd =
      new HTableDescriptor(Bytes.toBytes(cluster.getClass().getName()));
    admin.createTable(htd);
    cluster.shutdown();
  }
}
