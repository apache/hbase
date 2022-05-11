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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;

/**
 * This class creates a single process HBase cluster. each server. The master uses the 'default'
 * FileSystem. The RegionServers, if we are running on DistributedFilesystem, create a FileSystem
 * instance each and will close down their instance on the way out.
 */
@InterfaceAudience.Public
public class MiniHBaseCluster extends HBaseCluster {
  private static final Logger LOG = LoggerFactory.getLogger(MiniHBaseCluster.class.getName());
  public LocalHBaseCluster hbaseCluster;
  private static int index;

  /**
   * Start a MiniHBaseCluster.
   * @param conf             Configuration to be used for cluster
   * @param numRegionServers initial number of region servers to start. n
   */
  public MiniHBaseCluster(Configuration conf, int numRegionServers)
    throws IOException, InterruptedException {
    this(conf, 1, numRegionServers);
  }

  /**
   * Start a MiniHBaseCluster.
   * @param conf             Configuration to be used for cluster
   * @param numMasters       initial number of masters to start.
   * @param numRegionServers initial number of region servers to start. n
   */
  public MiniHBaseCluster(Configuration conf, int numMasters, int numRegionServers)
    throws IOException, InterruptedException {
    this(conf, numMasters, numRegionServers, null, null);
  }

  /**
   * Start a MiniHBaseCluster.
   * @param conf             Configuration to be used for cluster
   * @param numMasters       initial number of masters to start.
   * @param numRegionServers initial number of region servers to start.
   */
  public MiniHBaseCluster(Configuration conf, int numMasters, int numRegionServers,
    Class<? extends HMaster> masterClass,
    Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> regionserverClass)
    throws IOException, InterruptedException {
    this(conf, numMasters, 0, numRegionServers, null, masterClass, regionserverClass);
  }

  /**
   * @param rsPorts Ports that RegionServer should use; pass ports if you want to test cluster
   *                restart where for sure the regionservers come up on same address+port (but just
   *                with different startcode); by default mini hbase clusters choose new arbitrary
   *                ports on each cluster start. nn
   */
  public MiniHBaseCluster(Configuration conf, int numMasters, int numAlwaysStandByMasters,
    int numRegionServers, List<Integer> rsPorts, Class<? extends HMaster> masterClass,
    Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> regionserverClass)
    throws IOException, InterruptedException {
    super(conf);

    // Hadoop 2
    CompatibilityFactory.getInstance(MetricsAssertHelper.class).init();

    init(numMasters, numAlwaysStandByMasters, numRegionServers, rsPorts, masterClass,
      regionserverClass);
    this.initialClusterStatus = getClusterMetrics();
  }

  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Subclass so can get at protected methods (none at moment). Also, creates a FileSystem instance
   * per instantiation. Adds a shutdown own FileSystem on the way out. Shuts down own Filesystem
   * only, not All filesystems as the FileSystem system exit hook does.
   */
  public static class MiniHBaseClusterRegionServer extends HRegionServer {
    private Thread shutdownThread = null;
    private User user = null;
    /**
     * List of RegionServers killed so far. ServerName also comprises startCode of a server, so any
     * restarted instances of the same server will have different ServerName and will not coincide
     * with past dead ones. So there's no need to cleanup this list.
     */
    static Set<ServerName> killedServers = new HashSet<>();

    public MiniHBaseClusterRegionServer(Configuration conf)
      throws IOException, InterruptedException {
      super(conf);
      this.user = User.getCurrent();
    }

    /*
     * n * @param currentfs We return this if we did not make a new one.
     * @param uniqueName Same name used to help identify the created fs.
     * @return A new fs instance if we are up on DistributeFileSystem. n
     */

    @Override
    protected void handleReportForDutyResponse(final RegionServerStartupResponse c)
      throws IOException {
      super.handleReportForDutyResponse(c);
      // Run this thread to shutdown our filesystem on way out.
      this.shutdownThread = new SingleFileSystemShutdownThread(getFileSystem());
    }

    @Override
    public void run() {
      try {
        this.user.runAs(new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            runRegionServer();
            return null;
          }
        });
      } catch (Throwable t) {
        LOG.error("Exception in run", t);
      } finally {
        // Run this on the way out.
        if (this.shutdownThread != null) {
          this.shutdownThread.start();
          Threads.shutdown(this.shutdownThread, 30000);
        }
      }
    }

    private void runRegionServer() {
      super.run();
    }

    @Override
    protected void kill() {
      killedServers.add(getServerName());
      super.kill();
    }

    @Override
    public void abort(final String reason, final Throwable cause) {
      this.user.runAs(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          abortRegionServer(reason, cause);
          return null;
        }
      });
    }

    private void abortRegionServer(String reason, Throwable cause) {
      super.abort(reason, cause);
    }
  }

  /**
   * Alternate shutdown hook. Just shuts down the passed fs, not all as default filesystem hook
   * does.
   */
  static class SingleFileSystemShutdownThread extends Thread {
    private final FileSystem fs;

    SingleFileSystemShutdownThread(final FileSystem fs) {
      super("Shutdown of " + fs);
      this.fs = fs;
    }

    @Override
    public void run() {
      try {
        LOG.info("Hook closing fs=" + this.fs);
        this.fs.close();
      } catch (NullPointerException npe) {
        LOG.debug("Need to fix these: " + npe.toString());
      } catch (IOException e) {
        LOG.warn("Running hook", e);
      }
    }
  }

  private void init(final int nMasterNodes, final int numAlwaysStandByMasters,
    final int nRegionNodes, List<Integer> rsPorts, Class<? extends HMaster> masterClass,
    Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> regionserverClass)
    throws IOException, InterruptedException {
    try {
      if (masterClass == null) {
        masterClass = HMaster.class;
      }
      if (regionserverClass == null) {
        regionserverClass = MiniHBaseCluster.MiniHBaseClusterRegionServer.class;
      }

      // start up a LocalHBaseCluster
      hbaseCluster = new LocalHBaseCluster(conf, nMasterNodes, numAlwaysStandByMasters, 0,
        masterClass, regionserverClass);

      // manually add the regionservers as other users
      for (int i = 0; i < nRegionNodes; i++) {
        Configuration rsConf = HBaseConfiguration.create(conf);
        if (rsPorts != null) {
          rsConf.setInt(HConstants.REGIONSERVER_PORT, rsPorts.get(i));
        }
        User user = HBaseTestingUtility.getDifferentUser(rsConf, ".hfs." + index++);
        hbaseCluster.addRegionServer(rsConf, i, user);
      }

      hbaseCluster.startup();
    } catch (IOException e) {
      shutdown();
      throw e;
    } catch (Throwable t) {
      LOG.error("Error starting cluster", t);
      shutdown();
      throw new IOException("Shutting down", t);
    }
  }

  @Override
  public void startRegionServer(String hostname, int port) throws IOException {
    final Configuration newConf = HBaseConfiguration.create(conf);
    newConf.setInt(HConstants.REGIONSERVER_PORT, port);
    startRegionServer(newConf);
  }

  @Override
  public void killRegionServer(ServerName serverName) throws IOException {
    HRegionServer server = getRegionServer(getRegionServerIndex(serverName));
    if (server instanceof MiniHBaseClusterRegionServer) {
      LOG.info("Killing " + server.toString());
      ((MiniHBaseClusterRegionServer) server).kill();
    } else {
      abortRegionServer(getRegionServerIndex(serverName));
    }
  }

  @Override
  public boolean isKilledRS(ServerName serverName) {
    return MiniHBaseClusterRegionServer.killedServers.contains(serverName);
  }

  @Override
  public void stopRegionServer(ServerName serverName) throws IOException {
    stopRegionServer(getRegionServerIndex(serverName));
  }

  @Override
  public void suspendRegionServer(ServerName serverName) throws IOException {
    suspendRegionServer(getRegionServerIndex(serverName));
  }

  @Override
  public void resumeRegionServer(ServerName serverName) throws IOException {
    resumeRegionServer(getRegionServerIndex(serverName));
  }

  @Override
  public void waitForRegionServerToStop(ServerName serverName, long timeout) throws IOException {
    // ignore timeout for now
    waitOnRegionServer(getRegionServerIndex(serverName));
  }

  @Override
  public void startZkNode(String hostname, int port) throws IOException {
    LOG.warn("Starting zookeeper nodes on mini cluster is not supported");
  }

  @Override
  public void killZkNode(ServerName serverName) throws IOException {
    LOG.warn("Aborting zookeeper nodes on mini cluster is not supported");
  }

  @Override
  public void stopZkNode(ServerName serverName) throws IOException {
    LOG.warn("Stopping zookeeper nodes on mini cluster is not supported");
  }

  @Override
  public void waitForZkNodeToStart(ServerName serverName, long timeout) throws IOException {
    LOG.warn("Waiting for zookeeper nodes to start on mini cluster is not supported");
  }

  @Override
  public void waitForZkNodeToStop(ServerName serverName, long timeout) throws IOException {
    LOG.warn("Waiting for zookeeper nodes to stop on mini cluster is not supported");
  }

  @Override
  public void startDataNode(ServerName serverName) throws IOException {
    LOG.warn("Starting datanodes on mini cluster is not supported");
  }

  @Override
  public void killDataNode(ServerName serverName) throws IOException {
    LOG.warn("Aborting datanodes on mini cluster is not supported");
  }

  @Override
  public void stopDataNode(ServerName serverName) throws IOException {
    LOG.warn("Stopping datanodes on mini cluster is not supported");
  }

  @Override
  public void waitForDataNodeToStart(ServerName serverName, long timeout) throws IOException {
    LOG.warn("Waiting for datanodes to start on mini cluster is not supported");
  }

  @Override
  public void waitForDataNodeToStop(ServerName serverName, long timeout) throws IOException {
    LOG.warn("Waiting for datanodes to stop on mini cluster is not supported");
  }

  @Override
  public void startNameNode(ServerName serverName) throws IOException {
    LOG.warn("Starting namenodes on mini cluster is not supported");
  }

  @Override
  public void killNameNode(ServerName serverName) throws IOException {
    LOG.warn("Aborting namenodes on mini cluster is not supported");
  }

  @Override
  public void stopNameNode(ServerName serverName) throws IOException {
    LOG.warn("Stopping namenodes on mini cluster is not supported");
  }

  @Override
  public void waitForNameNodeToStart(ServerName serverName, long timeout) throws IOException {
    LOG.warn("Waiting for namenodes to start on mini cluster is not supported");
  }

  @Override
  public void waitForNameNodeToStop(ServerName serverName, long timeout) throws IOException {
    LOG.warn("Waiting for namenodes to stop on mini cluster is not supported");
  }

  @Override
  public void startMaster(String hostname, int port) throws IOException {
    this.startMaster();
  }

  @Override
  public void killMaster(ServerName serverName) throws IOException {
    abortMaster(getMasterIndex(serverName));
  }

  @Override
  public void stopMaster(ServerName serverName) throws IOException {
    stopMaster(getMasterIndex(serverName));
  }

  @Override
  public void waitForMasterToStop(ServerName serverName, long timeout) throws IOException {
    // ignore timeout for now
    waitOnMaster(getMasterIndex(serverName));
  }

  /**
   * Starts a region server thread running n * @return New RegionServerThread
   */
  public JVMClusterUtil.RegionServerThread startRegionServer() throws IOException {
    final Configuration newConf = HBaseConfiguration.create(conf);
    return startRegionServer(newConf);
  }

  private JVMClusterUtil.RegionServerThread startRegionServer(Configuration configuration)
    throws IOException {
    User rsUser = HBaseTestingUtility.getDifferentUser(configuration, ".hfs." + index++);
    JVMClusterUtil.RegionServerThread t = null;
    try {
      t =
        hbaseCluster.addRegionServer(configuration, hbaseCluster.getRegionServers().size(), rsUser);
      t.start();
      t.waitForServerOnline();
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted adding regionserver to cluster", ie);
    }
    return t;
  }

  /**
   * Starts a region server thread and waits until its processed by master. Throws an exception when
   * it can't start a region server or when the region server is not processed by master within the
   * timeout.
   * @return New RegionServerThread
   */
  public JVMClusterUtil.RegionServerThread startRegionServerAndWait(long timeout)
    throws IOException {

    JVMClusterUtil.RegionServerThread t = startRegionServer();
    ServerName rsServerName = t.getRegionServer().getServerName();

    long start = System.currentTimeMillis();
    ClusterStatus clusterStatus = getClusterStatus();
    while ((System.currentTimeMillis() - start) < timeout) {
      if (clusterStatus != null && clusterStatus.getServers().contains(rsServerName)) {
        return t;
      }
      Threads.sleep(100);
    }
    if (t.getRegionServer().isOnline()) {
      throw new IOException("RS: " + rsServerName + " online, but not processed by master");
    } else {
      throw new IOException("RS: " + rsServerName + " is offline");
    }
  }

  /**
   * Cause a region server to exit doing basic clean up only on its way out.
   * @param serverNumber Used as index into a list.
   */
  public String abortRegionServer(int serverNumber) {
    HRegionServer server = getRegionServer(serverNumber);
    LOG.info("Aborting " + server.toString());
    server.abort("Aborting for tests", new Exception("Trace info"));
    return server.toString();
  }

  /**
   * Shut down the specified region server cleanly
   * @param serverNumber Used as index into a list.
   * @return the region server that was stopped
   */
  public JVMClusterUtil.RegionServerThread stopRegionServer(int serverNumber) {
    return stopRegionServer(serverNumber, true);
  }

  /**
   * Shut down the specified region server cleanly
   * @param serverNumber Used as index into a list.
   * @param shutdownFS   True is we are to shutdown the filesystem as part of this regionserver's
   *                     shutdown. Usually we do but you do not want to do this if you are running
   *                     multiple regionservers in a test and you shut down one before end of the
   *                     test.
   * @return the region server that was stopped
   */
  public JVMClusterUtil.RegionServerThread stopRegionServer(int serverNumber,
    final boolean shutdownFS) {
    JVMClusterUtil.RegionServerThread server = hbaseCluster.getRegionServers().get(serverNumber);
    LOG.info("Stopping " + server.toString());
    server.getRegionServer().stop("Stopping rs " + serverNumber);
    return server;
  }

  /**
   * Suspend the specified region server
   * @param serverNumber Used as index into a list. n
   */
  public JVMClusterUtil.RegionServerThread suspendRegionServer(int serverNumber) {
    JVMClusterUtil.RegionServerThread server = hbaseCluster.getRegionServers().get(serverNumber);
    LOG.info("Suspending {}", server.toString());
    server.suspend();
    return server;
  }

  /**
   * Resume the specified region server
   * @param serverNumber Used as index into a list. n
   */
  public JVMClusterUtil.RegionServerThread resumeRegionServer(int serverNumber) {
    JVMClusterUtil.RegionServerThread server = hbaseCluster.getRegionServers().get(serverNumber);
    LOG.info("Resuming {}", server.toString());
    server.resume();
    return server;
  }

  /**
   * Wait for the specified region server to stop. Removes this thread from list of running threads.
   * n * @return Name of region server that just went down.
   */
  public String waitOnRegionServer(final int serverNumber) {
    return this.hbaseCluster.waitOnRegionServer(serverNumber);
  }

  /**
   * Starts a master thread running
   * @return New RegionServerThread
   */
  public JVMClusterUtil.MasterThread startMaster() throws IOException {
    Configuration c = HBaseConfiguration.create(conf);
    User user = HBaseTestingUtility.getDifferentUser(c, ".hfs." + index++);

    JVMClusterUtil.MasterThread t = null;
    try {
      t = hbaseCluster.addMaster(c, hbaseCluster.getMasters().size(), user);
      t.start();
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted adding master to cluster", ie);
    }
    conf.set(HConstants.MASTER_ADDRS_KEY,
      hbaseCluster.getConfiguration().get(HConstants.MASTER_ADDRS_KEY));
    return t;
  }

  /**
   * Returns the current active master, if available.
   * @return the active HMaster, null if none is active.
   */
  @Override
  public MasterService.BlockingInterface getMasterAdminService() {
    return this.hbaseCluster.getActiveMaster().getMasterRpcServices();
  }

  /**
   * Returns the current active master, if available.
   * @return the active HMaster, null if none is active.
   */
  public HMaster getMaster() {
    return this.hbaseCluster.getActiveMaster();
  }

  /**
   * Returns the current active master thread, if available.
   * @return the active MasterThread, null if none is active.
   */
  public MasterThread getMasterThread() {
    for (MasterThread mt : hbaseCluster.getLiveMasters()) {
      if (mt.getMaster().isActiveMaster()) {
        return mt;
      }
    }
    return null;
  }

  /**
   * Returns the master at the specified index, if available.
   * @return the active HMaster, null if none is active.
   */
  public HMaster getMaster(final int serverNumber) {
    return this.hbaseCluster.getMaster(serverNumber);
  }

  /**
   * Cause a master to exit without shutting down entire cluster.
   * @param serverNumber Used as index into a list.
   */
  public String abortMaster(int serverNumber) {
    HMaster server = getMaster(serverNumber);
    LOG.info("Aborting " + server.toString());
    server.abort("Aborting for tests", new Exception("Trace info"));
    return server.toString();
  }

  /**
   * Shut down the specified master cleanly
   * @param serverNumber Used as index into a list.
   * @return the region server that was stopped
   */
  public JVMClusterUtil.MasterThread stopMaster(int serverNumber) {
    return stopMaster(serverNumber, true);
  }

  /**
   * Shut down the specified master cleanly
   * @param serverNumber Used as index into a list.
   * @param shutdownFS   True is we are to shutdown the filesystem as part of this master's
   *                     shutdown. Usually we do but you do not want to do this if you are running
   *                     multiple master in a test and you shut down one before end of the test.
   * @return the master that was stopped
   */
  public JVMClusterUtil.MasterThread stopMaster(int serverNumber, final boolean shutdownFS) {
    JVMClusterUtil.MasterThread server = hbaseCluster.getMasters().get(serverNumber);
    LOG.info("Stopping " + server.toString());
    server.getMaster().stop("Stopping master " + serverNumber);
    return server;
  }

  /**
   * Wait for the specified master to stop. Removes this thread from list of running threads. n
   * * @return Name of master that just went down.
   */
  public String waitOnMaster(final int serverNumber) {
    return this.hbaseCluster.waitOnMaster(serverNumber);
  }

  /**
   * Blocks until there is an active master and that master has completed initialization.
   * @return true if an active master becomes available. false if there are no masters left. n
   */
  @Override
  public boolean waitForActiveAndReadyMaster(long timeout) throws IOException {
    List<JVMClusterUtil.MasterThread> mts;
    long start = System.currentTimeMillis();
    while (
      !(mts = getMasterThreads()).isEmpty() && (System.currentTimeMillis() - start) < timeout
    ) {
      for (JVMClusterUtil.MasterThread mt : mts) {
        if (mt.getMaster().isActiveMaster() && mt.getMaster().isInitialized()) {
          return true;
        }
      }

      Threads.sleep(100);
    }
    return false;
  }

  /**
   * @return List of master threads.
   */
  public List<JVMClusterUtil.MasterThread> getMasterThreads() {
    return this.hbaseCluster.getMasters();
  }

  /**
   * @return List of live master threads (skips the aborted and the killed)
   */
  public List<JVMClusterUtil.MasterThread> getLiveMasterThreads() {
    return this.hbaseCluster.getLiveMasters();
  }

  /**
   * Wait for Mini HBase Cluster to shut down.
   */
  public void join() {
    this.hbaseCluster.join();
  }

  /**
   * Shut down the mini HBase cluster
   */
  @Override
  public void shutdown() throws IOException {
    if (this.hbaseCluster != null) {
      this.hbaseCluster.shutdown();
    }
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0 Use
   *             {@link #getClusterMetrics()} instead.
   */
  @Deprecated
  public ClusterStatus getClusterStatus() throws IOException {
    HMaster master = getMaster();
    return master == null ? null : new ClusterStatus(master.getClusterMetrics());
  }

  @Override
  public ClusterMetrics getClusterMetrics() throws IOException {
    HMaster master = getMaster();
    return master == null ? null : master.getClusterMetrics();
  }

  private void executeFlush(HRegion region) throws IOException {
    if (!RegionReplicaUtil.isDefaultReplica(region.getRegionInfo())) {
      return;
    }
    // retry 5 times if we can not flush
    for (int i = 0; i < 5; i++) {
      FlushResult result = region.flush(true);
      if (result.getResult() != FlushResult.Result.CANNOT_FLUSH) {
        return;
      }
      Threads.sleep(1000);
    }
  }

  /**
   * Call flushCache on all regions on all participating regionservers.
   */
  public void flushcache() throws IOException {
    for (JVMClusterUtil.RegionServerThread t : this.hbaseCluster.getRegionServers()) {
      for (HRegion r : t.getRegionServer().getOnlineRegionsLocalContext()) {
        executeFlush(r);
      }
    }
  }

  /**
   * Call flushCache on all regions of the specified table.
   */
  public void flushcache(TableName tableName) throws IOException {
    for (JVMClusterUtil.RegionServerThread t : this.hbaseCluster.getRegionServers()) {
      for (HRegion r : t.getRegionServer().getOnlineRegionsLocalContext()) {
        if (r.getTableDescriptor().getTableName().equals(tableName)) {
          executeFlush(r);
        }
      }
    }
  }

  /**
   * Call flushCache on all regions on all participating regionservers. n
   */
  public void compact(boolean major) throws IOException {
    for (JVMClusterUtil.RegionServerThread t : this.hbaseCluster.getRegionServers()) {
      for (HRegion r : t.getRegionServer().getOnlineRegionsLocalContext()) {
        if (RegionReplicaUtil.isDefaultReplica(r.getRegionInfo())) {
          r.compact(major);
        }
      }
    }
  }

  /**
   * Call flushCache on all regions of the specified table. n
   */
  public void compact(TableName tableName, boolean major) throws IOException {
    for (JVMClusterUtil.RegionServerThread t : this.hbaseCluster.getRegionServers()) {
      for (HRegion r : t.getRegionServer().getOnlineRegionsLocalContext()) {
        if (r.getTableDescriptor().getTableName().equals(tableName)) {
          if (RegionReplicaUtil.isDefaultReplica(r.getRegionInfo())) {
            r.compact(major);
          }
        }
      }
    }
  }

  /**
   * @return Number of live region servers in the cluster currently.
   */
  public int getNumLiveRegionServers() {
    return this.hbaseCluster.getLiveRegionServers().size();
  }

  /**
   * @return List of region server threads. Does not return the master even though it is also a
   *         region server.
   */
  public List<JVMClusterUtil.RegionServerThread> getRegionServerThreads() {
    return this.hbaseCluster.getRegionServers();
  }

  /**
   * @return List of live region server threads (skips the aborted and the killed)
   */
  public List<JVMClusterUtil.RegionServerThread> getLiveRegionServerThreads() {
    return this.hbaseCluster.getLiveRegionServers();
  }

  /**
   * Grab a numbered region server of your choice. n * @return region server
   */
  public HRegionServer getRegionServer(int serverNumber) {
    return hbaseCluster.getRegionServer(serverNumber);
  }

  public HRegionServer getRegionServer(ServerName serverName) {
    return hbaseCluster.getRegionServers().stream().map(t -> t.getRegionServer())
      .filter(r -> r.getServerName().equals(serverName)).findFirst().orElse(null);
  }

  public List<HRegion> getRegions(byte[] tableName) {
    return getRegions(TableName.valueOf(tableName));
  }

  public List<HRegion> getRegions(TableName tableName) {
    List<HRegion> ret = new ArrayList<>();
    for (JVMClusterUtil.RegionServerThread rst : getRegionServerThreads()) {
      HRegionServer hrs = rst.getRegionServer();
      for (Region region : hrs.getOnlineRegionsLocalContext()) {
        if (region.getTableDescriptor().getTableName().equals(tableName)) {
          ret.add((HRegion) region);
        }
      }
    }
    return ret;
  }

  /**
   * @return Index into List of {@link MiniHBaseCluster#getRegionServerThreads()} of HRS carrying
   *         regionName. Returns -1 if none found.
   */
  public int getServerWithMeta() {
    return getServerWith(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
  }

  /**
   * Get the location of the specified region
   * @param regionName Name of the region in bytes
   * @return Index into List of {@link MiniHBaseCluster#getRegionServerThreads()} of HRS carrying
   *         hbase:meta. Returns -1 if none found.
   */
  public int getServerWith(byte[] regionName) {
    int index = -1;
    int count = 0;
    for (JVMClusterUtil.RegionServerThread rst : getRegionServerThreads()) {
      HRegionServer hrs = rst.getRegionServer();
      if (!hrs.isStopped()) {
        Region region = hrs.getOnlineRegion(regionName);
        if (region != null) {
          index = count;
          break;
        }
      }
      count++;
    }
    return index;
  }

  @Override
  public ServerName getServerHoldingRegion(final TableName tn, byte[] regionName)
    throws IOException {
    // Assume there is only one master thread which is the active master.
    // If there are multiple master threads, the backup master threads
    // should hold some regions. Please refer to #countServedRegions
    // to see how we find out all regions.
    HMaster master = getMaster();
    Region region = master.getOnlineRegion(regionName);
    if (region != null) {
      return master.getServerName();
    }
    int index = getServerWith(regionName);
    if (index < 0) {
      return null;
    }
    return getRegionServer(index).getServerName();
  }

  /**
   * Counts the total numbers of regions being served by the currently online region servers by
   * asking each how many regions they have. Does not look at hbase:meta at all. Count includes
   * catalog tables.
   * @return number of regions being served by all region servers
   */
  public long countServedRegions() {
    long count = 0;
    for (JVMClusterUtil.RegionServerThread rst : getLiveRegionServerThreads()) {
      count += rst.getRegionServer().getNumberOfOnlineRegions();
    }
    for (JVMClusterUtil.MasterThread mt : getLiveMasterThreads()) {
      count += mt.getMaster().getNumberOfOnlineRegions();
    }
    return count;
  }

  /**
   * Do a simulated kill all masters and regionservers. Useful when it is impossible to bring the
   * mini-cluster back for clean shutdown.
   */
  public void killAll() {
    // Do backups first.
    MasterThread activeMaster = null;
    for (MasterThread masterThread : getMasterThreads()) {
      if (!masterThread.getMaster().isActiveMaster()) {
        masterThread.getMaster().abort("killAll");
      } else {
        activeMaster = masterThread;
      }
    }
    // Do active after.
    if (activeMaster != null) {
      activeMaster.getMaster().abort("killAll");
    }
    for (RegionServerThread rst : getRegionServerThreads()) {
      rst.getRegionServer().abort("killAll");
    }
  }

  @Override
  public void waitUntilShutDown() {
    this.hbaseCluster.join();
  }

  public List<HRegion> findRegionsForTable(TableName tableName) {
    ArrayList<HRegion> ret = new ArrayList<>();
    for (JVMClusterUtil.RegionServerThread rst : getRegionServerThreads()) {
      HRegionServer hrs = rst.getRegionServer();
      for (Region region : hrs.getRegions(tableName)) {
        if (region.getTableDescriptor().getTableName().equals(tableName)) {
          ret.add((HRegion) region);
        }
      }
    }
    return ret;
  }

  protected int getRegionServerIndex(ServerName serverName) {
    // we have a small number of region servers, this should be fine for now.
    List<RegionServerThread> servers = getRegionServerThreads();
    for (int i = 0; i < servers.size(); i++) {
      if (servers.get(i).getRegionServer().getServerName().equals(serverName)) {
        return i;
      }
    }
    return -1;
  }

  protected int getMasterIndex(ServerName serverName) {
    List<MasterThread> masters = getMasterThreads();
    for (int i = 0; i < masters.size(); i++) {
      if (masters.get(i).getMaster().getServerName().equals(serverName)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public AdminService.BlockingInterface getAdminProtocol(ServerName serverName) throws IOException {
    return getRegionServer(getRegionServerIndex(serverName)).getRSRpcServices();
  }

  @Override
  public ClientService.BlockingInterface getClientProtocol(ServerName serverName)
    throws IOException {
    return getRegionServer(getRegionServerIndex(serverName)).getRSRpcServices();
  }
}
