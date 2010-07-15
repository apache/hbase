/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.client.ServerConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.executor.HBaseExecutorService;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.RegionServerOperationQueue.ProcessingResultCode;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.common.collect.Lists;

/**
 * HMaster is the "master server" for HBase. An HBase cluster has one active
 * master.  If many masters are started, all compete.  Whichever wins goes on to
 * run the cluster.  All others park themselves in their constructor until
 * master or cluster shutdown or until the active master loses its lease in
 * zookeeper.  Thereafter, all running master jostle to take over master role.
 * @see HMasterInterface
 * @see HMasterRegionInterface
 * @see Watcher
 */
public class HMaster extends Thread implements HMasterInterface,
    HMasterRegionInterface, Watcher, MasterStatus {
  // MASTER is name of the webapp and the attribute name used stuffing this
  //instance into web context.
  public static final String MASTER = "master";
  private static final Log LOG = LogFactory.getLog(HMaster.class.getName());

  // We start out with closed flag on.  Its set to off after construction.
  // Use AtomicBoolean rather than plain boolean because we want other threads
  // able to set shutdown flag.  Using AtomicBoolean can pass a reference
  // rather than have them have to know about the hosting Master class.
  final AtomicBoolean closed = new AtomicBoolean(true);
  // TODO: Is this separate flag necessary?
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  // The configuration for the Master
  private final Configuration conf;
  // server for the web ui
  private InfoServer infoServer;
  // Reporting to track master metrics.
  private final MasterMetrics metrics;

  // Our zk client. TODO: rename variable once we settle on naming
  private ZooKeeperWatcher zooKeeperWrapper;
  // Manager and zk listener for master election
  private ActiveMasterManager activeMasterManager;
  // A Sleeper that sleeps for threadWakeFrequency; sleep if nothing todo.
  private final Sleeper sleeper;
  // RPC server for the HMaster
  private final HBaseServer rpcServer;
  // Address of the HMaster
  private final HServerAddress address;
  // file system manager for the master FS operations
  private final FileSystemManager fileSystemManager;

  private final ServerConnection connection;
  // server manager to deal with region server info
  private final ServerManager serverManager;
  // region manager to deal with region specific stuff
  private final RegionManager regionManager;

  // True if this is the master that started the cluster.
  boolean isClusterStartup;

  // TODO: the following should eventually be removed from here
  private final RegionServerOperationQueue regionServerOperationQueue;
  private long lastFragmentationQuery = -1L;
  private Map<String, Integer> fragmentation = null;

  /**
   * Initializes the HMaster. The steps are as follows:
   *
   * <ol>
   * <li>Initialize HMaster RPC and address
   * <li>Connect to ZooKeeper and figure out if this is a fresh cluster start or
   *     a failed over master
   * <li>Initialize master components - server manager, region manager, metrics,
   *     region server queue, file system manager, etc
   * <li>Block until becoming active master
   * </ol>
   */
  public HMaster(Configuration conf) throws IOException {
    // initialize some variables
    this.conf = conf;
    // set the thread name
    setName(MASTER + "-" + this.address);

    /*
     * 1. Determine address and initialize RPC server (but do not start)
     *
     *    Get the master address and create an RPC server instance.  The RPC
     *    server ports can be ephemeral.
     */
    HServerAddress a = new HServerAddress(getMyAddress(this.conf));
    int numHandlers = conf.getInt("hbase.regionserver.handler.count", 10);
    this.rpcServer = HBaseRPC.getServer(this, a.getBindAddress(), a.getPort(),
                                        numHandlers, false, conf);
    this.address = new HServerAddress(rpcServer.getListenerAddress());

    /*
     * 2. Determine if this is a fresh cluster startup or failed over master
     *
     *    This is done by checking for the existence of any ephemeral
     *    RegionServer nodes in ZooKeeper.  These nodes are created by RSs on
     *    their initialization but only after they find the primary master.  As
     *    long as this check is done before we write our address into ZK, this
     *    will work.  Note that multiple masters could find this to be true on
     *    startup (none have become active master yet), which is why there is
     *    an additional check if this master does not become primary on its
     *    first attempt.
     */
    zooKeeperWrapper =
      new ZooKeeperWatcher(conf, getHServerAddress().toString(), this);
    isClusterStartup = (zooKeeperWrapper.scanRSDirectory().size() == 0);

    /*
     * 3. Initialize master components.
     *
     *    This includes the filesystem manager, server manager, region manager,
     *    metrics, queues, sleeper, etc...
     */
    this.connection = ServerConnectionManager.getConnection(conf);
    this.regionServerOperationQueue = new RegionServerOperationQueue(conf, closed);
    this.metrics = new MasterMetrics(this.getName());
    fileSystemManager = new FileSystemManager(conf, this);
    serverManager = new ServerManager(this, metrics, regionServerOperationQueue);
    regionManager = new RegionManager(this);
    // create a sleeper to sleep for a configured wait frequency
    int threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.sleeper = new Sleeper(threadWakeFrequency, this.closed);

    /*
     * 4. Block on becoming the active master.
     *
     *    We race with other masters to write our address into ZooKeeper.  If we
     *    succeed, we are the primary/active master and finish initialization.
     *
     *    If we do not succeed, there is another active master and we should
     *    now wait until it dies to try and become the next active master.  If
     *    we do not succeed on our first attempt, this is no longer a cluster
     *    startup.
     */
    activeMasterManager = new ActiveMasterManager(zooKeeperWrapper, address,
        this);
    zooKeeperWrapper.registerListener(activeMasterManager);
    // Wait here until we are the active master
    activeMasterManager.blockUntilBecomingActiveMaster();

    // We are the active master now.

    LOG.info("Server has become the active/primary master.  Address is " +
        this.address.toString());

    // run() is executed next
  }

  /**
   * Main processing loop for the HMaster.
   * 1. Handle both fresh cluster start as well as failed over initialization of
   *    the HMaster.
   * 2. Start the necessary services
   * 3. Reassign the root region
   * 4. The master is no longer closed - set "closed" to false
   */
  @Override
  public void run() {
    try {
      // If this is a fresh cluster start, make sure the root region exists.
      if(isClusterStartup()) {
        // Initialize the filesystem, which does the following:
        //   - Creates the root hbase directory in the FS
        //   - Checks the FS to make sure the root directory is readable
        //   - Creates the archive directory for logs
        fileSystemManager.initialize();
        // Do any log splitting necessary
        // TODO: Should do this in background rather than block master startup
        fileSystemManager.splitLogAfterStartup();
      }
      // TODO: fix the logic and naming for joinCluster()
      joinCluster();
      // start up all service threads.
      startServiceThreads();
      // assign the root region
      regionManager.reassignRootRegion();
      // set the master as opened
      this.closed.set(false);
      LOG.info("HMaster started on " + this.address.toString());

      while (!this.closed.get()) {
        // check if we should be shutting down
        if (this.shutdownRequested.get()) {
          // The region servers won't all exit until we stop scanning the
          // meta regions
          this.regionManager.stopScanners();
          if (this.serverManager.numServers() == 0) {
            startShutdown();
            break;
          }
          else {
            LOG.debug("Waiting on " +
             this.serverManager.getServersToServerInfo().keySet().toString());
          }
        }

        // process the operation, handle the result
        ProcessingResultCode resultCode = regionServerOperationQueue.process();
        // If FAILED op processing, bad. Will exit.
        if(resultCode == ProcessingResultCode.FAILED) {
          break;
        }
        // If bad filesystem, exit
        else if(resultCode == ProcessingResultCode.REQUEUED_BUT_PROBLEM) {
          if (!fileSystemManager.checkFileSystem()) {
            break;
          }
        }
        // Continue run loop if conditions are PROCESSED, NOOP, REQUEUED
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
      setClosed();
    }

    // Wait for all the remaining region servers to report in.
    this.serverManager.letRegionServersShutdown();

    // Clean up and close up shop
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    this.rpcServer.stop();
    this.regionManager.stop();
    this.zooKeeperWrapper.close();
    HBaseExecutorService.shutdown();
    LOG.info("HMaster main thread exiting");
  }

  /**
   * Returns true if this master process was responsible for starting the
   * cluster, false if not.
   */
  public boolean isClusterStartup() {
    return isClusterStartup;
  }

  /**
   * Sets whether this is a cluster startup or not.  Used by the
   * {@link ActiveMasterManager} to set to false if we determine another master
   * has become the primary.
   * @param isClusterStartup false if another master became active before us
   */
  public void setClusterStartup(boolean isClusterStartup) {
    this.isClusterStartup = isClusterStartup;
  }

  public HServerAddress getHServerAddress() {
    return address;
  }

  /*
   * @return This masters' address.
   * @throws UnknownHostException
   */
  private static String getMyAddress(final Configuration c)
  throws UnknownHostException {
    // Find out our address up in DNS.
    String s = DNS.getDefaultHost(c.get("hbase.master.dns.interface","default"),
      c.get("hbase.master.dns.nameserver","default"));
    s += ":" + c.get(HConstants.MASTER_PORT,
        Integer.toString(HConstants.DEFAULT_MASTER_PORT));
    return s;
  }

  /** @return HServerAddress of the master server */
  public HServerAddress getMasterAddress() {
    return this.address;
  }

  public long getProtocolVersion(String protocol, long clientVersion) {
    return HBaseRPCProtocolVersion.versionID;
  }

  /** @return InfoServer object. Maybe null.*/
  public InfoServer getInfoServer() {
    return this.infoServer;
  }

  /**
   * Return the file systen manager instance
   */
  public FileSystemManager getFileSystemManager() {
    return fileSystemManager;
  }

  /**
   * @return Return configuration being used by this server.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  public ServerManager getServerManager() {
    return this.serverManager;
  }

  public RegionManager getRegionManager() {
    return this.regionManager;
  }

  public AtomicBoolean getShutdownRequested() {
    return this.shutdownRequested;
  }

  public void setClosed() {
    this.closed.set(true);
  }

  public AtomicBoolean getClosed() {
    return this.closed;
  }

  public boolean isClosed() {
    return this.closed.get();
  }

  public ServerConnection getServerConnection() {
    return this.connection;
  }

  /**
   * Get the ZK wrapper object - needed by master_jsp.java
   * @return the zookeeper wrapper
   */
  public ZooKeeperWrapper getZooKeeperWrapper() {
    return this.zooKeeperWrapper;
  }
  /**
   * Get the HBase root dir - needed by master_jsp.java
   */
  public Path getRootDir() {
    return fileSystemManager.getRootDir();
  }

  public RegionServerOperationQueue getRegionServerOperationQueue() {
    return this.regionServerOperationQueue;
  }

  /*
   * Joins cluster.  Checks to see if this instance of HBase is fresh or the
   * master was started following a failover. In the second case, it inspects
   * the region server directory and gets their regions assignment.
   */
  private void joinCluster()  {
      LOG.debug("Checking cluster state...");
      HServerAddress rootLocation =
        this.zooKeeperWrapper.readRootRegionLocation();
      List<HServerAddress> addresses = this.zooKeeperWrapper.scanRSDirectory();
      // Check if this is a fresh start of the cluster
      if (addresses.isEmpty()) {
        LOG.debug("Master fresh start, proceeding with normal startup");
        fileSystemManager.splitLogAfterStartup();
        return;
      }
      // Failover case.
      LOG.info("Master failover, ZK inspection begins...");
      boolean isRootRegionAssigned = false;
      Map <byte[], HRegionInfo> assignedRegions =
        new HashMap<byte[], HRegionInfo>();
      // We must:
      // - contact every region server to add them to the regionservers list
      // - get their current regions assignment
      // TODO: Run in parallel?
      for (HServerAddress address : addresses) {
        HRegionInfo[] regions = null;
        try {
          HRegionInterface hri =
            this.connection.getHRegionConnection(address, false);
          HServerInfo info = hri.getHServerInfo();
          LOG.debug("Inspection found server " + info.getServerName());
          this.serverManager.recordNewServer(info, true);
          regions = hri.getRegionsAssignment();
        } catch (IOException e) {
          LOG.error("Failed contacting " + address.toString(), e);
          continue;
        }
        for (HRegionInfo r: regions) {
          if (r.isRootRegion()) {
            this.connection.setRootRegionLocation(new HRegionLocation(r, rootLocation));
            this.regionManager.setRootRegionLocation(rootLocation);
            // Undo the unassign work in the RegionManager constructor
            this.regionManager.removeRegion(r);
            isRootRegionAssigned = true;
          } else if (r.isMetaRegion()) {
            MetaRegion m = new MetaRegion(new HServerAddress(address), r);
            this.regionManager.addMetaRegionToScan(m);
          }
          assignedRegions.put(r.getRegionName(), r);
        }
      }
      LOG.info("Inspection found " + assignedRegions.size() + " regions, " +
        (isRootRegionAssigned ? "with -ROOT-" : "but -ROOT- was MIA"));
      fileSystemManager.splitLogAfterStartup();
  }

  /*
   * Start up all services. If any of these threads gets an unhandled exception
   * then they just die with a logged message.  This should be fine because
   * in general, we do not expect the master to get such unhandled exceptions
   *  as OOMEs; it should be lightly loaded. See what HRegionServer does if
   *  need to install an unexpected exception handler.
   */
  private void startServiceThreads() {
    try {
      // Start the unassigned watcher - which will create the unassigned region
      // in ZK. This is needed before RegionManager() constructor tries to assign
      // the root region.
      ZKUnassignedWatcher.start(this.conf, this);
      // start the "close region" executor service
      HBaseEventType.RS2ZK_REGION_CLOSED.startMasterExecutorService(address.toString());
      // start the "open region" executor service
      HBaseEventType.RS2ZK_REGION_OPENED.startMasterExecutorService(address.toString());
      // start the region manager
      this.regionManager.start();
      // Put up info server.
      int port = this.conf.getInt("hbase.master.info.port", 60010);
      if (port >= 0) {
        String a = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
        this.infoServer = new InfoServer(MASTER, a, port, false);
        this.infoServer.setAttribute(MASTER, this);
        this.infoServer.start();
      }
      // Start the server so everything else is running before we start
      // receiving requests.
      this.rpcServer.start();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started service threads");
      }
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        try {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        } catch (IOException ex) {
          LOG.warn("thread start", ex);
        }
      }
      // Something happened during startup. Shut things down.
      setClosed();
      LOG.error("Failed startup", e);
    }
  }

  /*
   * Start shutting down the master
   */
  public void startShutdown() {
    setClosed();
    this.regionManager.stopScanners();
    this.regionServerOperationQueue.shutdown();
    this.serverManager.notifyServers();
  }

  public MapWritable regionServerStartup(final HServerInfo serverInfo)
  throws IOException {
    // Set the ip into the passed in serverInfo.  Its ip is more than likely
    // not the ip that the master sees here.  See at end of this method where
    // we pass it back to the regionserver by setting "hbase.regionserver.address"
    String rsAddress = HBaseServer.getRemoteAddress();
    serverInfo.setServerAddress(new HServerAddress(rsAddress,
      serverInfo.getServerAddress().getPort()));
    // Register with server manager
    this.serverManager.regionServerStartup(serverInfo);
    // Send back some config info
    MapWritable mw = createConfigurationSubset();
     mw.put(new Text("hbase.regionserver.address"), new Text(rsAddress));
    return mw;
  }

  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   * the filesystem to use and root directory to use.
   */
  protected MapWritable createConfigurationSubset() {
    MapWritable mw = addConfig(new MapWritable(), HConstants.HBASE_DIR);
    return addConfig(mw, "fs.default.name");
  }

  private MapWritable addConfig(final MapWritable mw, final String key) {
    mw.put(new Text(key), new Text(this.conf.get(key)));
    return mw;
  }

  public HMsg [] regionServerReport(HServerInfo serverInfo, HMsg msgs[],
    HRegionInfo[] mostLoadedRegions)
  throws IOException {
    return adornRegionServerAnswer(serverInfo,
      this.serverManager.regionServerReport(serverInfo, msgs, mostLoadedRegions));
  }

  /**
   * Override if you'd add messages to return to regionserver <code>hsi</code>
   * or to send an exception.
   * @param msgs Messages to add to
   * @return Messages to return to
   * @throws IOException exceptions that were injected for the region servers
   */
  protected HMsg [] adornRegionServerAnswer(final HServerInfo hsi,
      final HMsg [] msgs) throws IOException {
    return msgs;
  }

  public boolean isMasterRunning() {
    return !this.closed.get();
  }

  public void shutdown() {
    LOG.info("Cluster shutdown requested. Starting to quiesce servers");
    this.shutdownRequested.set(true);
    this.zooKeeperWrapper.setClusterState(false);
  }

  public void createTable(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }
    HRegionInfo [] newRegions = null;
    if(splitKeys == null || splitKeys.length == 0) {
      newRegions = new HRegionInfo [] { new HRegionInfo(desc, null, null) };
    } else {
      int numRegions = splitKeys.length + 1;
      newRegions = new HRegionInfo[numRegions];
      byte [] startKey = null;
      byte [] endKey = null;
      for(int i=0;i<numRegions;i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
        newRegions[i] = new HRegionInfo(desc, startKey, endKey);
        startKey = endKey;
      }
    }
    int numRetries =  conf.getInt("hbase.client.retries.number", 2);
    for (int tries = 0; tries < numRetries; tries++) {
      try {
        // We can not create a table unless meta regions have already been
        // assigned and scanned.
        if (!this.regionManager.areAllMetaRegionsOnline()) {
          throw new NotAllMetaRegionsOnlineException();
        }
        if (!this.serverManager.canAssignUserRegions()) {
          throw new IOException("not enough servers to create table yet");
        }
        createTable(newRegions);
        LOG.info("created table " + desc.getNameAsString());
        break;
      } catch (TableExistsException e) {
        throw e;
      } catch (IOException e) {
        if (tries == numRetries - 1) {
          throw RemoteExceptionHandler.checkIOException(e);
        }
        this.sleeper.sleep();
      }
    }
  }

  private synchronized void createTable(final HRegionInfo [] newRegions)
  throws IOException {
    String tableName = newRegions[0].getTableDesc().getNameAsString();
    // 1. Check to see if table already exists. Get meta region where
    // table would sit should it exist. Open scanner on it. If a region
    // for the table we want to create already exists, then table already
    // created. Throw already-exists exception.
    MetaRegion m = regionManager.getFirstMetaRegionForRegion(newRegions[0]);
    byte [] metaRegionName = m.getRegionName();
    HRegionInterface srvr = this.connection.getHRegionConnection(m.getServer());
    byte[] firstRowInTable = Bytes.toBytes(tableName + ",,");
    Scan scan = new Scan(firstRowInTable);
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    long scannerid = srvr.openScanner(metaRegionName, scan);
    try {
      Result data = srvr.next(scannerid);
      if (data != null && data.size() > 0) {
        HRegionInfo info = Writables.getHRegionInfo(
          data.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER));
        if (info.getTableDesc().getNameAsString().equals(tableName)) {
          // A region for this table already exists. Ergo table exists.
          throw new TableExistsException(tableName);
        }
      }
    } finally {
      srvr.close(scannerid);
    }
    for(HRegionInfo newRegion : newRegions) {
      regionManager.createRegion(newRegion, srvr, metaRegionName);
    }
  }

  public void deleteTable(final byte [] tableName) throws IOException {
    if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
      throw new IOException("Can't delete root table");
    }
    new TableDelete(this, tableName).process();
    LOG.info("deleted table: " + Bytes.toString(tableName));
  }

  public void addColumn(byte [] tableName, HColumnDescriptor column)
  throws IOException {
    new AddColumn(this, tableName, column).process();
  }

  public void modifyColumn(byte [] tableName, byte [] columnName,
    HColumnDescriptor descriptor)
  throws IOException {
    new ModifyColumn(this, tableName, columnName, descriptor).process();
  }

  public void deleteColumn(final byte [] tableName, final byte [] c)
  throws IOException {
    new DeleteColumn(this, tableName, KeyValue.parseColumn(c)[0]).process();
  }

  public void enableTable(final byte [] tableName) throws IOException {
    if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
      throw new IOException("Can't enable root table");
    }
    new ChangeTableState(this, tableName, true).process();
  }

  public void disableTable(final byte [] tableName) throws IOException {
    if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
      throw new IOException("Can't disable root table");
    }
    new ChangeTableState(this, tableName, false).process();
  }

  /**
   * Get a list of the regions for a given table. The pairs may have
   * null for their second element in the case that they are not
   * currently deployed.
   * TODO: Redo so this method does not duplicate code with subsequent methods.
   */
  List<Pair<HRegionInfo,HServerAddress>> getTableRegions(
      final byte [] tableName)
  throws IOException {
    final ArrayList<Pair<HRegionInfo, HServerAddress>> result =
      Lists.newArrayList();
    MetaScannerVisitor visitor =
      new MetaScannerVisitor() {
        @Override
        public boolean processRow(Result data) throws IOException {
          if (data == null || data.size() <= 0) {
            return true;
          }
          Pair<HRegionInfo, HServerAddress> pair =
            metaRowToRegionPair(data);
          if (pair == null) {
            return false;
          }
          if (!Bytes.equals(pair.getFirst().getTableDesc().getName(),
                tableName)) {
            return false;
          }
          result.add(pair);
          return true;
        }
    };

    MetaScanner.metaScan(conf, visitor, tableName);
    return result;
  }

  private Pair<HRegionInfo, HServerAddress> metaRowToRegionPair(
      Result data) throws IOException {
    HRegionInfo info = Writables.getHRegionInfo(
        data.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER));
    final byte[] value = data.getValue(HConstants.CATALOG_FAMILY,
        HConstants.SERVER_QUALIFIER);
    if (value != null && value.length > 0) {
      HServerAddress server = new HServerAddress(Bytes.toString(value));
      return new Pair<HRegionInfo,HServerAddress>(info, server);
    } else {
      //undeployed
      return new Pair<HRegionInfo, HServerAddress>(info, null);
    }
  }

  /**
   * Return the region and current deployment for the region containing
   * the given row. If the region cannot be found, returns null. If it
   * is found, but not currently deployed, the second element of the pair
   * may be null.
   */
  Pair<HRegionInfo,HServerAddress> getTableRegionForRow(
      final byte [] tableName, final byte [] rowKey)
  throws IOException {
    final AtomicReference<Pair<HRegionInfo, HServerAddress>> result =
      new AtomicReference<Pair<HRegionInfo, HServerAddress>>(null);

    MetaScannerVisitor visitor =
      new MetaScannerVisitor() {
        @Override
        public boolean processRow(Result data) throws IOException {
          if (data == null || data.size() <= 0) {
            return true;
          }
          Pair<HRegionInfo, HServerAddress> pair =
            metaRowToRegionPair(data);
          if (pair == null) {
            return false;
          }
          if (!Bytes.equals(pair.getFirst().getTableDesc().getName(),
                tableName)) {
            return false;
          }
          result.set(pair);
          return true;
        }
    };

    MetaScanner.metaScan(conf, visitor, tableName, rowKey, 1);
    return result.get();
  }

  Pair<HRegionInfo,HServerAddress> getTableRegionFromName(
      final byte [] regionName)
  throws IOException {
    byte [] tableName = HRegionInfo.parseRegionName(regionName)[0];

    Set<MetaRegion> regions = regionManager.getMetaRegionsForTable(tableName);
    for (MetaRegion m: regions) {
      byte [] metaRegionName = m.getRegionName();
      HRegionInterface srvr = connection.getHRegionConnection(m.getServer());
      Get get = new Get(regionName);
      get.addColumn(HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER);
      get.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      Result data = srvr.get(metaRegionName, get);
      if(data == null || data.size() <= 0) {
        continue;
      }
      return metaRowToRegionPair(data);
    }
    return null;
  }

  /**
   * Get row from meta table.
   * @param row
   * @param family
   * @return Result
   * @throws IOException
   */
  protected Result getFromMETA(final byte [] row, final byte [] family)
  throws IOException {
    MetaRegion meta = this.regionManager.getMetaRegionForRow(row);
    HRegionInterface srvr = getMETAServer(meta);
    Get get = new Get(row);
    get.addFamily(family);
    return srvr.get(meta.getRegionName(), get);
  }

  /*
   * @param meta
   * @return Server connection to <code>meta</code> .META. region.
   * @throws IOException
   */
  private HRegionInterface getMETAServer(final MetaRegion meta)
  throws IOException {
    return this.connection.getHRegionConnection(meta.getServer());
  }

  public void modifyTable(final byte[] tableName, HConstants.Modify op,
      Writable[] args)
  throws IOException {
    switch (op) {
    case TABLE_SET_HTD:
      if (args == null || args.length < 1 ||
          !(args[0] instanceof HTableDescriptor)) {
        throw new IOException("SET_HTD request requires an HTableDescriptor");
      }
      HTableDescriptor htd = (HTableDescriptor) args[0];
      LOG.info("modifyTable(SET_HTD): " + htd);
      new ModifyTableMeta(this, tableName, htd).process();
      break;

    case TABLE_SPLIT:
    case TABLE_COMPACT:
    case TABLE_MAJOR_COMPACT:
    case TABLE_FLUSH:
      if (args != null && args.length > 0) {
        if (!(args[0] instanceof ImmutableBytesWritable)) {
          throw new IOException(
            "request argument must be ImmutableBytesWritable");
        }
        Pair<HRegionInfo,HServerAddress> pair = null;
        if(tableName == null) {
          byte [] regionName = ((ImmutableBytesWritable)args[0]).get();
          pair = getTableRegionFromName(regionName);
        } else {
          byte [] rowKey = ((ImmutableBytesWritable)args[0]).get();
          pair = getTableRegionForRow(tableName, rowKey);
        }
        if (pair != null && pair.getSecond() != null) {
          this.regionManager.startAction(pair.getFirst().getRegionName(),
            pair.getFirst(), pair.getSecond(), op);
        }
      } else {
        for (Pair<HRegionInfo,HServerAddress> pair: getTableRegions(tableName)) {
          if (pair.getSecond() == null) {
            continue; // undeployed
          }
          this.regionManager.startAction(pair.getFirst().getRegionName(),
            pair.getFirst(), pair.getSecond(), op);
        }
      }
      break;

    case CLOSE_REGION:
      if (args == null || args.length < 1 || args.length > 2) {
        throw new IOException("Requires at least a region name; " +
          "or cannot have more than region name and servername");
      }
      // Arguments are regionname and an optional server name.
      byte [] regionname = ((ImmutableBytesWritable)args[0]).get();
      LOG.debug("Attempting to close region: " + Bytes.toStringBinary(regionname));
      String hostnameAndPort = null;
      if (args.length == 2) {
        hostnameAndPort = Bytes.toString(((ImmutableBytesWritable)args[1]).get());
      }
      // Need hri
      Result rr = getFromMETA(regionname, HConstants.CATALOG_FAMILY);
      HRegionInfo hri = RegionManager.getHRegionInfo(rr.getRow(), rr);
      if (hostnameAndPort == null) {
        // Get server from the .META. if it wasn't passed as argument
        hostnameAndPort =
          Bytes.toString(rr.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER));
      }
      // Take region out of the intransistions in case it got stuck there doing
      // an open or whatever.
      this.regionManager.clearFromInTransition(regionname);
      // If hostnameAndPort is still null, then none, exit.
      if (hostnameAndPort == null) {
        break;
      }
      long startCode =
        Bytes.toLong(rr.getValue(HConstants.CATALOG_FAMILY,
            HConstants.STARTCODE_QUALIFIER));
      String name = HServerInfo.getServerName(hostnameAndPort, startCode);
      LOG.info("Marking " + hri.getRegionNameAsString() +
        " as closing on " + name + "; cleaning SERVER + STARTCODE; " +
          "master will tell regionserver to close region on next heartbeat");
      this.regionManager.setClosing(name, hri, hri.isOffline());
      MetaRegion meta = this.regionManager.getMetaRegionForRow(regionname);
      HRegionInterface srvr = getMETAServer(meta);
      HRegion.cleanRegionInMETA(srvr, meta.getRegionName(), hri);
      break;

    default:
      throw new IOException("unsupported modifyTable op " + op);
    }
  }

  /**
   * @return cluster status
   */
  public ClusterStatus getClusterStatus() {
    ClusterStatus status = new ClusterStatus();
    status.setHBaseVersion(VersionInfo.getVersion());
    status.setServerInfo(serverManager.getServersToServerInfo().values());
    status.setDeadServers(serverManager.getDeadServers());
    status.setRegionsInTransition(this.regionManager.getRegionsInTransition());
    return status;
  }

  /**
   * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
   */
  @Override
  public void process(WatchedEvent event) {
    LOG.debug("Event " + event.getType() +
              " with state " + event.getState() +
              " with path " + event.getPath());
    // Master should kill itself if its session expired or if its
    // znode was deleted manually (usually for testing purposes)
    if(event.getState() == KeeperState.Expired ||
      (event.getType().equals(EventType.NodeDeleted) &&
        event.getPath().equals(this.zooKeeperWrapper.getMasterElectionZNode())) &&
        !shutdownRequested.get()) {

      LOG.info("Master lost its znode, trying to get a new one");

      // Can we still be the master? If not, goodbye

      zooKeeperWrapper.close();
      try {
        // TODO: this is broken, we should just shutdown now not restart
        zooKeeperWrapper =
          new ZooKeeperWatcher(conf, HMaster.class.getName(), this);
        zooKeeperWrapper.registerListener(this);
        activeMasterManager = new ActiveMasterManager(zooKeeperWrapper,
            this.address, this);
        activeMasterManager.blockUntilBecomingActiveMaster();

        // we are a failed over master, reset the fact that we started the
        // cluster
        setClusterStartup(false);
        // Verify the cluster to see if anything happened while we were away
        joinCluster();
      } catch (Exception e) {
        LOG.error("Killing master because of", e);
        System.exit(1);
      }
    }
  }

  private static void printUsageAndExit() {
    System.err.println("Usage: Master [opts] start|stop");
    System.err.println(" start  Start Master. If local mode, start Master and RegionServer in same JVM");
    System.err.println(" stop   Start cluster shutdown; Master signals RegionServer shutdown");
    System.err.println(" where [opts] are:");
    System.err.println("   --minServers=<servers>    Minimum RegionServers needed to host user tables.");
    System.exit(0);
  }

  /**
   * Utility for constructing an instance of the passed HMaster class.
   * @param masterClass
   * @param conf
   * @return HMaster instance.
   */
  public static HMaster constructMaster(Class<? extends HMaster> masterClass,
      final Configuration conf)  {
    try {
      Constructor<? extends HMaster> c =
        masterClass.getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " +
        "Master: " + masterClass.toString() +
        ((e.getCause() != null)? e.getCause().getMessage(): ""), e);
    }
  }

  /*
   * Version of master that will shutdown the passed zk cluster on its way out.
   */
  static class LocalHMaster extends HMaster {
    private MiniZooKeeperCluster zkcluster = null;

    public LocalHMaster(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public void run() {
      super.run();
      if (this.zkcluster != null) {
        try {
          this.zkcluster.shutdown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    void setZKCluster(final MiniZooKeeperCluster zkcluster) {
      this.zkcluster = zkcluster;
    }
  }

  protected static void doMain(String [] args,
      Class<? extends HMaster> masterClass) {
    if (args.length < 1) {
      printUsageAndExit();
    }
    Configuration conf = HBaseConfiguration.create();
    // Process command-line args.
    for (String cmd: args) {

      if (cmd.startsWith("--minServers=")) {
        conf.setInt("hbase.regions.server.count.min",
          Integer.valueOf(cmd.substring(13)));
        continue;
      }

      if (cmd.equalsIgnoreCase("start")) {
        try {
          // Print out vm stats before starting up.
          RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
          if (runtime != null) {
            LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
              runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
            LOG.info("vmInputArguments=" + runtime.getInputArguments());
          }
          // If 'local', defer to LocalHBaseCluster instance.  Starts master
          // and regionserver both in the one JVM.
          if (LocalHBaseCluster.isLocal(conf)) {
            final MiniZooKeeperCluster zooKeeperCluster =
              new MiniZooKeeperCluster();
            File zkDataPath = new File(conf.get("hbase.zookeeper.property.dataDir"));
            int zkClientPort = conf.getInt("hbase.zookeeper.property.clientPort", 0);
            if (zkClientPort == 0) {
              throw new IOException("No config value for hbase.zookeeper.property.clientPort");
            }
            zooKeeperCluster.setTickTime(conf.getInt("hbase.zookeeper.property.tickTime", 3000));
            zooKeeperCluster.setClientPort(zkClientPort);
            int clientPort = zooKeeperCluster.startup(zkDataPath);
            if (clientPort != zkClientPort) {
              String errorMsg = "Couldnt start ZK at requested address of " +
                  zkClientPort + ", instead got: " + clientPort + ". Aborting. Why? " +
                  "Because clients (eg shell) wont be able to find this ZK quorum";
              System.err.println(errorMsg);
              throw new IOException(errorMsg);
            }
            conf.set("hbase.zookeeper.property.clientPort",
              Integer.toString(clientPort));
            // Need to have the zk cluster shutdown when master is shutdown.
            // Run a subclass that does the zk cluster shutdown on its way out.
            LocalHBaseCluster cluster = new LocalHBaseCluster(conf, 1,
              LocalHMaster.class, HRegionServer.class);
            ((LocalHMaster)cluster.getMaster()).setZKCluster(zooKeeperCluster);
            cluster.startup();
          } else {
            HMaster master = constructMaster(masterClass, conf);
            if (master.shutdownRequested.get()) {
              LOG.info("Won't bring the Master up as a shutdown is requested");
              return;
            }
            master.start();
          }
        } catch (Throwable t) {
          LOG.error("Failed to start master", t);
          System.exit(-1);
        }
        break;
      }

      if (cmd.equalsIgnoreCase("stop")) {
        HBaseAdmin adm = null;
        try {
          adm = new HBaseAdmin(conf);
        } catch (MasterNotRunningException e) {
          LOG.error("Master not running");
          System.exit(0);
        } catch (ZooKeeperConnectionException e) {
          LOG.error("ZooKeeper not available");
          System.exit(0);
        }
        try {
          adm.shutdown();
        } catch (Throwable t) {
          LOG.error("Failed to stop master", t);
          System.exit(-1);
        }
        break;
      }

      // Print out usage if we get to here.
      printUsageAndExit();
    }
  }

  public Map<String, Integer> getTableFragmentation() throws IOException {
    long now = System.currentTimeMillis();
    // only check every two minutes by default
    int check = this.conf.getInt("hbase.master.fragmentation.check.frequency", 2 * 60 * 1000);
    if (lastFragmentationQuery == -1 || now - lastFragmentationQuery > check) {
      fragmentation = FSUtils.getTableFragmentation(this);
      lastFragmentationQuery = now;
    }
    return fragmentation;
  }

  /**
   * Main program
   * @param args
   */
  public static void main(String [] args) {
    doMain(args, HMaster.class);
  }

  @Override
  public void abortServer() {
    this.startShutdown();
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zooKeeperWrapper;
  }
}
