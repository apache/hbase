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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.RegionAlreadyInTransitionException;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.regionserver.wal.OrphanHLogAfterSplitException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * This class abstracts a bunch of operations the HMaster needs to interact with
 * the underlying file system, including splitting log files, checking file
 * system status, etc.
 */
@InterfaceAudience.Private
public class MasterFileSystem {
  private static final Log LOG = LogFactory.getLog(MasterFileSystem.class.getName());
  // HBase configuration
  Configuration conf;
  // master status
  Server master;
  // metrics for master
  MasterMetrics metrics;
  // Persisted unique cluster ID
  private ClusterId clusterId;
  // Keep around for convenience.
  private final FileSystem fs;
  // Is the fileystem ok?
  private volatile boolean fsOk = true;
  // The Path to the old logs dir
  private final Path oldLogDir;
  // root hbase directory on the FS
  private final Path rootdir;
  // create the split log lock
  final Lock splitLogLock = new ReentrantLock();
  final boolean distributedLogSplitting;
  final SplitLogManager splitLogManager;
  private final MasterServices services;

  public MasterFileSystem(Server master, MasterServices services,
      MasterMetrics metrics, boolean masterRecovery)
  throws IOException {
    this.conf = master.getConfiguration();
    this.master = master;
    this.services = services;
    this.metrics = metrics;
    // Set filesystem to be that of this.rootdir else we get complaints about
    // mismatched filesystems if hbase.rootdir is hdfs and fs.defaultFS is
    // default localfs.  Presumption is that rootdir is fully-qualified before
    // we get to here with appropriate fs scheme.
    this.rootdir = FSUtils.getRootDir(conf);
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    this.fs = this.rootdir.getFileSystem(conf);
    String fsUri = this.fs.getUri().toString();
    conf.set("fs.default.name", fsUri);
    conf.set("fs.defaultFS", fsUri);
    // make sure the fs has the same conf
    fs.setConf(conf);
    this.distributedLogSplitting =
      conf.getBoolean(HConstants.DISTRIBUTED_LOG_SPLITTING_KEY, true);
    if (this.distributedLogSplitting) {
      this.splitLogManager = new SplitLogManager(master.getZooKeeper(),
          master.getConfiguration(), master, services, master.getServerName());
      this.splitLogManager.finishInitialization(masterRecovery);
    } else {
      this.splitLogManager = null;
    }
    // setup the filesystem variable
    // set up the archived logs path
    this.oldLogDir = createInitialFileSystemLayout();
    HFileSystem.addLocationsOrderInterceptor(conf);
  }

  /**
   * Create initial layout in filesystem.
   * <ol>
   * <li>Check if the root region exists and is readable, if not create it.
   * Create hbase.version and the -ROOT- directory if not one.
   * </li>
   * <li>Create a log archive directory for RS to put archived logs</li>
   * </ol>
   * Idempotent.
   */
  private Path createInitialFileSystemLayout() throws IOException {
    // check if the root directory exists
    checkRootDir(this.rootdir, conf, this.fs);

    Path oldLogDir = new Path(this.rootdir, HConstants.HREGION_OLDLOGDIR_NAME);

    // Make sure the region servers can archive their old logs
    if(!this.fs.exists(oldLogDir)) {
      this.fs.mkdirs(oldLogDir);
    }

    return oldLogDir;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  /**
   * Get the directory where old logs go
   * @return the dir
   */
  public Path getOldLogDir() {
    return this.oldLogDir;
  }

  /**
   * Checks to see if the file system is still accessible.
   * If not, sets closed
   * @return false if file system is not available
   */
  public boolean checkFileSystem() {
    if (this.fsOk) {
      try {
        FSUtils.checkFileSystemAvailable(this.fs);
        FSUtils.checkDfsSafeMode(this.conf);
      } catch (IOException e) {
        master.abort("Shutting down HBase cluster: file system not available", e);
        this.fsOk = false;
      }
    }
    return this.fsOk;
  }

  /**
   * @return HBase root dir.
   */
  public Path getRootDir() {
    return this.rootdir;
  }

  /**
   * @return The unique identifier generated for this cluster
   */
  public ClusterId getClusterId() {
    return clusterId;
  }

  /**
   * Inspect the log directory to recover any log file without
   * an active region server.
   */
  void splitLogAfterStartup() {
    boolean retrySplitting = !conf.getBoolean("hbase.hlog.split.skip.errors",
        HLog.SPLIT_SKIP_ERRORS_DEFAULT);
    Path logsDirPath = new Path(this.rootdir, HConstants.HREGION_LOGDIR_NAME);
    do {
      if (master.isStopped()) {
        LOG.warn("Master stopped while splitting logs");
        break;
      }
      List<ServerName> serverNames = new ArrayList<ServerName>();
      try {
        if (!this.fs.exists(logsDirPath)) return;
        FileStatus[] logFolders = FSUtils.listStatus(this.fs, logsDirPath, null);
        // Get online servers after getting log folders to avoid log folder deletion of newly
        // checked in region servers . see HBASE-5916
        Set<ServerName> onlineServers = ((HMaster) master).getServerManager().getOnlineServers()
            .keySet();

        if (logFolders == null || logFolders.length == 0) {
          LOG.debug("No log files to split, proceeding...");
          return;
        }
        for (FileStatus status : logFolders) {
          String sn = status.getPath().getName();
          // truncate splitting suffix if present (for ServerName parsing)
          if (sn.endsWith(HLog.SPLITTING_EXT)) {
            sn = sn.substring(0, sn.length() - HLog.SPLITTING_EXT.length());
          }
          ServerName serverName = ServerName.parseServerName(sn);
          if (!onlineServers.contains(serverName)) {
            LOG.info("Log folder " + status.getPath() + " doesn't belong "
                + "to a known region server, splitting");
            serverNames.add(serverName);
          } else {
            LOG.info("Log folder " + status.getPath()
                + " belongs to an existing region server");
          }
        }
        splitLog(serverNames);
        retrySplitting = false;
      } catch (IOException ioe) {
        LOG.warn("Failed splitting of " + serverNames, ioe);
        if (!checkFileSystem()) {
          LOG.warn("Bad Filesystem, exiting");
          Runtime.getRuntime().halt(1);
        }
        try {
          if (retrySplitting) {
            Thread.sleep(conf.getInt(
              "hbase.hlog.split.failure.retry.interval", 30 * 1000));
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted, aborting since cannot return w/o splitting");
          Thread.currentThread().interrupt();
          retrySplitting = false;
          Runtime.getRuntime().halt(1);
        }
      }
    } while (retrySplitting);
  }
  
  public void splitLog(final ServerName serverName) throws IOException {
    List<ServerName> serverNames = new ArrayList<ServerName>();
    serverNames.add(serverName);
    splitLog(serverNames);
  }
  
  public void splitLog(final List<ServerName> serverNames) throws IOException {
    long splitTime = 0, splitLogSize = 0;
    List<Path> logDirs = new ArrayList<Path>();
    for (ServerName serverName: serverNames) {
      Path logDir = new Path(this.rootdir, HLogUtil.getHLogDirectoryName(serverName.toString()));
      Path splitDir = logDir.suffix(HLog.SPLITTING_EXT);
      // Rename the directory so a rogue RS doesn't create more HLogs
      if (fs.exists(logDir)) {
        if (!this.fs.rename(logDir, splitDir)) {
          throw new IOException("Failed fs.rename for log split: " + logDir);
        }
        logDir = splitDir;
        LOG.debug("Renamed region directory: " + splitDir);
      } else if (!fs.exists(splitDir)) {
        LOG.info("Log dir for server " + serverName + " does not exist");
        continue;
      }
      logDirs.add(splitDir);
    }

    if (logDirs.isEmpty()) {
      LOG.info("No logs to split");
      return;
    }

    if (distributedLogSplitting) {
      splitLogManager.handleDeadWorkers(serverNames);
      splitTime = EnvironmentEdgeManager.currentTimeMillis();
      splitLogSize = splitLogManager.splitLogDistributed(logDirs);
      splitTime = EnvironmentEdgeManager.currentTimeMillis() - splitTime;
    } else {
      for(Path logDir: logDirs){
        // splitLogLock ensures that dead region servers' logs are processed
        // one at a time
        this.splitLogLock.lock();
        try {              
          HLogSplitter splitter = HLogSplitter.createLogSplitter(
            conf, rootdir, logDir, oldLogDir, this.fs);
          try {
            // If FS is in safe mode, just wait till out of it.
            FSUtils.waitOnSafeMode(conf, conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 1000));
            splitter.splitLog();
          } catch (OrphanHLogAfterSplitException e) {
            LOG.warn("Retrying splitting because of:", e);
            //An HLogSplitter instance can only be used once.  Get new instance.
            splitter = HLogSplitter.createLogSplitter(conf, rootdir, logDir,
              oldLogDir, this.fs);
            splitter.splitLog();
          }
          splitTime = splitter.getTime();
          splitLogSize = splitter.getSize();
        } finally {
          this.splitLogLock.unlock();
        }
      }
    }

    if (this.metrics != null) {
      this.metrics.addSplit(splitTime, splitLogSize);
    }
  }

  /**
   * Get the rootdir.  Make sure its wholesome and exists before returning.
   * @param rd
   * @param conf
   * @param fs
   * @return hbase.rootdir (after checks for existence and bootstrapping if
   * needed populating the directory with necessary bootup files).
   * @throws IOException
   */
  private Path checkRootDir(final Path rd, final Configuration c,
    final FileSystem fs)
  throws IOException {
    // If FS is in safe mode wait till out of it.
    FSUtils.waitOnSafeMode(c, c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000));
    // Filesystem is good. Go ahead and check for hbase.rootdir.
    try {
      if (!fs.exists(rd)) {
        fs.mkdirs(rd);
        // DFS leaves safe mode with 0 DNs when there are 0 blocks.
        // We used to handle this by checking the current DN count and waiting until
        // it is nonzero. With security, the check for datanode count doesn't work --
        // it is a privileged op. So instead we adopt the strategy of the jobtracker
        // and simply retry file creation during bootstrap indefinitely. As soon as
        // there is one datanode it will succeed. Permission problems should have
        // already been caught by mkdirs above.
        FSUtils.setVersion(fs, rd, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
          10 * 1000), c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      } else {
        if (!fs.isDirectory(rd)) {
          throw new IllegalArgumentException(rd.toString() + " is not a directory");
        }
        // as above
        FSUtils.checkVersion(fs, rd, true, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
          10 * 1000), c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      }
    } catch (DeserializationException de) {
      LOG.fatal("Please fix invalid configuration for " + HConstants.HBASE_DIR, de);
      IOException ioe = new IOException();
      ioe.initCause(de);
      throw ioe;
    } catch (IllegalArgumentException iae) {
      LOG.fatal("Please fix invalid configuration for "
        + HConstants.HBASE_DIR + " " + rd.toString(), iae);
      throw iae;
    }
    // Make sure cluster ID exists
    if (!FSUtils.checkClusterIdExists(fs, rd, c.getInt(
        HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000))) {
      FSUtils.setClusterId(fs, rd, new ClusterId(), c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000));
    }
    clusterId = FSUtils.getClusterId(fs, rd);

    // Make sure the root region directory exists!
    if (!FSUtils.rootRegionExists(fs, rd)) {
      bootstrap(rd, c);
    }
    createRootTableInfo(rd);
    return rd;
  }

  private void createRootTableInfo(Path rd) throws IOException {
    // Create ROOT tableInfo if required.
    if (!FSTableDescriptors.isTableInfoExists(fs, rd,
        Bytes.toString(HRegionInfo.ROOT_REGIONINFO.getTableName()))) {
      FSTableDescriptors.createTableDescriptor(HTableDescriptor.ROOT_TABLEDESC, this.conf);
    }
  }

  private static void bootstrap(final Path rd, final Configuration c)
  throws IOException {
    LOG.info("BOOTSTRAP: creating ROOT and first META regions");
    try {
      // Bootstrapping, make sure blockcache is off.  Else, one will be
      // created here in bootstap and it'll need to be cleaned up.  Better to
      // not make it in first place.  Turn off block caching for bootstrap.
      // Enable after.
      HRegionInfo rootHRI = new HRegionInfo(HRegionInfo.ROOT_REGIONINFO);
      setInfoFamilyCachingForRoot(false);
      HRegionInfo metaHRI = new HRegionInfo(HRegionInfo.FIRST_META_REGIONINFO);
      setInfoFamilyCachingForMeta(false);
      HRegion root = HRegion.createHRegion(rootHRI, rd, c,
          HTableDescriptor.ROOT_TABLEDESC);
      HRegion meta = HRegion.createHRegion(metaHRI, rd, c,
          HTableDescriptor.META_TABLEDESC);
      setInfoFamilyCachingForRoot(true);
      setInfoFamilyCachingForMeta(true);
      // Add first region from the META table to the ROOT region.
      HRegion.addRegionToMETA(root, meta);
      HRegion.closeHRegion(root);
      HRegion.closeHRegion(meta);
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.error("bootstrap", e);
      throw e;
    }
  }

  /**
   * Enable in-memory caching for -ROOT-
   */
  public static void setInfoFamilyCachingForRoot(final boolean b) {
    for (HColumnDescriptor hcd:
        HTableDescriptor.ROOT_TABLEDESC.getColumnFamilies()) {
       if (Bytes.equals(hcd.getName(), HConstants.CATALOG_FAMILY)) {
         hcd.setBlockCacheEnabled(b);
         hcd.setInMemory(b);
     }
    }
  }

  /**
   * Enable in memory caching for .META.
   */
  public static void setInfoFamilyCachingForMeta(final boolean b) {
    for (HColumnDescriptor hcd:
        HTableDescriptor.META_TABLEDESC.getColumnFamilies()) {
      if (Bytes.equals(hcd.getName(), HConstants.CATALOG_FAMILY)) {
        hcd.setBlockCacheEnabled(b);
        hcd.setInMemory(b);
      }
    }
  }


  public void deleteRegion(HRegionInfo region) throws IOException {
    HFileArchiver.archiveRegion(fs, region);
  }

  public void deleteTable(byte[] tableName) throws IOException {
    fs.delete(new Path(rootdir, Bytes.toString(tableName)), true);
  }

  public void updateRegionInfo(HRegionInfo region) {
    // TODO implement this.  i think this is currently broken in trunk i don't
    //      see this getting updated.
    //      @see HRegion.checkRegioninfoOnFilesystem()
  }

  public void deleteFamilyFromFS(HRegionInfo region, byte[] familyName)
      throws IOException {
    Path delDir = new Path(rootdir,
        new Path(region.getTableNameAsString(), new Path(
            region.getEncodedName(), new Path(Bytes.toString(familyName)))));
    if (fs.delete(delDir, true) == false) {
      throw new IOException("Could not delete family "
          + Bytes.toString(familyName) + " from FileSystem for region "
          + region.getRegionNameAsString() + "(" + region.getEncodedName()
          + ")");
    }
  }

  public void stop() {
    if (splitLogManager != null) {
      this.splitLogManager.stop();
    }
  }

  /**
   * Create new HTableDescriptor in HDFS.
   * 
   * @param htableDescriptor
   */
  public void createTableDescriptor(HTableDescriptor htableDescriptor)
      throws IOException {
    FSTableDescriptors.createTableDescriptor(htableDescriptor, conf);
  }

  /**
   * Delete column of a table
   * @param tableName
   * @param familyName
   * @return Modified HTableDescriptor with requested column deleted.
   * @throws IOException
   */
  public HTableDescriptor deleteColumn(byte[] tableName, byte[] familyName)
      throws IOException {
    LOG.info("DeleteColumn. Table = " + Bytes.toString(tableName)
        + " family = " + Bytes.toString(familyName));
    HTableDescriptor htd = this.services.getTableDescriptors().get(tableName);
    htd.removeFamily(familyName);
    this.services.getTableDescriptors().add(htd);
    return htd;
  }

  /**
   * Modify Column of a table
   * @param tableName
   * @param hcd HColumnDesciptor
   * @return Modified HTableDescriptor with the column modified.
   * @throws IOException
   */
  public HTableDescriptor modifyColumn(byte[] tableName, HColumnDescriptor hcd)
      throws IOException {
    LOG.info("AddModifyColumn. Table = " + Bytes.toString(tableName)
        + " HCD = " + hcd.toString());

    HTableDescriptor htd = this.services.getTableDescriptors().get(tableName);
    byte [] familyName = hcd.getName();
    if(!htd.hasFamily(familyName)) {
      throw new InvalidFamilyOperationException("Family '" +
        Bytes.toString(familyName) + "' doesn't exists so cannot be modified");
    }
    htd.addFamily(hcd);
    this.services.getTableDescriptors().add(htd);
    return htd;
  }

  /**
   * Add column to a table
   * @param tableName
   * @param hcd
   * @return Modified HTableDescriptor with new column added.
   * @throws IOException
   */
  public HTableDescriptor addColumn(byte[] tableName, HColumnDescriptor hcd)
      throws IOException {
    LOG.info("AddColumn. Table = " + Bytes.toString(tableName) + " HCD = " +
      hcd.toString());
    HTableDescriptor htd = this.services.getTableDescriptors().get(tableName);
    if (htd == null) {
      throw new InvalidFamilyOperationException("Family '" +
        hcd.getNameAsString() + "' cannot be modified as HTD is null");
    }
    htd.addFamily(hcd);
    this.services.getTableDescriptors().add(htd);
    return htd;
  }
}
