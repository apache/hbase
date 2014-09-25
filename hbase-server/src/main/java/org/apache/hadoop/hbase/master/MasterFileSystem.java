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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;

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
  private final MetricsMasterFileSystem metricsMasterFilesystem = new MetricsMasterFileSystem();
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
  // hbase temp directory used for table construction and deletion
  private final Path tempdir;
  // create the split log lock
  final Lock splitLogLock = new ReentrantLock();
  final boolean distributedLogReplay;
  final SplitLogManager splitLogManager;
  private final MasterServices services;

  final static PathFilter META_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      return HLogUtil.isMetaFile(p);
    }
  };

  final static PathFilter NON_META_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      return !HLogUtil.isMetaFile(p);
    }
  };

  public MasterFileSystem(Server master, MasterServices services, boolean masterRecovery)
  throws IOException {
    this.conf = master.getConfiguration();
    this.master = master;
    this.services = services;
    // Set filesystem to be that of this.rootdir else we get complaints about
    // mismatched filesystems if hbase.rootdir is hdfs and fs.defaultFS is
    // default localfs.  Presumption is that rootdir is fully-qualified before
    // we get to here with appropriate fs scheme.
    this.rootdir = FSUtils.getRootDir(conf);
    this.tempdir = new Path(this.rootdir, HConstants.HBASE_TEMP_DIRECTORY);
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    this.fs = this.rootdir.getFileSystem(conf);
    FSUtils.setFsDefault(conf, new Path(this.fs.getUri()));
    // make sure the fs has the same conf
    fs.setConf(conf);
    // setup the filesystem variable
    // set up the archived logs path
    this.oldLogDir = createInitialFileSystemLayout();
    HFileSystem.addLocationsOrderInterceptor(conf);
    try {
      this.splitLogManager = new SplitLogManager(master.getZooKeeper(), master.getConfiguration(),
 master, services,
              master.getServerName(), masterRecovery);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    this.distributedLogReplay = (this.splitLogManager.getRecoveryMode() == RecoveryMode.LOG_REPLAY);
  }

  /**
   * Create initial layout in filesystem.
   * <ol>
   * <li>Check if the meta region exists and is readable, if not create it.
   * Create hbase.version and the hbase:meta directory if not one.
   * </li>
   * <li>Create a log archive directory for RS to put archived logs</li>
   * </ol>
   * Idempotent.
   */
  private Path createInitialFileSystemLayout() throws IOException {
    // check if the root directory exists
    checkRootDir(this.rootdir, conf, this.fs);

    // check if temp directory exists and clean it
    checkTempDir(this.tempdir, conf, this.fs);

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
   * @return HBase temp dir.
   */
  public Path getTempDir() {
    return this.tempdir;
  }

  /**
   * @return The unique identifier generated for this cluster
   */
  public ClusterId getClusterId() {
    return clusterId;
  }

  /**
   * Inspect the log directory to find dead servers which need recovery work
   * @return A set of ServerNames which aren't running but still have WAL files left in file system
   */
  Set<ServerName> getFailedServersFromLogFolders() {
    boolean retrySplitting = !conf.getBoolean("hbase.hlog.split.skip.errors",
      HLog.SPLIT_SKIP_ERRORS_DEFAULT);

    Set<ServerName> serverNames = new HashSet<ServerName>();
    Path logsDirPath = new Path(this.rootdir, HConstants.HREGION_LOGDIR_NAME);

    do {
      if (master.isStopped()) {
        LOG.warn("Master stopped while trying to get failed servers.");
        break;
      }
      try {
        if (!this.fs.exists(logsDirPath)) return serverNames;
        FileStatus[] logFolders = FSUtils.listStatus(this.fs, logsDirPath, null);
        // Get online servers after getting log folders to avoid log folder deletion of newly
        // checked in region servers . see HBASE-5916
        Set<ServerName> onlineServers = ((HMaster) master).getServerManager().getOnlineServers()
            .keySet();

        if (logFolders == null || logFolders.length == 0) {
          LOG.debug("No log files to split, proceeding...");
          return serverNames;
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
            LOG.info("Log folder " + status.getPath() + " belongs to an existing region server");
          }
        }
        retrySplitting = false;
      } catch (IOException ioe) {
        LOG.warn("Failed getting failed servers to be recovered.", ioe);
        if (!checkFileSystem()) {
          LOG.warn("Bad Filesystem, exiting");
          Runtime.getRuntime().halt(1);
        }
        try {
          if (retrySplitting) {
            Thread.sleep(conf.getInt("hbase.hlog.split.failure.retry.interval", 30 * 1000));
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted, aborting since cannot return w/o splitting");
          Thread.currentThread().interrupt();
          retrySplitting = false;
          Runtime.getRuntime().halt(1);
        }
      }
    } while (retrySplitting);

    return serverNames;
  }

  public void splitLog(final ServerName serverName) throws IOException {
    Set<ServerName> serverNames = new HashSet<ServerName>();
    serverNames.add(serverName);
    splitLog(serverNames);
  }

  /**
   * Specialized method to handle the splitting for meta HLog
   * @param serverName
   * @throws IOException
   */
  public void splitMetaLog(final ServerName serverName) throws IOException {
    Set<ServerName> serverNames = new HashSet<ServerName>();
    serverNames.add(serverName);
    splitMetaLog(serverNames);
  }

  /**
   * Specialized method to handle the splitting for meta HLog
   * @param serverNames
   * @throws IOException
   */
  public void splitMetaLog(final Set<ServerName> serverNames) throws IOException {
    splitLog(serverNames, META_FILTER);
  }

  private List<Path> getLogDirs(final Set<ServerName> serverNames) throws IOException {
    List<Path> logDirs = new ArrayList<Path>();
    boolean needReleaseLock = false;
    if (!this.services.isInitialized()) {
      // during master initialization, we could have multiple places splitting a same wal
      this.splitLogLock.lock();
      needReleaseLock = true;
    }
    try {
      for (ServerName serverName : serverNames) {
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
    } finally {
      if (needReleaseLock) {
        this.splitLogLock.unlock();
      }
    }
    return logDirs;
  }

  /**
   * Mark regions in recovering state when distributedLogReplay are set true
   * @param serverNames Set of ServerNames to be replayed wals in order to recover changes contained
   *          in them
   * @throws IOException
   */
  public void prepareLogReplay(Set<ServerName> serverNames) throws IOException {
    if (!this.distributedLogReplay) {
      return;
    }
    // mark regions in recovering state
    for (ServerName serverName : serverNames) {
      NavigableMap<HRegionInfo, Result> regions = this.getServerUserRegions(serverName);
      if (regions == null) {
        continue;
      }
      try {
        this.splitLogManager.markRegionsRecoveringInZK(serverName, regions.keySet());
      } catch (KeeperException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Mark regions in recovering state when distributedLogReplay are set true
   * @param serverName Failed region server whose wals to be replayed
   * @param regions Set of regions to be recovered
   * @throws IOException
   */
  public void prepareLogReplay(ServerName serverName, Set<HRegionInfo> regions) throws IOException {
    if (!this.distributedLogReplay) {
      return;
    }
    // mark regions in recovering state
    if (regions == null || regions.isEmpty()) {
      return;
    }
    try {
      this.splitLogManager.markRegionsRecoveringInZK(serverName, regions);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public void splitLog(final Set<ServerName> serverNames) throws IOException {
    splitLog(serverNames, NON_META_FILTER);
  }

  /**
   * Wrapper function on {@link SplitLogManager#removeStaleRecoveringRegionsFromZK(Set)}
   * @param failedServers
   * @throws KeeperException
   */
  void removeStaleRecoveringRegionsFromZK(final Set<ServerName> failedServers)
      throws KeeperException {
    this.splitLogManager.removeStaleRecoveringRegionsFromZK(failedServers);
  }

  /**
   * This method is the base split method that splits HLog files matching a filter. Callers should
   * pass the appropriate filter for meta and non-meta HLogs.
   * @param serverNames
   * @param filter
   * @throws IOException
   */
  public void splitLog(final Set<ServerName> serverNames, PathFilter filter) throws IOException {
    long splitTime = 0, splitLogSize = 0;
    List<Path> logDirs = getLogDirs(serverNames);

    splitLogManager.handleDeadWorkers(serverNames);
    splitTime = EnvironmentEdgeManager.currentTimeMillis();
    splitLogSize = splitLogManager.splitLogDistributed(serverNames, logDirs, filter);
    splitTime = EnvironmentEdgeManager.currentTimeMillis() - splitTime;

    if (this.metricsMasterFilesystem != null) {
      if (filter == META_FILTER) {
        this.metricsMasterFilesystem.addMetaWALSplit(splitTime, splitLogSize);
      } else {
        this.metricsMasterFilesystem.addSplit(splitTime, splitLogSize);
      }
    }
  }

  /**
   * Get the rootdir.  Make sure its wholesome and exists before returning.
   * @param rd
   * @param c
   * @param fs
   * @return hbase.rootdir (after checks for existence and bootstrapping if
   * needed populating the directory with necessary bootup files).
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
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

    // Make sure the meta region directory exists!
    if (!FSUtils.metaRegionExists(fs, rd)) {
      bootstrap(rd, c);
    } else {
      // Migrate table descriptor files if necessary
      org.apache.hadoop.hbase.util.FSTableDescriptorMigrationToSubdir
        .migrateFSTableDescriptorsIfNecessary(fs, rd);
    }

    // Create tableinfo-s for hbase:meta if not already there.
    new FSTableDescriptors(fs, rd).createTableDescriptor(HTableDescriptor.META_TABLEDESC);

    return rd;
  }

  /**
   * Make sure the hbase temp directory exists and is empty.
   * NOTE that this method is only executed once just after the master becomes the active one.
   */
  private void checkTempDir(final Path tmpdir, final Configuration c, final FileSystem fs)
      throws IOException {
    // If the temp directory exists, clear the content (left over, from the previous run)
    if (fs.exists(tmpdir)) {
      // Archive table in temp, maybe left over from failed deletion,
      // if not the cleaner will take care of them.
      for (Path tabledir: FSUtils.getTableDirs(fs, tmpdir)) {
        for (Path regiondir: FSUtils.getRegionDirs(fs, tabledir)) {
          HFileArchiver.archiveRegion(fs, this.rootdir, tabledir, regiondir);
        }
      }
      if (!fs.delete(tmpdir, true)) {
        throw new IOException("Unable to clean the temp directory: " + tmpdir);
      }
    }

    // Create the temp directory
    if (!fs.mkdirs(tmpdir)) {
      throw new IOException("HBase temp directory '" + tmpdir + "' creation failure.");
    }
  }

  private static void bootstrap(final Path rd, final Configuration c)
  throws IOException {
    LOG.info("BOOTSTRAP: creating hbase:meta region");
    try {
      // Bootstrapping, make sure blockcache is off.  Else, one will be
      // created here in bootstrap and it'll need to be cleaned up.  Better to
      // not make it in first place.  Turn off block caching for bootstrap.
      // Enable after.
      HRegionInfo metaHRI = new HRegionInfo(HRegionInfo.FIRST_META_REGIONINFO);
      setInfoFamilyCachingForMeta(false);
      HRegion meta = HRegion.createHRegion(metaHRI, rd, c,
          HTableDescriptor.META_TABLEDESC, null, true, true);
      setInfoFamilyCachingForMeta(true);
      HRegion.closeHRegion(meta);
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.error("bootstrap", e);
      throw e;
    }
  }

  /**
   * Enable in memory caching for hbase:meta
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
    HFileArchiver.archiveRegion(conf, fs, region);
  }

  public void deleteTable(TableName tableName) throws IOException {
    fs.delete(FSUtils.getTableDir(rootdir, tableName), true);
  }

  /**
   * Move the specified table to the hbase temp directory
   * @param tableName Table name to move
   * @return The temp location of the table moved
   * @throws IOException in case of file-system failure
   */
  public Path moveTableToTemp(TableName tableName) throws IOException {
    Path srcPath = FSUtils.getTableDir(rootdir, tableName);
    Path tempPath = FSUtils.getTableDir(this.tempdir, tableName);

    // Ensure temp exists
    if (!fs.exists(tempPath.getParent()) && !fs.mkdirs(tempPath.getParent())) {
      throw new IOException("HBase temp directory '" + tempPath.getParent() + "' creation failure.");
    }

    if (!fs.rename(srcPath, tempPath)) {
      throw new IOException("Unable to move '" + srcPath + "' to temp '" + tempPath + "'");
    }

    return tempPath;
  }

  public void updateRegionInfo(HRegionInfo region) {
    // TODO implement this.  i think this is currently broken in trunk i don't
    //      see this getting updated.
    //      @see HRegion.checkRegioninfoOnFilesystem()
  }

  public void deleteFamilyFromFS(HRegionInfo region, byte[] familyName)
      throws IOException {
    // archive family store files
    Path tableDir = FSUtils.getTableDir(rootdir, region.getTable());
    HFileArchiver.archiveFamily(fs, conf, region, tableDir, familyName);

    // delete the family folder
    Path familyDir = new Path(tableDir,
      new Path(region.getEncodedName(), Bytes.toString(familyName)));
    if (fs.delete(familyDir, true) == false) {
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
   * Delete column of a table
   * @param tableName
   * @param familyName
   * @return Modified HTableDescriptor with requested column deleted.
   * @throws IOException
   */
  public HTableDescriptor deleteColumn(TableName tableName, byte[] familyName)
      throws IOException {
    LOG.info("DeleteColumn. Table = " + tableName
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
  public HTableDescriptor modifyColumn(TableName tableName, HColumnDescriptor hcd)
      throws IOException {
    LOG.info("AddModifyColumn. Table = " + tableName
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
  public HTableDescriptor addColumn(TableName tableName, HColumnDescriptor hcd)
      throws IOException {
    LOG.info("AddColumn. Table = " + tableName + " HCD = " +
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

  private NavigableMap<HRegionInfo, Result> getServerUserRegions(ServerName serverName)
      throws IOException {
    if (!this.master.isStopped()) {
      try {
        this.master.getCatalogTracker().waitForMeta();
        return MetaReader.getServerUserRegions(this.master.getCatalogTracker(), serverName);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
    }
    return null;
  }

  /**
   * The function is used in SSH to set recovery mode based on configuration after all outstanding
   * log split tasks drained.
   * @throws KeeperException
   * @throws InterruptedIOException
   */
  public void setLogRecoveryMode() throws IOException {
    try {
      this.splitLogManager.setRecoveryMode(false);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public RecoveryMode getLogRecoveryMode() {
    return this.splitLogManager.getRecoveryMode();
  }
}
