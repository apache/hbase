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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class abstracts a bunch of operations the HMaster needs
 * when splitting log files e.g. finding log files, dirs etc.
 */
@InterfaceAudience.Private
public class MasterWalManager {
  private static final Logger LOG = LoggerFactory.getLogger(MasterWalManager.class);

  /**
   * Filter *in* WAL files that are for the hbase:meta Region.
   */
  final static PathFilter META_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      return AbstractFSWALProvider.isMetaFile(p);
    }
  };

  /**
   * Filter *out* WAL files that are for the hbase:meta Region; i.e. return user-space WALs only.
   */
  public final static PathFilter NON_META_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      return !AbstractFSWALProvider.isMetaFile(p);
    }
  };

  // metrics for master
  // TODO: Rename it, since those metrics are split-manager related
  private final MetricsMasterFileSystem metricsMasterFilesystem = new MetricsMasterFileSystem();

  // Keep around for convenience.
  private final MasterServices services;
  private final Configuration conf;
  private final FileSystem fs;

  // The Path to the old logs dir
  private final Path oldLogDir;

  private final Path rootDir;

  // create the split log lock
  private final Lock splitLogLock = new ReentrantLock();

  /**
   * Superceded by {@link SplitWALManager}; i.e. procedure-based WAL splitting rather than
   *   'classic' zk-coordinated WAL splitting.
   * @deprecated  since 2.3.0 and 3.0.0 to be removed in 4.0.0; replaced by {@link SplitWALManager}.
   * @see SplitWALManager
   */
  @Deprecated
  private final SplitLogManager splitLogManager;

  // Is the fileystem ok?
  private volatile boolean fsOk = true;

  public MasterWalManager(MasterServices services) throws IOException {
    this(services.getConfiguration(), services.getMasterFileSystem().getWALFileSystem(),
      services.getMasterFileSystem().getWALRootDir(), services);
  }

  public MasterWalManager(Configuration conf, FileSystem fs, Path rootDir, MasterServices services)
      throws IOException {
    this.fs = fs;
    this.conf = conf;
    this.rootDir = rootDir;
    this.services = services;
    this.splitLogManager = new SplitLogManager(services, conf);
    this.oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
  }

  public void stop() {
    if (splitLogManager != null) {
      splitLogManager.stop();
    }
  }

  SplitLogManager getSplitLogManager() {
    return this.splitLogManager;
  }

  /**
   * Get the directory where old logs go
   * @return the dir
   */
  Path getOldLogDir() {
    return this.oldLogDir;
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  /**
   * Checks to see if the file system is still accessible.
   * If not, sets closed
   * @return false if file system is not available
   */
  private boolean checkFileSystem() {
    if (this.fsOk) {
      try {
        FSUtils.checkFileSystemAvailable(this.fs);
        FSUtils.checkDfsSafeMode(this.conf);
      } catch (IOException e) {
        services.abort("Shutting down HBase cluster: file system not available", e);
        this.fsOk = false;
      }
    }
    return this.fsOk;
  }

  /**
   * Get Servernames which are currently splitting; paths have a '-splitting' suffix.
   */
  public Set<ServerName> getSplittingServersFromWALDir() throws  IOException {
    return getServerNamesFromWALDirPath(
      p -> p.getName().endsWith(AbstractFSWALProvider.SPLITTING_EXT));
  }

  /**
   * Get Servernames that COULD BE 'alive'; excludes those that have a '-splitting' suffix as these
   * are already being split -- they cannot be 'alive'.
   */
  public Set<ServerName> getLiveServersFromWALDir() throws IOException {
    return getServerNamesFromWALDirPath(
      p -> !p.getName().endsWith(AbstractFSWALProvider.SPLITTING_EXT));
  }

  /**
   * @return listing of ServerNames found by parsing WAL directory paths in FS.
   */
  public Set<ServerName> getServerNamesFromWALDirPath(final PathFilter filter) throws IOException {
    FileStatus[] walDirForServerNames = getWALDirPaths(filter);
    return Stream.of(walDirForServerNames).map(s -> {
      ServerName serverName = AbstractFSWALProvider.getServerNameFromWALDirectoryName(s.getPath());
      if (serverName == null) {
        LOG.warn("Log folder {} doesn't look like its name includes a " +
          "region server name; leaving in place. If you see later errors about missing " +
          "write ahead logs they may be saved in this location.", s.getPath());
        return null;
      }
      return serverName;
    }).filter(s -> s != null).collect(Collectors.toSet());
  }

  /**
   * @return List of all RegionServer WAL dirs; i.e. this.rootDir/HConstants.HREGION_LOGDIR_NAME.
   */
  public FileStatus[] getWALDirPaths(final PathFilter filter) throws IOException {
    Path walDirPath = new Path(CommonFSUtils.getWALRootDir(conf), HConstants.HREGION_LOGDIR_NAME);
    FileStatus[] walDirForServerNames = CommonFSUtils.listStatus(fs, walDirPath, filter);
    return walDirForServerNames == null? new FileStatus[0]: walDirForServerNames;
  }

  /**
   * Inspect the log directory to find dead servers which need recovery work
   * @return A set of ServerNames which aren't running but still have WAL files left in file system
   * @deprecated With proc-v2, we can record the crash server with procedure store, so do not need
   *             to scan the wal directory to find out the splitting wal directory any more. Leave
   *             it here only because {@code RecoverMetaProcedure}(which is also deprecated) uses
   *             it.
   */
  @Deprecated
  public Set<ServerName> getFailedServersFromLogFolders() throws IOException {
    boolean retrySplitting = !conf.getBoolean(WALSplitter.SPLIT_SKIP_ERRORS_KEY,
        WALSplitter.SPLIT_SKIP_ERRORS_DEFAULT);

    Set<ServerName> serverNames = new HashSet<>();
    Path logsDirPath = new Path(CommonFSUtils.getWALRootDir(conf), HConstants.HREGION_LOGDIR_NAME);

    do {
      if (services.isStopped()) {
        LOG.warn("Master stopped while trying to get failed servers.");
        break;
      }
      try {
        if (!this.fs.exists(logsDirPath)) return serverNames;
        FileStatus[] logFolders = CommonFSUtils.listStatus(this.fs, logsDirPath, null);
        // Get online servers after getting log folders to avoid log folder deletion of newly
        // checked in region servers . see HBASE-5916
        Set<ServerName> onlineServers = services.getServerManager().getOnlineServers().keySet();

        if (logFolders == null || logFolders.length == 0) {
          LOG.debug("No log files to split, proceeding...");
          return serverNames;
        }
        for (FileStatus status : logFolders) {
          FileStatus[] curLogFiles = CommonFSUtils.listStatus(this.fs, status.getPath(), null);
          if (curLogFiles == null || curLogFiles.length == 0) {
            // Empty log folder. No recovery needed
            continue;
          }
          final ServerName serverName = AbstractFSWALProvider.getServerNameFromWALDirectoryName(
              status.getPath());
          if (null == serverName) {
            LOG.warn("Log folder " + status.getPath() + " doesn't look like its name includes a " +
                "region server name; leaving in place. If you see later errors about missing " +
                "write ahead logs they may be saved in this location.");
          } else if (!onlineServers.contains(serverName)) {
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
    splitLog(Collections.<ServerName>singleton(serverName));
  }

  /**
   * Specialized method to handle the splitting for meta WAL
   * @param serverName logs belonging to this server will be split
   */
  public void splitMetaLog(final ServerName serverName) throws IOException {
    splitMetaLog(Collections.<ServerName>singleton(serverName));
  }

  /**
   * Specialized method to handle the splitting for meta WAL
   * @param serverNames logs belonging to these servers will be split
   */
  public void splitMetaLog(final Set<ServerName> serverNames) throws IOException {
    splitLog(serverNames, META_FILTER);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UL_UNRELEASED_LOCK",
      justification = "We only release this lock when we set it. Updates to code "
        + "that uses it should verify use of the guard boolean.")
  List<Path> getLogDirs(final Set<ServerName> serverNames) throws IOException {
    List<Path> logDirs = new ArrayList<>();
    boolean needReleaseLock = false;
    if (!this.services.isInitialized()) {
      // during master initialization, we could have multiple places splitting a same wal
      // XXX: Does this still exist after we move to proc-v2?
      this.splitLogLock.lock();
      needReleaseLock = true;
    }
    try {
      for (ServerName serverName : serverNames) {
        Path logDir = new Path(this.rootDir,
          AbstractFSWALProvider.getWALDirectoryName(serverName.toString()));
        Path splitDir = logDir.suffix(AbstractFSWALProvider.SPLITTING_EXT);
        // Rename the directory so a rogue RS doesn't create more WALs
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
    } catch (IOException ioe) {
      if (!checkFileSystem()) {
        this.services.abort("Aborting due to filesystem unavailable", ioe);
        throw ioe;
      }
    } finally {
      if (needReleaseLock) {
        this.splitLogLock.unlock();
      }
    }
    return logDirs;
  }

  public void splitLog(final Set<ServerName> serverNames) throws IOException {
    splitLog(serverNames, NON_META_FILTER);
  }

  /**
   * This method is the base split method that splits WAL files matching a filter. Callers should
   * pass the appropriate filter for meta and non-meta WALs.
   * @param serverNames logs belonging to these servers will be split; this will rename the log
   *                    directory out from under a soft-failed server
   */
  public void splitLog(final Set<ServerName> serverNames, PathFilter filter) throws IOException {
    long splitTime = 0, splitLogSize = 0;
    List<Path> logDirs = getLogDirs(serverNames);

    splitLogManager.handleDeadWorkers(serverNames);
    splitTime = EnvironmentEdgeManager.currentTime();
    splitLogSize = splitLogManager.splitLogDistributed(serverNames, logDirs, filter);
    splitTime = EnvironmentEdgeManager.currentTime() - splitTime;

    if (this.metricsMasterFilesystem != null) {
      if (filter == META_FILTER) {
        this.metricsMasterFilesystem.addMetaWALSplit(splitTime, splitLogSize);
      } else {
        this.metricsMasterFilesystem.addSplit(splitTime, splitLogSize);
      }
    }
  }

  /**
   * The hbase:meta region may OPEN and CLOSE without issue on a server and then move elsewhere.
   * On CLOSE, the WAL for the hbase:meta table may not be archived yet (The WAL is only needed if
   * hbase:meta did not close cleanaly). Since meta region is no long on this server,
   * the ServerCrashProcedure won't split these leftover hbase:meta WALs, just leaving them in
   * the WAL splitting dir. If we try to delete the WAL splitting for the server,  it fail since
   * the dir is not totally empty. We can safely archive these hbase:meta log; then the
   * WAL dir can be deleted.
   * @param serverName the server to archive meta log
   */
  public void archiveMetaLog(final ServerName serverName) {
    try {
      Path logDir = new Path(this.rootDir,
          AbstractFSWALProvider.getWALDirectoryName(serverName.toString()));
      Path splitDir = logDir.suffix(AbstractFSWALProvider.SPLITTING_EXT);
      if (fs.exists(splitDir)) {
        FileStatus[] logfiles = CommonFSUtils.listStatus(fs, splitDir, META_FILTER);
        if (logfiles != null) {
          for (FileStatus status : logfiles) {
            if (!status.isDir()) {
              Path newPath = AbstractFSWAL.getWALArchivePath(this.oldLogDir,
                  status.getPath());
              if (!CommonFSUtils.renameAndSetModifyTime(fs, status.getPath(), newPath)) {
                LOG.warn("Unable to move  " + status.getPath() + " to " + newPath);
              } else {
                LOG.debug("Archived meta log " + status.getPath() + " to " + newPath);
              }
            }
          }
        }
        if (!fs.delete(splitDir, false)) {
          LOG.warn("Unable to delete log dir. Ignoring. " + splitDir);
        }
      }
    } catch (IOException ie) {
      LOG.warn("Failed archiving meta log for server " + serverName, ie);
    }
  }
}
