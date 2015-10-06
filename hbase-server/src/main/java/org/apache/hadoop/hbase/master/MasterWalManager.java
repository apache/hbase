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

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WALSplitter;

/**
 * This class abstracts a bunch of operations the HMaster needs
 * when splitting log files e.g. finding log files, dirs etc.
 */
@InterfaceAudience.Private
public class MasterWalManager {
  private static final Log LOG = LogFactory.getLog(MasterWalManager.class);

  final static PathFilter META_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      return AbstractFSWALProvider.isMetaFile(p);
    }
  };

  final static PathFilter NON_META_FILTER = new PathFilter() {
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
  private final SplitLogManager splitLogManager;
  private final boolean distributedLogReplay;

  // Is the fileystem ok?
  private volatile boolean fsOk = true;

  public MasterWalManager(MasterServices services) throws IOException {
    this(services.getConfiguration(), services.getMasterFileSystem().getFileSystem(),
      services.getMasterFileSystem().getRootDir(), services);
  }

  public MasterWalManager(Configuration conf, FileSystem fs, Path rootDir, MasterServices services)
      throws IOException {
    this.fs = fs;
    this.conf = conf;
    this.rootDir = rootDir;
    this.services = services;
    this.splitLogManager = new SplitLogManager(services, conf);
    this.distributedLogReplay = this.splitLogManager.isLogReplaying();

    this.oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
  }

  public void stop() {
    if (splitLogManager != null) {
      splitLogManager.stop();
    }
  }

  @VisibleForTesting
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
   * Inspect the log directory to find dead servers which need recovery work
   * @return A set of ServerNames which aren't running but still have WAL files left in file system
   */
  Set<ServerName> getFailedServersFromLogFolders() {
    boolean retrySplitting = !conf.getBoolean("hbase.hlog.split.skip.errors",
        WALSplitter.SPLIT_SKIP_ERRORS_DEFAULT);

    Set<ServerName> serverNames = new HashSet<ServerName>();
    Path logsDirPath = new Path(this.rootDir, HConstants.HREGION_LOGDIR_NAME);

    do {
      if (services.isStopped()) {
        LOG.warn("Master stopped while trying to get failed servers.");
        break;
      }
      try {
        if (!this.fs.exists(logsDirPath)) return serverNames;
        FileStatus[] logFolders = FSUtils.listStatus(this.fs, logsDirPath, null);
        // Get online servers after getting log folders to avoid log folder deletion of newly
        // checked in region servers . see HBASE-5916
        Set<ServerName> onlineServers = services.getServerManager().getOnlineServers().keySet();

        if (logFolders == null || logFolders.length == 0) {
          LOG.debug("No log files to split, proceeding...");
          return serverNames;
        }
        for (FileStatus status : logFolders) {
          FileStatus[] curLogFiles = FSUtils.listStatus(this.fs, status.getPath(), null);
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
    Set<ServerName> serverNames = new HashSet<ServerName>();
    serverNames.add(serverName);
    splitLog(serverNames);
  }

  /**
   * Specialized method to handle the splitting for meta WAL
   * @param serverName logs belonging to this server will be split
   */
  public void splitMetaLog(final ServerName serverName) throws IOException {
    Set<ServerName> serverNames = new HashSet<ServerName>();
    serverNames.add(serverName);
    splitMetaLog(serverNames);
  }

  /**
   * Specialized method to handle the splitting for meta WAL
   * @param serverNames logs belonging to these servers will be split
   */
  public void splitMetaLog(final Set<ServerName> serverNames) throws IOException {
    splitLog(serverNames, META_FILTER);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UL_UNRELEASED_LOCK", justification=
      "We only release this lock when we set it. Updates to code that uses it should verify use " +
      "of the guard boolean.")
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
    } finally {
      if (needReleaseLock) {
        this.splitLogLock.unlock();
      }
    }
    return logDirs;
  }

  /**
   * Mark regions in recovering state when distributedLogReplay are set true
   * @param serverName Failed region server whose wals to be replayed
   * @param regions Set of regions to be recovered
   */
  public void prepareLogReplay(ServerName serverName, Set<HRegionInfo> regions) throws IOException {
    if (!this.distributedLogReplay) {
      return;
    }
    // mark regions in recovering state
    if (regions == null || regions.isEmpty()) {
      return;
    }
    this.splitLogManager.markRegionsRecovering(serverName, regions);
  }

  public void splitLog(final Set<ServerName> serverNames) throws IOException {
    splitLog(serverNames, NON_META_FILTER);
  }

  /**
   * Wrapper function on {@link SplitLogManager#removeStaleRecoveringRegions(Set)}
   * @param failedServers A set of known failed servers
   */
  void removeStaleRecoveringRegionsFromZK(final Set<ServerName> failedServers)
      throws IOException, InterruptedIOException {
    this.splitLogManager.removeStaleRecoveringRegions(failedServers);
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
   * The function is used in SSH to set recovery mode based on configuration after all outstanding
   * log split tasks drained.
   */
  public void setLogRecoveryMode() throws IOException {
    this.splitLogManager.setRecoveryMode(false);
  }

  public RecoveryMode getLogRecoveryMode() {
    return this.splitLogManager.getRecoveryMode();
  }
}
