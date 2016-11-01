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
package org.apache.hadoop.hbase.fs.legacy.snapshot;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.fs.legacy.LegacyLayout;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Intelligently keep track of all the files for all the snapshots.
 * <p>
 * A cache of files is kept to avoid querying the {@link FileSystem} frequently. If there is a cache
 * miss the directory modification time is used to ensure that we don't rescan directories that we
 * already have in cache. We only check the modification times of the snapshot directories
 * (/hbase/.snapshot/[snapshot_name]) to determine if the files need to be loaded into the cache.
 * <p>
 * New snapshots will be added to the cache and deleted snapshots will be removed when we refresh
 * the cache. If the files underneath a snapshot directory are changed, but not the snapshot itself,
 * we will ignore updates to that snapshot's files.
 * <p>
 * This is sufficient because each snapshot has its own directory and is added via an atomic rename
 * <i>once</i>, when the snapshot is created. We don't need to worry about the data in the snapshot
 * being run.
 * <p>
 * Further, the cache is periodically refreshed ensure that files in snapshots that were deleted are
 * also removed from the cache.
 * <p>
 * A {@link SnapshotFileCache.SnapshotFileInspector} must be passed when creating <tt>this</tt> to
 * allow extraction of files under /hbase/.snapshot/[snapshot name] directory, for each snapshot.
 * This allows you to only cache files under, for instance, all the logs in the .logs directory or
 * all the files under all the regions.
 * <p>
 * <tt>this</tt> also considers all running snapshots (those under /hbase/.snapshot/.tmp) as valid
 * snapshots and will attempt to cache files from those snapshots as well.
 * <p>
 * Queries about a given file are thread-safe with respect to multiple queries and cache refreshes.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SnapshotFileCache implements Stoppable {
  interface SnapshotFileInspector {
    /**
     * Returns a collection of file names needed by the snapshot.
     * @param snapshotDir {@link Path} to the snapshot directory to scan.
     * @return the collection of file names needed by the snapshot.
     */
    Collection<String> filesUnderSnapshot(final Path snapshotDir) throws IOException;
  }

  private static final Log LOG = LogFactory.getLog(SnapshotFileCache.class);
  private volatile boolean stop = false;
  private final FileSystem fs;
  private final Path rootDir;
  private final SnapshotFileInspector fileInspector;
  private final Path snapshotDir;
  private final Set<String> cache = new HashSet<String>();
  /**
   * This is a helper map of information about the snapshot directories so we don't need to rescan
   * them if they haven't changed since the last time we looked.
   */
  private final Map<String, SnapshotDirectoryInfo> snapshots =
      new HashMap<String, SnapshotDirectoryInfo>();
  private final Timer refreshTimer;

  private long lastModifiedTime = Long.MIN_VALUE;

  /**
   * Create a snapshot file cache for all snapshots under the specified [root]/.snapshot on the
   * filesystem.
   * <p>
   * Immediately loads the file cache.
   * @param conf to extract the configured {@link FileSystem} where the snapshots are stored and
   *          hbase root directory
   * @param cacheRefreshPeriod frequency (ms) with which the cache should be refreshed
   * @param refreshThreadName name of the cache refresh thread
   * @param inspectSnapshotFiles Filter to apply to each snapshot to extract the files.
   * @throws IOException if the {@link FileSystem} or root directory cannot be loaded
   */
  public SnapshotFileCache(Configuration conf, long cacheRefreshPeriod, String refreshThreadName,
      SnapshotFileInspector inspectSnapshotFiles) throws IOException {
    this(FSUtils.getCurrentFileSystem(conf), FSUtils.getRootDir(conf), 0, cacheRefreshPeriod,
        refreshThreadName, inspectSnapshotFiles);
  }

  /**
   * Create a snapshot file cache for all snapshots under the specified [root]/.snapshot on the
   * filesystem
   * @param fs {@link FileSystem} where the snapshots are stored
   * @param rootDir hbase root directory
   * @param cacheRefreshPeriod period (ms) with which the cache should be refreshed
   * @param cacheRefreshDelay amount of time to wait for the cache to be refreshed
   * @param refreshThreadName name of the cache refresh thread
   * @param inspectSnapshotFiles Filter to apply to each snapshot to extract the files.
   */
  public SnapshotFileCache(FileSystem fs, Path rootDir, long cacheRefreshPeriod,
      long cacheRefreshDelay, String refreshThreadName, SnapshotFileInspector inspectSnapshotFiles) {
    this.fs = fs;
    this.rootDir = rootDir;
    this.fileInspector = inspectSnapshotFiles;
    this.snapshotDir = LegacyLayout.getSnapshotDir(rootDir);
    // periodically refresh the file cache to make sure we aren't superfluously saving files.
    this.refreshTimer = new Timer(refreshThreadName, true);
    this.refreshTimer.scheduleAtFixedRate(new RefreshCacheTask(), cacheRefreshDelay,
      cacheRefreshPeriod);
  }

  /**
   * Trigger a cache refresh, even if its before the next cache refresh. Does not affect pending
   * cache refreshes.
   * <p>
   * Blocks until the cache is refreshed.
   * <p>
   * Exposed for TESTING.
   */
  public void triggerCacheRefreshForTesting() {
    try {
      SnapshotFileCache.this.refreshCache();
    } catch (IOException e) {
      LOG.warn("Failed to refresh snapshot hfile cache!", e);
    }
    LOG.debug("Current cache:" + cache);
  }

  /**
   * Check to see if any of the passed file names is contained in any of the snapshots.
   * First checks an in-memory cache of the files to keep. If its not in the cache, then the cache
   * is refreshed and the cache checked again for that file.
   * This ensures that we never return files that exist.
   * <p>
   * Note this may lead to periodic false positives for the file being referenced. Periodically, the
   * cache is refreshed even if there are no requests to ensure that the false negatives get removed
   * eventually. For instance, suppose you have a file in the snapshot and it gets loaded into the
   * cache. Then at some point later that snapshot is deleted. If the cache has not been refreshed
   * at that point, cache will still think the file system contains that file and return
   * <tt>true</tt>, even if it is no longer present (false positive). However, if the file never was
   * on the filesystem, we will never find it and always return <tt>false</tt>.
   * @param files file to check, NOTE: Relies that files are loaded from hdfs before method
   *              is called (NOT LAZY)
   * @return <tt>unReferencedFiles</tt> the collection of files that do not have snapshot references
   * @throws IOException if there is an unexpected error reaching the filesystem.
   */
  // XXX this is inefficient to synchronize on the method, when what we really need to guard against
  // is an illegal access to the cache. Really we could do a mutex-guarded pointer swap on the
  // cache, but that seems overkill at the moment and isn't necessarily a bottleneck.
  public synchronized Iterable<FileStatus> getUnreferencedFiles(Iterable<FileStatus> files,
      final SnapshotManager snapshotManager)
      throws IOException {
    List<FileStatus> unReferencedFiles = Lists.newArrayList();
    List<String> snapshotsInProgress = null;
    boolean refreshed = false;
    for (FileStatus file : files) {
      String fileName = file.getPath().getName();
      if (!refreshed && !cache.contains(fileName)) {
        refreshCache();
        refreshed = true;
      }
      if (cache.contains(fileName)) {
        continue;
      }
      if (snapshotsInProgress == null) {
        snapshotsInProgress = getSnapshotsInProgress(snapshotManager);
      }
      if (snapshotsInProgress.contains(fileName)) {
        continue;
      }
      unReferencedFiles.add(file);
    }
    return unReferencedFiles;
  }

  private synchronized void refreshCache() throws IOException {
    long lastTimestamp = Long.MAX_VALUE;
    boolean hasChanges = false;

    // get the status of the snapshots directory and check if it is has changes
    try {
      FileStatus dirStatus = fs.getFileStatus(snapshotDir);
      lastTimestamp = dirStatus.getModificationTime();
      hasChanges |= (lastTimestamp >= lastModifiedTime);
    } catch (FileNotFoundException e) {
      if (this.cache.size() > 0) {
        LOG.error("Snapshot directory: " + snapshotDir + " doesn't exist");
      }
      return;
    }

    // get the status of the snapshots temporary directory and check if it has changes
    // The top-level directory timestamp is not updated, so we have to check the inner-level.
    try {
      Path snapshotTmpDir = LegacyLayout.getWorkingSnapshotDir(rootDir);
      FileStatus tempDirStatus = fs.getFileStatus(snapshotTmpDir);
      lastTimestamp = Math.min(lastTimestamp, tempDirStatus.getModificationTime());
      hasChanges |= (lastTimestamp >= lastModifiedTime);
      if (!hasChanges) {
        FileStatus[] tmpSnapshots = FSUtils.listStatus(fs, snapshotDir);
        if (tmpSnapshots != null) {
          for (FileStatus dirStatus: tmpSnapshots) {
            lastTimestamp = Math.min(lastTimestamp, dirStatus.getModificationTime());
          }
          hasChanges |= (lastTimestamp >= lastModifiedTime);
        }
      }
    } catch (FileNotFoundException e) {
      // Nothing todo, if the tmp dir is empty
    }

    // if the snapshot directory wasn't modified since we last check, we are done
    if (!hasChanges) {
      return;
    }

    // directory was modified, so we need to reload our cache
    // there could be a slight race here where we miss the cache, check the directory modification
    // time, then someone updates the directory, causing us to not scan the directory again.
    // However, snapshot directories are only created once, so this isn't an issue.

    // 1. update the modified time
    this.lastModifiedTime = lastTimestamp;

    // 2.clear the cache
    this.cache.clear();
    Map<String, SnapshotDirectoryInfo> known = new HashMap<String, SnapshotDirectoryInfo>();

    // 3. check each of the snapshot directories
    FileStatus[] snapshots = FSUtils.listStatus(fs, snapshotDir);
    if (snapshots == null) {
      // remove all the remembered snapshots because we don't have any left
      if (LOG.isDebugEnabled() && this.snapshots.size() > 0) {
        LOG.debug("No snapshots on-disk, cache empty");
      }
      this.snapshots.clear();
      return;
    }

    // 3.1 iterate through the on-disk snapshots
    for (FileStatus snapshot : snapshots) {
      String name = snapshot.getPath().getName();
      // its not the tmp dir,
      if (!name.equals(LegacyLayout.SNAPSHOT_TMP_DIR_NAME)) {
        SnapshotDirectoryInfo files = this.snapshots.remove(name);
        // 3.1.1 if we don't know about the snapshot or its been modified, we need to update the
        // files the latter could occur where I create a snapshot, then delete it, and then make a
        // new snapshot with the same name. We will need to update the cache the information from
        // that new snapshot, even though it has the same name as the files referenced have
        // probably changed.
        if (files == null || files.hasBeenModified(snapshot.getModificationTime())) {
          // get all files for the snapshot and create a new info
          Collection<String> storedFiles = fileInspector.filesUnderSnapshot(snapshot.getPath());
          files = new SnapshotDirectoryInfo(snapshot.getModificationTime(), storedFiles);
        }
        // 3.2 add all the files to cache
        this.cache.addAll(files.getFiles());
        known.put(name, files);
      }
    }

    // 4. set the snapshots we are tracking
    this.snapshots.clear();
    this.snapshots.putAll(known);
  }

  @VisibleForTesting List<String> getSnapshotsInProgress(
    final SnapshotManager snapshotManager) throws IOException {
    List<String> snapshotInProgress = Lists.newArrayList();
    // only add those files to the cache, but not to the known snapshots
    Path snapshotTmpDir = LegacyLayout.getWorkingSnapshotDir(rootDir);
    // only add those files to the cache, but not to the known snapshots
    FileStatus[] running = FSUtils.listStatus(fs, snapshotTmpDir);
    if (running != null) {
      for (FileStatus run : running) {
        ReentrantLock lock = null;
        if (snapshotManager != null) {
          lock = snapshotManager.getLocks().acquireLock(run.getPath().getName());
        }
        try {
          snapshotInProgress.addAll(fileInspector.filesUnderSnapshot(run.getPath()));
        } catch (CorruptedSnapshotException e) {
          // See HBASE-16464
          if (e.getCause() instanceof FileNotFoundException) {
            // If the snapshot is corrupt, we will delete it
            fs.delete(run.getPath(), true);
            LOG.warn("delete the " + run.getPath() + " due to exception:", e.getCause());
          } else {
            throw e;
          }
        } finally {
          if (lock != null) {
            lock.unlock();
          }
        }
      }
    }
    return snapshotInProgress;
  }

  /**
   * Simple helper task that just periodically attempts to refresh the cache
   */
  public class RefreshCacheTask extends TimerTask {
    @Override
    public void run() {
      try {
        SnapshotFileCache.this.refreshCache();
      } catch (IOException e) {
        LOG.warn("Failed to refresh snapshot hfile cache!", e);
      }
    }
  }

  @Override
  public void stop(String why) {
    if (!this.stop) {
      this.stop = true;
      this.refreshTimer.cancel();
    }

  }

  @Override
  public boolean isStopped() {
    return this.stop;
  }

  /**
   * Information about a snapshot directory
   */
  private static class SnapshotDirectoryInfo {
    long lastModified;
    Collection<String> files;

    public SnapshotDirectoryInfo(long mtime, Collection<String> files) {
      this.lastModified = mtime;
      this.files = files;
    }

    /**
     * @return the hfiles in the snapshot when <tt>this</tt> was made.
     */
    public Collection<String> getFiles() {
      return this.files;
    }

    /**
     * Check if the snapshot directory has been modified
     * @param mtime current modification time of the directory
     * @return <tt>true</tt> if it the modification time of the directory is newer time when we
     *         created <tt>this</tt>
     */
    public boolean hasBeenModified(long mtime) {
      return this.lastModified < mtime;
    }
  }
}
