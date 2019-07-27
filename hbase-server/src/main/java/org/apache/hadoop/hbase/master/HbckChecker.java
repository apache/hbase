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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HbckRegionInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Used to do the hbck checking job at master side.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HbckChecker extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(HbckChecker.class.getName());

  private static final String HBCK_CHECKER_INTERVAL = "hbase.master.hbck.checker.interval";
  private static final int DEFAULT_HBCK_CHECKER_INTERVAL = 60 * 60 * 1000;

  private final MasterServices master;

  /**
   * This map contains the state of all hbck items.  It maps from encoded region
   * name to HbckRegionInfo structure.  The information contained in HbckRegionInfo is used
   * to detect and correct consistency (hdfs/meta/deployment) problems.
   */
  private final Map<String, HbckRegionInfo> regionInfoMap = new HashMap<>();

  /**
   * The regions only opened on RegionServers, but no region info in meta.
   */
  private final Map<String, ServerName> orphanRegionsOnRS = new HashMap<>();
  /**
   * The regions have directory on FileSystem, but no region info in meta.
   */
  private final List<String> orphanRegionsOnFS = new LinkedList<>();
  /**
   * The inconsistent regions. There are three case:
   * case 1. Master thought this region opened, but no regionserver reported it.
   * case 2. Master thought this region opened on Server1, but regionserver reported Server2
   * case 3. More than one regionservers reported opened this region
   */
  private final Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions =
      new HashMap<>();

  /**
   * The "snapshot" is used to save the last round's HBCK checking report.
   */
  private final Map<String, ServerName> orphanRegionsOnRSSnapshot = new HashMap<>();
  private final List<String> orphanRegionsOnFSSnapshot = new LinkedList<>();
  private final Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegionsSnapshot =
      new HashMap<>();

  /**
   * The "snapshot" may be changed after checking. And this checking report "snapshot" may be
   * accessed by web ui. Use this rwLock to synchronize.
   */
  ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  /**
   * When running, the "snapshot" may be changed when this round's checking finish.
   */
  private volatile boolean running = false;
  private volatile long checkingStartTimestamp = 0;
  private volatile long checkingEndTimestamp = 0;

  public HbckChecker(MasterServices master) {
    super("HbckChecker-", master,
        master.getConfiguration().getInt(HBCK_CHECKER_INTERVAL, DEFAULT_HBCK_CHECKER_INTERVAL));
    this.master = master;
  }

  @Override
  protected void chore() {
    running = true;
    regionInfoMap.clear();
    orphanRegionsOnRS.clear();
    orphanRegionsOnFS.clear();
    inconsistentRegions.clear();
    checkingStartTimestamp = EnvironmentEdgeManager.currentTime();
    loadRegionsFromInMemoryState();
    loadRegionsFromRSReport();
    try {
      loadRegionsFromFS();
    } catch (IOException e) {
      LOG.warn("Failed to load the regions from filesystem", e);
    }
    saveCheckResultToSnapshot();
    running = false;
  }

  private void saveCheckResultToSnapshot() {
    // Need synchronized here, as this "snapshot" may be access by web ui.
    rwLock.writeLock().lock();
    try {
      orphanRegionsOnRSSnapshot.clear();
      orphanRegionsOnRS.entrySet()
          .forEach(e -> orphanRegionsOnRSSnapshot.put(e.getKey(), e.getValue()));
      orphanRegionsOnFSSnapshot.clear();
      orphanRegionsOnFSSnapshot.addAll(orphanRegionsOnFS);
      inconsistentRegionsSnapshot.clear();
      inconsistentRegions.entrySet()
          .forEach(e -> inconsistentRegionsSnapshot.put(e.getKey(), e.getValue()));
      checkingEndTimestamp = EnvironmentEdgeManager.currentTime();
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private void loadRegionsFromInMemoryState() {
    List<RegionState> regionStates =
        master.getAssignmentManager().getRegionStates().getRegionStates();
    for (RegionState regionState : regionStates) {
      RegionInfo regionInfo = regionState.getRegion();
      HbckRegionInfo.MetaEntry metaEntry =
          new HbckRegionInfo.MetaEntry(regionInfo, regionState.getServerName(),
              regionState.getStamp());
      regionInfoMap.put(regionInfo.getEncodedName(), new HbckRegionInfo(metaEntry));
    }
  }

  private void loadRegionsFromRSReport() {
    Map<ServerName, Set<byte[]>> rsReports = master.getAssignmentManager().getRSReports();
    for (Map.Entry<ServerName, Set<byte[]>> entry : rsReports.entrySet()) {
      ServerName serverName = entry.getKey();
      for (byte[] regionName : entry.getValue()) {
        String encodedRegionName = RegionInfo.encodeRegionName(regionName);
        HbckRegionInfo hri = regionInfoMap.get(encodedRegionName);
        if (hri == null) {
          orphanRegionsOnRS.put(encodedRegionName, serverName);
          continue;
        }
        hri.addServer(hri.getMetaEntry(), serverName);
      }
    }

    for (Map.Entry<String, HbckRegionInfo> entry : regionInfoMap.entrySet()) {
      String encodedRegionName = entry.getKey();
      HbckRegionInfo hri = entry.getValue();
      ServerName locationInMeta = hri.getMetaEntry().getRegionServer();
      if (hri.getDeployedOn().size() == 0) {
        // Master thought this region opened, but no regionserver reported it.
        inconsistentRegions.put(encodedRegionName, new Pair<>(locationInMeta, new LinkedList<>()));
      } else if (hri.getDeployedOn().size() > 1) {
        // More than one regionserver reported opened this region
        inconsistentRegions.put(encodedRegionName, new Pair<>(locationInMeta, hri.getDeployedOn()));
      } else if (!hri.getDeployedOn().get(0).equals(locationInMeta)) {
        // Master thought this region opened on Server1, but regionserver reported Server2
        inconsistentRegions.put(encodedRegionName, new Pair<>(locationInMeta, hri.getDeployedOn()));
      }
    }
  }

  private void loadRegionsFromFS() throws IOException {
    Path rootDir = master.getMasterFileSystem().getRootDir();
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    // list all tables from HDFS
    List<FileStatus> tableDirs = Lists.newArrayList();
    List<Path> paths = FSUtils.getTableDirs(fs, rootDir);
    for (Path path : paths) {
      tableDirs.add(fs.getFileStatus(path));
    }

    for (FileStatus tableDir : tableDirs) {
      FileStatus[] regionDirs = fs.listStatus(tableDir.getPath());
      for (FileStatus regionDir : regionDirs) {
        String encodedRegionName = regionDir.getPath().getName();
        HbckRegionInfo hri = regionInfoMap.get(encodedRegionName);
        if (hri == null) {
          orphanRegionsOnFS.add(encodedRegionName);
          continue;
        }
        HbckRegionInfo.HdfsEntry hdfsEntry =
            new HbckRegionInfo.HdfsEntry(regionDir.getPath(), regionDir.getModificationTime());
        hri.setHdfsEntry(hdfsEntry);
      }
    }
  }

  /**
   * When running, the HBCK report may be changed later.
   */
  public boolean isRunning() {
    return running;
  }

  /**
   * @return the regions only opened on RegionServers, but no region info in meta.
   */
  public Map<String, ServerName> getOrphanRegionsOnRS() {
    // Need synchronized here, as this "snapshot" may be changed after checking.
    rwLock.readLock().lock();
    try {
      return this.orphanRegionsOnRSSnapshot;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * @return the regions have directory on FileSystem, but no region info in meta.
   */
  public List<String> getOrphanRegionsOnFS() {
    // Need synchronized here, as this "snapshot" may be changed after checking.
    rwLock.readLock().lock();
    try {
      return this.orphanRegionsOnFSSnapshot;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Found the inconsistent regions. There are three case:
   * case 1. Master thought this region opened, but no regionserver reported it.
   * case 2. Master thought this region opened on Server1, but regionserver reported Server2
   * case 3. More than one regionservers reported opened this region
   *
   * @return the map of inconsistent regions. Key is the region name. Value is a pair of location in
   *         meta and the regionservers which reported opened this region.
   */
  public Map<String, Pair<ServerName, List<ServerName>>> getInconsistentRegions() {
    // Need synchronized here, as this "snapshot" may be changed after checking.
    rwLock.readLock().lock();
    try {
      return this.inconsistentRegionsSnapshot;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Used for web ui to show when the HBCK checking started.
   */
  public long getCheckingStartTimestamp() {
    return this.checkingStartTimestamp;
  }

  /**
   * Used for web ui to show when the HBCK checking report generated.
   */
  public long getCheckingEndTimestamp() {
    return this.checkingStartTimestamp;
  }
}