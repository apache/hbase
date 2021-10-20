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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HbckRegionInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to do the hbck checking job at master side.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HbckChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(HbckChore.class.getName());

  private static final String HBCK_CHORE_INTERVAL = "hbase.master.hbck.chore.interval";
  private static final int DEFAULT_HBCK_CHORE_INTERVAL = 60 * 60 * 1000;

  private final MasterServices master;

  /**
   * This map contains the state of all hbck items.  It maps from encoded region
   * name to HbckRegionInfo structure.  The information contained in HbckRegionInfo is used
   * to detect and correct consistency (hdfs/meta/deployment) problems.
   */
  private final Map<String, HbckRegionInfo> regionInfoMap = new HashMap<>();

  private final Set<String> disabledTableRegions = new HashSet<>();
  private final Set<String> splitParentRegions = new HashSet<>();

  /**
   * The regions only opened on RegionServers, but no region info in meta.
   */
  private final Map<String, ServerName> orphanRegionsOnRS = new HashMap<>();
  /**
   * The regions have directory on FileSystem, but no region info in meta.
   */
  private final Map<String, Path> orphanRegionsOnFS = new HashMap<>();
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
  private final Map<String, Path> orphanRegionsOnFSSnapshot = new HashMap<>();
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

  private boolean disabled = false;

  public HbckChore(MasterServices master) {
    super("HbckChore-", master,
        master.getConfiguration().getInt(HBCK_CHORE_INTERVAL, DEFAULT_HBCK_CHORE_INTERVAL));
    this.master = master;
    int interval =
        master.getConfiguration().getInt(HBCK_CHORE_INTERVAL, DEFAULT_HBCK_CHORE_INTERVAL);
    if (interval <= 0) {
      LOG.warn(HBCK_CHORE_INTERVAL + " is <=0 hence disabling hbck chore");
      disableChore();
    }
  }

  @Override
  protected synchronized void chore() {
    if (isDisabled() || isRunning()) {
      LOG.warn("hbckChore is either disabled or is already running. Can't run the chore");
      return;
    }
    regionInfoMap.clear();
    disabledTableRegions.clear();
    splitParentRegions.clear();
    orphanRegionsOnRS.clear();
    orphanRegionsOnFS.clear();
    inconsistentRegions.clear();
    checkingStartTimestamp = EnvironmentEdgeManager.currentTime();
    running = true;
    try {
      loadRegionsFromInMemoryState();
      loadRegionsFromRSReport();
      try {
        loadRegionsFromFS(scanForMergedParentRegions());
      } catch (IOException e) {
        LOG.warn("Failed to load the regions from filesystem", e);
      }
      saveCheckResultToSnapshot();
    } catch (Throwable t) {
      LOG.warn("Unexpected", t);
    }
    running = false;
    updateAssignmentManagerMetrics();
  }

  // This function does the sanity checks of making sure the chore is not run when it is
  // disabled or when it's already running. It returns whether the chore was actually run or not.
  protected boolean runChore() {
    if (isDisabled() || isRunning()) {
      if (isDisabled()) {
        LOG.warn("hbck chore is disabled! Set " + HBCK_CHORE_INTERVAL + " > 0 to enable it.");
      } else {
        LOG.warn("hbck chore already running. Can't run till it finishes.");
      }
      return false;
    }
    chore();
    return true;
  }

  private void disableChore() {
    this.disabled = true;
  }

  public boolean isDisabled() {
    return this.disabled;
  }

  private void saveCheckResultToSnapshot() {
    // Need synchronized here, as this "snapshot" may be access by web ui.
    rwLock.writeLock().lock();
    try {
      orphanRegionsOnRSSnapshot.clear();
      orphanRegionsOnRS.entrySet()
          .forEach(e -> orphanRegionsOnRSSnapshot.put(e.getKey(), e.getValue()));
      orphanRegionsOnFSSnapshot.clear();
      orphanRegionsOnFS.entrySet()
          .forEach(e -> orphanRegionsOnFSSnapshot.put(e.getKey(), e.getValue()));
      inconsistentRegionsSnapshot.clear();
      inconsistentRegions.entrySet()
          .forEach(e -> inconsistentRegionsSnapshot.put(e.getKey(), e.getValue()));
      checkingEndTimestamp = EnvironmentEdgeManager.currentTime();
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Scan hbase:meta to get set of merged parent regions, this is a very heavy scan.
   *
   * @return Return generated {@link HashSet}
   */
  private HashSet<String> scanForMergedParentRegions() throws IOException {
    HashSet<String> mergedParentRegions = new HashSet<>();
    // Null tablename means scan all of meta.
    MetaTableAccessor.scanMetaForTableRegions(this.master.getConnection(),
      r -> {
        List<RegionInfo> mergeParents = MetaTableAccessor.getMergeRegions(r.rawCells());
        if (mergeParents != null) {
          for (RegionInfo mergeRegion : mergeParents) {
            if (mergeRegion != null) {
              // This region is already being merged
              mergedParentRegions.add(mergeRegion.getEncodedName());
            }
          }
        }
        return true;
        },
      null);
    return mergedParentRegions;
  }

  private void loadRegionsFromInMemoryState() {
    List<RegionState> regionStates =
        master.getAssignmentManager().getRegionStates().getRegionStates();
    for (RegionState regionState : regionStates) {
      RegionInfo regionInfo = regionState.getRegion();
      if (master.getTableStateManager()
          .isTableState(regionInfo.getTable(), TableState.State.DISABLED)) {
        disabledTableRegions.add(regionInfo.getRegionNameAsString());
      }
      // Check both state and regioninfo for split status, see HBASE-26383
      if (regionState.isSplit() || regionInfo.isSplit()) {
        splitParentRegions.add(regionInfo.getRegionNameAsString());
      }
      HbckRegionInfo.MetaEntry metaEntry =
          new HbckRegionInfo.MetaEntry(regionInfo, regionState.getServerName(),
              regionState.getStamp());
      regionInfoMap.put(regionInfo.getEncodedName(), new HbckRegionInfo(metaEntry));
    }
    LOG.info("Loaded {} regions ({} disabled, {} split parents) from in-memory state",
      regionStates.size(), disabledTableRegions.size(), splitParentRegions.size());
    if (LOG.isDebugEnabled()) {
      Map<RegionState.State,Integer> stateCountMap = new HashMap<>();
      for (RegionState regionState : regionStates) {
        stateCountMap.compute(regionState.getState(), (k, v) -> (v == null) ? 1 : v + 1);
      }
      StringBuffer sb = new StringBuffer();
      sb.append("Regions by state: ");
      stateCountMap.entrySet().forEach(e -> {
          sb.append(e.getKey());
          sb.append('=');
          sb.append(e.getValue());
          sb.append(' ');
        }
      );
      LOG.debug(sb.toString());
    }
    if (LOG.isTraceEnabled()) {
      for (RegionState regionState : regionStates) {
        LOG.trace("{}: {}, serverName=", regionState.getRegion(), regionState.getState(),
          regionState.getServerName());
      }
    }
  }

  private void loadRegionsFromRSReport() {
    int numRegions = 0;
    Map<ServerName, Set<byte[]>> rsReports = master.getAssignmentManager().getRSReports();
    for (Map.Entry<ServerName, Set<byte[]>> entry : rsReports.entrySet()) {
      ServerName serverName = entry.getKey();
      for (byte[] regionName : entry.getValue()) {
        String encodedRegionName = RegionInfo.encodeRegionName(regionName);
        HbckRegionInfo hri = regionInfoMap.get(encodedRegionName);
        if (hri == null) {
          orphanRegionsOnRS.put(RegionInfo.getRegionNameAsString(regionName), serverName);
          continue;
        }
        hri.addServer(hri.getMetaEntry(), serverName);
      }
      numRegions += entry.getValue().size();
    }
    LOG.info("Loaded {} regions from {} regionservers' reports and found {} orphan regions",
        numRegions, rsReports.size(), orphanRegionsOnRS.size());

    for (Map.Entry<String, HbckRegionInfo> entry : regionInfoMap.entrySet()) {
      HbckRegionInfo hri = entry.getValue();
      ServerName locationInMeta = hri.getMetaEntry().getRegionServer();
      if (locationInMeta == null) {
        continue;
      }
      if (hri.getDeployedOn().size() == 0) {
        // skip the offline region which belong to disabled table.
        if (disabledTableRegions.contains(hri.getRegionNameAsString())) {
          continue;
        }
        // skip the split parent regions
        if (splitParentRegions.contains(hri.getRegionNameAsString())) {
          continue;
        }
        // Master thought this region opened, but no regionserver reported it.
        inconsistentRegions.put(hri.getRegionNameAsString(),
            new Pair<>(locationInMeta, new LinkedList<>()));
      } else if (hri.getDeployedOn().size() > 1) {
        // More than one regionserver reported opened this region
        inconsistentRegions.put(hri.getRegionNameAsString(),
            new Pair<>(locationInMeta, hri.getDeployedOn()));
      } else if (!hri.getDeployedOn().get(0).equals(locationInMeta)) {
        // Master thought this region opened on Server1, but regionserver reported Server2
        inconsistentRegions.put(hri.getRegionNameAsString(),
            new Pair<>(locationInMeta, hri.getDeployedOn()));
      }
    }
  }

  private void loadRegionsFromFS(final HashSet<String> mergedParentRegions) throws IOException {
    Path rootDir = master.getMasterFileSystem().getRootDir();
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    int numRegions = 0;
    List<Path> tableDirs = FSUtils.getTableDirs(fs, rootDir);
    for (Path tableDir : tableDirs) {
      List<Path> regionDirs = FSUtils.getRegionDirs(fs, tableDir);
      for (Path regionDir : regionDirs) {
        String encodedRegionName = regionDir.getName();
        if (encodedRegionName == null) {
          LOG.warn("Failed get of encoded name from {}", regionDir);
          continue;
        }
        HbckRegionInfo hri = regionInfoMap.get(encodedRegionName);
        // If it is not in in-memory database and not a merged region,
        // report it as an orphan region.
        if (hri == null && !mergedParentRegions.contains(encodedRegionName)) {
          orphanRegionsOnFS.put(encodedRegionName, regionDir);
          continue;
        }
      }
      numRegions += regionDirs.size();
    }
    LOG.info("Loaded {} tables {} regions from filesystem and found {} orphan regions",
        tableDirs.size(), numRegions, orphanRegionsOnFS.size());
  }

  private void updateAssignmentManagerMetrics() {
    master.getAssignmentManager().getAssignmentManagerMetrics()
        .updateOrphanRegionsOnRs(getOrphanRegionsOnRS().size());
    master.getAssignmentManager().getAssignmentManagerMetrics()
        .updateOrphanRegionsOnFs(getOrphanRegionsOnFS().size());
    master.getAssignmentManager().getAssignmentManagerMetrics()
        .updateInconsistentRegions(getInconsistentRegions().size());
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
  public Map<String, Path> getOrphanRegionsOnFS() {
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
    return this.checkingEndTimestamp;
  }
}
