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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MapreduceHFileArchiver;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IOUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper to Restore/Clone a Snapshot
 * <p>
 * The helper assumes that a table is already created, and by calling restore() the content present
 * in the snapshot will be restored as the new content of the table.
 * <p>
 * Clone from Snapshot: If the target table is empty, the restore operation is just a "clone
 * operation", where the only operations are:
 * <ul>
 * <li>for each region in the snapshot create a new region (note that the region will have a
 * different name, since the encoding contains the table name)
 * <li>for each file in the region create a new HFileLink to point to the original file.
 * <li>restore the logs, if any
 * </ul>
 * <p>
 * Restore from Snapshot:
 * <ul>
 * <li>for each region in the table verify which are available in the snapshot and which are not
 * <ul>
 * <li>if the region is not present in the snapshot, remove it.
 * <li>if the region is present in the snapshot
 * <ul>
 * <li>for each file in the table region verify which are available in the snapshot
 * <ul>
 * <li>if the hfile is not present in the snapshot, remove it
 * <li>if the hfile is present, keep it (nothing to do)
 * </ul>
 * <li>for each file in the snapshot region but not in the table
 * <ul>
 * <li>create a new HFileLink that point to the original file
 * </ul>
 * </ul>
 * </ul>
 * <li>for each region in the snapshot not present in the current table state
 * <ul>
 * <li>create a new region and for each file in the region create a new HFileLink (This is the same
 * as the clone operation)
 * </ul>
 * <li>restore the logs, if any
 * </ul>
 */
@InterfaceAudience.Private
public final class MapreduceRestoreSnapshotHelper {

  private static final Logger LOG = LoggerFactory.getLogger(MapreduceRestoreSnapshotHelper.class);
  private final Map<byte[], byte[]> regionsMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  private final Map<String, Pair<String, String>> parentsMap = new HashMap<>();

  private final ForeignExceptionDispatcher monitor;
  private final MonitoredTask status;

  private final SnapshotManifest snapshotManifest;
  private final SnapshotDescription snapshotDesc;
  private final TableName snapshotTable;

  private final TableDescriptor tableDesc;
  private final Path rootDir;
  private final Path tableDir;

  private final Configuration conf;
  private final FileSystem fs;
  private final boolean createBackRefs;

  public MapreduceRestoreSnapshotHelper(final Configuration conf, final FileSystem fs,
    final SnapshotManifest manifest, final TableDescriptor tableDescriptor, final Path rootDir,
    final ForeignExceptionDispatcher monitor, final MonitoredTask status,
    final boolean createBackRefs) {
    this.fs = fs;
    this.conf = conf;
    this.snapshotManifest = manifest;
    this.snapshotDesc = manifest.getSnapshotDescription();
    this.snapshotTable = TableName.valueOf(snapshotDesc.getTable());
    this.tableDesc = tableDescriptor;
    this.rootDir = rootDir;
    this.tableDir = CommonFSUtils.getTableDir(rootDir, tableDesc.getTableName());
    this.monitor = monitor;
    this.status = status;
    this.createBackRefs = createBackRefs;
  }

  /**
   * Restore the on-disk table to a specified snapshot state.
   * @return the set of regions touched by the restore operation
   */
  public MapreduceRestoreSnapshotHelper.RestoreMetaChanges restoreHdfsRegions() throws IOException {
    ThreadPoolExecutor exec = SnapshotManifest.createExecutor(conf, "RestoreSnapshot");
    try {
      return restoreHdfsRegions(exec);
    } finally {
      exec.shutdown();
    }
  }

  private MapreduceRestoreSnapshotHelper.RestoreMetaChanges restoreHdfsRegions(final ThreadPoolExecutor exec) throws IOException {
    LOG.info("starting restore table regions using snapshot={}", snapshotDesc);

    Map<String, SnapshotProtos.SnapshotRegionManifest> regionManifests = snapshotManifest.getRegionManifestsMap();
    if (regionManifests == null) {
      LOG.warn("Nothing to restore. Snapshot {} looks empty", snapshotDesc);
      return null;
    }

    MapreduceRestoreSnapshotHelper.RestoreMetaChanges
      metaChanges = new MapreduceRestoreSnapshotHelper.RestoreMetaChanges(tableDesc, parentsMap);

    // Take a copy of the manifest.keySet() since we are going to modify
    // this instance, by removing the regions already present in the restore dir.
    Set<String> regionNames = new HashSet<>(regionManifests.keySet());

    List<RegionInfo> tableRegions = getTableRegions();

    RegionInfo mobRegion =
      MobUtils.getMobRegionInfo(snapshotManifest.getTableDescriptor().getTableName());
    if (tableRegions != null) {
      // restore the mob region in case
      if (regionNames.contains(mobRegion.getEncodedName())) {
        monitor.rethrowException();
        status.setStatus("Restoring mob region...");
        List<RegionInfo> mobRegions = new ArrayList<>(1);
        mobRegions.add(mobRegion);
        restoreHdfsMobRegions(exec, regionManifests, mobRegions);
        regionNames.remove(mobRegion.getEncodedName());
        status.setStatus("Finished restoring mob region.");
      }
    }
    if (regionNames.contains(mobRegion.getEncodedName())) {
      // add the mob region
      monitor.rethrowException();
      status.setStatus("Cloning mob region...");
      cloneHdfsMobRegion(regionManifests, mobRegion);
      regionNames.remove(mobRegion.getEncodedName());
      status.setStatus("Finished cloning mob region.");
    }

    // Identify which region are still available and which not.
    // NOTE: we rely upon the region name as: "table name, start key, end key"
    if (tableRegions != null) {
      monitor.rethrowException();
      for (RegionInfo regionInfo : tableRegions) {
        String regionName = regionInfo.getEncodedName();
        if (regionNames.contains(regionName)) {
          LOG.info("region to restore: {}", regionName);
          regionNames.remove(regionName);
          metaChanges.addRegionToRestore(
            ProtobufUtil.toRegionInfo(regionManifests.get(regionName).getRegionInfo()));
        } else {
          LOG.info("region to remove: {}", regionName);
          metaChanges.addRegionToRemove(regionInfo);
        }
      }
    }

    // Regions to Add: present in the snapshot but not in the current table
    List<RegionInfo> regionsToAdd = new ArrayList<>(regionNames.size());
    if (!regionNames.isEmpty()) {
      monitor.rethrowException();
      for (String regionName : regionNames) {
        LOG.info("region to add: {}", regionName);
        regionsToAdd
          .add(ProtobufUtil.toRegionInfo(regionManifests.get(regionName).getRegionInfo()));
      }
    }

    // Create new regions cloning from the snapshot
    // HBASE-19980: We need to call cloneHdfsRegions() before restoreHdfsRegions() because
    // regionsMap is constructed in cloneHdfsRegions() and it can be used in restoreHdfsRegions().
    monitor.rethrowException();
    status.setStatus("Cloning regions...");
    RegionInfo[] clonedRegions = cloneHdfsRegions(exec, regionManifests, regionsToAdd);
    metaChanges.setNewRegions(clonedRegions);
    status.setStatus("Finished cloning regions.");

    // Restore regions using the snapshot data
    monitor.rethrowException();
    status.setStatus("Restoring table regions...");
    restoreHdfsRegions(exec, regionManifests, metaChanges.getRegionsToRestore());
    status.setStatus("Finished restoring all table regions.");

    // Remove regions from the current table
    monitor.rethrowException();
    status.setStatus("Starting to delete excess regions from table");
    removeHdfsRegions(exec, metaChanges.getRegionsToRemove());
    status.setStatus("Finished deleting excess regions from table.");

    LOG.info("finishing restore table regions using snapshot={}", snapshotDesc);

    return metaChanges;
  }

  /**
   * Describe the set of operations needed to update hbase:meta after restore.
   */
  private static class RestoreMetaChanges {
    private final Map<String, Pair<String, String>> parentsMap;
    private final TableDescriptor htd;

    private List<RegionInfo> regionsToRestore = null;
    private List<RegionInfo> regionsToRemove = null;
    private List<RegionInfo> regionsToAdd = null;

    public RestoreMetaChanges(TableDescriptor htd, Map<String, Pair<String, String>> parentsMap) {
      this.parentsMap = parentsMap;
      this.htd = htd;
    }

    /**
     * Returns the list of 'restored regions' during the on-disk restore. The caller is responsible
     * to add the regions to hbase:meta if not present.
     * @return the list of regions restored
     */
    public List<RegionInfo> getRegionsToRestore() {
      return this.regionsToRestore;
    }

    /**
     * Returns the list of regions removed during the on-disk restore. The caller is responsible to
     * remove the regions from META. e.g. MetaTableAccessor.deleteRegions(...)
     * @return the list of regions to remove from META
     */
    public List<RegionInfo> getRegionsToRemove() {
      return this.regionsToRemove;
    }

    void setNewRegions(final RegionInfo[] hris) {
      if (hris != null) {
        regionsToAdd = Arrays.asList(hris);
      } else {
        regionsToAdd = null;
      }
    }

    void addRegionToRemove(final RegionInfo hri) {
      if (regionsToRemove == null) {
        regionsToRemove = new LinkedList<>();
      }
      regionsToRemove.add(hri);
    }

    void addRegionToRestore(final RegionInfo hri) {
      if (regionsToRestore == null) {
        regionsToRestore = new LinkedList<>();
      }
      regionsToRestore.add(hri);
    }
  }

  /**
   * Remove specified regions from the file-system, using the archiver.
   */
  private void removeHdfsRegions(final ThreadPoolExecutor exec, final List<RegionInfo> regions)
    throws IOException {
    if (regions == null || regions.isEmpty()) return;
    ModifyRegionUtils.editRegions(exec, regions,
      (ModifyRegionUtils.RegionEditTask) hri -> MapreduceHFileArchiver.archiveRegion(conf, fs, hri, rootDir, tableDir));
  }

  /**
   * Restore specified regions by restoring content to the snapshot state.
   */
  private void restoreHdfsRegions(final ThreadPoolExecutor exec,
    final Map<String, SnapshotProtos.SnapshotRegionManifest> regionManifests, final List<RegionInfo> regions)
    throws IOException {
    if (regions == null || regions.isEmpty()) return;
    ModifyRegionUtils.editRegions(exec, regions,
      (ModifyRegionUtils.RegionEditTask) hri -> restoreRegion(hri, regionManifests.get(hri.getEncodedName())));
  }

  /**
   * Restore specified mob regions by restoring content to the snapshot state.
   */
  private void restoreHdfsMobRegions(final ThreadPoolExecutor exec,
    final Map<String, SnapshotProtos.SnapshotRegionManifest> regionManifests, final List<RegionInfo> regions)
    throws IOException {
    if (regions == null || regions.isEmpty()) return;
    ModifyRegionUtils.editRegions(exec, regions,
      (ModifyRegionUtils.RegionEditTask) hri -> restoreMobRegion(hri, regionManifests.get(hri.getEncodedName())));
  }

  private Map<String, List<SnapshotProtos.SnapshotRegionManifest.StoreFile>>
  getRegionHFileReferences(final SnapshotProtos.SnapshotRegionManifest manifest) {
    Map<String, List<SnapshotProtos.SnapshotRegionManifest.StoreFile>> familyMap =
      new HashMap<>(manifest.getFamilyFilesCount());
    for (SnapshotProtos.SnapshotRegionManifest.FamilyFiles familyFiles : manifest.getFamilyFilesList()) {
      familyMap.put(familyFiles.getFamilyName().toStringUtf8(),
        new ArrayList<>(familyFiles.getStoreFilesList()));
    }
    return familyMap;
  }

  /**
   * Restore region by removing files not in the snapshot and adding the missing ones from the
   * snapshot.
   */
  private void restoreRegion(final RegionInfo regionInfo,
    final SnapshotProtos.SnapshotRegionManifest regionManifest) throws IOException {
    restoreRegion(regionInfo, regionManifest, new Path(tableDir, regionInfo.getEncodedName()),
      tableDir);
  }

  /**
   * Restore mob region by removing files not in the snapshot and adding the missing ones from the
   * snapshot.
   */
  private void restoreMobRegion(final RegionInfo regionInfo,
    final SnapshotProtos.SnapshotRegionManifest regionManifest) throws IOException {
    if (regionManifest == null) {
      return;
    }
    restoreRegion(regionInfo, regionManifest,
      MobUtils.getMobRegionPath(conf, tableDesc.getTableName()),
      MobUtils.getMobTableDir(conf, tableDesc.getTableName()));
  }

  /**
   * Restore region by removing files not in the snapshot and adding the missing ones from the
   * snapshot.
   */
  private void restoreRegion(final RegionInfo regionInfo,
    final SnapshotProtos.SnapshotRegionManifest regionManifest, Path regionDir, Path tableDir) throws IOException {
    Map<String, List<SnapshotProtos.SnapshotRegionManifest.StoreFile>> snapshotFiles =
      getRegionHFileReferences(regionManifest);

    String tableName = tableDesc.getTableName().getNameAsString();
    final String snapshotName = snapshotDesc.getName();

    HRegionFileSystem regionFS = (fs.exists(regionDir))
      ? HRegionFileSystem.openRegionFromFileSystem(conf, fs, tableDir, regionInfo, false)
      : HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, regionInfo);

    // Restore families present in the table
    for (Path familyDir : FSUtils.getFamilyDirs(fs, regionDir)) {
      byte[] family = Bytes.toBytes(familyDir.getName());
      ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(family);
      StoreFileTracker tracker = StoreFileTrackerFactory.create(conf, true,
        StoreContext.getBuilder().withColumnFamilyDescriptor(familyDescriptor)
          .withFamilyStoreDirectoryPath(familyDir).withRegionFileSystem(regionFS).build());
      List<StoreFileInfo> storeFileInfos = tracker.load();
      List<String> familyFiles = storeFileInfos.stream()
        .map(storeFileInfo -> storeFileInfo.getPath().getName()).collect(Collectors.toList());
      List<SnapshotProtos.SnapshotRegionManifest.StoreFile> snapshotFamilyFiles =
        snapshotFiles.remove(familyDir.getName());
      List<StoreFileInfo> filesToTrack = new ArrayList<>();
      if (snapshotFamilyFiles != null) {
        List<SnapshotProtos.SnapshotRegionManifest.StoreFile> hfilesToAdd = new ArrayList<>();
        for (SnapshotProtos.SnapshotRegionManifest.StoreFile storeFile : snapshotFamilyFiles) {
          if (familyFiles.contains(storeFile.getName())) {
            // HFile already present
            familyFiles.remove(storeFile.getName());
            // no need to restore already present files, but we need to add those to tracker
            filesToTrack
              .add(tracker.getStoreFileInfo(new Path(familyDir, storeFile.getName()), true));
          } else {
            // HFile missing
            hfilesToAdd.add(storeFile);
          }
        }

        // Remove hfiles not present in the snapshot
        for (String hfileName : familyFiles) {
          for (StoreFileInfo storeFileInfo : storeFileInfos) {
            if (hfileName.equals(storeFileInfo.getPath().getName())) {
              tracker.removeStoreFiles(
                StoreUtils.toHStoreFile(Collections.singletonList(storeFileInfo), null, null));
              LOG.trace("Removing HFile={} not present in snapshot={} from region={} table={}",
                hfileName, snapshotName, regionInfo.getEncodedName(), tableName);
            }
          }
        }

        // Restore Missing files
        for (SnapshotProtos.SnapshotRegionManifest.StoreFile storeFile : hfilesToAdd) {
          LOG.debug("Restoring missing HFileLink {} of snapshot={} to region={} table={}",
            storeFile.getName(), snapshotName, regionInfo.getEncodedName(), tableName);
          StoreFileInfo storeFileInfo =
            restoreStoreFile(familyDir, regionInfo, storeFile, createBackRefs, tracker);
          // mark the reference file to be added to tracker
          filesToTrack.add(storeFileInfo);
        }
      } else {
        // Family doesn't exist in the snapshot
        LOG.trace("Removing family={} in snapshot={} from region={} table={}",
          Bytes.toString(family), snapshotName, regionInfo.getEncodedName(), tableName);
        LOG.debug("Removing family={} in snapshot={} from region={} table={}",
          Bytes.toString(family), snapshotName, regionInfo.getEncodedName(), tableName);
        MapreduceHFileArchiver.archiveFamilyByFamilyDir(fs, conf, regionInfo, familyDir, family);
        fs.delete(familyDir, true);
      }

      // simply reset list of tracked files with the matching files
      // and the extra one present in the snapshot
      tracker.set(filesToTrack);
    }

    // Add families not present in the table
    for (Map.Entry<String, List<SnapshotProtos.SnapshotRegionManifest.StoreFile>> familyEntry : snapshotFiles
      .entrySet()) {
      Path familyDir = new Path(regionDir, familyEntry.getKey());
      StoreFileTracker tracker =
        StoreFileTrackerFactory.create(conf, true, StoreContext.getBuilder()
          .withFamilyStoreDirectoryPath(familyDir).withRegionFileSystem(regionFS).build());
      List<StoreFileInfo> files = new ArrayList<>();
      if (!fs.mkdirs(familyDir)) {
        throw new IOException("Unable to create familyDir=" + familyDir);
      }

      for (SnapshotProtos.SnapshotRegionManifest.StoreFile storeFile : familyEntry.getValue()) {
        LOG.trace("Adding HFileLink (Not present in the table) {} of snapshot {} to table={}",
          storeFile.getName(), snapshotName, tableName);
        StoreFileInfo storeFileInfo =
          restoreStoreFile(familyDir, regionInfo, storeFile, createBackRefs, tracker);
        files.add(storeFileInfo);
      }
      tracker.set(files);
    }
  }

  /**
   * Clone specified regions. For each region create a new region and create a HFileLink for each
   * hfile.
   */
  private RegionInfo[] cloneHdfsRegions(final ThreadPoolExecutor exec,
    final Map<String, SnapshotProtos.SnapshotRegionManifest> regionManifests, final List<RegionInfo> regions)
    throws IOException {
    if (regions == null || regions.isEmpty()) return null;

    final Map<String, RegionInfo> snapshotRegions = new HashMap<>(regions.size());
    final String snapshotName = snapshotDesc.getName();

    // clone region info (change embedded tableName with the new one)
    RegionInfo[] clonedRegionsInfo = new RegionInfo[regions.size()];
    for (int i = 0; i < clonedRegionsInfo.length; ++i) {
      // clone the region info from the snapshot region info
      RegionInfo snapshotRegionInfo = regions.get(i);
      clonedRegionsInfo[i] = cloneRegionInfo(snapshotRegionInfo);

      // add the region name mapping between snapshot and cloned
      String snapshotRegionName = snapshotRegionInfo.getEncodedName();
      String clonedRegionName = clonedRegionsInfo[i].getEncodedName();
      regionsMap.put(Bytes.toBytes(snapshotRegionName), Bytes.toBytes(clonedRegionName));
      LOG.info("clone region={} as {} in snapshot {}", snapshotRegionName, clonedRegionName,
        snapshotName);

      // Add mapping between cloned region name and snapshot region info
      snapshotRegions.put(clonedRegionName, snapshotRegionInfo);
    }

    // create the regions on disk
    ModifyRegionUtils.createRegions(exec, conf, rootDir, tableDesc, clonedRegionsInfo,
      (ModifyRegionUtils.RegionFillTask) region -> {
        RegionInfo snapshotHri = snapshotRegions.get(region.getRegionInfo().getEncodedName());
        cloneRegion(region, snapshotHri, regionManifests.get(snapshotHri.getEncodedName()));
      });

    return clonedRegionsInfo;
  }

  /**
   * Clone the mob region. For the region create a new region and create a HFileLink for each hfile.
   */
  private void cloneHdfsMobRegion(final Map<String, SnapshotProtos.SnapshotRegionManifest> regionManifests,
    final RegionInfo region) throws IOException {
    // clone region info (change embedded tableName with the new one)
    Path clonedRegionPath = MobUtils.getMobRegionPath(rootDir, tableDesc.getTableName());
    cloneRegion(MobUtils.getMobRegionInfo(tableDesc.getTableName()), clonedRegionPath, region,
      regionManifests.get(region.getEncodedName()));
  }

  /**
   * Clone region directory content from the snapshot info. Each region is encoded with the table
   * name, so the cloned region will have a different region name. Instead of copying the hfiles a
   * HFileLink is created.
   * @param regionDir {@link Path} cloned dir
   */
  private void cloneRegion(final RegionInfo newRegionInfo, final Path regionDir,
    final RegionInfo snapshotRegionInfo, final SnapshotProtos.SnapshotRegionManifest manifest) throws IOException {
    final String tableName = tableDesc.getTableName().getNameAsString();
    final String snapshotName = snapshotDesc.getName();
    for (SnapshotProtos.SnapshotRegionManifest.FamilyFiles familyFiles : manifest.getFamilyFilesList()) {
      Path familyDir = new Path(regionDir, familyFiles.getFamilyName().toStringUtf8());
      List<StoreFileInfo> clonedFiles = new ArrayList<>();
      HRegionFileSystem regionFS = (fs.exists(regionDir))
        ? HRegionFileSystem.openRegionFromFileSystem(conf, fs, tableDir, newRegionInfo, false)
        : HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, newRegionInfo);

      Configuration sftConf = StoreUtils.createStoreConfiguration(conf, tableDesc,
        tableDesc.getColumnFamily(familyFiles.getFamilyName().toByteArray()));
      StoreFileTracker tracker =
        StoreFileTrackerFactory
          .create(sftConf, true,
            StoreContext.getBuilder()
              .withFamilyStoreDirectoryPath(
                new Path(regionDir, familyFiles.getFamilyName().toStringUtf8()))
              .withRegionFileSystem(regionFS)
              .withColumnFamilyDescriptor(
                ColumnFamilyDescriptorBuilder.of(familyFiles.getFamilyName().toByteArray()))
              .build());
      tracker.load();
      for (SnapshotProtos.SnapshotRegionManifest.StoreFile storeFile : familyFiles.getStoreFilesList()) {
        LOG.info("Adding HFileLink {} from cloned region in snapshot {} to table={}",
          storeFile.getName(), snapshotName, tableName);
        if (MobUtils.isMobRegionInfo(newRegionInfo)) {
          String mobFileName =
            HFileLink.createHFileLinkName(snapshotRegionInfo, storeFile.getName());
          Path mobPath = new Path(familyDir, mobFileName);
          if (fs.exists(mobPath)) {
            fs.delete(mobPath, true);
          }
          StoreFileInfo storeFileInfo =
            restoreStoreFile(familyDir, snapshotRegionInfo, storeFile, createBackRefs, tracker);
          clonedFiles.add(storeFileInfo);
        } else {
          StoreFileInfo storeFileInfo =
            restoreStoreFile(familyDir, snapshotRegionInfo, storeFile, createBackRefs, tracker);
          clonedFiles.add(storeFileInfo);
        }
      }
      tracker.add(clonedFiles);
    }

  }

  /**
   * Clone region directory content from the snapshot info. Each region is encoded with the table
   * name, so the cloned region will have a different region name. Instead of copying the hfiles a
   * HFileLink is created.
   * @param region {@link HRegion} cloned
   */
  private void cloneRegion(final HRegion region, final RegionInfo snapshotRegionInfo,
    final SnapshotProtos.SnapshotRegionManifest manifest) throws IOException {
    cloneRegion(region.getRegionInfo(), new Path(tableDir, region.getRegionInfo().getEncodedName()),
      snapshotRegionInfo, manifest);
  }

  /**
   * Create a new {@link HFileLink} to reference the store file.
   * <p>
   * The store file in the snapshot can be a simple hfile, an HFileLink or a reference.
   * <ul>
   * <li>hfile: abc -> table=region-abc
   * <li>reference: abc.1234 -> table=region-abc.1234
   * <li>hfilelink: table=region-hfile -> table=region-hfile
   * </ul>
   * @param familyDir     destination directory for the store file
   * @param regionInfo    destination region info for the table
   * @param createBackRef - Whether back reference should be created. Defaults to true.
   * @param storeFile     store file name (can be a Reference, HFileLink or simple HFile)
   */
  private StoreFileInfo restoreStoreFile(final Path familyDir, final RegionInfo regionInfo,
    final SnapshotProtos.SnapshotRegionManifest.StoreFile storeFile, final boolean createBackRef,
    final StoreFileTracker tracker) throws IOException {
    String hfileName = storeFile.getName();
    StoreFileInfo info = null;
    if (HFileLink.isHFileLink(hfileName) || StoreFileInfo.isMobFileLink(hfileName)) {
      HFileLink hfileLink = tracker.createFromHFileLink(hfileName, createBackRef);
      info = new StoreFileInfo(conf, fs, new Path(familyDir, hfileName), hfileLink);
      return info;
    } else if (StoreFileInfo.isReference(hfileName)) {
      return restoreReferenceFile(familyDir, regionInfo, storeFile, tracker);
    } else {
      HFileLink hfileLink = tracker.createAndCommitHFileLink(regionInfo.getTable(),
        regionInfo.getEncodedName(), hfileName, createBackRef);
      return new StoreFileInfo(conf, fs, new Path(familyDir, HFileLink
        .createHFileLinkName(regionInfo.getTable(), regionInfo.getEncodedName(), hfileName)),
        hfileLink);
    }
  }

  /**
   * Create a new {@link Reference} as copy of the source one.
   * <p>
   * <blockquote>
   *
   * <pre>
   * The source table looks like:
   *    1234/abc      (original file)
   *    5678/abc.1234 (reference file)
   *
   * After the clone operation looks like:
   *   wxyz/table=1234-abc
   *   stuv/table=1234-abc.wxyz
   *
   * NOTE that the region name in the clone changes (md5 of regioninfo)
   * and the reference should reflect that change.
   * </pre>
   *
   * </blockquote>
   * @param familyDir  destination directory for the store file
   * @param regionInfo destination region info for the table
   * @param storeFile  reference file name
   */
  private StoreFileInfo restoreReferenceFile(final Path familyDir, final RegionInfo regionInfo,
    final SnapshotProtos.SnapshotRegionManifest.StoreFile storeFile, final StoreFileTracker tracker)
    throws IOException {
    String hfileName = storeFile.getName();
    StoreFileInfo storeFileInfo = null;

    // Extract the referred information (hfile name and parent region)
    Path refPath =
      StoreFileInfo
        .getReferredToFile(
          new Path(
            new Path(
              new Path(new Path(snapshotTable.getNamespaceAsString(),
                snapshotTable.getQualifierAsString()), regionInfo.getEncodedName()),
              familyDir.getName()),
            hfileName));
    String snapshotRegionName = refPath.getParent().getParent().getName();
    String fileName = refPath.getName();

    // The new reference should have the cloned region name as parent, if it is a clone.
    String clonedRegionName = Bytes.toString(regionsMap.get(Bytes.toBytes(snapshotRegionName)));
    if (clonedRegionName == null) clonedRegionName = snapshotRegionName;

    // The output file should be a reference link table=snapshotRegion-fileName.clonedRegionName
    Path linkPath = null;
    String refLink = fileName;
    if (!HFileLink.isHFileLink(fileName)) {
      refLink = HFileLink.createHFileLinkName(snapshotTable, snapshotRegionName, fileName);
      linkPath = new Path(familyDir,
        HFileLink.createHFileLinkName(snapshotTable, regionInfo.getEncodedName(), hfileName));
    }

    Path outPath = new Path(familyDir, refLink + '.' + clonedRegionName);

    // Create the new reference
    if (storeFile.hasReference()) {
      Reference reference = Reference.convert(storeFile.getReference());
      tracker.createAndCommitReference(reference, outPath);
      storeFileInfo = new StoreFileInfo(conf, fs, outPath, reference);
    } else {
      InputStream in;
      if (linkPath != null) {
        HFileLink hfileLink = HFileLink.buildFromHFileLinkPattern(conf, linkPath);
        storeFileInfo = new StoreFileInfo(conf, fs, outPath, hfileLink);
        tracker.add(Collections.singletonList(storeFileInfo));
        in = hfileLink.open(fs);
      } else {
        linkPath = new Path(new Path(
          HRegion.getRegionDir(snapshotManifest.getSnapshotDir(), regionInfo.getEncodedName()),
          familyDir.getName()), hfileName);
        in = fs.open(linkPath);
      }
      OutputStream out = fs.create(outPath);
      IOUtils.copyBytes(in, out, conf);
    }

    // Add the daughter region to the map
    String regionName = Bytes.toString(regionsMap.get(regionInfo.getEncodedNameAsBytes()));
    if (regionName == null) {
      regionName = regionInfo.getEncodedName();
    }
    LOG.debug("Restore reference {} to {}", regionName, clonedRegionName);
    synchronized (parentsMap) {
      Pair<String, String> daughters = parentsMap.get(clonedRegionName);
      if (daughters == null) {
        // In case one side of the split is already compacted, regionName is put as both first and
        // second of Pair
        daughters = new Pair<>(regionName, regionName);
        parentsMap.put(clonedRegionName, daughters);
      } else if (!regionName.equals(daughters.getFirst())) {
        daughters.setSecond(regionName);
      }
    }
    return storeFileInfo;
  }

  /**
   * Create a new {@link RegionInfo} from the snapshot region info. Keep the same startKey, endKey,
   * regionId and split information but change the table name.
   * @param snapshotRegionInfo Info for region to clone.
   * @return the new HRegion instance
   */
  public RegionInfo cloneRegionInfo(final RegionInfo snapshotRegionInfo) {
    return cloneRegionInfo(tableDesc.getTableName(), snapshotRegionInfo);
  }

  public static RegionInfo cloneRegionInfo(TableName tableName, RegionInfo snapshotRegionInfo) {
    return RegionInfoBuilder.newBuilder(tableName).setStartKey(snapshotRegionInfo.getStartKey())
      .setEndKey(snapshotRegionInfo.getEndKey()).setSplit(snapshotRegionInfo.isSplit())
      .setRegionId(snapshotRegionInfo.getRegionId()).setOffline(snapshotRegionInfo.isOffline())
      .build();
  }

  /** Returns the set of the regions contained in the table */
  private List<RegionInfo> getTableRegions() throws IOException {
    LOG.debug("get table regions: {}", tableDir);
    FileStatus[] regionDirs =
      CommonFSUtils.listStatus(fs, tableDir, new FSUtils.RegionDirFilter(fs));
    if (regionDirs == null) {
      return null;
    }

    List<RegionInfo> regions = new ArrayList<>(regionDirs.length);
    for (FileStatus regionDir : regionDirs) {
      RegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir.getPath());
      regions.add(hri);
    }
    LOG.debug("found {} regions for table={}", regions.size(),
      tableDesc.getTableName().getNameAsString());
    return regions;
  }

  /**
   * Copy the snapshot files for a snapshot scanner, discards meta changes.
   */
  public static MapreduceRestoreSnapshotHelper.RestoreMetaChanges copySnapshotForScanner(Configuration conf, FileSystem fs,
    Path rootDir, Path restoreDir, String snapshotName) throws IOException {
    // ensure that restore dir is not under root dir
    if (!restoreDir.getFileSystem(conf).getUri().equals(rootDir.getFileSystem(conf).getUri())) {
      throw new IllegalArgumentException(
        "Filesystems for restore directory and HBase root " + "directory should be the same");
    }
    if (restoreDir.toUri().getPath().startsWith(rootDir.toUri().getPath() + "/")) {
      throw new IllegalArgumentException("Restore directory cannot be a sub directory of HBase "
        + "root directory. RootDir: " + rootDir + ", restoreDir: " + restoreDir);
    }
    String restorePath = restoreDir.toUri().getPath();
    String rootPath = rootDir.toUri().getPath();
    if (restorePath.equals(rootPath) || restorePath.startsWith(rootPath + "/")) {
      String message = "BLOCKED: MapReduce restore directory cannot be the HBase root directory "
        + "or a sub directory of it. This could lead to accidental archival and permanent "
        + "data loss if the path falls under " + rootDir + "/data/. Use a temporary directory "
        + "outside of hbase.rootdir for MR snapshot scanning. RootDir: " + rootDir
        + ", restoreDir: " + restoreDir;
      LOG.error(message);
      throw new IllegalArgumentException(message);
    }

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    // check if the snapshot is expired.
    boolean isExpired = SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDesc.getTtl(),
      snapshotDesc.getCreationTime(), EnvironmentEdgeManager.currentTime());
    if (isExpired) {
      throw new SnapshotTTLExpiredException(ProtobufUtil.createSnapshotDesc(snapshotDesc));
    }
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);

    MonitoredTask status = TaskMonitor.get()
      .createStatus("Restoring  snapshot '" + snapshotName + "' to directory " + restoreDir);
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher();

    // we send createBackRefs=false so that restored hfiles do not create back reference links
    // in the base hbase root dir.
    MapreduceRestoreSnapshotHelper helper = new MapreduceRestoreSnapshotHelper(conf, fs, manifest,
      manifest.getTableDescriptor(), restoreDir, monitor, status, false);
    MapreduceRestoreSnapshotHelper.RestoreMetaChanges metaChanges = helper.restoreHdfsRegions(); // TODO: parallelize.

    if (LOG.isDebugEnabled()) {
      LOG.debug("Restored table dir:{}", restoreDir);
      CommonFSUtils.logFileSystemState(fs, restoreDir, LOG);
    }
    return metaChanges;
  }
}
