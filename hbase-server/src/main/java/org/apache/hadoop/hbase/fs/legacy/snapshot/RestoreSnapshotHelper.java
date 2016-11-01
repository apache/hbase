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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.fs.RegionStorage;
import org.apache.hadoop.hbase.fs.StorageIdentifier;
import org.apache.hadoop.hbase.fs.legacy.LegacyPathIdentifier;
import org.apache.hadoop.hbase.fs.legacy.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotRestoreMetaChanges;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IOUtils;

/**
 * Helper to Restore/Clone a Snapshot
 *
 * <p>The helper assumes that a table is already created, and by calling restore()
 * the content present in the snapshot will be restored as the new content of the table.
 *
 * <p>Clone from Snapshot: If the target table is empty, the restore operation
 * is just a "clone operation", where the only operations are:
 * <ul>
 *  <li>for each region in the snapshot create a new region
 *    (note that the region will have a different name, since the encoding contains the table name)
 *  <li>for each file in the region create a new HFileLink to point to the original file.
 *  <li>restore the logs, if any
 * </ul>
 *
 * <p>Restore from Snapshot:
 * <ul>
 *  <li>for each region in the table verify which are available in the snapshot and which are not
 *    <ul>
 *    <li>if the region is not present in the snapshot, remove it.
 *    <li>if the region is present in the snapshot
 *      <ul>
 *      <li>for each file in the table region verify which are available in the snapshot
 *        <ul>
 *          <li>if the hfile is not present in the snapshot, remove it
 *          <li>if the hfile is present, keep it (nothing to do)
 *        </ul>
 *      <li>for each file in the snapshot region but not in the table
 *        <ul>
 *          <li>create a new HFileLink that point to the original file
 *        </ul>
 *      </ul>
 *    </ul>
 *  <li>for each region in the snapshot not present in the current table state
 *    <ul>
 *    <li>create a new region and for each file in the region create a new HFileLink
 *      (This is the same as the clone operation)
 *    </ul>
 *  <li>restore the logs, if any
 * </ul>
 *
 * TODO update for MasterStorage / RegionStorage
 */
@InterfaceAudience.Private
public class RestoreSnapshotHelper {
  private static final Log LOG = LogFactory.getLog(RestoreSnapshotHelper.class);

  private final Map<byte[], byte[]> regionsMap =
        new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

  private final Map<String, Pair<String, String> > parentsMap =
      new HashMap<String, Pair<String, String> >();

  private final ForeignExceptionDispatcher monitor;
  private final MonitoredTask status;

  private final SnapshotManifest snapshotManifest;
  private final SnapshotDescription snapshotDesc;
  private final TableName snapshotTable;

  private final HTableDescriptor tableDesc;
  private final Path tableDir;


  private final Configuration conf;
  private final FileSystem fs;
  private final MasterStorage<? extends StorageIdentifier> masterStorage;
  private final boolean createBackRefs;

  public RestoreSnapshotHelper(final MasterStorage<? extends StorageIdentifier> masterStorage,
      final SnapshotDescription snapshotDesc, final HTableDescriptor tableDescriptor,
      final ForeignExceptionDispatcher monitor, final MonitoredTask status) throws IOException {
    this(masterStorage, snapshotDesc, tableDescriptor, monitor, status, true);
  }

  public RestoreSnapshotHelper(final MasterStorage<? extends StorageIdentifier> masterStorage,
      final SnapshotDescription snapshotDesc, final HTableDescriptor tableDescriptor,
      final ForeignExceptionDispatcher monitor, final MonitoredTask status,
      final boolean createBackRefs) throws IOException {
    this.masterStorage = masterStorage;
    this.conf = masterStorage.getConfiguration();
    this.fs = masterStorage.getFileSystem();
    this.snapshotDesc = snapshotDesc;
    this.snapshotManifest = SnapshotManifest.open(conf, snapshotDesc);
    this.snapshotTable = TableName.valueOf(snapshotDesc.getTable());
    this.tableDesc = tableDescriptor;
    this.tableDir = FSUtils.getTableDir(FSUtils.getRootDir(conf), tableDesc.getTableName());
    this.monitor = monitor;
    this.status = status;
    this.createBackRefs = createBackRefs;
  }

  /**
   * Restore the on-disk table to a specified snapshot state.
   * @return the set of regions touched by the restore operation
   */
  public SnapshotRestoreMetaChanges restoreStorageRegions() throws IOException {
    ThreadPoolExecutor exec = SnapshotManifest.createExecutor(conf, "RestoreSnapshot");
    try {
      return restoreHdfsRegions(exec);
    } finally {
      exec.shutdown();
    }
  }

  private SnapshotRestoreMetaChanges restoreHdfsRegions(final ThreadPoolExecutor exec) throws IOException {
    LOG.info("starting restore table regions using snapshot=" + snapshotDesc);

    Map<String, SnapshotRegionManifest> regionManifests = snapshotManifest.getRegionManifestsMap();
    if (regionManifests == null) {
      LOG.warn("Nothing to restore. Snapshot " + snapshotDesc + " looks empty");
      return null;
    }

    SnapshotRestoreMetaChanges metaChanges = new SnapshotRestoreMetaChanges(tableDesc, parentsMap);

    // Take a copy of the manifest.keySet() since we are going to modify
    // this instance, by removing the regions already present in the restore dir.
    Set<String> regionNames = new HashSet<String>(regionManifests.keySet());

    HRegionInfo mobRegion = MobUtils.getMobRegionInfo(snapshotManifest.getTableDescriptor()
        .getTableName());
    // Identify which region are still available and which not.
    // NOTE: we rely upon the region name as: "table name, start key, end key"
    List<HRegionInfo> tableRegions = getTableRegions();
    if (tableRegions != null) {
      monitor.rethrowException();
      for (HRegionInfo regionInfo: tableRegions) {
        String regionName = regionInfo.getEncodedName();
        if (regionNames.contains(regionName)) {
          LOG.info("region to restore: " + regionName);
          regionNames.remove(regionName);
          metaChanges.addRegionToRestore(regionInfo);
        } else {
          LOG.info("region to remove: " + regionName);
          metaChanges.addRegionToRemove(regionInfo);
        }
      }

      // Restore regions using the snapshot data
      monitor.rethrowException();
      status.setStatus("Restoring table regions...");
      if (regionNames.contains(mobRegion.getEncodedName())) {
        // restore the mob region in case
        List<HRegionInfo> mobRegions = new ArrayList<HRegionInfo>(1);
        mobRegions.add(mobRegion);
        restoreHdfsMobRegions(exec, regionManifests, mobRegions);
        regionNames.remove(mobRegion.getEncodedName());
      }
      restoreHdfsRegions(exec, regionManifests, metaChanges.getRegionsToRestore());
      status.setStatus("Finished restoring all table regions.");

      // Remove regions from the current table
      monitor.rethrowException();
      status.setStatus("Starting to delete excess regions from table");
      removeHdfsRegions(exec, metaChanges.getRegionsToRemove());
      status.setStatus("Finished deleting excess regions from table.");
    }

    // Regions to Add: present in the snapshot but not in the current table
    if (regionNames.size() > 0) {
      List<HRegionInfo> regionsToAdd = new ArrayList<HRegionInfo>(regionNames.size());

      monitor.rethrowException();
      // add the mob region
      if (regionNames.contains(mobRegion.getEncodedName())) {
        cloneHdfsMobRegion(regionManifests, mobRegion);
        regionNames.remove(mobRegion.getEncodedName());
      }
      for (String regionName: regionNames) {
        LOG.info("region to add: " + regionName);
        regionsToAdd.add(HRegionInfo.convert(regionManifests.get(regionName).getRegionInfo()));
      }

      // Create new regions cloning from the snapshot
      monitor.rethrowException();
      status.setStatus("Cloning regions...");
      HRegionInfo[] clonedRegions = cloneHdfsRegions(exec, regionManifests, regionsToAdd);
      metaChanges.setNewRegions(clonedRegions);
      status.setStatus("Finished cloning regions.");
    }

    LOG.info("finishing restore table regions using snapshot=" + snapshotDesc);

    return metaChanges;
  }

  /**
   * Remove specified regions from the file-system, using the archiver.
   */
  private void removeHdfsRegions(final ThreadPoolExecutor exec, final List<HRegionInfo> regions)
      throws IOException {
    if (regions == null || regions.size() == 0) return;
    ModifyRegionUtils.editRegions(exec, regions, new ModifyRegionUtils.RegionEditTask() {
      @Override
      public void editRegion(final HRegionInfo hri) throws IOException {
        HFileArchiver.archiveRegion(conf, fs, hri);
      }
    });
  }

  /**
   * Restore specified regions by restoring content to the snapshot state.
   */
  private void restoreHdfsRegions(final ThreadPoolExecutor exec,
      final Map<String, SnapshotRegionManifest> regionManifests,
      final List<HRegionInfo> regions) throws IOException {
    if (regions == null || regions.size() == 0) return;
    ModifyRegionUtils.editRegions(exec, regions, new ModifyRegionUtils.RegionEditTask() {
      @Override
      public void editRegion(final HRegionInfo hri) throws IOException {
        restoreRegion(hri, regionManifests.get(hri.getEncodedName()));
      }
    });
  }

  /**
   * Restore specified mob regions by restoring content to the snapshot state.
   */
  private void restoreHdfsMobRegions(final ThreadPoolExecutor exec,
      final Map<String, SnapshotRegionManifest> regionManifests,
      final List<HRegionInfo> regions) throws IOException {
    if (regions == null || regions.size() == 0) return;
    ModifyRegionUtils.editRegions(exec, regions, new ModifyRegionUtils.RegionEditTask() {
      @Override
      public void editRegion(final HRegionInfo hri) throws IOException {
        restoreMobRegion(hri, regionManifests.get(hri.getEncodedName()));
      }
    });
  }

  private Map<String, List<SnapshotRegionManifest.StoreFile>> getRegionHFileReferences(
      final SnapshotRegionManifest manifest) {
    Map<String, List<SnapshotRegionManifest.StoreFile>> familyMap =
      new HashMap<String, List<SnapshotRegionManifest.StoreFile>>(manifest.getFamilyFilesCount());
    for (SnapshotRegionManifest.FamilyFiles familyFiles: manifest.getFamilyFilesList()) {
      familyMap.put(familyFiles.getFamilyName().toStringUtf8(),
        new ArrayList<SnapshotRegionManifest.StoreFile>(familyFiles.getStoreFilesList()));
    }
    return familyMap;
  }

  /**
   * Restore region by removing files not in the snapshot
   * and adding the missing ones from the snapshot.
   */
  private void restoreRegion(final HRegionInfo regionInfo,
      final SnapshotRegionManifest regionManifest) throws IOException {
    restoreRegion(regionInfo, regionManifest, new Path(tableDir, regionInfo.getEncodedName()));
  }

  /**
   * Restore mob region by removing files not in the snapshot
   * and adding the missing ones from the snapshot.
   */
  private void restoreMobRegion(final HRegionInfo regionInfo,
      final SnapshotRegionManifest regionManifest) throws IOException {
    if (regionManifest == null) {
      return;
    }
    restoreRegion(regionInfo, regionManifest,
      MobUtils.getMobRegionPath(conf, tableDesc.getTableName()));
  }

  /**
   * Restore region by removing files not in the snapshot
   * and adding the missing ones from the snapshot.
   */
  private void restoreRegion(final HRegionInfo regionInfo,
      final SnapshotRegionManifest regionManifest, Path regionDir) throws IOException {
    Map<String, List<SnapshotRegionManifest.StoreFile>> snapshotFiles =
                getRegionHFileReferences(regionManifest);

    String tableName = tableDesc.getTableName().getNameAsString();

    // Restore families present in the table
    for (Path familyDir: FSUtils.getFamilyDirs(fs, regionDir)) {
      byte[] family = Bytes.toBytes(familyDir.getName());
      Set<String> familyFiles = getTableRegionFamilyFiles(familyDir);
      List<SnapshotRegionManifest.StoreFile> snapshotFamilyFiles =
          snapshotFiles.remove(familyDir.getName());
      if (snapshotFamilyFiles != null) {
        List<SnapshotRegionManifest.StoreFile> hfilesToAdd =
            new ArrayList<SnapshotRegionManifest.StoreFile>();
        for (SnapshotRegionManifest.StoreFile storeFile: snapshotFamilyFiles) {
          if (familyFiles.contains(storeFile.getName())) {
            // HFile already present
            familyFiles.remove(storeFile.getName());
          } else {
            // HFile missing
            hfilesToAdd.add(storeFile);
          }
        }

        // Remove hfiles not present in the snapshot
        for (String hfileName: familyFiles) {
          Path hfile = new Path(familyDir, hfileName);
          LOG.trace("Removing hfile=" + hfileName +
            " from region=" + regionInfo.getEncodedName() + " table=" + tableName);
          HFileArchiver.archiveStoreFile(conf, fs, regionInfo, tableDir, family, hfile);
        }

        // Restore Missing files
        for (SnapshotRegionManifest.StoreFile storeFile: hfilesToAdd) {
          LOG.debug("Adding HFileLink " + storeFile.getName() +
            " to region=" + regionInfo.getEncodedName() + " table=" + tableName);
          restoreStoreFile(familyDir, regionInfo, storeFile, createBackRefs);
        }
      } else {
        // Family doesn't exists in the snapshot
        LOG.trace("Removing family=" + Bytes.toString(family) +
          " from region=" + regionInfo.getEncodedName() + " table=" + tableName);
        HFileArchiver.archiveFamily(fs, conf, regionInfo, tableDir, family);
        fs.delete(familyDir, true);
      }
    }

    // Add families not present in the table
    for (Map.Entry<String, List<SnapshotRegionManifest.StoreFile>> familyEntry:
                                                                      snapshotFiles.entrySet()) {
      Path familyDir = new Path(regionDir, familyEntry.getKey());
      if (!fs.mkdirs(familyDir)) {
        throw new IOException("Unable to create familyDir=" + familyDir);
      }

      for (SnapshotRegionManifest.StoreFile storeFile: familyEntry.getValue()) {
        LOG.trace("Adding HFileLink " + storeFile.getName() + " to table=" + tableName);
        restoreStoreFile(familyDir, regionInfo, storeFile, createBackRefs);
      }
    }
  }

  /**
   * @return The set of files in the specified family directory.
   */
  private Set<String> getTableRegionFamilyFiles(final Path familyDir) throws IOException {
    FileStatus[] hfiles = FSUtils.listStatus(fs, familyDir);
    if (hfiles == null) return Collections.emptySet();

    Set<String> familyFiles = new HashSet<String>(hfiles.length);
    for (int i = 0; i < hfiles.length; ++i) {
      String hfileName = hfiles[i].getPath().getName();
      familyFiles.add(hfileName);
    }

    return familyFiles;
  }

  /**
   * Clone specified regions. For each region create a new region
   * and create a HFileLink for each hfile.
   */
  private HRegionInfo[] cloneHdfsRegions(final ThreadPoolExecutor exec,
      final Map<String, SnapshotRegionManifest> regionManifests,
      final List<HRegionInfo> regions) throws IOException {
    if (regions == null || regions.size() == 0) return null;

    final Map<String, HRegionInfo> snapshotRegions =
      new HashMap<String, HRegionInfo>(regions.size());

    // clone region info (change embedded tableName with the new one)
    HRegionInfo[] clonedRegionsInfo = new HRegionInfo[regions.size()];
    for (int i = 0; i < clonedRegionsInfo.length; ++i) {
      // clone the region info from the snapshot region info
      HRegionInfo snapshotRegionInfo = regions.get(i);
      clonedRegionsInfo[i] = cloneRegionInfo(snapshotRegionInfo);

      // add the region name mapping between snapshot and cloned
      String snapshotRegionName = snapshotRegionInfo.getEncodedName();
      String clonedRegionName = clonedRegionsInfo[i].getEncodedName();
      regionsMap.put(Bytes.toBytes(snapshotRegionName), Bytes.toBytes(clonedRegionName));
      LOG.info("clone region=" + snapshotRegionName + " as " + clonedRegionName);

      // Add mapping between cloned region name and snapshot region info
      snapshotRegions.put(clonedRegionName, snapshotRegionInfo);
    }

    // create the regions on disk
    ModifyRegionUtils.createRegions(exec, conf,
        tableDesc, clonedRegionsInfo, new ModifyRegionUtils.RegionFillTask() {
        @Override
        public void fillRegion(final HRegion region) throws IOException {
          HRegionInfo snapshotHri = snapshotRegions.get(region.getRegionInfo().getEncodedName());
          cloneRegion(region, snapshotHri, regionManifests.get(snapshotHri.getEncodedName()));
        }
      });

    return clonedRegionsInfo;
  }

  /**
   * Clone the mob region. For the region create a new region
   * and create a HFileLink for each hfile.
   */
  private void cloneHdfsMobRegion(final Map<String, SnapshotRegionManifest> regionManifests,
      final HRegionInfo region) throws IOException {
    // clone region info (change embedded tableName with the new one)
    Path clonedRegionPath = MobUtils.getMobRegionPath(conf, tableDesc.getTableName());
    cloneRegion(clonedRegionPath, region, regionManifests.get(region.getEncodedName()));
  }

  /**
   * Clone region directory content from the snapshot info.
   *
   * Each region is encoded with the table name, so the cloned region will have
   * a different region name.
   *
   * Instead of copying the hfiles a HFileLink is created.
   *
   * @param regionDir {@link Path} cloned dir
   * @param snapshotRegionInfo
   */
  private void cloneRegion(final Path regionDir, final HRegionInfo snapshotRegionInfo,
      final SnapshotRegionManifest manifest) throws IOException {
    final String tableName = tableDesc.getTableName().getNameAsString();
    for (SnapshotRegionManifest.FamilyFiles familyFiles: manifest.getFamilyFilesList()) {
      Path familyDir = new Path(regionDir, familyFiles.getFamilyName().toStringUtf8());
      for (SnapshotRegionManifest.StoreFile storeFile: familyFiles.getStoreFilesList()) {
        LOG.info("Adding HFileLink " + storeFile.getName() + " to table=" + tableName);
        restoreStoreFile(familyDir, snapshotRegionInfo, storeFile, createBackRefs);
      }
    }
  }

  /**
   * Clone region directory content from the snapshot info.
   *
   * Each region is encoded with the table name, so the cloned region will have
   * a different region name.
   *
   * Instead of copying the hfiles a HFileLink is created.
   *
   * @param region {@link HRegion} cloned
   * @param snapshotRegionInfo
   */
  private void cloneRegion(final HRegion region, final HRegionInfo snapshotRegionInfo,
      final SnapshotRegionManifest manifest) throws IOException {
    cloneRegion(new Path(tableDir, region.getRegionInfo().getEncodedName()), snapshotRegionInfo,
      manifest);
  }

  /**
   * Create a new {@link HFileLink} to reference the store file.
   * <p>The store file in the snapshot can be a simple hfile, an HFileLink or a reference.
   * <ul>
   *   <li>hfile: abc -> table=region-abc
   *   <li>reference: abc.1234 -> table=region-abc.1234
   *   <li>hfilelink: table=region-hfile -> table=region-hfile
   * </ul>
   * @param familyDir destination directory for the store file
   * @param regionInfo destination region info for the table
   * @param createBackRef - Whether back reference should be created. Defaults to true.
   * @param storeFile store file name (can be a Reference, HFileLink or simple HFile)
   */
  private void restoreStoreFile(final Path familyDir, final HRegionInfo regionInfo,
      final SnapshotRegionManifest.StoreFile storeFile, final boolean createBackRef)
          throws IOException {
    String hfileName = storeFile.getName();
    if (HFileLink.isHFileLink(hfileName)) {
      HFileLink.createFromHFileLink(conf, fs, familyDir, hfileName, createBackRef);
    } else if (StoreFileInfo.isReference(hfileName)) {
      restoreReferenceFile(familyDir, regionInfo, storeFile);
    } else {
      HFileLink.create(conf, fs, familyDir, regionInfo, hfileName, createBackRef);
    }
  }

  /**
   * Create a new {@link Reference} as copy of the source one.
   * <p><blockquote><pre>
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
   * </pre></blockquote>
   * @param familyDir destination directory for the store file
   * @param regionInfo destination region info for the table
   * @param storeFile reference file name
   */
  private void restoreReferenceFile(final Path familyDir, final HRegionInfo regionInfo,
      final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
    String hfileName = storeFile.getName();

    // Extract the referred information (hfile name and parent region)
    Path refPath =
        StoreFileInfo.getReferredToFile(new Path(new Path(new Path(new Path(snapshotTable
            .getNamespaceAsString(), snapshotTable.getQualifierAsString()), regionInfo
            .getEncodedName()), familyDir.getName()), hfileName));
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
      reference.write(fs, outPath);
    } else {
      InputStream in;
      if (linkPath != null) {
        in = HFileLink.buildFromHFileLinkPattern(conf, linkPath).open(fs);
      } else {
        linkPath = new Path(new Path(HRegion.getRegionDir(snapshotManifest.getSnapshotDir(),
                        regionInfo.getEncodedName()), familyDir.getName()), hfileName);
        in = fs.open(linkPath);
      }
      OutputStream out = fs.create(outPath);
      IOUtils.copyBytes(in, out, conf);
    }

    // Add the daughter region to the map
    String regionName = Bytes.toString(regionsMap.get(regionInfo.getEncodedNameAsBytes()));
    LOG.debug("Restore reference " + regionName + " to " + clonedRegionName);
    synchronized (parentsMap) {
      Pair<String, String> daughters = parentsMap.get(clonedRegionName);
      if (daughters == null) {
        daughters = new Pair<String, String>(regionName, null);
        parentsMap.put(clonedRegionName, daughters);
      } else if (!regionName.equals(daughters.getFirst())) {
        daughters.setSecond(regionName);
      }
    }
  }

  /**
   * Create a new {@link HRegionInfo} from the snapshot region info.
   * Keep the same startKey, endKey, regionId and split information but change
   * the table name.
   *
   * @param snapshotRegionInfo Info for region to clone.
   * @return the new HRegion instance
   */
  public HRegionInfo cloneRegionInfo(final HRegionInfo snapshotRegionInfo) {
    return cloneRegionInfo(tableDesc.getTableName(), snapshotRegionInfo);
  }

  public static HRegionInfo cloneRegionInfo(TableName tableName, HRegionInfo snapshotRegionInfo) {
    HRegionInfo regionInfo = new HRegionInfo(tableName,
                      snapshotRegionInfo.getStartKey(), snapshotRegionInfo.getEndKey(),
                      snapshotRegionInfo.isSplit(), snapshotRegionInfo.getRegionId());
    regionInfo.setOffline(snapshotRegionInfo.isOffline());
    return regionInfo;
  }

  /**
   * @return the set of the regions contained in the table
   */
  private List<HRegionInfo> getTableRegions() throws IOException {
    LOG.debug("get table regions: " + tableDir);
    FileStatus[] regionDirs = FSUtils.listStatus(fs, tableDir, new FSUtils.RegionDirFilter(fs));
    if (regionDirs == null) return null;

    List<HRegionInfo> regions = new LinkedList<HRegionInfo>();
    for (FileStatus regionDir: regionDirs) {
      final RegionStorage rs = RegionStorage.open(conf, new LegacyPathIdentifier(regionDir.getPath()), false);
      regions.add(rs.getRegionInfo());
    }
    LOG.debug("found " + regions.size() + " regions for table=" +
        tableDesc.getTableName().getNameAsString());
    return regions;
  }

  /**
   * Copy the snapshot files for a snapshot scanner, discards meta changes.
   * @param masterStorage the {@link MasterStorage} to use
   * @param restoreDir
   * @param snapshotName
   * @throws IOException
   */
  public static SnapshotRestoreMetaChanges copySnapshotForScanner(
      final MasterStorage<? extends StorageIdentifier> masterStorage, Path restoreDir,
      String snapshotName) throws IOException {
    Configuration conf = masterStorage.getConfiguration();
    Path rootDir = ((LegacyPathIdentifier)masterStorage.getRootContainer()).path;
    // ensure that restore dir is not under root dir
    if (!restoreDir.getFileSystem(conf).getUri().equals(rootDir.getFileSystem(conf).getUri())) {
      throw new IllegalArgumentException("Filesystems for restore directory and HBase root " +
          "directory should be the same");
    }
    if (restoreDir.toUri().getPath().startsWith(rootDir.toUri().getPath())) {
      throw new IllegalArgumentException("Restore directory cannot be a sub directory of HBase " +
          "root directory. RootDir: " + rootDir + ", restoreDir: " + restoreDir);
    }

    SnapshotDescription snapshotDesc = masterStorage.getSnapshot(snapshotName);
    HTableDescriptor htd = masterStorage.getTableDescriptorForSnapshot(snapshotDesc);

    MonitoredTask status = TaskMonitor.get().createStatus(
        "Restoring  snapshot '" + snapshotName + "' to directory " + restoreDir);
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher();

    // we send createBackRefs=false so that restored hfiles do not create back reference links
    // in the base hbase root dir.
    RestoreSnapshotHelper helper = new RestoreSnapshotHelper(masterStorage, snapshotDesc, htd,
        monitor, status, false);
    SnapshotRestoreMetaChanges metaChanges = helper.restoreStorageRegions(); // TODO: parallelize.

    if (LOG.isDebugEnabled()) {
      LOG.debug("Restored table dir:" + restoreDir);
      FSUtils.logFileSystemState(masterStorage.getFileSystem(), restoreDir, LOG);
    }
    return metaChanges;
  }
}
