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

package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
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
  private final Path rootDir;
  private final Path tableDir;

  private final Configuration conf;
  private final FileSystem fs;
  private final boolean createBackRefs;

  public RestoreSnapshotHelper(final Configuration conf,
      final FileSystem fs,
      final SnapshotManifest manifest,
      final HTableDescriptor tableDescriptor,
      final Path rootDir,
      final ForeignExceptionDispatcher monitor,
      final MonitoredTask status) {
    this(conf, fs, manifest, tableDescriptor, rootDir, monitor, status, true);
  }

  public RestoreSnapshotHelper(final Configuration conf,
      final FileSystem fs,
      final SnapshotManifest manifest,
      final HTableDescriptor tableDescriptor,
      final Path rootDir,
      final ForeignExceptionDispatcher monitor,
      final MonitoredTask status,
      final boolean createBackRefs)
  {
    this.fs = fs;
    this.conf = conf;
    this.snapshotManifest = manifest;
    this.snapshotDesc = manifest.getSnapshotDescription();
    this.snapshotTable = TableName.valueOf(snapshotDesc.getTable());
    this.tableDesc = tableDescriptor;
    this.rootDir = rootDir;
    this.tableDir = FSUtils.getTableDir(rootDir, tableDesc.getTableName());
    this.monitor = monitor;
    this.status = status;
    this.createBackRefs = createBackRefs;
  }

  /**
   * Restore the on-disk table to a specified snapshot state.
   * @return the set of regions touched by the restore operation
   */
  public RestoreMetaChanges restoreHdfsRegions() throws IOException {
    ThreadPoolExecutor exec = SnapshotManifest.createExecutor(conf, "RestoreSnapshot");
    try {
      return restoreHdfsRegions(exec);
    } finally {
      exec.shutdown();
    }
  }

  private RestoreMetaChanges restoreHdfsRegions(final ThreadPoolExecutor exec) throws IOException {
    LOG.debug("starting restore");

    Map<String, SnapshotRegionManifest> regionManifests = snapshotManifest.getRegionManifestsMap();
    if (regionManifests == null) {
      LOG.warn("Nothing to restore. Snapshot " + snapshotDesc + " looks empty");
      return null;
    }

    RestoreMetaChanges metaChanges = new RestoreMetaChanges(parentsMap);

    // Take a copy of the manifest.keySet() since we are going to modify
    // this instance, by removing the regions already present in the restore dir.
    Set<String> regionNames = new HashSet<String>(regionManifests.keySet());

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

    return metaChanges;
  }

  /**
   * Describe the set of operations needed to update hbase:meta after restore.
   */
  public static class RestoreMetaChanges {
    private final Map<String, Pair<String, String> > parentsMap;

    private List<HRegionInfo> regionsToRestore = null;
    private List<HRegionInfo> regionsToRemove = null;
    private List<HRegionInfo> regionsToAdd = null;

    RestoreMetaChanges(final Map<String, Pair<String, String> > parentsMap) {
      this.parentsMap = parentsMap;
    }

    /**
     * @return true if there're new regions
     */
    public boolean hasRegionsToAdd() {
      return this.regionsToAdd != null && this.regionsToAdd.size() > 0;
    }

    /**
     * Returns the list of new regions added during the on-disk restore.
     * The caller is responsible to add the regions to META.
     * e.g MetaTableAccessor.addRegionsToMeta(...)
     * @return the list of regions to add to META
     */
    public List<HRegionInfo> getRegionsToAdd() {
      return this.regionsToAdd;
    }

    /**
     * @return true if there're regions to restore
     */
    public boolean hasRegionsToRestore() {
      return this.regionsToRestore != null && this.regionsToRestore.size() > 0;
    }

    /**
     * Returns the list of 'restored regions' during the on-disk restore.
     * The caller is responsible to add the regions to hbase:meta if not present.
     * @return the list of regions restored
     */
    public List<HRegionInfo> getRegionsToRestore() {
      return this.regionsToRestore;
    }

    /**
     * @return true if there're regions to remove
     */
    public boolean hasRegionsToRemove() {
      return this.regionsToRemove != null && this.regionsToRemove.size() > 0;
    }

    /**
     * Returns the list of regions removed during the on-disk restore.
     * The caller is responsible to remove the regions from META.
     * e.g. MetaTableAccessor.deleteRegions(...)
     * @return the list of regions to remove from META
     */
    public List<HRegionInfo> getRegionsToRemove() {
      return this.regionsToRemove;
    }

    void setNewRegions(final HRegionInfo[] hris) {
      if (hris != null) {
        regionsToAdd = Arrays.asList(hris);
      } else {
        regionsToAdd = null;
      }
    }

    void addRegionToRemove(final HRegionInfo hri) {
      if (regionsToRemove == null) {
        regionsToRemove = new LinkedList<HRegionInfo>();
      }
      regionsToRemove.add(hri);
    }

    void addRegionToRestore(final HRegionInfo hri) {
      if (regionsToRestore == null) {
        regionsToRestore = new LinkedList<HRegionInfo>();
      }
      regionsToRestore.add(hri);
    }

    public void updateMetaParentRegions(Connection connection,
        final List<HRegionInfo> regionInfos) throws IOException {
      if (regionInfos == null || parentsMap.isEmpty()) return;

      // Extract region names and offlined regions
      Map<String, HRegionInfo> regionsByName = new HashMap<String, HRegionInfo>(regionInfos.size());
      List<HRegionInfo> parentRegions = new LinkedList<>();
      for (HRegionInfo regionInfo: regionInfos) {
        if (regionInfo.isSplitParent()) {
          parentRegions.add(regionInfo);
        } else {
          regionsByName.put(regionInfo.getEncodedName(), regionInfo);
        }
      }

      // Update Offline parents
      for (HRegionInfo regionInfo: parentRegions) {
        Pair<String, String> daughters = parentsMap.get(regionInfo.getEncodedName());
        if (daughters == null) {
          // The snapshot contains an unreferenced region.
          // It will be removed by the CatalogJanitor.
          LOG.warn("Skip update of unreferenced offline parent: " + regionInfo);
          continue;
        }

        // One side of the split is already compacted
        if (daughters.getSecond() == null) {
          daughters.setSecond(daughters.getFirst());
        }

        LOG.debug("Update splits parent " + regionInfo.getEncodedName() + " -> " + daughters);
        MetaTableAccessor.addRegionToMeta(connection, regionInfo,
          regionsByName.get(daughters.getFirst()),
          regionsByName.get(daughters.getSecond()));
      }
    }
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
    Map<String, List<SnapshotRegionManifest.StoreFile>> snapshotFiles =
                getRegionHFileReferences(regionManifest);

    Path regionDir = new Path(tableDir, regionInfo.getEncodedName());
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
    Set<String> familyFiles = new HashSet<String>();

    FileStatus[] hfiles = FSUtils.listStatus(fs, familyDir);
    if (hfiles == null) return familyFiles;

    for (FileStatus hfileRef: hfiles) {
      String hfileName = hfileRef.getPath().getName();
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
    ModifyRegionUtils.createRegions(exec, conf, rootDir, tableDir,
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
    final Path regionDir = new Path(tableDir, region.getRegionInfo().getEncodedName());
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
   * Create a new {@link HFileLink} to reference the store file.
   * <p>The store file in the snapshot can be a simple hfile, an HFileLink or a reference.
   * <ul>
   *   <li>hfile: abc -> table=region-abc
   *   <li>reference: abc.1234 -> table=region-abc.1234
   *   <li>hfilelink: table=region-hfile -> table=region-hfile
   * </ul>
   * @param familyDir destination directory for the store file
   * @param regionInfo destination region info for the table
   * @param storeFile store file name (can be a Reference, HFileLink or simple HFile)
   * @param createBackRef - Whether back reference should be created. Defaults to true.
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
   * @param hfileName reference file name
   */
  private void restoreReferenceFile(final Path familyDir, final HRegionInfo regionInfo,
      final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
    String hfileName = storeFile.getName();

    // Extract the referred information (hfile name and parent region)
    Path refPath = StoreFileInfo.getReferredToFile(new Path(new Path(new Path(
        snapshotTable.getNameAsString(), regionInfo.getEncodedName()), familyDir.getName()),
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
      reference.write(fs, outPath);
    } else {
      InputStream in;
      if (linkPath != null) {
        in = new HFileLink(conf, linkPath).open(fs);
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
    HRegionInfo regionInfo = new HRegionInfo(tableDesc.getTableName(),
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
      HRegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir.getPath());
      regions.add(hri);
    }
    LOG.debug("found " + regions.size() + " regions for table=" +
        tableDesc.getTableName().getNameAsString());
    return regions;
  }

  /**
   * Create a new table descriptor cloning the snapshot table schema.
   *
   * @param snapshotTableDescriptor
   * @param tableName
   * @return cloned table descriptor
   * @throws IOException
   */
  public static HTableDescriptor cloneTableSchema(final HTableDescriptor snapshotTableDescriptor,
      final TableName tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (HColumnDescriptor hcd: snapshotTableDescriptor.getColumnFamilies()) {
      htd.addFamily(hcd);
    }
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        snapshotTableDescriptor.getValues().entrySet()) {
      htd.setValue(e.getKey(), e.getValue());
    }
    for (Map.Entry<String, String> e: snapshotTableDescriptor.getConfiguration().entrySet()) {
      htd.setConfiguration(e.getKey(), e.getValue());
    }
    return htd;
  }

  /**
   * Copy the snapshot files for a snapshot scanner, discards meta changes.
   * @param conf
   * @param fs
   * @param rootDir
   * @param restoreDir
   * @param snapshotName
   * @throws IOException
   */
  public static void copySnapshotForScanner(Configuration conf, FileSystem fs, Path rootDir,
      Path restoreDir, String snapshotName) throws IOException {
    // ensure that restore dir is not under root dir
    if (!restoreDir.getFileSystem(conf).getUri().equals(rootDir.getFileSystem(conf).getUri())) {
      throw new IllegalArgumentException("Filesystems for restore directory and HBase root directory " +
          "should be the same");
    }
    if (restoreDir.toUri().getPath().startsWith(rootDir.toUri().getPath())) {
      throw new IllegalArgumentException("Restore directory cannot be a sub directory of HBase " +
          "root directory. RootDir: " + rootDir + ", restoreDir: " + restoreDir);
    }

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);

    MonitoredTask status = TaskMonitor.get().createStatus(
        "Restoring  snapshot '" + snapshotName + "' to directory " + restoreDir);
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher();

    // we send createBackRefs=false so that restored hfiles do not create back reference links
    // in the base hbase root dir.
    RestoreSnapshotHelper helper = new RestoreSnapshotHelper(conf, fs,
      manifest, manifest.getTableDescriptor(), restoreDir, monitor, status, false);
    helper.restoreHdfsRegions(); // TODO: parallelize.

    if (LOG.isDebugEnabled()) {
      LOG.debug("Restored table dir:" + restoreDir);
      FSUtils.logFileSystemState(fs, restoreDir, LOG);
    }
  }
}
