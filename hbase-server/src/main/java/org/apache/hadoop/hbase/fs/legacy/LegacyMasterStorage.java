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

package org.apache.hadoop.hbase.fs.legacy;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.fs.legacy.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.fs.legacy.cleaner.HFileLinkCleaner;
import org.apache.hadoop.hbase.fs.legacy.cleaner.LogCleaner;
import org.apache.hadoop.hbase.fs.legacy.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.fs.legacy.snapshot.SnapshotHFileCleaner;
import org.apache.hadoop.hbase.fs.legacy.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotRestoreMetaChanges;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.fs.StorageContext;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MetaUtils;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.backup.HFileArchiver;

@InterfaceAudience.Private
public class LegacyMasterStorage extends MasterStorage<LegacyPathIdentifier> {
  // TODO: Modify all APIs to use ExecutorService and support parallel HDFS queries

  private static final Log LOG = LogFactory.getLog(LegacyMasterStorage.class);

  private final Path sidelineDir;
  private final Path snapshotDir;
  private final Path tmpSnapshotDir;
  private final Path archiveDataDir;
  private final Path archiveDir;
  private final Path tmpDataDir;
  private final Path dataDir;
  private final Path tmpDir;
  private final Path bulkDir;

  /*
   * In a secure env, the protected sub-directories and files under the HBase rootDir
   * would be restricted. The sub-directory will have '700' except the bulk load staging dir,
   * which will have '711'.  The default '700' can be overwritten by setting the property
   * 'hbase.rootdir.perms'. The protected files (version file, clusterId file) will have '600'.
   * The rootDir itself will be created with HDFS default permissions if it does not exist.
   * We will check the rootDir permissions to make sure it has 'x' for all to ensure access
   * to the staging dir. If it does not, we will add it.
   */
  // Permissions for the directories under rootDir that need protection
  private final FsPermission secureRootSubDirPerms;
  // Permissions for the files under rootDir that need protection
  private final FsPermission secureRootFilePerms = new FsPermission("600");
  // Permissions for bulk load staging directory under rootDir
  private final FsPermission HiddenDirPerms = FsPermission.valueOf("-rwx--x--x");

  private final boolean isSecurityEnabled;

  public static final String SPLITTING_EXT = "-splitting";

  public LegacyMasterStorage(Configuration conf, FileSystem fs, LegacyPathIdentifier rootDir) {
    super(conf, fs, rootDir);

    // base directories
    this.sidelineDir = LegacyLayout.getSidelineDir(rootDir.path);
    this.snapshotDir = LegacyLayout.getSnapshotDir(rootDir.path);
    this.tmpSnapshotDir = LegacyLayout.getWorkingSnapshotDir(rootDir.path);
    this.archiveDir = LegacyLayout.getArchiveDir(rootDir.path);
    this.archiveDataDir = LegacyLayout.getDataDir(this.archiveDir);
    this.dataDir = LegacyLayout.getDataDir(rootDir.path);
    this.tmpDir = LegacyLayout.getTempDir(rootDir.path);
    this.tmpDataDir = LegacyLayout.getDataDir(this.tmpDir);
    this.bulkDir = LegacyLayout.getBulkDir(rootDir.path);

    this.secureRootSubDirPerms = new FsPermission(conf.get("hbase.rootdir.perms", "700"));
    this.isSecurityEnabled = "kerberos".equalsIgnoreCase(conf.get("hbase.security.authentication"));
  }

  @Override
  public Iterable<ScheduledChore> getChores(Stoppable stopper, Map<String, Object> params) {
    ArrayList<ScheduledChore> chores = (ArrayList<ScheduledChore>) super.getChores(stopper, params);

    int cleanerInterval = getConfiguration().getInt("hbase.master.cleaner.interval", 60 * 1000);
    // add log cleaner chore
    chores.add(new LogCleaner(cleanerInterval, stopper, getConfiguration(), getFileSystem(),
        LegacyLayout.getOldLogDir(getRootContainer().path)));
    // add hfile archive cleaner chore
    chores.add(new HFileCleaner(cleanerInterval, stopper, getConfiguration(), getFileSystem(),
        LegacyLayout.getArchiveDir(getRootContainer().path), params));

    return chores;
  }

  // ==========================================================================
  //  PUBLIC Methods - Namespace related
  // ==========================================================================
  public void createNamespace(NamespaceDescriptor nsDescriptor) throws IOException {
    getFileSystem().mkdirs(getNamespaceDir(StorageContext.DATA, nsDescriptor.getName()));
  }

  public void deleteNamespace(String namespaceName) throws IOException {
    FileSystem fs = getFileSystem();
    Path nsDir = getNamespaceDir(StorageContext.DATA, namespaceName);

    try {
      for (FileStatus status : fs.listStatus(nsDir)) {
        if (!HConstants.HBASE_NON_TABLE_DIRS.contains(status.getPath().getName())) {
          throw new IOException("Namespace directory contains table dir: " + status.getPath());
        }
      }
      if (!fs.delete(nsDir, true)) {
        throw new IOException("Failed to remove namespace: " + namespaceName);
      }
    } catch (FileNotFoundException e) {
      // File already deleted, continue
      LOG.debug("deleteDirectory throws exception: " + e);
    }
  }

  public Collection<String> getNamespaces(StorageContext ctx) throws IOException {
    FileStatus[] stats = FSUtils.listStatus(getFileSystem(), getNamespaceDir(ctx));
    if (stats == null) return Collections.emptyList();

    ArrayList<String> namespaces = new ArrayList<String>(stats.length);
    for (int i = 0; i < stats.length; ++i) {
      namespaces.add(stats[i].getPath().getName());
    }
    return namespaces;
  }

  // should return or get a NamespaceDescriptor? how is that different from HTD?

  // ==========================================================================
  //  PUBLIC Methods - Table Descriptor related
  // ==========================================================================s
  @Override
  public boolean createTableDescriptor(StorageContext ctx, HTableDescriptor tableDesc, boolean force)
      throws IOException {
    return LegacyTableDescriptor.createTableDescriptor(getFileSystem(),
      getTableDir(ctx, tableDesc.getTableName()), tableDesc, force);
  }

  @Override
  public void updateTableDescriptor(StorageContext ctx, HTableDescriptor tableDesc) throws IOException {
    LegacyTableDescriptor.updateTableDescriptor(getFileSystem(),
        getTableDir(ctx, tableDesc.getTableName()), tableDesc);
  }

  @Override
  public HTableDescriptor getTableDescriptor(StorageContext ctx, TableName tableName)
      throws IOException {
    return LegacyTableDescriptor.getTableDescriptorFromFs(
        getFileSystem(), getTableDir(ctx, tableName));
  }

  // ==========================================================================
  //  PUBLIC Methods - Table related
  // ==========================================================================
  @Override
  public void deleteTable(StorageContext ctx, TableName tableName, boolean archive)
      throws IOException {
    if (archive) {
      archiveTable(ctx, tableName);
    }

    Path tableDir = getTableDir(ctx, tableName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting table '" + tableName + "' from '" + tableDir + "'.");
    }
    if (!FSUtils.deleteDirectory(getFileSystem(), tableDir)) {
      throw new IOException("Failed delete of " + tableName);
    }

    Path mobTableDir = LegacyLayout.getMobTableDir(getRootContainer().path, tableName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting MOB data '" + mobTableDir + "'.");
    }
    if (!FSUtils.deleteDirectory(getFileSystem(), mobTableDir)) {
      throw new IOException("Failed delete MOB data of table " + tableName);
    }
  }

  @Override
  public Collection<TableName> getTables(StorageContext ctx, String namespace)
      throws IOException {
    FileStatus[] stats = FSUtils.listStatus(getFileSystem(),
        getNamespaceDir(ctx, namespace), new FSUtils.UserTableDirFilter(getFileSystem()));
    if (stats == null) return Collections.emptyList();

    ArrayList<TableName> tables = new ArrayList<TableName>(stats.length);
    for (int i = 0; i < stats.length; ++i) {
      tables.add(TableName.valueOf(namespace, stats[i].getPath().getName()));
    }
    return tables;
  }

  @Override
  public void archiveTable(StorageContext ctx, TableName tableName) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Archiving table '" + tableName + "' from storage");
    }

    // archive all regions
    for (HRegionInfo hri : getRegions(ctx, tableName)) {
      archiveRegion(hri);
    }

    // archive MOB data
    Path mobTableDir = LegacyLayout.getMobTableDir(getRootContainer().path, tableName);
    Path mobRegionDir = new Path(mobTableDir,
        MobUtils.getMobRegionInfo(tableName).getEncodedName());
    if (getFileSystem().exists(mobRegionDir)) {
      HFileArchiver.archiveRegion(getFileSystem(), getRootContainer().path, mobTableDir,
          mobRegionDir);
    }
  }

  // ==========================================================================
  //  PUBLIC Methods - Table Regions related
  // ==========================================================================
  @Override
  public Collection<HRegionInfo> getRegions(StorageContext ctx, TableName tableName)
      throws IOException {
    FileStatus[] stats = FSUtils.listStatus(getFileSystem(),
        getTableDir(ctx, tableName), new FSUtils.RegionDirFilter(getFileSystem()));
    if (stats == null) return Collections.emptyList();

    ArrayList<HRegionInfo> regions = new ArrayList<HRegionInfo>(stats.length);
    for (int i = 0; i < stats.length; ++i) {
      regions.add(loadRegionInfo(stats[i].getPath()));
    }
    return regions;
  }

  protected HRegionInfo loadRegionInfo(Path regionDir) throws IOException {
    FSDataInputStream in = getFileSystem().open(LegacyLayout.getRegionInfoFile(regionDir));
    try {
      return HRegionInfo.parseFrom(in);
    } finally {
      in.close();
    }
  }

  @Override
  public HRegionInfo getRegion(StorageContext ctx, TableName tableName, String encodedName)
      throws IOException {
    Path regionDir = LegacyLayout.getRegionDir(getTableDir(ctx, tableName), encodedName);
    return loadRegionInfo(regionDir);
  }

  /**
   * Archives the specified region's storage artifacts (files, directories etc)
   * @param regionInfo
   * @throws IOException
   */
  @Override
  public void archiveRegion(HRegionInfo regionInfo) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Archiving region '" + regionInfo.getRegionNameAsString() + "' from storage.");
    }
    HFileArchiver.archiveRegion(getConfiguration(), getFileSystem(), regionInfo);
  }

  // ==========================================================================
  //  Methods - Snapshot related
  // ==========================================================================

  /**
   * Filter that only accepts completed snapshot directories
   */
  public static class CompletedSnapshotDirectoriesFilter extends FSUtils.BlackListDirFilter {
    /**
     * @param fs
     */
    public CompletedSnapshotDirectoriesFilter(FileSystem fs) {
      super(fs, Collections.singletonList(LegacyLayout.SNAPSHOT_TMP_DIR_NAME));
    }
  }

  /**
   * This method modifies chores configuration for snapshots. Please call this method before
   * instantiating and scheduling list of chores with {@link #getChores(Stoppable, Map)}.
   */
  @Override
  public void enableSnapshots() throws IOException {
    super.enableSnapshots();

    // check if an older version of snapshot directory was present
    Path oldSnapshotDir = new Path(getRootContainer().path, HConstants.OLD_SNAPSHOT_DIR_NAME);
    List<SnapshotDescription> oldSnapshots = getSnapshotDescriptions(oldSnapshotDir,
        new CompletedSnapshotDirectoriesFilter(getFileSystem()));
    if (oldSnapshots != null && !oldSnapshots.isEmpty()) {
      LOG.error("Snapshots from an earlier release were found under '" + oldSnapshotDir + "'.");
      LOG.error("Please rename the directory ");
    }

    // TODO: add check for old snapshot dir that existed just before HBASE-14439

    if (!isSnapshotsEnabled()) {
      // Extract cleaners from conf
      Set<String> hfileCleaners = new HashSet<>();
      String[] cleaners = getConfiguration().getStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
      if (cleaners != null) Collections.addAll(hfileCleaners, cleaners);

      // add snapshot related cleaners
      hfileCleaners.add(SnapshotHFileCleaner.class.getName());
      hfileCleaners.add(HFileLinkCleaner.class.getName());

      // Set cleaners conf
      getConfiguration().setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
          hfileCleaners.toArray(new String[hfileCleaners.size()]));
    }
  }

  @Override
  public boolean isSnapshotsEnabled() {
    // Extract cleaners from conf
    Set<String> hfileCleaners = new HashSet<>();
    String[] cleaners = getConfiguration().getStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
    if (cleaners != null) Collections.addAll(hfileCleaners, cleaners);
    return hfileCleaners.contains(SnapshotHFileCleaner.class.getName()) &&
        hfileCleaners.contains(HFileLinkCleaner.class.getName());
  }

  private List<SnapshotDescription> getSnapshotDescriptions(final Path dir,
      final PathFilter filter) throws IOException {
    List<SnapshotDescription> snapshotDescs = new ArrayList<>();
    if (!FSUtils.isExists(getFileSystem(), dir)) {
      return snapshotDescs;
    }

    for (FileStatus fileStatus : FSUtils.listStatus(getFileSystem(), dir, filter)) {
      Path info = new Path(fileStatus.getPath(), LegacyLayout.SNAPSHOTINFO_FILE);
      if (!FSUtils.isExists(getFileSystem(), info)) {
        LOG.error("Snapshot information for '" + fileStatus.getPath() + "' doesn't exist!");
        continue;
      }

      FSDataInputStream in = null;
      try {
        in = getFileSystem().open(info);
        SnapshotDescription desc = SnapshotDescription.parseFrom(in);
        snapshotDescs.add(desc);
      } catch (IOException e) {
        LOG.warn("Found a corrupted snapshot '" + fileStatus.getPath() + "'.", e);
      } finally {
        if (in != null) {
          in.close();
        }
      }
    }
    return snapshotDescs;
  }

  @Override
  public List<SnapshotDescription> getSnapshots(StorageContext ctx) throws IOException {
    return getSnapshotDescriptions(getSnapshotDirFromContext(ctx),
        new CompletedSnapshotDirectoriesFilter(getFileSystem()));
  }

  @Override
  public SnapshotDescription getSnapshot(String snapshotName, StorageContext ctx)
      throws IOException {
    SnapshotDescription retSnapshot = null;

    Path snapshotDir = getSnapshotDirFromContext(ctx, snapshotName);
    Path info = new Path(snapshotDir, LegacyLayout.SNAPSHOTINFO_FILE);
    if (!FSUtils.isExists(getFileSystem(), info)) {
      LOG.warn("Snapshot information for '" + snapshotName + "' doesn't exist!");
      return retSnapshot;
    }

    FSDataInputStream in = null;
    try {
      in = getFileSystem().open(info);
      retSnapshot = SnapshotDescription.parseFrom(in);
    } catch (IOException e) {
      LOG.warn("Found a corrupted snapshot '" + snapshotName + "'.", e);
    } finally {
      if (in != null) {
        in.close();
      }
    }

    return retSnapshot;
  }

  @Override
  public void visitSnapshots(final SnapshotVisitor visitor) throws IOException {
    visitSnapshots(StorageContext.DATA, visitor);
  }

  @Override
  public void visitSnapshots(StorageContext ctx, final SnapshotVisitor visitor) throws IOException {
    for (SnapshotDescription s : getSnapshots(ctx)) {
      visitor.visitSnapshot(s.getName(), s, ctx);
    }
  }

  private SnapshotManifest getSnapshotManifest(SnapshotDescription snapshot, StorageContext ctx)
    throws IOException {
    Path snapshotDir = getSnapshotDirFromContext(ctx, snapshot.getName());
    return SnapshotManifest.open(getConfiguration(), getFileSystem(), snapshotDir, snapshot);
  }

  @Override
  public HTableDescriptor getTableDescriptorForSnapshot(SnapshotDescription snapshot,
      StorageContext ctx) throws IOException {
    SnapshotManifest manifest = getSnapshotManifest(snapshot, ctx);
    return manifest.getTableDescriptor();
  }

  private List<SnapshotRegionManifest> getSnapshotRegionManifests(SnapshotDescription snapshot,
      StorageContext ctx) throws IOException {
    SnapshotManifest manifest = getSnapshotManifest(snapshot, ctx);
    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (regionManifests == null) {
      regionManifests = new ArrayList<>();
    }
    return regionManifests;
  }

  @Override
  public Map<String, HRegionInfo> getSnapshotRegions(SnapshotDescription snapshot,
      StorageContext ctx) throws IOException {
    Map<String, HRegionInfo> retRegions = new HashMap<>();
    for (SnapshotRegionManifest regionManifest: getSnapshotRegionManifests(snapshot, ctx)) {
      HRegionInfo hri = HRegionInfo.convert(regionManifest.getRegionInfo());
      retRegions.put(hri.getEncodedName(), hri);
    }
    return retRegions;
  }

  /**
   * Utility function for visiting/ listing store files for a snapshot.
   * @param snapshot
   * @param ctx
   * @param regionName If not null, then store files for the matching region are visited/ returned
   * @param familyName If not null, then store files for the matching family are visited/ returned
   * @param visitor If not null, visitor is call on each store file entry
   * @return List of store files base on suggested filters
   * @throws IOException
   */
  private List<SnapshotRegionManifest.StoreFile> visitAndGetSnapshotStoreFiles(
      SnapshotDescription snapshot, StorageContext ctx, String regionName, String familyName,
      SnapshotStoreFileVisitor visitor) throws IOException {
    List<SnapshotRegionManifest.StoreFile> snapshotStoreFiles = new ArrayList<>();

    for (SnapshotRegionManifest regionManifest: getSnapshotRegionManifests(snapshot, ctx)) {
      HRegionInfo hri = HRegionInfo.convert(regionManifest.getRegionInfo());

      // check for region name
      if (regionName != null) {
        if (!hri.getEncodedName().equals(regionName)) {
          continue;
        }
      }

      for (SnapshotRegionManifest.FamilyFiles familyFiles: regionManifest.getFamilyFilesList()) {
        String family = familyFiles.getFamilyName().toStringUtf8();
        // check for family name
        if (familyName != null && !familyName.equals(family)) {
          continue;
        }

        List<SnapshotRegionManifest.StoreFile> storeFiles = familyFiles.getStoreFilesList();
        snapshotStoreFiles.addAll(storeFiles);

        if (visitor != null) {
          for(SnapshotRegionManifest.StoreFile storeFile: storeFiles) {
            visitor.visitSnapshotStoreFile(snapshot, ctx, hri, family, storeFile);
          }
        }
      }
    }

    return snapshotStoreFiles;
  }

  @Override
  public void visitSnapshotStoreFiles(SnapshotDescription snapshot, StorageContext ctx,
      SnapshotStoreFileVisitor visitor) throws IOException {
    visitAndGetSnapshotStoreFiles(snapshot, ctx, null, null, visitor);
  }

  @Override
  public boolean snapshotExists(SnapshotDescription snapshot, StorageContext ctx)
      throws IOException {
    return snapshotExists(snapshot.getName(), ctx);
  }

  @Override
  public boolean snapshotExists(String snapshotName, StorageContext ctx) throws IOException {
    return getSnapshot(snapshotName, ctx) != null;
  }

  @Override
  public void deleteAllSnapshots(StorageContext ctx) throws IOException {
    Path snapshotDir = getSnapshotDirFromContext(ctx);
    if (!FSUtils.deleteDirectory(getFileSystem(), snapshotDir)) {
      LOG.warn("Couldn't delete working snapshot directory '" + snapshotDir + ".");
    }
  }

  private void deleteSnapshotDir(Path snapshotDir) throws IOException {
    LOG.debug("Deleting snapshot directory '" + snapshotDir + "'.");
    if (!FSUtils.deleteDirectory(getFileSystem(), snapshotDir)) {
      throw new HBaseSnapshotException("Failed to delete snapshot directory '" +
          snapshotDir + "'.");
    }
  }

  @Override
  public boolean deleteSnapshot(final SnapshotDescription snapshot, final StorageContext ctx)
      throws IOException {
    return deleteSnapshot(snapshot.getName(), ctx);
  }

  @Override
  public boolean deleteSnapshot(final String snapshotName, final StorageContext ctx)
      throws IOException {
    deleteSnapshotDir(getSnapshotDirFromContext(ctx, snapshotName));
    return false;
  }

  @Override
  public void prepareSnapshot(SnapshotDescription snapshot) throws IOException {
    if (snapshot == null) return;
    deleteSnapshot(snapshot);
    Path snapshotDir = getSnapshotDirFromContext(StorageContext.TEMP, snapshot.getName());
    if (getFileSystem().mkdirs(snapshotDir)) {
      throw new SnapshotCreationException("Couldn't create working directory '" + snapshotDir +
          "' for snapshot", ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  @Override
  public void initiateSnapshot(HTableDescriptor htd, SnapshotDescription snapshot,
                               final ForeignExceptionSnare monitor, StorageContext ctx) throws IOException {
    Path snapshotDir = getSnapshotDirFromContext(ctx, snapshot.getName());

    // write down the snapshot info in the working directory
    writeSnapshotInfo(snapshot, snapshotDir);

    // create manifest
    SnapshotManifest manifest = SnapshotManifest.create(getConfiguration(), getFileSystem(),
        snapshotDir, snapshot, monitor);
    manifest.addTableDescriptor(htd);
  }

  @Override
  public void consolidateSnapshot(SnapshotDescription snapshot, StorageContext ctx)
      throws IOException {
    SnapshotManifest manifest = getSnapshotManifest(snapshot, ctx);
    manifest.consolidate();
  }

  @Override
  public boolean changeSnapshotContext(SnapshotDescription snapshot, StorageContext src,
      StorageContext dest) throws IOException {
    Path srcDir = getSnapshotDirFromContext(src, snapshot.getName());
    Path destDir = getSnapshotDirFromContext(dest, snapshot.getName());
    return getFileSystem().rename(srcDir, destDir);
  }

  @Override
  public void addRegionToSnapshot(SnapshotDescription snapshot, HRegionInfo hri,
                                  StorageContext ctx) throws IOException {
    SnapshotManifest manifest = getSnapshotManifest(snapshot, ctx);
    Path tableDir = LegacyLayout.getTableDir(LegacyLayout.getDataDir(getRootContainer().path),
        hri.getTable());
    manifest.addRegion(tableDir, hri);
  }

  @Override
  public void addRegionsToSnapshot(SnapshotDescription snapshot, Collection<HRegionInfo> regions,
      StorageContext ctx) throws IOException {
    // TODO: use ExecutorService to add regions
    for (HRegionInfo r: regions) {
      addRegionToSnapshot(snapshot, r, ctx);
    }
  }

  @Override
  public SnapshotRestoreMetaChanges restoreSnapshot(final SnapshotDescription snapshot,
      final StorageContext snapshotCtx, final HTableDescriptor destHtd,
      final ForeignExceptionDispatcher monitor, final MonitoredTask status) throws IOException {
    // TODO: currently snapshotCtx is not used, modify RestoreSnapshotHelper to take ctx as an input
    RestoreSnapshotHelper restoreSnapshotHelper = new RestoreSnapshotHelper(this, snapshot,
        destHtd, monitor, status);
    return restoreSnapshotHelper.restoreStorageRegions();
  }

  /**
   * Write the snapshot description into the working directory of a snapshot
   *
   * @param snapshot description of the snapshot being taken
   * @param workingDir working directory of the snapshot
   * @throws IOException if we can't reach the filesystem and the file cannot be cleaned up on
   *           failure
   */
  // TODO: After ExportSnapshot refactoring make this private if not referred from outside package
  public void writeSnapshotInfo(SnapshotDescription snapshot, Path workingDir)
      throws IOException {
    FsPermission perms = FSUtils.getFilePermissions(getFileSystem(), getFileSystem().getConf(),
        HConstants.DATA_FILE_UMASK_KEY);
    Path snapshotInfo = new Path(workingDir, LegacyLayout.SNAPSHOTINFO_FILE);
    try {
      FSDataOutputStream out = FSUtils.create(getFileSystem(), snapshotInfo, perms, true);
      try {
        snapshot.writeTo(out);
      } finally {
        out.close();
      }
    } catch (IOException e) {
      // if we get an exception, try to remove the snapshot info
      if (!getFileSystem().delete(snapshotInfo, false)) {
        String msg = "Couldn't delete snapshot info file: " + snapshotInfo;
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }

  // ==========================================================================
  // PUBLIC - WAL
  // ==========================================================================
  @Override
  public boolean hasWALs(String serverName) throws IOException {
    Path logDir = new Path(getRootContainer().path, new StringBuilder(
        HConstants.HREGION_LOGDIR_NAME).append("/").append(serverName).toString());
    Path splitDir = logDir.suffix(SPLITTING_EXT);

    return checkWALs(logDir) || checkWALs(splitDir);
  }


  // ==========================================================================
  // PRIVATE - WAL
  // ==========================================================================

  private boolean checkWALs(Path dir) throws IOException {
    FileSystem fs = getFileSystem();

    if (!fs.exists(dir)) {
      LOG.debug(dir + " not found!");
      return false;
    } else if (!fs.getFileStatus(dir).isDirectory()) {
      LOG.warn(dir + " is not a directory");
      return false;
    }

    FileStatus[] files = FSUtils.listStatus(fs, dir);
    if (files == null || files.length == 0) {
      LOG.debug(dir + " has no files");
      return false;
    }

    for (FileStatus dentry: files) {
      if (dentry.isFile() && dentry.getLen() > 0) {
        LOG.debug(dir + " has a non-empty file: " + dentry.getPath());
        return true;
      } else if (dentry.isDirectory() && checkWALs(dentry.getPath())) {
        LOG.debug(dentry + " is a directory and has a non-empty file!");
        return true;
      }
    }
    LOG.debug("Found zero non-empty wal files for: " + dir);
    return false;
  }

  // ==========================================================================
  //  PROTECTED Methods - Bootstrap
  // ==========================================================================

  /**
   * Create initial layout in filesystem.
   * <ol>
   * <li>Check if the meta region exists and is readable, if not create it.
   * Create hbase.version and the hbase:meta directory if not one.
   * </li>
   * <li>Create a log archive directory for RS to put archived logs</li>
   * </ol>
   * Idempotent.
   * @throws IOException
   */
  @Override
  protected ClusterId startup() throws IOException {
    Configuration c = getConfiguration();
    Path rc = ((LegacyPathIdentifier)getRootContainer()).path;
    FileSystem fs = getFileSystem();

    // If FS is in safe mode wait till out of it.
    FSUtils.waitOnSafeMode(c, c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000));

    boolean isSecurityEnabled = "kerberos".equalsIgnoreCase(c.get("hbase.security.authentication"));
    FsPermission rootDirPerms = new FsPermission(c.get("hbase.rootdir.perms", "700"));

    // Filesystem is good. Go ahead and check for hbase.rootdir.
    try {
      if (!fs.exists(rc)) {
        if (isSecurityEnabled) {
          fs.mkdirs(rc, rootDirPerms);
        } else {
          fs.mkdirs(rc);
        }
        // DFS leaves safe mode with 0 DNs when there are 0 blocks.
        // We used to handle this by checking the current DN count and waiting until
        // it is nonzero. With security, the check for datanode count doesn't work --
        // it is a privileged op. So instead we adopt the strategy of the jobtracker
        // and simply retry file creation during bootstrap indefinitely. As soon as
        // there is one datanode it will succeed. Permission problems should have
        // already been caught by mkdirs above.
        FSUtils.setVersion(fs, rc, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
            10 * 1000), c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      } else {
        if (!fs.isDirectory(rc)) {
          throw new IllegalArgumentException(rc.toString() + " is not a directory");
        }
        if (isSecurityEnabled && !rootDirPerms.equals(fs.getFileStatus(rc).getPermission())) {
          // check whether the permission match
          LOG.warn("Found rootdir permissions NOT matching expected \"hbase.rootdir.perms\" for "
              + "rootdir=" + rc.toString() + " permissions=" + fs.getFileStatus(rc).getPermission()
              + " and  \"hbase.rootdir.perms\" configured as "
              + c.get("hbase.rootdir.perms", "700") + ". Automatically setting the permissions. You"
              + " can change the permissions by setting \"hbase.rootdir.perms\" in hbase-site.xml "
              + "and restarting the master");
          fs.setPermission(rc, rootDirPerms);
        }
        // as above
        FSUtils.checkVersion(fs, rc, true, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
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
          + HConstants.HBASE_DIR + " " + rc.toString(), iae);
      throw iae;
    }
    // Make sure cluster ID exists
    if (!FSUtils.checkClusterIdExists(fs, rc, c.getInt(
        HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000))) {
      FSUtils.setClusterId(fs, rc, new ClusterId(), c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10
          * 1000));
    }
    return FSUtils.getClusterId(fs, rc);
  }

  @Override
  public void logStorageState(Log log) throws IOException {
    FSUtils.logFileSystemState(getFileSystem(), ((LegacyPathIdentifier)getRootContainer()).path,
        LOG);
  }

  @Override
  protected void bootstrapMeta() throws IOException {
    // TODO ask RegionStorage
    if (!FSUtils.metaRegionExists(getFileSystem(), getRootContainer().path)) {
      bootstrapMeta(getConfiguration());
    }

    // Create tableinfo-s for hbase:meta if not already there.
    // assume, created table descriptor is for enabling table
    // meta table is a system table, so descriptors are predefined,
    // we should get them from registry.
    createTableDescriptor(HTableDescriptor.metaTableDescriptor(getConfiguration()), false);
  }

  private static void bootstrapMeta(final Configuration c) throws IOException {
    LOG.info("BOOTSTRAP: creating hbase:meta region");
    try {
      // Bootstrapping, make sure blockcache is off.  Else, one will be
      // created here in bootstrap and it'll need to be cleaned up.  Better to
      // not make it in first place.  Turn off block caching for bootstrap.
      // Enable after.
      HRegionInfo metaHRI = new HRegionInfo(HRegionInfo.FIRST_META_REGIONINFO);
      HTableDescriptor metaDescriptor = HTableDescriptor.metaTableDescriptor(c);
      MetaUtils.setInfoFamilyCachingForMeta(metaDescriptor, false);
      HRegion meta = HRegion.createHRegion(c, metaDescriptor, metaHRI, null);
      MetaUtils.setInfoFamilyCachingForMeta(metaDescriptor, true);
      meta.close();
    } catch (IOException e) {
        e = e instanceof RemoteException ?
                ((RemoteException)e).unwrapRemoteException() : e;
      LOG.error("bootstrap", e);
      throw e;
    }
  }

  @Override
  protected void startupCleanup() throws IOException {
    final FileSystem fs = getFileSystem();
    // Check the directories under rootdir.
    checkTempDir(getTempContainer().path, getConfiguration(), getFileSystem());
    final String[] protectedSubDirs = new String[] {
        HConstants.BASE_NAMESPACE_DIR,
        HConstants.HFILE_ARCHIVE_DIRECTORY,
        HConstants.HREGION_LOGDIR_NAME,
        HConstants.HREGION_OLDLOGDIR_NAME,
        MasterProcedureConstants.MASTER_PROCEDURE_LOGDIR,
        HConstants.CORRUPT_DIR_NAME,
        HConstants.HBCK_SIDELINEDIR_NAME,
        MobConstants.MOB_DIR_NAME
    };
    for (String subDir : protectedSubDirs) {
      checkSubDir(new Path(getRootContainer().path, subDir));
    }

    checkStagingDir();

    // Handle the last few special files and set the final rootDir permissions
    // rootDir needs 'x' for all to support bulk load staging dir
    if (isSecurityEnabled) {
      fs.setPermission(new Path(getRootContainer().path, HConstants.VERSION_FILE_NAME), secureRootFilePerms);
      fs.setPermission(new Path(getRootContainer().path, HConstants.CLUSTER_ID_FILE_NAME), secureRootFilePerms);
    }
    FsPermission currentRootPerms = fs.getFileStatus(getRootContainer().path).getPermission();
    if (!currentRootPerms.getUserAction().implies(FsAction.EXECUTE)
        || !currentRootPerms.getGroupAction().implies(FsAction.EXECUTE)
        || !currentRootPerms.getOtherAction().implies(FsAction.EXECUTE)) {
      LOG.warn("rootdir permissions do not contain 'excute' for user, group or other. "
        + "Automatically adding 'excute' permission for all");
      fs.setPermission(
        getRootContainer().path,
        new FsPermission(currentRootPerms.getUserAction().or(FsAction.EXECUTE), currentRootPerms
            .getGroupAction().or(FsAction.EXECUTE), currentRootPerms.getOtherAction().or(
          FsAction.EXECUTE)));
    }
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
          HFileArchiver.archiveRegion(fs, getRootContainer().path, tabledir, regiondir);
        }
      }
      if (!fs.delete(tmpdir, true)) {
        throw new IOException("Unable to clean the temp directory: " + tmpdir);
      }
    }

    // Create the temp directory
    if (isSecurityEnabled) {
      if (!fs.mkdirs(tmpdir, secureRootSubDirPerms)) {
        throw new IOException("HBase temp directory '" + tmpdir + "' creation failure.");
      }
    } else {
      if (!fs.mkdirs(tmpdir)) {
        throw new IOException("HBase temp directory '" + tmpdir + "' creation failure.");
      }
    }
  }

  /**
   * Make sure the directories under rootDir have good permissions. Create if necessary.
   * @param p
   * @throws IOException
   */
  private void checkSubDir(final Path p) throws IOException {
    final FileSystem fs = getFileSystem();
    if (!fs.exists(p)) {
      if (isSecurityEnabled) {
        if (!fs.mkdirs(p, secureRootSubDirPerms)) {
          throw new IOException("HBase directory '" + p + "' creation failure.");
        }
      } else {
        if (!fs.mkdirs(p)) {
          throw new IOException("HBase directory '" + p + "' creation failure.");
        }
      }
    }
    else {
      if (isSecurityEnabled && !secureRootSubDirPerms.equals(fs.getFileStatus(p).getPermission())) {
        // check whether the permission match
        LOG.warn("Found HBase directory permissions NOT matching expected permissions for "
            + p.toString() + " permissions=" + fs.getFileStatus(p).getPermission()
            + ", expecting " + secureRootSubDirPerms + ". Automatically setting the permissions. "
            + "You can change the permissions by setting \"hbase.rootdir.perms\" in hbase-site.xml "
            + "and restarting the master");
        fs.setPermission(p, secureRootSubDirPerms);
      }
    }
  }

  /**
   * Check permissions for bulk load staging directory. This directory has special hidden
   * permissions. Create it if necessary.
   * @throws IOException
   */
  private void checkStagingDir() throws IOException {
    final FileSystem fs = getFileSystem();
    Path p = new Path(getRootContainer().path, HConstants.BULKLOAD_STAGING_DIR_NAME);
    try {
      if (!fs.exists(p)) {
        if (!fs.mkdirs(p, HiddenDirPerms)) {
          throw new IOException("Failed to create staging directory " + p.toString());
        }
      } else {
        fs.setPermission(p, HiddenDirPerms);
      }
    } catch (IOException e) {
      LOG.error("Failed to create or set permission on staging directory " + p.toString());
      throw new IOException("Failed to create or set permission on staging directory "
          + p.toString(), e);
    }
  }

  // ==========================================================================
  //  PROTECTED Methods - Path
  // ==========================================================================
  protected Path getNamespaceDir(StorageContext ctx) {
    return getBaseDirFromContext(ctx);
  }

  protected Path getNamespaceDir(StorageContext ctx, String namespace) {
    return LegacyLayout.getNamespaceDir(getBaseDirFromContext(ctx), namespace);
  }

  protected Path getTableDir(StorageContext ctx, TableName table) {
    return LegacyLayout.getTableDir(getBaseDirFromContext(ctx), table);
  }

  protected Path getRegionDir(StorageContext ctx, TableName table, HRegionInfo hri) {
    return LegacyLayout.getRegionDir(getTableDir(ctx, table), hri);
  }

  @Override
  public LegacyPathIdentifier getTempContainer() {
    return new LegacyPathIdentifier(tmpDir);
  }

  protected Path getSnapshotDirFromContext(StorageContext ctx, String snapshot) {
    switch(ctx) {
      case TEMP: return LegacyLayout.getWorkingSnapshotDir(getRootContainer().path, snapshot);
      case DATA: return LegacyLayout.getCompletedSnapshotDir(getRootContainer().path, snapshot);
      default: throw new RuntimeException("Invalid context: " + ctx);
    }
  }

  protected Path getSnapshotDirFromContext(StorageContext ctx) {
    switch (ctx) {
      case TEMP: return tmpSnapshotDir;
      case DATA: return snapshotDir;
      default: throw new RuntimeException("Invalid context: " + ctx);
    }
  }

  protected Path getBaseDirFromContext(StorageContext ctx) {
    switch (ctx) {
      case TEMP: return tmpDataDir;
      case DATA: return dataDir;
      case ARCHIVE: return archiveDataDir;
      case SIDELINE: return sidelineDir;
      default: throw new RuntimeException("Invalid context: " + ctx);
    }
  }
}
