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

package org.apache.hadoop.hbase.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.fs.legacy.LegacyMasterStorage;
import org.apache.hadoop.hbase.fs.RegionStorage.StoreFileVisitor;
import org.apache.hadoop.hbase.fs.legacy.LegacyPathIdentifier;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.SnapshotRestoreMetaChanges;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

@InterfaceAudience.Private
public abstract class MasterStorage<IDENTIFIER extends StorageIdentifier> {
  private static Log LOG = LogFactory.getLog(MasterStorage.class);

  // Persisted unique cluster ID
  private ClusterId clusterId;


  private Configuration conf;
  private FileSystem fs;  // TODO: definitely remove
  private IDENTIFIER rootContainer;

  protected MasterStorage(Configuration conf, FileSystem fs, IDENTIFIER rootContainer) {
    this.rootContainer = rootContainer;
    this.conf = conf;
    this.fs = fs;
  }

  public Configuration getConfiguration() { return conf; }
  public FileSystem getFileSystem() { return fs; }  // TODO: definitely remove
  public IDENTIFIER getRootContainer() { return rootContainer; }

  /**
   * Get Chores that are required to be run from time to time for the underlying MasterStorage
   * implementation. A few setup methods e.g. {@link #enableSnapshots()} may have their own chores.
   * The returned list of chores or their configuration may vary depending on when in sequence
   * this method is called with respect to other methods. Generally, a call to this method for
   * getting and scheduling chores, needs to be after storage is setup properly by calling those
   * methods first.
   *
   * Please refer to the documentation of specific method implementation for more details.
   *
   * @param stopper the stopper
   * @return  storage chores.
   */
  public Iterable<ScheduledChore> getChores(Stoppable stopper, Map<String, Object> params) {
    return new ArrayList<>();
  }

  // ==========================================================================
  //  PUBLIC Interfaces - Visitors
  // ==========================================================================
  public interface NamespaceVisitor {
    void visitNamespace(String namespace) throws IOException;
  }

  public interface TableVisitor {
    void visitTable(TableName tableName) throws IOException;
  }

  public interface RegionVisitor {
    void visitRegion(HRegionInfo regionInfo) throws IOException;
  }

  public interface SnapshotVisitor {
    void visitSnapshot(final String snapshotName, final SnapshotDescription snapshot,
        StorageContext ctx);
  }

  public interface SnapshotStoreFileVisitor {
    // TODO: Instead of SnapshotRegionManifest.StoreFile return common object across all
    void visitSnapshotStoreFile(SnapshotDescription snapshot, StorageContext ctx, HRegionInfo hri,
        String familyName, final SnapshotRegionManifest.StoreFile storeFile) throws IOException;
  }

  // ==========================================================================
  //  PUBLIC Methods - Namespace related
  // ==========================================================================
  public abstract void createNamespace(NamespaceDescriptor nsDescriptor) throws IOException;
  public abstract void deleteNamespace(String namespaceName) throws IOException;
  public abstract Collection<String> getNamespaces(StorageContext ctx) throws IOException;

  public Collection<String> getNamespaces() throws IOException {
    return getNamespaces(StorageContext.DATA);
  }
  // should return or get a NamespaceDescriptor? how is that different from HTD?

  // ==========================================================================
  //  PUBLIC Methods - Table Descriptor related
  // ==========================================================================
  public HTableDescriptor getTableDescriptor(TableName tableName)
      throws IOException {
    return getTableDescriptor(StorageContext.DATA, tableName);
  }

  public boolean createTableDescriptor(HTableDescriptor tableDesc, boolean force)
      throws IOException {
    return createTableDescriptor(StorageContext.DATA, tableDesc, force);
  }

  public void updateTableDescriptor(HTableDescriptor tableDesc) throws IOException {
    updateTableDescriptor(StorageContext.DATA, tableDesc);
  }

  public abstract HTableDescriptor getTableDescriptor(StorageContext ctx, TableName tableName)
      throws IOException;
  public abstract boolean createTableDescriptor(StorageContext ctx, HTableDescriptor tableDesc,
                                                boolean force) throws IOException;
  public abstract void updateTableDescriptor(StorageContext ctx, HTableDescriptor tableDesc)
      throws IOException;

  // ==========================================================================
  //  PUBLIC Methods - Table related
  // ==========================================================================

  /**
   * Deletes table from master storage (without archival before delete and from DATA context)
   * @param tableName
   * @throws IOException
   */
  public void deleteTable(TableName tableName) throws IOException {
    deleteTable(StorageContext.DATA, tableName, false);
  }

  /**
   * Deletes table from master storage (from DATA context)
   * @param tableName
   * @param archive if true, all regions are archived before deletion
   * @throws IOException
   */
  public void deleteTable(TableName tableName, boolean archive) throws IOException {
    deleteTable(StorageContext.DATA, tableName, archive);
  }

  /**
   * Deletes table from master storage
   * @param ctx Storage context of the table
   * @param tableName
   * @param archive if true, all regions are archived for the table before deletion.
   * @throws IOException
   */
  public abstract void deleteTable(StorageContext ctx, TableName tableName, boolean archive)
      throws IOException;

  public Collection<TableName> getTables(String namespace) throws IOException {
    return getTables(StorageContext.DATA, namespace);
  }

  public abstract Collection<TableName> getTables(StorageContext ctx, String namespace)
      throws IOException;

  public Collection<TableName> getTables() throws IOException {
    ArrayList<TableName> tables = new ArrayList<TableName>();
    for (String ns: getNamespaces()) {
      tables.addAll(getTables(ns));
    }
    return tables;
  }

  /**
   * Archives a table and all it's regions
   * @param tableName
   * @throws IOException
   */
  public void archiveTable(TableName tableName) throws IOException {
    archiveTable(StorageContext.DATA, tableName);
  }

  /**
   * Archives a table and all it's regions
   * @param ctx Storage context of the table.
   * @param tableName
   * @throws IOException
   */
  public abstract void archiveTable(StorageContext ctx, TableName tableName) throws IOException;

  /**
   * Runs through all tables and checks how many stores for each table
   * have more than one file in them. Checks -ROOT- and hbase:meta too. The total
   * percentage across all tables is stored under the special key "-TOTAL-".
   *
   * @return A map for each table and its percentage.
   *
   * @throws IOException When scanning the directory fails.
   */
  public Map<String, Integer> getTableFragmentation() throws IOException {
    final Map<String, Integer> frags = new HashMap<>();
    int cfCountTotal = 0;
    int cfFragTotal = 0;

    for (TableName table: getTables()) {
      int cfCount = 0;
      int cfFrag = 0;
      for (HRegionInfo hri: getRegions(table)) {
        RegionStorage<IDENTIFIER> rs = getRegionStorage(hri);
        final Collection<String> families = rs.getFamilies();
        for (String family: families) {
          cfCount++;
          cfCountTotal++;
          if (rs.getStoreFiles(family).size() > 1) {
            cfFrag++;
            cfFragTotal++;
          }
        }
      }
      // compute percentage per table and store in result list
      frags.put(table.getNameAsString(),
          cfCount == 0? 0: Math.round((float) cfFrag / cfCount * 100));
    }
    // set overall percentage for all tables
    frags.put("-TOTAL-",
        cfCountTotal == 0? 0: Math.round((float) cfFragTotal / cfCountTotal * 100));
    return frags;
  }

  // ==========================================================================
  //  PUBLIC Methods - Table Region related
  // ==========================================================================
  public void deleteRegion(HRegionInfo regionInfo) throws IOException {
    RegionStorage.destroy(conf, regionInfo);
  }

  public Collection<HRegionInfo> getRegions(TableName tableName) throws IOException {
    return getRegions(StorageContext.DATA, tableName);
  }

  public abstract Collection<HRegionInfo> getRegions(StorageContext ctx, TableName tableName)
    throws IOException;

  /**
   * Returns region info given table name and encoded region name
   * @throws IOException
   */
  public HRegionInfo getRegion(TableName tableName, String encodedName) throws IOException {
    return getRegion(StorageContext.DATA, tableName, encodedName);
  }

  public abstract HRegionInfo getRegion(StorageContext ctx, TableName tableName, String regionName)
      throws IOException;

  // TODO: Move in HRegionStorage
  public void deleteFamilyFromStorage(HRegionInfo regionInfo, byte[] familyName, boolean hasMob)
      throws IOException {
    getRegionStorage(regionInfo).deleteFamily(Bytes.toString(familyName), hasMob);
  }

  public RegionStorage getRegionStorage(HRegionInfo regionInfo) throws IOException {
    return RegionStorage.open(conf, regionInfo, false);
  }

  /**
   * Returns true if region exists on the Storage
   * @param regionInfo
   * @return true, if region exists on the storage
   * @throws IOException
   */
  public boolean regionExists(HRegionInfo regionInfo) throws IOException {
      RegionStorage regionStorage = getRegionStorage(regionInfo);
      return regionStorage.exists();
  }

  /**
   * Archives region's storage artifacts (files, directories etc)
   * @param regionInfo
   * @throws IOException
   */
  public abstract void archiveRegion(HRegionInfo regionInfo) throws IOException;

  // ==========================================================================
  //  PUBLIC Methods - Snapshot related
  // ==========================================================================
  /**
   * This method should be called to prepare storage implementation/s for snapshots. The default
   * implementation does nothing. MasterStorage subclasses need to override this method to
   * provide specific preparatory steps.
   */
  public void enableSnapshots() throws IOException {
    return;
  }

  /**
   * Returns true if MasterStorage is prepared for snapshots
   */
  public boolean isSnapshotsEnabled() {
    return true;
  }

  /**
   * Gets the list of all snapshots.
   * @return list of SnapshotDescriptions
   * @throws IOException Storage exception
   */
  public List<SnapshotDescription> getSnapshots() throws IOException {
    return getSnapshots(StorageContext.DATA);
  }

  public abstract List<SnapshotDescription> getSnapshots(StorageContext ctx) throws IOException;

  /**
   * Gets snapshot description of a snapshot
   * @return Snapshot description of a snapshot if found, null otherwise
   * @throws IOException
   */
  public SnapshotDescription getSnapshot(final String snapshotName)
      throws IOException {
    return getSnapshot(snapshotName, StorageContext.DATA);
  }

  public abstract SnapshotDescription getSnapshot(final String snapshotName, StorageContext ctx)
    throws IOException;

  /**
   * @return {@link HTableDescriptor} for a snapshot
   * @param snapshot
   * @throws IOException if can't read from the storage
   */
  public HTableDescriptor getTableDescriptorForSnapshot(final SnapshotDescription snapshot)
      throws IOException {
    return getTableDescriptorForSnapshot(snapshot, StorageContext.DATA);
  }

  public abstract HTableDescriptor getTableDescriptorForSnapshot(final SnapshotDescription
    snapshot, StorageContext ctx) throws IOException;

  /**
   * Returns all {@link HRegionInfo} for a snapshot
   *
   * @param snapshot
   * @throws IOException
   */
  public Map<String, HRegionInfo> getSnapshotRegions(final SnapshotDescription snapshot)
      throws IOException {
    return getSnapshotRegions(snapshot, StorageContext.DATA);
  }

  public abstract Map<String, HRegionInfo> getSnapshotRegions(final SnapshotDescription snapshot,
      StorageContext ctx) throws IOException;

  /**
   * Check to see if the snapshot is one of the currently snapshots on the storage.
   *
   * @param snapshot
   * @throws IOException
   */
  public boolean snapshotExists(SnapshotDescription snapshot) throws IOException {
    return snapshotExists(snapshot, StorageContext.DATA);
  }

  public abstract boolean snapshotExists(SnapshotDescription snapshot, StorageContext ctx)
      throws IOException;

  public boolean snapshotExists(String snapshotName) throws IOException {
    return snapshotExists(snapshotName, StorageContext.DATA);
  }

  public abstract boolean snapshotExists(String snapshotName, StorageContext ctx) throws
      IOException;

  /**
   * Cleans up all snapshots.
   *
   * @throws IOException if can't reach the storage
   */
  public void deleteAllSnapshots() throws IOException {
    deleteAllSnapshots(StorageContext.DATA);
  }

  public abstract void deleteAllSnapshots(StorageContext ctx) throws IOException;

  /**
   * Deletes a snapshot
   * @param snapshot
   * @throws SnapshotDoesNotExistException If the specified snapshot does not exist.
   * @throws IOException For storage IOExceptions
   */
  public boolean deleteSnapshot(final SnapshotDescription snapshot) throws IOException {
    return deleteSnapshot(snapshot, StorageContext.DATA) &&
        deleteSnapshot(snapshot, StorageContext.TEMP);
  }

  public boolean deleteSnapshot(final String snapshotName) throws IOException {
    return deleteSnapshot(snapshotName, StorageContext.DATA) &&
        deleteSnapshot(snapshotName, StorageContext.TEMP);
  }

  public abstract boolean deleteSnapshot(final SnapshotDescription snapshot,
      final StorageContext ctx) throws IOException;

  public abstract boolean deleteSnapshot(final String snapshotName, final StorageContext ctx)
      throws IOException;

  /**
   * Deletes old in-progress and/ or completed snapshot and prepares for new one with the same
   * description
   *
   * @param snapshot
   * @throws IOException for storage IOExceptions
   */
  public abstract void prepareSnapshot(SnapshotDescription snapshot) throws IOException;

  /**
   * In general snapshot is created with following steps:
   * <ul>
   *   <li>Initiate a snapshot for a table in TEMP context</li>
   *   <li>Snapshot and add regions to the snapshot in TEMP</li>
   *   <li>Consolidate snapshot</li>
   *   <li>Change context of a snapshot from TEMP to DATA</li>
   * </ul>
   * @param htd
   * @param snapshot
   * @param monitor
   * @throws IOException
   */
  public void initiateSnapshot(HTableDescriptor htd, SnapshotDescription snapshot, final
      ForeignExceptionSnare monitor) throws IOException {
    initiateSnapshot(htd, snapshot, monitor, StorageContext.DATA);
  }

  public abstract void initiateSnapshot(HTableDescriptor htd, SnapshotDescription snapshot,
                                        final ForeignExceptionSnare monitor, StorageContext ctx) throws IOException;

  /**
   * Consolidates added regions and verifies snapshot
   * @param snapshot
   * @throws IOException
   */
  public void consolidateSnapshot(SnapshotDescription snapshot) throws IOException {
    consolidateSnapshot(snapshot, StorageContext.DATA);
  }

  public abstract void consolidateSnapshot(SnapshotDescription snapshot, StorageContext ctx)
      throws IOException;

  /**
   * Changes {@link StorageContext} of a snapshot from src to dest
   *
   * @param snapshot
   * @param src Source {@link StorageContext}
   * @param dest Destination {@link StorageContext}
   * @throws IOException
   */
  public abstract boolean changeSnapshotContext(SnapshotDescription snapshot, StorageContext src,
                                                StorageContext dest) throws IOException;

  /**
   * Adds given region to the snapshot.
   *
   * @param snapshot
   * @param hri
   * @throws IOException
   */
  public void addRegionToSnapshot(SnapshotDescription snapshot, HRegionInfo hri)
      throws IOException {
    addRegionToSnapshot(snapshot, hri, StorageContext.DATA);
  }

  public abstract void addRegionToSnapshot(SnapshotDescription snapshot, HRegionInfo hri,
      StorageContext ctx) throws IOException;

  public void addRegionsToSnapshot(SnapshotDescription snapshot, Collection<HRegionInfo> regions)
      throws IOException {
    addRegionsToSnapshot(snapshot, regions, StorageContext.DATA);
  }

  public abstract void addRegionsToSnapshot(SnapshotDescription snapshot,
      Collection<HRegionInfo> regions, StorageContext ctx) throws IOException;

  /**
   * Restore snapshot to dest table and returns instance of {@link SnapshotRestoreMetaChanges}
   * describing changes required for META.
   * @param snapshot
   * @param destHtd
   * @param monitor
   * @param status
   * @throws IOException
   */
  public SnapshotRestoreMetaChanges restoreSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor destHtd, final ForeignExceptionDispatcher monitor,
      final MonitoredTask status) throws IOException {
    return restoreSnapshot(snapshot, StorageContext.DATA, destHtd, monitor, status);
  }

  public abstract SnapshotRestoreMetaChanges restoreSnapshot(final SnapshotDescription snapshot,
      final StorageContext snapshotCtx, final HTableDescriptor destHtd,
      final ForeignExceptionDispatcher monitor, final MonitoredTask status) throws IOException;

  // ==========================================================================
  // PUBLIC Methods - WAL
  // ==========================================================================

  /**
   * Returns true if given region server has non-empty WAL files
   * @param serverName
   */
  public abstract boolean hasWALs(String serverName) throws IOException;

  // ==========================================================================
  //  PUBLIC Methods - visitors
  // ==========================================================================
  // TODO: remove implementations. How to visit store files is up to implementation, may use
  // threadpool etc.
  public void visitStoreFiles(StoreFileVisitor visitor)
      throws IOException {
    visitStoreFiles(StorageContext.DATA, visitor);
  }

  public void visitStoreFiles(String namespace, StoreFileVisitor visitor)
      throws IOException {
    visitStoreFiles(StorageContext.DATA, namespace, visitor);
  }

  public void visitStoreFiles(TableName table, StoreFileVisitor visitor)
      throws IOException {
    visitStoreFiles(StorageContext.DATA, table, visitor);
  }

  public void visitStoreFiles(StorageContext ctx, StoreFileVisitor visitor)
      throws IOException {
    for (String namespace: getNamespaces()) {
      visitStoreFiles(ctx, namespace, visitor);
    }
  }

  public void visitStoreFiles(StorageContext ctx, String namespace, StoreFileVisitor visitor)
      throws IOException {
    for (TableName tableName: getTables(namespace)) {
      visitStoreFiles(ctx, tableName, visitor);
    }
  }

  public void visitStoreFiles(StorageContext ctx, TableName table, StoreFileVisitor visitor)
      throws IOException {
    for (HRegionInfo hri: getRegions(ctx, table)) {
      RegionStorage.open(conf, hri, false).visitStoreFiles(visitor);
    }
  }

  /**
   * Visit all snapshots on a storage with visitor instance
   * @param visitor
   * @throws IOException
   */
  public abstract void visitSnapshots(final SnapshotVisitor visitor) throws IOException;

  public abstract void visitSnapshots(StorageContext ctx, final SnapshotVisitor visitor)
      throws IOException;

  /**
   * Visit all store files of a snapshot with visitor instance
   *
   * @param snapshot
   * @param ctx
   * @param visitor
   * @throws IOException
   */
  public abstract void visitSnapshotStoreFiles(SnapshotDescription snapshot, StorageContext ctx,
                                               SnapshotStoreFileVisitor visitor) throws IOException;


  // ==========================================================================
  //  PUBLIC Methods - bootstrap
  // ==========================================================================
  public abstract IDENTIFIER getTempContainer();

  public abstract void logStorageState(Log log) throws IOException;

  /**
   * @return The unique identifier generated for this cluster
   */
  public ClusterId getClusterId() {
    return clusterId;
  }

  /**
   * Bootstrap MasterStorage
   * @throws IOException
   */
  protected void bootstrap() throws IOException {
    // Initialize
    clusterId = startup();

    // Make sure the meta region exists!
    bootstrapMeta();

    // check if temp directory exists and clean it
    startupCleanup();
  }

  protected abstract ClusterId startup() throws IOException;
  protected abstract void bootstrapMeta() throws IOException;
  protected abstract void startupCleanup() throws IOException;

  // ==========================================================================
  //  PUBLIC
  // ==========================================================================
  public static MasterStorage open(Configuration conf, boolean bootstrap)
      throws IOException {
    return open(conf, FSUtils.getRootDir(conf), bootstrap);
  }

  public static MasterStorage open(Configuration conf, Path rootDir, boolean bootstrap)
      throws IOException {
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    FileSystem fs = rootDir.getFileSystem(conf);
    FSUtils.setFsDefault(conf, new Path(fs.getUri()));
    // make sure the fs has the same conf
    fs.setConf(conf);

    MasterStorage ms = getInstance(conf, fs, rootDir);
    if (bootstrap) {
      ms.bootstrap();
    }
    HFileSystem.addLocationsOrderInterceptor(conf);
    return ms;
  }

  private static MasterStorage getInstance(Configuration conf, final FileSystem fs,
                                           Path rootDir) throws IOException {
    String storageType = conf.get("hbase.storage.type", "legacy").toLowerCase();
    switch (storageType) {
      case "legacy":
        return new LegacyMasterStorage(conf, fs, new LegacyPathIdentifier(rootDir));
      default:
        throw new IOException("Invalid filesystem type " + storageType);
    }
  }
}
