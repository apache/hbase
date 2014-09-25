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
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotDataManifest;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Utility class to help read/write the Snapshot Manifest.
 *
 * The snapshot format is transparent for the users of this class,
 * once the snapshot is written, it will never be modified.
 * On open() the snapshot will be loaded to the current in-memory format.
 */
@InterfaceAudience.Private
public class SnapshotManifest {
  private static final Log LOG = LogFactory.getLog(SnapshotManifest.class);

  private static final String DATA_MANIFEST_NAME = "data.manifest";

  private List<SnapshotRegionManifest> regionManifests;
  private SnapshotDescription desc;
  private HTableDescriptor htd;

  private final ForeignExceptionSnare monitor;
  private final Configuration conf;
  private final Path workingDir;
  private final FileSystem fs;

  private SnapshotManifest(final Configuration conf, final FileSystem fs,
      final Path workingDir, final SnapshotDescription desc,
      final ForeignExceptionSnare monitor) {
    this.monitor = monitor;
    this.desc = desc;
    this.workingDir = workingDir;
    this.conf = conf;
    this.fs = fs;
  }

  /**
   * Return a SnapshotManifest instance, used for writing a snapshot.
   *
   * There are two usage pattern:
   *  - The Master will create a manifest, add the descriptor, offline regions
   *    and consolidate the snapshot by writing all the pending stuff on-disk.
   *      manifest = SnapshotManifest.create(...)
   *      manifest.addRegion(tableDir, hri)
   *      manifest.consolidate()
   *  - The RegionServer will create a single region manifest
   *      manifest = SnapshotManifest.create(...)
   *      manifest.addRegion(region)
   */
  public static SnapshotManifest create(final Configuration conf, final FileSystem fs,
      final Path workingDir, final SnapshotDescription desc,
      final ForeignExceptionSnare monitor) {
    return new SnapshotManifest(conf, fs, workingDir, desc, monitor);
  }

  /**
   * Return a SnapshotManifest instance with the information already loaded in-memory.
   *    SnapshotManifest manifest = SnapshotManifest.open(...)
   *    HTableDescriptor htd = manifest.getTableDescriptor()
   *    for (SnapshotRegionManifest regionManifest: manifest.getRegionManifests())
   *      hri = regionManifest.getRegionInfo()
   *      for (regionManifest.getFamilyFiles())
   *        ...
   */
  public static SnapshotManifest open(final Configuration conf, final FileSystem fs,
      final Path workingDir, final SnapshotDescription desc) throws IOException {
    SnapshotManifest manifest = new SnapshotManifest(conf, fs, workingDir, desc, null);
    manifest.load();
    return manifest;
  }


  /**
   * Add the table descriptor to the snapshot manifest
   */
  public void addTableDescriptor(final HTableDescriptor htd) throws IOException {
    this.htd = htd;
  }

  interface RegionVisitor<TRegion, TFamily> {
    TRegion regionOpen(final HRegionInfo regionInfo) throws IOException;
    void regionClose(final TRegion region) throws IOException;

    TFamily familyOpen(final TRegion region, final byte[] familyName) throws IOException;
    void familyClose(final TRegion region, final TFamily family) throws IOException;

    void storeFile(final TRegion region, final TFamily family, final StoreFileInfo storeFile)
      throws IOException;
  }

  private RegionVisitor createRegionVisitor(final SnapshotDescription desc) throws IOException {
    switch (getSnapshotFormat(desc)) {
      case SnapshotManifestV1.DESCRIPTOR_VERSION:
        return new SnapshotManifestV1.ManifestBuilder(conf, fs, workingDir);
      case SnapshotManifestV2.DESCRIPTOR_VERSION:
        return new SnapshotManifestV2.ManifestBuilder(conf, fs, workingDir);
      default:
        throw new CorruptedSnapshotException("Invalid Snapshot version: "+ desc.getVersion(), desc);
    }
  }

  /**
   * Creates a 'manifest' for the specified region, by reading directly from the HRegion object.
   * This is used by the "online snapshot" when the table is enabled.
   */
  public void addRegion(final HRegion region) throws IOException {
    // 0. Get the ManifestBuilder/RegionVisitor
    RegionVisitor visitor = createRegionVisitor(desc);

    // 1. dump region meta info into the snapshot directory
    LOG.debug("Storing '" + region + "' region-info for snapshot.");
    Object regionData = visitor.regionOpen(region.getRegionInfo());
    monitor.rethrowException();

    // 2. iterate through all the stores in the region
    LOG.debug("Creating references for hfiles");

    for (Store store : region.getStores().values()) {
      // 2.1. build the snapshot reference for the store
      Object familyData = visitor.familyOpen(regionData, store.getFamily().getName());
      monitor.rethrowException();

      List<StoreFile> storeFiles = new ArrayList<StoreFile>(store.getStorefiles());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding snapshot references for " + storeFiles  + " hfiles");
      }

      // 2.2. iterate through all the store's files and create "references".
      for (int i = 0, sz = storeFiles.size(); i < sz; i++) {
        StoreFile storeFile = storeFiles.get(i);
        monitor.rethrowException();

        // create "reference" to this store file.
        LOG.debug("Adding reference for file (" + (i+1) + "/" + sz + "): " + storeFile.getPath());
        visitor.storeFile(regionData, familyData, storeFile.getFileInfo());
      }
      visitor.familyClose(regionData, familyData);
    }
    visitor.regionClose(regionData);
  }

  /**
   * Creates a 'manifest' for the specified region, by reading directly from the disk.
   * This is used by the "offline snapshot" when the table is disabled.
   */
  public void addRegion(final Path tableDir, final HRegionInfo regionInfo) throws IOException {
    // 0. Get the ManifestBuilder/RegionVisitor
    RegionVisitor visitor = createRegionVisitor(desc);

    // Open the RegionFS
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(conf, fs,
          tableDir, regionInfo, true);
    monitor.rethrowException();

    // 1. dump region meta info into the snapshot directory
    LOG.debug("Storing region-info for snapshot.");
    Object regionData = visitor.regionOpen(regionInfo);
    monitor.rethrowException();

    // 2. iterate through all the stores in the region
    LOG.debug("Creating references for hfiles");

    // This ensures that we have an atomic view of the directory as long as we have < ls limit
    // (batch size of the files in a directory) on the namenode. Otherwise, we get back the files in
    // batches and may miss files being added/deleted. This could be more robust (iteratively
    // checking to see if we have all the files until we are sure), but the limit is currently 1000
    // files/batch, far more than the number of store files under a single column family.
    Collection<String> familyNames = regionFs.getFamilies();
    if (familyNames != null) {
      for (String familyName: familyNames) {
        Object familyData = visitor.familyOpen(regionData, Bytes.toBytes(familyName));
        monitor.rethrowException();

        Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(familyName);
        if (storeFiles == null) {
          LOG.debug("No files under family: " + familyName);
          continue;
        }

        // 2.1. build the snapshot reference for the store
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding snapshot references for " + storeFiles  + " hfiles");
        }

        // 2.2. iterate through all the store's files and create "references".
        int i = 0;
        int sz = storeFiles.size();
        for (StoreFileInfo storeFile: storeFiles) {
          monitor.rethrowException();

          // create "reference" to this store file.
          LOG.debug("Adding reference for file ("+ (++i) +"/" + sz + "): " + storeFile.getPath());
          visitor.storeFile(regionData, familyData, storeFile);
        }
        visitor.familyClose(regionData, familyData);
      }
    }
    visitor.regionClose(regionData);
  }

  /**
   * Load the information in the SnapshotManifest. Called by SnapshotManifest.open()
   *
   * If the format is v2 and there is no data-manifest, means that we are loading an
   * in-progress snapshot. Since we support rolling-upgrades, we loook for v1 and v2
   * regions format.
   */
  private void load() throws IOException {
    switch (getSnapshotFormat(desc)) {
      case SnapshotManifestV1.DESCRIPTOR_VERSION: {
        this.htd = FSTableDescriptors.getTableDescriptorFromFs(fs, workingDir);
        ThreadPoolExecutor tpool = createExecutor("SnapshotManifestLoader");
        try {
          this.regionManifests =
            SnapshotManifestV1.loadRegionManifests(conf, tpool, fs, workingDir, desc);
        } finally {
          tpool.shutdown();
        }
        break;
      }
      case SnapshotManifestV2.DESCRIPTOR_VERSION: {
        SnapshotDataManifest dataManifest = readDataManifest();
        if (dataManifest != null) {
          htd = HTableDescriptor.convert(dataManifest.getTableSchema());
          regionManifests = dataManifest.getRegionManifestsList();
        } else {
          // Compatibility, load the v1 regions
          // This happens only when the snapshot is in-progress and the cache wants to refresh.
          List<SnapshotRegionManifest> v1Regions, v2Regions;
          ThreadPoolExecutor tpool = createExecutor("SnapshotManifestLoader");
          try {
            v1Regions = SnapshotManifestV1.loadRegionManifests(conf, tpool, fs, workingDir, desc);
            v2Regions = SnapshotManifestV2.loadRegionManifests(conf, tpool, fs, workingDir, desc);
          } finally {
            tpool.shutdown();
          }
          if (v1Regions != null && v2Regions != null) {
            regionManifests =
              new ArrayList<SnapshotRegionManifest>(v1Regions.size() + v2Regions.size());
            regionManifests.addAll(v1Regions);
            regionManifests.addAll(v2Regions);
          } else if (v1Regions != null) {
            regionManifests = v1Regions;
          } else /* if (v2Regions != null) */ {
            regionManifests = v2Regions;
          }
        }
        break;
      }
      default:
        throw new CorruptedSnapshotException("Invalid Snapshot version: "+ desc.getVersion(), desc);
    }
  }

  /**
   * Get the current snapshot working dir
   */
  public Path getSnapshotDir() {
    return this.workingDir;
  }

  /**
   * Get the SnapshotDescription
   */
  public SnapshotDescription getSnapshotDescription() {
    return this.desc;
  }

  /**
   * Get the table descriptor from the Snapshot
   */
  public HTableDescriptor getTableDescriptor() {
    return this.htd;
  }

  /**
   * Get all the Region Manifest from the snapshot
   */
  public List<SnapshotRegionManifest> getRegionManifests() {
    return this.regionManifests;
  }

  /**
   * Get all the Region Manifest from the snapshot.
   * This is an helper to get a map with the region encoded name
   */
  public Map<String, SnapshotRegionManifest> getRegionManifestsMap() {
    if (regionManifests == null || regionManifests.size() == 0) return null;

    HashMap<String, SnapshotRegionManifest> regionsMap =
        new HashMap<String, SnapshotRegionManifest>(regionManifests.size());
    for (SnapshotRegionManifest manifest: regionManifests) {
      String regionName = getRegionNameFromManifest(manifest);
      regionsMap.put(regionName, manifest);
    }
    return regionsMap;
  }

  public void consolidate() throws IOException {
    if (getSnapshotFormat(desc) == SnapshotManifestV1.DESCRIPTOR_VERSION) {
      Path rootDir = FSUtils.getRootDir(conf);
      LOG.info("Using old Snapshot Format");
      // write a copy of descriptor to the snapshot directory
      new FSTableDescriptors(fs, rootDir)
        .createTableDescriptorForTableDirectory(workingDir, htd, false);
    } else {
      LOG.debug("Convert to Single Snapshot Manifest");
      convertToV2SingleManifest();
    }
  }

  /*
   * In case of rolling-upgrade, we try to read all the formats and build
   * the snapshot with the latest format.
   */
  private void convertToV2SingleManifest() throws IOException {
    // Try to load v1 and v2 regions
    List<SnapshotRegionManifest> v1Regions, v2Regions;
    ThreadPoolExecutor tpool = createExecutor("SnapshotManifestLoader");
    try {
      v1Regions = SnapshotManifestV1.loadRegionManifests(conf, tpool, fs, workingDir, desc);
      v2Regions = SnapshotManifestV2.loadRegionManifests(conf, tpool, fs, workingDir, desc);
    } finally {
      tpool.shutdown();
    }

    SnapshotDataManifest.Builder dataManifestBuilder = SnapshotDataManifest.newBuilder();
    dataManifestBuilder.setTableSchema(htd.convert());

    if (v1Regions != null && v1Regions.size() > 0) {
      dataManifestBuilder.addAllRegionManifests(v1Regions);
    }
    if (v2Regions != null && v2Regions.size() > 0) {
      dataManifestBuilder.addAllRegionManifests(v2Regions);
    }

    // Write the v2 Data Manifest.
    // Once the data-manifest is written, the snapshot can be considered complete.
    // Currently snapshots are written in a "temporary" directory and later
    // moved to the "complated" snapshot directory.
    SnapshotDataManifest dataManifest = dataManifestBuilder.build();
    writeDataManifest(dataManifest);
    this.regionManifests = dataManifest.getRegionManifestsList();

    // Remove the region manifests. Everything is now in the data-manifest.
    // The delete operation is "relaxed", unless we get an exception we keep going.
    // The extra files in the snapshot directory will not give any problem,
    // since they have the same content as the data manifest, and even by re-reading
    // them we will get the same information.
    if (v1Regions != null && v1Regions.size() > 0) {
      for (SnapshotRegionManifest regionManifest: v1Regions) {
        SnapshotManifestV1.deleteRegionManifest(fs, workingDir, regionManifest);
      }
    }
    if (v2Regions != null && v2Regions.size() > 0) {
      for (SnapshotRegionManifest regionManifest: v2Regions) {
        SnapshotManifestV2.deleteRegionManifest(fs, workingDir, regionManifest);
      }
    }
  }

  /*
   * Write the SnapshotDataManifest file
   */
  private void writeDataManifest(final SnapshotDataManifest manifest)
      throws IOException {
    FSDataOutputStream stream = fs.create(new Path(workingDir, DATA_MANIFEST_NAME));
    try {
      manifest.writeTo(stream);
    } finally {
      stream.close();
    }
  }

  /*
   * Read the SnapshotDataManifest file
   */
  private SnapshotDataManifest readDataManifest() throws IOException {
    FSDataInputStream in = null;
    try {
      in = fs.open(new Path(workingDir, DATA_MANIFEST_NAME));
      return SnapshotDataManifest.parseFrom(in);
    } catch (FileNotFoundException e) {
      return null;
    } finally {
      if (in != null) in.close();
    }
  }

  private ThreadPoolExecutor createExecutor(final String name) {
    return createExecutor(conf, name);
  }

  public static ThreadPoolExecutor createExecutor(final Configuration conf, final String name) {
    int maxThreads = conf.getInt("hbase.snapshot.thread.pool.max", 8);
    return Threads.getBoundedCachedThreadPool(maxThreads, 30L, TimeUnit.SECONDS,
              Threads.getNamedThreadFactory(name));
  }

  /**
   * Extract the region encoded name from the region manifest
   */
  static String getRegionNameFromManifest(final SnapshotRegionManifest manifest) {
    byte[] regionName = HRegionInfo.createRegionName(
            ProtobufUtil.toTableName(manifest.getRegionInfo().getTableName()),
            manifest.getRegionInfo().getStartKey().toByteArray(),
            manifest.getRegionInfo().getRegionId(), true);
    return HRegionInfo.encodeRegionName(regionName);
  }

  /*
   * Return the snapshot format
   */
  private static int getSnapshotFormat(final SnapshotDescription desc) {
    return desc.hasVersion() ? desc.getVersion() : SnapshotManifestV1.DESCRIPTOR_VERSION;
  }
}
