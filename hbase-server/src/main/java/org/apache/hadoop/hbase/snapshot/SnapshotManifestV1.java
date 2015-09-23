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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * DO NOT USE DIRECTLY. USE {@link SnapshotManifest}.
 *
 * Snapshot v1 layout format
 *  - Each region in the table is represented by a directory with the .hregioninfo file
 *      /snapshotName/regionName/.hregioninfo
 *  - Each file present in the table is represented by an empty file
 *      /snapshotName/regionName/familyName/fileName
 */
@InterfaceAudience.Private
public final class SnapshotManifestV1 {
  private static final Log LOG = LogFactory.getLog(SnapshotManifestV1.class);

  public static final int DESCRIPTOR_VERSION = 0;

  private SnapshotManifestV1() {
  }

  static class ManifestBuilder implements SnapshotManifest.RegionVisitor<
                                                          HRegionFileSystem, Path> {
    private final Configuration conf;
    private final Path snapshotDir;
    private final FileSystem fs;

    public ManifestBuilder(final Configuration conf, final FileSystem fs, final Path snapshotDir) {
      this.snapshotDir = snapshotDir;
      this.conf = conf;
      this.fs = fs;
    }

    public HRegionFileSystem regionOpen(final HRegionInfo regionInfo) throws IOException {
      HRegionFileSystem snapshotRegionFs = HRegionFileSystem.createRegionOnFileSystem(conf,
        fs, snapshotDir, regionInfo);
      return snapshotRegionFs;
    }

    public void regionClose(final HRegionFileSystem region) {
    }

    public Path familyOpen(final HRegionFileSystem snapshotRegionFs, final byte[] familyName) {
      Path familyDir = snapshotRegionFs.getStoreDir(Bytes.toString(familyName));
      return familyDir;
    }

    public void familyClose(final HRegionFileSystem region, final Path family) {
    }

    public void storeFile(final HRegionFileSystem region, final Path familyDir,
        final StoreFileInfo storeFile) throws IOException {
      Path referenceFile = new Path(familyDir, storeFile.getPath().getName());
      boolean success = true;
      if (storeFile.isReference()) {
        // write the Reference object to the snapshot
        storeFile.getReference().write(fs, referenceFile);
      } else {
        // create "reference" to this store file.  It is intentionally an empty file -- all
        // necessary information is captured by its fs location and filename.  This allows us to
        // only figure out what needs to be done via a single nn operation (instead of having to
        // open and read the files as well).
        success = fs.createNewFile(referenceFile);
      }
      if (!success) {
        throw new IOException("Failed to create reference file:" + referenceFile);
      }
    }
  }

  static List<SnapshotRegionManifest> loadRegionManifests(final Configuration conf,
      final Executor executor,final FileSystem fs, final Path snapshotDir,
      final SnapshotDescription desc) throws IOException {
    FileStatus[] regions = FSUtils.listStatus(fs, snapshotDir, new FSUtils.RegionDirFilter(fs));
    if (regions == null) {
      LOG.info("No regions under directory:" + snapshotDir);
      return null;
    }

    final ExecutorCompletionService<SnapshotRegionManifest> completionService =
      new ExecutorCompletionService<SnapshotRegionManifest>(executor);
    for (final FileStatus region: regions) {
      completionService.submit(new Callable<SnapshotRegionManifest>() {
        @Override
        public SnapshotRegionManifest call() throws IOException {
          HRegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, region.getPath());
          return buildManifestFromDisk(conf, fs, snapshotDir, hri);
        }
      });
    }

    ArrayList<SnapshotRegionManifest> regionsManifest =
        new ArrayList<SnapshotRegionManifest>(regions.length);
    try {
      for (int i = 0; i < regions.length; ++i) {
        regionsManifest.add(completionService.take().get());
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      IOException ex = new IOException();
      ex.initCause(e.getCause());
      throw ex;
    }
    return regionsManifest;
  }

  static void deleteRegionManifest(final FileSystem fs, final Path snapshotDir,
      final SnapshotRegionManifest manifest) throws IOException {
    String regionName = SnapshotManifest.getRegionNameFromManifest(manifest);
    fs.delete(new Path(snapshotDir, regionName), true);
  }

  static SnapshotRegionManifest buildManifestFromDisk(final Configuration conf,
      final FileSystem fs, final Path tableDir, final HRegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(conf, fs,
          tableDir, regionInfo, true);
    SnapshotRegionManifest.Builder manifest = SnapshotRegionManifest.newBuilder();

    // 1. dump region meta info into the snapshot directory
    LOG.debug("Storing region-info for snapshot.");
    manifest.setRegionInfo(HRegionInfo.convert(regionInfo));

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
        Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(familyName, false);
        if (storeFiles == null) {
          LOG.debug("No files under family: " + familyName);
          continue;
        }

        // 2.1. build the snapshot reference for the store
        SnapshotRegionManifest.FamilyFiles.Builder family =
              SnapshotRegionManifest.FamilyFiles.newBuilder();
        family.setFamilyName(ByteStringer.wrap(Bytes.toBytes(familyName)));

        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding snapshot references for " + storeFiles  + " hfiles");
        }

        // 2.2. iterate through all the store's files and create "references".
        int i = 0;
        int sz = storeFiles.size();
        for (StoreFileInfo storeFile: storeFiles) {
          // create "reference" to this store file.
          LOG.debug("Adding reference for file ("+ (++i) +"/" + sz + "): " + storeFile.getPath());
          SnapshotRegionManifest.StoreFile.Builder sfManifest =
                SnapshotRegionManifest.StoreFile.newBuilder();
          sfManifest.setName(storeFile.getPath().getName());
          family.addStoreFiles(sfManifest.build());
        }
        manifest.addFamilyFiles(family.build());
      }
    }
    return manifest.build();
  }
}
