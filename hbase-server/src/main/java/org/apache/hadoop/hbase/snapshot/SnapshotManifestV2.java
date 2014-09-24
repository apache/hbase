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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * DO NOT USE DIRECTLY. USE {@link SnapshotManifest}.
 *
 * Snapshot v2 layout format
 *  - Single Manifest file containing all the information of regions
 *  - In the online-snapshot case each region will write a "region manifest"
 *      /snapshotName/manifest.regionName
 */
@InterfaceAudience.Private
public class SnapshotManifestV2 {
  private static final Log LOG = LogFactory.getLog(SnapshotManifestV2.class);

  public static final int DESCRIPTOR_VERSION = 2;

  private static final String SNAPSHOT_MANIFEST_PREFIX = "region-manifest.";

  static class ManifestBuilder implements SnapshotManifest.RegionVisitor<
                    SnapshotRegionManifest.Builder, SnapshotRegionManifest.FamilyFiles.Builder> {
    private final Configuration conf;
    private final Path snapshotDir;
    private final FileSystem fs;

    public ManifestBuilder(final Configuration conf, final FileSystem fs, final Path snapshotDir) {
      this.snapshotDir = snapshotDir;
      this.conf = conf;
      this.fs = fs;
    }

    public SnapshotRegionManifest.Builder regionOpen(final HRegionInfo regionInfo) {
      SnapshotRegionManifest.Builder manifest = SnapshotRegionManifest.newBuilder();
      manifest.setRegionInfo(HRegionInfo.convert(regionInfo));
      return manifest;
    }

    public void regionClose(final SnapshotRegionManifest.Builder region) throws IOException {
      SnapshotRegionManifest manifest = region.build();
      FSDataOutputStream stream = fs.create(getRegionManifestPath(snapshotDir, manifest));
      try {
        manifest.writeTo(stream);
      } finally {
        stream.close();
      }
    }

    public SnapshotRegionManifest.FamilyFiles.Builder familyOpen(
        final SnapshotRegionManifest.Builder region, final byte[] familyName) {
      SnapshotRegionManifest.FamilyFiles.Builder family =
          SnapshotRegionManifest.FamilyFiles.newBuilder();
      family.setFamilyName(ByteStringer.wrap(familyName));
      return family;
    }

    public void familyClose(final SnapshotRegionManifest.Builder region,
        final SnapshotRegionManifest.FamilyFiles.Builder family) {
      region.addFamilyFiles(family.build());
    }

    public void storeFile(final SnapshotRegionManifest.Builder region,
        final SnapshotRegionManifest.FamilyFiles.Builder family, final StoreFileInfo storeFile)
        throws IOException {
      SnapshotRegionManifest.StoreFile.Builder sfManifest =
            SnapshotRegionManifest.StoreFile.newBuilder();
      sfManifest.setName(storeFile.getPath().getName());
      if (storeFile.isReference()) {
        sfManifest.setReference(storeFile.getReference().convert());
      }
      sfManifest.setFileSize(storeFile.getReferencedFileStatus(fs).getLen());
      family.addStoreFiles(sfManifest.build());
    }
  }

  static List<SnapshotRegionManifest> loadRegionManifests(final Configuration conf,
      final Executor executor,final FileSystem fs, final Path snapshotDir,
      final SnapshotDescription desc) throws IOException {
    FileStatus[] manifestFiles = FSUtils.listStatus(fs, snapshotDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(SNAPSHOT_MANIFEST_PREFIX);
      }
    });

    if (manifestFiles == null || manifestFiles.length == 0) return null;

    final ExecutorCompletionService<SnapshotRegionManifest> completionService =
      new ExecutorCompletionService<SnapshotRegionManifest>(executor);
    for (final FileStatus st: manifestFiles) {
      completionService.submit(new Callable<SnapshotRegionManifest>() {
        @Override
        public SnapshotRegionManifest call() throws IOException {
          FSDataInputStream stream = fs.open(st.getPath());
          try {
            return SnapshotRegionManifest.parseFrom(stream);
          } finally {
            stream.close();
          }
        }
      });
    }

    ArrayList<SnapshotRegionManifest> regionsManifest =
        new ArrayList<SnapshotRegionManifest>(manifestFiles.length);
    try {
      for (int i = 0; i < manifestFiles.length; ++i) {
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
    fs.delete(getRegionManifestPath(snapshotDir, manifest), true);
  }

  private static Path getRegionManifestPath(final Path snapshotDir,
      final SnapshotRegionManifest manifest) {
    String regionName = SnapshotManifest.getRegionNameFromManifest(manifest);
    return new Path(snapshotDir, SNAPSHOT_MANIFEST_PREFIX + regionName);
  }
}
