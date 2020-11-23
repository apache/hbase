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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * DO NOT USE DIRECTLY. USE {@link SnapshotManifest}.
 *
 * Snapshot v2 layout format
 *  - Single Manifest file containing all the information of regions
 *  - In the online-snapshot case each region will write a "region manifest"
 *      /snapshotName/manifest.regionName
 */
@InterfaceAudience.Private
public final class SnapshotManifestV2 {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManifestV2.class);

  public static final int DESCRIPTOR_VERSION = 2;

  public static final String SNAPSHOT_MANIFEST_PREFIX = "region-manifest.";

  private SnapshotManifestV2() {}

  static class ManifestBuilder implements SnapshotManifest.RegionVisitor<
                    SnapshotRegionManifest.Builder, SnapshotRegionManifest.FamilyFiles.Builder> {
    private final Configuration conf;
    private final Path snapshotDir;
    private final FileSystem rootFs;

    public ManifestBuilder(final Configuration conf, final FileSystem rootFs,
        final Path snapshotDir) {
      this.snapshotDir = snapshotDir;
      this.conf = conf;
      this.rootFs = rootFs;
    }

    @Override
    public SnapshotRegionManifest.Builder regionOpen(final RegionInfo regionInfo) {
      SnapshotRegionManifest.Builder manifest = SnapshotRegionManifest.newBuilder();
      manifest.setRegionInfo(ProtobufUtil.toRegionInfo(regionInfo));
      return manifest;
    }

    @Override
    public void regionClose(final SnapshotRegionManifest.Builder region) throws IOException {
      // we should ensure the snapshot dir exist, maybe it has been deleted by master
      // see HBASE-16464
      FileSystem workingDirFs = snapshotDir.getFileSystem(this.conf);
      if (workingDirFs.exists(snapshotDir)) {
        SnapshotRegionManifest manifest = region.build();
        try (FSDataOutputStream stream = workingDirFs.create(
            getRegionManifestPath(snapshotDir, manifest))) {
          manifest.writeTo(stream);
        }
      } else {
        LOG.warn("can't write manifest without parent dir, maybe it has been deleted by master?");
      }
    }

    @Override
    public SnapshotRegionManifest.FamilyFiles.Builder familyOpen(
        final SnapshotRegionManifest.Builder region, final byte[] familyName) {
      SnapshotRegionManifest.FamilyFiles.Builder family =
          SnapshotRegionManifest.FamilyFiles.newBuilder();
      family.setFamilyName(UnsafeByteOperations.unsafeWrap(familyName));
      return family;
    }

    @Override
    public void familyClose(final SnapshotRegionManifest.Builder region,
        final SnapshotRegionManifest.FamilyFiles.Builder family) {
      region.addFamilyFiles(family.build());
    }

    @Override
    public void storeFile(final SnapshotRegionManifest.Builder region,
        final SnapshotRegionManifest.FamilyFiles.Builder family, final StoreFileInfo storeFile)
        throws IOException {
      SnapshotRegionManifest.StoreFile.Builder sfManifest =
            SnapshotRegionManifest.StoreFile.newBuilder();
      sfManifest.setName(storeFile.getPath().getName());
      if (storeFile.isReference()) {
        sfManifest.setReference(storeFile.getReference().convert());
      }
      if (!storeFile.isReference() && !storeFile.isLink()) {
        sfManifest.setFileSize(storeFile.getSize());
      } else {
        sfManifest.setFileSize(storeFile.getReferencedFileStatus(rootFs).getLen());
      }
      family.addStoreFiles(sfManifest.build());
    }
  }

  static List<SnapshotRegionManifest> loadRegionManifests(final Configuration conf,
      final Executor executor, final FileSystem fs, final Path snapshotDir,
      final SnapshotDescription desc, final int manifestSizeLimit) throws IOException {
    FileStatus[] manifestFiles = CommonFSUtils.listStatus(fs, snapshotDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(SNAPSHOT_MANIFEST_PREFIX);
      }
    });

    if (manifestFiles == null || manifestFiles.length == 0) return null;

    final ExecutorCompletionService<SnapshotRegionManifest> completionService =
      new ExecutorCompletionService<>(executor);
    for (final FileStatus st: manifestFiles) {
      completionService.submit(new Callable<SnapshotRegionManifest>() {
        @Override
        public SnapshotRegionManifest call() throws IOException {
          try (FSDataInputStream stream = fs.open(st.getPath())) {
            CodedInputStream cin = CodedInputStream.newInstance(stream);
            cin.setSizeLimit(manifestSizeLimit);
            return SnapshotRegionManifest.parseFrom(cin);
          }
        }
      });
    }

    ArrayList<SnapshotRegionManifest> regionsManifest = new ArrayList<>(manifestFiles.length);
    try {
      for (int i = 0; i < manifestFiles.length; ++i) {
        regionsManifest.add(completionService.take().get());
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      Throwable t = e.getCause();

      if(t instanceof InvalidProtocolBufferException) {
        throw (InvalidProtocolBufferException)t;
      } else {
        throw new IOException("ExecutionException", e.getCause());
      }
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
